#include "skills/plane_skills_service.h"

#include <algorithm>
#include <set>
#include <stdexcept>

namespace comet::controller {

namespace {

using nlohmann::json;

std::vector<std::pair<std::string, std::string>> DefaultJsonHeaders() {
  return {{"Content-Type", "application/json"}};
}

const InstanceSpec* FindSkillsInstance(const DesiredState& desired_state) {
  const auto it = std::find_if(
      desired_state.instances.begin(),
      desired_state.instances.end(),
      [](const InstanceSpec& instance) { return instance.role == InstanceRole::Skills; });
  if (it == desired_state.instances.end()) {
    return nullptr;
  }
  return &*it;
}

std::string NormalizeSkillPathSuffix(const std::string& path_suffix) {
  if (path_suffix.empty() || path_suffix == "/") {
    return "/v1/skills";
  }
  if (path_suffix.front() == '/') {
    return "/v1/skills" + path_suffix;
  }
  return "/v1/skills/" + path_suffix;
}

}  // namespace

bool PlaneSkillsService::IsEnabled(const DesiredState& desired_state) const {
  return desired_state.skills.has_value() && desired_state.skills->enabled;
}

std::optional<ControllerEndpointTarget> PlaneSkillsService::ResolveTarget(
    const DesiredState& desired_state) const {
  const auto* skills = FindSkillsInstance(desired_state);
  if (skills == nullptr) {
    return std::nullopt;
  }
  const auto published = std::find_if(
      skills->published_ports.begin(),
      skills->published_ports.end(),
      [](const PublishedPort& port) { return port.host_port > 0; });
  if (published == skills->published_ports.end()) {
    return std::nullopt;
  }
  ControllerEndpointTarget target;
  target.host = skills->node_name;
  target.port = published->host_port;
  target.raw = "http://" + target.host + ":" + std::to_string(target.port);
  return target;
}

std::optional<std::string> PlaneSkillsService::ParseSessionId(const json& payload) {
  if (!payload.contains("session_id") || payload.at("session_id").is_null()) {
    return std::nullopt;
  }
  if (!payload.at("session_id").is_string()) {
    throw std::runtime_error("session_id must be a string");
  }
  const std::string value = payload.at("session_id").get<std::string>();
  if (value.empty()) {
    throw std::runtime_error("session_id must not be empty");
  }
  return value;
}

std::optional<std::vector<std::string>> PlaneSkillsService::ParseSkillIds(const json& payload) {
  if (!payload.contains("skill_ids") || payload.at("skill_ids").is_null()) {
    return std::nullopt;
  }
  if (!payload.at("skill_ids").is_array()) {
    throw std::runtime_error("skill_ids must be an array");
  }
  std::vector<std::string> result;
  result.reserve(payload.at("skill_ids").size());
  std::set<std::string> seen;
  for (const auto& item : payload.at("skill_ids")) {
    if (!item.is_string()) {
      throw std::runtime_error("skill_ids items must be strings");
    }
    const std::string skill_id = item.get<std::string>();
    if (skill_id.empty()) {
      throw std::runtime_error("skill_ids items must not be empty");
    }
    if (seen.insert(skill_id).second) {
      result.push_back(skill_id);
    }
  }
  return result;
}

std::string PlaneSkillsService::BuildSystemInstruction(const json& skills) {
  std::string instruction = "Skills currently applied for this request:";
  for (const auto& skill : skills) {
    const std::string name = skill.value("name", std::string{});
    const std::string description = skill.value("description", std::string{});
    const std::string content = skill.value("content", std::string{});
    if (content.empty()) {
      continue;
    }
    instruction += "\n\nSkill: " + name;
    if (!description.empty()) {
      instruction += "\nDescription: " + description;
    }
    instruction += "\n\nInstructions:\n" + content;
  }
  return instruction;
}

std::optional<InteractionValidationError> PlaneSkillsService::ResolveInteractionSkills(
    const PlaneInteractionResolution& resolution,
    InteractionRequestContext* context) const {
  if (context == nullptr) {
    throw std::invalid_argument("interaction request context is required");
  }

  std::optional<std::string> session_id;
  std::optional<std::vector<std::string>> skill_ids;
  try {
    session_id = ParseSessionId(context->payload);
    skill_ids = ParseSkillIds(context->payload);
  } catch (const std::exception& error) {
    return InteractionValidationError{
        "malformed_request",
        error.what(),
        false,
        json::object(),
    };
  }

  const bool wants_skills =
      session_id.has_value() || (skill_ids.has_value() && !skill_ids->empty());
  if (!wants_skills) {
    return std::nullopt;
  }
  if (!IsEnabled(resolution.desired_state)) {
    return InteractionValidationError{
        "skills_disabled",
        "skills are not enabled for this plane",
        false,
        json::object(),
    };
  }

  const auto target = ResolveTarget(resolution.desired_state);
  if (!target.has_value()) {
    return InteractionValidationError{
        "skills_not_ready",
        "skills service is not ready for this plane",
        true,
        json::object(),
    };
  }

  json request_payload = json::object();
  if (session_id.has_value()) {
    request_payload["session_id"] = *session_id;
  }
  if (skill_ids.has_value()) {
    request_payload["skill_ids"] = *skill_ids;
  }

  try {
    const HttpResponse response = SendControllerHttpRequest(
        *target, "POST", "/v1/skills/resolve", request_payload.dump(), DefaultJsonHeaders());
    if (response.status_code == 400) {
      json error_payload = response.body.empty() ? json::object() : json::parse(response.body);
      const std::string code = error_payload.value("error", std::string("invalid_skill_reference"));
      return InteractionValidationError{
          code == "invalid_skill_reference" ? code : "invalid_skill_reference",
          error_payload.value("message", std::string("invalid skill reference")),
          false,
          json::object(),
      };
    }
    if (response.status_code != 200) {
      return InteractionValidationError{
          "skills_not_ready",
          "skills service is not ready for this plane",
          true,
          json::object(),
      };
    }

    const json response_payload =
        response.body.empty() ? json::object() : json::parse(response.body);
    const json resolved_skills = response_payload.value("skills", json::array());
    if (!resolved_skills.is_array()) {
      return InteractionValidationError{
          "upstream_invalid_response",
          "skills service returned malformed resolve payload",
          true,
          json::object(),
      };
    }

    json applied_skills = json::array();
    for (const auto& skill : resolved_skills) {
      if (!skill.is_object()) {
        continue;
      }
      applied_skills.push_back(
          json{{"id", skill.value("id", std::string{})},
               {"name", skill.value("name", std::string{})},
               {"source", skill.value("source", std::string{})}});
    }
    if (!resolved_skills.empty()) {
      context->payload[kSystemInstructionPayloadKey] = BuildSystemInstruction(resolved_skills);
    }
    context->payload[kAppliedSkillsPayloadKey] = applied_skills;
    if (response_payload.contains("skills_session_id") &&
        !response_payload.at("skills_session_id").is_null()) {
      context->payload[kSkillsSessionIdPayloadKey] = response_payload.at("skills_session_id");
    } else if (session_id.has_value()) {
      context->payload[kSkillsSessionIdPayloadKey] = *session_id;
    }
  } catch (const std::exception&) {
    return InteractionValidationError{
        "skills_not_ready",
        "skills service is not ready for this plane",
        true,
        json::object(),
    };
  }

  return std::nullopt;
}

std::optional<HttpResponse> PlaneSkillsService::ProxyPlaneSkillsRequest(
    const PlaneInteractionResolution& resolution,
    const std::string& method,
    const std::string& path_suffix,
    const std::string& body,
    std::string* error_code,
    std::string* error_message) const {
  if (!IsEnabled(resolution.desired_state)) {
    if (error_code != nullptr) {
      *error_code = "skills_disabled";
    }
    if (error_message != nullptr) {
      *error_message = "skills are not enabled for this plane";
    }
    return std::nullopt;
  }

  const auto target = ResolveTarget(resolution.desired_state);
  if (!target.has_value()) {
    if (error_code != nullptr) {
      *error_code = "skills_not_ready";
    }
    if (error_message != nullptr) {
      *error_message = "skills service is not ready for this plane";
    }
    return std::nullopt;
  }

  try {
    return SendControllerHttpRequest(
        *target, method, NormalizeSkillPathSuffix(path_suffix), body, DefaultJsonHeaders());
  } catch (const std::exception&) {
    if (error_code != nullptr) {
      *error_code = "skills_not_ready";
    }
    if (error_message != nullptr) {
      *error_message = "skills service is not ready for this plane";
    }
    return std::nullopt;
  }
}

}  // namespace comet::controller
