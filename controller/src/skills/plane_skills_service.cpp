#include "skills/plane_skills_service.h"

#include <algorithm>
#include <set>
#include <stdexcept>

#include "naim/state/sqlite_store.h"
#include "skills/plane_skill_contextual_resolver_service.h"
#include "skills/plane_skill_runtime_sync_service.h"
#include "skills/plane_skills_target_resolver.h"

namespace naim::controller {

using nlohmann::json;

namespace {

bool ContainsSkillId(
    const std::vector<std::string>& skill_ids,
    const std::string& skill_id) {
  return std::find(skill_ids.begin(), skill_ids.end(), skill_id) != skill_ids.end();
}

std::optional<json> ResolveSkillsFromControllerCatalog(
    const std::string& db_path,
    const DesiredState& desired_state,
    const json& request_payload,
    const ContextualSkillSelection& contextual_selection,
    std::string* error_code,
    std::string* error_message) {
  if (db_path.empty() || !desired_state.skills.has_value() ||
      !desired_state.skills->enabled) {
    return std::nullopt;
  }

  const auto requested = request_payload.value("skill_ids", json::array());
  if (!requested.is_array()) {
    if (error_code != nullptr) {
      *error_code = "invalid_skill_reference";
    }
    if (error_message != nullptr) {
      *error_message = "skill_ids must be an array";
    }
    return std::nullopt;
  }

  ControllerStore store(db_path);
  store.Initialize();
  std::set<std::string> contextual_ids(
      contextual_selection.selected_skill_ids.begin(),
      contextual_selection.selected_skill_ids.end());
  json skills = json::array();
  for (const auto& item : requested) {
    if (!item.is_string()) {
      if (error_code != nullptr) {
        *error_code = "invalid_skill_reference";
      }
      if (error_message != nullptr) {
        *error_message = "skill_ids items must be strings";
      }
      return std::nullopt;
    }
    const auto skill_id = item.get<std::string>();
    if (!ContainsSkillId(desired_state.skills->factory_skill_ids, skill_id)) {
      if (error_code != nullptr) {
        *error_code = "invalid_skill_reference";
      }
      if (error_message != nullptr) {
        *error_message = "skill '" + skill_id + "' is not attached to this plane";
      }
      return std::nullopt;
    }
    const auto canonical = store.LoadSkillsFactorySkill(skill_id);
    if (!canonical.has_value()) {
      if (error_code != nullptr) {
        *error_code = "invalid_skill_reference";
      }
      if (error_message != nullptr) {
        *error_message = "skill '" + skill_id + "' not found";
      }
      return std::nullopt;
    }
    const auto binding =
        store.LoadPlaneSkillBinding(desired_state.plane_name, skill_id);
    if (binding.has_value() && !binding->enabled) {
      if (error_code != nullptr) {
        *error_code = "invalid_skill_reference";
      }
      if (error_message != nullptr) {
        *error_message = "skill '" + skill_id + "' is disabled for this plane";
      }
      return std::nullopt;
    }
    skills.push_back(json{
        {"id", canonical->id},
        {"name", canonical->name},
        {"description", canonical->description},
        {"content", canonical->content},
        {"source", contextual_ids.count(canonical->id) > 0 ? "contextual" : "explicit"},
    });
  }

  json payload{{"skills", skills}};
  if (request_payload.contains("session_id") &&
      !request_payload.at("session_id").is_null()) {
    payload["skills_session_id"] = request_payload.at("session_id");
  }
  return payload;
}

}  // namespace

bool PlaneSkillsService::IsEnabled(const DesiredState& desired_state) const {
  return desired_state.skills.has_value() && desired_state.skills->enabled;
}

std::optional<ControllerEndpointTarget> PlaneSkillsService::ResolveTarget(
    const DesiredState& desired_state) const {
  return PlaneSkillsTargetResolver::ResolvePlaneLocalTarget(desired_state);
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

bool PlaneSkillsService::ParseAutoSkills(const json& payload) {
  if (!payload.contains("auto_skills") || payload.at("auto_skills").is_null()) {
    return true;
  }
  if (!payload.at("auto_skills").is_boolean()) {
    throw std::runtime_error("auto_skills must be a boolean");
  }
  return payload.at("auto_skills").get<bool>();
}

PlaneSkillsService::ParsedInteractionSkillRequest
PlaneSkillsService::ParseInteractionSkillRequest(const json& payload) {
  ParsedInteractionSkillRequest request;
  request.session_id = ParseSessionId(payload);
  request.skill_ids = ParseSkillIds(payload);
  request.auto_skills = ParseAutoSkills(payload);
  return request;
}

void PlaneSkillsService::SetNoResolvedSkills(InteractionRequestContext* context) {
  if (context == nullptr) {
    return;
  }
  context->payload[kAppliedSkillsPayloadKey] = json::array();
  context->payload[kAutoAppliedSkillsPayloadKey] = json::array();
  context->payload[kSkillResolutionModePayloadKey] = "none";
}

json PlaneSkillsService::BuildResolveRequestPayload(
    const ParsedInteractionSkillRequest& request,
    const ContextualSkillSelection& contextual_selection) {
  json request_payload = json::object();
  if (request.session_id.has_value()) {
    request_payload["session_id"] = *request.session_id;
  }
  if (request.HasExplicitSkillIds()) {
    request_payload["skill_ids"] = *request.skill_ids;
  } else if (!contextual_selection.selected_skill_ids.empty()) {
    request_payload["skill_ids"] = contextual_selection.selected_skill_ids;
  }
  return request_payload;
}

void PlaneSkillsService::ApplyResolvedSkillMetadata(
    const ParsedInteractionSkillRequest& request,
    const ContextualSkillSelection& contextual_selection,
    const nlohmann::json& resolved_skills,
    const nlohmann::json& response_payload,
    InteractionRequestContext* context) {
  if (context == nullptr) {
    return;
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
    context->payload[kSystemInstructionPayloadKey] =
        BuildSystemInstruction(resolved_skills);
  }
  context->payload[kAppliedSkillsPayloadKey] = applied_skills;
  context->payload[kAutoAppliedSkillsPayloadKey] =
      contextual_selection.mode == "contextual" ? applied_skills : json::array();
  context->payload[kSkillResolutionModePayloadKey] =
      contextual_selection.mode == "contextual" ? "contextual" : "explicit";
  if (response_payload.contains("skills_session_id") &&
      !response_payload.at("skills_session_id").is_null()) {
    context->payload[kSkillsSessionIdPayloadKey] =
        response_payload.at("skills_session_id");
  } else if (request.session_id.has_value()) {
    context->payload[kSkillsSessionIdPayloadKey] = *request.session_id;
  }
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

  ParsedInteractionSkillRequest request;
  try {
    request = ParseInteractionSkillRequest(context->payload);
  } catch (const std::exception& error) {
    return InteractionValidationError{
        "malformed_request",
        error.what(),
        false,
        json::object(),
    };
  }

  ContextualSkillSelection contextual_selection;
  if (!request.HasExplicitSelection() &&
      request.auto_skills &&
      !resolution.db_path.empty()) {
    try {
      contextual_selection = PlaneSkillContextualResolverService().Resolve(
          resolution.db_path, resolution, context->payload);
    } catch (const std::exception& error) {
      return InteractionValidationError{
          "malformed_request",
          error.what(),
          false,
          json::object(),
      };
    }
  }

  if (!request.HasExplicitSelection() && contextual_selection.mode == "none") {
    SetNoResolvedSkills(context);
    return std::nullopt;
  }

  if (!IsEnabled(resolution.desired_state)) {
    if (!request.HasExplicitSelection()) {
      SetNoResolvedSkills(context);
      return std::nullopt;
    }
    return InteractionValidationError{
        "skills_disabled",
        "skills are not enabled for this plane",
        false,
        json::object(),
    };
  }

  const json request_payload =
      BuildResolveRequestPayload(request, contextual_selection);

  auto apply_controller_catalog_fallback =
      [&](const std::string& fallback_error_code,
          const std::string& fallback_error_message)
      -> std::optional<InteractionValidationError> {
    std::string catalog_error_code;
    std::string catalog_error_message;
    const auto response_payload = ResolveSkillsFromControllerCatalog(
        resolution.db_path,
        resolution.desired_state,
        request_payload,
        contextual_selection,
        &catalog_error_code,
        &catalog_error_message);
    if (response_payload.has_value()) {
      const json resolved_skills = response_payload->value("skills", json::array());
      ApplyResolvedSkillMetadata(
          request,
          contextual_selection,
          resolved_skills,
          *response_payload,
          context);
      return std::nullopt;
    }
    if (!catalog_error_code.empty()) {
      return InteractionValidationError{
          catalog_error_code,
          catalog_error_message.empty() ? "invalid skill reference" : catalog_error_message,
          false,
          json::object(),
      };
    }
    return InteractionValidationError{
        fallback_error_code,
        fallback_error_message,
        fallback_error_code == "skills_not_ready",
        json::object(),
    };
  };

  const auto target = ResolveTarget(resolution.desired_state);
  if (!target.has_value()) {
    return apply_controller_catalog_fallback(
        "skills_not_ready",
        "skills service is not ready for this plane");
  }

  try {
    if (!resolution.db_path.empty()) {
      (void)PlaneSkillRuntimeSyncService().SyncPlane(
          resolution.db_path, resolution.desired_state);
    }
    const HttpResponse response = SendControllerHttpRequest(
        *target,
        "POST",
        "/v1/skills/resolve",
        request_payload.dump(),
        PlaneSkillsTargetResolver::DefaultJsonHeaders());
    if (response.status_code == 400) {
      json error_payload = response.body.empty() ? json::object() : json::parse(response.body);
      const std::string code = error_payload.value("error", std::string("invalid_skill_reference"));
      return apply_controller_catalog_fallback(
          code == "invalid_skill_reference" ? code : "invalid_skill_reference",
          error_payload.value("message", std::string("invalid skill reference")));
    }
    if (response.status_code != 200) {
      return apply_controller_catalog_fallback(
          "skills_not_ready",
          "skills service is not ready for this plane");
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
    ApplyResolvedSkillMetadata(
        request, contextual_selection, resolved_skills, response_payload, context);
  } catch (const std::exception&) {
    return apply_controller_catalog_fallback(
        "skills_not_ready",
        "skills service is not ready for this plane");
  }

  return std::nullopt;
}

nlohmann::json PlaneSkillsService::BuildContextResolutionPayload(
    const std::string& db_path,
    const PlaneInteractionResolution& resolution,
    const nlohmann::json& payload) const {
  return PlaneSkillContextualResolverService().BuildDebugPayload(
      db_path, resolution, payload);
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
        *target,
        method,
        PlaneSkillsTargetResolver::NormalizeSkillPathSuffix(path_suffix),
        body,
        PlaneSkillsTargetResolver::DefaultJsonHeaders());
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

}  // namespace naim::controller
