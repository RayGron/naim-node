#include "interaction/interaction_request_contract_support.h"

#include <cctype>
#include <vector>

#include "browsing/interaction_browsing_service.h"
#include "skills/plane_skills_service.h"

namespace naim::controller {

bool InteractionRequestContractSupport::PayloadContainsUnsupportedInteractionField(
    const nlohmann::json& payload,
    std::string* field_name) const {
  static const std::vector<std::string> unsupported_fields = {
      "tools",
      "tool_choice",
      "functions",
      "function_call",
  };
  for (const auto& field : unsupported_fields) {
    if (payload.contains(field)) {
      if (field_name != nullptr) {
        *field_name = field;
      }
      return true;
    }
  }
  return false;
}

std::optional<std::string> InteractionRequestContractSupport::ParsePublicSessionId(
    const nlohmann::json& payload) const {
  if (!payload.contains("session_id") || payload.at("session_id").is_null()) {
    return std::nullopt;
  }
  if (!payload.at("session_id").is_string()) {
    throw std::runtime_error("session_id must be a string");
  }
  const std::string session_id = TrimCopy(payload.at("session_id").get<std::string>());
  if (session_id.empty()) {
    throw std::runtime_error("session_id must not be empty");
  }
  return session_id;
}

nlohmann::json InteractionRequestContractSupport::RequestAppliedSkills(
    const InteractionRequestContext& request_context) const {
  if (request_context.payload.contains(PlaneSkillsService::kAppliedSkillsPayloadKey) &&
      request_context.payload.at(PlaneSkillsService::kAppliedSkillsPayloadKey).is_array()) {
    return request_context.payload.at(PlaneSkillsService::kAppliedSkillsPayloadKey);
  }
  return nlohmann::json::array();
}

nlohmann::json InteractionRequestContractSupport::RequestAutoAppliedSkills(
    const InteractionRequestContext& request_context) const {
  if (request_context.payload.contains(
          PlaneSkillsService::kAutoAppliedSkillsPayloadKey) &&
      request_context.payload.at(
          PlaneSkillsService::kAutoAppliedSkillsPayloadKey).is_array()) {
    return request_context.payload.at(
        PlaneSkillsService::kAutoAppliedSkillsPayloadKey);
  }
  return nlohmann::json::array();
}

std::optional<std::string> InteractionRequestContractSupport::RequestSkillsSessionId(
    const InteractionRequestContext& request_context) const {
  if (request_context.payload.contains(PlaneSkillsService::kSkillsSessionIdPayloadKey) &&
      request_context.payload.at(PlaneSkillsService::kSkillsSessionIdPayloadKey).is_string()) {
    return request_context.payload.at(PlaneSkillsService::kSkillsSessionIdPayloadKey)
        .get<std::string>();
  }
  return std::nullopt;
}

std::string InteractionRequestContractSupport::RequestSkillResolutionMode(
    const InteractionRequestContext& request_context) const {
  if (request_context.payload.contains(
          PlaneSkillsService::kSkillResolutionModePayloadKey) &&
      request_context.payload.at(
          PlaneSkillsService::kSkillResolutionModePayloadKey).is_string()) {
    return request_context.payload.at(
        PlaneSkillsService::kSkillResolutionModePayloadKey)
        .get<std::string>();
  }
  return "none";
}

nlohmann::json InteractionRequestContractSupport::RequestBrowsingSummary(
    const InteractionRequestContext& request_context) const {
  if (request_context.payload.contains(
          InteractionBrowsingService::kWebGatewayContextPayloadKey) &&
      request_context.payload.at(
          InteractionBrowsingService::kWebGatewayContextPayloadKey).is_object()) {
    return request_context.payload.at(
        InteractionBrowsingService::kWebGatewayContextPayloadKey);
  }
  if (request_context.payload.contains(
          InteractionBrowsingService::kSummaryPayloadKey) &&
      request_context.payload.at(
          InteractionBrowsingService::kSummaryPayloadKey).is_object()) {
    return request_context.payload.at(
        InteractionBrowsingService::kSummaryPayloadKey);
  }
  return DefaultBrowsingSummary();
}

std::optional<std::string> InteractionRequestContractSupport::ParseInteractionStreamPlaneName(
    const std::string& request_method,
    const std::string& request_path) const {
  if (request_method != "POST") {
    return std::nullopt;
  }
  constexpr std::string_view kPrefix = "/api/v1/planes/";
  constexpr std::string_view kSuffix = "/interaction/chat/completions/stream";
  if (request_path.rfind(std::string(kPrefix), 0) != 0) {
    return std::nullopt;
  }
  if (request_path.size() <= kPrefix.size() + kSuffix.size()) {
    return std::nullopt;
  }
  if (!request_path.ends_with(kSuffix)) {
    return std::nullopt;
  }
  return request_path.substr(
      kPrefix.size(),
      request_path.size() - kPrefix.size() - kSuffix.size());
}

std::map<std::string, std::string>
InteractionRequestContractSupport::BuildInteractionResponseHeaders(
    const std::string& request_id) const {
  return {
      {"X-Naim-Request-Id", request_id},
  };
}

std::string InteractionRequestContractSupport::ResolveInteractionServedModelName(
    const PlaneInteractionResolution& resolution) const {
  if (resolution.runtime_status.has_value() &&
      !resolution.runtime_status->active_served_model_name.empty()) {
    return resolution.runtime_status->active_served_model_name;
  }
  return ReadJsonStringOrEmpty(resolution.status_payload, "served_model_name");
}

std::string InteractionRequestContractSupport::ResolveInteractionActiveModelId(
    const PlaneInteractionResolution& resolution) const {
  if (resolution.runtime_status.has_value() &&
      !resolution.runtime_status->active_model_id.empty()) {
    return resolution.runtime_status->active_model_id;
  }
  return ReadJsonStringOrEmpty(resolution.status_payload, "active_model_id");
}

nlohmann::json InteractionRequestContractSupport::BuildInteractionContractMetadata(
    const PlaneInteractionResolution& resolution,
    const std::string& request_id,
    const std::optional<std::string>& session_id,
    const std::optional<int>& segment_count,
    const std::optional<int>& continuation_count) const {
  nlohmann::json metadata{
      {"request_id", request_id},
      {"plane_name", resolution.status_payload.value("plane_name", std::string{})},
      {"served_model_name", ResolveInteractionServedModelName(resolution)},
      {"active_model_id", ResolveInteractionActiveModelId(resolution)},
      {"reason", resolution.status_payload.value("reason", std::string{})},
  };
  if (session_id.has_value()) {
    metadata["session_id"] = *session_id;
  }
  if (segment_count.has_value()) {
    metadata["segment_count"] = *segment_count;
  }
  if (continuation_count.has_value()) {
    metadata["continuation_count"] = *continuation_count;
  }
  if (resolution.desired_state.interaction.has_value()) {
    if (resolution.desired_state.interaction->completion_policy.has_value()) {
      metadata["completion_policy"] = nlohmann::json{
          {"response_mode",
           resolution.desired_state.interaction->completion_policy->response_mode},
          {"max_tokens",
           resolution.desired_state.interaction->completion_policy->max_tokens},
          {"thinking_enabled",
           resolution.desired_state.interaction->thinking_enabled},
      };
    }
    if (resolution.desired_state.interaction->long_completion_policy.has_value()) {
      metadata["long_completion_policy"] = nlohmann::json{
          {"response_mode",
           resolution.desired_state.interaction->long_completion_policy->response_mode},
          {"max_tokens",
           resolution.desired_state.interaction->long_completion_policy->max_tokens},
      };
    }
    if (resolution.desired_state.interaction->analysis_completion_policy.has_value()) {
      metadata["analysis_completion_policy"] = nlohmann::json{
          {"response_mode",
           resolution.desired_state.interaction->analysis_completion_policy->response_mode},
          {"max_tokens",
           resolution.desired_state.interaction->analysis_completion_policy->max_tokens},
      };
    }
    if (resolution.desired_state.interaction->analysis_long_completion_policy
            .has_value()) {
      metadata["analysis_long_completion_policy"] = nlohmann::json{
          {"response_mode",
           resolution.desired_state.interaction->analysis_long_completion_policy
               ->response_mode},
          {"max_tokens",
           resolution.desired_state.interaction->analysis_long_completion_policy
               ->max_tokens},
      };
    }
  }
  return metadata;
}

std::string InteractionRequestContractSupport::ReadJsonStringOrEmpty(
    const nlohmann::json& payload,
    std::string_view key) const {
  const auto found = payload.find(std::string(key));
  if (found == payload.end() || found->is_null() || !found->is_string()) {
    return {};
  }
  return found->get<std::string>();
}

std::string InteractionRequestContractSupport::TrimCopy(
    const std::string& value) const {
  std::size_t start = 0;
  while (start < value.size() &&
         std::isspace(static_cast<unsigned char>(value[start])) != 0) {
    ++start;
  }
  std::size_t end = value.size();
  while (end > start &&
         std::isspace(static_cast<unsigned char>(value[end - 1])) != 0) {
    --end;
  }
  return value.substr(start, end - start);
}

nlohmann::json InteractionRequestContractSupport::DefaultBrowsingSummary() const {
  return nlohmann::json{
      {"mode", "disabled"},
      {"mode_source", "default_off"},
      {"plane_enabled", false},
      {"ready", false},
      {"session_backend", "broker_fallback"},
      {"rendered_browser_enabled", true},
      {"rendered_browser_ready", false},
      {"login_enabled", false},
      {"toggle_only", false},
      {"decision", "disabled"},
      {"reason", "web_mode_disabled"},
      {"lookup_state", "disabled"},
      {"lookup_attempted", false},
      {"lookup_required", false},
      {"evidence_attached", false},
      {"searches", nlohmann::json::array()},
      {"sources", nlohmann::json::array()},
      {"errors", nlohmann::json::array()},
      {"indicator",
       nlohmann::json{
           {"compact", "web:off"},
           {"label", "Web disabled"},
           {"active", false},
           {"ready", false},
           {"lookup_state", "disabled"},
           {"lookup_attempted", false},
           {"session_backend", "broker_fallback"},
           {"rendered_browser_ready", false},
           {"search_count", 0},
           {"source_count", 0},
           {"error_count", 0},
       }},
      {"trace",
       nlohmann::json::array(
           {nlohmann::json{{"stage", "mode"}, {"status", "off"}, {"compact", "web:off"}},
            nlohmann::json{{"stage", "decision"},
                           {"status", "disabled"},
                           {"compact", "decide:disabled"}}})},
  };
}

}  // namespace naim::controller
