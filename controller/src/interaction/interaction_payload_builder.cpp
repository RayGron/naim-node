#include "interaction/interaction_payload_builder.h"

#include <map>
#include <string>
#include <string_view>
#include <vector>

#include "app/controller_composition_support.h"
#include "app/controller_language_support.h"
#include "browsing/interaction_browsing_service.h"
#include "comet/runtime/model_adapter.h"
#include "skills/plane_skills_service.h"

namespace comet::controller {

namespace {

std::string ReadJsonStringOrEmpty(
    const nlohmann::json& payload,
    std::string_view key) {
  const auto found = payload.find(std::string(key));
  if (found == payload.end() || found->is_null() || !found->is_string()) {
    return {};
  }
  return found->get<std::string>();
}

bool IsUtf8ContinuationByte(unsigned char value) {
  return (value & 0xC0) == 0x80;
}

std::size_t Utf8SequenceLength(unsigned char lead) {
  if ((lead & 0x80) == 0) {
    return 1;
  }
  if ((lead & 0xE0) == 0xC0) {
    return 2;
  }
  if ((lead & 0xF0) == 0xE0) {
    return 3;
  }
  if ((lead & 0xF8) == 0xF0) {
    return 4;
  }
  return 0;
}

std::string SanitizeUtf8String(const std::string& value) {
  std::string sanitized;
  sanitized.reserve(value.size());
  std::size_t index = 0;
  while (index < value.size()) {
    const unsigned char lead = static_cast<unsigned char>(value[index]);
    const std::size_t sequence_length = Utf8SequenceLength(lead);
    if (sequence_length == 0 || index + sequence_length > value.size()) {
      sanitized.push_back('?');
      ++index;
      continue;
    }
    bool valid = true;
    for (std::size_t offset = 1; offset < sequence_length; ++offset) {
      if (!IsUtf8ContinuationByte(
              static_cast<unsigned char>(value[index + offset]))) {
        valid = false;
        break;
      }
    }
    if (!valid) {
      sanitized.push_back('?');
      ++index;
      continue;
    }
    sanitized.append(value, index, sequence_length);
    index += sequence_length;
  }
  return sanitized;
}

nlohmann::json SanitizeJsonUtf8(const nlohmann::json& value) {
  if (value.is_string()) {
    return SanitizeUtf8String(value.get<std::string>());
  }
  if (value.is_array()) {
    nlohmann::json sanitized = nlohmann::json::array();
    for (const auto& item : value) {
      sanitized.push_back(SanitizeJsonUtf8(item));
    }
    return sanitized;
  }
  if (value.is_object()) {
    nlohmann::json sanitized = nlohmann::json::object();
    for (const auto& [key, item] : value.items()) {
      sanitized[SanitizeUtf8String(key)] = SanitizeJsonUtf8(item);
    }
    return sanitized;
  }
  return value;
}

comet::runtime::ModelIdentity BuildModelIdentity(
    const PlaneInteractionResolution& resolution) {
  comet::runtime::ModelIdentity identity;
  identity.model_id = ReadJsonStringOrEmpty(resolution.status_payload, "active_model_id");
  identity.served_model_name =
      ReadJsonStringOrEmpty(resolution.status_payload, "served_model_name");
  if (resolution.runtime_status.has_value()) {
    if (identity.model_id.empty()) {
      identity.model_id = resolution.runtime_status->active_model_id;
    }
    if (identity.served_model_name.empty()) {
      identity.served_model_name = resolution.runtime_status->active_served_model_name;
    }
    identity.cached_local_model_path = resolution.runtime_status->cached_local_model_path;
    identity.cached_runtime_model_path = resolution.runtime_status->model_path;
    identity.runtime_profile = resolution.runtime_status->active_runtime_profile;
  }
  return identity;
}

}  // namespace

std::string BuildInteractionUpstreamBodyPayload(
    const PlaneInteractionResolution& resolution,
    nlohmann::json payload,
    bool force_stream,
    const ResolvedInteractionPolicy& resolved_policy,
    bool structured_output_json) {
  const auto& policy = resolved_policy.policy;
  if (!payload.contains("messages") || !payload.at("messages").is_array()) {
    payload["messages"] = nlohmann::json::array();
  }
  const auto preferred_language =
      ControllerLanguageSupport::ResolveInteractionPreferredLanguage(
          resolution.desired_state,
          payload);

  std::vector<std::string> system_instruction_parts;
  bool thinking_enabled =
      resolution.desired_state.interaction.has_value() &&
      resolution.desired_state.interaction->thinking_enabled;
  if (!payload.contains("chat_template_kwargs") ||
      !payload.at("chat_template_kwargs").is_object()) {
    payload["chat_template_kwargs"] = nlohmann::json::object();
  }
  if (payload.at("chat_template_kwargs").contains("enable_thinking") &&
      payload.at("chat_template_kwargs").at("enable_thinking").is_boolean()) {
    thinking_enabled =
        payload.at("chat_template_kwargs").at("enable_thinking").get<bool>();
  }
  if (resolution.desired_state.interaction.has_value() &&
      resolution.desired_state.interaction->system_prompt.has_value() &&
      !resolution.desired_state.interaction->system_prompt->empty()) {
    system_instruction_parts.push_back(*resolution.desired_state.interaction->system_prompt);
  }
  if (payload.contains(PlaneSkillsService::kSystemInstructionPayloadKey) &&
      payload.at(PlaneSkillsService::kSystemInstructionPayloadKey).is_string()) {
    const std::string skills_instruction =
        payload.at(PlaneSkillsService::kSystemInstructionPayloadKey).get<std::string>();
    if (!skills_instruction.empty()) {
      system_instruction_parts.push_back(skills_instruction);
    }
  }
  if (payload.contains(InteractionBrowsingService::kSystemInstructionPayloadKey) &&
      payload.at(InteractionBrowsingService::kSystemInstructionPayloadKey).is_string()) {
    const std::string browsing_instruction =
        payload.at(InteractionBrowsingService::kSystemInstructionPayloadKey)
            .get<std::string>();
    if (!browsing_instruction.empty()) {
      system_instruction_parts.push_back(browsing_instruction);
    }
  }
  if (resolved_policy.repository_analysis &&
      resolution.desired_state.interaction.has_value() &&
      resolution.desired_state.interaction->analysis_system_prompt.has_value() &&
      !resolution.desired_state.interaction->analysis_system_prompt->empty()) {
    system_instruction_parts.push_back(
        *resolution.desired_state.interaction->analysis_system_prompt);
  }
  if (resolved_policy.repository_analysis) {
    system_instruction_parts.push_back(BuildRepositoryAnalysisInstruction());
  }
  system_instruction_parts.push_back(
      ControllerLanguageSupport::BuildLanguageInstruction(
          resolution.desired_state,
          preferred_language));
  if (policy.require_completion_marker || policy.max_continuations > 0) {
    system_instruction_parts.push_back(BuildSemanticCompletionInstruction(policy));
  }
  if (structured_output_json) {
    system_instruction_parts.push_back(
        "Structured output requirement: return one valid JSON object only. "
        "Do not wrap it in markdown fences. "
        "Do not add commentary before or after the JSON object.");
  }

  nlohmann::json merged_messages = nlohmann::json::array();
  std::string combined_system_instruction;
  for (const auto& part : system_instruction_parts) {
    if (part.empty()) {
      continue;
    }
    if (!combined_system_instruction.empty()) {
      combined_system_instruction += "\n\n";
    }
    combined_system_instruction += part;
  }

  for (const auto& message : payload.at("messages")) {
    if (!message.is_object() || message.value("role", std::string{}) != "system") {
      continue;
    }
    std::string system_content;
    if (message.contains("content")) {
      if (message.at("content").is_string()) {
        system_content = message.at("content").get<std::string>();
      } else {
        system_content = message.at("content").dump();
      }
    }
    if (system_content.empty()) {
      continue;
    }
    if (!combined_system_instruction.empty()) {
      combined_system_instruction += "\n\n";
    }
    combined_system_instruction += system_content;
  }

  if (thinking_enabled) {
    if (!combined_system_instruction.empty()) {
      combined_system_instruction += "\n\n";
    }
    combined_system_instruction +=
        "Thinking mode is enabled. Keep all reasoning hidden. "
        "Your visible assistant content must contain only the final answer for the user. "
        "Do not output <think> blocks, reasoning preambles, or an empty final answer.";
  }

  if (!combined_system_instruction.empty()) {
    merged_messages.push_back(
        nlohmann::json{{"role", "system"}, {"content", combined_system_instruction}});
  }
  for (const auto& message : payload.at("messages")) {
    if (message.is_object() && message.value("role", std::string{}) == "system") {
      continue;
    }
    merged_messages.push_back(message);
  }
  payload["messages"] = merged_messages;

  if (preferred_language.has_value()) {
    payload["preferred_language"] = *preferred_language;
  }
  payload.erase("max_completion_tokens");
  payload.erase("target_completion_tokens");
  payload.erase("max_continuations");
  payload.erase("max_total_completion_tokens");
  payload.erase("max_elapsed_time_ms");
  payload.erase("semantic_goal");
  payload.erase("response_format");
  payload.erase("session_id");
  payload.erase("skill_ids");
  payload.erase(PlaneSkillsService::kSystemInstructionPayloadKey);
  payload.erase(PlaneSkillsService::kAppliedSkillsPayloadKey);
  payload.erase(PlaneSkillsService::kSkillsSessionIdPayloadKey);
  payload.erase(InteractionBrowsingService::kSystemInstructionPayloadKey);
  payload.erase(InteractionBrowsingService::kSummaryPayloadKey);
  payload.erase(InteractionBrowsingService::kWebGatewayContextPayloadKey);
  payload.erase(InteractionBrowsingService::kWebGatewayPolicyPayloadKey);
  payload.erase(InteractionBrowsingService::kWebGatewayReviewPayloadKey);
  payload["chat_template_kwargs"]["enable_thinking"] = thinking_enabled;
  comet::runtime::ModelAdapter::AdaptInteractionPayload(
      &payload,
      BuildModelIdentity(resolution),
      comet::runtime::ModelAdapterPolicy{thinking_enabled});
  if (force_stream) {
    payload["stream"] = true;
  }
  payload["max_tokens"] = policy.max_tokens;
  if (!payload.contains("temperature")) {
    payload["temperature"] =
        resolution.desired_state.interaction.has_value() &&
                resolution.desired_state.interaction->default_temperature.has_value()
            ? *resolution.desired_state.interaction->default_temperature
            : 0.2;
  }
  if (!payload.contains("top_p")) {
    payload["top_p"] =
        resolution.desired_state.interaction.has_value() &&
                resolution.desired_state.interaction->default_top_p.has_value()
            ? *resolution.desired_state.interaction->default_top_p
            : 0.8;
  }
  payload["response_mode"] = policy.response_mode;
  return SanitizeJsonUtf8(payload).dump();
}

}  // namespace comet::controller
