#include "interaction/interaction_payload_builder.h"

#include <map>
#include <string>
#include <vector>

#include "app/controller_composition_support.h"
#include "app/controller_language_support.h"
#include "browsing/interaction_browsing_service.h"
#include "naim/runtime/model_adapter.h"
#include "interaction/interaction_completion_policy_support.h"
#include "interaction/interaction_model_identity_builder.h"
#include "interaction/interaction_utf8_payload_sanitizer.h"
#include "skills/plane_skills_service.h"

namespace naim::controller {

namespace {

constexpr const char* kKnowledgeSystemInstructionPayloadKey =
    "__naim_knowledge_system_instruction";
constexpr const char* kKnowledgeContextPayloadKey = "__naim_knowledge_context";
constexpr const char* kKnowledgeWarningPayloadKey = "__naim_knowledge_warning";

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
  const InteractionCompletionPolicySupport completion_policy_support;

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
  if (payload.contains(kKnowledgeSystemInstructionPayloadKey) &&
      payload.at(kKnowledgeSystemInstructionPayloadKey).is_string()) {
    const std::string knowledge_instruction =
        payload.at(kKnowledgeSystemInstructionPayloadKey).get<std::string>();
    if (!knowledge_instruction.empty()) {
      system_instruction_parts.push_back(knowledge_instruction);
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
    system_instruction_parts.push_back(
        completion_policy_support.BuildRepositoryAnalysisInstruction());
  }
  system_instruction_parts.push_back(
      ControllerLanguageSupport::BuildLanguageInstruction(
          resolution.desired_state,
          preferred_language));
  if (policy.require_completion_marker || policy.max_continuations > 0) {
    system_instruction_parts.push_back(
        completion_policy_support.BuildSemanticCompletionInstruction(policy));
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
  payload.erase("__naim_policy_user_message");
  payload.erase(PlaneSkillsService::kSystemInstructionPayloadKey);
  payload.erase(PlaneSkillsService::kAppliedSkillsPayloadKey);
  payload.erase(PlaneSkillsService::kSkillsSessionIdPayloadKey);
  payload.erase(InteractionBrowsingService::kSystemInstructionPayloadKey);
  payload.erase(InteractionBrowsingService::kSummaryPayloadKey);
  payload.erase(InteractionBrowsingService::kWebGatewayContextPayloadKey);
  payload.erase(InteractionBrowsingService::kWebGatewayPolicyPayloadKey);
  payload.erase(InteractionBrowsingService::kWebGatewayReviewPayloadKey);
  payload.erase(kKnowledgeSystemInstructionPayloadKey);
  payload.erase(kKnowledgeContextPayloadKey);
  payload.erase(kKnowledgeWarningPayloadKey);
  payload["chat_template_kwargs"]["enable_thinking"] = thinking_enabled;
  const InteractionModelIdentityBuilder model_identity_builder;
  naim::runtime::ModelAdapter::AdaptInteractionPayload(
      &payload,
      model_identity_builder.BuildStatusPreferred(resolution),
      naim::runtime::ModelAdapterPolicy{thinking_enabled});
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
  return InteractionUtf8PayloadSanitizer{}.SanitizeJson(payload).dump();
}

}  // namespace naim::controller
