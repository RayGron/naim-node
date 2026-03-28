#include "app/controller_http_service_support.h"

#include "app/controller_composition_support.h"
#include "auth/auth_http_support.h"
#include "interaction/interaction_http_support.h"

namespace comet::controller::http_service_support {

namespace {

using SocketHandle = comet::platform::SocketHandle;

std::string Lowercase(std::string value) {
  std::transform(
      value.begin(),
      value.end(),
      value.begin(),
      [](unsigned char ch) { return static_cast<char>(std::tolower(ch)); });
  return value;
}

std::string Trim(const std::string& value) {
  return composition_support::Trim(value);
}

}  // namespace

std::string BuildInteractionUpstreamBody(
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
  if (resolution.desired_state.interaction.has_value() &&
      resolution.desired_state.interaction->system_prompt.has_value() &&
      !resolution.desired_state.interaction->system_prompt->empty()) {
    system_instruction_parts.push_back(*resolution.desired_state.interaction->system_prompt);
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
    system_instruction_parts.push_back(
        BuildSemanticCompletionInstruction(policy));
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
  const bool uses_vllm_runtime =
      resolution.runtime_status.has_value() &&
      Lowercase(resolution.runtime_status->runtime_backend).find("vllm") != std::string::npos;
  if (uses_vllm_runtime) {
    if (!payload.contains("chat_template_kwargs") ||
        !payload.at("chat_template_kwargs").is_object()) {
      payload["chat_template_kwargs"] = nlohmann::json::object();
    }
    payload["chat_template_kwargs"]["enable_thinking"] = false;
  }
  if (force_stream) {
    payload["stream"] = true;
  }
  payload["max_tokens"] = policy.max_tokens;
  if (!payload.contains("temperature")) {
    payload["temperature"] = 0.2;
  }
  if (!payload.contains("top_p")) {
    payload["top_p"] = 0.8;
  }
  payload["response_mode"] = policy.response_mode;
  return payload.dump();
}

InteractionHttpService CreateInteractionHttpService(
    const ControllerRuntimeSupportService& runtime_support_service,
    const DesiredStatePolicyService& desired_state_policy_service,
    const InteractionRuntimeSupportService& interaction_runtime_support_service) {
  return InteractionHttpService(InteractionHttpSupport(
      runtime_support_service,
      desired_state_policy_service,
      interaction_runtime_support_service));
}

HostdHttpService CreateHostdHttpService() {
  return HostdHttpService(HostdHttpSupport(
      [](comet::ControllerStore& store,
         const std::string& event_type,
         const std::string& message,
         const nlohmann::json& payload,
         const std::string& node_name,
         const std::string& severity) {
        composition_support::AppendControllerEvent(
            store,
            "host-registry",
            event_type,
            message,
            payload,
            "",
            node_name,
            "",
            std::nullopt,
            std::nullopt,
            severity);
      }));
}

AuthHttpService CreateAuthHttpService(AuthSupportService& auth_support) {
  return AuthHttpService(AuthHttpSupport(auth_support));
}

ModelLibraryService CreateModelLibraryService(
    const ControllerRequestSupport& request_support) {
  return ModelLibraryService(ModelLibrarySupport(request_support));
}

ModelLibraryHttpService CreateModelLibraryHttpService(
    const ModelLibraryService& model_library_service) {
  return ModelLibraryHttpService(ModelLibraryHttpSupport(model_library_service));
}

}  // namespace comet::controller::http_service_support
