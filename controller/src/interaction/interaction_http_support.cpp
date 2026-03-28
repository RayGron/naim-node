#include "interaction/interaction_http_support.h"

#include "app/controller_composition_support.h"

using nlohmann::json;

namespace {

std::string Lowercase(std::string value) {
  std::transform(
      value.begin(),
      value.end(),
      value.begin(),
      [](unsigned char ch) { return static_cast<char>(std::tolower(ch)); });
  return value;
}

}  // namespace

InteractionHttpSupport::InteractionHttpSupport(
    const comet::controller::ControllerRuntimeSupportService& runtime_support_service,
    const comet::controller::DesiredStatePolicyService& desired_state_policy_service,
    const comet::controller::InteractionRuntimeSupportService& interaction_runtime_support_service)
    : runtime_support_service_(runtime_support_service),
      desired_state_policy_service_(desired_state_policy_service),
      interaction_runtime_support_service_(interaction_runtime_support_service) {}

HttpResponse InteractionHttpSupport::BuildJsonResponse(
    int status_code,
    const json& payload,
    const std::map<std::string, std::string>& headers) const {
  return comet::controller::composition_support::BuildJsonResponse(
      status_code,
      payload,
      headers);
}

std::string InteractionHttpSupport::BuildInteractionUpstreamBody(
    const comet::controller::PlaneInteractionResolution& resolution,
    json payload,
    bool force_stream,
    const comet::controller::ResolvedInteractionPolicy& resolved_policy,
    bool structured_output_json) const {
  const auto& policy = resolved_policy.policy;
  if (!payload.contains("messages") || !payload.at("messages").is_array()) {
    payload["messages"] = json::array();
  }
  const auto preferred_language =
      comet::controller::ControllerLanguageSupport::ResolveInteractionPreferredLanguage(
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
    system_instruction_parts.push_back(comet::controller::BuildRepositoryAnalysisInstruction());
  }
  system_instruction_parts.push_back(
      comet::controller::ControllerLanguageSupport::BuildLanguageInstruction(
          resolution.desired_state,
          preferred_language));
  if (policy.require_completion_marker || policy.max_continuations > 0) {
    system_instruction_parts.push_back(
        comet::controller::BuildSemanticCompletionInstruction(policy));
  }
  if (structured_output_json) {
    system_instruction_parts.push_back(
        "Structured output requirement: return one valid JSON object only. "
        "Do not wrap it in markdown fences. "
        "Do not add commentary before or after the JSON object.");
  }

  json merged_messages = json::array();
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
    merged_messages.push_back(json{{"role", "system"}, {"content", combined_system_instruction}});
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
      payload["chat_template_kwargs"] = json::object();
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

std::optional<std::string> InteractionHttpSupport::FindInferInstanceName(
    const comet::DesiredState& desired_state) const {
  return interaction_runtime_support_service_.FindInferInstanceName(desired_state);
}

std::vector<comet::RuntimeProcessStatus> InteractionHttpSupport::ParseInstanceRuntimeStatuses(
    const comet::HostObservation& observation) const {
  return runtime_support_service_.ParseInstanceRuntimeStatuses(observation);
}

bool InteractionHttpSupport::ObservationMatchesPlane(
    const comet::HostObservation& observation,
    const std::string& plane_name) const {
  return comet::controller::composition_support::ObservationMatchesPlane(
      observation,
      plane_name);
}

std::optional<comet::RuntimeStatus> InteractionHttpSupport::BuildPlaneScopedRuntimeStatus(
    const comet::DesiredState& desired_state,
    const comet::HostObservation& observation) const {
  return interaction_runtime_support_service_.BuildPlaneScopedRuntimeStatus(
      desired_state,
      observation,
      [&](const comet::HostObservation& current_observation) {
        return runtime_support_service_.ParseInstanceRuntimeStatuses(current_observation);
      });
}

std::optional<comet::controller::ControllerEndpointTarget>
InteractionHttpSupport::ParseInteractionTarget(
    const std::string& gateway_listen,
    int fallback_port) const {
  return interaction_runtime_support_service_.ParseInteractionTarget(
      gateway_listen,
      fallback_port);
}

int InteractionHttpSupport::CountReadyWorkerMembers(
    comet::ControllerStore& store,
    const comet::DesiredState& desired_state) const {
  return interaction_runtime_support_service_.CountReadyWorkerMembers(
      store,
      desired_state,
      [&](const comet::HostObservation& observation) {
        return runtime_support_service_.ParseInstanceRuntimeStatuses(observation);
      });
}

bool InteractionHttpSupport::ProbeControllerTargetOk(
    const std::optional<comet::controller::ControllerEndpointTarget>& target,
    const std::string& path) const {
  return interaction_runtime_support_service_.ProbeControllerTargetOk(target, path);
}

std::optional<std::string> InteractionHttpSupport::DescribeUnsupportedControllerLocalRuntime(
    const comet::DesiredState& desired_state,
    const std::string& node_name) const {
  return desired_state_policy_service_.DescribeUnsupportedControllerLocalRuntime(
      desired_state,
      node_name);
}

HttpResponse InteractionHttpSupport::SendControllerHttpRequest(
    const comet::controller::ControllerEndpointTarget& target,
    const std::string& method,
    const std::string& path,
    const std::string& body,
    const std::vector<std::pair<std::string, std::string>>& headers) const {
  return ::SendControllerHttpRequest(target, method, path, body, headers);
}

void InteractionHttpSupport::SendHttpResponse(
    comet::platform::SocketHandle client_fd,
    const HttpResponse& response) const {
  comet::controller::ControllerNetworkManager::SendHttpResponse(client_fd, response);
}

void InteractionHttpSupport::ShutdownAndCloseSocket(
    comet::platform::SocketHandle client_fd) const {
  comet::controller::ControllerNetworkManager::ShutdownAndCloseSocket(client_fd);
}

bool InteractionHttpSupport::SendSseHeaders(
    comet::platform::SocketHandle client_fd,
    const std::map<std::string, std::string>& headers) const {
  return comet::controller::ControllerNetworkManager::SendSseHeaders(client_fd, headers);
}

bool InteractionHttpSupport::SendAll(
    comet::platform::SocketHandle fd,
    const std::string& payload) const {
  return comet::controller::ControllerNetworkManager::SendAll(fd, payload);
}
