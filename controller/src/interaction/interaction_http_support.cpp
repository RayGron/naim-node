#include "interaction/interaction_http_support.h"

#include "app/controller_composition_support.h"
#include "interaction/interaction_payload_builder.h"
#include "skills/plane_skills_service.h"

using nlohmann::json;

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
  return comet::controller::BuildInteractionUpstreamBodyPayload(
      resolution, std::move(payload), force_stream, resolved_policy, structured_output_json);
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
