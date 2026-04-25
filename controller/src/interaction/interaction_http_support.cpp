#include "interaction/interaction_http_support.h"

#include "app/controller_composition_support.h"
#include "interaction/interaction_payload_builder.h"
#include "interaction/interaction_runtime_request_codec.h"
#include "skills/plane_skills_service.h"

using nlohmann::json;

InteractionHttpSupport::InteractionHttpSupport(
    const naim::controller::ControllerRuntimeSupportService& runtime_support_service,
    const naim::controller::DesiredStatePolicyService& desired_state_policy_service,
    const naim::controller::InteractionRuntimeSupportService& interaction_runtime_support_service)
    : runtime_support_service_(runtime_support_service),
      desired_state_policy_service_(desired_state_policy_service),
      interaction_runtime_support_service_(interaction_runtime_support_service) {}

HttpResponse InteractionHttpSupport::BuildJsonResponse(
    int status_code,
    const json& payload,
    const std::map<std::string, std::string>& headers) const {
  return naim::controller::composition_support::BuildJsonResponse(
      status_code,
      payload,
      headers);
}

std::string InteractionHttpSupport::BuildInteractionUpstreamBody(
    const naim::controller::PlaneInteractionResolution& resolution,
    json payload,
    bool force_stream,
    const naim::controller::ResolvedInteractionPolicy& resolved_policy,
    bool structured_output_json) const {
  return naim::controller::BuildInteractionUpstreamBodyPayload(
      resolution, std::move(payload), force_stream, resolved_policy, structured_output_json);
}

std::string InteractionHttpSupport::BuildInteractionRuntimeRequestBody(
    const naim::controller::PlaneInteractionResolution& resolution,
    json payload,
    bool force_stream,
    const naim::controller::ResolvedInteractionPolicy& resolved_policy,
    bool structured_output_json) const {
  return naim::controller::InteractionRuntimeRequestCodec{}.Serialize(
      naim::controller::InteractionRuntimeExecutionRequest{
          resolution.desired_state,
          resolution.status_payload,
          std::move(payload),
          resolved_policy,
          structured_output_json,
          force_stream,
      });
}

std::optional<std::string> InteractionHttpSupport::FindInferInstanceName(
    const naim::DesiredState& desired_state) const {
  return interaction_runtime_support_service_.FindInferInstanceName(desired_state);
}

std::vector<naim::RuntimeProcessStatus> InteractionHttpSupport::ParseInstanceRuntimeStatuses(
    const naim::HostObservation& observation) const {
  return runtime_support_service_.ParseInstanceRuntimeStatuses(observation);
}

bool InteractionHttpSupport::ObservationMatchesPlane(
    const naim::HostObservation& observation,
    const std::string& plane_name) const {
  return plane_observation_matcher_.ObservationMatchesPlane(observation, plane_name);
}

std::optional<naim::RuntimeStatus> InteractionHttpSupport::BuildPlaneScopedRuntimeStatus(
    const naim::DesiredState& desired_state,
    const naim::HostObservation& observation) const {
  return interaction_runtime_support_service_.BuildPlaneScopedRuntimeStatus(
      desired_state,
      observation,
      [&](const naim::HostObservation& current_observation) {
        return runtime_support_service_.ParseInstanceRuntimeStatuses(current_observation);
      });
}

std::optional<naim::controller::ControllerEndpointTarget>
InteractionHttpSupport::ParseInteractionTarget(
    const std::string& gateway_listen,
    int fallback_port) const {
  return interaction_runtime_support_service_.ParseInteractionTarget(
      gateway_listen,
      fallback_port);
}

std::optional<naim::controller::ControllerEndpointTarget>
InteractionHttpSupport::ResolvePlaneLocalInteractionTarget(
    const naim::DesiredState& desired_state) const {
  return interaction_runtime_support_service_.ResolvePlaneLocalInteractionTarget(
      desired_state);
}

int InteractionHttpSupport::CountReadyWorkerMembers(
    naim::ControllerStore& store,
    const naim::DesiredState& desired_state) const {
  return interaction_runtime_support_service_.CountReadyWorkerMembers(
      store,
      desired_state,
      [&](const naim::HostObservation& observation) {
        return runtime_support_service_.ParseInstanceRuntimeStatuses(observation);
      });
}

bool InteractionHttpSupport::ProbeControllerTargetOk(
    const std::optional<naim::controller::ControllerEndpointTarget>& target,
    const std::string& path) const {
  return interaction_runtime_support_service_.ProbeControllerTargetOk(target, path);
}

std::optional<std::string> InteractionHttpSupport::DescribeUnsupportedControllerLocalRuntime(
    const naim::DesiredState& desired_state,
    const std::string& node_name) const {
  return desired_state_policy_service_.DescribeUnsupportedControllerLocalRuntime(
      desired_state,
      node_name);
}

HttpResponse InteractionHttpSupport::SendControllerHttpRequest(
    const naim::controller::ControllerEndpointTarget& target,
    const std::string& method,
    const std::string& path,
    const std::string& body,
    const std::vector<std::pair<std::string, std::string>>& headers) const {
  return ::SendControllerHttpRequest(target, method, path, body, headers);
}

void InteractionHttpSupport::SendHttpResponse(
    naim::platform::SocketHandle client_fd,
    const HttpResponse& response) const {
  naim::controller::ControllerNetworkManager::SendHttpResponse(client_fd, response);
}

void InteractionHttpSupport::ShutdownAndCloseSocket(
    naim::platform::SocketHandle client_fd) const {
  naim::controller::ControllerNetworkManager::ShutdownAndCloseSocket(client_fd);
}

bool InteractionHttpSupport::SendSseHeaders(
    naim::platform::SocketHandle client_fd,
    const std::map<std::string, std::string>& headers) const {
  return naim::controller::ControllerNetworkManager::SendSseHeaders(client_fd, headers);
}

bool InteractionHttpSupport::SendAll(
    naim::platform::SocketHandle fd,
    const std::string& payload) const {
  return naim::controller::ControllerNetworkManager::SendAll(fd, payload);
}
