#include "interaction/interaction_http_support.h"

#include <chrono>
#include <thread>

#include "app/controller_composition_support.h"
#include "interaction/interaction_payload_builder.h"
#include "interaction/interaction_runtime_request_codec.h"
#include "skills/plane_skills_service.h"

using nlohmann::json;

namespace {

bool IsLoopbackHost(const std::string& host) {
  return host == "127.0.0.1" || host == "localhost" || host == "::1";
}

json BuildHeaderArray(
    const std::vector<std::pair<std::string, std::string>>& headers) {
  json result = json::array();
  for (const auto& [key, value] : headers) {
    result.push_back(json::array({key, value}));
  }
  return result;
}

HttpResponse BuildProxyErrorResponse(
    int status_code,
    const std::string& code,
    const std::string& message) {
  return naim::controller::composition_support::BuildJsonResponse(
      status_code,
      json{{"status", "error"}, {"code", code}, {"message", message}},
      {});
}

HttpResponse ParseProxyResponsePayload(const json& progress) {
  if (!progress.is_object() || !progress.contains("response") ||
      !progress.at("response").is_object()) {
    return BuildProxyErrorResponse(
        502,
        "runtime_proxy_response_missing",
        "hostd runtime proxy did not return an upstream response");
  }
  const auto& response_payload = progress.at("response");
  HttpResponse response;
  response.status_code = response_payload.value("status_code", 502);
  response.content_type =
      response_payload.value("content_type", std::string("application/json"));
  response.body = response_payload.value("body", std::string{});
  if (response_payload.contains("headers") &&
      response_payload.at("headers").is_object()) {
    for (const auto& [key, value] : response_payload.at("headers").items()) {
      if (value.is_string()) {
        response.headers[key] = value.get<std::string>();
      }
    }
  }
  if (!response.content_type.empty()) {
    response.headers["Content-Type"] = response.content_type;
  }
  return response;
}

}  // namespace

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

HttpResponse InteractionHttpSupport::SendRuntimeHttpRequest(
    const naim::controller::PlaneInteractionResolution& resolution,
    const naim::controller::ControllerEndpointTarget& target,
    const std::string& method,
    const std::string& path,
    const std::string& body,
    const std::string& request_id,
    const std::vector<std::pair<std::string, std::string>>& headers) const {
  if (target.route_via_hostd_proxy) {
    return SendHostdRuntimeProxyRequest(
        resolution.db_path,
        target.node_name,
        resolution.desired_state.plane_name,
        target,
        method,
        path,
        body,
        request_id,
        headers,
        "runtime");
  }
  return SendControllerHttpRequest(target, method, path, body, headers);
}

HttpResponse InteractionHttpSupport::SendHostdRuntimeProxyRequest(
    const std::string& db_path,
    const std::string& node_name,
    const std::string& plane_name,
    const naim::controller::ControllerEndpointTarget& target,
    const std::string& method,
    const std::string& path,
    const std::string& body,
    const std::string& request_id,
    const std::vector<std::pair<std::string, std::string>>& headers,
    const std::string& policy) const {
  if (db_path.empty() || node_name.empty()) {
    return BuildProxyErrorResponse(
        502,
        "runtime_proxy_route_missing",
        "runtime target requires hostd proxy routing but has no node binding");
  }
  if (!IsLoopbackHost(target.host)) {
    return BuildProxyErrorResponse(
        502,
        "runtime_proxy_target_rejected",
        "hostd runtime proxy only accepts node-local loopback targets");
  }

  naim::ControllerStore store(db_path);
  store.Initialize();
  const std::string proxy_plane_name =
      "runtime-http-proxy:" + plane_name + ":" + request_id;
  naim::HostAssignment assignment;
  assignment.node_name = node_name;
  assignment.plane_name = proxy_plane_name;
  assignment.desired_generation = 0;
  assignment.max_attempts = 1;
  assignment.assignment_type = "runtime-http-proxy";
  assignment.desired_state_json =
      json{
          {"target_host", target.host},
          {"target_port", target.port},
          {"method", method},
          {"path", path},
          {"body", body},
          {"headers", BuildHeaderArray(headers)},
          {"request_id", request_id},
          {"policy", policy},
      }
          .dump();
  assignment.artifacts_root = "";
  assignment.status = naim::HostAssignmentStatus::Pending;
  assignment.status_message = "proxy runtime HTTP request";
  store.EnqueueHostAssignments({assignment}, "superseded runtime HTTP proxy request");

  const auto assignments = store.LoadHostAssignments(
      node_name,
      std::nullopt,
      proxy_plane_name);
  if (assignments.empty()) {
    return BuildProxyErrorResponse(
        500,
        "runtime_proxy_queue_failed",
        "failed to queue hostd runtime proxy assignment");
  }
  const int assignment_id = assignments.back().id;
  const auto started_at = std::chrono::steady_clock::now();
  while (std::chrono::duration_cast<std::chrono::milliseconds>(
             std::chrono::steady_clock::now() - started_at)
             .count() < 30000) {
    const auto current = store.LoadHostAssignment(assignment_id);
    if (!current.has_value()) {
      return BuildProxyErrorResponse(
          500,
          "runtime_proxy_assignment_missing",
          "hostd runtime proxy assignment disappeared");
    }
    if (current->status == naim::HostAssignmentStatus::Applied) {
      const auto progress = json::parse(current->progress_json, nullptr, false);
      return ParseProxyResponsePayload(progress);
    }
    if (current->status == naim::HostAssignmentStatus::Failed) {
      return BuildProxyErrorResponse(
          502,
          "runtime_proxy_failed",
          current->status_message.empty()
              ? "hostd runtime proxy request failed"
              : current->status_message);
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
  }
  return BuildProxyErrorResponse(
      504,
      "runtime_proxy_timeout",
      "timed out waiting for hostd runtime proxy response");
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
