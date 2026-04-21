#include "interaction/interaction_http_support.h"

#include "app/controller_composition_support.h"
#include "interaction/interaction_payload_builder.h"
#include "naim/security/crypto_utils.h"
#include "skills/plane_skills_service.h"

#include <chrono>
#include <thread>

using nlohmann::json;

namespace {

std::optional<std::string> FindHeaderValue(
    const std::vector<std::pair<std::string, std::string>>& headers,
    const std::string& name) {
  for (const auto& [key, value] : headers) {
    if (key == name) {
      return value;
    }
  }
  return std::nullopt;
}

bool IsAllowedRuntimeRelayPath(const std::string& method, const std::string& path) {
  if (method == "GET" && path == "/health") {
    return true;
  }
  if (method == "GET" && path.rfind("/v1/models", 0) == 0) {
    return true;
  }
  if (method == "POST" && path.rfind("/v1/chat/completions", 0) == 0) {
    return true;
  }
  return false;
}

bool IsLoopbackRelayTarget(const naim::controller::ControllerEndpointTarget& target) {
  return target.host == "127.0.0.1" || target.host == "localhost" ||
         target.host == "::1";
}

HttpResponse SendHostdRuntimeRelayRequest(
    const naim::controller::ControllerEndpointTarget& target,
    const std::string& method,
    const std::string& path,
    const std::string& body,
    const std::vector<std::pair<std::string, std::string>>& headers) {
  if (target.relay_db_path.empty() || target.relay_node_name.empty()) {
    throw std::runtime_error("hostd runtime relay target is missing db or node metadata");
  }
  if (!IsLoopbackRelayTarget(target)) {
    throw std::runtime_error("hostd runtime relay only accepts loopback runtime targets");
  }
  if (!IsAllowedRuntimeRelayPath(method, path)) {
    throw std::runtime_error("hostd runtime relay rejected unsupported runtime path: " + path);
  }

  const std::string request_id =
      FindHeaderValue(headers, "X-Naim-Request-Id").value_or(naim::RandomTokenBase64(18));
  const std::string relay_id = naim::RandomTokenBase64(18);
  const std::string relay_plane =
      target.relay_plane_name.empty() ? std::string("runtime") : target.relay_plane_name;
  const std::string assignment_plane = "runtime-proxy:" + relay_id;

  naim::ControllerStore store(target.relay_db_path);
  store.Initialize();
  naim::HostAssignment assignment;
  assignment.node_name = target.relay_node_name;
  assignment.plane_name = assignment_plane;
  assignment.desired_generation = 0;
  assignment.max_attempts = 1;
  assignment.assignment_type = "runtime-http-proxy";
  assignment.desired_state_json =
      json{{"request_id", request_id},
           {"relay_id", relay_id},
           {"plane_name", relay_plane},
           {"target_host", target.host},
           {"target_port", target.port},
           {"method", method},
           {"path", path},
           {"body", body},
           {"headers", headers}}
          .dump();
  assignment.status_message = "queued runtime HTTP proxy request";
  assignment.progress_json =
      json{{"phase", "queued"},
           {"title", "Runtime proxy queued"},
           {"detail", "Waiting for hostd to execute the runtime HTTP request."},
           {"percent", 0},
           {"request_id", request_id}}
          .dump();
  store.EnqueueHostAssignments({assignment}, "superseded by newer runtime proxy request");

  int assignment_id = 0;
  for (const auto& candidate : store.LoadHostAssignments(
           std::make_optional<std::string>(target.relay_node_name),
           std::make_optional<naim::HostAssignmentStatus>(naim::HostAssignmentStatus::Pending),
           std::make_optional<std::string>(assignment_plane))) {
    const json desired = json::parse(candidate.desired_state_json, nullptr, false);
    if (desired.is_object() && desired.value("relay_id", std::string{}) == relay_id) {
      assignment_id = candidate.id;
      break;
    }
  }
  if (assignment_id <= 0) {
    throw std::runtime_error("failed to queue hostd runtime relay request");
  }

  const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(240);
  while (std::chrono::steady_clock::now() < deadline) {
    const auto current = store.LoadHostAssignment(assignment_id);
    if (!current.has_value()) {
      throw std::runtime_error("hostd runtime relay assignment disappeared");
    }
    if (current->status == naim::HostAssignmentStatus::Applied) {
      const json progress = json::parse(current->progress_json, nullptr, false);
      if (!progress.is_object() || progress.value("phase", std::string{}) != "response-ready") {
        throw std::runtime_error("hostd runtime relay completed without a response payload");
      }
      HttpResponse response;
      response.status_code = progress.value("status_code", 502);
      response.content_type = progress.value("content_type", std::string("application/json"));
      response.body = progress.value("body", std::string{});
      if (progress.contains("headers") && progress["headers"].is_object()) {
        for (const auto& [key, value] : progress["headers"].items()) {
          if (value.is_string()) {
            response.headers[key] = value.get<std::string>();
          }
        }
      }
      if (!response.content_type.empty()) {
        response.headers["content-type"] = response.content_type;
      }
      return response;
    }
    if (current->status == naim::HostAssignmentStatus::Failed ||
        current->status == naim::HostAssignmentStatus::Superseded) {
      throw std::runtime_error(
          "hostd runtime relay failed: " + current->status_message);
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }

  store.UpdateHostAssignmentStatus(
      assignment_id,
      naim::HostAssignmentStatus::Failed,
      "timed out waiting for hostd runtime relay response");
  throw std::runtime_error("timed out waiting for hostd runtime relay response");
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
  if (target.has_value() && target->use_hostd_runtime_relay) {
    try {
      const HttpResponse response =
          SendHostdRuntimeRelayRequest(*target, "GET", path, "", {});
      return response.status_code >= 200 && response.status_code < 300;
    } catch (const std::exception&) {
      return false;
    }
  }
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
  if (target.use_hostd_runtime_relay) {
    return SendHostdRuntimeRelayRequest(target, method, path, body, headers);
  }
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
