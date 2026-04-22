#include "interaction/interaction_hostd_runtime_relay_service.h"

#include <chrono>
#include <stdexcept>
#include <thread>

#include "naim/security/crypto_utils.h"
#include "naim/state/sqlite_store.h"

namespace naim::controller {

HttpResponse InteractionHostdRuntimeRelayService::Send(
    const ControllerEndpointTarget& target,
    const std::string& method,
    const std::string& path,
    const std::string& body,
    const std::vector<std::pair<std::string, std::string>>& headers) const {
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
      nlohmann::json{{"request_id", request_id},
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
      nlohmann::json{{"phase", "queued"},
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
    const nlohmann::json desired =
        nlohmann::json::parse(candidate.desired_state_json, nullptr, false);
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
      const nlohmann::json progress =
          nlohmann::json::parse(current->progress_json, nullptr, false);
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

std::optional<std::string> InteractionHostdRuntimeRelayService::FindHeaderValue(
    const std::vector<std::pair<std::string, std::string>>& headers,
    const std::string& name) {
  for (const auto& [key, value] : headers) {
    if (key == name) {
      return value;
    }
  }
  return std::nullopt;
}

bool InteractionHostdRuntimeRelayService::IsAllowedRuntimeRelayPath(
    const std::string& method,
    const std::string& path) {
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

bool InteractionHostdRuntimeRelayService::IsLoopbackRelayTarget(
    const ControllerEndpointTarget& target) {
  return target.host == "127.0.0.1" || target.host == "localhost" ||
         target.host == "::1";
}

}  // namespace naim::controller
