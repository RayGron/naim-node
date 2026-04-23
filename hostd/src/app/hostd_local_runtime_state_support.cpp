#include "app/hostd_local_runtime_state_support.h"

#include <stdexcept>
#include <thread>

#include "naim/state/state_json.h"

namespace naim::hostd {

HostdLocalRuntimeStateSupport::HostdLocalRuntimeStateSupport(
    const HostdDesiredStatePathSupport& desired_state_path_support,
    const HostdLocalStateRepository& local_state_repository,
    const HostdRuntimeTelemetrySupport& runtime_telemetry_support)
    : desired_state_path_support_(desired_state_path_support),
      local_state_repository_(local_state_repository),
      runtime_telemetry_support_(runtime_telemetry_support) {}

std::optional<naim::RuntimeStatus> HostdLocalRuntimeStateSupport::LoadLocalRuntimeStatus(
    const std::string& state_root,
    const std::string& node_name,
    const std::optional<std::string>& plane_name) const {
  if (plane_name.has_value()) {
    const auto local_state =
        local_state_repository_.LoadLocalAppliedState(state_root, node_name, plane_name);
    if (!local_state.has_value()) {
      return std::nullopt;
    }
    const auto runtime_status_path =
        desired_state_path_support_.RuntimeStatusPathForNode(*local_state, node_name);
    if (!runtime_status_path.has_value()) {
      return std::nullopt;
    }
    return naim::LoadRuntimeStatusJson(*runtime_status_path);
  }

  for (const auto& local_state :
       local_state_repository_.LoadAllLocalAppliedStates(state_root, node_name)) {
    const auto runtime_status_path =
        desired_state_path_support_.RuntimeStatusPathForNode(local_state, node_name);
    if (!runtime_status_path.has_value()) {
      continue;
    }
    const auto runtime_status = naim::LoadRuntimeStatusJson(*runtime_status_path);
    if (runtime_status.has_value()) {
      return runtime_status;
    }
  }
  return std::nullopt;
}

void HostdLocalRuntimeStateSupport::WaitForLocalRuntimeStatus(
    const std::string& state_root,
    const std::string& node_name,
    const std::optional<std::string>& plane_name,
    std::chrono::seconds timeout) const {
  const auto deadline = std::chrono::steady_clock::now() + timeout;
  while (std::chrono::steady_clock::now() < deadline) {
    if (const auto runtime_status = LoadLocalRuntimeStatus(state_root, node_name, plane_name);
        runtime_status.has_value() && runtime_status->ready && runtime_status->launch_ready &&
        runtime_status->inference_ready &&
        (runtime_status->gateway_health_url.empty() || runtime_status->gateway_ready)) {
      return;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(250));
  }
  throw std::runtime_error(
      "timed out waiting for plane runtime readiness on node '" + node_name + "'");
}

std::size_t HostdLocalRuntimeStateSupport::ExpectedRuntimeStatusCountForNode(
    const naim::DesiredState& desired_node_state,
    const std::string& node_name) const {
  std::size_t count = 0;
  for (const auto& instance : desired_node_state.instances) {
    if (instance.node_name == node_name && InstanceProducesRuntimeStatus(instance)) {
      ++count;
    }
  }
  return count;
}

void HostdLocalRuntimeStateSupport::WaitForLocalInstanceRuntimeStatuses(
    const std::string& state_root,
    const std::string& node_name,
    const std::optional<std::string>& plane_name,
    std::size_t expected_count,
    std::chrono::seconds timeout) const {
  if (expected_count == 0) {
    return;
  }
  const auto deadline = std::chrono::steady_clock::now() + timeout;
  while (std::chrono::steady_clock::now() < deadline) {
    const auto statuses = runtime_telemetry_support_.LoadLocalInstanceRuntimeStatuses(
        state_root,
        node_name,
        plane_name);
    std::size_t ready_count = 0;
    for (const auto& status : statuses) {
      if (status.ready &&
          (status.runtime_phase == "running" || status.runtime_phase == "ready")) {
        ++ready_count;
      }
    }
    if (ready_count >= expected_count) {
      return;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(250));
  }
  throw std::runtime_error(
      "timed out waiting for instance runtime readiness on node '" + node_name + "'");
}

bool HostdLocalRuntimeStateSupport::InstanceProducesRuntimeStatus(
    const naim::InstanceSpec& instance) {
  return instance.role == naim::InstanceRole::Infer ||
         instance.role == naim::InstanceRole::Worker ||
         instance.role == naim::InstanceRole::Skills ||
         instance.role == naim::InstanceRole::Interaction;
}

}  // namespace naim::hostd
