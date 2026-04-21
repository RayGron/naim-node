#pragma once

#include <chrono>
#include <optional>
#include <string>

#include "app/hostd_desired_state_path_support.h"
#include "app/hostd_local_state_repository.h"
#include "app/hostd_runtime_telemetry_support.h"
#include "naim/runtime/runtime_status.h"
#include "naim/state/models.h"

namespace naim::hostd {

class HostdLocalRuntimeStateSupport final {
 public:
  HostdLocalRuntimeStateSupport(
      const HostdDesiredStatePathSupport& desired_state_path_support,
      const HostdLocalStateRepository& local_state_repository,
      const HostdRuntimeTelemetrySupport& runtime_telemetry_support);

  std::optional<naim::RuntimeStatus> LoadLocalRuntimeStatus(
      const std::string& state_root,
      const std::string& node_name,
      const std::optional<std::string>& plane_name = std::nullopt) const;

  void WaitForLocalRuntimeStatus(
      const std::string& state_root,
      const std::string& node_name,
      const std::optional<std::string>& plane_name,
      std::chrono::seconds timeout) const;

  std::size_t ExpectedRuntimeStatusCountForNode(
      const naim::DesiredState& desired_node_state,
      const std::string& node_name) const;

  void WaitForLocalInstanceRuntimeStatuses(
      const std::string& state_root,
      const std::string& node_name,
      const std::optional<std::string>& plane_name,
      std::size_t expected_count,
      std::chrono::seconds timeout) const;

 private:
  static bool InstanceProducesRuntimeStatus(const naim::InstanceSpec& instance);

  const HostdDesiredStatePathSupport& desired_state_path_support_;
  const HostdLocalStateRepository& local_state_repository_;
  const HostdRuntimeTelemetrySupport& runtime_telemetry_support_;
};

}  // namespace naim::hostd
