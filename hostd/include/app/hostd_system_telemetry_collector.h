#pragma once

#include <string>
#include <vector>

#include "app/hostd_command_support.h"
#include "naim/runtime/runtime_status.h"
#include "naim/state/models.h"

namespace naim::hostd {

class HostdSystemTelemetryCollector final {
 public:
  naim::GpuTelemetrySnapshot CollectGpuTelemetry(
      const naim::DesiredState& state,
      const std::string& node_name,
      const std::vector<naim::RuntimeProcessStatus>& instance_statuses) const;

  naim::DiskTelemetrySnapshot CollectDiskTelemetry(
      const naim::DesiredState& state,
      const std::string& node_name) const;

  naim::DiskTelemetryRecord BuildStorageRootTelemetry(
      const std::string& node_name,
      const std::string& storage_root) const;

  naim::CpuTelemetrySnapshot CollectCpuTelemetry() const;
  naim::NetworkTelemetrySnapshot CollectNetworkTelemetry(
      const std::string& state_root) const;

 private:
  HostdCommandSupport command_support_;
};

}  // namespace naim::hostd
