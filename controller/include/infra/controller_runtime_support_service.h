#pragma once

#include <map>
#include <optional>
#include <string>
#include <vector>

#include "naim/core/platform_compat.h"
#include "naim/runtime/runtime_status.h"
#include "naim/state/sqlite_store.h"

namespace naim::controller {

class ControllerRuntimeSupportService {
 public:
  std::map<std::string, naim::NodeAvailabilityOverride> BuildAvailabilityOverrideMap(
      const std::vector<naim::NodeAvailabilityOverride>& availability_overrides) const;

  naim::NodeAvailability ResolveNodeAvailability(
      const std::map<std::string, naim::NodeAvailabilityOverride>& availability_overrides,
      const std::string& node_name) const;

  std::optional<naim::HostObservation> FindHostObservationForNode(
      const std::vector<naim::HostObservation>& observations,
      const std::string& node_name) const;

  std::optional<long long> HeartbeatAgeSeconds(const std::string& heartbeat_at) const;
  std::optional<long long> TimestampAgeSeconds(const std::string& timestamp_text) const;
  std::string UtcNowSqlTimestamp() const;

  std::string HealthFromAge(
      const std::optional<long long>& age_seconds,
      int stale_after_seconds) const;

  std::optional<naim::RuntimeStatus> ParseRuntimeStatus(
      const naim::HostObservation& observation) const;

  std::vector<naim::RuntimeProcessStatus> ParseInstanceRuntimeStatuses(
      const naim::HostObservation& observation) const;

  std::optional<naim::GpuTelemetrySnapshot> ParseGpuTelemetry(
      const naim::HostObservation& observation) const;

  std::optional<naim::DiskTelemetrySnapshot> ParseDiskTelemetry(
      const naim::HostObservation& observation) const;

  std::optional<naim::NetworkTelemetrySnapshot> ParseNetworkTelemetry(
      const naim::HostObservation& observation) const;

  std::optional<naim::CpuTelemetrySnapshot> ParseCpuTelemetry(
      const naim::HostObservation& observation) const;

 private:
  std::time_t ToUtcTime(std::tm* timestamp) const;
};

}  // namespace naim::controller
