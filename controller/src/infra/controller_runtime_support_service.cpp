#include "infra/controller_runtime_support_service.h"

#include <ctime>
#include <iomanip>
#include <sstream>
#include <stdexcept>

namespace naim::controller {

std::map<std::string, naim::NodeAvailabilityOverride>
ControllerRuntimeSupportService::BuildAvailabilityOverrideMap(
    const std::vector<naim::NodeAvailabilityOverride>& availability_overrides) const {
  std::map<std::string, naim::NodeAvailabilityOverride> result;
  for (const auto& availability_override : availability_overrides) {
    result.emplace(availability_override.node_name, availability_override);
  }
  return result;
}

naim::NodeAvailability ControllerRuntimeSupportService::ResolveNodeAvailability(
    const std::map<std::string, naim::NodeAvailabilityOverride>& availability_overrides,
    const std::string& node_name) const {
  const auto it = availability_overrides.find(node_name);
  if (it == availability_overrides.end()) {
    return naim::NodeAvailability::Active;
  }
  return it->second.availability;
}

std::optional<naim::HostObservation>
ControllerRuntimeSupportService::FindHostObservationForNode(
    const std::vector<naim::HostObservation>& observations,
    const std::string& node_name) const {
  for (const auto& observation : observations) {
    if (observation.node_name == node_name) {
      return observation;
    }
  }
  return std::nullopt;
}

std::time_t ControllerRuntimeSupportService::ToUtcTime(std::tm* timestamp) const {
#if defined(_WIN32)
  return _mkgmtime(timestamp);
#else
  return timegm(timestamp);
#endif
}

std::optional<long long> ControllerRuntimeSupportService::HeartbeatAgeSeconds(
    const std::string& heartbeat_at) const {
  if (heartbeat_at.empty()) {
    return std::nullopt;
  }

  std::tm heartbeat_tm{};
  std::istringstream input(heartbeat_at);
  input >> std::get_time(&heartbeat_tm, "%Y-%m-%d %H:%M:%S");
  if (input.fail()) {
    return std::nullopt;
  }

  const std::time_t heartbeat_time = ToUtcTime(&heartbeat_tm);
  if (heartbeat_time < 0) {
    return std::nullopt;
  }

  const std::time_t now = std::time(nullptr);
  return static_cast<long long>(now - heartbeat_time);
}

std::optional<long long> ControllerRuntimeSupportService::TimestampAgeSeconds(
    const std::string& timestamp_text) const {
  return HeartbeatAgeSeconds(timestamp_text);
}

std::string ControllerRuntimeSupportService::UtcNowSqlTimestamp() const {
  const std::time_t now = std::time(nullptr);
  std::tm tm{};
  if (!naim::platform::GmTime(&now, &tm)) {
    throw std::runtime_error("failed to format current UTC timestamp");
  }
  std::ostringstream out;
  out << std::put_time(&tm, "%Y-%m-%d %H:%M:%S");
  return out.str();
}

std::string ControllerRuntimeSupportService::HealthFromAge(
    const std::optional<long long>& age_seconds,
    int stale_after_seconds) const {
  if (!age_seconds.has_value()) {
    return "unknown";
  }
  return *age_seconds > stale_after_seconds ? "stale" : "online";
}

std::optional<naim::RuntimeStatus>
ControllerRuntimeSupportService::ParseRuntimeStatus(
    const naim::HostObservation& observation) const {
  if (observation.runtime_status_json.empty()) {
    return std::nullopt;
  }
  return naim::DeserializeRuntimeStatusJson(observation.runtime_status_json);
}

std::vector<naim::RuntimeProcessStatus>
ControllerRuntimeSupportService::ParseInstanceRuntimeStatuses(
    const naim::HostObservation& observation) const {
  if (observation.instance_runtime_json.empty()) {
    return {};
  }
  return naim::DeserializeRuntimeStatusListJson(observation.instance_runtime_json);
}

std::optional<naim::GpuTelemetrySnapshot>
ControllerRuntimeSupportService::ParseGpuTelemetry(
    const naim::HostObservation& observation) const {
  if (observation.gpu_telemetry_json.empty()) {
    return std::nullopt;
  }
  return naim::DeserializeGpuTelemetryJson(observation.gpu_telemetry_json);
}

std::optional<naim::DiskTelemetrySnapshot>
ControllerRuntimeSupportService::ParseDiskTelemetry(
    const naim::HostObservation& observation) const {
  if (observation.disk_telemetry_json.empty()) {
    return std::nullopt;
  }
  return naim::DeserializeDiskTelemetryJson(observation.disk_telemetry_json);
}

std::optional<naim::NetworkTelemetrySnapshot>
ControllerRuntimeSupportService::ParseNetworkTelemetry(
    const naim::HostObservation& observation) const {
  if (observation.network_telemetry_json.empty()) {
    return std::nullopt;
  }
  return naim::DeserializeNetworkTelemetryJson(observation.network_telemetry_json);
}

std::optional<naim::CpuTelemetrySnapshot>
ControllerRuntimeSupportService::ParseCpuTelemetry(
    const naim::HostObservation& observation) const {
  if (observation.cpu_telemetry_json.empty()) {
    return std::nullopt;
  }
  return naim::DeserializeCpuTelemetryJson(observation.cpu_telemetry_json);
}

}  // namespace naim::controller
