#pragma once

#include <map>
#include <optional>
#include <string>
#include <vector>

#include "infra/controller_runtime_support_service.h"
#include "plane/plane_realization_service.h"

namespace naim::controller {

class SchedulerDomainSupport {
 public:
  virtual ~SchedulerDomainSupport() = default;

  virtual std::optional<long long> HeartbeatAgeSeconds(
      const std::string& heartbeat_at) const = 0;
  virtual std::string HealthFromAge(
      const std::optional<long long>& age_seconds,
      int stale_after_seconds) const = 0;
  virtual std::optional<naim::RuntimeStatus> ParseRuntimeStatus(
      const naim::HostObservation& observation) const = 0;
  virtual std::optional<naim::GpuTelemetrySnapshot> ParseGpuTelemetry(
      const naim::HostObservation& observation) const = 0;
  virtual std::map<std::string, naim::NodeAvailabilityOverride>
  BuildAvailabilityOverrideMap(
      const std::vector<naim::NodeAvailabilityOverride>& availability_overrides) const = 0;
  virtual naim::NodeAvailability ResolveNodeAvailability(
      const std::map<std::string, naim::NodeAvailabilityOverride>& availability_overrides,
      const std::string& node_name) const = 0;
  virtual bool IsNodeSchedulable(naim::NodeAvailability availability) const = 0;
  virtual std::optional<long long> TimestampAgeSeconds(
      const std::string& timestamp_text) const = 0;
  virtual std::optional<std::string> ObservedSchedulingGateReason(
      const std::vector<naim::HostObservation>& observations,
      const std::string& node_name,
      int stale_after_seconds) const = 0;
};

class ControllerSchedulerDomainSupport final : public SchedulerDomainSupport {
 public:
  ControllerSchedulerDomainSupport(
      const ControllerRuntimeSupportService& runtime_support_service,
      const PlaneRealizationService& plane_realization_service);

  std::optional<long long> HeartbeatAgeSeconds(
      const std::string& heartbeat_at) const override;
  std::string HealthFromAge(
      const std::optional<long long>& age_seconds,
      int stale_after_seconds) const override;
  std::optional<naim::RuntimeStatus> ParseRuntimeStatus(
      const naim::HostObservation& observation) const override;
  std::optional<naim::GpuTelemetrySnapshot> ParseGpuTelemetry(
      const naim::HostObservation& observation) const override;
  std::map<std::string, naim::NodeAvailabilityOverride>
  BuildAvailabilityOverrideMap(
      const std::vector<naim::NodeAvailabilityOverride>& availability_overrides) const override;
  naim::NodeAvailability ResolveNodeAvailability(
      const std::map<std::string, naim::NodeAvailabilityOverride>& availability_overrides,
      const std::string& node_name) const override;
  bool IsNodeSchedulable(naim::NodeAvailability availability) const override;
  std::optional<long long> TimestampAgeSeconds(
      const std::string& timestamp_text) const override;
  std::optional<std::string> ObservedSchedulingGateReason(
      const std::vector<naim::HostObservation>& observations,
      const std::string& node_name,
      int stale_after_seconds) const override;

 private:
  const ControllerRuntimeSupportService& runtime_support_service_;
  const PlaneRealizationService& plane_realization_service_;
};

}  // namespace naim::controller
