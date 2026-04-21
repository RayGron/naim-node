#include "scheduler/scheduler_domain_support.h"

namespace naim::controller {

ControllerSchedulerDomainSupport::ControllerSchedulerDomainSupport(
    const ControllerRuntimeSupportService& runtime_support_service,
    const PlaneRealizationService& plane_realization_service)
    : runtime_support_service_(runtime_support_service),
      plane_realization_service_(plane_realization_service) {}

std::optional<long long> ControllerSchedulerDomainSupport::HeartbeatAgeSeconds(
    const std::string& heartbeat_at) const {
  return runtime_support_service_.HeartbeatAgeSeconds(heartbeat_at);
}

std::string ControllerSchedulerDomainSupport::HealthFromAge(
    const std::optional<long long>& age_seconds,
    int stale_after_seconds) const {
  return runtime_support_service_.HealthFromAge(age_seconds, stale_after_seconds);
}

std::optional<naim::RuntimeStatus> ControllerSchedulerDomainSupport::ParseRuntimeStatus(
    const naim::HostObservation& observation) const {
  return runtime_support_service_.ParseRuntimeStatus(observation);
}

std::optional<naim::GpuTelemetrySnapshot> ControllerSchedulerDomainSupport::ParseGpuTelemetry(
    const naim::HostObservation& observation) const {
  return runtime_support_service_.ParseGpuTelemetry(observation);
}

std::map<std::string, naim::NodeAvailabilityOverride>
ControllerSchedulerDomainSupport::BuildAvailabilityOverrideMap(
    const std::vector<naim::NodeAvailabilityOverride>& availability_overrides) const {
  return runtime_support_service_.BuildAvailabilityOverrideMap(availability_overrides);
}

naim::NodeAvailability ControllerSchedulerDomainSupport::ResolveNodeAvailability(
    const std::map<std::string, naim::NodeAvailabilityOverride>& availability_overrides,
    const std::string& node_name) const {
  return runtime_support_service_.ResolveNodeAvailability(availability_overrides, node_name);
}

bool ControllerSchedulerDomainSupport::IsNodeSchedulable(
    naim::NodeAvailability availability) const {
  return availability == naim::NodeAvailability::Active;
}

std::optional<long long> ControllerSchedulerDomainSupport::TimestampAgeSeconds(
    const std::string& timestamp_text) const {
  return runtime_support_service_.TimestampAgeSeconds(timestamp_text);
}

std::optional<std::string> ControllerSchedulerDomainSupport::ObservedSchedulingGateReason(
    const std::vector<naim::HostObservation>& observations,
    const std::string& node_name,
    int stale_after_seconds) const {
  return plane_realization_service_.ObservedSchedulingGateReason(
      observations,
      node_name,
      stale_after_seconds);
}

}  // namespace naim::controller
