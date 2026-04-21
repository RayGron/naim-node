#include "scheduler/scheduler_execution_dependencies.h"

namespace naim::controller {

ControllerSchedulerAssignmentQuerySupport::ControllerSchedulerAssignmentQuerySupport(
    const PlaneRealizationService& plane_realization_service,
    std::string default_artifacts_root)
    : plane_realization_service_(plane_realization_service),
      default_artifacts_root_(std::move(default_artifacts_root)) {}

std::optional<naim::HostAssignment>
ControllerSchedulerAssignmentQuerySupport::FindLatestHostAssignmentForNode(
    const std::vector<naim::HostAssignment>& assignments,
    const std::string& node_name) const {
  return plane_realization_service_.FindLatestHostAssignmentForNode(assignments, node_name);
}

std::optional<naim::HostAssignment>
ControllerSchedulerAssignmentQuerySupport::FindLatestHostAssignmentForPlane(
    const std::vector<naim::HostAssignment>& assignments,
    const std::string& plane_name) const {
  return plane_realization_service_.FindLatestHostAssignmentForPlane(assignments, plane_name);
}

std::string ControllerSchedulerAssignmentQuerySupport::DefaultArtifactsRoot() const {
  return default_artifacts_root_;
}

ControllerSchedulerVerificationSupport::ControllerSchedulerVerificationSupport(
    const ControllerRuntimeSupportService& runtime_support_service)
    : runtime_support_service_(runtime_support_service) {}

std::optional<naim::HostObservation>
ControllerSchedulerVerificationSupport::FindHostObservationForNode(
    const std::vector<naim::HostObservation>& observations,
    const std::string& node_name) const {
  return runtime_support_service_.FindHostObservationForNode(observations, node_name);
}

std::vector<naim::RuntimeProcessStatus>
ControllerSchedulerVerificationSupport::ParseInstanceRuntimeStatuses(
    const naim::HostObservation& observation) const {
  return runtime_support_service_.ParseInstanceRuntimeStatuses(observation);
}

std::optional<naim::GpuTelemetrySnapshot>
ControllerSchedulerVerificationSupport::ParseGpuTelemetry(
    const naim::HostObservation& observation) const {
  return runtime_support_service_.ParseGpuTelemetry(observation);
}

std::optional<long long> ControllerSchedulerVerificationSupport::TimestampAgeSeconds(
    const std::string& timestamp_text) const {
  return runtime_support_service_.TimestampAgeSeconds(timestamp_text);
}

std::string ControllerSchedulerVerificationSupport::UtcNowSqlTimestamp() const {
  return runtime_support_service_.UtcNowSqlTimestamp();
}

}  // namespace naim::controller
