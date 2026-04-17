#include "app/hostd_observed_state_snapshot_builder.h"

#include "naim/state/state_json.h"

namespace naim::hostd {

HostdObservedStateSnapshotBuilder::HostdObservedStateSnapshotBuilder(
    const HostdLocalStateRepository& local_state_repository,
    const HostdLocalRuntimeStateSupport& local_runtime_state_support,
    const HostdRuntimeTelemetrySupport& runtime_telemetry_support,
    const HostdSystemTelemetryCollector& system_telemetry_collector)
    : local_state_repository_(local_state_repository),
      local_runtime_state_support_(local_runtime_state_support),
      runtime_telemetry_support_(runtime_telemetry_support),
      system_telemetry_collector_(system_telemetry_collector) {}

naim::HostObservation HostdObservedStateSnapshotBuilder::BuildObservedStateSnapshot(
    const std::string& node_name,
    const std::string& storage_root,
    const std::string& state_root,
    naim::HostObservationStatus status,
    const std::string& status_message,
    const std::optional<int>& assignment_id) const {
  naim::HostObservation observation;
  observation.node_name = node_name;
  observation.status = status;
  observation.status_message = status_message;
  observation.last_assignment_id = assignment_id;
  observation.applied_generation =
      local_state_repository_.LoadLocalAppliedGeneration(state_root, node_name);

  const auto local_state = local_state_repository_.LoadLocalAppliedState(state_root, node_name);
  if (local_state.has_value()) {
    observation.plane_name = local_state->plane_name;
    observation.observed_state_json = naim::SerializeDesiredStateJson(*local_state);
  }
  const auto runtime_status =
      local_runtime_state_support_.LoadLocalRuntimeStatus(state_root, node_name);
  if (runtime_status.has_value()) {
    observation.runtime_status_json = naim::SerializeRuntimeStatusJson(*runtime_status);
  }
  auto instance_statuses =
      runtime_telemetry_support_.LoadLocalInstanceRuntimeStatuses(state_root, node_name);
  runtime_telemetry_support_.ResolveInstanceHostPids(&instance_statuses);
  if (!instance_statuses.empty()) {
    observation.instance_runtime_json = naim::SerializeRuntimeStatusListJson(instance_statuses);
  }
  const naim::DesiredState telemetry_state =
      local_state.has_value() ? *local_state : naim::DesiredState{};
  observation.gpu_telemetry_json = naim::SerializeGpuTelemetryJson(
      system_telemetry_collector_.CollectGpuTelemetry(
          telemetry_state,
          node_name,
          instance_statuses));
  naim::DiskTelemetrySnapshot disk_snapshot;
  disk_snapshot.contract_version = 1;
  disk_snapshot.source = "hostd";
  disk_snapshot.collected_at = "";
  disk_snapshot.items.push_back(
      system_telemetry_collector_.BuildStorageRootTelemetry(node_name, storage_root));
  if (local_state.has_value()) {
    const auto managed_snapshot =
        system_telemetry_collector_.CollectDiskTelemetry(*local_state, node_name);
    disk_snapshot.degraded = managed_snapshot.degraded;
    if (!managed_snapshot.source.empty()) {
      disk_snapshot.source = managed_snapshot.source;
    }
    if (!managed_snapshot.collected_at.empty()) {
      disk_snapshot.collected_at = managed_snapshot.collected_at;
    }
    disk_snapshot.items.insert(
        disk_snapshot.items.end(),
        managed_snapshot.items.begin(),
        managed_snapshot.items.end());
  }
  observation.disk_telemetry_json = naim::SerializeDiskTelemetryJson(disk_snapshot);
  observation.network_telemetry_json = naim::SerializeNetworkTelemetryJson(
      system_telemetry_collector_.CollectNetworkTelemetry(state_root));
  observation.cpu_telemetry_json = naim::SerializeCpuTelemetryJson(
      system_telemetry_collector_.CollectCpuTelemetry());

  return observation;
}

}  // namespace naim::hostd
