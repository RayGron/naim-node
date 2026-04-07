#include "read_model/read_model_service.h"

#include <algorithm>
#include <cstdint>
#include <set>
#include <stdexcept>
#include <utility>

#include "comet/state/sqlite_store.h"
#include "comet/state/state_json.h"

using nlohmann::json;

namespace comet::controller {

namespace {

bool MatchesPlaneInstanceName(
    const std::string& instance_name,
    const std::string& plane_name,
    const std::set<std::string>& plane_instance_names) {
  if (plane_name.empty()) {
    return true;
  }
  if (!plane_instance_names.empty()) {
    return plane_instance_names.find(instance_name) != plane_instance_names.end();
  }

  const std::vector<std::string> prefixes = {
      "infer-",
      "worker-",
      "skills-",
      "app-",
      "webgateway-",
      "browsing-",
  };
  for (const auto& prefix : prefixes) {
    const std::string stem = prefix + plane_name;
    if (instance_name == stem || instance_name.rfind(stem + "-", 0) == 0) {
      return true;
    }
  }
  return false;
}

std::set<std::string> CollectPlaneInstanceNames(
    const comet::DesiredState& observed_state,
    const std::string& plane_name) {
  std::set<std::string> names;
  for (const auto& instance : observed_state.instances) {
    if (instance.plane_name == plane_name) {
      names.insert(instance.name);
    }
  }
  return names;
}

comet::DesiredState FilterObservedStateForPlane(
    const comet::DesiredState& observed_state,
    const std::string& plane_name) {
  comet::DesiredState filtered = observed_state;
  filtered.plane_name = plane_name;

  filtered.disks.erase(
      std::remove_if(
          filtered.disks.begin(),
          filtered.disks.end(),
          [&](const comet::DiskSpec& disk) { return disk.plane_name != plane_name; }),
      filtered.disks.end());
  filtered.instances.erase(
      std::remove_if(
          filtered.instances.begin(),
          filtered.instances.end(),
          [&](const comet::InstanceSpec& instance) {
            return instance.plane_name != plane_name;
          }),
      filtered.instances.end());

  const auto plane_instance_names = CollectPlaneInstanceNames(filtered, plane_name);
  filtered.worker_group.members.erase(
      std::remove_if(
          filtered.worker_group.members.begin(),
          filtered.worker_group.members.end(),
          [&](const comet::WorkerGroupMemberSpec& member) {
            return !MatchesPlaneInstanceName(
                member.name, plane_name, plane_instance_names) &&
                   !MatchesPlaneInstanceName(
                       member.infer_instance_name, plane_name, plane_instance_names);
          }),
      filtered.worker_group.members.end());
  filtered.runtime_gpu_nodes.erase(
      std::remove_if(
          filtered.runtime_gpu_nodes.begin(),
          filtered.runtime_gpu_nodes.end(),
          [&](const comet::RuntimeGpuNode& gpu_node) {
            return !MatchesPlaneInstanceName(
                gpu_node.name, plane_name, plane_instance_names);
          }),
      filtered.runtime_gpu_nodes.end());

  std::set<std::string> plane_node_names;
  for (const auto& instance : filtered.instances) {
    if (!instance.node_name.empty()) {
      plane_node_names.insert(instance.node_name);
    }
  }
  for (const auto& disk : filtered.disks) {
    if (!disk.node_name.empty()) {
      plane_node_names.insert(disk.node_name);
    }
  }
  if (!plane_node_names.empty()) {
    filtered.nodes.erase(
        std::remove_if(
            filtered.nodes.begin(),
            filtered.nodes.end(),
            [&](const comet::NodeInventory& node) {
              return plane_node_names.find(node.name) == plane_node_names.end();
            }),
        filtered.nodes.end());
  }

  filtered.worker_group.expected_workers = static_cast<int>(std::count_if(
      filtered.worker_group.members.begin(),
      filtered.worker_group.members.end(),
      [](const comet::WorkerGroupMemberSpec& member) { return member.enabled; }));
  if (!MatchesPlaneInstanceName(
          filtered.worker_group.infer_instance_name, plane_name, plane_instance_names)) {
    const auto infer_it = std::find_if(
        filtered.instances.begin(),
        filtered.instances.end(),
        [](const comet::InstanceSpec& instance) {
          return instance.role == comet::InstanceRole::Infer;
        });
    filtered.worker_group.infer_instance_name =
        infer_it != filtered.instances.end() ? infer_it->name : std::string{};
  }

  const auto shared_disk_it = std::find_if(
      filtered.disks.begin(),
      filtered.disks.end(),
      [](const comet::DiskSpec& disk) { return disk.kind == comet::DiskKind::PlaneShared; });
  filtered.plane_shared_disk_name =
      shared_disk_it != filtered.disks.end() ? shared_disk_it->name : std::string{};

  return filtered;
}

std::optional<comet::DesiredState> ParseObservedStateForPlane(
    const comet::HostObservation& observation,
    const std::optional<std::string>& plane_name) {
  if (observation.observed_state_json.empty()) {
    return std::nullopt;
  }

  const auto observed_state =
      comet::DeserializeDesiredStateJson(observation.observed_state_json);
  if (!plane_name.has_value()) {
    return observed_state;
  }

  const auto filtered = FilterObservedStateForPlane(observed_state, *plane_name);
  if (observed_state.plane_name == *plane_name || !filtered.instances.empty() ||
      !filtered.disks.empty() || !filtered.worker_group.members.empty() ||
      !filtered.runtime_gpu_nodes.empty()) {
    return filtered;
  }
  return std::nullopt;
}

std::optional<comet::RuntimeStatus> FilterRuntimeStatusForPlane(
    const std::optional<comet::RuntimeStatus>& runtime_status,
    const std::optional<std::string>& plane_name,
    const std::set<std::string>& plane_instance_names) {
  if (!runtime_status.has_value() || !plane_name.has_value()) {
    return runtime_status;
  }
  if (!runtime_status->plane_name.empty() && runtime_status->plane_name != *plane_name) {
    return std::nullopt;
  }
  if (!runtime_status->instance_name.empty() &&
      !MatchesPlaneInstanceName(
          runtime_status->instance_name, *plane_name, plane_instance_names)) {
    return std::nullopt;
  }
  return runtime_status;
}

std::vector<comet::RuntimeProcessStatus> FilterInstanceRuntimeStatusesForPlane(
    const std::vector<comet::RuntimeProcessStatus>& statuses,
    const std::optional<std::string>& plane_name,
    const std::set<std::string>& plane_instance_names) {
  if (!plane_name.has_value()) {
    return statuses;
  }

  std::vector<comet::RuntimeProcessStatus> filtered;
  for (const auto& status : statuses) {
    if (MatchesPlaneInstanceName(
            status.instance_name, *plane_name, plane_instance_names)) {
      filtered.push_back(status);
    }
  }
  return filtered;
}

std::optional<comet::GpuTelemetrySnapshot> FilterGpuTelemetryForPlane(
    const std::optional<comet::GpuTelemetrySnapshot>& snapshot,
    const std::optional<std::string>& plane_name,
    const std::set<std::string>& plane_instance_names) {
  if (!snapshot.has_value() || !plane_name.has_value()) {
    return snapshot;
  }

  comet::GpuTelemetrySnapshot filtered = *snapshot;
  for (auto& device : filtered.devices) {
    device.processes.erase(
        std::remove_if(
            device.processes.begin(),
            device.processes.end(),
            [&](const comet::GpuProcessTelemetry& process) {
              return process.instance_name != "unknown" &&
                     !MatchesPlaneInstanceName(
                         process.instance_name, *plane_name, plane_instance_names);
            }),
        device.processes.end());
  }
  return filtered;
}

std::optional<comet::DiskTelemetrySnapshot> FilterDiskTelemetryForPlane(
    const std::optional<comet::DiskTelemetrySnapshot>& snapshot,
    const std::optional<std::string>& plane_name) {
  if (!snapshot.has_value() || !plane_name.has_value()) {
    return snapshot;
  }

  comet::DiskTelemetrySnapshot filtered = *snapshot;
  filtered.items.erase(
      std::remove_if(
          filtered.items.begin(),
          filtered.items.end(),
          [&](const comet::DiskTelemetryRecord& item) {
            return item.plane_name != *plane_name;
          }),
      filtered.items.end());
  return filtered;
}

}  // namespace

ReadModelService::ReadModelService() = default;

ReadModelService::ReadModelService(
    ControllerRuntimeSupportService runtime_support_service)
    : runtime_support_service_(std::move(runtime_support_service)) {}

json ReadModelService::BuildEventPayloadItem(
    const comet::EventRecord& event) const {
  json payload = json::object();
  if (!event.payload_json.empty()) {
    try {
      payload = json::parse(event.payload_json);
    } catch (...) {
      payload = json{{"raw", event.payload_json}};
    }
  }
  return json{
      {"id", event.id},
      {"plane_name",
       event.plane_name.empty() ? json(nullptr) : json(event.plane_name)},
      {"node_name",
       event.node_name.empty() ? json(nullptr) : json(event.node_name)},
      {"worker_name",
       event.worker_name.empty() ? json(nullptr) : json(event.worker_name)},
      {"assignment_id",
       event.assignment_id.has_value() ? json(*event.assignment_id)
                                       : json(nullptr)},
      {"rollout_action_id",
       event.rollout_action_id.has_value() ? json(*event.rollout_action_id)
                                           : json(nullptr)},
      {"category", event.category},
      {"event_type", event.event_type},
      {"severity", event.severity},
      {"message", event.message},
      {"payload", payload},
      {"created_at", event.created_at},
  };
}

bool ReadModelService::ObservationMatchesPlane(
    const comet::HostObservation& observation,
    const std::string& plane_name) const {
  if (observation.plane_name == plane_name) {
    return true;
  }
  if (observation.observed_state_json.empty()) {
    return false;
  }

  const auto observed_state =
      comet::DeserializeDesiredStateJson(observation.observed_state_json);
  if (observed_state.plane_name == plane_name) {
    return true;
  }
  for (const auto& disk : observed_state.disks) {
    if (disk.plane_name == plane_name) {
      return true;
    }
  }
  for (const auto& instance : observed_state.instances) {
    if (instance.plane_name == plane_name) {
      return true;
    }
  }
  try {
    const auto instance_statuses =
        runtime_support_service_.ParseInstanceRuntimeStatuses(observation);
    for (const auto& status : instance_statuses) {
      const std::string worker_prefix = "worker-" + plane_name + "-";
      if (status.instance_name == "infer-" + plane_name ||
          status.instance_name == "worker-" + plane_name ||
          status.instance_name.rfind(worker_prefix, 0) == 0) {
        return true;
      }
    }
  } catch (const std::exception&) {
  }
  return false;
}

std::vector<comet::HostObservation> ReadModelService::FilterHostObservationsForPlane(
    const std::vector<comet::HostObservation>& observations,
    const std::string& plane_name) const {
  std::vector<comet::HostObservation> result;
  for (const auto& observation : observations) {
    if (ObservationMatchesPlane(observation, plane_name)) {
      result.push_back(observation);
    }
  }
  return result;
}

json ReadModelService::BuildHostAssignmentsPayload(
    const std::string& db_path,
    const std::optional<std::string>& node_name) const {
  comet::ControllerStore store(db_path);
  store.Initialize();

  json assignments = json::array();
  for (const auto& assignment : store.LoadHostAssignments(node_name)) {
    const comet::DesiredState desired_node_state =
        comet::DeserializeDesiredStateJson(assignment.desired_state_json);
    assignments.push_back(json{
        {"id", assignment.id},
        {"node_name", assignment.node_name},
        {"plane_name", assignment.plane_name},
        {"desired_generation", assignment.desired_generation},
        {"attempt_count", assignment.attempt_count},
        {"max_attempts", assignment.max_attempts},
        {"assignment_type", assignment.assignment_type},
        {"status", comet::ToString(assignment.status)},
        {"status_message", assignment.status_message},
        {"progress",
         (!assignment.progress_json.empty() && assignment.progress_json != "{}")
             ? json::parse(assignment.progress_json)
             : json(nullptr)},
        {"artifacts_root", assignment.artifacts_root},
        {"instance_count", desired_node_state.instances.size()},
    });
  }

  return json{
      {"service", "comet-controller"},
      {"db_path", db_path},
      {"node_name", node_name.has_value() ? json(*node_name) : json(nullptr)},
      {"assignments", assignments},
  };
}

json ReadModelService::BuildHostObservationsPayload(
    const std::string& db_path,
    const std::optional<std::string>& node_name,
    const std::optional<std::string>& plane_name,
    int stale_after_seconds) const {
  comet::ControllerStore store(db_path);
  store.Initialize();
  const auto observations =
      plane_name.has_value()
          ? FilterHostObservationsForPlane(
                store.LoadHostObservations(node_name), *plane_name)
          : store.LoadHostObservations(node_name);

  json items = json::array();
  for (const auto& observation : observations) {
    const auto observed_state = ParseObservedStateForPlane(observation, plane_name);
    const auto plane_instance_names =
        plane_name.has_value() && observed_state.has_value()
            ? CollectPlaneInstanceNames(*observed_state, *plane_name)
            : std::set<std::string>{};
    const auto runtime_status = FilterRuntimeStatusForPlane(
        runtime_support_service_.ParseRuntimeStatus(observation),
        plane_name,
        plane_instance_names);
    const auto telemetry = FilterGpuTelemetryForPlane(
        runtime_support_service_.ParseGpuTelemetry(observation),
        plane_name,
        plane_instance_names);
    const auto cpu_telemetry = runtime_support_service_.ParseCpuTelemetry(observation);
    const auto instance_statuses = FilterInstanceRuntimeStatusesForPlane(
        runtime_support_service_.ParseInstanceRuntimeStatuses(observation),
        plane_name,
        plane_instance_names);
    const auto disk_telemetry = FilterDiskTelemetryForPlane(
        runtime_support_service_.ParseDiskTelemetry(observation),
        plane_name);
    const auto network_telemetry =
        runtime_support_service_.ParseNetworkTelemetry(observation);

    const auto build_runtime_status_payload =
        [&](const std::optional<comet::RuntimeStatus>& status) -> json {
      if (!status.has_value()) {
        return json{
            {"contract_version", 1},
            {"available", false},
            {"runtime", nullptr},
        };
      }
      return json{
          {"contract_version", 1},
          {"available", true},
          {"runtime", json::parse(comet::SerializeRuntimeStatusJson(*status))},
      };
    };

    const auto build_instance_runtime_payload =
        [&](const std::vector<comet::RuntimeProcessStatus>& statuses) -> json {
      int ready_count = 0;
      int gpu_bound_count = 0;
      int running_count = 0;
      for (const auto& status : statuses) {
        if (status.ready) {
          ++ready_count;
        }
        if (!status.gpu_device.empty()) {
          ++gpu_bound_count;
        }
        if (!status.runtime_phase.empty() && status.runtime_phase != "stopped") {
          ++running_count;
        }
      }
      return json{
          {"contract_version", 1},
          {"available", !statuses.empty()},
          {"summary",
           {
               {"count", statuses.size()},
               {"ready_count", ready_count},
               {"running_count", running_count},
               {"gpu_bound_count", gpu_bound_count},
           }},
          {"items",
           statuses.empty()
               ? json::array()
               : json::parse(comet::SerializeRuntimeStatusListJson(statuses))},
      };
    };

    const auto build_gpu_telemetry_payload =
        [&](const std::optional<comet::GpuTelemetrySnapshot>& snapshot) -> json {
      if (!snapshot.has_value()) {
        return json{
            {"contract_version", 1},
            {"available", false},
            {"degraded", true},
            {"source", nullptr},
            {"collected_at", nullptr},
            {"summary",
             {
                 {"device_count", 0},
                 {"owned_process_count", 0},
                 {"unknown_process_count", 0},
                 {"total_vram_mb", 0},
                 {"used_vram_mb", 0},
                 {"free_vram_mb", 0},
             }},
            {"devices", json::array()},
        };
      }

      int owned_process_count = 0;
      int unknown_process_count = 0;
      int total_vram_mb = 0;
      int used_vram_mb = 0;
      int free_vram_mb = 0;
      int temperature_device_count = 0;
      int hottest_temperature_c = 0;
      double average_temperature_c = 0.0;
      int total_temperature_c = 0;
      for (const auto& device : snapshot->devices) {
        total_vram_mb += device.total_vram_mb;
        used_vram_mb += device.used_vram_mb;
        free_vram_mb += device.free_vram_mb;
        if (device.temperature_available) {
          hottest_temperature_c =
              temperature_device_count == 0
                  ? device.temperature_c
                  : std::max(hottest_temperature_c, device.temperature_c);
          total_temperature_c += device.temperature_c;
          ++temperature_device_count;
        }
        for (const auto& process : device.processes) {
          if (process.instance_name == "unknown") {
            ++unknown_process_count;
          } else {
            ++owned_process_count;
          }
        }
      }
      if (temperature_device_count > 0) {
        average_temperature_c =
            static_cast<double>(total_temperature_c) /
            static_cast<double>(temperature_device_count);
      }

      return json{
          {"contract_version", snapshot->contract_version},
          {"available", true},
          {"degraded", snapshot->degraded},
          {"source",
           snapshot->source.empty() ? json(nullptr) : json(snapshot->source)},
          {"collected_at",
           snapshot->collected_at.empty() ? json(nullptr)
                                          : json(snapshot->collected_at)},
          {"summary",
           {
               {"device_count", snapshot->devices.size()},
               {"owned_process_count", owned_process_count},
               {"unknown_process_count", unknown_process_count},
               {"total_vram_mb", total_vram_mb},
               {"used_vram_mb", used_vram_mb},
               {"free_vram_mb", free_vram_mb},
               {"temperature_device_count", temperature_device_count},
               {"average_temperature_c", average_temperature_c},
               {"hottest_temperature_c", hottest_temperature_c},
           }},
          {"devices",
           json::parse(comet::SerializeGpuTelemetryJson(*snapshot)).at("devices")},
      };
    };

    const auto build_disk_telemetry_payload =
        [&](const std::optional<comet::DiskTelemetrySnapshot>& snapshot) -> json {
      if (!snapshot.has_value()) {
        return json{
            {"contract_version", 1},
            {"available", false},
            {"degraded", true},
            {"source", nullptr},
            {"collected_at", nullptr},
            {"summary",
             {
                 {"disk_count", 0},
                 {"mounted_count", 0},
                 {"healthy_count", 0},
                 {"total_bytes", 0},
                 {"used_bytes", 0},
                 {"free_bytes", 0},
             }},
            {"items", json::array()},
        };
      }

      int mounted_count = 0;
      int healthy_count = 0;
      std::uint64_t total_bytes = 0;
      std::uint64_t used_bytes = 0;
      std::uint64_t free_bytes = 0;
      std::uint64_t read_ios = 0;
      std::uint64_t write_ios = 0;
      std::uint64_t read_bytes = 0;
      std::uint64_t write_bytes = 0;
      std::uint64_t io_time_ms = 0;
      std::uint64_t weighted_io_time_ms = 0;
      int io_in_progress = 0;
      int warning_count = 0;
      int fault_count = 0;
      int read_only_count = 0;
      int perf_counters_count = 0;
      int io_error_counter_count = 0;
      for (const auto& item : snapshot->items) {
        if (item.mounted) {
          ++mounted_count;
        }
        if (item.health == "ok") {
          ++healthy_count;
        }
        total_bytes += item.total_bytes;
        used_bytes += item.used_bytes;
        free_bytes += item.free_bytes;
        read_ios += item.read_ios;
        write_ios += item.write_ios;
        read_bytes += item.read_bytes;
        write_bytes += item.write_bytes;
        io_time_ms += item.io_time_ms;
        weighted_io_time_ms += item.weighted_io_time_ms;
        io_in_progress += item.io_in_progress;
        warning_count += item.warning_count;
        fault_count += item.fault_count;
        if (item.read_only) {
          ++read_only_count;
        }
        if (item.perf_counters_available) {
          ++perf_counters_count;
        }
        if (item.io_error_counters_available) {
          ++io_error_counter_count;
        }
      }

      return json{
          {"contract_version", snapshot->contract_version},
          {"available", true},
          {"degraded", snapshot->degraded},
          {"source",
           snapshot->source.empty() ? json(nullptr) : json(snapshot->source)},
          {"collected_at",
           snapshot->collected_at.empty() ? json(nullptr)
                                          : json(snapshot->collected_at)},
          {"summary",
           {
               {"disk_count", snapshot->items.size()},
               {"mounted_count", mounted_count},
               {"healthy_count", healthy_count},
               {"total_bytes", total_bytes},
               {"used_bytes", used_bytes},
               {"free_bytes", free_bytes},
               {"read_ios", read_ios},
               {"write_ios", write_ios},
               {"read_bytes", read_bytes},
               {"write_bytes", write_bytes},
               {"io_time_ms", io_time_ms},
               {"weighted_io_time_ms", weighted_io_time_ms},
               {"io_in_progress", io_in_progress},
               {"warning_count", warning_count},
               {"fault_count", fault_count},
               {"read_only_count", read_only_count},
               {"perf_counters_count", perf_counters_count},
               {"io_error_counter_count", io_error_counter_count},
           }},
          {"items",
           json::parse(comet::SerializeDiskTelemetryJson(*snapshot)).at("items")},
      };
    };

    const auto build_network_telemetry_payload =
        [&](const std::optional<comet::NetworkTelemetrySnapshot>& snapshot) -> json {
      if (!snapshot.has_value()) {
        return json{
            {"contract_version", 1},
            {"available", false},
            {"degraded", true},
            {"source", nullptr},
            {"collected_at", nullptr},
            {"summary",
             {
                 {"interface_count", 0},
                 {"up_count", 0},
                 {"loopback_count", 0},
                 {"rx_bytes", 0},
                 {"tx_bytes", 0},
             }},
            {"interfaces", json::array()},
        };
      }

      int up_count = 0;
      int loopback_count = 0;
      std::uint64_t rx_bytes = 0;
      std::uint64_t tx_bytes = 0;
      for (const auto& interface : snapshot->interfaces) {
        if (interface.link_state == "up" || interface.oper_state == "up") {
          ++up_count;
        }
        if (interface.loopback) {
          ++loopback_count;
        }
        rx_bytes += interface.rx_bytes;
        tx_bytes += interface.tx_bytes;
      }

      return json{
          {"contract_version", snapshot->contract_version},
          {"available", true},
          {"degraded", snapshot->degraded},
          {"source",
           snapshot->source.empty() ? json(nullptr) : json(snapshot->source)},
          {"collected_at",
           snapshot->collected_at.empty() ? json(nullptr)
                                          : json(snapshot->collected_at)},
          {"summary",
           {
               {"interface_count", snapshot->interfaces.size()},
               {"up_count", up_count},
               {"loopback_count", loopback_count},
               {"rx_bytes", rx_bytes},
               {"tx_bytes", tx_bytes},
           }},
          {"interfaces",
           json::parse(comet::SerializeNetworkTelemetryJson(*snapshot)).at(
               "interfaces")},
      };
    };

    const auto build_cpu_telemetry_payload =
        [&](const std::optional<comet::CpuTelemetrySnapshot>& snapshot) -> json {
      if (!snapshot.has_value()) {
        return json{
            {"contract_version", 1},
            {"available", false},
            {"degraded", true},
            {"source", nullptr},
            {"collected_at", nullptr},
            {"summary",
             {
                 {"core_count", 0},
                 {"utilization_pct", 0.0},
                 {"loadavg_1m", 0.0},
                 {"loadavg_5m", 0.0},
                 {"loadavg_15m", 0.0},
                 {"temperature_available", false},
                 {"temperature_c", 0.0},
                 {"max_temperature_c", 0.0},
                 {"total_memory_bytes", 0},
                 {"available_memory_bytes", 0},
                 {"used_memory_bytes", 0},
             }},
            {"snapshot", nullptr},
        };
      }

      return json{
          {"contract_version", snapshot->contract_version},
          {"available", true},
          {"degraded", snapshot->degraded},
          {"source",
           snapshot->source.empty() ? json(nullptr) : json(snapshot->source)},
          {"collected_at",
           snapshot->collected_at.empty() ? json(nullptr)
                                          : json(snapshot->collected_at)},
          {"summary",
           {
               {"core_count", snapshot->core_count},
               {"utilization_pct", snapshot->utilization_pct},
               {"loadavg_1m", snapshot->loadavg_1m},
               {"loadavg_5m", snapshot->loadavg_5m},
               {"loadavg_15m", snapshot->loadavg_15m},
               {"temperature_available", snapshot->temperature_available},
               {"temperature_c", snapshot->temperature_c},
               {"max_temperature_c", snapshot->max_temperature_c},
               {"total_memory_bytes", snapshot->total_memory_bytes},
               {"available_memory_bytes", snapshot->available_memory_bytes},
               {"used_memory_bytes", snapshot->used_memory_bytes},
           }},
          {"snapshot", json::parse(comet::SerializeCpuTelemetryJson(*snapshot))},
      };
    };

    json entry{
        {"node_name", observation.node_name},
        {"plane_name",
         observation.plane_name.empty() ? json(nullptr)
                                        : json(observation.plane_name)},
        {"status", comet::ToString(observation.status)},
        {"status_message", observation.status_message},
        {"heartbeat_at", observation.heartbeat_at},
    };
    entry["applied_generation"] =
        observation.applied_generation.has_value()
            ? json(*observation.applied_generation)
            : json(nullptr);
    entry["last_assignment_id"] =
        observation.last_assignment_id.has_value()
            ? json(*observation.last_assignment_id)
            : json(nullptr);
    const auto age_seconds =
        runtime_support_service_.HeartbeatAgeSeconds(observation.heartbeat_at);
    entry["age_seconds"] =
        age_seconds.has_value() ? json(*age_seconds) : json(nullptr);
    entry["health"] =
        runtime_support_service_.HealthFromAge(age_seconds, stale_after_seconds);

    if (observed_state.has_value()) {
      entry["observed_state"] =
          json::parse(comet::SerializeDesiredStateJson(*observed_state));
    } else {
      entry["observed_state"] = nullptr;
    }
    entry["runtime_status"] = build_runtime_status_payload(runtime_status);
    entry["gpu_telemetry"] = build_gpu_telemetry_payload(telemetry);
    entry["disk_telemetry"] = build_disk_telemetry_payload(disk_telemetry);
    entry["network_telemetry"] =
        build_network_telemetry_payload(network_telemetry);
    entry["cpu_telemetry"] = build_cpu_telemetry_payload(cpu_telemetry);
    entry["instance_runtimes"] =
        build_instance_runtime_payload(instance_statuses);

    items.push_back(std::move(entry));
  }

  return json{
      {"service", "comet-controller"},
      {"db_path", db_path},
      {"plane_name", plane_name.has_value() ? json(*plane_name) : json(nullptr)},
      {"node_name", node_name.has_value() ? json(*node_name) : json(nullptr)},
      {"stale_after_seconds", stale_after_seconds},
      {"observations", items},
  };
}

json ReadModelService::BuildHostHealthPayload(
    const std::string& db_path,
    const std::optional<std::string>& node_name,
    int stale_after_seconds) const {
  comet::ControllerStore store(db_path);
  store.Initialize();
  const auto desired_state = store.LoadDesiredState();
  const auto observations = store.LoadHostObservations(node_name);
  const auto availability_override_map =
      runtime_support_service_.BuildAvailabilityOverrideMap(
          store.LoadNodeAvailabilityOverrides(node_name));

  std::map<std::string, comet::HostObservation> observation_by_node;
  for (const auto& observation : observations) {
    observation_by_node.emplace(observation.node_name, observation);
  }

  std::vector<std::string> nodes;
  std::set<std::string> seen_nodes;
  if (desired_state.has_value()) {
    for (const auto& node : desired_state->nodes) {
      if (!node_name.has_value() || node.name == *node_name) {
        nodes.push_back(node.name);
        seen_nodes.insert(node.name);
      }
    }
  }
  for (const auto& [observed_node_name, observation] : observation_by_node) {
    (void)observation;
    if ((!node_name.has_value() || observed_node_name == *node_name) &&
        seen_nodes.find(observed_node_name) == seen_nodes.end()) {
      nodes.push_back(observed_node_name);
      seen_nodes.insert(observed_node_name);
    }
  }

  int online_count = 0;
  int stale_count = 0;
  int unknown_count = 0;
  json items = json::array();
  for (const auto& current_node_name : nodes) {
    json item{
        {"node_name", current_node_name},
        {"availability",
         comet::ToString(
             runtime_support_service_.ResolveNodeAvailability(
                 availability_override_map,
                 current_node_name))},
    };
    const auto observation_it = observation_by_node.find(current_node_name);
    if (observation_it == observation_by_node.end()) {
      item["health"] = "unknown";
      item["status"] = nullptr;
      ++unknown_count;
      items.push_back(std::move(item));
      continue;
    }

    const auto age_seconds =
        runtime_support_service_.HeartbeatAgeSeconds(
            observation_it->second.heartbeat_at);
    const std::string health =
        runtime_support_service_.HealthFromAge(age_seconds, stale_after_seconds);
    item["health"] = health;
    item["status"] = comet::ToString(observation_it->second.status);
    item["age_seconds"] =
        age_seconds.has_value() ? json(*age_seconds) : json(nullptr);
    item["heartbeat_at"] = observation_it->second.heartbeat_at;
    item["applied_generation"] =
        observation_it->second.applied_generation.has_value()
            ? json(*observation_it->second.applied_generation)
            : json(nullptr);
    if (const auto runtime_status =
            runtime_support_service_.ParseRuntimeStatus(observation_it->second);
        runtime_status.has_value()) {
      item["runtime_phase"] = runtime_status->runtime_phase;
      item["runtime_launch_ready"] = runtime_status->launch_ready;
      item["runtime_backend"] = runtime_status->runtime_backend;
    }
    if (const auto telemetry =
            runtime_support_service_.ParseGpuTelemetry(observation_it->second);
        telemetry.has_value()) {
      item["telemetry_degraded"] = telemetry->degraded;
      item["telemetry_source"] = telemetry->source;
      item["gpu_device_count"] = telemetry->devices.size();
    }
    if (health == "online") {
      ++online_count;
    } else if (health == "stale") {
      ++stale_count;
    } else {
      ++unknown_count;
    }
    items.push_back(std::move(item));
  }

  return json{
      {"service", "comet-controller"},
      {"db_path", db_path},
      {"node_name", node_name.has_value() ? json(*node_name) : json(nullptr)},
      {"stale_after_seconds", stale_after_seconds},
      {"summary",
       {
           {"online", online_count},
           {"stale", stale_count},
           {"unknown", unknown_count},
       }},
      {"items", items},
  };
}

json ReadModelService::BuildDiskStatePayload(
    const std::string& db_path,
    const std::optional<std::string>& node_name,
    const std::optional<std::string>& plane_name) const {
  comet::ControllerStore store(db_path);
  store.Initialize();
  const auto desired_state = plane_name.has_value()
                                 ? store.LoadDesiredState(*plane_name)
                                 : store.LoadDesiredState();

  json payload{
      {"service", "comet-controller"},
      {"db_path", db_path},
      {"plane_name", plane_name.has_value() ? json(*plane_name) : json(nullptr)},
      {"node_name", node_name.has_value() ? json(*node_name) : json(nullptr)},
  };
  if (!desired_state.has_value()) {
    payload["desired_state"] = nullptr;
    payload["items"] = json::array();
    return payload;
  }

  const auto desired_generation =
      plane_name.has_value() ? store.LoadDesiredGeneration(*plane_name)
                             : store.LoadDesiredGeneration();
  const auto runtime_states =
      desired_state.has_value()
          ? store.LoadDiskRuntimeStates(desired_state->plane_name, node_name)
          : std::vector<comet::DiskRuntimeState>{};
  const auto observations =
      plane_name.has_value()
          ? FilterHostObservationsForPlane(
                store.LoadHostObservations(node_name), *plane_name)
          : store.LoadHostObservations(node_name);

  payload["plane_name"] = desired_state->plane_name;
  payload["desired_generation"] =
      desired_generation.has_value() ? json(*desired_generation) : json(nullptr);

  std::map<std::string, comet::DiskRuntimeState> runtime_by_key;
  for (const auto& runtime_state : runtime_states) {
    runtime_by_key.emplace(
        runtime_state.disk_name + "@" + runtime_state.node_name,
        runtime_state);
  }
  std::map<std::string, comet::DiskTelemetryRecord> telemetry_by_key;
  for (const auto& observation : observations) {
    const auto disk_telemetry =
        runtime_support_service_.ParseDiskTelemetry(observation);
    if (!disk_telemetry.has_value()) {
      continue;
    }
    for (const auto& item : disk_telemetry->items) {
      telemetry_by_key[item.disk_name + "@" + item.node_name] = item;
    }
  }

  json items = json::array();
  for (const auto& disk : desired_state->disks) {
    if (node_name.has_value() && disk.node_name != *node_name) {
      continue;
    }
    json item{
        {"disk_name", disk.name},
        {"kind", comet::ToString(disk.kind)},
        {"plane_name", disk.plane_name},
        {"owner_name", disk.owner_name},
        {"node_name", disk.node_name},
        {"size_gb", disk.size_gb},
        {"desired_host_path", disk.host_path},
        {"desired_container_path", disk.container_path},
    };
    const std::string key = disk.name + "@" + disk.node_name;
    const auto runtime_it = runtime_by_key.find(key);
    if (runtime_it == runtime_by_key.end()) {
      item["realized_state"] = "missing-runtime-state";
    } else {
      const auto& runtime_state = runtime_it->second;
      item["realized_state"] = runtime_state.runtime_state.empty()
                                   ? json("(empty)")
                                   : json(runtime_state.runtime_state);
      item["mount_point"] = runtime_state.mount_point.empty()
                                ? json(nullptr)
                                : json(runtime_state.mount_point);
      item["filesystem_type"] = runtime_state.filesystem_type.empty()
                                    ? json(nullptr)
                                    : json(runtime_state.filesystem_type);
      item["image_path"] = runtime_state.image_path.empty()
                               ? json(nullptr)
                               : json(runtime_state.image_path);
      item["loop_device"] = runtime_state.loop_device.empty()
                                ? json(nullptr)
                                : json(runtime_state.loop_device);
      item["last_verified_at"] = runtime_state.last_verified_at.empty()
                                     ? json(nullptr)
                                     : json(runtime_state.last_verified_at);
      item["status_message"] = runtime_state.status_message.empty()
                                   ? json(nullptr)
                                   : json(runtime_state.status_message);
    }
    const auto telemetry_it = telemetry_by_key.find(key);
    if (telemetry_it != telemetry_by_key.end()) {
      const auto& telemetry = telemetry_it->second;
      item["usage_bytes"] = {
          {"total_bytes", telemetry.total_bytes},
          {"used_bytes", telemetry.used_bytes},
          {"free_bytes", telemetry.free_bytes},
      };
      item["io_bytes"] = {
          {"read_bytes", telemetry.read_bytes},
          {"write_bytes", telemetry.write_bytes},
      };
      item["io_ops"] = {
          {"read_ios", telemetry.read_ios},
          {"write_ios", telemetry.write_ios},
      };
      item["io_time_ms"] = telemetry.io_time_ms;
      item["weighted_io_time_ms"] = telemetry.weighted_io_time_ms;
      item["io_error_count"] = telemetry.io_error_count;
      item["io_in_progress"] = telemetry.io_in_progress;
      item["warning_count"] = telemetry.warning_count;
      item["fault_count"] = telemetry.fault_count;
      item["read_only"] = telemetry.read_only;
      item["perf_counters_available"] = telemetry.perf_counters_available;
      item["io_error_counters_available"] =
          telemetry.io_error_counters_available;
      item["mount_source"] = telemetry.mount_source.empty()
                                 ? json(nullptr)
                                 : json(telemetry.mount_source);
      item["fault_reasons"] = telemetry.fault_reasons;
      item["mount_health"] =
          telemetry.health.empty() ? json(nullptr) : json(telemetry.health);
      item["mounted"] = telemetry.mounted;
      item["telemetry_status_message"] = telemetry.status_message.empty()
                                             ? json(nullptr)
                                             : json(telemetry.status_message);
    }
    items.push_back(std::move(item));
  }

  for (const auto& runtime_state : runtime_states) {
    if (node_name.has_value() && runtime_state.node_name != *node_name) {
      continue;
    }
    const std::string key = runtime_state.disk_name + "@" + runtime_state.node_name;
    bool found_in_desired = false;
    for (const auto& disk : desired_state->disks) {
      if (disk.name + "@" + disk.node_name == key) {
        found_in_desired = true;
        break;
      }
    }
    if (found_in_desired) {
      continue;
    }
    items.push_back(json{
        {"disk_name", runtime_state.disk_name},
        {"plane_name", runtime_state.plane_name},
        {"node_name", runtime_state.node_name},
        {"realized_state",
         runtime_state.runtime_state.empty() ? json("(empty)")
                                             : json(runtime_state.runtime_state)},
        {"desired_state", "orphan-runtime-state"},
        {"mount_point", runtime_state.mount_point.empty()
                            ? json(nullptr)
                            : json(runtime_state.mount_point)},
        {"image_path", runtime_state.image_path.empty()
                           ? json(nullptr)
                           : json(runtime_state.image_path)},
        {"loop_device", runtime_state.loop_device.empty()
                            ? json(nullptr)
                            : json(runtime_state.loop_device)},
        {"status_message", runtime_state.status_message.empty()
                               ? json(nullptr)
                               : json(runtime_state.status_message)},
    });
  }

  payload["desired_state"] = "present";
  payload["items"] = std::move(items);
  return payload;
}

json ReadModelService::BuildEventsPayload(
    const std::string& db_path,
    const std::optional<std::string>& plane_name,
    const std::optional<std::string>& node_name,
    const std::optional<std::string>& worker_name,
    const std::optional<std::string>& category,
    int limit) const {
  comet::ControllerStore store(db_path);
  store.Initialize();

  json items = json::array();
  for (const auto& event : store.LoadEvents(
           plane_name,
           node_name,
           worker_name,
           category,
           limit)) {
    items.push_back(BuildEventPayloadItem(event));
  }

  return json{
      {"service", "comet-controller"},
      {"db_path", db_path},
      {"plane_name", plane_name.has_value() ? json(*plane_name) : json(nullptr)},
      {"node_name", node_name.has_value() ? json(*node_name) : json(nullptr)},
      {"worker_name",
       worker_name.has_value() ? json(*worker_name) : json(nullptr)},
      {"category", category.has_value() ? json(*category) : json(nullptr)},
      {"limit", limit},
      {"events", std::move(items)},
  };
}

json ReadModelService::BuildNodeAvailabilityPayload(
    const std::string& db_path,
    const std::optional<std::string>& node_name) const {
  comet::ControllerStore store(db_path);
  store.Initialize();
  const auto overrides = store.LoadNodeAvailabilityOverrides(node_name);

  json items = json::array();
  for (const auto& override_record : overrides) {
    items.push_back(json{
        {"node_name", override_record.node_name},
        {"availability", comet::ToString(override_record.availability)},
        {"status_message",
         override_record.status_message.empty()
             ? json(nullptr)
             : json(override_record.status_message)},
        {"updated_at",
         override_record.updated_at.empty()
             ? json(nullptr)
             : json(override_record.updated_at)},
    });
  }

  return json{
      {"service", "comet-controller"},
      {"db_path", db_path},
      {"node_name", node_name.has_value() ? json(*node_name) : json(nullptr)},
      {"items", items},
  };
}

}  // namespace comet::controller
