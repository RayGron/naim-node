#include "infra/controller_print_service.h"

#include <iomanip>
#include <iostream>
#include <sstream>
#include <set>
#include <stdexcept>
#include <utility>

#include "naim/state/state_json.h"

namespace naim::controller {

ControllerPrintService::ControllerPrintService() = default;

ControllerPrintService::ControllerPrintService(
    ControllerRuntimeSupportService runtime_support_service)
    : runtime_support_service_(std::move(runtime_support_service)) {}

std::optional<std::tm> ControllerPrintService::ParseDisplayTimestamp(
    const std::string& value) const {
  if (value.empty()) {
    return std::nullopt;
  }
  for (const char* format : {"%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S"}) {
    std::tm tm{};
    std::istringstream input(value);
    input >> std::get_time(&tm, format);
    if (!input.fail()) {
      return tm;
    }
  }
  return std::nullopt;
}

std::string ControllerPrintService::FormatDisplayTimestamp(
    const std::string& value) const {
  const auto parsed = ParseDisplayTimestamp(value);
  if (!parsed.has_value()) {
    return value.empty() ? "(empty)" : value;
  }
  std::ostringstream output;
  output << std::put_time(&*parsed, "%d/%m/%Y %H:%M:%S");
  return output.str();
}

void ControllerPrintService::PrintStateSummary(const naim::DesiredState& state) const {
  std::cout << "plane: " << state.plane_name << "\n";
  std::cout << "control_root: " << state.control_root << "\n";
  std::cout << "inference:\n";
  std::cout << "  primary_infer_node=" << state.inference.primary_infer_node
            << " net_if=" << state.inference.net_if
            << " llama_port=" << state.inference.llama_port
            << " max_model_len=" << state.inference.max_model_len
            << " llama_ctx_size=" << state.inference.llama_ctx_size
            << " max_num_seqs=" << state.inference.max_num_seqs
            << " gpu_memory_utilization=" << state.inference.gpu_memory_utilization
            << "\n";
  std::cout << "gateway:\n";
  std::cout << "  listen=" << state.gateway.listen_host << ":" << state.gateway.listen_port
            << " server_name=" << state.gateway.server_name << "\n";
  std::cout << "nodes:\n";
  for (const auto& node : state.nodes) {
    std::cout << "  - " << node.name << " (" << node.platform << "), gpus=";
    for (std::size_t index = 0; index < node.gpu_devices.size(); ++index) {
      if (index > 0) {
        std::cout << ",";
      }
      const auto it = node.gpu_memory_mb.find(node.gpu_devices[index]);
      std::cout << node.gpu_devices[index];
      if (it != node.gpu_memory_mb.end()) {
        std::cout << "(" << it->second << "MB)";
      }
    }
    std::cout << "\n";
  }

  std::cout << "disks:\n";
  for (const auto& disk : state.disks) {
    std::cout << "  - " << disk.name
              << " kind=" << naim::ToString(disk.kind)
              << " node=" << disk.node_name
              << " host_path=" << disk.host_path
              << " container_path=" << disk.container_path
              << " size_gb=" << disk.size_gb
              << "\n";
  }

  std::cout << "instances:\n";
  for (const auto& instance : state.instances) {
    std::cout << "  - " << instance.name
              << " role=" << naim::ToString(instance.role)
              << " node=" << instance.node_name;
    if (instance.gpu_device.has_value()) {
      std::cout << " gpu=" << *instance.gpu_device
                << " fraction=" << instance.gpu_fraction
                << " placement_mode=" << naim::ToString(instance.placement_mode)
                << " share_mode=" << naim::ToString(instance.share_mode)
                << " priority=" << instance.priority
                << " preemptible=" << (instance.preemptible ? "true" : "false");
      if (instance.memory_cap_mb.has_value()) {
        std::cout << " memory_cap_mb=" << *instance.memory_cap_mb;
      }
      const auto placement_it = instance.labels.find("naim.placement");
      if (placement_it != instance.labels.end()) {
        std::cout << " placement=" << placement_it->second;
      }
      const auto action_it = instance.labels.find("naim.placement.action");
      if (action_it != instance.labels.end()) {
        std::cout << " placement_action=" << action_it->second;
      }
      const auto score_it = instance.labels.find("naim.placement.score");
      if (score_it != instance.labels.end()) {
        std::cout << " placement_score=" << score_it->second;
      }
      const auto decision_it = instance.labels.find("naim.placement.decision");
      if (decision_it != instance.labels.end()) {
        std::cout << " placement_decision=" << decision_it->second;
      }
      const auto next_action_it = instance.labels.find("naim.placement.next_action");
      if (next_action_it != instance.labels.end()) {
        std::cout << " next_action=" << next_action_it->second;
      }
      const auto next_target_it = instance.labels.find("naim.placement.next_target");
      if (next_target_it != instance.labels.end()) {
        std::cout << " next_target=" << next_target_it->second;
      }
      const auto victims_it = instance.labels.find("naim.preemption.victims");
      if (victims_it != instance.labels.end()) {
        std::cout << " preemption_victims=" << victims_it->second;
      }
      const auto defer_reason_it = instance.labels.find("naim.placement.defer_reason");
      if (defer_reason_it != instance.labels.end()) {
        std::cout << " defer_reason=" << defer_reason_it->second;
      }
    }
    std::cout << "\n";
  }
}

void ControllerPrintService::PrintDiskRuntimeStates(
    const std::vector<naim::DiskRuntimeState>& runtime_states) const {
  std::cout << "disk-runtime-state:\n";
  if (runtime_states.empty()) {
    std::cout << "  (empty)\n";
    return;
  }
  for (const auto& runtime_state : runtime_states) {
    std::cout << "  - disk=" << runtime_state.disk_name
              << " node=" << runtime_state.node_name
              << " state="
              << (runtime_state.runtime_state.empty() ? "(empty)" : runtime_state.runtime_state);
    if (!runtime_state.mount_point.empty()) {
      std::cout << " mount_point=" << runtime_state.mount_point;
    }
    if (!runtime_state.filesystem_type.empty()) {
      std::cout << " filesystem=" << runtime_state.filesystem_type;
    }
    if (!runtime_state.image_path.empty()) {
      std::cout << " image=" << runtime_state.image_path;
    }
    if (!runtime_state.loop_device.empty()) {
      std::cout << " loop_device=" << runtime_state.loop_device;
    }
    if (!runtime_state.last_verified_at.empty()) {
      std::cout << " last_verified_at=" << runtime_state.last_verified_at;
    }
    std::cout << "\n";
    if (!runtime_state.status_message.empty()) {
      std::cout << "    message=" << runtime_state.status_message << "\n";
    }
  }
}

void ControllerPrintService::PrintDetailedDiskState(
    const naim::DesiredState& state,
    const std::vector<naim::DiskRuntimeState>& runtime_states,
    const std::vector<naim::HostObservation>& observations,
    const std::optional<std::string>& node_name) const {
  std::map<std::string, naim::DiskRuntimeState> runtime_by_key;
  for (const auto& runtime_state : runtime_states) {
    runtime_by_key.emplace(runtime_state.disk_name + "@" + runtime_state.node_name, runtime_state);
  }
  std::map<std::string, naim::DiskTelemetryRecord> telemetry_by_key;
  for (const auto& observation : observations) {
    const auto disk_telemetry = runtime_support_service_.ParseDiskTelemetry(observation);
    if (!disk_telemetry.has_value()) {
      continue;
    }
    for (const auto& item : disk_telemetry->items) {
      telemetry_by_key[item.disk_name + "@" + item.node_name] = item;
    }
  }

  std::cout << "disk-state:\n";
  bool printed = false;
  for (const auto& disk : state.disks) {
    if (node_name.has_value() && disk.node_name != *node_name) {
      continue;
    }
    printed = true;
    const std::string key = disk.name + "@" + disk.node_name;
    const auto runtime_it = runtime_by_key.find(key);
    std::cout << "  - disk=" << disk.name
              << " kind=" << naim::ToString(disk.kind)
              << " node=" << disk.node_name
              << " size_gb=" << disk.size_gb
              << " desired_host_path=" << disk.host_path
              << " desired_container_path=" << disk.container_path;
    if (runtime_it == runtime_by_key.end()) {
      std::cout << " realized_state=missing-runtime-state\n";
      continue;
    }
    const auto& runtime_state = runtime_it->second;
    std::cout << " realized_state="
              << (runtime_state.runtime_state.empty() ? "(empty)" : runtime_state.runtime_state);
    if (!runtime_state.mount_point.empty()) {
      std::cout << " mount_point=" << runtime_state.mount_point;
    }
    if (!runtime_state.filesystem_type.empty()) {
      std::cout << " filesystem=" << runtime_state.filesystem_type;
    }
    if (!runtime_state.image_path.empty()) {
      std::cout << " image=" << runtime_state.image_path;
    }
    if (!runtime_state.loop_device.empty()) {
      std::cout << " loop_device=" << runtime_state.loop_device;
    }
    if (!runtime_state.last_verified_at.empty()) {
      std::cout << " last_verified_at=" << runtime_state.last_verified_at;
    }
    const auto telemetry_it = telemetry_by_key.find(key);
    if (telemetry_it != telemetry_by_key.end()) {
      std::cout << " usage_bytes=" << telemetry_it->second.used_bytes
                << "/" << telemetry_it->second.total_bytes
                << " free_bytes=" << telemetry_it->second.free_bytes
                << " read_bytes=" << telemetry_it->second.read_bytes
                << " write_bytes=" << telemetry_it->second.write_bytes
                << " read_ios=" << telemetry_it->second.read_ios
                << " write_ios=" << telemetry_it->second.write_ios
                << " io_time_ms=" << telemetry_it->second.io_time_ms
                << " fault_count=" << telemetry_it->second.fault_count
                << " warning_count=" << telemetry_it->second.warning_count
                << " perf_counters="
                << (telemetry_it->second.perf_counters_available ? "yes" : "no")
                << " io_error_counters="
                << (telemetry_it->second.io_error_counters_available ? "yes" : "no")
                << " mount_health="
                << (telemetry_it->second.health.empty() ? "(empty)" : telemetry_it->second.health);
    }
    std::cout << "\n";
    if (!runtime_state.status_message.empty()) {
      std::cout << "    message=" << runtime_state.status_message << "\n";
    }
  }

  for (const auto& runtime_state : runtime_states) {
    if (node_name.has_value() && runtime_state.node_name != *node_name) {
      continue;
    }
    const std::string key = runtime_state.disk_name + "@" + runtime_state.node_name;
    bool found_in_desired = false;
    for (const auto& disk : state.disks) {
      if (disk.name + "@" + disk.node_name == key) {
        found_in_desired = true;
        break;
      }
    }
    if (found_in_desired) {
      continue;
    }
    printed = true;
    std::cout << "  - disk=" << runtime_state.disk_name
              << " node=" << runtime_state.node_name
              << " realized_state="
              << (runtime_state.runtime_state.empty() ? "(empty)" : runtime_state.runtime_state)
              << " desired_state=(orphan-runtime-state)";
    if (!runtime_state.mount_point.empty()) {
      std::cout << " mount_point=" << runtime_state.mount_point;
    }
    if (!runtime_state.image_path.empty()) {
      std::cout << " image=" << runtime_state.image_path;
    }
    if (!runtime_state.loop_device.empty()) {
      std::cout << " loop_device=" << runtime_state.loop_device;
    }
    const auto telemetry_it = telemetry_by_key.find(key);
    if (telemetry_it != telemetry_by_key.end()) {
      std::cout << " usage_bytes=" << telemetry_it->second.used_bytes
                << "/" << telemetry_it->second.total_bytes
                << " free_bytes=" << telemetry_it->second.free_bytes
                << " read_bytes=" << telemetry_it->second.read_bytes
                << " write_bytes=" << telemetry_it->second.write_bytes
                << " read_ios=" << telemetry_it->second.read_ios
                << " write_ios=" << telemetry_it->second.write_ios
                << " io_time_ms=" << telemetry_it->second.io_time_ms
                << " fault_count=" << telemetry_it->second.fault_count
                << " warning_count=" << telemetry_it->second.warning_count
                << " perf_counters="
                << (telemetry_it->second.perf_counters_available ? "yes" : "no")
                << " io_error_counters="
                << (telemetry_it->second.io_error_counters_available ? "yes" : "no")
                << " mount_health="
                << (telemetry_it->second.health.empty() ? "(empty)" : telemetry_it->second.health);
    }
    std::cout << "\n";
    if (!runtime_state.status_message.empty()) {
      std::cout << "    message=" << runtime_state.status_message << "\n";
    }
  }
  if (!printed) {
    std::cout << "  (empty)\n";
  }
}

void ControllerPrintService::PrintSchedulerDecisionSummary(
    const naim::DesiredState& state) const {
  bool has_decisions = false;
  for (const auto& instance : state.instances) {
    if (instance.role != naim::InstanceRole::Worker) {
      continue;
    }
    if (instance.labels.find("naim.placement.decision") == instance.labels.end()) {
      continue;
    }
    if (!has_decisions) {
      std::cout << "scheduler-decisions:\n";
      has_decisions = true;
    }
    std::cout << "  - worker=" << instance.name;
    const auto decision_it = instance.labels.find("naim.placement.decision");
    if (decision_it != instance.labels.end()) {
      std::cout << " decision=" << decision_it->second;
    }
    const auto next_action_it = instance.labels.find("naim.placement.next_action");
    if (next_action_it != instance.labels.end()) {
      std::cout << " next_action=" << next_action_it->second;
    }
    const auto next_target_it = instance.labels.find("naim.placement.next_target");
    if (next_target_it != instance.labels.end()) {
      std::cout << " next_target=" << next_target_it->second;
    }
    const auto victims_it = instance.labels.find("naim.preemption.victims");
    if (victims_it != instance.labels.end()) {
      std::cout << " victims=" << victims_it->second;
    }
    const auto defer_reason_it = instance.labels.find("naim.placement.defer_reason");
    if (defer_reason_it != instance.labels.end()) {
      std::cout << " defer_reason=" << defer_reason_it->second;
    }
    std::cout << "\n";
  }
}

void ControllerPrintService::PrintRolloutGateSummary(
    const naim::SchedulingPolicyReport& scheduling_report) const {
  if (scheduling_report.rollout_actions.empty()) {
    return;
  }
  std::set<std::string> worker_names;
  std::set<std::string> node_names;
  for (const auto& action : scheduling_report.rollout_actions) {
    if (!action.worker_name.empty()) {
      worker_names.insert(action.worker_name);
    }
    if (!action.target_node_name.empty()) {
      node_names.insert(action.target_node_name);
    }
  }
  std::cout << "rollout-gates:\n";
  std::cout << "  gated_workers=" << worker_names.size()
            << " gated_nodes=" << node_names.size()
            << " deferred_actions=" << scheduling_report.rollout_actions.size() << "\n";
}

void ControllerPrintService::PrintPersistedRolloutActions(
    const std::vector<naim::RolloutActionRecord>& actions) const {
  std::cout << "rollout-actions:\n";
  if (actions.empty()) {
    std::cout << "  (empty)\n";
    return;
  }
  for (const auto& action : actions) {
    std::cout << "  - id=" << action.id
              << " generation=" << action.desired_generation
              << " step=" << action.step
              << " worker=" << action.worker_name
              << " action=" << action.action
              << " target=" << action.target_node_name << ":" << action.target_gpu_device
              << " status=" << naim::ToString(action.status);
    if (!action.victim_worker_names.empty()) {
      std::cout << " victims=";
      for (std::size_t index = 0; index < action.victim_worker_names.size(); ++index) {
        if (index > 0) {
          std::cout << ",";
        }
        std::cout << action.victim_worker_names[index];
      }
    }
    if (!action.reason.empty()) {
      std::cout << " reason=" << action.reason;
    }
    std::cout << "\n";
    if (!action.status_message.empty()) {
      std::cout << "    message=" << action.status_message << "\n";
    }
  }
}

void ControllerPrintService::PrintNodeAvailabilityOverrides(
    const std::vector<naim::NodeAvailabilityOverride>& availability_overrides) const {
  std::cout << "node-availability:\n";
  if (availability_overrides.empty()) {
    std::cout << "  (empty)\n";
    return;
  }
  for (const auto& availability_override : availability_overrides) {
    std::cout << "  - node=" << availability_override.node_name
              << " availability=" << naim::ToString(availability_override.availability)
              << " updated_at="
              << FormatDisplayTimestamp(availability_override.updated_at)
              << "\n";
    if (!availability_override.status_message.empty()) {
      std::cout << "    message=" << availability_override.status_message << "\n";
    }
  }
}

void ControllerPrintService::PrintAssignmentDispatchSummary(
    const naim::DesiredState& desired_state,
    const std::map<std::string, naim::NodeAvailabilityOverride>& availability_overrides,
    const std::vector<naim::HostObservation>& observations,
    int stale_after_seconds) const {
  std::size_t schedulable_nodes = 0;
  std::vector<std::string> skipped_nodes;
  for (const auto& node : desired_state.nodes) {
    const auto availability =
        runtime_support_service_.ResolveNodeAvailability(availability_overrides, node.name);
    if (!IsNodeSchedulable(availability)) {
      skipped_nodes.push_back(node.name + "(" + naim::ToString(availability) + ")");
      continue;
    }
    const auto observed_gate_reason =
        ObservedSchedulingGateReason(observations, node.name, stale_after_seconds);
    if (observed_gate_reason.has_value()) {
      skipped_nodes.push_back(node.name + "(" + *observed_gate_reason + ")");
      continue;
    }
    ++schedulable_nodes;
  }
  std::cout << "assignment-dispatch:\n";
  std::cout << "  schedulable_nodes=" << schedulable_nodes << "/"
            << desired_state.nodes.size() << "\n";
  if (!skipped_nodes.empty()) {
    std::cout << "  skipped_nodes=";
    for (std::size_t index = 0; index < skipped_nodes.size(); ++index) {
      if (index > 0) {
        std::cout << ",";
      }
      std::cout << skipped_nodes[index];
    }
    std::cout << "\n";
  }
}

void ControllerPrintService::PrintHostAssignments(
    const std::vector<naim::HostAssignment>& assignments) const {
  std::cout << "host-assignments:\n";
  if (assignments.empty()) {
    std::cout << "  (empty)\n";
    return;
  }
  for (const auto& assignment : assignments) {
    std::size_t instance_count = 0;
    try {
      const auto desired_node_state =
          naim::DeserializeDesiredStateJson(assignment.desired_state_json);
      instance_count = desired_node_state.instances.size();
    } catch (const std::exception&) {
      instance_count = 0;
    }
    std::cout << "  - id=" << assignment.id
              << " node=" << assignment.node_name
              << " plane=" << assignment.plane_name
              << " generation=" << assignment.desired_generation
              << " attempts=" << assignment.attempt_count << "/" << assignment.max_attempts
              << " type=" << assignment.assignment_type
              << " status=" << naim::ToString(assignment.status)
              << " instances=" << instance_count
              << " artifacts_root=" << assignment.artifacts_root << "\n";
    if (!assignment.status_message.empty()) {
      std::cout << "    message=" << assignment.status_message << "\n";
    }
  }
}

void ControllerPrintService::PrintHostObservations(
    const std::vector<naim::HostObservation>& observations,
    int stale_after_seconds) const {
  std::cout << "host-observations:\n";
  if (observations.empty()) {
    std::cout << "  (empty)\n";
    return;
  }
  for (const auto& observation : observations) {
    std::size_t disk_count = 0;
    std::size_t instance_count = 0;
    if (!observation.observed_state_json.empty()) {
      const auto observed_state =
          naim::DeserializeDesiredStateJson(observation.observed_state_json);
      disk_count = observed_state.disks.size();
      instance_count = observed_state.instances.size();
    }
    const auto runtime_status = runtime_support_service_.ParseRuntimeStatus(observation);
    const auto instance_statuses =
        runtime_support_service_.ParseInstanceRuntimeStatuses(observation);
    const auto gpu_telemetry = runtime_support_service_.ParseGpuTelemetry(observation);
    const auto disk_telemetry = runtime_support_service_.ParseDiskTelemetry(observation);
    const auto network_telemetry = runtime_support_service_.ParseNetworkTelemetry(observation);
    const auto cpu_telemetry = runtime_support_service_.ParseCpuTelemetry(observation);
    const auto age_seconds = runtime_support_service_.HeartbeatAgeSeconds(observation.heartbeat_at);

    std::cout << "  - node=" << observation.node_name
              << " plane=" << (observation.plane_name.empty() ? "(none)" : observation.plane_name)
              << " status=" << naim::ToString(observation.status);
    if (observation.applied_generation.has_value()) {
      std::cout << " applied_generation=" << *observation.applied_generation;
    }
    if (observation.last_assignment_id.has_value()) {
      std::cout << " last_assignment_id=" << *observation.last_assignment_id;
    }
    std::cout << " disks=" << disk_count
              << " instances=" << instance_count
              << " heartbeat_at="
              << FormatDisplayTimestamp(observation.heartbeat_at);
    if (age_seconds.has_value()) {
      std::cout << " age_seconds=" << *age_seconds
                << " health="
                << runtime_support_service_.HealthFromAge(age_seconds, stale_after_seconds);
    }
    if (runtime_status.has_value()) {
      std::cout << " runtime_backend="
                << (runtime_status->runtime_backend.empty()
                        ? "(empty)"
                        : runtime_status->runtime_backend)
                << " runtime_phase="
                << (runtime_status->runtime_phase.empty()
                        ? "(empty)"
                        : runtime_status->runtime_phase)
                << " runtime_launch_ready="
                << (runtime_status->launch_ready ? "yes" : "no")
                << " runtime_model="
                << (runtime_status->active_model_id.empty()
                        ? "(empty)"
                        : runtime_status->active_model_id)
                << " gateway="
                << (runtime_status->gateway_listen.empty()
                        ? "(empty)"
                        : runtime_status->gateway_listen);
    }
    if (gpu_telemetry.has_value()) {
      std::cout << " telemetry_source="
                << (gpu_telemetry->source.empty() ? "(empty)" : gpu_telemetry->source)
                << " telemetry_degraded=" << (gpu_telemetry->degraded ? "yes" : "no")
                << " gpu_devices=" << gpu_telemetry->devices.size();
    }
    if (disk_telemetry.has_value()) {
      std::cout << " disk_telemetry_source="
                << (disk_telemetry->source.empty() ? "(empty)" : disk_telemetry->source)
                << " disk_count=" << disk_telemetry->items.size();
      std::uint64_t total_read_bytes = 0;
      std::uint64_t total_write_bytes = 0;
      int total_fault_count = 0;
      int total_warning_count = 0;
      for (const auto& disk : disk_telemetry->items) {
        total_read_bytes += disk.read_bytes;
        total_write_bytes += disk.write_bytes;
        total_fault_count += disk.fault_count;
        total_warning_count += disk.warning_count;
      }
      std::cout << " disk_read_bytes=" << total_read_bytes
                << " disk_write_bytes=" << total_write_bytes
                << " disk_faults=" << total_fault_count
                << " disk_warnings=" << total_warning_count;
    }
    if (network_telemetry.has_value()) {
      std::cout << " network_telemetry_source="
                << (network_telemetry->source.empty() ? "(empty)" : network_telemetry->source)
                << " net_ifaces=" << network_telemetry->interfaces.size();
    }
    if (cpu_telemetry.has_value()) {
      std::cout << " cpu_telemetry_source="
                << (cpu_telemetry->source.empty() ? "(empty)" : cpu_telemetry->source)
                << " cpu_utilization_pct=" << static_cast<int>(cpu_telemetry->utilization_pct)
                << " cpu_cores=" << cpu_telemetry->core_count;
    }
    if (!instance_statuses.empty()) {
      std::cout << " instance_runtimes=" << instance_statuses.size();
    }
    std::cout << "\n";
    if (!observation.status_message.empty()) {
      std::cout << "    message=" << observation.status_message << "\n";
    }
    if (runtime_status.has_value()) {
      std::cout << "    runtime aliases=";
      if (runtime_status->aliases.empty()) {
        std::cout << "(empty)";
      } else {
        for (std::size_t index = 0; index < runtime_status->aliases.size(); ++index) {
          if (index > 0) {
            std::cout << ",";
          }
          std::cout << runtime_status->aliases[index];
        }
      }
      std::cout << " runtime_profile="
                << (runtime_status->active_runtime_profile.empty()
                        ? "(empty)"
                        : runtime_status->active_runtime_profile)
                << " inference_ready=" << (runtime_status->inference_ready ? "yes" : "no")
                << " gateway_ready=" << (runtime_status->gateway_ready ? "yes" : "no")
                << "\n";
    }
    if (gpu_telemetry.has_value()) {
      for (const auto& device : gpu_telemetry->devices) {
        std::cout << "    gpu device=" << device.gpu_device
                  << " used_vram_mb=" << device.used_vram_mb
                  << "/" << device.total_vram_mb
                  << " free_vram_mb=" << device.free_vram_mb
                  << " util_pct=" << device.gpu_utilization_pct;
        if (!device.processes.empty()) {
          std::cout << " processes=";
          for (std::size_t index = 0; index < device.processes.size(); ++index) {
            if (index > 0) {
              std::cout << ",";
            }
            std::cout << device.processes[index].instance_name
                      << ":" << device.processes[index].pid
                      << ":" << device.processes[index].used_vram_mb << "MB";
          }
        }
        std::cout << "\n";
      }
    }
    if (disk_telemetry.has_value()) {
      for (const auto& disk : disk_telemetry->items) {
        std::cout << "    disk name=" << disk.disk_name
                  << " phase="
                  << (disk.runtime_state.empty() ? "(empty)" : disk.runtime_state)
                  << " mounted=" << (disk.mounted ? "yes" : "no")
                  << " health=" << (disk.health.empty() ? "(empty)" : disk.health)
                  << " used_bytes=" << disk.used_bytes
                  << " free_bytes=" << disk.free_bytes
                  << " read_bytes=" << disk.read_bytes
                  << " write_bytes=" << disk.write_bytes
                  << " read_ios=" << disk.read_ios
                  << " write_ios=" << disk.write_ios
                  << " io_time_ms=" << disk.io_time_ms
                  << " io_in_progress=" << disk.io_in_progress
                  << " fault_count=" << disk.fault_count
                  << " warning_count=" << disk.warning_count
                  << " perf_counters=" << (disk.perf_counters_available ? "yes" : "no")
                  << " io_error_counters="
                  << (disk.io_error_counters_available ? "yes" : "no")
                  << " read_only=" << (disk.read_only ? "yes" : "no");
        if (!disk.mount_point.empty()) {
          std::cout << " mount_point=" << disk.mount_point;
        }
        if (!disk.mount_source.empty()) {
          std::cout << " mount_source=" << disk.mount_source;
        }
        if (!disk.filesystem_type.empty()) {
          std::cout << " filesystem=" << disk.filesystem_type;
        }
        if (!disk.fault_reasons.empty()) {
          std::cout << " faults=";
          for (std::size_t index = 0; index < disk.fault_reasons.size(); ++index) {
            if (index > 0) {
              std::cout << ",";
            }
            std::cout << disk.fault_reasons[index];
          }
        }
        std::cout << "\n";
      }
    }
    if (network_telemetry.has_value()) {
      for (const auto& interface : network_telemetry->interfaces) {
        std::cout << "    net iface=" << interface.interface_name
                  << " oper_state="
                  << (interface.oper_state.empty() ? "(empty)" : interface.oper_state)
                  << " link_state="
                  << (interface.link_state.empty() ? "(empty)" : interface.link_state)
                  << " rx_bytes=" << interface.rx_bytes
                  << " tx_bytes=" << interface.tx_bytes
                  << " loopback=" << (interface.loopback ? "yes" : "no")
                  << "\n";
      }
    }
    if (cpu_telemetry.has_value()) {
      std::cout << "    cpu loadavg="
                << cpu_telemetry->loadavg_1m << ","
                << cpu_telemetry->loadavg_5m << ","
                << cpu_telemetry->loadavg_15m
                << " mem_used_bytes=" << cpu_telemetry->used_memory_bytes
                << " mem_total_bytes=" << cpu_telemetry->total_memory_bytes
                << " degraded=" << (cpu_telemetry->degraded ? "yes" : "no")
                << "\n";
    }
    if (!instance_statuses.empty()) {
      for (const auto& instance_status : instance_statuses) {
        std::cout << "    instance name=" << instance_status.instance_name
                  << " role=" << instance_status.instance_role
                  << " phase=" << instance_status.runtime_phase
                  << " ready=" << (instance_status.ready ? "yes" : "no")
                  << " pid=" << instance_status.runtime_pid
                  << " gpu="
                  << (instance_status.gpu_device.empty() ? "(empty)" : instance_status.gpu_device);
        if (!instance_status.model_path.empty()) {
          std::cout << " model_path=" << instance_status.model_path;
        }
        std::cout << "\n";
      }
    }
  }
}

void ControllerPrintService::PrintHostHealth(
    const std::optional<naim::DesiredState>& desired_state,
    const std::vector<naim::HostObservation>& observations,
    const std::vector<naim::NodeAvailabilityOverride>& availability_overrides,
    const std::optional<std::string>& node_name,
    int stale_after_seconds) const {
  std::map<std::string, naim::HostObservation> observation_by_node;
  for (const auto& observation : observations) {
    observation_by_node.emplace(observation.node_name, observation);
  }
  const auto availability_override_map =
      runtime_support_service_.BuildAvailabilityOverrideMap(availability_overrides);

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

  std::cout << "host-health:\n";
  if (nodes.empty()) {
    std::cout << "  (empty)\n";
    return;
  }

  int online_count = 0;
  int stale_count = 0;
  int unknown_count = 0;
  for (const auto& current_node_name : nodes) {
    const auto observation_it = observation_by_node.find(current_node_name);
    if (observation_it == observation_by_node.end()) {
      std::cout << "  - node=" << current_node_name
                << " availability="
                << naim::ToString(
                       runtime_support_service_.ResolveNodeAvailability(
                           availability_override_map, current_node_name))
                << " health=unknown status=(none)\n";
      ++unknown_count;
      continue;
    }

    const auto age_seconds =
        runtime_support_service_.HeartbeatAgeSeconds(observation_it->second.heartbeat_at);
    const std::string health =
        runtime_support_service_.HealthFromAge(age_seconds, stale_after_seconds);
    const auto runtime_status =
        runtime_support_service_.ParseRuntimeStatus(observation_it->second);
    const auto gpu_telemetry =
        runtime_support_service_.ParseGpuTelemetry(observation_it->second);
    if (health == "online") {
      ++online_count;
    } else if (health == "stale") {
      ++stale_count;
    } else {
      ++unknown_count;
    }

    std::cout << "  - node=" << current_node_name
              << " availability="
              << naim::ToString(
                     runtime_support_service_.ResolveNodeAvailability(
                         availability_override_map, current_node_name))
              << " health=" << health
              << " status=" << naim::ToString(observation_it->second.status);
    if (observation_it->second.applied_generation.has_value()) {
      std::cout << " applied_generation=" << *observation_it->second.applied_generation;
    }
    if (age_seconds.has_value()) {
      std::cout << " age_seconds=" << *age_seconds;
    }
    if (observation_it->second.last_assignment_id.has_value()) {
      std::cout << " last_assignment_id=" << *observation_it->second.last_assignment_id;
    }
    if (runtime_status.has_value()) {
      std::cout << " runtime_backend="
                << (runtime_status->runtime_backend.empty()
                        ? "(empty)"
                        : runtime_status->runtime_backend)
                << " runtime_phase="
                << (runtime_status->runtime_phase.empty()
                        ? "(empty)"
                        : runtime_status->runtime_phase)
                << " runtime_launch_ready="
                << (runtime_status->launch_ready ? "yes" : "no")
                << " runtime_model="
                << (runtime_status->active_model_id.empty()
                        ? "(empty)"
                        : runtime_status->active_model_id);
    }
    if (gpu_telemetry.has_value()) {
      std::cout << " telemetry="
                << (gpu_telemetry->degraded ? "degraded" : "ok")
                << ":" << (gpu_telemetry->source.empty() ? "unknown" : gpu_telemetry->source);
    }
    std::cout << "\n";
    if (!observation_it->second.status_message.empty()) {
      std::cout << "    message=" << observation_it->second.status_message << "\n";
    }
  }

  std::cout << "summary: online=" << online_count
            << " stale=" << stale_count
            << " unknown=" << unknown_count << "\n";
}

void ControllerPrintService::PrintEvents(
    const std::vector<naim::EventRecord>& events) const {
  std::cout << "events:\n";
  if (events.empty()) {
    std::cout << "  (empty)\n";
    return;
  }
  for (const auto& event : events) {
    std::cout << "  - id=" << event.id
              << " category=" << event.category
              << " type=" << event.event_type
              << " severity=" << event.severity;
    if (!event.plane_name.empty()) {
      std::cout << " plane=" << event.plane_name;
    }
    if (!event.node_name.empty()) {
      std::cout << " node=" << event.node_name;
    }
    if (!event.worker_name.empty()) {
      std::cout << " worker=" << event.worker_name;
    }
    if (event.assignment_id.has_value()) {
      std::cout << " assignment_id=" << *event.assignment_id;
    }
    if (event.rollout_action_id.has_value()) {
      std::cout << " rollout_action_id=" << *event.rollout_action_id;
    }
    std::cout << " at=" << FormatDisplayTimestamp(event.created_at)
              << " message="
              << (event.message.empty() ? "(empty)" : event.message)
              << "\n";
  }
}

bool ControllerPrintService::IsNodeSchedulable(naim::NodeAvailability availability) const {
  return availability == naim::NodeAvailability::Active;
}

std::optional<std::string> ControllerPrintService::ObservedSchedulingGateReason(
    const std::vector<naim::HostObservation>& observations,
    const std::string& node_name,
    int stale_after_seconds) const {
  const auto observation =
      runtime_support_service_.FindHostObservationForNode(observations, node_name);
  if (!observation.has_value()) {
    return std::nullopt;
  }
  if (observation->status == naim::HostObservationStatus::Failed) {
    return std::string("failed");
  }
  const auto age_seconds =
      runtime_support_service_.HeartbeatAgeSeconds(observation->heartbeat_at);
  if (runtime_support_service_.HealthFromAge(age_seconds, stale_after_seconds) == "stale") {
    return std::string("stale");
  }
  const auto runtime_status = runtime_support_service_.ParseRuntimeStatus(*observation);
  if (runtime_status.has_value() && runtime_status->runtime_phase == "failed") {
    return std::string("runtime-failed");
  }
  const auto gpu_telemetry = runtime_support_service_.ParseGpuTelemetry(*observation);
  if (gpu_telemetry.has_value() && gpu_telemetry->degraded) {
    return std::string("telemetry-degraded");
  }
  return std::nullopt;
}

}  // namespace naim::controller
