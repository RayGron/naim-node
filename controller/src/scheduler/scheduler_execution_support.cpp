#include "scheduler/scheduler_execution_support.h"

#include <algorithm>
#include <sstream>
#include <stdexcept>
#include <utility>

#include "naim/state/state_json.h"

namespace naim::controller {

namespace {

std::string RolloutActionTag(int action_id) {
  return "rollout_action_id=" + std::to_string(action_id);
}

bool AssignmentReferencesRolloutAction(
    const naim::HostAssignment& assignment,
    int action_id) {
  return assignment.status_message.find(RolloutActionTag(action_id)) != std::string::npos;
}

}  // namespace

SchedulerExecutionSupport::SchedulerExecutionSupport(
    std::shared_ptr<const SchedulerAssignmentQuerySupport> assignment_query_support,
    std::shared_ptr<const SchedulerVerificationSupport> verification_support,
    SchedulerExecutionVerificationConfig verification_config)
    : assignment_query_support_(std::move(assignment_query_support)),
      verification_support_(std::move(verification_support)),
      verification_config_(std::move(verification_config)) {}

std::optional<naim::RolloutActionRecord> SchedulerExecutionSupport::FindPriorRolloutActionForWorker(
    const std::vector<naim::RolloutActionRecord>& actions,
    const naim::RolloutActionRecord& action,
    const std::string& requested_action_name) const {
  std::optional<naim::RolloutActionRecord> result;
  for (const auto& candidate_action : actions) {
    if (candidate_action.desired_generation != action.desired_generation ||
        candidate_action.worker_name != action.worker_name ||
        candidate_action.step >= action.step ||
        candidate_action.action != requested_action_name) {
      continue;
    }
    result = candidate_action;
  }
  return result;
}

void SchedulerExecutionSupport::RemoveWorkerFromDesiredState(
    naim::DesiredState* state,
    const std::string& worker_name) const {
  if (state == nullptr) {
    return;
  }

  state->instances.erase(
      std::remove_if(
          state->instances.begin(),
          state->instances.end(),
          [&](const naim::InstanceSpec& instance) { return instance.name == worker_name; }),
      state->instances.end());
  state->runtime_gpu_nodes.erase(
      std::remove_if(
          state->runtime_gpu_nodes.begin(),
          state->runtime_gpu_nodes.end(),
          [&](const naim::RuntimeGpuNode& gpu_node) { return gpu_node.name == worker_name; }),
      state->runtime_gpu_nodes.end());
  state->disks.erase(
      std::remove_if(
          state->disks.begin(),
          state->disks.end(),
          [&](const naim::DiskSpec& disk) {
            return disk.kind == naim::DiskKind::WorkerPrivate &&
                   disk.owner_name == worker_name;
          }),
      state->disks.end());
  for (auto& instance : state->instances) {
    instance.depends_on.erase(
        std::remove(instance.depends_on.begin(), instance.depends_on.end(), worker_name),
        instance.depends_on.end());
  }
}

void SchedulerExecutionSupport::MaterializeRetryPlacementAction(
    naim::DesiredState* state,
    const naim::RolloutActionRecord& action,
    const std::vector<std::string>& victim_worker_names) const {
  if (state == nullptr) {
    return;
  }

  for (const auto& victim_worker_name : victim_worker_names) {
    RemoveWorkerFromDesiredState(state, victim_worker_name);
  }

  auto instance_it = std::find_if(
      state->instances.begin(),
      state->instances.end(),
      [&](const naim::InstanceSpec& instance) {
        return instance.role == naim::InstanceRole::Worker &&
               instance.name == action.worker_name;
      });
  if (instance_it == state->instances.end()) {
    throw std::runtime_error(
        "worker '" + action.worker_name + "' not found in desired state");
  }

  instance_it->node_name = action.target_node_name;
  instance_it->gpu_device = action.target_gpu_device;
  instance_it->share_mode = naim::GpuShareMode::Exclusive;
  instance_it->gpu_fraction = 1.0;
  instance_it->labels["naim.node"] = action.target_node_name;
  instance_it->labels["naim.placement"] = "auto";
  instance_it->labels["naim.placement.action"] = "materialized-retry-placement";
  instance_it->labels["naim.placement.decision"] = "applied";
  instance_it->labels.erase("naim.placement.next_action");
  instance_it->labels.erase("naim.placement.next_target");
  instance_it->labels.erase("naim.placement.defer_reason");
  instance_it->labels.erase("naim.preemption.victims");

  auto runtime_gpu_it = std::find_if(
      state->runtime_gpu_nodes.begin(),
      state->runtime_gpu_nodes.end(),
      [&](const naim::RuntimeGpuNode& gpu_node) {
        return gpu_node.name == action.worker_name;
      });
  if (runtime_gpu_it != state->runtime_gpu_nodes.end()) {
    runtime_gpu_it->node_name = action.target_node_name;
    runtime_gpu_it->gpu_device = action.target_gpu_device;
    runtime_gpu_it->share_mode = naim::GpuShareMode::Exclusive;
    runtime_gpu_it->gpu_fraction = 1.0;
  }

  auto disk_it = std::find_if(
      state->disks.begin(),
      state->disks.end(),
      [&](const naim::DiskSpec& disk) {
        return disk.kind == naim::DiskKind::WorkerPrivate &&
               disk.owner_name == action.worker_name;
      });
  if (disk_it != state->disks.end()) {
    disk_it->node_name = action.target_node_name;
  }
}

std::vector<naim::HostAssignment> SchedulerExecutionSupport::BuildEvictionAssignmentsForAction(
    const naim::DesiredState& desired_state,
    int desired_generation,
    const naim::RolloutActionRecord& action,
    const std::vector<naim::HostAssignment>& existing_assignments) const {
  if (action.action != "evict-best-effort") {
    throw std::runtime_error(
        "rollout action id=" + std::to_string(action.id) +
        " is not an evict-best-effort action");
  }
  if (action.victim_worker_names.empty()) {
    throw std::runtime_error(
        "rollout action id=" + std::to_string(action.id) +
        " has no victim workers to evict");
  }

  std::map<std::string, std::vector<std::string>> victim_workers_by_node;
  for (const auto& victim_worker_name : action.victim_worker_names) {
    bool found = false;
    for (const auto& instance : desired_state.instances) {
      if (instance.role == naim::InstanceRole::Worker &&
          instance.name == victim_worker_name) {
        victim_workers_by_node[instance.node_name].push_back(victim_worker_name);
        found = true;
        break;
      }
    }
    if (!found) {
      throw std::runtime_error(
          "victim worker '" + victim_worker_name +
          "' not found in desired state for rollout action id=" +
          std::to_string(action.id));
    }
  }

  naim::DesiredState eviction_state = desired_state;
  int required_memory_cap_mb = 0;
  for (const auto& instance : desired_state.instances) {
    if (instance.role == naim::InstanceRole::Worker &&
        instance.name == action.worker_name) {
      required_memory_cap_mb = instance.memory_cap_mb.value_or(0);
      break;
    }
  }
  for (const auto& victim_worker_name : action.victim_worker_names) {
    RemoveWorkerFromDesiredState(&eviction_state, victim_worker_name);
  }

  const auto plane_assignment =
      assignment_query_support_->FindLatestHostAssignmentForPlane(
          existing_assignments,
          desired_state.plane_name);
  std::vector<naim::HostAssignment> assignments;
  for (const auto& [node_name, victim_workers] : victim_workers_by_node) {
    naim::HostAssignment assignment;
    assignment.node_name = node_name;
    assignment.plane_name = desired_state.plane_name;
    assignment.desired_generation = desired_generation;
    assignment.assignment_type = "evict-workers";
    assignment.desired_state_json =
        naim::SerializeDesiredStateJson(
            naim::SliceDesiredStateForNode(eviction_state, node_name));
    const auto latest_assignment =
        assignment_query_support_->FindLatestHostAssignmentForNode(existing_assignments, node_name);
    assignment.artifacts_root = latest_assignment.has_value()
                                    ? latest_assignment->artifacts_root
                                    : (plane_assignment.has_value()
                                           ? plane_assignment->artifacts_root
                                           : assignment_query_support_->DefaultArtifactsRoot());
    assignment.status = naim::HostAssignmentStatus::Pending;
    std::ostringstream message;
    message << RolloutActionTag(action.id)
            << " evict workers for rollout worker=" << action.worker_name
            << " target_gpu=" << action.target_gpu_device
            << " required_memory_cap_mb=" << required_memory_cap_mb
            << " victims=";
    for (std::size_t index = 0; index < victim_workers.size(); ++index) {
      if (index > 0) {
        message << ",";
      }
      message << victim_workers[index];
    }
    assignment.status_message = message.str();
    assignments.push_back(std::move(assignment));
  }
  return assignments;
}

bool SchedulerExecutionSupport::AreRolloutEvictionAssignmentsApplied(
    const std::vector<naim::HostAssignment>& assignments,
    int action_id) const {
  bool found = false;
  for (const auto& assignment : assignments) {
    if (assignment.assignment_type != "evict-workers" ||
        !AssignmentReferencesRolloutAction(assignment, action_id)) {
      continue;
    }
    found = true;
    if (assignment.status != naim::HostAssignmentStatus::Applied) {
      return false;
    }
  }
  return found;
}

void SchedulerExecutionSupport::MaterializeRebalancePlanEntry(
    naim::DesiredState* state,
    const RebalancePlanEntry& entry) const {
  if (state == nullptr) {
    return;
  }

  auto instance_it = std::find_if(
      state->instances.begin(),
      state->instances.end(),
      [&](const naim::InstanceSpec& instance) {
        return instance.role == naim::InstanceRole::Worker &&
               instance.name == entry.worker_name;
      });
  if (instance_it == state->instances.end()) {
    throw std::runtime_error(
        "worker '" + entry.worker_name + "' not found in desired state");
  }

  instance_it->node_name = entry.target_node_name;
  instance_it->gpu_device = entry.target_gpu_device;
  instance_it->environment["NAIM_NODE_NAME"] = entry.target_node_name;
  if (!entry.target_gpu_device.empty()) {
    instance_it->environment["NAIM_GPU_DEVICE"] = entry.target_gpu_device;
  } else {
    instance_it->environment.erase("NAIM_GPU_DEVICE");
  }
  if (entry.action == "upgrade-to-exclusive") {
    instance_it->share_mode = naim::GpuShareMode::Exclusive;
    instance_it->gpu_fraction = 1.0;
  }
  instance_it->labels["naim.node"] = entry.target_node_name;
  instance_it->labels["naim.placement"] = "auto";
  instance_it->labels["naim.placement.action"] = "materialized-rebalance-" + entry.action;
  instance_it->labels["naim.placement.score"] = std::to_string(entry.score);
  instance_it->labels["naim.placement.decision"] = "applied";
  instance_it->labels.erase("naim.placement.next_action");
  instance_it->labels.erase("naim.placement.next_target");
  instance_it->labels.erase("naim.placement.defer_reason");
  if (!entry.victim_worker_names.empty()) {
    std::ostringstream victims;
    for (std::size_t index = 0; index < entry.victim_worker_names.size(); ++index) {
      if (index > 0) {
        victims << ",";
      }
      victims << entry.victim_worker_names[index];
    }
    instance_it->labels["naim.preemption.victims"] = victims.str();
  } else {
    instance_it->labels.erase("naim.preemption.victims");
  }

  auto runtime_gpu_it = std::find_if(
      state->runtime_gpu_nodes.begin(),
      state->runtime_gpu_nodes.end(),
      [&](const naim::RuntimeGpuNode& gpu_node) {
        return gpu_node.name == entry.worker_name;
      });
  if (runtime_gpu_it != state->runtime_gpu_nodes.end()) {
    runtime_gpu_it->node_name = entry.target_node_name;
    runtime_gpu_it->gpu_device = entry.target_gpu_device;
    if (entry.action == "upgrade-to-exclusive") {
      runtime_gpu_it->share_mode = naim::GpuShareMode::Exclusive;
      runtime_gpu_it->gpu_fraction = 1.0;
    }
  }

  auto disk_it = std::find_if(
      state->disks.begin(),
      state->disks.end(),
      [&](const naim::DiskSpec& disk) {
        return disk.kind == naim::DiskKind::WorkerPrivate &&
               disk.owner_name == entry.worker_name;
      });
  if (disk_it != state->disks.end()) {
    disk_it->node_name = entry.target_node_name;
  }
}

const naim::RuntimeProcessStatus* SchedulerExecutionSupport::FindInstanceRuntimeStatus(
    const std::vector<naim::RuntimeProcessStatus>& statuses,
    const std::string& instance_name,
    const std::string& gpu_device) const {
  for (const auto& status : statuses) {
    if (status.instance_name == instance_name && status.gpu_device == gpu_device) {
      return &status;
    }
  }
  return nullptr;
}

bool SchedulerExecutionSupport::TelemetryShowsOwnedProcess(
    const std::optional<naim::GpuTelemetrySnapshot>& telemetry,
    const std::string& gpu_device,
    const std::string& instance_name) const {
  if (!telemetry.has_value()) {
    return false;
  }
  for (const auto& device : telemetry->devices) {
    if (device.gpu_device != gpu_device) {
      continue;
    }
    for (const auto& process : device.processes) {
      if (process.instance_name == instance_name) {
        return true;
      }
    }
  }
  return false;
}

SchedulerVerificationResult SchedulerExecutionSupport::EvaluateSchedulerActionVerification(
    const naim::SchedulerPlaneRuntime& plane_runtime,
    const std::vector<naim::HostObservation>& observations) const {
  SchedulerVerificationResult result;
  const bool rollback_mode = plane_runtime.phase == "rollback-applied" ||
                             plane_runtime.phase == "rollback-planned";
  const std::string expected_node =
      rollback_mode ? plane_runtime.source_node_name : plane_runtime.target_node_name;
  const std::string expected_gpu =
      rollback_mode ? plane_runtime.source_gpu_device : plane_runtime.target_gpu_device;
  const std::string cleared_node =
      rollback_mode ? plane_runtime.target_node_name : plane_runtime.source_node_name;
  const std::string cleared_gpu =
      rollback_mode ? plane_runtime.target_gpu_device : plane_runtime.source_gpu_device;

  const auto target_observation =
      verification_support_->FindHostObservationForNode(observations, expected_node);
  const auto source_observation =
      verification_support_->FindHostObservationForNode(observations, cleared_node);
  if (!target_observation.has_value()) {
    result.detail = "missing-target-observation";
  } else {
    const auto target_runtimes =
        verification_support_->ParseInstanceRuntimeStatuses(*target_observation);
    const auto target_runtime = FindInstanceRuntimeStatus(
        target_runtimes,
        plane_runtime.active_worker_name,
        expected_gpu);
    const auto target_telemetry = verification_support_->ParseGpuTelemetry(*target_observation);
    const bool target_generation_applied =
        target_observation->applied_generation.has_value() &&
        *target_observation->applied_generation >= plane_runtime.action_generation;
    const bool target_runtime_ready =
        target_runtime != nullptr &&
        target_runtime->ready &&
        (target_runtime->runtime_phase == "running" ||
         target_runtime->runtime_phase == "ready" ||
         target_runtime->runtime_phase == "loaded");
    const bool target_gpu_owned =
        TelemetryShowsOwnedProcess(
            target_telemetry,
            expected_gpu,
            plane_runtime.active_worker_name);

    bool source_cleared = true;
    if (source_observation.has_value()) {
      const auto source_runtimes =
          verification_support_->ParseInstanceRuntimeStatuses(*source_observation);
      const auto source_runtime = FindInstanceRuntimeStatus(
          source_runtimes,
          plane_runtime.active_worker_name,
          cleared_gpu);
      const auto source_telemetry =
          verification_support_->ParseGpuTelemetry(*source_observation);
      source_cleared =
          source_runtime == nullptr &&
          !TelemetryShowsOwnedProcess(
              source_telemetry,
              cleared_gpu,
              plane_runtime.active_worker_name);
    }

      result.converged =
        target_generation_applied && target_runtime_ready && target_gpu_owned && source_cleared;
    if (result.converged) {
      result.next_stable_samples = plane_runtime.stable_samples + 1;
      result.stable =
          result.next_stable_samples >=
          verification_config_.verification_stable_samples_required;
      result.detail = "verified-sample";
    } else {
      result.next_stable_samples = 0;
      std::ostringstream detail;
      detail << "target_generation_applied=" << (target_generation_applied ? "yes" : "no")
             << " target_runtime_ready=" << (target_runtime_ready ? "yes" : "no")
             << " target_gpu_owned=" << (target_gpu_owned ? "yes" : "no")
             << " source_cleared=" << (source_cleared ? "yes" : "no");
      result.detail = detail.str();
    }
  }

  const auto action_age =
      verification_support_->TimestampAgeSeconds(plane_runtime.started_at);
  result.timed_out =
      action_age.has_value() &&
      *action_age >= verification_config_.verification_timeout_seconds;
  return result;
}

void SchedulerExecutionSupport::MarkWorkerMoveVerified(
    naim::ControllerStore* store,
    const naim::SchedulerPlaneRuntime& plane_runtime) const {
  if (store == nullptr) {
    return;
  }
  const std::string now = verification_support_->UtcNowSqlTimestamp();
  naim::SchedulerWorkerRuntime worker_runtime;
  if (const auto current = store->LoadSchedulerWorkerRuntime(plane_runtime.active_worker_name);
      current.has_value()) {
    worker_runtime = *current;
  }
  worker_runtime.plane_name = plane_runtime.plane_name;
  worker_runtime.worker_name = plane_runtime.active_worker_name;
  worker_runtime.last_move_at = now;
  worker_runtime.last_verified_generation = plane_runtime.action_generation;
  worker_runtime.last_scheduler_phase = "verified";
  worker_runtime.last_status_message =
      plane_runtime.phase == "rollback-applied"
          ? "rollback verification succeeded"
          : "move verification succeeded";
  worker_runtime.manual_intervention_required = false;
  store->UpsertSchedulerWorkerRuntime(worker_runtime);

  for (const auto& node_name : {plane_runtime.source_node_name, plane_runtime.target_node_name}) {
    if (node_name.empty()) {
      continue;
    }
    naim::SchedulerNodeRuntime node_runtime;
    if (const auto current = store->LoadSchedulerNodeRuntime(node_name); current.has_value()) {
      node_runtime = *current;
    }
    node_runtime.plane_name = plane_runtime.plane_name;
    node_runtime.node_name = node_name;
    node_runtime.last_move_at = now;
    node_runtime.last_verified_generation = plane_runtime.action_generation;
    store->UpsertSchedulerNodeRuntime(node_runtime);
  }
}

void SchedulerExecutionSupport::MarkWorkersEvicted(
    naim::ControllerStore* store,
    const std::string& plane_name,
    const std::vector<std::string>& worker_names) const {
  if (store == nullptr) {
    return;
  }
  const std::string now = verification_support_->UtcNowSqlTimestamp();
  for (const auto& worker_name : worker_names) {
    if (worker_name.empty()) {
      continue;
    }
    naim::SchedulerWorkerRuntime runtime;
    if (const auto current = store->LoadSchedulerWorkerRuntime(worker_name); current.has_value()) {
      runtime = *current;
    }
    runtime.plane_name = plane_name;
    runtime.worker_name = worker_name;
    runtime.last_eviction_at = now;
    runtime.last_scheduler_phase = "evicted";
    runtime.last_status_message = "eviction verified";
    store->UpsertSchedulerWorkerRuntime(runtime);
  }
}

}  // namespace naim::controller
