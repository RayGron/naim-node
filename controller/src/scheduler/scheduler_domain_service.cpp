#include "scheduler/scheduler_domain_service.h"

#include <algorithm>
#include <set>
#include <stdexcept>
#include <utility>

namespace naim::controller {

namespace {

std::optional<naim::HostAssignment> FindLatestHostAssignmentForNodeGeneration(
    const std::vector<naim::HostAssignment>& assignments,
    const std::string& node_name,
    int desired_generation) {
  std::optional<naim::HostAssignment> result;
  for (const auto& assignment : assignments) {
    if (assignment.node_name != node_name ||
        assignment.desired_generation != desired_generation) {
      continue;
    }
    result = assignment;
  }
  return result;
}

std::optional<naim::HostObservation> FindHostObservationForNode(
    const std::vector<naim::HostObservation>& observations,
    const std::string& node_name) {
  for (const auto& observation : observations) {
    if (observation.node_name == node_name) {
      return observation;
    }
  }
  return std::nullopt;
}

bool AssignmentReferencesRolloutAction(
    const naim::HostAssignment& assignment,
    int action_id) {
  return assignment.status_message.find(
             "rollout_action_id=" + std::to_string(action_id)) != std::string::npos;
}

bool HasRolloutEvictionAssignments(
    const std::vector<naim::HostAssignment>& assignments,
    int action_id) {
  for (const auto& assignment : assignments) {
    if (assignment.assignment_type == "evict-workers" &&
        AssignmentReferencesRolloutAction(assignment, action_id)) {
      return true;
    }
  }
  return false;
}

bool AreRolloutEvictionAssignmentsApplied(
    const std::vector<naim::HostAssignment>& assignments,
    int action_id) {
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

std::optional<RolloutLifecycleEntry> FindRolloutLifecycleEntry(
    const std::vector<RolloutLifecycleEntry>& entries,
    const std::string& worker_name) {
  for (const auto& entry : entries) {
    if (entry.worker_name == worker_name) {
      return entry;
    }
  }
  return std::nullopt;
}

bool RolloutPhaseBlocksRebalance(SchedulerRolloutPhase phase) {
  return phase != SchedulerRolloutPhase::RolloutApplied;
}

bool HostAssignmentBlocksRebalance(const naim::HostAssignment& assignment) {
  return assignment.status == naim::HostAssignmentStatus::Pending ||
         assignment.status == naim::HostAssignmentStatus::Claimed;
}

bool NodeHasBlockingHostAssignment(
    const std::vector<naim::HostAssignment>& assignments,
    const std::string& node_name) {
  for (const auto& assignment : assignments) {
    if (assignment.node_name == node_name &&
        HostAssignmentBlocksRebalance(assignment)) {
      return true;
    }
  }
  return false;
}

const naim::InstanceSpec* FindWorkerInstance(
    const naim::DesiredState& state,
    const std::string& worker_name) {
  for (const auto& instance : state.instances) {
    if (instance.role == naim::InstanceRole::Worker && instance.name == worker_name) {
      return &instance;
    }
  }
  return nullptr;
}

std::optional<naim::GpuDeviceTelemetry> FindObservedGpuDeviceTelemetry(
    const std::vector<naim::HostObservation>& observations,
    const std::string& node_name,
    const std::string& gpu_device,
    const SchedulerDomainSupport& domain_support) {
  const auto observation = FindHostObservationForNode(observations, node_name);
  if (!observation.has_value()) {
    return std::nullopt;
  }
  const auto telemetry = domain_support.ParseGpuTelemetry(*observation);
  if (!telemetry.has_value()) {
    return std::nullopt;
  }
  for (const auto& device : telemetry->devices) {
    if (device.gpu_device == gpu_device) {
      return device;
    }
  }
  return std::nullopt;
}

bool ObservedGpuDeviceHasForeignProcess(
    const std::vector<naim::HostObservation>& observations,
    const std::string& node_name,
    const std::string& gpu_device,
    const std::string& worker_name,
    const SchedulerDomainSupport& domain_support) {
  const auto device =
      FindObservedGpuDeviceTelemetry(observations, node_name, gpu_device, domain_support);
  if (!device.has_value()) {
    return false;
  }
  for (const auto& process : device->processes) {
    if (process.instance_name != worker_name && process.instance_name != "unknown") {
      return true;
    }
  }
  return false;
}

std::optional<std::string> ObservedGpuPlacementGateReason(
    const std::vector<naim::HostObservation>& observations,
    const naim::InstanceSpec& worker,
    const std::string& target_node_name,
    const std::string& target_gpu_device,
    bool moving_to_different_gpu,
    const SchedulerDomainSupport& domain_support,
    const SchedulerDomainPolicyConfig& policy_config) {
  const auto device =
      FindObservedGpuDeviceTelemetry(
          observations,
          target_node_name,
          target_gpu_device,
          domain_support);
  if (!device.has_value()) {
    return std::nullopt;
  }

  if (worker.memory_cap_mb.has_value() &&
      device->free_vram_mb <
          (*worker.memory_cap_mb + policy_config.observed_move_vram_reserve_mb)) {
    return std::string("observed-insufficient-vram");
  }

  if (moving_to_different_gpu &&
      device->gpu_utilization_pct >=
          policy_config.compute_pressure_utilization_threshold_pct &&
      ObservedGpuDeviceHasForeignProcess(
          observations,
          target_node_name,
          target_gpu_device,
          worker.name,
          domain_support)) {
    return std::string("compute-pressure");
  }

  return std::nullopt;
}

}  // namespace

SchedulerDomainService::SchedulerDomainService(
    std::shared_ptr<const SchedulerDomainSupport> domain_support,
    SchedulerDomainPolicyConfig policy_config)
    : domain_support_(std::move(domain_support)),
      policy_config_(std::move(policy_config)) {}

std::vector<RolloutLifecycleEntry> SchedulerDomainService::BuildRolloutLifecycleEntries(
    const naim::DesiredState& desired_state,
    int desired_generation,
    const std::vector<naim::RolloutActionRecord>& rollout_actions,
    const std::vector<naim::HostAssignment>& assignments,
    const std::vector<naim::HostObservation>& observations) const {
  std::map<std::string, std::vector<naim::RolloutActionRecord>> actions_by_worker;
  for (const auto& action : rollout_actions) {
    if (action.desired_generation == desired_generation) {
      actions_by_worker[action.worker_name].push_back(action);
    }
  }

  std::vector<RolloutLifecycleEntry> entries;
  for (auto& [worker_name, actions] : actions_by_worker) {
    std::sort(
        actions.begin(),
        actions.end(),
        [](const naim::RolloutActionRecord& left, const naim::RolloutActionRecord& right) {
          if (left.step != right.step) {
            return left.step < right.step;
          }
          return left.id < right.id;
        });

    const naim::RolloutActionRecord* evict_action = nullptr;
    const naim::RolloutActionRecord* retry_action = nullptr;
    for (const auto& action : actions) {
      if (action.action == "evict-best-effort" && evict_action == nullptr) {
        evict_action = &action;
      } else if (action.action == "retry-placement" && retry_action == nullptr) {
        retry_action = &action;
      }
    }
    if (evict_action == nullptr && retry_action == nullptr) {
      continue;
    }

    RolloutLifecycleEntry entry;
    entry.worker_name = worker_name;
    entry.desired_generation = desired_generation;
    const auto* target_action = retry_action != nullptr ? retry_action : evict_action;
    entry.target_node_name = target_action->target_node_name;
    entry.target_gpu_device = target_action->target_gpu_device;
    if (evict_action != nullptr) {
      entry.victim_worker_names = evict_action->victim_worker_names;
    }

    if (evict_action != nullptr) {
      entry.action_id = evict_action->id;
      if (evict_action->status == naim::RolloutActionStatus::Pending) {
        entry.phase = SchedulerRolloutPhase::Planned;
        entry.detail = "awaiting eviction enqueue";
      } else if (evict_action->status == naim::RolloutActionStatus::Acknowledged) {
        if (AreRolloutEvictionAssignmentsApplied(assignments, evict_action->id)) {
          entry.phase = SchedulerRolloutPhase::EvictionApplied;
          entry.detail = "eviction assignments applied";
        } else if (HasRolloutEvictionAssignments(assignments, evict_action->id)) {
          entry.phase = SchedulerRolloutPhase::EvictionEnqueued;
          entry.detail = "eviction assignments enqueued";
        } else {
          entry.phase = SchedulerRolloutPhase::EvictionEnqueued;
          entry.detail = evict_action->status_message.empty()
                             ? "eviction acknowledged"
                             : evict_action->status_message;
        }
      } else if (evict_action->status == naim::RolloutActionStatus::ReadyToRetry) {
        entry.phase = SchedulerRolloutPhase::EvictionApplied;
        entry.detail = "eviction completed";
      }
    }

    if (retry_action != nullptr &&
        retry_action->status == naim::RolloutActionStatus::ReadyToRetry) {
      entry.phase = SchedulerRolloutPhase::RetryReady;
      entry.action_id = retry_action->id;
      entry.detail = "retry placement can be materialized";
    }

    entries.push_back(std::move(entry));
  }

  for (const auto& instance : desired_state.instances) {
    if (instance.role != naim::InstanceRole::Worker) {
      continue;
    }
    const auto placement_action_it = instance.labels.find("naim.placement.action");
    const auto placement_decision_it = instance.labels.find("naim.placement.decision");
    if (placement_action_it == instance.labels.end() ||
        placement_decision_it == instance.labels.end() ||
        placement_action_it->second != "materialized-retry-placement" ||
        placement_decision_it->second != "applied") {
      continue;
    }
    if (actions_by_worker.find(instance.name) != actions_by_worker.end()) {
      continue;
    }

    RolloutLifecycleEntry entry;
    entry.worker_name = instance.name;
    entry.desired_generation = desired_generation;
    entry.phase = SchedulerRolloutPhase::RetryMaterialized;
    entry.target_node_name = instance.node_name;
    entry.target_gpu_device = instance.gpu_device.value_or("");

    const auto target_assignment =
        FindLatestHostAssignmentForNodeGeneration(
            assignments,
            instance.node_name,
            desired_generation);
    const auto target_observation =
        FindHostObservationForNode(observations, instance.node_name);
    if (target_observation.has_value() &&
        target_observation->status == naim::HostObservationStatus::Failed) {
      entry.phase = SchedulerRolloutPhase::HostFailed;
      entry.detail = "target node observation failed";
    } else if (
        target_observation.has_value() &&
        domain_support_->HealthFromAge(
            domain_support_->HeartbeatAgeSeconds(target_observation->heartbeat_at),
            policy_config_.default_stale_after_seconds) == "stale") {
      entry.phase = SchedulerRolloutPhase::HostStale;
      entry.detail = "target node observation stale";
    } else if (
        target_observation.has_value() &&
        domain_support_->ParseRuntimeStatus(*target_observation).has_value() &&
        domain_support_->ParseRuntimeStatus(*target_observation)->runtime_phase == "failed") {
      entry.phase = SchedulerRolloutPhase::RuntimeFailed;
      entry.detail = "target runtime reported failed phase";
    } else if (
        target_observation.has_value() &&
        target_observation->status == naim::HostObservationStatus::Applied &&
        target_observation->applied_generation.has_value() &&
        *target_observation->applied_generation >= desired_generation) {
      entry.phase = SchedulerRolloutPhase::RolloutApplied;
      entry.detail = "target node observed desired generation applied";
    } else if (target_assignment.has_value()) {
      entry.detail =
          "target node assignment status=" + naim::ToString(target_assignment->status);
    } else {
      entry.detail = "materialized in desired state";
    }

    entries.push_back(std::move(entry));
  }

  std::sort(
      entries.begin(),
      entries.end(),
      [](const RolloutLifecycleEntry& left, const RolloutLifecycleEntry& right) {
        return left.worker_name < right.worker_name;
      });
  return entries;
}

RebalanceControllerGateSummary
SchedulerDomainService::BuildRebalanceControllerGateSummary(
    const naim::DesiredState& desired_state,
    int desired_generation,
    const std::vector<naim::NodeAvailabilityOverride>& availability_overrides,
    const std::vector<RolloutLifecycleEntry>& rollout_lifecycle_entries,
    const std::vector<naim::HostAssignment>& assignments,
    const SchedulerRuntimeView& scheduler_runtime,
    const std::vector<naim::HostObservation>& observations,
    int stale_after_seconds) const {
  RebalanceControllerGateSummary summary;
  std::set<std::string> active_rollout_workers;
  for (const auto& entry : rollout_lifecycle_entries) {
    if (RolloutPhaseBlocksRebalance(entry.phase)) {
      active_rollout_workers.insert(entry.worker_name);
    }
  }
  if (scheduler_runtime.plane_runtime.has_value() &&
      !scheduler_runtime.plane_runtime->active_action.empty() &&
      !scheduler_runtime.plane_runtime->active_worker_name.empty()) {
    active_rollout_workers.insert(
        scheduler_runtime.plane_runtime->active_worker_name);
  }

  std::set<std::string> blocking_assignment_nodes;
  for (const auto& assignment : assignments) {
    if (HostAssignmentBlocksRebalance(assignment)) {
      blocking_assignment_nodes.insert(assignment.node_name);
    }
  }

  summary.active_rollout_workers.assign(
      active_rollout_workers.begin(), active_rollout_workers.end());
  summary.blocking_assignment_nodes.assign(
      blocking_assignment_nodes.begin(), blocking_assignment_nodes.end());
  summary.active_rollout_count =
      static_cast<int>(summary.active_rollout_workers.size());
  summary.blocking_assignment_count =
      static_cast<int>(summary.blocking_assignment_nodes.size());

  const auto availability_override_map =
      domain_support_->BuildAvailabilityOverrideMap(availability_overrides);
  std::set<std::string> unconverged_nodes;
  for (const auto& node : desired_state.nodes) {
    if (!domain_support_->IsNodeSchedulable(
            domain_support_->ResolveNodeAvailability(availability_override_map, node.name))) {
      continue;
    }
    const auto observation = FindHostObservationForNode(observations, node.name);
    if (!observation.has_value()) {
      unconverged_nodes.insert(node.name);
      continue;
    }
    if (observation->status == naim::HostObservationStatus::Failed) {
      unconverged_nodes.insert(node.name);
      continue;
    }
    const auto age_seconds = domain_support_->HeartbeatAgeSeconds(observation->heartbeat_at);
    if (domain_support_->HealthFromAge(age_seconds, stale_after_seconds) != "online") {
      unconverged_nodes.insert(node.name);
      continue;
    }
    if (!observation->applied_generation.has_value() ||
        *observation->applied_generation != desired_generation) {
      unconverged_nodes.insert(node.name);
      continue;
    }
  }

  summary.unconverged_nodes.assign(
      unconverged_nodes.begin(), unconverged_nodes.end());
  summary.unconverged_node_count =
      static_cast<int>(summary.unconverged_nodes.size());
  summary.cluster_ready =
      summary.active_rollout_count == 0 &&
      summary.blocking_assignment_count == 0 &&
      summary.unconverged_node_count == 0;
  return summary;
}

std::vector<RebalancePlanEntry> SchedulerDomainService::BuildRebalancePlanEntries(
    const naim::DesiredState& state,
    const naim::SchedulingPolicyReport& scheduling_report,
    const std::vector<naim::NodeAvailabilityOverride>& availability_overrides,
    const std::vector<RolloutLifecycleEntry>& rollout_lifecycle_entries,
    const std::vector<naim::HostAssignment>& assignments,
    const SchedulerRuntimeView& scheduler_runtime,
    const std::vector<naim::HostObservation>& observations,
    int stale_after_seconds,
    const std::optional<std::string>& node_name_filter) const {
  std::vector<RebalancePlanEntry> entries;
  for (const auto& recommendation : scheduling_report.placement_recommendations) {
    const auto* worker = FindWorkerInstance(state, recommendation.worker_name);
    if (worker == nullptr) {
      continue;
    }
    if (worker->placement_mode == naim::PlacementMode::Manual) {
      continue;
    }
    if (node_name_filter.has_value() && worker->node_name != *node_name_filter) {
      bool candidate_matches = false;
      for (const auto& candidate : recommendation.candidates) {
        if (candidate.node_name == *node_name_filter) {
          candidate_matches = true;
          break;
        }
      }
      if (!candidate_matches) {
        continue;
      }
    }

    RebalancePlanEntry entry;
    entry.worker_name = recommendation.worker_name;
    entry.placement_mode = worker->placement_mode;
    entry.current_node_name = recommendation.current_node_name;
    entry.current_gpu_device = recommendation.current_gpu_device;
    const auto availability_override_map =
        domain_support_->BuildAvailabilityOverrideMap(availability_overrides);
    const auto source_availability =
        domain_support_->ResolveNodeAvailability(
            availability_override_map,
            recommendation.current_node_name);
    const bool source_requires_exit =
        source_availability != naim::NodeAvailability::Active;

    const auto worker_runtime_it =
        scheduler_runtime.worker_runtime_by_name.find(recommendation.worker_name);
    if (worker_runtime_it != scheduler_runtime.worker_runtime_by_name.end() &&
        worker_runtime_it->second.manual_intervention_required) {
      entry.rebalance_class = "gated";
      entry.decision = "hold";
      entry.state = "manual-intervention-required";
      entry.gate_reason = "manual-intervention-required";
      entries.push_back(std::move(entry));
      continue;
    }

    if (scheduler_runtime.plane_runtime.has_value() &&
        !scheduler_runtime.plane_runtime->active_action.empty() &&
        scheduler_runtime.plane_runtime->active_worker_name ==
            recommendation.worker_name) {
      entry.rebalance_class = "gated";
      entry.decision = "hold";
      entry.state = scheduler_runtime.plane_runtime->phase.empty()
                        ? "active-scheduler-action"
                        : scheduler_runtime.plane_runtime->phase;
      entry.target_node_name = scheduler_runtime.plane_runtime->target_node_name;
      entry.target_gpu_device = scheduler_runtime.plane_runtime->target_gpu_device;
      entry.gate_reason = scheduler_runtime.plane_runtime->active_action;
      entries.push_back(std::move(entry));
      continue;
    }

    if (scheduler_runtime.plane_runtime.has_value() &&
        !scheduler_runtime.plane_runtime->active_action.empty() &&
        scheduler_runtime.plane_runtime->active_worker_name !=
            recommendation.worker_name) {
      entry.rebalance_class = "gated";
      entry.decision = "hold";
      entry.state = "active-scheduler-action";
      entry.gate_reason = scheduler_runtime.plane_runtime->active_action;
      entries.push_back(std::move(entry));
      continue;
    }

    const auto rollout_lifecycle =
        FindRolloutLifecycleEntry(rollout_lifecycle_entries, recommendation.worker_name);
    if (rollout_lifecycle.has_value() &&
        RolloutPhaseBlocksRebalance(rollout_lifecycle->phase)) {
      entry.rebalance_class = "rollout-class";
      entry.decision = "hold";
      entry.state = "active-rollout";
      entry.target_node_name = rollout_lifecycle->target_node_name;
      entry.target_gpu_device = rollout_lifecycle->target_gpu_device;
      entry.gate_reason = ToString(rollout_lifecycle->phase);
      entries.push_back(std::move(entry));
      continue;
    }

    const naim::PlacementCandidate* selected_candidate = nullptr;
    if (source_requires_exit) {
      for (const auto& candidate : recommendation.candidates) {
        if (candidate.action == "insufficient-memory" ||
            candidate.action == "insufficient-fraction") {
          continue;
        }
        const auto target_availability =
            domain_support_->ResolveNodeAvailability(
                availability_override_map,
                candidate.node_name);
        if (candidate.node_name != recommendation.current_node_name &&
            domain_support_->IsNodeSchedulable(target_availability)) {
          selected_candidate = &candidate;
          break;
        }
      }
    }
    if (selected_candidate == nullptr) {
      for (const auto& candidate : recommendation.candidates) {
        if (candidate.action == "insufficient-memory" ||
            candidate.action == "insufficient-fraction") {
          continue;
        }
        selected_candidate = &candidate;
        break;
      }
    }
    if (selected_candidate == nullptr && !recommendation.candidates.empty()) {
      selected_candidate = &recommendation.candidates.front();
    }
    if (selected_candidate == nullptr) {
      entry.rebalance_class = source_requires_exit ? "gated" : "no-candidate";
      entry.decision = "hold";
      entry.state = source_requires_exit ? "draining-source" : "no-candidate";
      entry.gate_reason =
          source_requires_exit ? "no-active-drain-target" : std::string{};
      entries.push_back(std::move(entry));
      continue;
    }

    entry.target_node_name = selected_candidate->node_name;
    entry.target_gpu_device = selected_candidate->gpu_device;
    entry.action = selected_candidate->action;
    entry.score = selected_candidate->score;
    entry.preemption_required = selected_candidate->preemption_required;
    entry.victim_worker_names = selected_candidate->preemption_victims;
    const auto target_availability =
        domain_support_->ResolveNodeAvailability(
            availability_override_map,
            selected_candidate->node_name);

    if (worker_runtime_it != scheduler_runtime.worker_runtime_by_name.end()) {
      const auto last_move_age =
          domain_support_->TimestampAgeSeconds(worker_runtime_it->second.last_move_at);
      if (last_move_age.has_value() &&
          *last_move_age < policy_config_.worker_minimum_residency_seconds) {
        entry.rebalance_class = "stable";
        entry.decision = "hold";
        entry.state = "min-residency";
        entry.gate_reason =
            "min-residency(" + std::to_string(*last_move_age) + "<" +
            std::to_string(policy_config_.worker_minimum_residency_seconds) + ")";
        entries.push_back(std::move(entry));
        continue;
      }
    }

    auto source_node_runtime_it =
        scheduler_runtime.node_runtime_by_name.find(recommendation.current_node_name);
    auto target_node_runtime_it =
        scheduler_runtime.node_runtime_by_name.find(selected_candidate->node_name);
    const auto source_move_age =
        source_node_runtime_it == scheduler_runtime.node_runtime_by_name.end()
            ? std::optional<long long>{}
            : domain_support_->TimestampAgeSeconds(source_node_runtime_it->second.last_move_at);
    const auto target_move_age =
        target_node_runtime_it == scheduler_runtime.node_runtime_by_name.end()
            ? std::optional<long long>{}
            : domain_support_->TimestampAgeSeconds(target_node_runtime_it->second.last_move_at);
    if ((source_move_age.has_value() &&
         *source_move_age < policy_config_.node_cooldown_after_move_seconds) ||
        (target_move_age.has_value() &&
         *target_move_age < policy_config_.node_cooldown_after_move_seconds)) {
      entry.rebalance_class = "gated";
      entry.decision = "hold";
      entry.state = "cooldown";
      if (source_move_age.has_value() && target_move_age.has_value() &&
          *source_move_age < policy_config_.node_cooldown_after_move_seconds &&
          *target_move_age < policy_config_.node_cooldown_after_move_seconds) {
        entry.gate_reason = "cooldown-source-and-target";
      } else if (source_move_age.has_value() &&
                 *source_move_age < policy_config_.node_cooldown_after_move_seconds) {
        entry.gate_reason = "cooldown-source";
      } else {
        entry.gate_reason = "cooldown-target";
      }
      entries.push_back(std::move(entry));
      continue;
    }

    if (source_requires_exit &&
        selected_candidate->node_name == recommendation.current_node_name) {
      entry.rebalance_class = "gated";
      entry.decision = "hold";
      entry.state = "draining-source";
      entry.gate_reason = "no-active-drain-target";
      entries.push_back(std::move(entry));
      continue;
    }

    if (!domain_support_->IsNodeSchedulable(target_availability)) {
      entry.rebalance_class = "gated";
      entry.decision = "hold";
      entry.state = "gated-target";
      entry.gate_reason =
          target_availability == naim::NodeAvailability::Draining
              ? "draining-target"
              : "unavailable-target";
      entries.push_back(std::move(entry));
      continue;
    }

    const bool source_assignment_busy =
        NodeHasBlockingHostAssignment(assignments, recommendation.current_node_name);
    const bool target_assignment_busy =
        NodeHasBlockingHostAssignment(assignments, selected_candidate->node_name);
    if (source_assignment_busy || target_assignment_busy) {
      entry.rebalance_class = "gated";
      entry.decision = "hold";
      entry.state = "assignment-in-flight";
      if (source_assignment_busy && target_assignment_busy) {
        entry.gate_reason = "source-and-target-node-busy";
      } else if (source_assignment_busy) {
        entry.gate_reason = "source-node-busy";
      } else {
        entry.gate_reason = "target-node-busy";
      }
      entries.push_back(std::move(entry));
      continue;
    }

    const auto gate_reason =
        domain_support_->ObservedSchedulingGateReason(
            observations,
            selected_candidate->node_name,
            stale_after_seconds);
    if (gate_reason.has_value()) {
      entry.rebalance_class = "gated";
      entry.decision = "hold";
      entry.state = "gated-target";
      entry.gate_reason = *gate_reason;
    } else if (const auto gpu_gate_reason =
                   ObservedGpuPlacementGateReason(
                       observations,
                       *worker,
                       selected_candidate->node_name,
                       selected_candidate->gpu_device,
                       selected_candidate->node_name != recommendation.current_node_name ||
                           selected_candidate->gpu_device !=
                               recommendation.current_gpu_device,
                       *domain_support_,
                       policy_config_);
               gpu_gate_reason.has_value()) {
      entry.rebalance_class = "gated";
      entry.decision = "hold";
      entry.state = "gated-target";
      entry.gate_reason = *gpu_gate_reason;
    } else if (selected_candidate->preemption_required) {
      entry.rebalance_class = "rollout-class";
      entry.decision = "defer";
      entry.state = source_requires_exit ? "drain-preemption" : "deferred-preemption";
    } else if (selected_candidate->score <
               policy_config_.minimum_safe_direct_rebalance_score) {
      entry.rebalance_class = "stable";
      entry.decision = "hold";
      entry.state = "below-threshold";
      entry.gate_reason =
          "score-below-threshold(" + std::to_string(selected_candidate->score) +
          "<" + std::to_string(policy_config_.minimum_safe_direct_rebalance_score) + ")";
    } else if (selected_candidate->same_node &&
               selected_candidate->action == "upgrade-to-exclusive") {
      entry.rebalance_class = "safe-direct";
      entry.decision = "propose";
      entry.state = "ready-in-place-upgrade";
    } else if (selected_candidate->same_node) {
      entry.rebalance_class = "stable";
      entry.decision = "hold";
      entry.state = "stay";
    } else {
      entry.rebalance_class = "safe-direct";
      entry.decision = "propose";
      entry.state = source_requires_exit ? "ready-drain-move" : "ready-move";
    }

    entries.push_back(std::move(entry));
  }

  std::sort(
      entries.begin(),
      entries.end(),
      [](const RebalancePlanEntry& left, const RebalancePlanEntry& right) {
        if (left.worker_name != right.worker_name) {
          return left.worker_name < right.worker_name;
        }
        return left.target_node_name < right.target_node_name;
      });
  return entries;
}

}  // namespace naim::controller
