#include "scheduler/scheduler_view_service.h"

#include <iostream>

using nlohmann::json;

namespace {

void PrintNameListLine(
    std::ostream& out,
    const std::string& label,
    const std::vector<std::string>& values) {
  if (values.empty()) {
    return;
  }
  out << "  " << label << "=";
  for (std::size_t index = 0; index < values.size(); ++index) {
    if (index > 0) {
      out << ",";
    }
    out << values[index];
  }
  out << "\n";
}

}  // namespace

std::string ToString(SchedulerRolloutPhase phase) {
  switch (phase) {
    case SchedulerRolloutPhase::Planned:
      return "planned";
    case SchedulerRolloutPhase::EvictionEnqueued:
      return "eviction-enqueued";
    case SchedulerRolloutPhase::EvictionApplied:
      return "eviction-applied";
    case SchedulerRolloutPhase::RetryReady:
      return "retry-ready";
    case SchedulerRolloutPhase::RetryMaterialized:
      return "retry-materialized";
    case SchedulerRolloutPhase::HostFailed:
      return "host-failed";
    case SchedulerRolloutPhase::HostStale:
      return "host-stale";
    case SchedulerRolloutPhase::RuntimeFailed:
      return "runtime-failed";
    case SchedulerRolloutPhase::RolloutApplied:
      return "rollout-applied";
  }
  return "unknown";
}

nlohmann::json SchedulerViewService::BuildRolloutActionsPayload(
    const RolloutActionsViewData& view) const {
  json payload{
      {"service", "naim-controller"},
      {"db_path", view.db_path},
      {"plane_name",
       view.plane_name.has_value() ? json(*view.plane_name) : json(nullptr)},
      {"node_name",
       view.node_name.has_value() ? json(*view.node_name) : json(nullptr)},
  };
  payload["desired_generation"] =
      view.desired_generation.has_value() ? json(*view.desired_generation)
                                          : json(nullptr);

  json action_items = json::array();
  for (const auto& action : view.actions) {
    action_items.push_back(json{
        {"id", action.id},
        {"desired_generation", action.desired_generation},
        {"step", action.step},
        {"worker_name", action.worker_name},
        {"action", action.action},
        {"target_node_name", action.target_node_name},
        {"target_gpu_device", action.target_gpu_device},
        {"victim_worker_names", action.victim_worker_names},
        {"reason", action.reason},
        {"status", naim::ToString(action.status)},
        {"status_message", action.status_message},
    });
  }
  payload["rollout_gates"] = json{
      {"gated_workers", view.gated_worker_count},
      {"gated_nodes", view.gated_node_count},
      {"deferred_actions", view.actions.size()},
  };
  payload["actions"] = std::move(action_items);

  if (view.desired_state.has_value() && view.desired_generation.has_value()) {
    json lifecycle_items = json::array();
    for (const auto& entry : view.lifecycle) {
      lifecycle_items.push_back(json{
          {"worker_name", entry.worker_name},
          {"desired_generation", entry.desired_generation},
          {"phase", ToString(entry.phase)},
          {"action_id",
           entry.action_id.has_value() ? json(*entry.action_id) : json(nullptr)},
          {"target_node_name",
           entry.target_node_name.empty() ? json(nullptr)
                                          : json(entry.target_node_name)},
          {"target_gpu_device",
           entry.target_gpu_device.empty() ? json(nullptr)
                                           : json(entry.target_gpu_device)},
          {"victim_worker_names", entry.victim_worker_names},
          {"detail", entry.detail.empty() ? json(nullptr) : json(entry.detail)},
      });
    }
    payload["rollout_lifecycle"] = std::move(lifecycle_items);
  } else {
    payload["rollout_lifecycle"] = json::array();
  }

  return payload;
}

nlohmann::json SchedulerViewService::BuildRebalancePlanPayload(
    const RebalancePlanViewData& view) const {
  json payload{
      {"service", "naim-controller"},
      {"db_path", view.db_path},
      {"plane_name",
       view.plane_name.has_value() ? json(*view.plane_name) : json(nullptr)},
      {"node_name",
       view.node_name.has_value() ? json(*view.node_name) : json(nullptr)},
      {"stale_after_seconds", view.stale_after_seconds},
  };
  if (!view.desired_state.has_value()) {
    payload["desired_state"] = nullptr;
    payload["rebalance_plan"] = json::array();
    return payload;
  }

  json plan_items = json::array();
  for (const auto& entry : view.rebalance_entries) {
    json item;
    item["worker_name"] = entry.worker_name;
    item["placement_mode"] = naim::ToString(entry.placement_mode);
    item["current_node_name"] = entry.current_node_name;
    item["current_gpu_device"] = entry.current_gpu_device;
    item["target_node_name"] =
        entry.target_node_name.empty() ? json(nullptr)
                                       : json(entry.target_node_name);
    item["target_gpu_device"] =
        entry.target_gpu_device.empty() ? json(nullptr)
                                        : json(entry.target_gpu_device);
    item["rebalance_class"] = entry.rebalance_class;
    item["decision"] = entry.decision;
    item["state"] = entry.state;
    item["action"] = entry.action.empty() ? json(nullptr) : json(entry.action);
    item["score"] = entry.score;
    item["preemption_required"] = entry.preemption_required;
    item["victim_worker_names"] = entry.victim_worker_names;
    item["gate_reason"] =
        entry.gate_reason.empty() ? json(nullptr) : json(entry.gate_reason);
    plan_items.push_back(std::move(item));
  }

  json worker_runtime_items = json::array();
  for (const auto& [worker_name, runtime] :
       view.scheduler_runtime.worker_runtime_by_name) {
    json item;
    item["worker_name"] = worker_name;
    item["last_move_at"] =
        runtime.last_move_at.empty() ? json(nullptr) : json(runtime.last_move_at);
    item["last_eviction_at"] = runtime.last_eviction_at.empty()
                                   ? json(nullptr)
                                   : json(runtime.last_eviction_at);
    item["last_verified_generation"] =
        runtime.last_verified_generation.has_value()
            ? json(*runtime.last_verified_generation)
            : json(nullptr);
    item["last_scheduler_phase"] = runtime.last_scheduler_phase.empty()
                                       ? json(nullptr)
                                       : json(runtime.last_scheduler_phase);
    item["last_status_message"] = runtime.last_status_message.empty()
                                      ? json(nullptr)
                                      : json(runtime.last_status_message);
    item["manual_intervention_required"] = runtime.manual_intervention_required;
    worker_runtime_items.push_back(std::move(item));
  }
  json node_runtime_items = json::array();
  for (const auto& [runtime_node_name, runtime] :
       view.scheduler_runtime.node_runtime_by_name) {
    json item;
    item["node_name"] = runtime_node_name;
    item["last_move_at"] =
        runtime.last_move_at.empty() ? json(nullptr) : json(runtime.last_move_at);
    item["last_verified_generation"] =
        runtime.last_verified_generation.has_value()
            ? json(*runtime.last_verified_generation)
            : json(nullptr);
    node_runtime_items.push_back(std::move(item));
  }

  payload["plane_name"] = view.desired_state->plane_name;
  payload["desired_generation"] = view.desired_generation;
  payload["rebalance_plan"] = std::move(plan_items);
  payload["controller_gate"] = json{
      {"cluster_ready", view.controller_gate_summary.cluster_ready},
      {"active_rollouts", view.controller_gate_summary.active_rollout_count},
      {"blocking_assignment_nodes",
       view.controller_gate_summary.blocking_assignment_count},
      {"unconverged_nodes", view.controller_gate_summary.unconverged_node_count},
      {"active_rollout_workers",
       view.controller_gate_summary.active_rollout_workers},
      {"blocking_assignment_node_names",
       view.controller_gate_summary.blocking_assignment_nodes},
      {"unconverged_node_names",
       view.controller_gate_summary.unconverged_nodes},
  };
  payload["iteration_budget"] = json{
      {"current_iteration", view.iteration_budget_summary.current_iteration},
      {"max_iterations", view.iteration_budget_summary.max_iterations},
      {"exhausted", view.iteration_budget_summary.exhausted},
  };
  payload["loop_status"] = json{
      {"state", view.loop_status.state},
      {"reason", view.loop_status.reason},
  };
  payload["policy_summary"] = json{
      {"actionable", view.policy_summary.actionable_count},
      {"safe_direct", view.policy_summary.safe_direct_count},
      {"rollout_class", view.policy_summary.rollout_class_count},
      {"gated", view.policy_summary.gated_count},
      {"blocked_active_rollouts",
       view.policy_summary.blocked_active_rollout_count},
      {"assignment_busy", view.policy_summary.assignment_busy_count},
      {"observation_gated", view.policy_summary.observation_gated_count},
      {"stable_holds", view.policy_summary.stable_hold_count},
      {"below_threshold", view.policy_summary.below_threshold_count},
      {"deferred", view.policy_summary.defer_count},
      {"no_candidate", view.policy_summary.no_candidate_count},
      {"actionable_workers", view.policy_summary.actionable_workers},
      {"safe_direct_workers", view.policy_summary.safe_direct_workers},
      {"rollout_class_workers", view.policy_summary.rollout_class_workers},
      {"gated_workers", view.policy_summary.gated_workers},
      {"stable_hold_workers", view.policy_summary.stable_hold_workers},
  };
  json scheduler_runtime_json;
  if (view.scheduler_runtime.plane_runtime.has_value()) {
    json plane_runtime_json;
    plane_runtime_json["active_action"] =
        view.scheduler_runtime.plane_runtime->active_action;
    plane_runtime_json["active_worker_name"] =
        view.scheduler_runtime.plane_runtime->active_worker_name;
    plane_runtime_json["phase"] = view.scheduler_runtime.plane_runtime->phase;
    plane_runtime_json["action_generation"] =
        view.scheduler_runtime.plane_runtime->action_generation;
    plane_runtime_json["stable_samples"] =
        view.scheduler_runtime.plane_runtime->stable_samples;
    plane_runtime_json["rollback_attempt_count"] =
        view.scheduler_runtime.plane_runtime->rollback_attempt_count;
    plane_runtime_json["source_node_name"] =
        view.scheduler_runtime.plane_runtime->source_node_name;
    plane_runtime_json["source_gpu_device"] =
        view.scheduler_runtime.plane_runtime->source_gpu_device;
    plane_runtime_json["target_node_name"] =
        view.scheduler_runtime.plane_runtime->target_node_name;
    plane_runtime_json["target_gpu_device"] =
        view.scheduler_runtime.plane_runtime->target_gpu_device;
    plane_runtime_json["status_message"] =
        view.scheduler_runtime.plane_runtime->status_message;
    plane_runtime_json["started_at"] =
        view.scheduler_runtime.plane_runtime->started_at;
    scheduler_runtime_json["plane_runtime"] = std::move(plane_runtime_json);
  } else {
    scheduler_runtime_json["plane_runtime"] = nullptr;
  }
  scheduler_runtime_json["worker_runtime"] = std::move(worker_runtime_items);
  scheduler_runtime_json["node_runtime"] = std::move(node_runtime_items);
  payload["scheduler_runtime"] = std::move(scheduler_runtime_json);
  return payload;
}

RebalancePolicySummary SchedulerViewService::BuildRebalancePolicySummary(
    const std::vector<RebalancePlanEntry>& entries) const {
  RebalancePolicySummary summary;
  for (const auto& entry : entries) {
    if (entry.state == "no-candidate") {
      ++summary.gated_count;
      ++summary.no_candidate_count;
      summary.gated_workers.push_back(entry.worker_name);
      summary.no_candidate_workers.push_back(entry.worker_name);
      continue;
    }
    if (entry.decision == "propose") {
      ++summary.actionable_count;
      ++summary.safe_direct_count;
      ++summary.propose_count;
      summary.actionable_workers.push_back(entry.worker_name);
      summary.safe_direct_workers.push_back(entry.worker_name);
      summary.proposed_workers.push_back(entry.worker_name);
      continue;
    }
    if (entry.decision == "defer") {
      ++summary.rollout_class_count;
      ++summary.defer_count;
      summary.rollout_class_workers.push_back(entry.worker_name);
      summary.deferred_workers.push_back(entry.worker_name);
      continue;
    }
    if (entry.state == "active-rollout") {
      ++summary.rollout_class_count;
      ++summary.blocked_active_rollout_count;
      summary.rollout_class_workers.push_back(entry.worker_name);
      summary.blocked_active_rollout_workers.push_back(entry.worker_name);
    } else if (entry.state == "assignment-in-flight" ||
               entry.state == "gated-target" ||
               entry.state == "draining-source" ||
               entry.state == "manual-intervention-required" ||
               entry.state == "active-scheduler-action" ||
               entry.state == "cooldown" ||
               entry.state == "min-residency") {
      ++summary.gated_count;
      summary.gated_workers.push_back(entry.worker_name);
      if (entry.state == "assignment-in-flight") {
        ++summary.assignment_busy_count;
        summary.assignment_busy_workers.push_back(entry.worker_name);
      } else if (entry.state == "gated-target") {
        ++summary.observation_gated_count;
        summary.observation_gated_workers.push_back(entry.worker_name);
      }
    } else {
      if (entry.state == "below-threshold") {
        ++summary.below_threshold_count;
        summary.below_threshold_workers.push_back(entry.worker_name);
      }
      ++summary.stable_hold_count;
      summary.stable_hold_workers.push_back(entry.worker_name);
    }
    ++summary.hold_count;
    summary.held_workers.push_back(entry.worker_name);
  }
  return summary;
}

RebalanceIterationBudgetSummary
SchedulerViewService::BuildRebalanceIterationBudgetSummary(
    int current_iteration,
    int max_iterations) const {
  RebalanceIterationBudgetSummary summary;
  summary.current_iteration = current_iteration;
  summary.max_iterations = max_iterations;
  summary.exhausted = summary.current_iteration >= summary.max_iterations;
  return summary;
}

RebalanceLoopStatusSummary SchedulerViewService::BuildRebalanceLoopStatusSummary(
    const RebalanceControllerGateSummary& controller_gate_summary,
    const RebalanceIterationBudgetSummary& iteration_budget_summary,
    const RebalancePolicySummary& policy_summary) const {
  RebalanceLoopStatusSummary summary;
  if (!controller_gate_summary.cluster_ready) {
    summary.state = "waiting-for-convergence";
    if (controller_gate_summary.unconverged_node_count > 0) {
      summary.reason =
          "unconverged-nodes=" +
          std::to_string(controller_gate_summary.unconverged_node_count);
    } else if (controller_gate_summary.blocking_assignment_count > 0) {
      summary.reason =
          "blocking-assignments=" +
          std::to_string(controller_gate_summary.blocking_assignment_count);
    } else {
      summary.reason =
          "active-rollouts=" +
          std::to_string(controller_gate_summary.active_rollout_count);
    }
    return summary;
  }
  if (iteration_budget_summary.exhausted && policy_summary.actionable_count > 0) {
    summary.state = "complete";
    summary.reason =
        "rebalance-iteration-limit=" +
        std::to_string(iteration_budget_summary.current_iteration) + "/" +
        std::to_string(iteration_budget_summary.max_iterations);
    return summary;
  }
  if (policy_summary.rollout_class_count > 0) {
    summary.state = "waiting-for-rollout";
    summary.reason =
        "rollout-class-workers=" +
        std::to_string(policy_summary.rollout_class_count);
    return summary;
  }
  if (policy_summary.actionable_count > 0) {
    summary.state = "actionable";
    summary.reason =
        "safe-direct-workers=" + std::to_string(policy_summary.actionable_count);
    return summary;
  }
  summary.state = "complete";
  if (policy_summary.below_threshold_count > 0) {
    summary.reason =
        "remaining-moves-below-threshold=" +
        std::to_string(policy_summary.below_threshold_count);
  } else if (policy_summary.no_candidate_count > 0) {
    summary.reason =
        "no-candidate-workers=" +
        std::to_string(policy_summary.no_candidate_count);
  } else {
    summary.reason = "no-actionable-rebalance";
  }
  return summary;
}

void SchedulerViewService::PrintRolloutLifecycleEntries(
    std::ostream& out,
    const std::vector<RolloutLifecycleEntry>& entries) const {
  out << "rollout-lifecycle:\n";
  if (entries.empty()) {
    out << "  (empty)\n";
    return;
  }

  for (const auto& entry : entries) {
    out << "  - worker=" << entry.worker_name
        << " generation=" << entry.desired_generation
        << " phase=" << ToString(entry.phase);
    if (entry.action_id.has_value()) {
      out << " action_id=" << *entry.action_id;
    }
    if (!entry.target_node_name.empty() || !entry.target_gpu_device.empty()) {
      out << " target=" << entry.target_node_name << ":" << entry.target_gpu_device;
    }
    if (!entry.victim_worker_names.empty()) {
      out << " victims=";
      for (std::size_t index = 0; index < entry.victim_worker_names.size(); ++index) {
        if (index > 0) {
          out << ",";
        }
        out << entry.victim_worker_names[index];
      }
    }
    if (!entry.detail.empty()) {
      out << " detail=" << entry.detail;
    }
    out << "\n";
  }
}

void SchedulerViewService::PrintRebalancePlanEntries(
    std::ostream& out,
    const std::vector<RebalancePlanEntry>& entries) const {
  out << "rebalance-plan:\n";
  if (entries.empty()) {
    out << "  (empty)\n";
    return;
  }
  for (const auto& entry : entries) {
    out << "  - worker=" << entry.worker_name
        << " placement_mode=" << naim::ToString(entry.placement_mode)
        << " current=" << entry.current_node_name << ":" << entry.current_gpu_device
        << " class=" << (entry.rebalance_class.empty() ? "(empty)" : entry.rebalance_class)
        << " decision=" << entry.decision
        << " state=" << entry.state;
    if (!entry.target_node_name.empty() || !entry.target_gpu_device.empty()) {
      out << " target=" << entry.target_node_name << ":" << entry.target_gpu_device;
    }
    if (!entry.action.empty()) {
      out << " action=" << entry.action;
    }
    out << " score=" << entry.score
        << " preemption_required=" << (entry.preemption_required ? "yes" : "no");
    if (!entry.victim_worker_names.empty()) {
      out << " victims=";
      for (std::size_t index = 0; index < entry.victim_worker_names.size(); ++index) {
        if (index > 0) {
          out << ",";
        }
        out << entry.victim_worker_names[index];
      }
    }
    if (!entry.gate_reason.empty()) {
      out << " gate_reason=" << entry.gate_reason;
    }
    out << "\n";
  }
}

void SchedulerViewService::PrintRebalancePolicySummary(
    std::ostream& out,
    const RebalancePolicySummary& summary) const {
  out << "rebalance-policy:\n";
  out << "  actionable=" << summary.actionable_count
      << " safe_direct=" << summary.safe_direct_count
      << " rollout_class=" << summary.rollout_class_count
      << " gated=" << summary.gated_count
      << " blocked_active_rollouts=" << summary.blocked_active_rollout_count
      << " assignment_busy=" << summary.assignment_busy_count
      << " observation_gated=" << summary.observation_gated_count
      << " stable_holds=" << summary.stable_hold_count
      << " below_threshold=" << summary.below_threshold_count
      << " deferred=" << summary.defer_count
      << " no_candidate=" << summary.no_candidate_count << "\n";
  out << "  propose=" << summary.propose_count
      << " hold=" << summary.hold_count
      << " defer=" << summary.defer_count
      << " no_candidate=" << summary.no_candidate_count << "\n";
  PrintNameListLine(out, "actionable_workers", summary.actionable_workers);
  PrintNameListLine(out, "safe_direct_workers", summary.safe_direct_workers);
  PrintNameListLine(out, "rollout_class_workers", summary.rollout_class_workers);
  PrintNameListLine(out, "gated_workers", summary.gated_workers);
  PrintNameListLine(
      out,
      "blocked_active_rollout_workers",
      summary.blocked_active_rollout_workers);
  PrintNameListLine(out, "assignment_busy_workers", summary.assignment_busy_workers);
  PrintNameListLine(out, "observation_gated_workers", summary.observation_gated_workers);
  PrintNameListLine(out, "stable_hold_workers", summary.stable_hold_workers);
  PrintNameListLine(out, "below_threshold_workers", summary.below_threshold_workers);
  PrintNameListLine(out, "proposed_workers", summary.proposed_workers);
  PrintNameListLine(out, "held_workers", summary.held_workers);
  PrintNameListLine(out, "deferred_workers", summary.deferred_workers);
  PrintNameListLine(out, "no_candidate_workers", summary.no_candidate_workers);
}

void SchedulerViewService::PrintRebalanceControllerGateSummary(
    std::ostream& out,
    const RebalanceControllerGateSummary& summary) const {
  out << "rebalance-controller-gate:\n";
  out << "  cluster_ready=" << (summary.cluster_ready ? "yes" : "no")
      << " active_rollouts=" << summary.active_rollout_count
      << " blocking_assignment_nodes=" << summary.blocking_assignment_count
      << " unconverged_nodes=" << summary.unconverged_node_count << "\n";
  PrintNameListLine(out, "active_rollout_workers", summary.active_rollout_workers);
  PrintNameListLine(
      out,
      "blocking_assignment_nodes",
      summary.blocking_assignment_nodes);
  PrintNameListLine(out, "unconverged_nodes", summary.unconverged_nodes);
}

void SchedulerViewService::PrintRebalanceIterationBudgetSummary(
    std::ostream& out,
    const RebalanceIterationBudgetSummary& summary) const {
  out << "rebalance-iteration-budget:\n";
  out << "  iteration=" << summary.current_iteration << "/" << summary.max_iterations
      << " exhausted=" << (summary.exhausted ? "yes" : "no") << "\n";
}

void SchedulerViewService::PrintRebalanceLoopStatusSummary(
    std::ostream& out,
    const RebalanceLoopStatusSummary& summary) const {
  out << "rebalance-loop-status:\n";
  out << "  state=" << summary.state;
  if (!summary.reason.empty()) {
    out << " reason=" << summary.reason;
  }
  out << "\n";
}

void SchedulerViewService::PrintSchedulerRuntimeView(
    std::ostream& out,
    const SchedulerRuntimeView& view,
    int verification_stable_samples_required) const {
  out << "scheduler-runtime:\n";
  if (!view.plane_runtime.has_value()) {
    out << "  plane_action=(none)\n";
  } else {
    const auto& runtime = *view.plane_runtime;
    out << "  plane_action="
        << (runtime.active_action.empty() ? "(none)" : runtime.active_action)
        << " phase=" << (runtime.phase.empty() ? "(empty)" : runtime.phase)
        << " worker="
        << (runtime.active_worker_name.empty() ? "(empty)" : runtime.active_worker_name)
        << " generation=" << runtime.action_generation
        << " stable_samples=" << runtime.stable_samples << "/"
        << verification_stable_samples_required
        << " rollback_attempts=" << runtime.rollback_attempt_count << "\n";
    if (!runtime.status_message.empty()) {
      out << "  status_message=" << runtime.status_message << "\n";
    }
  }
  if (!view.worker_runtime_by_name.empty()) {
    out << "  worker_runtime:\n";
    for (const auto& [worker_name, runtime] : view.worker_runtime_by_name) {
      out << "    - worker=" << worker_name
          << " last_move_at="
          << (runtime.last_move_at.empty() ? "(empty)" : runtime.last_move_at)
          << " last_eviction_at="
          << (runtime.last_eviction_at.empty() ? "(empty)" : runtime.last_eviction_at);
      if (runtime.last_verified_generation.has_value()) {
        out << " last_verified_generation=" << *runtime.last_verified_generation;
      }
      if (!runtime.last_scheduler_phase.empty()) {
        out << " last_phase=" << runtime.last_scheduler_phase;
      }
      out << " manual_intervention_required="
          << (runtime.manual_intervention_required ? "yes" : "no") << "\n";
      if (!runtime.last_status_message.empty()) {
        out << "      status_message=" << runtime.last_status_message << "\n";
      }
    }
  }
  if (!view.node_runtime_by_name.empty()) {
    out << "  node_runtime:\n";
    for (const auto& [node_name, runtime] : view.node_runtime_by_name) {
      out << "    - node=" << node_name
          << " last_move_at="
          << (runtime.last_move_at.empty() ? "(empty)" : runtime.last_move_at);
      if (runtime.last_verified_generation.has_value()) {
        out << " last_verified_generation=" << *runtime.last_verified_generation;
      }
      out << "\n";
    }
  }
}
