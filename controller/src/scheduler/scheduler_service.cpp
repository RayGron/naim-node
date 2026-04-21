#include "scheduler/scheduler_service.h"

#include <algorithm>
#include <iostream>
#include <set>
#include <sstream>
#include <stdexcept>
#include <utility>

#include "infra/controller_event_service.h"
#include "infra/controller_print_service.h"
#include "infra/controller_runtime_support_service.h"
#include "host/host_assignment_reconciliation_service.h"
#include "plane/plane_realization_service.h"
#include "read_model/read_model_cli_service.h"
#include "read_model/state_aggregate_loader.h"
#include "scheduler/scheduler_execution_support.h"
#include "skills/plane_skill_runtime_sync_service.h"

#include "naim/planning/execution_plan.h"
#include "naim/planning/scheduling_policy.h"
#include "naim/state/state_json.h"

namespace naim::controller {

std::optional<naim::RolloutActionRecord> SchedulerService::FindRolloutActionById(
    const std::vector<naim::RolloutActionRecord>& actions,
    int action_id) const {
  for (const auto& action : actions) {
    if (action.id == action_id) {
      return action;
    }
  }
  return std::nullopt;
}

SchedulerService::SchedulerService(
    std::string db_path,
    std::string artifacts_root,
    int default_stale_after_seconds,
    int verification_stable_samples_required,
    const StateAggregateLoader& state_aggregate_loader,
    const SchedulerViewService& scheduler_view_service,
    const ReadModelCliService& read_model_cli_service,
    const ControllerPrintService& controller_print_service,
    const ControllerRuntimeSupportService& runtime_support_service,
    const SchedulerExecutionSupport& scheduler_execution_support,
    const PlaneRealizationService& plane_realization_service,
    const ControllerEventService& controller_event_service)
    : db_path_(std::move(db_path)),
      artifacts_root_(std::move(artifacts_root)),
      default_stale_after_seconds_(default_stale_after_seconds),
      verification_stable_samples_required_(verification_stable_samples_required),
      state_aggregate_loader_(state_aggregate_loader),
      scheduler_view_service_(scheduler_view_service),
      read_model_cli_service_(read_model_cli_service),
      controller_print_service_(controller_print_service),
      runtime_support_service_(runtime_support_service),
      scheduler_execution_support_(scheduler_execution_support),
      plane_realization_service_(plane_realization_service),
      controller_event_service_(controller_event_service) {}

int SchedulerService::ShowRolloutActions(
    const std::optional<std::string>& node_name,
    const std::optional<std::string>& plane_name) const {
  const auto view =
      state_aggregate_loader_.LoadRolloutActionsViewData(
          db_path_,
          node_name,
          plane_name);

  std::cout << "db: " << view.db_path << "\n";
  if (view.desired_generation.has_value()) {
    std::cout << "desired generation: " << *view.desired_generation << "\n";
  }
  if (!view.actions.empty()) {
    std::cout << "rollout-gates:\n";
    std::cout << "  gated_workers=" << view.gated_worker_count
              << " gated_nodes=" << view.gated_node_count
              << " deferred_actions=" << view.actions.size() << "\n";
  }
  controller_print_service_.PrintPersistedRolloutActions(view.actions);
  if (view.scheduler_runtime.has_value()) {
    scheduler_view_service_.PrintSchedulerRuntimeView(
        std::cout,
        *view.scheduler_runtime,
        verification_stable_samples_required_);
  }
  if (!view.lifecycle.empty()) {
    scheduler_view_service_.PrintRolloutLifecycleEntries(
        std::cout,
        view.lifecycle);
  }
  return 0;
}

int SchedulerService::ShowRebalancePlan(
    const std::optional<std::string>& node_name,
    const std::optional<std::string>& plane_name) const {
  const auto view = state_aggregate_loader_.LoadRebalancePlanViewData(
      db_path_,
      node_name,
      default_stale_after_seconds_,
      plane_name);
  if (!view.desired_state.has_value()) {
    std::cout << "rebalance-plan:\n  (empty)\n";
    return 0;
  }

  std::cout << "db: " << view.db_path << "\n";
  std::cout << "desired generation: " << view.desired_generation << "\n";
  scheduler_view_service_.PrintRebalanceControllerGateSummary(
      std::cout,
      view.controller_gate_summary);
  scheduler_view_service_.PrintRebalanceIterationBudgetSummary(
      std::cout,
      view.iteration_budget_summary);
  scheduler_view_service_.PrintRebalanceLoopStatusSummary(
      std::cout,
      view.loop_status);
  scheduler_view_service_.PrintRebalancePlanEntries(
      std::cout,
      view.rebalance_entries);
  scheduler_view_service_.PrintRebalancePolicySummary(
      std::cout,
      view.policy_summary);
  scheduler_view_service_.PrintSchedulerRuntimeView(
      std::cout,
      view.scheduler_runtime,
      verification_stable_samples_required_);
  return 0;
}

int SchedulerService::ShowEvents(
    const std::optional<std::string>& plane_name,
    const std::optional<std::string>& node_name,
    const std::optional<std::string>& worker_name,
    const std::optional<std::string>& category,
    int limit) const {
  return read_model_cli_service_.ShowEvents(
      db_path_,
      plane_name,
      node_name,
      worker_name,
      category,
      limit);
}

ControllerActionResult SchedulerService::ExecuteApplyRebalanceProposal(
    const std::string& worker_name) const {
  return RunControllerActionResult(
      "apply-rebalance-proposal",
      [&]() { return ApplyRebalanceProposal(worker_name); });
}

ControllerActionResult SchedulerService::ExecuteReconcileRebalanceProposals() const {
  return RunControllerActionResult(
      "reconcile-rebalance-proposals",
      [&]() { return ReconcileRebalanceProposals(); });
}

ControllerActionResult SchedulerService::ExecuteSchedulerTick() const {
  return RunControllerActionResult(
      "scheduler-tick",
      [&]() { return SchedulerTick(); });
}

ControllerActionResult SchedulerService::ExecuteSetRolloutActionStatus(
    int action_id,
    const std::string& requested_status,
    const std::optional<std::string>& message) const {
  return RunControllerActionResult(
      "set-rollout-action-status",
      [&]() {
        return SetRolloutActionStatus(
            action_id,
            naim::ParseRolloutActionStatus(requested_status),
            message);
      });
}

ControllerActionResult SchedulerService::ExecuteEnqueueRolloutEviction(int action_id) const {
  return RunControllerActionResult(
      "enqueue-rollout-eviction",
      [&]() { return EnqueueRolloutEviction(action_id); });
}

ControllerActionResult SchedulerService::ExecuteReconcileRolloutActions() const {
  return RunControllerActionResult(
      "reconcile-rollout-actions",
      [&]() { return ReconcileRolloutActions(); });
}

ControllerActionResult SchedulerService::ExecuteApplyReadyRolloutAction(int action_id) const {
  return RunControllerActionResult(
      "apply-ready-rollout-action",
      [&]() { return ApplyReadyRolloutAction(action_id); });
}

int SchedulerService::ApplyRebalanceProposal(const std::string& worker_name) const {
  naim::ControllerStore store(db_path_);
  store.Initialize();

  const auto desired_state = store.LoadDesiredState();
  const auto desired_generation = store.LoadDesiredGeneration();
  const auto rebalance_iteration = store.LoadRebalanceIteration();
  if (!desired_state.has_value() || !desired_generation.has_value()) {
    throw std::runtime_error("no desired state found in controller db");
  }

  const auto observations = store.LoadHostObservations();
  const auto rebalance_view = state_aggregate_loader_.LoadRebalancePlanViewData(
      db_path_,
      std::nullopt,
      default_stale_after_seconds_,
      desired_state->plane_name);
  const auto rebalance_entries = rebalance_view.rebalance_entries;

  const auto rebalance_it = std::find_if(
      rebalance_entries.begin(),
      rebalance_entries.end(),
      [&](const RebalancePlanEntry& entry) { return entry.worker_name == worker_name; });
  if (rebalance_it == rebalance_entries.end()) {
    throw std::runtime_error(
        "no rebalance plan entry found for worker '" + worker_name + "'");
  }
  if (rebalance_it->decision != "propose") {
    throw std::runtime_error(
        "worker '" + worker_name + "' is not actionable for rebalance; current decision=" +
        rebalance_it->decision + " state=" + rebalance_it->state);
  }
  const auto iteration_budget_summary = rebalance_view.iteration_budget_summary;
  if (iteration_budget_summary.exhausted) {
    throw std::runtime_error(
        "rebalance iteration budget exhausted (" +
        std::to_string(iteration_budget_summary.current_iteration) + "/" +
        std::to_string(iteration_budget_summary.max_iterations) +
        "); apply a fresh bundle or rollout generation before materializing another direct rebalance");
  }

  naim::DesiredState updated_state = *desired_state;
  scheduler_execution_support_.MaterializeRebalancePlanEntry(&updated_state, *rebalance_it);
  naim::RequireSchedulingPolicy(updated_state);
  const naim::SchedulingPolicyReport updated_scheduling_report =
      naim::EvaluateSchedulingPolicy(updated_state);
  const int next_generation = *desired_generation + 1;
  const auto availability_overrides = store.LoadNodeAvailabilityOverrides();
  const auto host_plans =
      naim::BuildNodeExecutionPlans(desired_state, updated_state, artifacts_root_);

  plane_realization_service_.MaterializeComposeArtifacts(updated_state, host_plans);
  plane_realization_service_.MaterializeInferRuntimeArtifact(updated_state, artifacts_root_);
  store.ReplaceDesiredState(
      updated_state,
      next_generation,
      rebalance_iteration.value_or(0) + 1);
  store.ReplaceRolloutActions(
      updated_state.plane_name,
      next_generation,
      updated_scheduling_report.rollout_actions);
  store.ReplaceHostAssignments(
      plane_realization_service_.BuildHostAssignments(
          updated_state,
          artifacts_root_,
          next_generation,
          availability_overrides,
          observations,
          updated_scheduling_report));
  naim::SchedulerPlaneRuntime plane_runtime;
  plane_runtime.plane_name = updated_state.plane_name;
  plane_runtime.active_action = "rebalance";
  plane_runtime.active_worker_name = rebalance_it->worker_name;
  plane_runtime.phase = "verifying-move";
  plane_runtime.action_generation = next_generation;
  plane_runtime.stable_samples = 0;
  plane_runtime.rollback_attempt_count = 0;
  plane_runtime.source_node_name = rebalance_it->current_node_name;
  plane_runtime.source_gpu_device = rebalance_it->current_gpu_device;
  plane_runtime.target_node_name = rebalance_it->target_node_name;
  plane_runtime.target_gpu_device = rebalance_it->target_gpu_device;
  plane_runtime.previous_state_json = naim::SerializeDesiredStateJson(*desired_state);
  plane_runtime.status_message = "awaiting post-move verification";
  store.UpsertSchedulerPlaneRuntime(plane_runtime);
  controller_event_service_.AppendEvent(
      store,
      "scheduler",
      "rebalance-materialized",
      "materialized safe-direct rebalance proposal",
      nlohmann::json{
          {"desired_generation", next_generation},
          {"source_node", rebalance_it->current_node_name},
          {"source_gpu", rebalance_it->current_gpu_device},
          {"target_node", rebalance_it->target_node_name},
          {"target_gpu", rebalance_it->target_gpu_device},
          {"action", rebalance_it->action},
          {"score", rebalance_it->score},
      },
      updated_state.plane_name,
      rebalance_it->target_node_name,
      rebalance_it->worker_name,
      std::nullopt,
      std::nullopt,
      "info");

  std::cout << "applied rebalance proposal for worker '" << worker_name << "'\n";
  std::cout << "desired generation: " << next_generation << "\n";
  std::cout << "target=" << rebalance_it->target_node_name << ":"
            << rebalance_it->target_gpu_device << "\n";
  controller_print_service_.PrintStateSummary(updated_state);
  std::cout << naim::RenderSchedulingPolicyReport(updated_scheduling_report);
  controller_print_service_.PrintSchedulerDecisionSummary(updated_state);
  controller_print_service_.PrintRolloutGateSummary(updated_scheduling_report);
  controller_print_service_.PrintAssignmentDispatchSummary(
      updated_state,
      runtime_support_service_.BuildAvailabilityOverrideMap(availability_overrides),
      observations,
      default_stale_after_seconds_);
  return 0;
}

int SchedulerService::ReconcileRebalanceProposals() const {
  naim::ControllerStore store(db_path_);
  store.Initialize();

  const auto desired_state = store.LoadDesiredState();
  const auto desired_generation = store.LoadDesiredGeneration();
  if (!desired_state.has_value() || !desired_generation.has_value()) {
    throw std::runtime_error("no desired state found in controller db");
  }

  auto rebalance_view = state_aggregate_loader_.LoadRebalancePlanViewData(
      db_path_,
      std::nullopt,
      default_stale_after_seconds_,
      desired_state->plane_name);
  auto rebalance_entries = rebalance_view.rebalance_entries;
  const auto& controller_gate_summary = rebalance_view.controller_gate_summary;
  const auto& iteration_budget_summary = rebalance_view.iteration_budget_summary;
  const auto& rebalance_policy_summary = rebalance_view.policy_summary;
  scheduler_view_service_.PrintRebalanceControllerGateSummary(
      std::cout,
      controller_gate_summary);
  scheduler_view_service_.PrintRebalanceIterationBudgetSummary(
      std::cout,
      iteration_budget_summary);
  scheduler_view_service_.PrintRebalanceLoopStatusSummary(
      std::cout,
      scheduler_view_service_.BuildRebalanceLoopStatusSummary(
          controller_gate_summary,
          iteration_budget_summary,
          rebalance_policy_summary));

  if (!controller_gate_summary.cluster_ready) {
    std::cout << "rebalance proposals: blocked by controller gate\n";
    return 0;
  }

  rebalance_entries.erase(
      std::remove_if(
          rebalance_entries.begin(),
          rebalance_entries.end(),
          [](const RebalancePlanEntry& entry) {
            return entry.decision != "propose" || entry.rebalance_class != "safe-direct";
          }),
      rebalance_entries.end());

  if (rebalance_entries.empty()) {
    std::cout << "rebalance proposals: none actionable\n";
    return 0;
  }
  if (iteration_budget_summary.exhausted) {
    std::cout << "rebalance proposals: blocked by iteration budget\n";
    return 0;
  }

  std::sort(
      rebalance_entries.begin(),
      rebalance_entries.end(),
      [](const RebalancePlanEntry& left, const RebalancePlanEntry& right) {
        if (left.score != right.score) {
          return left.score > right.score;
        }
        return left.worker_name < right.worker_name;
      });

  std::cout << "selected rebalance proposal: worker=" << rebalance_entries.front().worker_name
            << " target=" << rebalance_entries.front().target_node_name << ":"
            << rebalance_entries.front().target_gpu_device
            << " score=" << rebalance_entries.front().score << "\n";
  return ApplyRebalanceProposal(rebalance_entries.front().worker_name);
}

int SchedulerService::SchedulerTick() const {
  naim::ControllerStore store(db_path_);
  store.Initialize();
  const HostAssignmentReconciliationService reconciliation_service;
  (void)reconciliation_service.Reconcile(store);
  const PlaneSkillRuntimeSyncService runtime_sync_service;
  for (const auto& state : store.LoadDesiredStates()) {
    (void)runtime_sync_service.SyncPlane(db_path_, state);
  }

  const auto desired_state = store.LoadDesiredState();
  const auto desired_generation = store.LoadDesiredGeneration();
  if (!desired_state.has_value() || !desired_generation.has_value()) {
    std::cout << "scheduler-tick: no desired state\n";
    return 0;
  }

  if (const auto plane_runtime = store.LoadSchedulerPlaneRuntime(desired_state->plane_name);
      plane_runtime.has_value() && !plane_runtime->active_action.empty()) {
    std::cout << "scheduler-tick: step=active-scheduler-action\n";
    return AdvanceActiveSchedulerAction();
  }

  const auto rollout_actions = store.LoadRolloutActions(desired_state->plane_name);
  bool has_active_rollout = false;
  for (const auto& action : rollout_actions) {
    if (action.desired_generation == *desired_generation &&
        action.status != naim::RolloutActionStatus::ReadyToRetry) {
      has_active_rollout = true;
      break;
    }
  }
  if (!rollout_actions.empty()) {
    std::cout << "scheduler-tick: step=rollout-reconcile\n";
    return ReconcileRolloutActions();
  }

  std::cout << "scheduler-tick: step=rebalance-reconcile\n";
  if (has_active_rollout) {
    std::cout << "scheduler-tick: rollout still active\n";
    return 0;
  }
  return ReconcileRebalanceProposals();
}

int SchedulerService::SetRolloutActionStatus(
    int action_id,
    naim::RolloutActionStatus status,
    const std::optional<std::string>& status_message) const {
  naim::ControllerStore store(db_path_);
  store.Initialize();
  if (!store.UpdateRolloutActionStatus(action_id, status, status_message.value_or(""))) {
    throw std::runtime_error(
        "rollout action id=" + std::to_string(action_id) + " not found");
  }
  const auto updated_action = FindRolloutActionById(store.LoadRolloutActions(), action_id);
  if (updated_action.has_value()) {
    controller_event_service_.AppendEvent(
        store,
        "rollout-action",
        "status-updated",
        "updated rollout action status",
        nlohmann::json{
            {"status", naim::ToString(status)},
            {"status_message", status_message.value_or("")},
            {"action", updated_action->action},
            {"step", updated_action->step},
        },
        updated_action->plane_name,
        updated_action->target_node_name,
        updated_action->worker_name,
        std::nullopt,
        action_id,
        "info");
  }
  std::cout << "updated rollout action id=" << action_id
            << " status=" << naim::ToString(status) << "\n";
  if (updated_action.has_value()) {
    controller_print_service_.PrintPersistedRolloutActions(
        store.LoadRolloutActions(updated_action->plane_name));
  } else {
    controller_print_service_.PrintPersistedRolloutActions(store.LoadRolloutActions());
  }
  return 0;
}

int SchedulerService::EnqueueRolloutEviction(int action_id) const {
  naim::ControllerStore store(db_path_);
  store.Initialize();

  const auto desired_state = store.LoadDesiredState();
  const auto desired_generation = store.LoadDesiredGeneration();
  if (!desired_state.has_value() || !desired_generation.has_value()) {
    throw std::runtime_error("no desired state found in controller db");
  }

  const auto rollout_actions = store.LoadRolloutActions(desired_state->plane_name);
  const auto action = FindRolloutActionById(rollout_actions, action_id);
  if (!action.has_value()) {
    throw std::runtime_error(
        "rollout action id=" + std::to_string(action_id) + " not found");
  }
  if (action->desired_generation != *desired_generation) {
    throw std::runtime_error(
        "rollout action id=" + std::to_string(action_id) +
        " does not belong to current desired generation " +
        std::to_string(*desired_generation));
  }
  if (action->action != "evict-best-effort") {
    throw std::runtime_error(
        "rollout action id=" + std::to_string(action_id) +
        " is not an evict-best-effort action");
  }
  if (action->status != naim::RolloutActionStatus::Pending &&
      action->status != naim::RolloutActionStatus::Acknowledged) {
    throw std::runtime_error(
        "rollout action id=" + std::to_string(action_id) +
        " cannot enqueue eviction from status=" +
        naim::ToString(action->status));
  }

  const auto existing_assignments = store.LoadHostAssignments();
  const auto eviction_assignments = scheduler_execution_support_.BuildEvictionAssignmentsForAction(
      *desired_state,
      *desired_generation,
      *action,
      existing_assignments);
  store.EnqueueHostAssignments(
      eviction_assignments,
      "superseded by rollout eviction action id=" + std::to_string(action_id));

  std::set<std::string> node_names;
  for (const auto& assignment : eviction_assignments) {
    node_names.insert(assignment.node_name);
  }
  std::ostringstream message;
  message << "eviction assignments enqueued on nodes ";
  bool first = true;
  for (const auto& node_name : node_names) {
    if (!first) {
      message << ",";
    }
    first = false;
    message << node_name;
  }
  store.UpdateRolloutActionStatus(
      action_id,
      naim::RolloutActionStatus::Acknowledged,
      message.str());
  controller_event_service_.AppendEvent(
      store,
      "rollout-action",
      "eviction-enqueued",
      message.str(),
      nlohmann::json{
          {"victims", action->victim_worker_names},
          {"target_node", action->target_node_name},
          {"target_gpu", action->target_gpu_device},
      },
      desired_state->plane_name,
      action->target_node_name,
      action->worker_name,
      std::nullopt,
      action_id,
      "info");

  std::cout << "enqueued rollout eviction action id=" << action_id << "\n";
  controller_print_service_.PrintPersistedRolloutActions(
      store.LoadRolloutActions(desired_state->plane_name));
  for (const auto& node_name : node_names) {
    controller_print_service_.PrintHostAssignments(store.LoadHostAssignments(node_name));
  }
  return 0;
}

int SchedulerService::ReconcileRolloutActions() const {
  naim::ControllerStore store(db_path_);
  store.Initialize();

  const auto desired_state = store.LoadDesiredState();
  const auto desired_generation = store.LoadDesiredGeneration();
  if (!desired_state.has_value() || !desired_generation.has_value()) {
    throw std::runtime_error("no desired state found in controller db");
  }

  const auto all_rollout_actions = store.LoadRolloutActions(desired_state->plane_name);
  std::vector<naim::RolloutActionRecord> rollout_actions;
  for (const auto& action : all_rollout_actions) {
    if (action.desired_generation == *desired_generation) {
      rollout_actions.push_back(action);
    }
  }

  std::cout << "db: " << db_path_ << "\n";
  std::cout << "desired generation: " << *desired_generation << "\n";
  if (rollout_actions.empty()) {
    std::cout << "rollout reconcile: no rollout actions for current generation\n";
    return 0;
  }

  bool changed = false;
  for (const auto& action : rollout_actions) {
    if (action.action == "evict-best-effort") {
      if (action.status == naim::RolloutActionStatus::Pending) {
        const auto existing_assignments = store.LoadHostAssignments();
        const auto eviction_assignments =
            scheduler_execution_support_.BuildEvictionAssignmentsForAction(
            *desired_state,
            *desired_generation,
            action,
            existing_assignments);
        store.EnqueueHostAssignments(
            eviction_assignments,
            "superseded by rollout eviction action id=" + std::to_string(action.id));
        store.UpdateRolloutActionStatus(
            action.id,
            naim::RolloutActionStatus::Acknowledged,
            "controller-managed eviction assignments enqueued");
        std::cout << "rollout reconcile: enqueued eviction action id=" << action.id << "\n";
        changed = true;
        continue;
      }

      if (action.status == naim::RolloutActionStatus::Acknowledged &&
          scheduler_execution_support_.AreRolloutEvictionAssignmentsApplied(
              store.LoadHostAssignments(),
              action.id)) {
        store.UpdateRolloutActionStatus(
            action.id,
            naim::RolloutActionStatus::ReadyToRetry,
            "eviction assignments applied");
        scheduler_execution_support_.MarkWorkersEvicted(
            &store,
            desired_state->plane_name,
            action.victim_worker_names);
        std::cout << "rollout reconcile: eviction action id=" << action.id
                  << " is ready-to-retry\n";
        changed = true;
      }
      continue;
    }

    if (action.action != "retry-placement") {
      continue;
    }

    auto current_action = FindRolloutActionById(
        store.LoadRolloutActions(desired_state->plane_name),
        action.id);
    if (!current_action.has_value()) {
      continue;
    }

    const auto prior_evict_action = scheduler_execution_support_.FindPriorRolloutActionForWorker(
        store.LoadRolloutActions(desired_state->plane_name),
        *current_action,
        "evict-best-effort");
    if (current_action->status == naim::RolloutActionStatus::Pending &&
        prior_evict_action.has_value() &&
        prior_evict_action->status == naim::RolloutActionStatus::ReadyToRetry) {
      store.UpdateRolloutActionStatus(
          current_action->id,
          naim::RolloutActionStatus::ReadyToRetry,
          "preceding eviction completed");
      std::cout << "rollout reconcile: retry action id=" << current_action->id
                << " is ready-to-retry\n";
      changed = true;
      current_action = FindRolloutActionById(
          store.LoadRolloutActions(desired_state->plane_name),
          action.id);
    }

    if (current_action.has_value() &&
        current_action->status == naim::RolloutActionStatus::ReadyToRetry) {
      std::cout << "rollout reconcile: materializing retry action id="
                << current_action->id << "\n";
      return ApplyReadyRolloutAction(current_action->id);
    }
  }

  if (!changed) {
    std::cout << "rollout reconcile: no state changes\n";
  }
  controller_print_service_.PrintPersistedRolloutActions(
      store.LoadRolloutActions(desired_state->plane_name));
  if (const auto state = store.LoadDesiredState(); state.has_value()) {
    if (store.LoadDesiredGeneration().has_value()) {
      const auto rollout_view = state_aggregate_loader_.LoadRolloutActionsViewData(
          db_path_,
          std::nullopt,
          state->plane_name);
      scheduler_view_service_.PrintRolloutLifecycleEntries(
          std::cout,
          rollout_view.lifecycle);
    }
  }
  return 0;
}

int SchedulerService::ApplyReadyRolloutAction(int action_id) const {
  naim::ControllerStore store(db_path_);
  store.Initialize();

  const auto desired_state = store.LoadDesiredState();
  const auto desired_generation = store.LoadDesiredGeneration();
  if (!desired_state.has_value() || !desired_generation.has_value()) {
    throw std::runtime_error("no desired state found in controller db");
  }

  const auto rollout_actions = store.LoadRolloutActions(desired_state->plane_name);
  const auto action = FindRolloutActionById(rollout_actions, action_id);
  if (!action.has_value()) {
    throw std::runtime_error(
        "rollout action id=" + std::to_string(action_id) + " not found");
  }
  if (action->status != naim::RolloutActionStatus::ReadyToRetry) {
    throw std::runtime_error(
        "rollout action id=" + std::to_string(action_id) +
        " is not ready-to-retry; current status=" +
        naim::ToString(action->status));
  }
  if (action->action != "retry-placement") {
    throw std::runtime_error(
        "rollout action id=" + std::to_string(action_id) +
        " is not a retry-placement action");
  }

  std::vector<std::string> victim_worker_names;
  for (const auto& candidate_action : rollout_actions) {
    if (candidate_action.desired_generation != action->desired_generation ||
        candidate_action.worker_name != action->worker_name ||
        candidate_action.step >= action->step) {
      continue;
    }
    if (candidate_action.status != naim::RolloutActionStatus::ReadyToRetry) {
      throw std::runtime_error(
          "prior rollout step id=" + std::to_string(candidate_action.id) +
          " is not ready-to-retry");
    }
    if (candidate_action.action == "evict-best-effort") {
      victim_worker_names = candidate_action.victim_worker_names;
    }
  }

  naim::DesiredState updated_state = *desired_state;
  scheduler_execution_support_.MaterializeRetryPlacementAction(
      &updated_state, *action, victim_worker_names);
  naim::RequireSchedulingPolicy(updated_state);
  const naim::SchedulingPolicyReport scheduling_report =
      naim::EvaluateSchedulingPolicy(updated_state);
  const int next_generation = *desired_generation + 1;
  const auto availability_overrides = store.LoadNodeAvailabilityOverrides();
  const auto observations = store.LoadHostObservations();

  store.ReplaceDesiredState(updated_state, next_generation, 0);
  store.ClearSchedulerPlaneRuntime(updated_state.plane_name);
  store.ReplaceRolloutActions(
      updated_state.plane_name, next_generation, scheduling_report.rollout_actions);
  store.ReplaceHostAssignments(
      plane_realization_service_.BuildHostAssignments(
          updated_state,
          artifacts_root_,
          next_generation,
          availability_overrides,
          observations,
          scheduling_report));
  controller_event_service_.AppendEvent(
      store,
      "rollout-action",
      "retry-placement-applied",
      "materialized ready rollout action",
      nlohmann::json{
          {"desired_generation", next_generation},
          {"target_node", action->target_node_name},
          {"target_gpu", action->target_gpu_device},
          {"victims", victim_worker_names},
      },
      updated_state.plane_name,
      action->target_node_name,
      action->worker_name,
      std::nullopt,
      action_id,
      "info");

  std::cout << "applied ready rollout action id=" << action_id << "\n";
  std::cout << "desired generation: " << next_generation << "\n";
  controller_print_service_.PrintStateSummary(updated_state);
  std::cout << naim::RenderSchedulingPolicyReport(scheduling_report);
  controller_print_service_.PrintSchedulerDecisionSummary(updated_state);
  controller_print_service_.PrintRolloutGateSummary(scheduling_report);
  return 0;
}

int SchedulerService::AdvanceActiveSchedulerAction() const {
  naim::ControllerStore store(db_path_);
  store.Initialize();

  const auto desired_state = store.LoadDesiredState();
  const auto desired_generation = store.LoadDesiredGeneration();
  if (!desired_state.has_value() || !desired_generation.has_value()) {
    std::cout << "scheduler active-action: no desired state\n";
    return 0;
  }

  const auto plane_runtime = store.LoadSchedulerPlaneRuntime(desired_state->plane_name);
  if (!plane_runtime.has_value() || plane_runtime->active_action.empty()) {
    std::cout << "scheduler active-action: none\n";
    return 0;
  }

  if (plane_runtime->phase == "rollback-planned") {
    if (plane_runtime->previous_state_json.empty()) {
      throw std::runtime_error(
          "rollback-planned action has no previous desired state payload");
    }
    const naim::DesiredState rollback_state =
        naim::DeserializeDesiredStateJson(plane_runtime->previous_state_json);
    naim::RequireSchedulingPolicy(rollback_state);
    const auto availability_overrides = store.LoadNodeAvailabilityOverrides();
    const auto observations = store.LoadHostObservations();
    const auto rollback_report = naim::EvaluateSchedulingPolicy(rollback_state);
    const int rollback_generation = *desired_generation + 1;
    store.ReplaceDesiredState(rollback_state, rollback_generation, 0);
    store.ReplaceRolloutActions(
        rollback_state.plane_name,
        rollback_generation,
        rollback_report.rollout_actions);
    store.ReplaceHostAssignments(
        plane_realization_service_.BuildHostAssignments(
            rollback_state,
            artifacts_root_,
            rollback_generation,
            availability_overrides,
            observations,
            rollback_report));
    naim::SchedulerPlaneRuntime updated_runtime = *plane_runtime;
    updated_runtime.phase = "rollback-applied";
    updated_runtime.action_generation = rollback_generation;
    updated_runtime.stable_samples = 0;
    updated_runtime.rollback_attempt_count = 1;
    updated_runtime.started_at = runtime_support_service_.UtcNowSqlTimestamp();
    updated_runtime.status_message = "rollback materialized after verification timeout";
    store.UpsertSchedulerPlaneRuntime(updated_runtime);
    controller_event_service_.AppendEvent(
        store,
        "scheduler",
        "rollback-applied",
        updated_runtime.status_message,
        nlohmann::json{
            {"worker", updated_runtime.active_worker_name},
            {"generation", rollback_generation},
            {"phase", updated_runtime.phase},
        },
        rollback_state.plane_name,
        updated_runtime.target_node_name,
        updated_runtime.active_worker_name,
        std::nullopt,
        std::nullopt,
        "info");
    std::cout << "scheduler active-action: rollback-applied worker="
              << updated_runtime.active_worker_name
              << " generation=" << rollback_generation << "\n";
    return 0;
  }

  const auto observations = store.LoadHostObservations();
  const auto verification =
      scheduler_execution_support_.EvaluateSchedulerActionVerification(
          *plane_runtime, observations);
  naim::SchedulerPlaneRuntime updated_runtime = *plane_runtime;
  updated_runtime.stable_samples = verification.next_stable_samples;
  updated_runtime.status_message = verification.detail;

  if (verification.stable) {
    scheduler_execution_support_.MarkWorkerMoveVerified(&store, updated_runtime);
    controller_event_service_.AppendEvent(
        store,
        "scheduler",
        "move-verified",
        verification.detail,
        nlohmann::json{
            {"worker", updated_runtime.active_worker_name},
            {"generation", updated_runtime.action_generation},
            {"phase", updated_runtime.phase},
            {"stable_samples", updated_runtime.stable_samples},
        },
        updated_runtime.plane_name,
        updated_runtime.target_node_name,
        updated_runtime.active_worker_name,
        std::nullopt,
        std::nullopt,
        "info");
    store.ClearSchedulerPlaneRuntime(updated_runtime.plane_name);
    std::cout << "scheduler active-action: verified worker="
              << updated_runtime.active_worker_name
              << " phase=" << updated_runtime.phase << "\n";
    return 0;
  }

  if (!verification.timed_out) {
    store.UpsertSchedulerPlaneRuntime(updated_runtime);
    std::cout << "scheduler active-action: waiting worker="
              << updated_runtime.active_worker_name
              << " phase=" << updated_runtime.phase
              << " stable_samples=" << updated_runtime.stable_samples << "/"
              << verification_stable_samples_required_
              << " detail=" << verification.detail << "\n";
    return 0;
  }

  if (updated_runtime.rollback_attempt_count == 0 &&
      !updated_runtime.previous_state_json.empty()) {
    naim::SchedulerWorkerRuntime worker_runtime;
    if (const auto current = store.LoadSchedulerWorkerRuntime(updated_runtime.active_worker_name);
        current.has_value()) {
      worker_runtime = *current;
    }
    worker_runtime.plane_name = updated_runtime.plane_name;
    worker_runtime.worker_name = updated_runtime.active_worker_name;
    worker_runtime.last_scheduler_phase = "failed-verification";
    worker_runtime.last_status_message = verification.detail;
    worker_runtime.manual_intervention_required = false;
    store.UpsertSchedulerWorkerRuntime(worker_runtime);
    updated_runtime.phase = "rollback-planned";
    updated_runtime.stable_samples = 0;
    updated_runtime.started_at = runtime_support_service_.UtcNowSqlTimestamp();
    updated_runtime.status_message = "verification timed out; rollback planned";
    store.UpsertSchedulerPlaneRuntime(updated_runtime);
    controller_event_service_.AppendEvent(
        store,
        "scheduler",
        "rollback-planned",
        verification.detail,
        nlohmann::json{
            {"worker", updated_runtime.active_worker_name},
            {"generation", updated_runtime.action_generation},
            {"phase", updated_runtime.phase},
        },
        updated_runtime.plane_name,
        updated_runtime.target_node_name,
        updated_runtime.active_worker_name,
        std::nullopt,
        std::nullopt,
        "warning");
    std::cout << "scheduler active-action: rollback-planned worker="
              << updated_runtime.active_worker_name
              << " generation=" << updated_runtime.action_generation << "\n";
    return 0;
  }

  naim::SchedulerWorkerRuntime worker_runtime;
  if (const auto current = store.LoadSchedulerWorkerRuntime(updated_runtime.active_worker_name);
      current.has_value()) {
    worker_runtime = *current;
  }
  worker_runtime.plane_name = updated_runtime.plane_name;
  worker_runtime.worker_name = updated_runtime.active_worker_name;
  worker_runtime.last_scheduler_phase = "manual-intervention-required";
  worker_runtime.last_status_message = verification.detail;
  worker_runtime.manual_intervention_required = true;
  store.UpsertSchedulerWorkerRuntime(worker_runtime);
  controller_event_service_.AppendEvent(
      store,
      "scheduler",
      "manual-intervention-required",
      verification.detail,
      nlohmann::json{
          {"worker", updated_runtime.active_worker_name},
          {"generation", updated_runtime.action_generation},
          {"phase", updated_runtime.phase},
      },
      updated_runtime.plane_name,
      updated_runtime.target_node_name,
      updated_runtime.active_worker_name,
      std::nullopt,
      std::nullopt,
      "error");
  store.ClearSchedulerPlaneRuntime(updated_runtime.plane_name);
  std::cout << "scheduler active-action: manual-intervention-required worker="
            << updated_runtime.active_worker_name
            << " detail=" << verification.detail << "\n";
  return 0;
}

int SchedulerService::SetRolloutActionStatus(
    int action_id,
    const std::string& requested_status,
    const std::optional<std::string>& message) const {
  return SetRolloutActionStatus(
      action_id,
      naim::ParseRolloutActionStatus(requested_status),
      message);
}

}  // namespace naim::controller
