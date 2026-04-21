#include "read_model/state_aggregate_loader.h"

#include <set>
#include <utility>

namespace naim::controller {

StateAggregateLoader::StateAggregateLoader(
    const SchedulerDomainService& scheduler_domain_service,
    const SchedulerViewService& scheduler_view_service,
    ControllerRuntimeSupportService runtime_support_service,
    int maximum_rebalance_iterations)
    : scheduler_domain_service_(scheduler_domain_service),
      scheduler_view_service_(scheduler_view_service),
      runtime_support_service_(std::move(runtime_support_service)),
      maximum_rebalance_iterations_(maximum_rebalance_iterations) {}

SchedulerRuntimeView StateAggregateLoader::LoadSchedulerRuntimeView(
    naim::ControllerStore& store,
    const std::optional<naim::DesiredState>& desired_state) const {
  SchedulerRuntimeView view;
  if (!desired_state.has_value()) {
    return view;
  }
  view.plane_runtime = store.LoadSchedulerPlaneRuntime(desired_state->plane_name);
  for (const auto& runtime : store.LoadSchedulerWorkerRuntimes(desired_state->plane_name)) {
    view.worker_runtime_by_name.emplace(runtime.worker_name, runtime);
  }
  for (const auto& runtime : store.LoadSchedulerNodeRuntimes(desired_state->plane_name)) {
    view.node_runtime_by_name.emplace(runtime.node_name, runtime);
  }
  return view;
}

RolloutActionsViewData StateAggregateLoader::LoadRolloutActionsViewData(
    const std::string& db_path,
    const std::optional<std::string>& node_name,
    const std::optional<std::string>& plane_name) const {
  naim::ControllerStore store(db_path);
  store.Initialize();

  RolloutActionsViewData view;
  view.db_path = db_path;
  view.plane_name = plane_name;
  view.node_name = node_name;
  view.desired_state =
      plane_name.has_value() ? store.LoadDesiredState(*plane_name)
                             : store.LoadDesiredState();
  view.desired_generation =
      plane_name.has_value() ? store.LoadDesiredGeneration(*plane_name)
                             : store.LoadDesiredGeneration();
  view.actions =
      view.desired_state.has_value()
          ? store.LoadRolloutActions(view.desired_state->plane_name, node_name)
          : store.LoadRolloutActions(plane_name, node_name);

  std::set<std::string> worker_names;
  std::set<std::string> node_names;
  for (const auto& action : view.actions) {
    worker_names.insert(action.worker_name);
    node_names.insert(action.target_node_name);
  }
  view.gated_worker_count = worker_names.size();
  view.gated_node_count = node_names.size();

  if (view.desired_state.has_value()) {
    view.scheduler_runtime = LoadSchedulerRuntimeView(store, view.desired_state);
    if (view.desired_generation.has_value()) {
      const auto plane_assignments =
          store.LoadHostAssignments(std::nullopt, std::nullopt, view.desired_state->plane_name);
      const auto plane_observations = plane_observation_matcher_.FilterHostObservationsForPlane(
          store.LoadHostObservations(),
          view.desired_state->plane_name);
      view.lifecycle = scheduler_domain_service_.BuildRolloutLifecycleEntries(
          *view.desired_state,
          *view.desired_generation,
          view.actions,
          plane_assignments,
          plane_observations);
    }
  }
  return view;
}

RebalancePlanViewData StateAggregateLoader::LoadRebalancePlanViewData(
    const std::string& db_path,
    const std::optional<std::string>& node_name,
    int stale_after_seconds,
    const std::optional<std::string>& plane_name) const {
  naim::ControllerStore store(db_path);
  store.Initialize();

  RebalancePlanViewData view;
  view.db_path = db_path;
  view.plane_name = plane_name;
  view.node_name = node_name;
  view.stale_after_seconds = stale_after_seconds;
  view.desired_state =
      plane_name.has_value() ? store.LoadDesiredState(*plane_name)
                             : store.LoadDesiredState();
  if (!view.desired_state.has_value()) {
    return view;
  }

  view.desired_generation =
      (plane_name.has_value() ? store.LoadDesiredGeneration(*plane_name)
                              : store.LoadDesiredGeneration())
          .value_or(0);
  const auto observations = plane_observation_matcher_.FilterHostObservationsForPlane(
      store.LoadHostObservations(),
      view.desired_state->plane_name);
  const auto assignments =
      store.LoadHostAssignments(std::nullopt, std::nullopt, view.desired_state->plane_name);
  const auto availability_overrides = store.LoadNodeAvailabilityOverrides();
  const auto scheduling_report = naim::EvaluateSchedulingPolicy(*view.desired_state);
  view.scheduler_runtime = LoadSchedulerRuntimeView(store, view.desired_state);
  const auto rollout_actions = store.LoadRolloutActions(view.desired_state->plane_name);
  const auto rollout_lifecycle =
      scheduler_domain_service_.BuildRolloutLifecycleEntries(
          *view.desired_state,
          view.desired_generation,
          rollout_actions,
          assignments,
          observations);
  view.rebalance_entries =
      scheduler_domain_service_.BuildRebalancePlanEntries(
          *view.desired_state,
          scheduling_report,
          availability_overrides,
          rollout_lifecycle,
          assignments,
          view.scheduler_runtime,
          observations,
          stale_after_seconds,
          node_name);
  view.controller_gate_summary =
      scheduler_domain_service_.BuildRebalanceControllerGateSummary(
          *view.desired_state,
          view.desired_generation,
          availability_overrides,
          rollout_lifecycle,
          assignments,
          view.scheduler_runtime,
          observations,
          stale_after_seconds);
  view.iteration_budget_summary =
      scheduler_view_service_.BuildRebalanceIterationBudgetSummary(
          store.LoadRebalanceIteration().value_or(0),
          maximum_rebalance_iterations_);
  view.policy_summary =
      scheduler_view_service_.BuildRebalancePolicySummary(view.rebalance_entries);
  view.loop_status =
      scheduler_view_service_.BuildRebalanceLoopStatusSummary(
          view.controller_gate_summary,
          view.iteration_budget_summary,
          view.policy_summary);
  return view;
}

StateAggregateViewData StateAggregateLoader::LoadStateAggregateViewData(
    const std::string& db_path,
    int stale_after_seconds,
    const std::optional<std::string>& plane_name) const {
  naim::ControllerStore store(db_path);
  store.Initialize();

  StateAggregateViewData view;
  view.db_path = db_path;
  view.stale_after_seconds = stale_after_seconds;
  view.planes = store.LoadPlanes();
  view.desired_states = store.LoadDesiredStates();
  view.desired_state =
      plane_name.has_value() ? store.LoadDesiredState(*plane_name)
                             : store.LoadDesiredState();
  view.desired_generation =
      plane_name.has_value() ? store.LoadDesiredGeneration(*plane_name)
                             : store.LoadDesiredGeneration();
  if (!view.desired_state.has_value()) {
    return view;
  }

  view.disk_runtime_states = store.LoadDiskRuntimeStates(view.desired_state->plane_name);
  view.scheduling_report = naim::EvaluateSchedulingPolicy(*view.desired_state);
  view.observations =
      plane_name.has_value()
          ? plane_observation_matcher_.FilterHostObservationsForPlane(
                store.LoadHostObservations(),
                *plane_name)
          : store.LoadHostObservations();
  view.assignments =
      plane_name.has_value()
          ? store.LoadHostAssignments(std::nullopt, std::nullopt, *plane_name)
          : store.LoadHostAssignments();
  view.availability_overrides = store.LoadNodeAvailabilityOverrides();
  view.scheduler_runtime = LoadSchedulerRuntimeView(store, view.desired_state);
  const auto plane_rollout_actions = store.LoadRolloutActions(view.desired_state->plane_name);
  view.rollout_lifecycle =
      view.desired_generation.has_value()
          ? scheduler_domain_service_.BuildRolloutLifecycleEntries(
                *view.desired_state,
                *view.desired_generation,
                plane_rollout_actions,
                view.assignments,
                view.observations)
          : std::vector<RolloutLifecycleEntry>{};
  view.rebalance_entries =
      scheduler_domain_service_.BuildRebalancePlanEntries(
          *view.desired_state,
          view.scheduling_report,
          view.availability_overrides,
          view.rollout_lifecycle,
          view.assignments,
          view.scheduler_runtime,
          view.observations,
          stale_after_seconds);
  view.controller_gate_summary =
      scheduler_domain_service_.BuildRebalanceControllerGateSummary(
          *view.desired_state,
          view.desired_generation.value_or(0),
          view.availability_overrides,
          view.rollout_lifecycle,
          view.assignments,
          view.scheduler_runtime,
          view.observations,
          stale_after_seconds);
  view.iteration_budget_summary =
      scheduler_view_service_.BuildRebalanceIterationBudgetSummary(
          store.LoadRebalanceIteration().value_or(0),
          maximum_rebalance_iterations_);
  view.rebalance_policy_summary =
      scheduler_view_service_.BuildRebalancePolicySummary(view.rebalance_entries);
  view.loop_status =
      scheduler_view_service_.BuildRebalanceLoopStatusSummary(
          view.controller_gate_summary,
          view.iteration_budget_summary,
          view.rebalance_policy_summary);
  return view;
}

}  // namespace naim::controller
