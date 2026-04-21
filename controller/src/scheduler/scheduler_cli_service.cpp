#include "scheduler/scheduler_cli_service.h"

#include <iostream>
#include <stdexcept>
#include <utility>

namespace naim::controller {

SchedulerCliService::SchedulerCliService(
    const StateAggregateLoader& state_aggregate_loader,
    const SchedulerViewService& scheduler_view_service,
    const ControllerPrintService& controller_print_service,
    int default_stale_after_seconds,
    int verification_stable_samples_required)
    : state_aggregate_loader_(state_aggregate_loader),
      scheduler_view_service_(scheduler_view_service),
      controller_print_service_(controller_print_service),
      default_stale_after_seconds_(default_stale_after_seconds),
      verification_stable_samples_required_(verification_stable_samples_required) {}

int SchedulerCliService::ShowRolloutActions(
    const std::string& db_path,
    const std::optional<std::string>& node_name,
    const std::optional<std::string>& plane_name) const {
  const auto view =
      state_aggregate_loader_.LoadRolloutActionsViewData(db_path, node_name, plane_name);

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

int SchedulerCliService::ShowRebalancePlan(
    const std::string& db_path,
    const std::optional<std::string>& node_name,
    const std::optional<std::string>& plane_name) const {
  const auto view = state_aggregate_loader_.LoadRebalancePlanViewData(
      db_path,
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

}  // namespace naim::controller
