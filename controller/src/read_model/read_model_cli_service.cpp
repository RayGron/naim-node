#include "read_model/read_model_cli_service.h"

#include <iostream>
#include <stdexcept>
#include <utility>

namespace naim::controller {

namespace {

struct HostAssignmentsViewData {
  std::string db_path;
  std::optional<std::string> node_name;
  std::vector<naim::HostAssignment> assignments;
};

struct HostObservationsViewData {
  std::string db_path;
  std::optional<std::string> plane_name;
  std::optional<std::string> node_name;
  int stale_after_seconds = 0;
  std::vector<naim::HostObservation> observations;
};

struct HostHealthViewData {
  std::string db_path;
  std::optional<std::string> node_name;
  int stale_after_seconds = 0;
  std::optional<naim::DesiredState> desired_state;
  std::vector<naim::HostObservation> observations;
  std::vector<naim::NodeAvailabilityOverride> availability_overrides;
};

struct DiskStateViewData {
  std::string db_path;
  std::optional<std::string> plane_name;
  std::optional<std::string> node_name;
  std::optional<naim::DesiredState> desired_state;
  std::optional<int> desired_generation;
  std::vector<naim::DiskRuntimeState> runtime_states;
  std::vector<naim::HostObservation> observations;
};

struct EventsViewData {
  std::string db_path;
  std::optional<std::string> plane_name;
  std::optional<std::string> node_name;
  std::optional<std::string> worker_name;
  std::optional<std::string> category;
  int limit = 100;
  std::vector<naim::EventRecord> events;
};

}  // namespace

ReadModelCliService::ReadModelCliService(
    const ControllerPrintService& controller_print_service,
    const StateAggregateLoader& state_aggregate_loader,
    const SchedulerViewService& scheduler_view_service,
    int default_stale_after_seconds,
    int verification_stable_samples_required)
    : controller_print_service_(controller_print_service),
      state_aggregate_loader_(state_aggregate_loader),
      scheduler_view_service_(scheduler_view_service),
      default_stale_after_seconds_(default_stale_after_seconds),
      verification_stable_samples_required_(verification_stable_samples_required) {}

int ReadModelCliService::ShowHostAssignments(
    const std::string& db_path,
    const std::optional<std::string>& node_name) const {
  naim::ControllerStore store(db_path);
  store.Initialize();
  const HostAssignmentsViewData view{
      db_path,
      node_name,
      store.LoadHostAssignments(node_name),
  };
  std::cout << "db: " << view.db_path << "\n";
  controller_print_service_.PrintHostAssignments(view.assignments);
  return 0;
}

int ReadModelCliService::ShowHostObservations(
    const std::string& db_path,
    const std::optional<std::string>& plane_name,
    const std::optional<std::string>& node_name,
    int stale_after_seconds) const {
  naim::ControllerStore store(db_path);
  store.Initialize();
  const auto observations = plane_name.has_value()
                                ? plane_observation_matcher_.FilterHostObservationsForPlane(
                                      store.LoadHostObservations(node_name),
                                      *plane_name)
                                : store.LoadHostObservations(node_name);
  const HostObservationsViewData view{
      db_path,
      plane_name,
      node_name,
      stale_after_seconds,
      observations,
  };
  std::cout << "db: " << view.db_path << "\n";
  if (view.plane_name.has_value()) {
    std::cout << "plane: " << *view.plane_name << "\n";
  }
  std::cout << "stale_after_seconds: " << view.stale_after_seconds << "\n";
  controller_print_service_.PrintHostObservations(
      view.observations, view.stale_after_seconds);
  return 0;
}

int ReadModelCliService::ShowNodeAvailability(
    const std::string& db_path,
    const std::optional<std::string>& node_name) const {
  naim::ControllerStore store(db_path);
  store.Initialize();

  std::cout << "db: " << db_path << "\n";
  controller_print_service_.PrintNodeAvailabilityOverrides(
      store.LoadNodeAvailabilityOverrides(node_name));
  return 0;
}

int ReadModelCliService::ShowHostHealth(
    const std::string& db_path,
    const std::optional<std::string>& node_name,
    int stale_after_seconds) const {
  naim::ControllerStore store(db_path);
  store.Initialize();
  const HostHealthViewData view{
      db_path,
      node_name,
      stale_after_seconds,
      store.LoadDesiredState(),
      store.LoadHostObservations(node_name),
      store.LoadNodeAvailabilityOverrides(node_name),
  };
  std::cout << "db: " << view.db_path << "\n";
  std::cout << "stale_after_seconds: " << view.stale_after_seconds << "\n";
  controller_print_service_.PrintHostHealth(
      view.desired_state,
      view.observations,
      view.availability_overrides,
      view.node_name,
      view.stale_after_seconds);
  return 0;
}

int ReadModelCliService::ShowEvents(
    const std::string& db_path,
    const std::optional<std::string>& plane_name,
    const std::optional<std::string>& node_name,
    const std::optional<std::string>& worker_name,
    const std::optional<std::string>& category,
    int limit) const {
  naim::ControllerStore store(db_path);
  store.Initialize();
  const EventsViewData view{
      db_path,
      plane_name,
      node_name,
      worker_name,
      category,
      limit,
      store.LoadEvents(
          plane_name,
          node_name,
          worker_name,
          category,
          limit),
  };
  std::cout << "db: " << view.db_path << "\n";
  std::cout << "limit: " << view.limit << "\n";
  if (view.plane_name.has_value()) {
    std::cout << "plane: " << *view.plane_name << "\n";
  }
  if (view.node_name.has_value()) {
    std::cout << "node: " << *view.node_name << "\n";
  }
  if (view.worker_name.has_value()) {
    std::cout << "worker: " << *view.worker_name << "\n";
  }
  if (view.category.has_value()) {
    std::cout << "category: " << *view.category << "\n";
  }
  controller_print_service_.PrintEvents(view.events);
  return 0;
}

int ReadModelCliService::ShowState(const std::string& db_path) const {
  const auto view = state_aggregate_loader_.LoadStateAggregateViewData(
      db_path, default_stale_after_seconds_);
  if (!view.desired_state.has_value()) {
    std::cout << "state: empty\n";
    return 0;
  }

  std::cout << "db: " << view.db_path << "\n";
  if (view.desired_generation.has_value()) {
    std::cout << "desired generation: " << *view.desired_generation << "\n";
  }
  controller_print_service_.PrintStateSummary(*view.desired_state);
  controller_print_service_.PrintDiskRuntimeStates(view.disk_runtime_states);
  controller_print_service_.PrintDetailedDiskState(
      *view.desired_state,
      view.disk_runtime_states,
      view.observations,
      std::nullopt);
  std::cout << naim::RenderSchedulingPolicyReport(view.scheduling_report);
  controller_print_service_.PrintSchedulerDecisionSummary(*view.desired_state);
  controller_print_service_.PrintRolloutGateSummary(view.scheduling_report);
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
      view.rebalance_policy_summary);
  scheduler_view_service_.PrintSchedulerRuntimeView(
      std::cout,
      view.scheduler_runtime,
      verification_stable_samples_required_);
  if (view.desired_generation.has_value()) {
    scheduler_view_service_.PrintRolloutLifecycleEntries(
        std::cout,
        view.rollout_lifecycle);
  }
  std::cout << "\n";
  controller_print_service_.PrintNodeAvailabilityOverrides(view.availability_overrides);
  std::cout << "\n";
  controller_print_service_.PrintHostObservations(
      view.observations, view.stale_after_seconds);
  std::cout << "\n";
  controller_print_service_.PrintHostHealth(
      view.desired_state,
      view.observations,
      view.availability_overrides,
      std::nullopt,
      view.stale_after_seconds);
  return 0;
}

int ReadModelCliService::ShowDiskState(
    const std::string& db_path,
    const std::optional<std::string>& node_name,
    const std::optional<std::string>& plane_name) const {
  naim::ControllerStore store(db_path);
  store.Initialize();
  const auto desired_state =
      plane_name.has_value() ? store.LoadDesiredState(*plane_name)
                             : store.LoadDesiredState();
  const DiskStateViewData view{
      db_path,
      plane_name,
      node_name,
      desired_state,
      plane_name.has_value() ? store.LoadDesiredGeneration(*plane_name)
                             : store.LoadDesiredGeneration(),
      desired_state.has_value()
          ? store.LoadDiskRuntimeStates(desired_state->plane_name, node_name)
          : std::vector<naim::DiskRuntimeState>{},
      plane_name.has_value()
          ? plane_observation_matcher_.FilterHostObservationsForPlane(
                store.LoadHostObservations(node_name),
                *plane_name)
          : store.LoadHostObservations(node_name),
  };
  if (!view.desired_state.has_value()) {
    std::cout << "disk-state:\n";
    std::cout << "  (empty)\n";
    return 0;
  }

  std::cout << "db: " << view.db_path << "\n";
  if (view.plane_name.has_value()) {
    std::cout << "plane_filter: " << *view.plane_name << "\n";
  }
  if (view.node_name.has_value()) {
    std::cout << "node_filter: " << *view.node_name << "\n";
  }
  controller_print_service_.PrintDetailedDiskState(
      *view.desired_state,
      view.runtime_states,
      view.observations,
      view.node_name);
  return 0;
}

}  // namespace naim::controller
