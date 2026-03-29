#include "plane/plane_service.h"

#include <algorithm>
#include <iostream>
#include <set>
#include <stdexcept>
#include <utility>

#include "plane/plane_deletion_support.h"

namespace comet::controller {

PlaneService::PlaneService(
    std::string db_path,
    PlaneTimestampFormatter timestamp_formatter,
    PlaneStateSummaryPrinter state_summary_printer,
    PlaneStatePreparer state_preparer,
    PlaneEventAppender event_appender,
    PlaneDeleteFinalizer can_finalize_deleted_plane,
    PlaneHostAssignmentFinder find_latest_host_assignment,
    PlaneStartAssignmentBuilder build_start_assignments,
    PlaneStopAssignmentBuilder build_stop_assignments,
    PlaneDeleteAssignmentBuilder build_delete_assignments,
    DefaultArtifactsRootProvider default_artifacts_root_provider)
    : db_path_(std::move(db_path)),
      timestamp_formatter_(std::move(timestamp_formatter)),
      state_summary_printer_(std::move(state_summary_printer)),
      state_preparer_(std::move(state_preparer)),
      event_appender_(std::move(event_appender)),
      can_finalize_deleted_plane_(std::move(can_finalize_deleted_plane)),
      find_latest_host_assignment_(std::move(find_latest_host_assignment)),
      build_start_assignments_(std::move(build_start_assignments)),
      build_stop_assignments_(std::move(build_stop_assignments)),
      build_delete_assignments_(std::move(build_delete_assignments)),
      default_artifacts_root_provider_(std::move(default_artifacts_root_provider)) {}

int PlaneService::ListPlanes() const {
  comet::ControllerStore store(db_path_);
  store.Initialize();
  const auto planes = store.LoadPlanes();
  if (planes.empty()) {
    std::cout << "planes: empty\n";
    return 0;
  }

  std::cout << "planes:\n";
  for (const auto& plane : planes) {
    std::cout << "  - name=" << plane.name << " state=" << plane.state
              << " generation=" << plane.generation
              << " rebalance_iteration=" << plane.rebalance_iteration << "\n";
  }
  return 0;
}

int PlaneService::ShowPlane(const std::string& plane_name) const {
  comet::ControllerStore store(db_path_);
  store.Initialize();
  plane_deletion_support::FinalizeDeletedPlaneIfReady(
      store,
      plane_name,
      can_finalize_deleted_plane_,
      event_appender_);
  const auto state = store.LoadDesiredState(plane_name);
  const auto plane = store.LoadPlane(plane_name);
  if (!state.has_value() || !plane.has_value()) {
    throw std::runtime_error("plane '" + plane_name + "' not found");
  }

  std::cout << "plane:\n";
  std::cout << "  name=" << plane->name << "\n";
  std::cout << "  state=" << plane->state << "\n";
  std::cout << "  generation=" << plane->generation << "\n";
  std::cout << "  rebalance_iteration=" << plane->rebalance_iteration << "\n";
  std::cout << "  created_at=" << timestamp_formatter_(plane->created_at) << "\n";
  state_summary_printer_(*state);
  return 0;
}

int PlaneService::StartPlane(const std::string& plane_name) const {
  comet::ControllerStore store(db_path_);
  store.Initialize();
  const auto plane = store.LoadPlane(plane_name);
  auto desired_state = store.LoadDesiredState(plane_name);
  if (!plane.has_value()) {
    throw std::runtime_error("plane '" + plane_name + "' not found");
  }
  if (!desired_state.has_value()) {
    throw std::runtime_error("desired state for plane '" + plane_name + "' not found");
  }

  state_preparer_(store, &*desired_state);
  if (plane->state == "running") {
    std::cout << "plane already running: " << plane_name << "\n";
    return 0;
  }
  if (!store.UpdatePlaneState(plane_name, "running")) {
    throw std::runtime_error("failed to update plane state for '" + plane_name + "'");
  }

  const auto availability_overrides = store.LoadNodeAvailabilityOverrides();
  const auto observations = store.LoadHostObservations();
  const auto scheduling_report = comet::EvaluateSchedulingPolicy(*desired_state);
  const std::string artifacts_root = [&]() {
    if (!plane->artifacts_root.empty()) {
      return plane->artifacts_root;
    }
    const auto assignments = store.LoadHostAssignments();
    const auto plane_assignment = find_latest_host_assignment_(assignments, plane_name);
    return plane_assignment.has_value() ? plane_assignment->artifacts_root
                                        : default_artifacts_root_provider_();
  }();

  store.ReplaceRolloutActions(
      desired_state->plane_name, plane->generation, scheduling_report.rollout_actions);
  store.EnqueueHostAssignments(
      build_start_assignments_(
          *desired_state,
          artifacts_root,
          plane->generation,
          availability_overrides,
          observations,
          scheduling_report),
      "superseded by start-plane lifecycle transition");
  event_appender_(
      store,
      "plane",
      "started",
      "plane lifecycle moved to running and apply assignments were queued",
      nlohmann::json{
          {"previous_state", plane->state},
          {"next_state", "running"},
          {"desired_generation", plane->generation},
      },
      plane_name);
  std::cout << "plane started: " << plane_name
            << " desired_generation=" << plane->generation << "\n";
  return 0;
}

int PlaneService::StopPlane(const std::string& plane_name) const {
  comet::ControllerStore store(db_path_);
  store.Initialize();
  const auto plane = store.LoadPlane(plane_name);
  const auto desired_state = store.LoadDesiredState(plane_name);
  if (!plane.has_value()) {
    throw std::runtime_error("plane '" + plane_name + "' not found");
  }
  if (!desired_state.has_value()) {
    throw std::runtime_error("desired state for plane '" + plane_name + "' not found");
  }
  if (plane->state == "stopped") {
    std::cout << "plane already stopped: " << plane_name << "\n";
    return 0;
  }

  const int superseded = store.SupersedeHostAssignmentsForPlane(
      plane_name,
      "superseded by stop-plane controller lifecycle transition");
  const auto availability_overrides = store.LoadNodeAvailabilityOverrides();
  const std::string artifacts_root = [&]() {
    if (!plane->artifacts_root.empty()) {
      return plane->artifacts_root;
    }
    const auto assignments = store.LoadHostAssignments();
    const auto plane_assignment = find_latest_host_assignment_(assignments, plane_name);
    return plane_assignment.has_value() ? plane_assignment->artifacts_root
                                        : default_artifacts_root_provider_();
  }();

  store.EnqueueHostAssignments(
      build_stop_assignments_(
          *desired_state,
          plane->generation,
          artifacts_root,
          availability_overrides),
      "superseded by stop-plane lifecycle transition");
  if (!store.UpdatePlaneState(plane_name, "stopped")) {
    throw std::runtime_error("failed to update plane state for '" + plane_name + "'");
  }
  event_appender_(
      store,
      "plane",
      "stopped",
      "plane lifecycle moved to stopped and stop assignments were queued",
      nlohmann::json{
          {"previous_state", plane->state},
          {"next_state", "stopped"},
          {"superseded_assignments", superseded},
          {"desired_generation", plane->generation},
      },
      plane_name);
  std::cout << "plane stopped: " << plane_name
            << " superseded_assignments=" << superseded
            << " desired_generation=" << plane->generation << "\n";
  return 0;
}

int PlaneService::DeletePlane(const std::string& plane_name) const {
  comet::ControllerStore store(db_path_);
  store.Initialize();
  const auto plane = store.LoadPlane(plane_name);
  const auto desired_state = store.LoadDesiredState(plane_name);
  if (!plane.has_value()) {
    throw std::runtime_error("plane '" + plane_name + "' not found");
  }
  if (!desired_state.has_value()) {
    throw std::runtime_error("desired state for plane '" + plane_name + "' not found");
  }

  if (plane->state == "deleting" && can_finalize_deleted_plane_(store, plane_name)) {
    store.DeletePlane(plane_name);
    event_appender_(
        store,
        "plane",
        "deleted",
        "plane deleted from controller registry after cleanup convergence",
        nlohmann::json{
            {"plane_name", plane_name},
            {"deleted_generation", plane->generation},
        },
        "");
    std::cout << "plane deleted: " << plane_name
              << " desired_generation=" << plane->generation << "\n";
    return 0;
  }

  const int superseded = store.SupersedeHostAssignmentsForPlane(
      plane_name,
      "superseded by delete-plane controller lifecycle transition");
  const std::string artifacts_root = [&]() {
    if (!plane->artifacts_root.empty()) {
      return plane->artifacts_root;
    }
    const auto assignments = store.LoadHostAssignments(std::nullopt, std::nullopt, plane_name);
    const auto plane_assignment = find_latest_host_assignment_(assignments, plane_name);
    return plane_assignment.has_value() ? plane_assignment->artifacts_root
                                        : default_artifacts_root_provider_();
  }();
  if (!store.UpdatePlaneState(plane_name, "deleting")) {
    throw std::runtime_error("failed to update plane state for '" + plane_name + "'");
  }

  const auto all_observations = store.LoadHostObservations();
  const auto registered_hosts = store.LoadRegisteredHosts();
  std::set<std::string> cleanup_nodes;
  std::vector<std::string> skipped_nodes;
  for (const auto& node : desired_state->nodes) {
    const bool has_observation = std::any_of(
        all_observations.begin(),
        all_observations.end(),
        [&](const auto& observation) { return observation.node_name == node.name; });
    const bool connected_host = std::any_of(
        registered_hosts.begin(),
        registered_hosts.end(),
        [&](const auto& host) {
          return host.node_name == node.name &&
                 host.registration_state == "registered" &&
                 host.session_state == "connected";
        });
    if (has_observation || connected_host) {
      cleanup_nodes.insert(node.name);
    } else {
      skipped_nodes.push_back(node.name);
    }
  }

  store.ReplaceRolloutActions(plane_name, plane->generation, {});
  store.ClearSchedulerPlaneRuntime(plane_name);
  store.EnqueueHostAssignments(
      [&]() {
        comet::DesiredState cleanup_state = *desired_state;
        std::vector<comet::NodeInventory> nodes;
        for (const auto& node : cleanup_state.nodes) {
          if (cleanup_nodes.count(node.name) > 0) {
            nodes.push_back(node);
          }
        }
        cleanup_state.nodes = std::move(nodes);
        return build_delete_assignments_(cleanup_state, plane->generation, artifacts_root);
      }(),
      "superseded by delete-plane lifecycle transition");
  event_appender_(
      store,
      "plane",
      "delete-requested",
      "plane delete was requested and cleanup assignments were queued",
      nlohmann::json{
          {"previous_state", plane->state},
          {"next_state", "deleting"},
          {"superseded_assignments", superseded},
          {"desired_generation", plane->generation},
          {"cleanup_nodes", cleanup_nodes},
          {"skipped_nodes", skipped_nodes},
      },
      plane_name);
  std::cout << "plane delete started: " << plane_name
            << " desired_generation=" << plane->generation << "\n";
  return 0;
}

}  // namespace comet::controller
