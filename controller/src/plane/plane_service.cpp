#include "plane/plane_service.h"

#include <algorithm>
#include <iostream>
#include <iterator>
#include <map>
#include <memory>
#include <set>
#include <stdexcept>
#include <utility>

#include "host/host_assignment_reconciliation_service.h"
#include "naim/state/state_json.h"

namespace naim::controller {

std::set<std::string> DesiredNodeNames(const naim::DesiredState& state) {
  std::set<std::string> result;
  for (const auto& node : state.nodes) {
    result.insert(node.name);
  }
  return result;
}

std::map<std::string, naim::HostAssignment> LatestAssignmentsByNode(
    const std::vector<naim::HostAssignment>& assignments,
    const std::string& plane_name) {
  std::map<std::string, naim::HostAssignment> result;
  for (const auto& assignment : assignments) {
    if (assignment.plane_name != plane_name) {
      continue;
    }
    result[assignment.node_name] = assignment;
  }
  return result;
}

bool HasNodeLocalRuntimeState(const naim::DesiredState& state) {
  return !state.instances.empty() || !state.disks.empty();
}

std::vector<naim::HostAssignment> BuildRemovedNodeCleanupAssignments(
    const PlaneLifecycleSupport& lifecycle_support,
    const std::vector<naim::HostAssignment>& existing_assignments,
    const naim::DesiredState& desired_state,
    int desired_generation,
    const std::string& artifacts_root) {
  std::vector<naim::HostAssignment> cleanup_assignments;
  const auto desired_nodes = DesiredNodeNames(desired_state);
  const auto latest_assignments = LatestAssignmentsByNode(
      existing_assignments,
      desired_state.plane_name);

  for (const auto& [node_name, assignment] : latest_assignments) {
    if (desired_nodes.count(node_name) > 0 || assignment.desired_state_json.empty()) {
      continue;
    }
    try {
      const auto previous_node_state =
          naim::DeserializeDesiredStateJson(assignment.desired_state_json);
      if (!HasNodeLocalRuntimeState(previous_node_state)) {
        continue;
      }
      auto node_cleanup = lifecycle_support.BuildDeleteAssignments(
          previous_node_state,
          desired_generation,
          artifacts_root);
      cleanup_assignments.insert(
          cleanup_assignments.end(),
          std::make_move_iterator(node_cleanup.begin()),
          std::make_move_iterator(node_cleanup.end()));
    } catch (const std::exception&) {
      continue;
    }
  }

  return cleanup_assignments;
}

PlaneService::PlaneService(
    std::string db_path,
    std::shared_ptr<const PlaneStatePresentationSupport> state_presentation_support,
    std::shared_ptr<const PlaneLifecycleSupport> lifecycle_support)
    : db_path_(std::move(db_path)),
      state_presentation_support_(std::move(state_presentation_support)),
      lifecycle_support_(std::move(lifecycle_support)) {}

bool PlaneService::FinalizeDeletedPlaneIfReady(
    naim::ControllerStore& store,
    const std::string& plane_name) const {
  const auto plane = store.LoadPlane(plane_name);
  if (!plane.has_value() || plane->state != "deleting" ||
      !lifecycle_support_->CanFinalizeDeletedPlane(store, plane_name)) {
    return false;
  }

  store.DeletePlane(plane_name);
  lifecycle_support_->AppendPlaneEvent(
      store,
      "deleted",
      "plane deleted from controller registry after cleanup convergence",
      nlohmann::json{
          {"plane_name", plane_name},
          {"deleted_generation", plane->generation},
      },
      "");
  return true;
}

std::string PlaneService::ResolveArtifactsRoot(
    naim::ControllerStore& store,
    const naim::PlaneRecord& plane,
    const std::string& plane_name) const {
  if (!plane.artifacts_root.empty()) {
    return plane.artifacts_root;
  }
  const auto assignments = store.LoadHostAssignments(std::nullopt, std::nullopt, plane_name);
  const auto plane_assignment =
      lifecycle_support_->FindLatestHostAssignmentForPlane(assignments, plane_name);
  return plane_assignment.has_value() ? plane_assignment->artifacts_root
                                      : lifecycle_support_->DefaultArtifactsRoot();
}

int PlaneService::ListPlanes() const {
  naim::ControllerStore store(db_path_);
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
  naim::ControllerStore store(db_path_);
  store.Initialize();
  (void)FinalizeDeletedPlaneIfReady(store, plane_name);
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
  std::cout << "  created_at=" << state_presentation_support_->FormatTimestamp(plane->created_at)
            << "\n";
  state_presentation_support_->PrintStateSummary(*state);
  return 0;
}

int PlaneService::StartPlane(const std::string& plane_name) const {
  naim::ControllerStore store(db_path_);
  store.Initialize();
  const HostAssignmentReconciliationService reconciliation_service;
  const auto plane = store.LoadPlane(plane_name);
  auto desired_state = store.LoadDesiredState(plane_name);
  if (!plane.has_value()) {
    throw std::runtime_error("plane '" + plane_name + "' not found");
  }
  if (!desired_state.has_value()) {
    throw std::runtime_error("desired state for plane '" + plane_name + "' not found");
  }

  lifecycle_support_->PrepareDesiredState(store, &*desired_state);
  if (plane->state == "running") {
    std::cout << "plane already running: " << plane_name << "\n";
    return 0;
  }
  if (!store.UpdatePlaneState(plane_name, "running")) {
    throw std::runtime_error("failed to update plane state for '" + plane_name + "'");
  }

  const auto availability_overrides = store.LoadNodeAvailabilityOverrides();
  const auto observations = store.LoadHostObservations();
  const auto scheduling_report = naim::EvaluateSchedulingPolicy(*desired_state);
  const std::string artifacts_root = ResolveArtifactsRoot(store, *plane, plane_name);

  store.ReplaceRolloutActions(
      desired_state->plane_name, plane->generation, scheduling_report.rollout_actions);
  auto start_assignments = lifecycle_support_->BuildStartAssignments(
      *desired_state,
      artifacts_root,
      plane->generation,
      availability_overrides,
      observations,
      scheduling_report);
  auto removed_node_cleanup_assignments = BuildRemovedNodeCleanupAssignments(
      *lifecycle_support_,
      store.LoadHostAssignments(std::nullopt, std::nullopt, plane_name),
      *desired_state,
      plane->generation,
      artifacts_root);
  start_assignments.insert(
      start_assignments.end(),
      std::make_move_iterator(removed_node_cleanup_assignments.begin()),
      std::make_move_iterator(removed_node_cleanup_assignments.end()));
  store.EnqueueHostAssignments(
      std::move(start_assignments),
      "superseded by start-plane lifecycle transition");
  (void)reconciliation_service.Reconcile(store, plane_name);
  lifecycle_support_->AppendPlaneEvent(
      store,
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
  naim::ControllerStore store(db_path_);
  store.Initialize();
  const HostAssignmentReconciliationService reconciliation_service;
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
  const std::string artifacts_root = ResolveArtifactsRoot(store, *plane, plane_name);

  store.EnqueueHostAssignments(
      lifecycle_support_->BuildStopAssignments(
          *desired_state,
          plane->generation,
          artifacts_root,
          availability_overrides),
      "superseded by stop-plane lifecycle transition");
  (void)reconciliation_service.Reconcile(store, plane_name);
  if (!store.UpdatePlaneState(plane_name, "stopped")) {
    throw std::runtime_error("failed to update plane state for '" + plane_name + "'");
  }
  lifecycle_support_->AppendPlaneEvent(
      store,
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
  naim::ControllerStore store(db_path_);
  store.Initialize();
  const HostAssignmentReconciliationService reconciliation_service;
  const auto plane = store.LoadPlane(plane_name);
  const auto desired_state = store.LoadDesiredState(plane_name);
  if (!plane.has_value()) {
    throw std::runtime_error("plane '" + plane_name + "' not found");
  }
  if (!desired_state.has_value()) {
    throw std::runtime_error("desired state for plane '" + plane_name + "' not found");
  }

  if (plane->state == "deleting" && FinalizeDeletedPlaneIfReady(store, plane_name)) {
    std::cout << "plane deleted: " << plane_name
              << " desired_generation=" << plane->generation << "\n";
    return 0;
  }

  const int superseded = store.SupersedeHostAssignmentsForPlane(
      plane_name,
      "superseded by delete-plane controller lifecycle transition");
  const std::string artifacts_root = ResolveArtifactsRoot(store, *plane, plane_name);
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
        naim::DesiredState cleanup_state = *desired_state;
        std::vector<naim::NodeInventory> nodes;
        for (const auto& node : cleanup_state.nodes) {
          if (cleanup_nodes.count(node.name) > 0) {
            nodes.push_back(node);
          }
        }
        cleanup_state.nodes = std::move(nodes);
        return lifecycle_support_->BuildDeleteAssignments(
            cleanup_state,
            plane->generation,
            artifacts_root);
      }(),
      "superseded by delete-plane lifecycle transition");
  (void)reconciliation_service.Reconcile(store, plane_name);
  lifecycle_support_->AppendPlaneEvent(
      store,
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

}  // namespace naim::controller
