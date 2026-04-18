#include "host/host_assignment_reconciliation_service.h"

#include <set>

namespace naim::controller {

HostAssignmentReconciliationService::Result
HostAssignmentReconciliationService::Reconcile(
    naim::ControllerStore& store,
    const std::optional<std::string>& plane_name) const {
  const auto claimed_assignments = LoadClaimedApplyAssignments(store, plane_name);
  if (claimed_assignments.empty()) {
    return {};
  }

  const auto observations = store.LoadHostObservations();
  Result result;
  for (const auto& candidate_plane_name : BuildPlaneNames(claimed_assignments)) {
    const Result plane_result =
        ReconcilePlane(store, candidate_plane_name, claimed_assignments, observations);
    result.applied += plane_result.applied;
    result.superseded += plane_result.superseded;
  }
  return result;
}

HostAssignmentReconciliationService::Result
HostAssignmentReconciliationService::ReconcilePlane(
    naim::ControllerStore& store,
    const std::string& plane_name,
    const std::vector<naim::HostAssignment>& claimed_assignments,
    const std::vector<naim::HostObservation>& observations) const {
  const auto plane = store.LoadPlane(plane_name);
  const auto latest_assignments_by_node =
      BuildLatestAssignmentsByNode(store.LoadHostAssignments(std::nullopt, std::nullopt, plane_name));

  Result result;
  for (const auto& assignment : claimed_assignments) {
    if (assignment.plane_name != plane_name) {
      continue;
    }

    if (ShouldSupersedeClaimedAssignment(assignment, latest_assignments_by_node)) {
      if (store.TransitionClaimedHostAssignment(
              assignment.id,
              naim::HostAssignmentStatus::Superseded,
              "superseded by controller reconciliation after a newer assignment replaced it")) {
        ++result.superseded;
      }
      continue;
    }

    const auto observation = FindObservationForNode(observations, assignment.node_name);
    if (ShouldMarkClaimedAssignmentApplied(assignment, plane, observation)) {
      if (store.TransitionClaimedHostAssignment(
              assignment.id,
              naim::HostAssignmentStatus::Applied,
              "marked applied by controller reconciliation after plane convergence")) {
        ++result.applied;
      }
    }
  }

  return result;
}

std::vector<naim::HostAssignment>
HostAssignmentReconciliationService::LoadClaimedApplyAssignments(
    naim::ControllerStore& store,
    const std::optional<std::string>& plane_name) const {
  std::vector<naim::HostAssignment> claimed_apply_assignments;
  for (const auto& assignment :
       store.LoadHostAssignments(std::nullopt, naim::HostAssignmentStatus::Claimed, plane_name)) {
    if (assignment.assignment_type == "apply-node-state") {
      claimed_apply_assignments.push_back(assignment);
    }
  }
  return claimed_apply_assignments;
}

std::vector<std::string> HostAssignmentReconciliationService::BuildPlaneNames(
    const std::vector<naim::HostAssignment>& claimed_assignments) const {
  std::set<std::string> plane_names;
  for (const auto& assignment : claimed_assignments) {
    plane_names.insert(assignment.plane_name);
  }
  return {plane_names.begin(), plane_names.end()};
}

std::map<std::string, naim::HostAssignment>
HostAssignmentReconciliationService::BuildLatestAssignmentsByNode(
    const std::vector<naim::HostAssignment>& assignments) const {
  std::map<std::string, naim::HostAssignment> latest_assignments_by_node;
  for (const auto& assignment : assignments) {
    auto it = latest_assignments_by_node.find(assignment.node_name);
    if (it == latest_assignments_by_node.end() || assignment.id > it->second.id) {
      latest_assignments_by_node[assignment.node_name] = assignment;
    }
  }
  return latest_assignments_by_node;
}

std::optional<naim::HostObservation>
HostAssignmentReconciliationService::FindObservationForNode(
    const std::vector<naim::HostObservation>& observations,
    const std::string& node_name) const {
  for (const auto& observation : observations) {
    if (observation.node_name == node_name) {
      return observation;
    }
  }
  return std::nullopt;
}

bool HostAssignmentReconciliationService::ShouldSupersedeClaimedAssignment(
    const naim::HostAssignment& assignment,
    const std::map<std::string, naim::HostAssignment>& latest_assignments_by_node) const {
  const auto latest_assignment_it = latest_assignments_by_node.find(assignment.node_name);
  if (latest_assignment_it == latest_assignments_by_node.end()) {
    return false;
  }
  return latest_assignment_it->second.id != assignment.id;
}

bool HostAssignmentReconciliationService::ShouldMarkClaimedAssignmentApplied(
    const naim::HostAssignment& assignment,
    const std::optional<naim::PlaneRecord>& plane,
    const std::optional<naim::HostObservation>& observation) const {
  if (!plane.has_value() || plane->applied_generation < assignment.desired_generation) {
    return false;
  }
  if (!observation.has_value() ||
      observation->plane_name != assignment.plane_name ||
      observation->status == naim::HostObservationStatus::Failed) {
    return false;
  }
  if (!observation->last_assignment_id.has_value() ||
      *observation->last_assignment_id < assignment.id) {
    return false;
  }
  return observation->applied_generation.has_value() &&
         *observation->applied_generation >= assignment.desired_generation;
}

}  // namespace naim::controller
