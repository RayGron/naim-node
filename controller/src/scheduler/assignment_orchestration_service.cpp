#include "scheduler/assignment_orchestration_service.h"

#include <iostream>

#include "naim/state/state_json.h"

namespace naim::controller {

namespace {

std::optional<naim::HostAssignment> FindLatestHostAssignmentForNodePlane(
    const std::vector<naim::HostAssignment>& assignments,
    const std::string& node_name,
    const std::string& plane_name) {
  std::optional<naim::HostAssignment> result;
  for (const auto& assignment : assignments) {
    if (assignment.node_name != node_name || assignment.plane_name != plane_name) {
      continue;
    }
    result = assignment;
  }
  return result;
}

std::optional<naim::HostAssignment> FindLatestHostAssignmentForPlane(
    const std::vector<naim::HostAssignment>& assignments,
    const std::string& plane_name) {
  std::optional<naim::HostAssignment> result;
  for (const auto& assignment : assignments) {
    if (assignment.plane_name != plane_name) {
      continue;
    }
    result = assignment;
  }
  return result;
}

bool AssignmentRepresentsDrainedNode(const naim::HostAssignment& assignment) {
  return assignment.assignment_type == "drain-node-state" &&
         (assignment.status == naim::HostAssignmentStatus::Pending ||
          assignment.status == naim::HostAssignmentStatus::Claimed ||
          assignment.status == naim::HostAssignmentStatus::Applied);
}

bool ObservedNodeStateNeedsResync(
    const naim::DesiredState& desired_state,
    const std::string& node_name,
    const naim::HostObservation& observation) {
  if (observation.observed_state_json.empty()) {
    return true;
  }

  const naim::DesiredState observed_node_state =
      naim::DeserializeDesiredStateJson(observation.observed_state_json);
  const naim::DesiredState desired_node_state =
      naim::SliceDesiredStateForNode(desired_state, node_name);

  if (desired_node_state.disks.empty() && desired_node_state.instances.empty()) {
    return false;
  }

  std::size_t observed_disk_count = 0;
  std::size_t observed_instance_count = 0;
  for (const auto& disk : observed_node_state.disks) {
    if (disk.node_name == node_name && disk.plane_name == desired_state.plane_name) {
      ++observed_disk_count;
    }
  }
  for (const auto& instance : observed_node_state.instances) {
    if (instance.node_name == node_name && instance.plane_name == desired_state.plane_name) {
      ++observed_instance_count;
    }
  }

  if (!desired_node_state.disks.empty() && observed_disk_count == 0) {
    return true;
  }
  if (!desired_node_state.instances.empty() && observed_instance_count == 0) {
    return true;
  }
  return false;
}

std::optional<naim::NodeInventory> FindNodeInventory(
    const naim::DesiredState& desired_state,
    const std::string& node_name) {
  for (const auto& node : desired_state.nodes) {
    if (node.name == node_name) {
      return node;
    }
  }
  return std::nullopt;
}

}  // namespace

AssignmentOrchestrationService::AssignmentOrchestrationService(
    const ControllerEventService& controller_event_service,
    const ControllerPrintService& controller_print_service,
    std::string default_artifacts_root)
    : controller_event_service_(controller_event_service),
      controller_print_service_(controller_print_service),
      default_artifacts_root_(std::move(default_artifacts_root)) {}

std::optional<naim::HostAssignment>
AssignmentOrchestrationService::BuildResyncAssignmentForNode(
    const naim::DesiredState& desired_state,
    int desired_generation,
    const std::string& node_name,
    const std::vector<naim::HostAssignment>& existing_assignments,
    const std::optional<naim::HostObservation>& observation) const {
  bool node_exists = false;
  for (const auto& node : desired_state.nodes) {
    if (node.name == node_name) {
      node_exists = true;
      break;
    }
  }
  if (!node_exists) {
    return std::nullopt;
  }

  const auto latest_assignment = FindLatestHostAssignmentForNodePlane(
      existing_assignments,
      node_name,
      desired_state.plane_name);
  const bool latest_assignment_is_drain =
      latest_assignment.has_value() &&
      latest_assignment->desired_generation == desired_generation &&
      AssignmentRepresentsDrainedNode(*latest_assignment);

  if (observation.has_value() &&
      observation->applied_generation.has_value() &&
      *observation->applied_generation == desired_generation &&
      observation->status != naim::HostObservationStatus::Failed &&
      !latest_assignment_is_drain &&
      !ObservedNodeStateNeedsResync(desired_state, node_name, *observation)) {
    return std::nullopt;
  }

  if (latest_assignment.has_value() &&
      latest_assignment->desired_generation == desired_generation &&
      latest_assignment->assignment_type == "apply-node-state" &&
      (latest_assignment->status == naim::HostAssignmentStatus::Pending ||
       latest_assignment->status == naim::HostAssignmentStatus::Claimed ||
       latest_assignment->status == naim::HostAssignmentStatus::Applied)) {
    return std::nullopt;
  }

  naim::HostAssignment assignment;
  assignment.node_name = node_name;
  assignment.plane_name = desired_state.plane_name;
  assignment.desired_generation = desired_generation;
  assignment.assignment_type = "apply-node-state";
  assignment.desired_state_json =
      naim::SerializeDesiredStateJson(
          naim::SliceDesiredStateForNode(desired_state, node_name));
  const auto plane_assignment =
      FindLatestHostAssignmentForPlane(existing_assignments, desired_state.plane_name);
  assignment.artifacts_root = latest_assignment.has_value()
                                  ? latest_assignment->artifacts_root
                                  : (plane_assignment.has_value()
                                         ? plane_assignment->artifacts_root
                                         : default_artifacts_root_);
  assignment.status = naim::HostAssignmentStatus::Pending;
  assignment.status_message = "resync after node returned to active";
  return assignment;
}

std::optional<naim::HostAssignment>
AssignmentOrchestrationService::BuildDrainAssignmentForNode(
    const naim::DesiredState& desired_state,
    int desired_generation,
    const std::string& node_name,
    const std::vector<naim::HostAssignment>& existing_assignments) const {
  const auto node = FindNodeInventory(desired_state, node_name);
  if (!node.has_value()) {
    return std::nullopt;
  }

  naim::DesiredState drain_state;
  drain_state.plane_name = desired_state.plane_name;
  drain_state.plane_shared_disk_name = desired_state.plane_shared_disk_name;
  drain_state.control_root = desired_state.control_root;
  drain_state.inference = desired_state.inference;
  drain_state.gateway = desired_state.gateway;
  drain_state.runtime_gpu_nodes = desired_state.runtime_gpu_nodes;
  drain_state.nodes.push_back(*node);

  const auto latest_assignment = FindLatestHostAssignmentForNodePlane(
      existing_assignments,
      node_name,
      desired_state.plane_name);
  const auto plane_assignment =
      FindLatestHostAssignmentForPlane(existing_assignments, desired_state.plane_name);

  naim::HostAssignment assignment;
  assignment.node_name = node_name;
  assignment.plane_name = desired_state.plane_name;
  assignment.desired_generation = desired_generation;
  assignment.assignment_type = "drain-node-state";
  assignment.desired_state_json = naim::SerializeDesiredStateJson(drain_state);
  assignment.artifacts_root = latest_assignment.has_value()
                                  ? latest_assignment->artifacts_root
                                  : (plane_assignment.has_value()
                                         ? plane_assignment->artifacts_root
                                         : default_artifacts_root_);
  assignment.status = naim::HostAssignmentStatus::Pending;
  assignment.status_message = "drain after node availability changed";
  return assignment;
}

int AssignmentOrchestrationService::SetNodeAvailability(
    const std::string& db_path,
    const std::string& node_name,
    naim::NodeAvailability availability,
    const std::optional<std::string>& status_message) const {
  naim::ControllerStore store(db_path);
  store.Initialize();
  const auto previous_override = store.LoadNodeAvailabilityOverride(node_name);
  const auto previous_availability =
      previous_override.has_value() ? previous_override->availability
                                    : naim::NodeAvailability::Active;

  naim::NodeAvailabilityOverride availability_override;
  availability_override.node_name = node_name;
  availability_override.availability = availability;
  availability_override.status_message = status_message.value_or("");
  store.UpsertNodeAvailabilityOverride(availability_override);
  controller_event_service_.AppendEvent(
      store,
      "node-availability",
      "updated",
      "updated node availability override",
      nlohmann::json{
          {"availability", naim::ToString(availability)},
          {"previous_availability", naim::ToString(previous_availability)},
          {"status_message", status_message.value_or("")},
      },
      "",
      node_name,
      "",
      std::nullopt);

  std::cout << "updated node availability for " << node_name << "\n";
  controller_print_service_.PrintNodeAvailabilityOverrides(
      store.LoadNodeAvailabilityOverrides(node_name));

  const auto desired_states = store.LoadDesiredStates();
  if (!desired_states.empty()) {
    const auto existing_assignments = store.LoadHostAssignments();
    const auto node_observation = store.LoadHostObservation(node_name);
    if (previous_availability == naim::NodeAvailability::Active &&
        availability != naim::NodeAvailability::Active) {
      std::vector<naim::HostAssignment> drain_assignments;
      for (const auto& desired_state : desired_states) {
        const auto plane = store.LoadPlane(desired_state.plane_name);
        if (!plane.has_value() || plane->state == "stopped") {
          continue;
        }
        const auto desired_generation = store.LoadDesiredGeneration(desired_state.plane_name);
        if (!desired_generation.has_value()) {
          continue;
        }
        const auto drain_assignment = BuildDrainAssignmentForNode(
            desired_state,
            *desired_generation,
            node_name,
            existing_assignments);
        if (drain_assignment.has_value()) {
          drain_assignments.push_back(*drain_assignment);
        }
      }
      if (!drain_assignments.empty()) {
        store.EnqueueHostAssignments(
            drain_assignments,
            "superseded by node drain for availability transition");
        std::cout << "queued drain assignment for " << node_name
                  << " planes=" << drain_assignments.size() << "\n";
        controller_print_service_.PrintHostAssignments(store.LoadHostAssignments(node_name));
      }
    }

    if (previous_availability != naim::NodeAvailability::Active &&
        availability == naim::NodeAvailability::Active) {
      std::vector<naim::HostAssignment> resync_assignments;
      for (const auto& desired_state : desired_states) {
        const auto plane = store.LoadPlane(desired_state.plane_name);
        if (!plane.has_value() || plane->state == "stopped") {
          continue;
        }
        const auto desired_generation = store.LoadDesiredGeneration(desired_state.plane_name);
        if (!desired_generation.has_value()) {
          continue;
        }
        const auto resync_assignment = BuildResyncAssignmentForNode(
            desired_state,
            *desired_generation,
            node_name,
            existing_assignments,
            node_observation);
        if (resync_assignment.has_value()) {
          resync_assignments.push_back(*resync_assignment);
        }
      }
      if (!resync_assignments.empty()) {
        store.EnqueueHostAssignments(
            resync_assignments,
            "superseded by node reactivation for availability transition");
        std::cout << "queued resync assignment for " << node_name
                  << " planes=" << resync_assignments.size() << "\n";
        controller_print_service_.PrintHostAssignments(store.LoadHostAssignments(node_name));
      } else {
        std::cout << "no resync assignment needed for " << node_name << "\n";
      }
    }
  }
  return 0;
}

int AssignmentOrchestrationService::RetryHostAssignment(
    const std::string& db_path,
    int assignment_id) const {
  naim::ControllerStore store(db_path);
  store.Initialize();

  const auto assignment = store.LoadHostAssignment(assignment_id);
  if (!assignment.has_value()) {
    throw std::runtime_error(
        "host assignment id=" + std::to_string(assignment_id) + " not found");
  }
  if (assignment->status != naim::HostAssignmentStatus::Failed) {
    throw std::runtime_error(
        "host assignment id=" + std::to_string(assignment_id) +
        " is not failed; current status=" + naim::ToString(assignment->status));
  }

  const auto latest_generation = store.LoadDesiredGeneration();
  if (latest_generation.has_value() &&
      assignment->desired_generation != *latest_generation) {
    throw std::runtime_error(
        "host assignment id=" + std::to_string(assignment_id) +
        " belongs to stale desired generation " +
        std::to_string(assignment->desired_generation) +
        "; latest generation is " + std::to_string(*latest_generation));
  }

  if (!store.RetryFailedHostAssignment(
          assignment_id,
          "requeued by operator for desired generation " +
              std::to_string(assignment->desired_generation))) {
    throw std::runtime_error(
        "failed to requeue host assignment id=" + std::to_string(assignment_id));
  }
  controller_event_service_.AppendEvent(
      store,
      "host-assignment",
      "retried",
      "requeued failed host assignment",
      nlohmann::json{
          {"desired_generation", assignment->desired_generation},
          {"assignment_type", assignment->assignment_type},
          {"attempt_count", assignment->attempt_count},
      },
      assignment->plane_name,
      assignment->node_name,
      "",
      assignment_id);

  const auto updated_assignment = store.LoadHostAssignment(assignment_id);
  std::cout << "requeued host assignment id=" << assignment_id << "\n";
  if (updated_assignment.has_value()) {
    controller_print_service_.PrintHostAssignments({*updated_assignment});
  }
  return 0;
}

ControllerActionResult AssignmentOrchestrationService::ExecuteSetNodeAvailabilityAction(
    const std::string& db_path,
    const std::string& node_name,
    naim::NodeAvailability availability,
    const std::optional<std::string>& status_message) const {
  return RunControllerActionResult(
      "set-node-availability",
      [&]() { return SetNodeAvailability(db_path, node_name, availability, status_message); });
}

ControllerActionResult AssignmentOrchestrationService::ExecuteRetryHostAssignmentAction(
    const std::string& db_path,
    int assignment_id) const {
  return RunControllerActionResult(
      "retry-host-assignment",
      [&]() { return RetryHostAssignment(db_path, assignment_id); });
}

}  // namespace naim::controller
