#include "plane/plane_realization_service.h"

#include <filesystem>
#include <fstream>
#include <map>
#include <set>
#include <sstream>
#include <stdexcept>

#include "comet/planning/compose_renderer.h"
#include "comet/runtime/infer_runtime_config.h"
#include "comet/planning/planner.h"
#include "comet/state/state_json.h"

namespace comet::controller {

namespace {

std::map<std::string, comet::NodeComposePlan> BuildComposePlanMap(
    const comet::DesiredState& state) {
  std::map<std::string, comet::NodeComposePlan> result;
  for (const auto& plan : comet::BuildNodeComposePlans(state)) {
    result.emplace(plan.node_name, plan);
  }
  return result;
}

void WriteTextFile(const std::string& path, const std::string& contents) {
  const std::filesystem::path file_path(path);
  if (file_path.has_parent_path()) {
    std::filesystem::create_directories(file_path.parent_path());
  }

  std::ofstream out(path, std::ios::binary | std::ios::trunc);
  if (!out.is_open()) {
    throw std::runtime_error("failed to open file for write: " + path);
  }
  out << contents;
  if (!out.good()) {
    throw std::runtime_error("failed to write file: " + path);
  }
}

void RemoveFileIfExists(const std::string& path) {
  std::error_code error;
  std::filesystem::remove(path, error);
  if (error) {
    throw std::runtime_error("failed to remove file '" + path + "': " + error.message());
  }
}

std::string InferRuntimeArtifactPath(
    const std::string& artifacts_root,
    const std::string& plane_name) {
  return (std::filesystem::path(artifacts_root) / plane_name / "infer-runtime.json")
      .string();
}

std::string InferRuntimeArtifactPathForInstance(
    const std::string& artifacts_root,
    const std::string& plane_name,
    const std::string& infer_instance_name) {
  return (std::filesystem::path(artifacts_root) / plane_name /
          comet::InferRuntimeConfigRelativePath(infer_instance_name))
      .string();
}

std::map<std::string, std::vector<comet::SchedulerRolloutAction>> BuildRolloutActionsByTargetNode(
    const comet::SchedulingPolicyReport& scheduling_report) {
  std::map<std::string, std::vector<comet::SchedulerRolloutAction>> result;
  for (const auto& action : scheduling_report.rollout_actions) {
    result[action.target_node_name].push_back(action);
  }
  return result;
}

comet::DesiredState BuildStoppedPlaneNodeState(
    const comet::DesiredState& desired_state,
    const std::string& node_name) {
  comet::DesiredState stopped_state = comet::SliceDesiredStateForNode(desired_state, node_name);
  stopped_state.instances.clear();
  return stopped_state;
}

comet::DesiredState BuildDeletedPlaneNodeState(
    const comet::DesiredState& desired_state,
    const std::string& node_name) {
  comet::DesiredState deleted_state = comet::SliceDesiredStateForNode(desired_state, node_name);
  deleted_state.instances.clear();
  deleted_state.disks.clear();
  deleted_state.plane_shared_disk_name.clear();
  deleted_state.control_root.clear();
  return deleted_state;
}

}  // namespace

PlaneRealizationService::PlaneRealizationService(
    const ControllerRuntimeSupportService* runtime_support_service,
    int default_stale_after_seconds)
    : runtime_support_service_(runtime_support_service),
      default_stale_after_seconds_(default_stale_after_seconds) {}

bool PlaneRealizationService::IsNodeSchedulable(comet::NodeAvailability availability) const {
  return availability == comet::NodeAvailability::Active;
}

std::optional<std::string> PlaneRealizationService::ObservedSchedulingGateReason(
    const std::vector<comet::HostObservation>& observations,
    const std::string& node_name,
    int stale_after_seconds) const {
  if (runtime_support_service_ == nullptr) {
    throw std::runtime_error("plane realization runtime support is not configured");
  }
  const auto observation =
      runtime_support_service_->FindHostObservationForNode(observations, node_name);
  if (!observation.has_value()) {
    return std::nullopt;
  }
  if (observation->status == comet::HostObservationStatus::Failed) {
    return std::string("failed");
  }
  const auto age_seconds =
      runtime_support_service_->HeartbeatAgeSeconds(observation->heartbeat_at);
  if (runtime_support_service_->HealthFromAge(age_seconds, stale_after_seconds) ==
      "stale") {
    return std::string("stale");
  }
  const auto runtime_status =
      runtime_support_service_->ParseRuntimeStatus(*observation);
  if (runtime_status.has_value() && runtime_status->runtime_phase == "failed") {
    return std::string("runtime-failed");
  }
  const auto gpu_telemetry =
      runtime_support_service_->ParseGpuTelemetry(*observation);
  if (gpu_telemetry.has_value() && gpu_telemetry->degraded) {
    return std::string("telemetry-degraded");
  }
  return std::nullopt;
}

void PlaneRealizationService::MaterializeComposeArtifacts(
    const comet::DesiredState& desired_state,
    const std::vector<comet::NodeExecutionPlan>& host_plans) const {
  const auto desired_compose_plans = BuildComposePlanMap(desired_state);

  for (const auto& host_plan : host_plans) {
    for (const auto& operation : host_plan.operations) {
      if (operation.kind == comet::HostOperationKind::WriteComposeFile) {
        const auto compose_it = desired_compose_plans.find(host_plan.node_name);
        if (compose_it == desired_compose_plans.end()) {
          throw std::runtime_error(
              "missing compose plan for node '" + host_plan.node_name + "'");
        }
        WriteTextFile(operation.target, comet::RenderComposeYaml(compose_it->second));
      }

      if (operation.kind == comet::HostOperationKind::RemoveComposeFile) {
        RemoveFileIfExists(operation.target);
      }
    }
  }
}

void PlaneRealizationService::MaterializeInferRuntimeArtifact(
    const comet::DesiredState& desired_state,
    const std::string& artifacts_root) const {
  bool wrote_primary = false;
  for (const auto& instance : desired_state.instances) {
    if (instance.role != comet::InstanceRole::Infer) {
      continue;
    }
    WriteTextFile(
        InferRuntimeArtifactPathForInstance(artifacts_root, desired_state.plane_name, instance.name),
        comet::RenderInferRuntimeConfigJsonForInstance(desired_state, instance.name));
    if (!wrote_primary) {
      WriteTextFile(
          InferRuntimeArtifactPath(artifacts_root, desired_state.plane_name),
          comet::RenderInferRuntimeConfigJsonForInstance(desired_state, instance.name));
      wrote_primary = true;
    }
  }
}

std::vector<comet::HostAssignment> PlaneRealizationService::BuildHostAssignments(
    const comet::DesiredState& desired_state,
    const std::string& artifacts_root,
    int desired_generation,
    const std::vector<comet::NodeAvailabilityOverride>& availability_overrides,
    const std::vector<comet::HostObservation>& observations,
    const std::optional<comet::SchedulingPolicyReport>& scheduling_report) const {
  if (runtime_support_service_ == nullptr) {
    throw std::runtime_error("plane realization runtime support is not configured");
  }

  std::vector<comet::HostAssignment> assignments;
  assignments.reserve(desired_state.nodes.size());
  const auto availability_override_map =
      runtime_support_service_->BuildAvailabilityOverrideMap(availability_overrides);
  const auto rollout_actions_by_target_node =
      scheduling_report.has_value()
          ? BuildRolloutActionsByTargetNode(*scheduling_report)
          : std::map<std::string, std::vector<comet::SchedulerRolloutAction>>{};

  for (const auto& node : desired_state.nodes) {
    if (!IsNodeSchedulable(
            runtime_support_service_->ResolveNodeAvailability(
                availability_override_map, node.name))) {
      continue;
    }
    if (ObservedSchedulingGateReason(
            observations, node.name, default_stale_after_seconds_)
            .has_value()) {
      continue;
    }
    comet::HostAssignment assignment;
    assignment.node_name = node.name;
    assignment.plane_name = desired_state.plane_name;
    assignment.desired_generation = desired_generation;
    assignment.assignment_type = "apply-node-state";
    assignment.desired_state_json = comet::SerializeDesiredStateJson(
        comet::SliceDesiredStateForNode(desired_state, node.name));
    assignment.artifacts_root = artifacts_root;
    assignment.status = comet::HostAssignmentStatus::Pending;
    const auto rollout_it = rollout_actions_by_target_node.find(node.name);
    if (rollout_it != rollout_actions_by_target_node.end() &&
        !rollout_it->second.empty()) {
      std::set<std::string> gated_workers;
      for (const auto& action : rollout_it->second) {
        if (!action.worker_name.empty()) {
          gated_workers.insert(action.worker_name);
        }
      }
      std::ostringstream message;
      message << "scheduler rollout actions pending on target node " << node.name
              << " for workers ";
      bool first = true;
      for (const auto& worker_name : gated_workers) {
        if (!first) {
          message << ",";
        }
        first = false;
        message << worker_name;
      }
      assignment.status_message = message.str();
    }
    assignments.push_back(std::move(assignment));
  }

  return assignments;
}

std::vector<comet::HostAssignment> PlaneRealizationService::BuildStopPlaneAssignments(
    const comet::DesiredState& desired_state,
    int desired_generation,
    const std::string& artifacts_root,
    const std::vector<comet::NodeAvailabilityOverride>& availability_overrides) const {
  if (runtime_support_service_ == nullptr) {
    throw std::runtime_error("plane realization runtime support is not configured");
  }

  std::vector<comet::HostAssignment> assignments;
  assignments.reserve(desired_state.nodes.size());
  const auto availability_override_map =
      runtime_support_service_->BuildAvailabilityOverrideMap(availability_overrides);
  for (const auto& node : desired_state.nodes) {
    if (!IsNodeSchedulable(
            runtime_support_service_->ResolveNodeAvailability(
                availability_override_map, node.name))) {
      continue;
    }
    comet::HostAssignment assignment;
    assignment.node_name = node.name;
    assignment.plane_name = desired_state.plane_name;
    assignment.desired_generation = desired_generation;
    assignment.assignment_type = "stop-plane-state";
    assignment.desired_state_json = comet::SerializeDesiredStateJson(
        BuildStoppedPlaneNodeState(desired_state, node.name));
    assignment.artifacts_root = artifacts_root;
    assignment.status = comet::HostAssignmentStatus::Pending;
    assignment.status_message = "plane stop lifecycle transition";
    assignments.push_back(std::move(assignment));
  }
  return assignments;
}

std::vector<comet::HostAssignment> PlaneRealizationService::BuildDeletePlaneAssignments(
    const comet::DesiredState& desired_state,
    int desired_generation,
    const std::string& artifacts_root) const {
  std::vector<comet::HostAssignment> assignments;
  assignments.reserve(desired_state.nodes.size());
  for (const auto& node : desired_state.nodes) {
    comet::HostAssignment assignment;
    assignment.node_name = node.name;
    assignment.plane_name = desired_state.plane_name;
    assignment.desired_generation = desired_generation;
    assignment.assignment_type = "delete-plane-state";
    assignment.desired_state_json = comet::SerializeDesiredStateJson(
        BuildDeletedPlaneNodeState(desired_state, node.name));
    assignment.artifacts_root = artifacts_root;
    assignment.status = comet::HostAssignmentStatus::Pending;
    assignment.status_message = "plane delete lifecycle transition";
    assignments.push_back(std::move(assignment));
  }
  return assignments;
}

std::optional<comet::HostAssignment>
PlaneRealizationService::FindLatestHostAssignmentForNode(
    const std::vector<comet::HostAssignment>& assignments,
    const std::string& node_name) const {
  std::optional<comet::HostAssignment> result;
  for (const auto& assignment : assignments) {
    if (assignment.node_name != node_name) {
      continue;
    }
    result = assignment;
  }
  return result;
}

std::optional<comet::HostAssignment>
PlaneRealizationService::FindLatestHostAssignmentForPlane(
    const std::vector<comet::HostAssignment>& assignments,
    const std::string& plane_name) const {
  std::optional<comet::HostAssignment> result;
  for (const auto& assignment : assignments) {
    if (assignment.plane_name != plane_name) {
      continue;
    }
    result = assignment;
  }
  return result;
}

}  // namespace comet::controller
