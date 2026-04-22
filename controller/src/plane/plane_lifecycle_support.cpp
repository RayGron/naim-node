#include "plane/plane_lifecycle_support.h"

#include "app/controller_composition_support.h"
#include "skills/knowledge_vault_common_skills.h"

namespace naim::controller {

ControllerPlaneLifecycleSupport::ControllerPlaneLifecycleSupport(
    const DesiredStatePolicyService& desired_state_policy_service,
    const PlaneRealizationService& plane_realization_service,
    std::string default_artifacts_root)
    : desired_state_policy_service_(desired_state_policy_service),
      plane_realization_service_(plane_realization_service),
      default_artifacts_root_(std::move(default_artifacts_root)) {}

void ControllerPlaneLifecycleSupport::PrepareDesiredState(
    naim::ControllerStore& store,
    naim::DesiredState* desired_state) const {
  desired_state_policy_service_.ApplyRegisteredHostExecutionModes(store, desired_state);
  desired_state_policy_service_.ResolveDesiredStateDynamicPlacements(store, desired_state);
  EnsureKnowledgeVaultCommonSkills(store, desired_state);
  desired_state_policy_service_.ValidateDesiredStateForControllerAdmission(
      store,
      *desired_state);
  desired_state_policy_service_.ValidateDesiredStateExecutionModes(*desired_state);
}

void ControllerPlaneLifecycleSupport::AppendPlaneEvent(
    naim::ControllerStore& store,
    const std::string& event_type,
    const std::string& message,
    const nlohmann::json& payload,
    const std::string& plane_name) const {
  composition_support::AppendControllerEvent(
      store,
      "plane",
      event_type,
      message,
      payload,
      plane_name);
}

bool ControllerPlaneLifecycleSupport::CanFinalizeDeletedPlane(
    naim::ControllerStore& store,
    const std::string& plane_name) const {
  return composition_support::CanFinalizeDeletedPlane(store, plane_name);
}

std::optional<naim::HostAssignment>
ControllerPlaneLifecycleSupport::FindLatestHostAssignmentForPlane(
    const std::vector<naim::HostAssignment>& assignments,
    const std::string& plane_name) const {
  return plane_realization_service_.FindLatestHostAssignmentForPlane(assignments, plane_name);
}

std::vector<naim::HostAssignment> ControllerPlaneLifecycleSupport::BuildStartAssignments(
    const naim::DesiredState& desired_state,
    const std::string& artifacts_root,
    int desired_generation,
    const std::vector<naim::NodeAvailabilityOverride>& availability_overrides,
    const std::vector<naim::HostObservation>& observations,
    const naim::SchedulingPolicyReport& scheduling_report) const {
  return plane_realization_service_.BuildHostAssignments(
      desired_state,
      artifacts_root,
      desired_generation,
      availability_overrides,
      observations,
      scheduling_report);
}

std::vector<naim::HostAssignment> ControllerPlaneLifecycleSupport::BuildStopAssignments(
    const naim::DesiredState& desired_state,
    int desired_generation,
    const std::string& artifacts_root,
    const std::vector<naim::NodeAvailabilityOverride>& availability_overrides) const {
  return plane_realization_service_.BuildStopPlaneAssignments(
      desired_state,
      desired_generation,
      artifacts_root,
      availability_overrides);
}

std::vector<naim::HostAssignment> ControllerPlaneLifecycleSupport::BuildDeleteAssignments(
    const naim::DesiredState& desired_state,
    int desired_generation,
    const std::string& artifacts_root) const {
  return plane_realization_service_.BuildDeletePlaneAssignments(
      desired_state,
      desired_generation,
      artifacts_root);
}

std::string ControllerPlaneLifecycleSupport::DefaultArtifactsRoot() const {
  return default_artifacts_root_;
}

}  // namespace naim::controller
