#pragma once

#include <optional>
#include <string>
#include <vector>

#include "infra/controller_runtime_support_service.h"

#include "naim/planning/execution_plan.h"
#include "naim/state/models.h"
#include "naim/planning/scheduling_policy.h"
#include "naim/state/sqlite_store.h"

namespace naim::controller {

class PlaneRealizationService {
 public:
  PlaneRealizationService(
      const ControllerRuntimeSupportService* runtime_support_service,
      int default_stale_after_seconds);

  void MaterializeComposeArtifacts(
      const naim::DesiredState& desired_state,
      const std::vector<naim::NodeExecutionPlan>& host_plans) const;

  void MaterializeInferRuntimeArtifact(
      const naim::DesiredState& desired_state,
      const std::string& artifacts_root) const;

  std::vector<naim::HostAssignment> BuildHostAssignments(
      const naim::DesiredState& desired_state,
      const std::string& artifacts_root,
      int desired_generation,
      const std::vector<naim::NodeAvailabilityOverride>& availability_overrides,
      const std::vector<naim::HostObservation>& observations,
      const std::optional<naim::SchedulingPolicyReport>& scheduling_report) const;

  std::vector<naim::HostAssignment> BuildStopPlaneAssignments(
      const naim::DesiredState& desired_state,
      int desired_generation,
      const std::string& artifacts_root,
      const std::vector<naim::NodeAvailabilityOverride>& availability_overrides) const;

  std::vector<naim::HostAssignment> BuildDeletePlaneAssignments(
      const naim::DesiredState& desired_state,
      int desired_generation,
      const std::string& artifacts_root) const;

  std::optional<naim::HostAssignment> FindLatestHostAssignmentForNode(
      const std::vector<naim::HostAssignment>& assignments,
      const std::string& node_name) const;

  std::optional<naim::HostAssignment> FindLatestHostAssignmentForPlane(
      const std::vector<naim::HostAssignment>& assignments,
      const std::string& plane_name) const;

  std::optional<std::string> ObservedSchedulingGateReason(
      const std::vector<naim::HostObservation>& observations,
      const std::string& node_name,
      int stale_after_seconds) const;

 private:
  bool IsNodeSchedulable(naim::NodeAvailability availability) const;

  const ControllerRuntimeSupportService* runtime_support_service_;
  int default_stale_after_seconds_ = 0;
};

}  // namespace naim::controller
