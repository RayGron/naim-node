#pragma once

#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "scheduler/scheduler_execution_dependencies.h"
#include "scheduler/scheduler_service.h"

#include "naim/runtime/runtime_status.h"
#include "naim/state/sqlite_store.h"

namespace naim::controller {

struct SchedulerExecutionVerificationConfig {
  int verification_timeout_seconds = 0;
  int verification_stable_samples_required = 0;
};

class SchedulerExecutionSupport {
 public:
  SchedulerExecutionSupport(
      std::shared_ptr<const SchedulerAssignmentQuerySupport> assignment_query_support,
      std::shared_ptr<const SchedulerVerificationSupport> verification_support,
      SchedulerExecutionVerificationConfig verification_config);

  std::optional<naim::RolloutActionRecord> FindPriorRolloutActionForWorker(
      const std::vector<naim::RolloutActionRecord>& actions,
      const naim::RolloutActionRecord& action,
      const std::string& requested_action_name) const;

  void MaterializeRetryPlacementAction(
      naim::DesiredState* state,
      const naim::RolloutActionRecord& action,
      const std::vector<std::string>& victim_worker_names) const;

  std::vector<naim::HostAssignment> BuildEvictionAssignmentsForAction(
      const naim::DesiredState& desired_state,
      int desired_generation,
      const naim::RolloutActionRecord& action,
      const std::vector<naim::HostAssignment>& existing_assignments) const;

  bool AreRolloutEvictionAssignmentsApplied(
      const std::vector<naim::HostAssignment>& assignments,
      int action_id) const;

  void MaterializeRebalancePlanEntry(
      naim::DesiredState* state,
      const RebalancePlanEntry& entry) const;

  SchedulerVerificationResult EvaluateSchedulerActionVerification(
      const naim::SchedulerPlaneRuntime& plane_runtime,
      const std::vector<naim::HostObservation>& observations) const;

  void MarkWorkerMoveVerified(
      naim::ControllerStore* store,
      const naim::SchedulerPlaneRuntime& plane_runtime) const;

  void MarkWorkersEvicted(
      naim::ControllerStore* store,
      const std::string& plane_name,
      const std::vector<std::string>& worker_names) const;

 private:
  void RemoveWorkerFromDesiredState(
      naim::DesiredState* state,
      const std::string& worker_name) const;

  const naim::RuntimeProcessStatus* FindInstanceRuntimeStatus(
      const std::vector<naim::RuntimeProcessStatus>& statuses,
      const std::string& instance_name,
      const std::string& gpu_device) const;

  bool TelemetryShowsOwnedProcess(
      const std::optional<naim::GpuTelemetrySnapshot>& telemetry,
      const std::string& gpu_device,
      const std::string& instance_name) const;

  std::shared_ptr<const SchedulerAssignmentQuerySupport> assignment_query_support_;
  std::shared_ptr<const SchedulerVerificationSupport> verification_support_;
  SchedulerExecutionVerificationConfig verification_config_;
};

}  // namespace naim::controller
