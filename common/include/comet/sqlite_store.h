#pragma once

#include <optional>
#include <string>
#include <vector>

#include "comet/models.h"
#include "comet/scheduling_policy.h"

namespace comet {

enum class HostAssignmentStatus {
  Pending,
  Claimed,
  Applied,
  Failed,
  Superseded,
};

enum class HostObservationStatus {
  Idle,
  Applying,
  Applied,
  Failed,
};

enum class NodeAvailability {
  Active,
  Draining,
  Unavailable,
};

enum class RolloutActionStatus {
  Pending,
  Acknowledged,
  ReadyToRetry,
};

struct HostAssignment {
  int id = 0;
  std::string node_name;
  std::string plane_name;
  int desired_generation = 0;
  int attempt_count = 0;
  int max_attempts = 3;
  std::string assignment_type;
  std::string desired_state_json;
  std::string artifacts_root;
  HostAssignmentStatus status = HostAssignmentStatus::Pending;
  std::string status_message;
};

struct HostObservation {
  std::string node_name;
  std::string plane_name;
  std::optional<int> applied_generation;
  std::optional<int> last_assignment_id;
  HostObservationStatus status = HostObservationStatus::Idle;
  std::string status_message;
  std::string observed_state_json;
  std::string runtime_status_json;
  std::string instance_runtime_json;
  std::string gpu_telemetry_json;
  std::string heartbeat_at;
};

struct NodeAvailabilityOverride {
  std::string node_name;
  NodeAvailability availability = NodeAvailability::Active;
  std::string status_message;
  std::string updated_at;
};

struct RolloutActionRecord {
  int id = 0;
  int desired_generation = 0;
  int step = 0;
  std::string worker_name;
  std::string action;
  std::string target_node_name;
  std::string target_gpu_device;
  std::vector<std::string> victim_worker_names;
  std::string reason;
  RolloutActionStatus status = RolloutActionStatus::Pending;
  std::string status_message;
};

struct SchedulerPlaneRuntime {
  std::string plane_name;
  std::string active_action;
  std::string active_worker_name;
  std::string phase;
  int action_generation = 0;
  int stable_samples = 0;
  int rollback_attempt_count = 0;
  std::string source_node_name;
  std::string source_gpu_device;
  std::string target_node_name;
  std::string target_gpu_device;
  std::string previous_state_json;
  std::string status_message;
  std::string started_at;
  std::string updated_at;
};

struct SchedulerWorkerRuntime {
  std::string plane_name;
  std::string worker_name;
  std::string last_move_at;
  std::string last_eviction_at;
  std::optional<int> last_verified_generation;
  std::string last_scheduler_phase;
  std::string last_status_message;
  bool manual_intervention_required = false;
  std::string updated_at;
};

struct SchedulerNodeRuntime {
  std::string plane_name;
  std::string node_name;
  std::string last_move_at;
  std::optional<int> last_verified_generation;
  std::string updated_at;
};

class ControllerStore {
 public:
  explicit ControllerStore(std::string db_path);
  ~ControllerStore();

  ControllerStore(const ControllerStore&) = delete;
  ControllerStore& operator=(const ControllerStore&) = delete;

  void Initialize();
  void ReplaceDesiredState(const DesiredState& state, int generation);
  void ReplaceDesiredState(
      const DesiredState& state,
      int generation,
      int rebalance_iteration);
  void ReplaceDesiredState(const DesiredState& state);
  std::optional<DesiredState> LoadDesiredState() const;
  std::optional<int> LoadDesiredGeneration() const;
  std::optional<int> LoadRebalanceIteration() const;
  void UpsertNodeAvailabilityOverride(const NodeAvailabilityOverride& availability_override);
  std::optional<NodeAvailabilityOverride> LoadNodeAvailabilityOverride(
      const std::string& node_name) const;
  std::vector<NodeAvailabilityOverride> LoadNodeAvailabilityOverrides(
      const std::optional<std::string>& node_name = std::nullopt) const;
  void ReplaceRolloutActions(
      int desired_generation,
      const std::vector<SchedulerRolloutAction>& actions);
  std::vector<RolloutActionRecord> LoadRolloutActions(
      const std::optional<std::string>& target_node_name = std::nullopt,
      const std::optional<RolloutActionStatus>& status = std::nullopt) const;
  bool UpdateRolloutActionStatus(
      int action_id,
      RolloutActionStatus status,
      const std::string& status_message = "");
  void UpsertHostObservation(const HostObservation& observation);
  std::optional<HostObservation> LoadHostObservation(const std::string& node_name) const;
  std::vector<HostObservation> LoadHostObservations(
      const std::optional<std::string>& node_name = std::nullopt) const;
  void UpsertSchedulerPlaneRuntime(const SchedulerPlaneRuntime& runtime);
  std::optional<SchedulerPlaneRuntime> LoadSchedulerPlaneRuntime(
      const std::string& plane_name) const;
  void ClearSchedulerPlaneRuntime(const std::string& plane_name);
  void UpsertSchedulerWorkerRuntime(const SchedulerWorkerRuntime& runtime);
  std::optional<SchedulerWorkerRuntime> LoadSchedulerWorkerRuntime(
      const std::string& worker_name) const;
  std::vector<SchedulerWorkerRuntime> LoadSchedulerWorkerRuntimes(
      const std::optional<std::string>& plane_name = std::nullopt) const;
  void UpsertSchedulerNodeRuntime(const SchedulerNodeRuntime& runtime);
  std::optional<SchedulerNodeRuntime> LoadSchedulerNodeRuntime(
      const std::string& node_name) const;
  std::vector<SchedulerNodeRuntime> LoadSchedulerNodeRuntimes(
      const std::optional<std::string>& plane_name = std::nullopt) const;
  void ReplaceHostAssignments(const std::vector<HostAssignment>& assignments);
  void EnqueueHostAssignments(
      const std::vector<HostAssignment>& assignments,
      const std::string& supersede_reason = "");
  std::optional<HostAssignment> LoadHostAssignment(int assignment_id) const;
  std::vector<HostAssignment> LoadHostAssignments(
      const std::optional<std::string>& node_name = std::nullopt,
      const std::optional<HostAssignmentStatus>& status = std::nullopt) const;
  std::optional<HostAssignment> ClaimNextHostAssignment(const std::string& node_name);
  bool TransitionClaimedHostAssignment(
      int assignment_id,
      HostAssignmentStatus status,
      const std::string& status_message = "");
  bool RetryFailedHostAssignment(
      int assignment_id,
      const std::string& status_message = "");
  void UpdateHostAssignmentStatus(
      int assignment_id,
      HostAssignmentStatus status,
      const std::string& status_message = "");

  const std::string& db_path() const;

 private:
  std::string db_path_;
  void* db_ = nullptr;
};

std::string ToString(HostAssignmentStatus status);
HostAssignmentStatus ParseHostAssignmentStatus(const std::string& value);
std::string ToString(HostObservationStatus status);
HostObservationStatus ParseHostObservationStatus(const std::string& value);
std::string ToString(NodeAvailability availability);
NodeAvailability ParseNodeAvailability(const std::string& value);
std::string ToString(RolloutActionStatus status);
RolloutActionStatus ParseRolloutActionStatus(const std::string& value);

}  // namespace comet
