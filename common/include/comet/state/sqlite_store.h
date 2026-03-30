#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <vector>

#include "comet/state/models.h"
#include "comet/planning/scheduling_policy.h"

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
  std::string progress_json = "{}";
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
  std::string disk_telemetry_json;
  std::string network_telemetry_json;
  std::string cpu_telemetry_json;
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
  std::string plane_name;
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

struct DiskRuntimeState {
  std::string disk_name;
  std::string plane_name;
  std::string node_name;
  std::string image_path;
  std::string filesystem_type;
  std::string loop_device;
  std::string mount_point;
  std::string runtime_state;
  std::string attached_at;
  std::string mounted_at;
  std::string last_verified_at;
  std::string status_message;
  std::string updated_at;
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

struct EventRecord {
  int id = 0;
  std::string plane_name;
  std::string node_name;
  std::string worker_name;
  std::optional<int> assignment_id;
  std::optional<int> rollout_action_id;
  std::string category;
  std::string event_type;
  std::string severity = "info";
  std::string message;
  std::string payload_json = "{}";
  std::string created_at;
};

struct PlaneRecord {
  std::string name;
  std::string shared_disk_name;
  std::string control_root;
  std::string artifacts_root;
  std::string plane_mode = "compute";
  int generation = 0;
  int applied_generation = 0;
  int rebalance_iteration = 0;
  std::string state;
  std::string created_at;
};

struct RegisteredHostRecord {
  std::string node_name;
  std::string advertised_address;
  std::string public_key_base64;
  std::string controller_public_key_fingerprint;
  std::string transport_mode;
  std::string execution_mode = "mixed";
  std::string registration_state;
  std::string session_state;
  std::string session_token;
  std::string session_expires_at;
  std::int64_t session_host_sequence = 0;
  std::int64_t session_controller_sequence = 0;
  std::string capabilities_json = "{}";
  std::string status_message;
  std::string last_session_at;
  std::string last_heartbeat_at;
  std::string created_at;
  std::string updated_at;
};

struct UserRecord {
  int id = 0;
  std::string username;
  std::string role;
  std::string password_hash;
  std::string created_at;
  std::string updated_at;
  std::string last_login_at;
};

struct WebAuthnCredentialRecord {
  int id = 0;
  int user_id = 0;
  std::string credential_id;
  std::string public_key;
  std::uint32_t counter = 0;
  std::string transports_json = "[]";
  std::string created_at;
  std::string updated_at;
  std::string last_used_at;
};

struct RegistrationInviteRecord {
  int id = 0;
  std::string token;
  int created_by_user_id = 0;
  std::string expires_at;
  std::string created_at;
  std::optional<int> used_by_user_id;
  std::string used_at;
  std::string revoked_at;
};

struct UserSshKeyRecord {
  int id = 0;
  int user_id = 0;
  std::string label;
  std::string public_key;
  std::string fingerprint;
  std::string created_at;
  std::string revoked_at;
  std::string last_used_at;
};

struct AuthSessionRecord {
  std::string token;
  int user_id = 0;
  std::string session_kind;
  std::string plane_name;
  std::string expires_at;
  std::string created_at;
  std::string revoked_at;
  std::string last_used_at;
};

struct ModelLibraryDownloadJobRecord {
  std::string id;
  std::string status = "queued";
  std::string model_id;
  std::string target_root;
  std::string target_subdir;
  std::vector<std::string> source_urls;
  std::vector<std::string> target_paths;
  std::string current_item;
  std::optional<std::uintmax_t> bytes_total;
  std::uintmax_t bytes_done = 0;
  int part_count = 0;
  std::string error_message;
  bool hidden = false;
  std::string created_at;
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
  std::optional<DesiredState> LoadDesiredState(const std::string& plane_name) const;
  std::vector<DesiredState> LoadDesiredStates() const;
  std::optional<int> LoadDesiredGeneration() const;
  std::optional<int> LoadDesiredGeneration(const std::string& plane_name) const;
  std::optional<int> LoadRebalanceIteration() const;
  std::optional<int> LoadRebalanceIteration(const std::string& plane_name) const;
  std::vector<PlaneRecord> LoadPlanes() const;
  std::optional<PlaneRecord> LoadPlane(const std::string& plane_name) const;
  void DeletePlane(const std::string& plane_name);
  void UpsertRegisteredHost(const RegisteredHostRecord& host);
  std::optional<RegisteredHostRecord> LoadRegisteredHost(const std::string& node_name) const;
  std::vector<RegisteredHostRecord> LoadRegisteredHosts(
      const std::optional<std::string>& node_name = std::nullopt) const;
  int LoadUserCount() const;
  std::optional<UserRecord> LoadUserById(int user_id) const;
  std::optional<UserRecord> LoadUserByUsername(const std::string& username) const;
  std::vector<UserRecord> LoadUsers() const;
  UserRecord CreateBootstrapAdmin(
      const std::string& username,
      const std::string& password_hash);
  UserRecord CreateInvitedUser(
      const std::string& invite_token,
      const std::string& username,
      const std::string& password_hash);
  void UpdateUserLastLoginAt(int user_id, const std::string& last_login_at);
  void InsertWebAuthnCredential(const WebAuthnCredentialRecord& credential);
  void UpdateWebAuthnCredentialCounter(
      const std::string& credential_id,
      std::uint32_t counter,
      const std::string& last_used_at);
  std::vector<WebAuthnCredentialRecord> LoadWebAuthnCredentialsForUser(int user_id) const;
  std::optional<WebAuthnCredentialRecord> LoadWebAuthnCredentialById(
      const std::string& credential_id) const;
  RegistrationInviteRecord CreateRegistrationInvite(
      int created_by_user_id,
      const std::string& token,
      const std::string& expires_at);
  std::optional<RegistrationInviteRecord> LoadRegistrationInviteByToken(
      const std::string& token) const;
  std::vector<RegistrationInviteRecord> LoadActiveRegistrationInvites() const;
  bool MarkRegistrationInviteUsed(
      const std::string& token,
      int used_by_user_id,
      const std::string& used_at);
  bool RevokeRegistrationInvite(
      int invite_id,
      const std::string& revoked_at);
  void InsertUserSshKey(const UserSshKeyRecord& ssh_key);
  std::vector<UserSshKeyRecord> LoadActiveUserSshKeys(int user_id) const;
  std::optional<UserSshKeyRecord> LoadActiveUserSshKeyByFingerprint(
      int user_id,
      const std::string& fingerprint) const;
  std::optional<UserSshKeyRecord> LoadActiveUserSshKeyById(int ssh_key_id) const;
  bool RevokeUserSshKey(
      int ssh_key_id,
      const std::string& revoked_at);
  void TouchUserSshKey(
      int ssh_key_id,
      const std::string& last_used_at);
  void InsertAuthSession(const AuthSessionRecord& session);
  std::optional<AuthSessionRecord> LoadActiveAuthSession(
      const std::string& token,
      const std::optional<std::string>& session_kind = std::nullopt,
      const std::optional<std::string>& plane_name = std::nullopt) const;
  bool RevokeAuthSession(
      const std::string& token,
      const std::string& revoked_at);
  bool TouchAuthSession(
      const std::string& token,
      const std::string& last_used_at);
  void UpsertModelLibraryDownloadJob(const ModelLibraryDownloadJobRecord& job);
  std::optional<ModelLibraryDownloadJobRecord> LoadModelLibraryDownloadJob(
      const std::string& job_id) const;
  std::vector<ModelLibraryDownloadJobRecord> LoadModelLibraryDownloadJobs(
      const std::optional<std::string>& status = std::nullopt) const;
  bool DeleteModelLibraryDownloadJob(const std::string& job_id);
  bool UpdatePlaneState(
      const std::string& plane_name,
      const std::string& state);
  bool UpdatePlaneAppliedGeneration(
      const std::string& plane_name,
      int applied_generation);
  bool UpdatePlaneArtifactsRoot(
      const std::string& plane_name,
      const std::string& artifacts_root);
  int SupersedeHostAssignmentsForPlane(
      const std::string& plane_name,
      const std::string& status_message);
  void UpsertNodeAvailabilityOverride(const NodeAvailabilityOverride& availability_override);
  std::optional<NodeAvailabilityOverride> LoadNodeAvailabilityOverride(
      const std::string& node_name) const;
  std::vector<NodeAvailabilityOverride> LoadNodeAvailabilityOverrides(
      const std::optional<std::string>& node_name = std::nullopt) const;
  void UpsertDiskRuntimeState(const DiskRuntimeState& runtime_state);
  std::optional<DiskRuntimeState> LoadDiskRuntimeState(
      const std::string& disk_name,
      const std::string& node_name) const;
  std::vector<DiskRuntimeState> LoadDiskRuntimeStates(
      const std::optional<std::string>& plane_name = std::nullopt,
      const std::optional<std::string>& node_name = std::nullopt) const;
  void ReplaceRolloutActions(
      const std::string& plane_name,
      int desired_generation,
      const std::vector<SchedulerRolloutAction>& actions);
  std::vector<RolloutActionRecord> LoadRolloutActions(
      const std::optional<std::string>& plane_name = std::nullopt,
      const std::optional<std::string>& target_node_name = std::nullopt,
      const std::optional<RolloutActionStatus>& status = std::nullopt) const;
  bool UpdateRolloutActionStatus(
      int action_id,
      RolloutActionStatus status,
      const std::string& status_message = "");
  void UpsertHostObservation(const HostObservation& observation);
  std::optional<HostObservation> LoadHostObservation(const std::string& node_name) const;
  std::vector<HostObservation> LoadHostObservations(
      const std::optional<std::string>& node_name = std::nullopt,
      const std::optional<std::string>& plane_name = std::nullopt) const;
  void AppendEvent(const EventRecord& event);
  std::vector<EventRecord> LoadEvents(
      const std::optional<std::string>& plane_name = std::nullopt,
      const std::optional<std::string>& node_name = std::nullopt,
      const std::optional<std::string>& worker_name = std::nullopt,
      const std::optional<std::string>& category = std::nullopt,
      int limit = 100,
      const std::optional<int>& since_id = std::nullopt,
      bool ascending = false) const;
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
      const std::optional<HostAssignmentStatus>& status = std::nullopt,
      const std::optional<std::string>& plane_name = std::nullopt) const;
  std::optional<HostAssignment> ClaimNextHostAssignment(const std::string& node_name);
  bool UpdateHostAssignmentProgress(
      int assignment_id,
      const std::string& progress_json);
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
