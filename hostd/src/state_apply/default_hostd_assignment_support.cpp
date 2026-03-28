#include "state_apply/default_hostd_assignment_support.h"

#include "app/hostd_app_support.h"

namespace comet::hostd {

comet::DesiredState DefaultHostdAssignmentSupport::RebaseStateForRuntimeRoot(
    comet::DesiredState state,
    const std::string& storage_root,
    const std::optional<std::string>& runtime_root) const {
  return appsupport::RebaseStateForRuntimeRoot(std::move(state), storage_root, runtime_root);
}

nlohmann::json DefaultHostdAssignmentSupport::BuildAssignmentProgressPayload(
    const std::string& phase,
    const std::string& phase_label,
    const std::string& message,
    int progress_percent,
    const std::string& plane_name,
    const std::string& node_name) const {
  return appsupport::BuildAssignmentProgressPayload(
      phase,
      phase_label,
      message,
      progress_percent,
      plane_name,
      node_name);
}

void DefaultHostdAssignmentSupport::PublishAssignmentProgress(
    HostdBackend* backend,
    const std::optional<int>& assignment_id,
    const nlohmann::json& progress) const {
  appsupport::PublishAssignmentProgress(backend, assignment_id, progress);
}

std::vector<std::string> DefaultHostdAssignmentSupport::ParseTaggedCsv(
    const std::string& tagged_message,
    const std::string& tag) const {
  return appsupport::ParseTaggedCsv(tagged_message, tag);
}

comet::HostObservation DefaultHostdAssignmentSupport::BuildObservedStateSnapshot(
    const std::string& node_name,
    const std::string& state_root,
    comet::HostObservationStatus status,
    const std::string& status_message,
    const std::optional<int>& assignment_id) const {
  return appsupport::BuildObservedStateSnapshot(
      node_name,
      state_root,
      status,
      status_message,
      assignment_id);
}

std::map<std::string, int> DefaultHostdAssignmentSupport::CaptureServiceHostPids(
    const std::vector<std::string>& service_names) const {
  return appsupport::CaptureServiceHostPids(service_names);
}

bool DefaultHostdAssignmentSupport::VerifyEvictionAssignment(
    const comet::DesiredState& desired_state,
    const std::string& node_name,
    const std::string& state_root,
    const std::string& tagged_message,
    const std::map<std::string, int>& expected_victim_host_pids) const {
  return appsupport::VerifyEvictionAssignment(
      desired_state,
      node_name,
      state_root,
      tagged_message,
      expected_victim_host_pids);
}

void DefaultHostdAssignmentSupport::ApplyDesiredNodeState(
    const comet::DesiredState& desired_node_state,
    const std::string& artifacts_root,
    const std::string& storage_root,
    const std::optional<std::string>& runtime_root,
    const std::string& state_root,
    ComposeMode compose_mode,
    const std::string& source_label,
    const std::optional<int>& desired_generation,
    const std::optional<int>& assignment_id,
    HostdBackend* backend) const {
  appsupport::ApplyDesiredNodeState(
      desired_node_state,
      artifacts_root,
      storage_root,
      runtime_root,
      state_root,
      compose_mode,
      source_label,
      desired_generation,
      assignment_id,
      backend);
}

void DefaultHostdAssignmentSupport::ShowDemoOps(
    const std::string& node_name,
    const std::string& storage_root,
    const std::optional<std::string>& runtime_root) const {
  appsupport::ShowDemoOps(node_name, storage_root, runtime_root);
}

void DefaultHostdAssignmentSupport::ShowStateOps(
    const std::string& db_path,
    const std::string& node_name,
    const std::string& artifacts_root,
    const std::string& storage_root,
    const std::optional<std::string>& runtime_root,
    const std::string& state_root) const {
  appsupport::ShowStateOps(
      db_path,
      node_name,
      artifacts_root,
      storage_root,
      runtime_root,
      state_root);
}

void DefaultHostdAssignmentSupport::AppendHostdEvent(
    HostdBackend& backend,
    const std::string& category,
    const std::string& event_type,
    const std::string& message,
    const nlohmann::json& payload,
    const std::string& plane_name,
    const std::string& node_name,
    const std::string& worker_name,
    const std::optional<int>& assignment_id,
    const std::optional<int>& rollout_action_id,
    const std::string& severity) const {
  appsupport::AppendHostdEvent(
      backend,
      category,
      event_type,
      message,
      payload,
      plane_name,
      node_name,
      worker_name,
      assignment_id,
      rollout_action_id,
      severity);
}

}  // namespace comet::hostd
