#pragma once

#include "state_apply/hostd_assignment_service.h"

namespace comet::hostd {

class DefaultHostdAssignmentSupport final : public IHostdAssignmentSupport {
 public:
  comet::DesiredState RebaseStateForRuntimeRoot(
      comet::DesiredState state,
      const std::string& storage_root,
      const std::optional<std::string>& runtime_root) const override;
  nlohmann::json BuildAssignmentProgressPayload(
      const std::string& phase,
      const std::string& phase_label,
      const std::string& message,
      int progress_percent,
      const std::string& plane_name,
      const std::string& node_name) const override;
  void PublishAssignmentProgress(
      HostdBackend* backend,
      const std::optional<int>& assignment_id,
      const nlohmann::json& progress) const override;
  std::vector<std::string> ParseTaggedCsv(
      const std::string& tagged_message,
      const std::string& tag) const override;
  comet::HostObservation BuildObservedStateSnapshot(
      const std::string& node_name,
      const std::string& state_root,
      comet::HostObservationStatus status,
      const std::string& status_message,
      const std::optional<int>& assignment_id = std::nullopt) const override;
  std::map<std::string, int> CaptureServiceHostPids(
      const std::vector<std::string>& service_names) const override;
  bool VerifyEvictionAssignment(
      const comet::DesiredState& desired_state,
      const std::string& node_name,
      const std::string& state_root,
      const std::string& tagged_message,
      const std::map<std::string, int>& expected_victim_host_pids) const override;
  void ApplyDesiredNodeState(
      const comet::DesiredState& desired_node_state,
      const std::string& artifacts_root,
      const std::string& storage_root,
      const std::optional<std::string>& runtime_root,
      const std::string& state_root,
      ComposeMode compose_mode,
      const std::string& source_label,
      const std::optional<int>& desired_generation,
      const std::optional<int>& assignment_id,
      HostdBackend* backend) const override;
  void ShowDemoOps(
      const std::string& node_name,
      const std::string& storage_root,
      const std::optional<std::string>& runtime_root) const override;
  void ShowStateOps(
      const std::string& db_path,
      const std::string& node_name,
      const std::string& artifacts_root,
      const std::string& storage_root,
      const std::optional<std::string>& runtime_root,
      const std::string& state_root) const override;
  void AppendHostdEvent(
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
      const std::string& severity) const override;
};

}  // namespace comet::hostd
