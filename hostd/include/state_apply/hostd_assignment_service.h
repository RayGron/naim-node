#pragma once

#include <map>
#include <optional>
#include <string>
#include <vector>

#include "backend/hostd_backend_factory.h"
#include "cli/hostd_command_line.h"
#include "observation/hostd_observation_service.h"
#include "naim/state/models.h"

namespace naim::hostd {

class IHostdAssignmentSupport {
 public:
  virtual ~IHostdAssignmentSupport() = default;

  virtual naim::DesiredState RebaseStateForRuntimeRoot(
      naim::DesiredState state,
      const std::string& storage_root,
      const std::optional<std::string>& runtime_root) const = 0;
  virtual nlohmann::json BuildAssignmentProgressPayload(
      const std::string& phase,
      const std::string& phase_label,
      const std::string& message,
      int progress_percent,
      const std::string& plane_name,
      const std::string& node_name) const = 0;
  virtual void PublishAssignmentProgress(
      HostdBackend* backend,
      const std::optional<int>& assignment_id,
      const nlohmann::json& progress) const = 0;
  virtual std::vector<std::string> ParseTaggedCsv(
      const std::string& tagged_message,
      const std::string& tag) const = 0;
  virtual naim::HostObservation BuildObservedStateSnapshot(
      const std::string& node_name,
      const std::string& storage_root,
      const std::string& state_root,
      naim::HostObservationStatus status,
      const std::string& status_message,
      const std::optional<int>& assignment_id = std::nullopt) const = 0;
  virtual std::map<std::string, int> CaptureServiceHostPids(
      const std::vector<std::string>& service_names) const = 0;
  virtual bool VerifyEvictionAssignment(
      const naim::DesiredState& desired_state,
      const std::string& node_name,
      const std::string& state_root,
      const std::string& tagged_message,
      const std::map<std::string, int>& expected_victim_host_pids) const = 0;
  virtual void ApplyDesiredNodeState(
      const naim::DesiredState& desired_node_state,
      const std::string& artifacts_root,
      const std::string& storage_root,
      const std::optional<std::string>& runtime_root,
      const std::string& state_root,
      ComposeMode compose_mode,
      const std::string& source_label,
      const std::optional<int>& desired_generation,
      const std::optional<int>& assignment_id,
      HostdBackend* backend) const = 0;
  virtual void DownloadModelLibraryArtifacts(
      const nlohmann::json& payload,
      const std::string& node_name,
      HostdBackend* backend,
      const std::optional<int>& assignment_id) const = 0;
  virtual void ReadModelArtifactChunk(
      const nlohmann::json& payload,
      const std::string& node_name,
      HostdBackend* backend,
      const std::optional<int>& assignment_id) const = 0;
  virtual void BuildModelArtifactManifest(
      const nlohmann::json& payload,
      const std::string& node_name,
      HostdBackend* backend,
      const std::optional<int>& assignment_id) const = 0;
  virtual void ExecuteRuntimeHttpProxy(
      const nlohmann::json& payload,
      const std::string& node_name,
      HostdBackend* backend,
      const std::optional<int>& assignment_id) const = 0;
  virtual void ApplyKnowledgeVaultService(
      const nlohmann::json& payload,
      const std::string& node_name,
      const std::string& storage_root,
      HostdBackend* backend,
      const std::optional<int>& assignment_id) const = 0;
  virtual void StopKnowledgeVaultService(
      const nlohmann::json& payload,
      const std::string& node_name,
      HostdBackend* backend,
      const std::optional<int>& assignment_id) const = 0;
  virtual void ExecuteKnowledgeVaultHttpProxy(
      const nlohmann::json& payload,
      const std::string& node_name,
      HostdBackend* backend,
      const std::optional<int>& assignment_id) const = 0;
  virtual void ExecuteHostSelfUpdate(
      const nlohmann::json& payload,
      const std::string& node_name,
      const std::optional<std::string>& host_private_key_path,
      HostdBackend* backend,
      const std::optional<int>& assignment_id) const = 0;
  virtual void ShowDemoOps(
      const std::string& node_name,
      const std::string& storage_root,
      const std::optional<std::string>& runtime_root) const = 0;
  virtual void ShowStateOps(
      const std::string& db_path,
      const std::string& node_name,
      const std::string& artifacts_root,
      const std::string& storage_root,
      const std::optional<std::string>& runtime_root,
      const std::string& state_root) const = 0;
  virtual void AppendHostdEvent(
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
      const std::string& severity) const = 0;
};

class HostdAssignmentService {
 public:
  HostdAssignmentService(
      const IHostdBackendFactory& backend_factory,
      const IHostdAssignmentSupport& support,
      const HostdObservationService& observation_service);

  void ShowDemoOps(
      const std::string& node_name,
      const std::string& storage_root,
      const std::optional<std::string>& runtime_root) const;
  void ShowStateOps(
      const std::string& db_path,
      const std::string& node_name,
      const std::string& artifacts_root,
      const std::string& storage_root,
      const std::optional<std::string>& runtime_root,
      const std::string& state_root) const;
  void ApplyStateOps(
      const std::string& db_path,
      const std::string& node_name,
      const std::string& artifacts_root,
      const std::string& storage_root,
      const std::optional<std::string>& runtime_root,
      const std::string& state_root,
      ComposeMode compose_mode) const;
  void ApplyNextAssignment(
      const std::optional<std::string>& db_path,
      const std::optional<std::string>& controller_url,
      const std::optional<std::string>& host_private_key_path,
      const std::optional<std::string>& controller_fingerprint,
      const std::optional<std::string>& onboarding_key,
      const std::string& node_name,
      const std::string& storage_root,
      const std::optional<std::string>& runtime_root,
      const std::string& state_root,
      ComposeMode compose_mode) const;

 private:
  const IHostdBackendFactory& backend_factory_;
  const IHostdAssignmentSupport& support_;
  const HostdObservationService& observation_service_;
};

}  // namespace naim::hostd
