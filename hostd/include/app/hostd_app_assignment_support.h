#pragma once

#include <map>
#include <optional>
#include <string>
#include <vector>

#include <nlohmann/json.hpp>

#include "app/hostd_bootstrap_model_support_factory.h"
#include "app/hostd_app_observation_support.h"
#include "app/hostd_bootstrap_transfer_support.h"
#include "app/hostd_command_support.h"
#include "app/hostd_compose_runtime_support.h"
#include "app/hostd_container_name_support.h"
#include "app/hostd_desired_state_apply_plan_support.h"
#include "app/hostd_desired_state_apply_support.h"
#include "app/hostd_desired_state_display_support.h"
#include "app/hostd_desired_state_path_support.h"
#include "app/hostd_disk_runtime_support.h"
#include "app/hostd_file_support.h"
#include "app/hostd_local_runtime_state_support.h"
#include "app/hostd_local_state_path_support.h"
#include "app/hostd_local_state_repository.h"
#include "app/hostd_model_artifact_request_support.h"
#include "app/hostd_post_deploy_support.h"
#include "app/hostd_reporting_support.h"
#include "app/hostd_runtime_http_proxy.h"
#include "backend/hostd_backend.h"
#include "cli/hostd_command_line.h"
#include "naim/state/models.h"
#include "state_apply/hostd_assignment_service.h"

namespace naim::hostd {

class HostdAppAssignmentSupport final : public IHostdAssignmentSupport {
 public:
  HostdAppAssignmentSupport();

  naim::DesiredState RebaseStateForRuntimeRoot(
      naim::DesiredState state,
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
  naim::HostObservation BuildObservedStateSnapshot(
      const std::string& node_name,
      const std::string& storage_root,
      const std::string& state_root,
      naim::HostObservationStatus status,
      const std::string& status_message,
      const std::optional<int>& assignment_id = std::nullopt) const override;
  std::map<std::string, int> CaptureServiceHostPids(
      const std::vector<std::string>& service_names) const override;
  bool VerifyEvictionAssignment(
      const naim::DesiredState& desired_state,
      const std::string& node_name,
      const std::string& state_root,
      const std::string& tagged_message,
      const std::map<std::string, int>& expected_victim_host_pids) const override;
  void ApplyDesiredNodeState(
      const naim::DesiredState& desired_node_state,
      const std::string& artifacts_root,
      const std::string& storage_root,
      const std::optional<std::string>& runtime_root,
      const std::string& state_root,
      ComposeMode compose_mode,
      const std::string& source_label,
      const std::optional<int>& desired_generation,
      const std::optional<int>& assignment_id,
      HostdBackend* backend) const override;
  void DownloadModelLibraryArtifacts(
      const nlohmann::json& payload,
      const std::string& node_name,
      HostdBackend* backend,
      const std::optional<int>& assignment_id) const override;
  void ReadModelArtifactChunk(
      const nlohmann::json& payload,
      const std::string& node_name,
      HostdBackend* backend,
      const std::optional<int>& assignment_id) const override;
  void BuildModelArtifactManifest(
      const nlohmann::json& payload,
      const std::string& node_name,
      HostdBackend* backend,
      const std::optional<int>& assignment_id) const override;
  void ExecuteRuntimeHttpProxy(
      const nlohmann::json& payload,
      const std::string& node_name,
      HostdBackend* backend,
      const std::optional<int>& assignment_id) const override;
  void ApplyKnowledgeVaultService(
      const nlohmann::json& payload,
      const std::string& node_name,
      const std::string& storage_root,
      HostdBackend* backend,
      const std::optional<int>& assignment_id) const override;
  void StopKnowledgeVaultService(
      const nlohmann::json& payload,
      const std::string& node_name,
      HostdBackend* backend,
      const std::optional<int>& assignment_id) const override;
  void ExecuteKnowledgeVaultHttpProxy(
      const nlohmann::json& payload,
      const std::string& node_name,
      HostdBackend* backend,
      const std::optional<int>& assignment_id) const override;
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

 private:
  HostdDesiredStatePathSupport path_support_;
  HostdRuntimeTelemetrySupport runtime_telemetry_support_;
  HostdLocalStatePathSupport local_state_path_support_;
  HostdLocalStateRepository local_state_repository_;
  HostdLocalRuntimeStateSupport local_runtime_state_support_;
  HostdCommandSupport command_support_;
  HostdFileSupport file_support_;
  HostdComposeRuntimeSupport compose_runtime_support_;
  HostdDiskRuntimeSupport disk_runtime_support_;
  HostdDesiredStateApplyPlanSupport apply_plan_support_;
  HostdPostDeploySupport post_deploy_support_;
  HostdReportingSupport reporting_support_;
  HostdRuntimeHttpProxy runtime_http_proxy_;
  HostdModelArtifactRequestSupport model_artifact_request_support_;
  HostdContainerNameSupport container_name_support_;
  HostdBootstrapTransferSupport model_library_transfer_support_;
  HostdBootstrapModelSupportFactory bootstrap_model_support_factory_;
  HostdBootstrapModelSupport bootstrap_model_support_;
  HostdDesiredStateDisplaySupport display_support_;
  HostdDesiredStateApplySupport apply_support_;
  HostdAppObservationSupport observation_support_;
};

}  // namespace naim::hostd
