#pragma once

#include <nlohmann/json.hpp>

#include "naim/state/models.h"

namespace naim {

class DesiredStateV2Renderer final {
 public:
  static DesiredState Render(const nlohmann::json& value);

 private:
  explicit DesiredStateV2Renderer(const nlohmann::json& value);

  DesiredState RenderState();

  void RenderIdentity();
  void RenderPlacement();
  void RenderFeatures();
  void RenderHooks();
  void RenderModel();
  void RenderInteraction();
  void RenderRuntime();
  void RenderNodeTopology();
  void RenderWorkerGroup();
  void RenderSharedDisk();
  void RenderInferInstance();
  void RenderWorkerInstances();
  void RenderAppInstance();
  void RenderSkillsInstance();
  void RenderWebGatewayInstance();

  bool InferEnabled() const;
  int InferReplicaCount() const;
  int WorkerCount() const;
  int ExpectedWorkers() const;
  bool HasExternalAppHost() const;
  std::string ExternalAppHostAuthMode() const;
  bool LegacyTopologyPlacementEnabled() const;
  std::string ResolveInferNodeName() const;
  std::string ResolveInferNodeName(int infer_index) const;
  std::string ResolveAppNodeName() const;
  std::string ResolveWorkerNodeName(int worker_index) const;
  std::optional<std::string> ResolveWorkerGpuDevice(int worker_index) const;
  std::optional<std::string> ResolveLegacyServiceNodeName(
      const nlohmann::json& service_json,
      const char* service_name) const;
  std::optional<std::string> ResolveLegacyWorkerAssignmentNodeName(int worker_index) const;
  std::string DefaultNodeName() const;
  const NodeInventory& RequireNode(const std::string& node_name, const char* context) const;
  std::string SharedDiskNodeName() const;

  std::string StripBundleRefPrefix(const std::string& value) const;
  std::string BuildWorkerName(int index, int total_workers) const;
  std::string BuildPlaneSharedDiskName() const;
  std::string BuildInferInstanceName(int infer_index = 0) const;
  std::string BuildAppInstanceName() const;
  std::string BuildSkillsInstanceName() const;
  std::string BuildWebGatewayInstanceName() const;
  int BuildInferApiPort(int infer_index) const;
  int BuildInferGatewayPort(int infer_index) const;
  int BuildInferLlamaPort(int infer_index) const;
  int BuildSkillsHostPort() const;
  int BuildWebGatewayHostPort() const;
  std::string BuildReplicaUpstreams(const std::vector<InstanceSpec>& infer_instances) const;
  std::string InferInstanceNameForWorker(int worker_index) const;
  std::string BuildPlaneSharedHostPath() const;
  std::string BuildInstancePrivateHostPath(const std::string& instance_name) const;
  std::string BuildAppCommandFromScriptRef(const std::string& script_ref) const;
  std::string BuildCommandFromStartSpec(
      const nlohmann::json& start,
      const std::string& default_command) const;
  void ApplyExternalAppHostMetadata(
      InstanceSpec* instance,
      const std::string& binding) const;
  std::string DefaultInferRuntimeBackend() const;
  std::string DefaultWorkerBootMode() const;
  void NormalizeInferenceSettings();

  int ExtractPrivateDiskSizeGb(
      const nlohmann::json& service_json,
      int default_size_gb,
      const std::string& legacy_volume_key = "volumes") const;
  std::string ExtractPrivateMountPath(
      const nlohmann::json& service_json,
      const std::string& default_mount_path,
      const std::string& legacy_volume_key = "volumes") const;

  InteractionSettings::CompletionPolicy DefaultCompletionPolicy() const;
  InteractionSettings::CompletionPolicy DefaultLongCompletionPolicy() const;

  const nlohmann::json& value_;
  DesiredState state_;
  nlohmann::json infer_json_;
  nlohmann::json worker_json_;
  nlohmann::json resources_json_;
  nlohmann::json worker_resources_json_;
  nlohmann::json app_json_;
  nlohmann::json skills_json_;
  nlohmann::json browsing_json_;
  nlohmann::json knowledge_json_;
  nlohmann::json features_json_;
  std::vector<std::string> infer_names_;
};

}  // namespace naim
