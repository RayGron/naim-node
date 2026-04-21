#pragma once

#include <map>
#include <optional>
#include <string>

#include "infra/controller_runtime_support_service.h"

#include "naim/state/models.h"
#include "naim/state/sqlite_store.h"

namespace naim::controller {

class DesiredStatePolicyService {
 public:
  std::optional<std::string> DescribeUnsupportedControllerLocalRuntime(
      const naim::DesiredState& desired_state,
      const std::string& node_name) const;

  void ApplyRegisteredHostExecutionModes(
      naim::ControllerStore& store,
      naim::DesiredState* desired_state) const;

  void ResolveDesiredStateDynamicPlacements(
      naim::ControllerStore& store,
      naim::DesiredState* desired_state) const;

  void ValidateDesiredStateForControllerAdmission(
      naim::ControllerStore& store,
      const naim::DesiredState& desired_state) const;

  void ValidateDesiredStateExecutionModes(
      const naim::DesiredState& desired_state) const;

 private:
  struct PlacementUsage {
    double allocated_fraction = 0.0;
    int allocated_memory_mb = 0;
  };

  struct AutoPlacementDecision {
    std::string node_name;
    std::string gpu_device;
    int score = 0;
    bool idle_target = false;
    bool upgrade_to_exclusive = false;
    double allocated_fraction = 0.0;
    int allocated_memory_mb = 0;
    int observed_free_vram_mb = -1;
    int observed_utilization_pct = -1;
    int node_order = 0;
    int gpu_order = 0;
  };

  std::string CurrentControllerPlatform() const;
  const naim::NodeInventory* FindPlaneNodeInventory(
      const naim::DesiredState& desired_state,
      const std::string& node_name) const;
  bool PlaneNodeUsesGpuRuntime(
      const naim::DesiredState& desired_state,
      const std::string& node_name) const;
  bool NodeAllowsInstanceRole(
      naim::HostExecutionMode execution_mode,
      naim::InstanceRole role) const;
  std::string EffectiveWorkerSelectionPolicy(
      const naim::DesiredState& state) const;
  int AutoPlacementPolicyRank(
      const std::string& policy,
      const AutoPlacementDecision& candidate) const;
  int ScoreAutoPlacementCandidate(
      const naim::NodeInventory& node,
      const std::string& gpu_device,
      const PlacementUsage& usage,
      const naim::InferenceRuntimeSettings& inference,
      int observed_free_vram_mb,
      int observed_utilization_pct,
      const std::optional<std::string>& preferred_node_name,
      const std::optional<std::string>& preferred_gpu_device) const;
  bool HybridGpuAlreadyAssigned(
      const naim::DesiredState& desired_state,
      const naim::InstanceSpec& current_worker,
      const std::string& node_name,
      const std::string& gpu_device) const;
  bool UsesLlamaRpcRuntime(const naim::DesiredState& desired_state) const;
  void ReservePlacement(
      std::map<std::pair<std::string, std::string>, PlacementUsage>* placement_usage,
      const naim::InstanceSpec& worker) const;
  const naim::InstanceSpec* FindInferInstance(
      const naim::DesiredState& desired_state) const;
  std::string InferInstanceNameForWorker(const naim::InstanceSpec& instance) const;
  void RefreshDerivedWorkerMetadata(
      naim::DesiredState* desired_state) const;
  void ApplyObservedHostGpuInventory(
      naim::ControllerStore& store,
      naim::DesiredState* desired_state) const;
  std::optional<AutoPlacementDecision> SelectAutoPlacement(
      const naim::DesiredState& desired_state,
      const std::map<std::pair<std::string, std::string>, PlacementUsage>& placement_usage,
      const std::map<std::pair<std::string, std::string>, std::pair<int, int>>&
          observed_gpu_headroom,
      const naim::InstanceSpec& worker,
      const std::optional<std::string>& requested_node_name,
      const std::optional<std::string>& requested_gpu_device) const;

  ControllerRuntimeSupportService runtime_support_;
};

}  // namespace naim::controller
