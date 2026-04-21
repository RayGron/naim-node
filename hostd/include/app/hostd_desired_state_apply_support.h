#pragma once

#include <chrono>
#include <functional>
#include <optional>
#include <string>

#include "app/hostd_bootstrap_model_support.h"
#include "app/hostd_desired_state_apply_plan_support.h"
#include "app/hostd_desired_state_display_support.h"
#include "app/hostd_desired_state_path_support.h"
#include "app/hostd_disk_runtime_support.h"
#include "app/hostd_local_runtime_state_support.h"
#include "app/hostd_local_state_path_support.h"
#include "app/hostd_local_state_repository.h"
#include "app/hostd_post_deploy_support.h"
#include "backend/hostd_backend.h"
#include "cli/hostd_command_line.h"
#include "naim/state/models.h"

namespace naim::hostd {

class HostdDesiredStateApplySupport final {
 public:
  using ProgressPublisher = HostdDesiredStateApplyPlanSupport::ProgressPublisher;

  HostdDesiredStateApplySupport(
      const HostdDesiredStatePathSupport& path_support,
      const HostdDesiredStateDisplaySupport& display_support,
      const HostdDesiredStateApplyPlanSupport& apply_plan_support,
      const HostdDiskRuntimeSupport& disk_runtime_support,
      const HostdPostDeploySupport& post_deploy_support,
      const HostdLocalStateRepository& local_state_repository,
      const HostdLocalRuntimeStateSupport& local_runtime_state_support,
      const HostdBootstrapModelSupport& bootstrap_model_support);

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
      HostdBackend* backend,
      const ProgressPublisher& publish_progress) const;

 private:
  static std::string CurrentHostPlatform();
  static const naim::NodeInventory* FindNodeInventory(
      const naim::DesiredState& desired_node_state,
      const std::string& node_name);
  static bool NodeHasInferInstance(const naim::DesiredState& state);
  static naim::NodeComposePlan RequireNodeComposePlan(
      const naim::DesiredState& state,
      const std::string& node_name);
  static bool NodeUsesManagedRuntimeServices(
      const naim::DesiredState& desired_node_state,
      const std::string& node_name);
  static bool NodeUsesGpuRuntime(
      const naim::DesiredState& desired_node_state,
      const std::string& node_name);

  void ValidateDesiredNodeStateForCurrentHost(
      const naim::DesiredState& desired_node_state,
      ComposeMode compose_mode) const;

  const HostdDesiredStatePathSupport& path_support_;
  const HostdDesiredStateDisplaySupport& display_support_;
  const HostdDesiredStateApplyPlanSupport& apply_plan_support_;
  const HostdDiskRuntimeSupport& disk_runtime_support_;
  const HostdPostDeploySupport& post_deploy_support_;
  const HostdLocalStateRepository& local_state_repository_;
  const HostdLocalRuntimeStateSupport& local_runtime_state_support_;
  const HostdBootstrapModelSupport& bootstrap_model_support_;
};

}  // namespace naim::hostd
