#include "app/hostd_desired_state_apply_support.h"

#include <algorithm>
#include <iostream>
#include <stdexcept>

#include "app/hostd_runtime_telemetry_support.h"
#include "naim/planning/planner.h"

namespace naim::hostd {

HostdDesiredStateApplySupport::HostdDesiredStateApplySupport(
    const HostdDesiredStatePathSupport& path_support,
    const HostdDesiredStateDisplaySupport& display_support,
    const HostdDesiredStateApplyPlanSupport& apply_plan_support,
    const HostdDiskRuntimeSupport& disk_runtime_support,
    const HostdPostDeploySupport& post_deploy_support,
    const HostdLocalStateRepository& local_state_repository,
    const HostdLocalRuntimeStateSupport& local_runtime_state_support,
    const HostdBootstrapModelSupport& bootstrap_model_support)
    : path_support_(path_support),
      display_support_(display_support),
      apply_plan_support_(apply_plan_support),
      disk_runtime_support_(disk_runtime_support),
      post_deploy_support_(post_deploy_support),
      local_state_repository_(local_state_repository),
      local_runtime_state_support_(local_runtime_state_support),
      bootstrap_model_support_(bootstrap_model_support) {}

void HostdDesiredStateApplySupport::ApplyDesiredNodeState(
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
    const ProgressPublisher& publish_progress) const {
  const std::string node_name =
      local_state_repository_.RequireSingleNodeName(desired_node_state);
  const std::string& plane_name = desired_node_state.plane_name;
  const auto current_local_state =
      local_state_repository_.LoadLocalAppliedState(
          state_root,
          node_name,
          plane_name);
  const auto applied_generation =
      local_state_repository_.LoadLocalAppliedGeneration(
          state_root,
          node_name,
          plane_name);
  const auto execution_plan = HostdDesiredStateDisplaySupport::ResolveNodeExecutionPlan(
      naim::BuildNodeExecutionPlans(current_local_state, desired_node_state, artifacts_root),
      current_local_state,
      desired_node_state,
      node_name,
      artifacts_root);
  const auto compose_plan = RequireNodeComposePlan(desired_node_state, node_name);

  std::cout << source_label << "\n";
  std::cout << "artifacts_root=" << artifacts_root << "\n";
  std::cout << "state_path="
            << HostdLocalStatePathSupport().LocalPlaneStatePath(
                   state_root,
                   node_name,
                   plane_name)
            << "\n";
  if (desired_generation.has_value()) {
    std::cout << "desired_generation=" << *desired_generation << "\n";
  }
  if (applied_generation.has_value()) {
    std::cout << "applied_generation=" << *applied_generation << "\n";
  }
  if (runtime_root.has_value()) {
    std::cout << "runtime_root=" << *runtime_root << "\n";
  }
  if (const auto runtime_config_path =
          path_support_.InferRuntimeConfigPathForNode(desired_node_state, node_name)) {
    std::cout << "infer_runtime_config=" << *runtime_config_path << "\n";
    std::cout << "infer_runtime_summary="
              << HostdDesiredStateDisplaySupport::RuntimeConfigSummary(desired_node_state)
              << "\n";
  }
  std::cout << "compose_mode="
            << (compose_mode == ComposeMode::Exec ? "exec" : "skip") << "\n";

  ValidateDesiredNodeStateForCurrentHost(desired_node_state, compose_mode);

  auto maybe_publish_progress =
      [&](const std::string& phase,
          const std::string& title,
          const std::string& detail,
          int percent,
          const std::string& current_plane_name,
          const std::string& current_node_name) {
        if (publish_progress) {
          publish_progress(
              phase,
              title,
              detail,
              percent,
              current_plane_name,
              current_node_name);
        }
      };

  if (execution_plan.operations.empty()) {
    std::cout << "no local changes for node=" << node_name << "\n";
    disk_runtime_support_.PersistDiskRuntimeStateForDesiredDisks(
        backend,
        desired_node_state,
        storage_root,
        runtime_root,
        "disk runtime verified by hostd");
    if (HostdDesiredStateApplyPlanSupport::IsDesiredNodeStateEmpty(desired_node_state)) {
      local_state_repository_.RemoveLocalAppliedPlaneState(
          state_root,
          node_name,
          plane_name);
    } else {
      local_state_repository_.SaveLocalAppliedState(
          state_root,
          node_name,
          desired_node_state,
          plane_name);
      if (desired_generation.has_value()) {
        local_state_repository_.SaveLocalAppliedGeneration(
            state_root,
            node_name,
            *desired_generation,
            plane_name);
      }
    }
    local_state_repository_.RewriteAggregateLocalState(state_root, node_name);
    local_state_repository_.RewriteAggregateLocalGeneration(state_root, node_name);
    maybe_publish_progress(
        "completed",
        "Assignment complete",
        "No local changes were required for the node.",
        100,
        plane_name,
        node_name);
    return;
  }

  disk_runtime_support_.EnsureDesiredDisksReady(
      backend,
      desired_node_state,
      storage_root,
      runtime_root);
  bootstrap_model_support_.BootstrapPlaneModelIfNeeded(
      desired_node_state,
      node_name,
      backend,
      assignment_id);

  apply_plan_support_.ApplyNodePlan(
      execution_plan,
      desired_node_state,
      compose_plan,
      storage_root,
      runtime_root,
      compose_mode,
      backend,
      maybe_publish_progress);
  disk_runtime_support_.PersistDiskRuntimeStateForRemovedDisks(
      backend,
      current_local_state,
      execution_plan);
  disk_runtime_support_.PersistDiskRuntimeStateForDesiredDisks(
      backend,
      desired_node_state,
      storage_root,
      runtime_root,
      "disk runtime applied by hostd");
  if (HostdDesiredStateApplyPlanSupport::IsDesiredNodeStateEmpty(desired_node_state)) {
    local_state_repository_.RemoveLocalAppliedPlaneState(
        state_root,
        node_name,
        plane_name);
    local_state_repository_.RewriteAggregateLocalState(state_root, node_name);
    local_state_repository_.RewriteAggregateLocalGeneration(state_root, node_name);
    return;
  }
  local_state_repository_.SaveLocalAppliedState(
      state_root,
      node_name,
      desired_node_state,
      plane_name);
  if (desired_generation.has_value()) {
    local_state_repository_.SaveLocalAppliedGeneration(
        state_root,
        node_name,
        *desired_generation,
        plane_name);
  }
  local_state_repository_.RewriteAggregateLocalState(state_root, node_name);
  local_state_repository_.RewriteAggregateLocalGeneration(state_root, node_name);
  if (compose_mode == ComposeMode::Exec) {
    maybe_publish_progress(
        "waiting-runtime-ready",
        "Waiting for runtime readiness",
        "Runtime was started; waiting for infer and worker observation to converge.",
        97,
        plane_name,
        node_name);
    if (NodeHasInferInstance(desired_node_state)) {
      local_runtime_state_support_.WaitForLocalRuntimeStatus(
          state_root,
          node_name,
          plane_name,
          std::chrono::seconds(300));
    }
    local_runtime_state_support_.WaitForLocalInstanceRuntimeStatuses(
        state_root,
        node_name,
        plane_name,
        apply_plan_support_.ExpectedRuntimeStatusCountForComposePlan(compose_plan),
        std::chrono::seconds(300));
    post_deploy_support_.RunIfNeeded(
        desired_node_state,
        node_name,
        artifacts_root,
        storage_root,
        runtime_root,
        state_root,
        desired_generation,
        assignment_id,
        backend);
  }
  maybe_publish_progress(
      "completed",
      "Assignment complete",
      "Desired runtime state was applied on the node.",
      100,
      plane_name,
      node_name);
}

std::string HostdDesiredStateApplySupport::CurrentHostPlatform() {
#if defined(_WIN32)
  return "windows";
#elif defined(__APPLE__)
  return "macos";
#elif defined(__linux__)
  return "linux";
#else
  return "unknown";
#endif
}

const naim::NodeInventory* HostdDesiredStateApplySupport::FindNodeInventory(
    const naim::DesiredState& desired_node_state,
    const std::string& node_name) {
  for (const auto& node : desired_node_state.nodes) {
    if (node.name == node_name) {
      return &node;
    }
  }
  return nullptr;
}

bool HostdDesiredStateApplySupport::NodeHasInferInstance(const naim::DesiredState& state) {
  for (const auto& instance : state.instances) {
    if (instance.role == naim::InstanceRole::Infer) {
      return true;
    }
  }
  return false;
}

naim::NodeComposePlan HostdDesiredStateApplySupport::RequireNodeComposePlan(
    const naim::DesiredState& state,
    const std::string& node_name) {
  const auto plan = naim::FindNodeComposePlan(state, node_name);
  if (!plan.has_value()) {
    throw std::runtime_error("node '" + node_name + "' not found in compose plan");
  }
  return *plan;
}

bool HostdDesiredStateApplySupport::NodeUsesManagedRuntimeServices(
    const naim::DesiredState& desired_node_state,
    const std::string& node_name) {
  for (const auto& instance : desired_node_state.instances) {
    if (instance.node_name == node_name) {
      return true;
    }
  }
  return false;
}

bool HostdDesiredStateApplySupport::NodeUsesGpuRuntime(
    const naim::DesiredState& desired_node_state,
    const std::string& node_name) {
  for (const auto& runtime_gpu_node : desired_node_state.runtime_gpu_nodes) {
    if (runtime_gpu_node.enabled && runtime_gpu_node.node_name == node_name) {
      return true;
    }
  }
  for (const auto& instance : desired_node_state.instances) {
    if (instance.node_name == node_name &&
        (instance.role == naim::InstanceRole::Worker ||
         (instance.gpu_device.has_value() && !instance.gpu_device->empty()))) {
      return true;
    }
  }
  if (const auto* node = FindNodeInventory(desired_node_state, node_name);
      node != nullptr && NodeHasConfiguredGpuDevices(*node)) {
    return true;
  }
  return false;
}

void HostdDesiredStateApplySupport::ValidateDesiredNodeStateForCurrentHost(
    const naim::DesiredState& desired_node_state,
    ComposeMode compose_mode) const {
  if (compose_mode != ComposeMode::Exec) {
    return;
  }

  if (desired_node_state.nodes.empty()) {
    throw std::runtime_error("desired node state is empty");
  }
  const std::string node_name = desired_node_state.nodes.front().name;
  const std::string host_platform = CurrentHostPlatform();
  if (const auto* node = FindNodeInventory(desired_node_state, node_name);
      node != nullptr && !node->platform.empty() && node->platform != host_platform) {
    throw std::runtime_error(
        "node '" + node_name + "' targets platform '" + node->platform +
        "', but hostd is running on '" + host_platform + "'");
  }

  if (host_platform == "macos" &&
      NodeUsesManagedRuntimeServices(desired_node_state, node_name) &&
      NodeUsesGpuRuntime(desired_node_state, node_name)) {
    throw std::runtime_error(
        "node '" + node_name +
        "' requests Linux/NVIDIA GPU runtime, but hostd compose exec is unsupported on macOS");
  }
}

}  // namespace naim::hostd
