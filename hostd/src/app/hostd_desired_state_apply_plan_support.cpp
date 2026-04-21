#include "app/hostd_desired_state_apply_plan_support.h"

#include <algorithm>
#include <filesystem>
#include <iostream>
#include <stdexcept>

#include "naim/core/platform_compat.h"
#include "naim/planning/compose_renderer.h"
#include "naim/runtime/infer_runtime_config.h"

namespace naim::hostd {

HostdDesiredStateApplyPlanSupport::HostdDesiredStateApplyPlanSupport(
    const HostdCommandSupport& command_support,
    const HostdComposeRuntimeSupport& compose_runtime_support,
    const HostdDiskRuntimeSupport& disk_runtime_support,
    const HostdFileSupport& file_support)
    : command_support_(command_support),
      compose_runtime_support_(compose_runtime_support),
      disk_runtime_support_(disk_runtime_support),
      file_support_(file_support) {}

void HostdDesiredStateApplyPlanSupport::ApplyNodePlan(
    const naim::NodeExecutionPlan& plan,
    const naim::DesiredState& desired_node_state,
    const naim::NodeComposePlan& compose_plan,
    const std::string& storage_root,
    const std::optional<std::string>& runtime_root,
    ComposeMode compose_mode,
    HostdBackend* backend,
    const ProgressPublisher& publish_progress) const {
  std::cout << "applying node=" << plan.node_name << "\n";
  std::cout << "compose=" << plan.compose_file_path << "\n";

  auto maybe_publish_progress =
      [&](const std::string& phase,
          const std::string& title,
          const std::string& detail,
          int percent) {
        if (publish_progress) {
          publish_progress(
              phase,
              title,
              detail,
              percent,
              desired_node_state.plane_name,
              plan.node_name);
        }
      };

  auto apply_operation = [&](const naim::HostOperation& operation) {
    switch (operation.kind) {
      case naim::HostOperationKind::EnsureDisk: {
        const auto disk =
            disk_runtime_support_.FindDiskInStateByKey(
                std::optional<naim::DesiredState>(desired_node_state),
                operation.target);
        if (!disk.has_value()) {
          throw std::runtime_error(
              "missing desired disk for ensure operation '" + operation.target + "'");
        }
        const auto realized_state =
            disk_runtime_support_.EnsureDesiredDiskRuntimeState(
                *disk,
                operation.target,
                storage_root,
                runtime_root);
        if (backend != nullptr) {
          backend->UpsertDiskRuntimeState(realized_state);
        }
        PrintOperationApplied(operation, "applied");
        break;
      }
      case naim::HostOperationKind::RemoveDisk: {
        const auto disk_key = disk_runtime_support_.SplitDiskKey(operation.target);
        const std::string& disk_name = disk_key.first;
        const std::string& disk_node_name = disk_key.second;
        const auto runtime_state =
            backend == nullptr ? std::nullopt
                               : backend->LoadDiskRuntimeState(disk_name, disk_node_name);
        const bool is_plane_shared_disk = std::any_of(
            desired_node_state.disks.begin(),
            desired_node_state.disks.end(),
            [&](const naim::DiskSpec& disk) {
              return disk.name == disk_name &&
                     disk.node_name == disk_node_name &&
                     disk.kind == naim::DiskKind::PlaneShared;
            });
        const bool delegated_shared_remove =
            is_plane_shared_disk &&
            disk_node_name != desired_node_state.inference.primary_infer_node;
        bool removed = false;
        const bool mounted_now =
            !runtime_state.has_value()
                ? false
                : !runtime_state->mount_point.empty() &&
                      runtime_state->runtime_state == "mounted";
        if (naim::platform::HasElevatedPrivileges() &&
            (mounted_now ||
             (runtime_state.has_value() &&
              (runtime_state->runtime_state == "mounted" ||
               !runtime_state->loop_device.empty() ||
               !runtime_state->image_path.empty())))) {
          naim::DiskRuntimeState effective_state;
          if (runtime_state.has_value()) {
            effective_state = *runtime_state;
          } else {
            effective_state.disk_name = disk_name;
            effective_state.plane_name = plan.plane_name;
            effective_state.node_name = disk_node_name;
            effective_state.mount_point = operation.details;
            effective_state.runtime_state = "mounted";
            effective_state.status_message = "runtime state recovered from live mount";
          }
          if (effective_state.plane_name.empty()) {
            effective_state.plane_name = plan.plane_name;
          }
          if (effective_state.node_name.empty()) {
            effective_state.node_name = disk_node_name;
          }
          removed = true;
          if (!delegated_shared_remove) {
            disk_runtime_support_.RemoveRealDiskMount(effective_state, runtime_root);
          }
          auto removed_state = effective_state;
          removed_state.runtime_state = "removed";
          removed_state.status_message =
              delegated_shared_remove
                  ? "plane-shared disk removal delegated to primary infer node"
                  : "managed disk detached and removed by hostd";
          removed_state.loop_device.clear();
          removed_state.mount_point = operation.details;
          if (runtime_root.has_value()) {
            removed_state.image_path.clear();
          }
          if (backend != nullptr) {
            backend->UpsertDiskRuntimeState(removed_state);
          }
        } else {
          RemoveDiskDirectory(operation.details, runtime_root);
          removed = runtime_root.has_value();
        }
        PrintOperationApplied(operation, removed ? "applied" : "skipped");
        break;
      }
      case naim::HostOperationKind::EnsureService:
      case naim::HostOperationKind::RemoveService:
        PrintOperationApplied(operation, "planned");
        break;
      case naim::HostOperationKind::WriteInferRuntimeConfig:
        maybe_publish_progress(
            "rendering-runtime",
            "Rendering runtime",
            "Writing infer runtime configuration.",
            84);
        file_support_.EnsureParentDirectory(operation.target);
        file_support_.WriteTextFile(
            operation.target,
            operation.details.empty()
                ? naim::RenderInferRuntimeConfigJson(desired_node_state)
                : naim::RenderInferRuntimeConfigJsonForInstance(
                      desired_node_state, operation.details));
        PrintOperationApplied(operation, "applied");
        break;
      case naim::HostOperationKind::RemoveInferRuntimeConfig:
        file_support_.RemoveFileIfExists(operation.target);
        PrintOperationApplied(operation, "applied");
        break;
      case naim::HostOperationKind::WriteComposeFile:
        maybe_publish_progress(
            "rendering-runtime",
            "Rendering runtime",
            "Writing docker compose plan for the node.",
            88);
        file_support_.WriteTextFile(operation.target, naim::RenderComposeYaml(compose_plan));
        PrintOperationApplied(operation, "applied");
        break;
      case naim::HostOperationKind::RemoveComposeFile:
        file_support_.RemoveFileIfExists(operation.target);
        PrintOperationApplied(operation, "applied");
        break;
      case naim::HostOperationKind::ComposeUp:
        maybe_publish_progress(
            "starting-runtime",
            "Starting runtime",
            "Starting infer and worker services on the node.",
            92);
        compose_runtime_support_.EnsureComposeImagesAvailable(compose_plan, compose_mode);
        compose_runtime_support_.EnsureComposeMeshNetworkAvailable(compose_plan, compose_mode);
        compose_runtime_support_.RunComposeCommand(operation.target, "up -d", compose_mode);
        PrintOperationApplied(
            operation,
            compose_mode == ComposeMode::Exec ? "applied" : "skipped");
        break;
      case naim::HostOperationKind::ComposeDown:
        compose_runtime_support_.RunComposeCommand(operation.target, "down", compose_mode);
        PrintOperationApplied(
            operation,
            compose_mode == ComposeMode::Exec ? "applied" : "skipped");
        break;
    }
  };

  for (const auto& operation : plan.operations) {
    if (operation.kind == naim::HostOperationKind::ComposeDown ||
        operation.kind == naim::HostOperationKind::RemoveComposeFile) {
      apply_operation(operation);
    }
  }
  for (const auto& operation : plan.operations) {
    if (operation.kind == naim::HostOperationKind::ComposeDown ||
        operation.kind == naim::HostOperationKind::RemoveComposeFile ||
        operation.kind == naim::HostOperationKind::ComposeUp) {
      continue;
    }
    apply_operation(operation);
  }
  for (const auto& operation : plan.operations) {
    if (operation.kind == naim::HostOperationKind::ComposeUp) {
      apply_operation(operation);
    }
  }

  if (IsDesiredNodeStateEmpty(desired_node_state)) {
    compose_runtime_support_.RemoveComposeMeshNetworkIfUnused(plan.plane_name, compose_mode);
  }
}

std::size_t HostdDesiredStateApplyPlanSupport::ExpectedRuntimeStatusCountForComposePlan(
    const naim::NodeComposePlan& compose_plan) const {
  std::size_t count = 0;
  for (const auto& service : compose_plan.services) {
    const auto role_it = service.environment.find("NAIM_INSTANCE_ROLE");
    if (role_it == service.environment.end()) {
      continue;
    }
    if (role_it->second == "infer" || role_it->second == "worker") {
      ++count;
    }
  }
  return count;
}

bool HostdDesiredStateApplyPlanSupport::IsDesiredNodeStateEmpty(
    const naim::DesiredState& state) {
  return state.disks.empty() && state.instances.empty();
}

void HostdDesiredStateApplyPlanSupport::PrintOperationApplied(
    const naim::HostOperation& operation,
    const std::string& status) const {
  std::cout << "[" << status << "] " << naim::ToString(operation.kind)
            << " " << operation.target;
  if (!operation.details.empty()) {
    std::cout << " :: " << operation.details;
  }
  std::cout << "\n";
}

bool HostdDesiredStateApplyPlanSupport::IsUnderRoot(
    const std::filesystem::path& path,
    const std::optional<std::string>& runtime_root) const {
  if (!runtime_root.has_value()) {
    return false;
  }

  const std::filesystem::path normalized_path = path.lexically_normal();
  const std::filesystem::path normalized_root =
      std::filesystem::path(*runtime_root).lexically_normal();
  const auto path_text = normalized_path.generic_string();
  const auto root_text = normalized_root.generic_string();

  if (root_text == "/") {
    return !path_text.empty() && path_text.front() == '/';
  }

  return path_text == root_text ||
         (path_text.size() > root_text.size() &&
          path_text.compare(0, root_text.size(), root_text) == 0 &&
          path_text[root_text.size()] == '/');
}

void HostdDesiredStateApplyPlanSupport::RemoveDiskDirectory(
    const std::string& path,
    const std::optional<std::string>& runtime_root) const {
  const std::filesystem::path disk_path(path);
  if (!IsUnderRoot(disk_path, runtime_root)) {
    return;
  }

  std::error_code error;
  std::filesystem::remove_all(disk_path, error);
  if ((error == std::errc::permission_denied ||
       error == std::errc::operation_not_permitted) &&
      std::filesystem::exists(disk_path) &&
      disk_path.has_parent_path()) {
    const std::filesystem::path parent = disk_path.parent_path();
    const std::string helper_image = "naim/base-runtime:dev";
    const std::string docker = command_support_.ResolvedDockerCommand();
    if (!command_support_.RunCommandOk(
            docker + " image inspect " + command_support_.ShellQuote(helper_image) +
            " >/dev/null 2>&1")) {
      command_support_.RunCommandOk(
          docker + " pull " + command_support_.ShellQuote(helper_image) +
          " >/dev/null 2>&1");
    }
    const std::string helper_command =
        docker + " run --rm --user 0:0" +
        " -v " + command_support_.ShellQuote(parent.string() + ":/cleanup-parent") +
        " --entrypoint /bin/rm " + command_support_.ShellQuote(helper_image) +
        " -rf -- " +
        command_support_.ShellQuote("/cleanup-parent/" + disk_path.filename().string());
    if (command_support_.RunCommandOk(helper_command)) {
      error.clear();
      std::filesystem::remove_all(disk_path, error);
    }
  }
  if (error) {
    throw std::runtime_error(
        "failed to remove managed disk path '" + path + "': " + error.message());
  }
}

}  // namespace naim::hostd
