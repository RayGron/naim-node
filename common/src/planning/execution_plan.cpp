#include "naim/planning/execution_plan.h"

#include <algorithm>
#include <filesystem>
#include <map>
#include <optional>
#include <set>
#include <sstream>
#include <string>
#include <vector>

#include "naim/runtime/infer_runtime_config.h"

namespace naim {

namespace {

std::string DiskKey(const DiskSpec& disk) {
  return disk.name + "@" + disk.node_name;
}

std::string DiskSignature(const DiskSpec& disk) {
  return disk.plane_name + "|" + disk.owner_name + "|" + disk.host_path + "|" +
         disk.container_path + "|" + std::to_string(disk.size_gb) + "|" +
         ToString(disk.kind);
}

std::string OptionalValue(const std::optional<std::string>& value) {
  return value.value_or("-");
}

std::string MapSignature(const std::map<std::string, std::string>& values) {
  std::ostringstream out;
  bool first = true;
  for (const auto& [key, value] : values) {
    if (!first) {
      out << ";";
    }
    first = false;
    out << key << "=" << value;
  }
  return out.str();
}

std::string PublishedPortsSignature(const std::vector<PublishedPort>& ports) {
  std::vector<std::string> entries;
  entries.reserve(ports.size());
  for (const auto& port : ports) {
    entries.push_back(
        port.host_ip + ":" + std::to_string(port.host_port) + ":" +
        std::to_string(port.container_port));
  }
  std::sort(entries.begin(), entries.end());
  std::ostringstream out;
  bool first = true;
  for (const auto& entry : entries) {
    if (!first) {
      out << ";";
    }
    first = false;
    out << entry;
  }
  return out.str();
}

std::string InstanceSignature(const InstanceSpec& instance) {
  return instance.node_name + "|" + instance.image + "|" + instance.command + "|" +
         instance.private_disk_name + "|" + instance.shared_disk_name + "|" +
         OptionalValue(instance.gpu_device) + "|" + ToString(instance.share_mode) + "|" +
         std::to_string(instance.gpu_fraction) + "|" + std::to_string(instance.priority) + "|" +
         std::to_string(instance.preemptible ? 1 : 0) + "|" +
         std::to_string(instance.memory_cap_mb.value_or(0)) + "|" +
         PublishedPortsSignature(instance.published_ports) + "|" +
         MapSignature(instance.environment) + "|" + MapSignature(instance.labels);
}

std::map<std::string, DiskSpec> BuildDiskMapForNode(
    const std::vector<DiskSpec>& disks,
    const std::string& node_name) {
  std::map<std::string, DiskSpec> result;
  for (const auto& disk : disks) {
    if (disk.node_name == node_name) {
      result[DiskKey(disk)] = disk;
    }
  }
  return result;
}

std::map<std::string, InstanceSpec> BuildInstanceMapForNode(
    const std::vector<InstanceSpec>& instances,
    const std::string& node_name) {
  std::map<std::string, InstanceSpec> result;
  for (const auto& instance : instances) {
    if (instance.node_name == node_name) {
      result[instance.name] = instance;
    }
  }
  return result;
}

bool HasDesiredServices(const std::map<std::string, InstanceSpec>& desired_instances) {
  return !desired_instances.empty();
}

bool HasCurrentServices(const std::map<std::string, InstanceSpec>& current_instances) {
  return !current_instances.empty();
}

std::string ComposePath(
    const std::string& artifacts_root,
    const std::string& plane_name,
    const std::string& node_name) {
  return (std::filesystem::path(artifacts_root) / plane_name / node_name / "docker-compose.yml")
      .string();
}

std::optional<std::filesystem::path> ControlRootPathForNode(
    const DesiredState& state,
    const std::string& node_name) {
  bool has_infer_on_node = false;
  for (const auto& instance : state.instances) {
    if (instance.node_name == node_name && instance.role == InstanceRole::Infer) {
      has_infer_on_node = true;
      break;
    }
  }
  if (!has_infer_on_node) {
    return std::nullopt;
  }

  const DiskSpec* shared_disk = nullptr;
  for (const auto& disk : state.disks) {
    if (disk.node_name == node_name && disk.kind == DiskKind::PlaneShared) {
      shared_disk = &disk;
      break;
    }
  }
  if (shared_disk == nullptr) {
    return std::nullopt;
  }

  const std::filesystem::path control_root(state.control_root);
  const std::filesystem::path shared_container_path(shared_disk->container_path);
  std::filesystem::path relative_control_path;
  if (!state.control_root.empty() &&
      control_root.is_absolute() &&
      shared_container_path.is_absolute()) {
    const auto control_text = control_root.generic_string();
    const auto shared_text = shared_container_path.generic_string();
    if (control_text == shared_text) {
      relative_control_path = ".";
    } else if (control_text.size() > shared_text.size() &&
               control_text.compare(0, shared_text.size(), shared_text) == 0 &&
               control_text[shared_text.size()] == '/') {
      relative_control_path = control_root.lexically_relative(shared_container_path);
    }
  }
  if (relative_control_path.empty()) {
    relative_control_path = std::filesystem::path("control") / state.plane_name;
  }

  return std::filesystem::path(shared_disk->host_path) / relative_control_path;
}

std::map<std::string, std::string> InferRuntimeConfigPathsForNode(
    const DesiredState& state,
    const std::string& node_name) {
  std::map<std::string, std::string> result;
  const auto control_root = ControlRootPathForNode(state, node_name);
  if (!control_root.has_value()) {
    return result;
  }
  for (const auto& instance : state.instances) {
    if (instance.node_name != node_name || instance.role != InstanceRole::Infer) {
      continue;
    }
    result[instance.name] =
        ((*control_root) / InferRuntimeConfigRelativePath(instance.name)).string();
  }
  return result;
}

std::map<std::string, std::string> InferRuntimeSignaturesForNode(
    const DesiredState& state,
    const std::string& node_name) {
  std::map<std::string, std::string> result;
  for (const auto& instance : state.instances) {
    if (instance.node_name != node_name || instance.role != InstanceRole::Infer) {
      continue;
    }
    result[instance.name] = RenderInferRuntimeConfigJsonForInstance(state, instance.name);
  }
  return result;
}

bool HasWriteInferRuntimeConfigOperation(
    const std::vector<HostOperation>& operations,
    const std::string& infer_instance_name) {
  return std::any_of(
      operations.begin(),
      operations.end(),
      [&](const HostOperation& operation) {
        return operation.kind == HostOperationKind::WriteInferRuntimeConfig &&
               operation.details == infer_instance_name;
      });
}

}  // namespace

std::vector<NodeExecutionPlan> BuildNodeExecutionPlans(
    const std::optional<DesiredState>& current_state,
    const DesiredState& desired_state,
    const std::string& artifacts_root) {
  std::set<std::string> node_names;
  for (const auto& node : desired_state.nodes) {
    node_names.insert(node.name);
  }
  if (current_state.has_value()) {
    for (const auto& node : current_state->nodes) {
      node_names.insert(node.name);
    }
  }

  std::vector<NodeExecutionPlan> plans;
  for (const auto& node_name : node_names) {
    NodeExecutionPlan plan;
    plan.node_name = node_name;

    const auto current_disks =
        current_state.has_value() ? BuildDiskMapForNode(current_state->disks, node_name)
                                  : std::map<std::string, DiskSpec>{};
    const auto desired_disks = BuildDiskMapForNode(desired_state.disks, node_name);
    const auto current_instances =
        current_state.has_value() ? BuildInstanceMapForNode(current_state->instances, node_name)
                                  : std::map<std::string, InstanceSpec>{};
    const auto desired_instances = BuildInstanceMapForNode(desired_state.instances, node_name);
    const bool has_current_services = HasCurrentServices(current_instances);
    const bool has_desired_services = HasDesiredServices(desired_instances);
    const auto current_infer_runtime_paths =
        current_state.has_value() ? InferRuntimeConfigPathsForNode(*current_state, node_name)
                                  : std::map<std::string, std::string>{};
    const auto desired_infer_runtime_paths =
        InferRuntimeConfigPathsForNode(desired_state, node_name);
    const auto current_infer_runtime_signatures =
        current_state.has_value() ? InferRuntimeSignaturesForNode(*current_state, node_name)
                                  : std::map<std::string, std::string>{};
    const auto desired_infer_runtime_signatures =
        InferRuntimeSignaturesForNode(desired_state, node_name);

    const std::string current_plane_name =
        current_state.has_value() ? current_state->plane_name : desired_state.plane_name;
    const std::string desired_plane_name =
        desired_state.plane_name.empty() ? current_plane_name : desired_state.plane_name;
    const std::string current_compose_path =
        ComposePath(artifacts_root, current_plane_name, node_name);
    const std::string desired_compose_path =
        ComposePath(artifacts_root, desired_plane_name, node_name);
    const bool compose_path_changed =
        has_current_services && current_compose_path != desired_compose_path;

    plan.plane_name = has_desired_services ? desired_plane_name : current_plane_name;
    plan.compose_file_path = has_desired_services ? desired_compose_path : current_compose_path;

    for (const auto& [key, disk] : desired_disks) {
      const auto current_it = current_disks.find(key);
      if (current_it == current_disks.end()) {
        plan.operations.push_back(
            HostOperation{HostOperationKind::EnsureDisk, key, disk.host_path});
        continue;
      }
      if (DiskSignature(current_it->second) != DiskSignature(disk)) {
        plan.operations.push_back(
            HostOperation{HostOperationKind::RemoveDisk, key, current_it->second.host_path});
        plan.operations.push_back(
            HostOperation{HostOperationKind::EnsureDisk, key, disk.host_path});
      }
    }

    for (const auto& [key, disk] : current_disks) {
      if (desired_disks.find(key) == desired_disks.end()) {
        plan.operations.push_back(
            HostOperation{HostOperationKind::RemoveDisk, key, disk.host_path});
      }
    }

    for (const auto& [name, instance] : desired_instances) {
      const auto current_it = current_instances.find(name);
      if (current_it == current_instances.end() ||
          InstanceSignature(current_it->second) != InstanceSignature(instance)) {
        plan.operations.push_back(
            HostOperation{HostOperationKind::EnsureService, name, instance.image});
      }
    }

    for (const auto& [name, instance] : current_instances) {
      if (desired_instances.find(name) == desired_instances.end()) {
        plan.operations.push_back(
            HostOperation{HostOperationKind::RemoveService, name, instance.image});
      }
    }

    for (const auto& [infer_instance_name, desired_runtime_path] : desired_infer_runtime_paths) {
      const auto current_path_it = current_infer_runtime_paths.find(infer_instance_name);
      const auto current_signature_it = current_infer_runtime_signatures.find(infer_instance_name);
      const auto desired_signature_it = desired_infer_runtime_signatures.find(infer_instance_name);
      const bool needs_write =
          current_path_it == current_infer_runtime_paths.end() ||
          current_path_it->second != desired_runtime_path ||
          current_signature_it == current_infer_runtime_signatures.end() ||
          desired_signature_it == desired_infer_runtime_signatures.end() ||
          current_signature_it->second != desired_signature_it->second;
      if (needs_write) {
        plan.operations.push_back(
            HostOperation{
                HostOperationKind::WriteInferRuntimeConfig,
                desired_runtime_path,
                infer_instance_name,
            });
      }
    }

    for (const auto& [infer_instance_name, current_runtime_path] : current_infer_runtime_paths) {
      if (desired_infer_runtime_paths.find(infer_instance_name) != desired_infer_runtime_paths.end()) {
        continue;
      }
      plan.operations.push_back(
          HostOperation{
              HostOperationKind::RemoveInferRuntimeConfig,
              current_runtime_path,
              infer_instance_name,
          });
    }

    const bool has_resource_changes = !plan.operations.empty();

    if (has_current_services &&
        (!has_desired_services || compose_path_changed)) {
      plan.operations.push_back(
          HostOperation{
              HostOperationKind::ComposeDown,
              current_compose_path,
              "docker compose down",
          });
      plan.operations.push_back(
          HostOperation{
              HostOperationKind::RemoveComposeFile,
              current_compose_path,
              "remove stale compose artifact",
          });
    }

    const bool needs_compose_up =
        has_desired_services &&
        (!has_current_services || compose_path_changed || has_resource_changes);

    if (needs_compose_up) {
      for (const auto& [infer_instance_name, desired_runtime_path] : desired_infer_runtime_paths) {
        if (HasWriteInferRuntimeConfigOperation(plan.operations, infer_instance_name)) {
          continue;
        }
        plan.operations.push_back(
            HostOperation{
                HostOperationKind::WriteInferRuntimeConfig,
                desired_runtime_path,
                infer_instance_name,
            });
      }
      plan.operations.push_back(
          HostOperation{
              HostOperationKind::WriteComposeFile,
              desired_compose_path,
              "render compose artifact",
          });
      plan.operations.push_back(
          HostOperation{
              HostOperationKind::ComposeUp,
              desired_compose_path,
              "docker compose up -d",
          });
    }

    if (!plan.operations.empty()) {
      plans.push_back(std::move(plan));
    }
  }

  std::sort(
      plans.begin(),
      plans.end(),
      [](const NodeExecutionPlan& left, const NodeExecutionPlan& right) {
        return left.node_name < right.node_name;
      });
  return plans;
}

std::string RenderNodeExecutionPlans(const std::vector<NodeExecutionPlan>& plans) {
  std::ostringstream out;
  out << "host-plans:\n";
  for (const auto& plan : plans) {
    out << "  node " << plan.node_name << ":\n";
    out << "    compose=" << plan.compose_file_path << "\n";
    for (const auto& operation : plan.operations) {
      out << "    - [" << ToString(operation.kind) << "] " << operation.target;
      if (!operation.details.empty()) {
        out << " :: " << operation.details;
      }
      out << "\n";
    }
  }
  return out.str();
}

std::string ToString(HostOperationKind kind) {
  switch (kind) {
    case HostOperationKind::EnsureDisk:
      return "ensure-disk";
    case HostOperationKind::RemoveDisk:
      return "remove-disk";
    case HostOperationKind::EnsureService:
      return "ensure-service";
    case HostOperationKind::RemoveService:
      return "remove-service";
    case HostOperationKind::WriteInferRuntimeConfig:
      return "write-infer-runtime";
    case HostOperationKind::RemoveInferRuntimeConfig:
      return "remove-infer-runtime";
    case HostOperationKind::WriteComposeFile:
      return "write-compose";
    case HostOperationKind::RemoveComposeFile:
      return "remove-compose";
    case HostOperationKind::ComposeUp:
      return "compose-up";
    case HostOperationKind::ComposeDown:
      return "compose-down";
  }
  return "unknown";
}

}  // namespace naim
