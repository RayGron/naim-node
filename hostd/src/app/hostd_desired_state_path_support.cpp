#include "app/hostd_desired_state_path_support.h"

#include <filesystem>

#include "naim/runtime/infer_runtime_config.h"

namespace naim::hostd {

bool HostdDesiredStatePathSupport::HasPathPrefix(
    const std::filesystem::path& path,
    const std::filesystem::path& prefix) const {
  const auto path_text = path.lexically_normal().generic_string();
  const auto prefix_text = prefix.lexically_normal().generic_string();
  return path_text == prefix_text ||
         (path_text.size() > prefix_text.size() &&
          path_text.compare(0, prefix_text.size(), prefix_text) == 0 &&
          path_text[prefix_text.size()] == '/');
}

std::string HostdDesiredStatePathSupport::RebaseManagedStorageRoot(
    const std::string& path,
    const std::string& storage_root) const {
  const std::filesystem::path original(path);
  if (!original.is_absolute()) {
    return path;
  }

  const std::filesystem::path default_root(kDefaultManagedStorageRoot);
  if (!HasPathPrefix(original, default_root)) {
    return path;
  }

  const std::filesystem::path configured_root(storage_root);
  if (configured_root.lexically_normal() == default_root.lexically_normal()) {
    return path;
  }

  return (configured_root / original.lexically_relative(default_root)).string();
}

std::string HostdDesiredStatePathSupport::RebaseManagedPath(
    const std::string& path,
    const std::string& storage_root,
    const std::optional<std::string>& runtime_root,
    const std::optional<std::string>& node_name) const {
  const std::string storage_rebased = RebaseManagedStorageRoot(path, storage_root);
  if (!runtime_root.has_value()) {
    return storage_rebased;
  }

  const std::filesystem::path original(storage_rebased);
  const std::filesystem::path base(*runtime_root);
  const std::filesystem::path rebased =
      original.is_absolute() ? (base / original.relative_path()) : (base / original);
  if (node_name.has_value() && !node_name->empty()) {
    return (base / "nodes" / *node_name / rebased.lexically_relative(base)).string();
  }
  return rebased.string();
}

naim::DesiredState HostdDesiredStatePathSupport::RebaseStateForRuntimeRoot(
    naim::DesiredState state,
    const std::string& storage_root,
    const std::optional<std::string>& runtime_root) const {
  if (!runtime_root.has_value() &&
      storage_root == std::string(kDefaultManagedStorageRoot)) {
    return state;
  }

  for (auto& disk : state.disks) {
    disk.host_path = RebaseManagedPath(
        disk.host_path,
        storage_root,
        runtime_root,
        naim::IsNodeLocalDiskKind(disk.kind) ? std::optional<std::string>(disk.node_name)
                                              : std::nullopt);
  }
  return state;
}

const naim::DiskSpec* HostdDesiredStatePathSupport::FindSharedDiskForNode(
    const naim::DesiredState& state,
    const std::string& node_name) const {
  for (const auto& disk : state.disks) {
    if (disk.node_name == node_name && disk.kind == naim::DiskKind::PlaneShared) {
      return &disk;
    }
  }
  return nullptr;
}

std::optional<std::string> HostdDesiredStatePathSupport::ControlFilePathForNode(
    const naim::DesiredState& state,
    const std::string& node_name,
    const std::string& file_name) const {
  const auto* shared_disk = FindSharedDiskForNode(state, node_name);
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
    } else if (
        control_text.size() > shared_text.size() &&
        control_text.compare(0, shared_text.size(), shared_text) == 0 &&
        control_text[shared_text.size()] == '/') {
      relative_control_path = control_root.lexically_relative(shared_container_path);
    }
  }
  if (relative_control_path.empty()) {
    relative_control_path = std::filesystem::path("control") / state.plane_name;
  }

  return (std::filesystem::path(shared_disk->host_path) / relative_control_path / file_name)
      .string();
}

std::optional<std::string> HostdDesiredStatePathSupport::InferRuntimeConfigPathForNode(
    const naim::DesiredState& state,
    const std::string& node_name) const {
  return ControlFilePathForNode(state, node_name, "infer-runtime.json");
}

const naim::InstanceSpec* HostdDesiredStatePathSupport::PrimaryInferInstanceForNode(
    const naim::DesiredState& state,
    const std::string& node_name) const {
  for (const auto& instance : state.instances) {
    if (instance.role == naim::InstanceRole::Infer && instance.node_name == node_name) {
      return &instance;
    }
  }
  return nullptr;
}

std::optional<std::string> HostdDesiredStatePathSupport::InferRuntimeStatusPathForInstance(
    const naim::DesiredState& state,
    const naim::InstanceSpec& infer_instance) const {
  if (infer_instance.role != naim::InstanceRole::Infer || infer_instance.name.empty()) {
    return std::nullopt;
  }
  return ControlFilePathForNode(
      state,
      infer_instance.node_name,
      naim::InferRuntimeStatusRelativePath(infer_instance.name));
}

std::optional<std::string> HostdDesiredStatePathSupport::RuntimeStatusPathForNode(
    const naim::DesiredState& state,
    const std::string& node_name) const {
  if (const auto* infer = PrimaryInferInstanceForNode(state, node_name); infer != nullptr) {
    return InferRuntimeStatusPathForInstance(state, *infer);
  }
  return ControlFilePathForNode(state, node_name, "runtime-status.json");
}

std::string HostdDesiredStatePathSupport::SharedDiskHostPathForContainerPath(
    const naim::DiskSpec& shared_disk,
    const std::string& container_path,
    const std::string& fallback_relative_path) const {
  const std::filesystem::path shared_container_path(shared_disk.container_path);
  const std::filesystem::path requested_path(container_path);
  std::filesystem::path relative_path(fallback_relative_path);
  if (!container_path.empty() &&
      shared_container_path.is_absolute() &&
      requested_path.is_absolute()) {
    const auto shared_text = shared_container_path.generic_string();
    const auto requested_text = requested_path.generic_string();
    if (requested_text == shared_text) {
      relative_path = ".";
    } else if (
        requested_text.size() > shared_text.size() &&
        requested_text.compare(0, shared_text.size(), shared_text) == 0 &&
        requested_text[shared_text.size()] == '/') {
      relative_path = requested_path.lexically_relative(shared_container_path);
    }
  }
  return (std::filesystem::path(shared_disk.host_path) / relative_path).string();
}

}  // namespace naim::hostd
