#pragma once

#include <filesystem>
#include <optional>
#include <string>

#include "naim/state/models.h"

namespace naim::hostd {

class HostdDesiredStatePathSupport final {
 public:
  static constexpr const char* kDefaultManagedStorageRoot = "/var/lib/naim";

  std::string RebaseManagedStorageRoot(
      const std::string& path,
      const std::string& storage_root) const;
  std::string RebaseManagedPath(
      const std::string& path,
      const std::string& storage_root,
      const std::optional<std::string>& runtime_root,
      const std::optional<std::string>& node_name = std::nullopt) const;
  naim::DesiredState RebaseStateForRuntimeRoot(
      naim::DesiredState state,
      const std::string& storage_root,
      const std::optional<std::string>& runtime_root) const;
  const naim::DiskSpec* FindSharedDiskForNode(
      const naim::DesiredState& state,
      const std::string& node_name) const;
  std::optional<std::string> ControlFilePathForNode(
      const naim::DesiredState& state,
      const std::string& node_name,
      const std::string& file_name) const;
  std::optional<std::string> InferRuntimeConfigPathForNode(
      const naim::DesiredState& state,
      const std::string& node_name) const;
  std::optional<std::string> InferRuntimeStatusPathForInstance(
      const naim::DesiredState& state,
      const naim::InstanceSpec& infer_instance) const;
  std::optional<std::string> RuntimeStatusPathForNode(
      const naim::DesiredState& state,
      const std::string& node_name) const;
  std::string SharedDiskHostPathForContainerPath(
      const naim::DiskSpec& shared_disk,
      const std::string& container_path,
      const std::string& fallback_relative_path) const;

 private:
  bool HasPathPrefix(
      const std::filesystem::path& path,
      const std::filesystem::path& prefix) const;
  const naim::InstanceSpec* PrimaryInferInstanceForNode(
      const naim::DesiredState& state,
      const std::string& node_name) const;
};

}  // namespace naim::hostd
