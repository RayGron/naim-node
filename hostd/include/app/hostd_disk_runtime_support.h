#pragma once

#include <filesystem>
#include <optional>
#include <string>
#include <utility>

#include "app/hostd_command_support.h"
#include "app/hostd_desired_state_path_support.h"
#include "app/hostd_file_support.h"
#include "backend/hostd_backend.h"
#include "naim/planning/execution_plan.h"
#include "naim/state/models.h"

namespace naim::hostd {

class HostdDiskRuntimeSupport final {
 public:
  HostdDiskRuntimeSupport(
      const HostdCommandSupport& command_support,
      const HostdDesiredStatePathSupport& path_support,
      const HostdFileSupport& file_support);

  std::optional<naim::DiskSpec> FindDiskInStateByKey(
      const std::optional<naim::DesiredState>& state,
      const std::string& disk_key) const;
  std::pair<std::string, std::string> SplitDiskKey(const std::string& disk_key) const;

  naim::DiskRuntimeState EnsureDesiredDiskRuntimeState(
      const naim::DiskSpec& disk,
      const std::string& disk_key,
      const std::string& storage_root,
      const std::optional<std::string>& runtime_root) const;

  void PersistDiskRuntimeStateForDesiredDisks(
      HostdBackend* backend,
      const naim::DesiredState& desired_node_state,
      const std::string& storage_root,
      const std::optional<std::string>& runtime_root,
      const std::string& status_message) const;

  void EnsureDesiredDisksReady(
      HostdBackend* backend,
      const naim::DesiredState& desired_node_state,
      const std::string& storage_root,
      const std::optional<std::string>& runtime_root) const;

  void PersistDiskRuntimeStateForRemovedDisks(
      HostdBackend* backend,
      const std::optional<naim::DesiredState>& previous_state,
      const naim::NodeExecutionPlan& execution_plan) const;

 void RemoveRealDiskMount(
      const naim::DiskRuntimeState& runtime_state,
      const std::optional<std::string>& runtime_root) const;

 private:
  bool IsUnderRoot(
      const std::filesystem::path& path,
      const std::optional<std::string>& runtime_root) const;
  std::string SanitizeDiskPathComponent(const std::string& value) const;
  std::string ManagedDiskImagePath(
      const naim::DiskSpec& disk,
      const std::string& storage_root,
      const std::optional<std::string>& runtime_root) const;
  void EnsureDiskDirectory(const std::string& path, const std::string& disk_key) const;
  void RemoveDiskDirectory(
      const std::string& path,
      const std::optional<std::string>& runtime_root) const;
  bool HostCanManageRealDisks() const;
  std::string NormalizeManagedPath(const std::string& path) const;
  std::string NormalizeLoopImagePath(const std::string& image_path) const;
  std::string NormalizeMountPointPath(const std::string& mount_point) const;
  std::optional<std::string> DetectExistingLoopDevice(const std::string& image_path) const;
  std::string RequireLoopDeviceForImage(const std::string& image_path) const;
  std::string DetectFilesystemTypeForDevice(const std::string& device_path) const;
  bool IsPathMounted(const std::string& mount_point) const;
  std::optional<std::string> CurrentMountSource(const std::string& mount_point) const;
  void CreateSparseImageFile(const std::string& image_path, int size_gb) const;
  bool IsSharedManagedDiskImagePath(const std::string& image_path) const;
  naim::DiskRuntimeState BuildDiskRuntimeState(
      const naim::DiskSpec& disk,
      const std::string& runtime_state,
      const std::string& status_message) const;
  naim::DiskRuntimeState EnsureRealDiskMount(
      const naim::DiskSpec& disk,
      const std::string& storage_root,
      const std::optional<std::string>& runtime_root) const;
  naim::DiskRuntimeState InspectRealDiskRuntime(
      const naim::DiskSpec& disk,
      const std::string& storage_root,
      const std::optional<std::string>& runtime_root) const;

  const HostdCommandSupport& command_support_;
  const HostdDesiredStatePathSupport& path_support_;
  const HostdFileSupport& file_support_;
};

}  // namespace naim::hostd
