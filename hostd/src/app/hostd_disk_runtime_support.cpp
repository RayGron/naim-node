#include "app/hostd_disk_runtime_support.h"

#include <algorithm>
#include <array>
#include <chrono>
#include <cctype>
#include <filesystem>
#include <fstream>
#include <stdexcept>
#include <string>
#include <thread>

#include "naim/core/platform_compat.h"

namespace naim::hostd {

HostdDiskRuntimeSupport::HostdDiskRuntimeSupport(
    const HostdCommandSupport& command_support,
    const HostdDesiredStatePathSupport& path_support,
    const HostdFileSupport& file_support)
    : command_support_(command_support),
      path_support_(path_support),
      file_support_(file_support) {}

bool HostdDiskRuntimeSupport::IsUnderRoot(
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

std::string HostdDiskRuntimeSupport::SanitizeDiskPathComponent(
    const std::string& value) const {
  std::string result;
  result.reserve(value.size());
  for (char ch : value) {
    if (std::isalnum(static_cast<unsigned char>(ch)) != 0 || ch == '-' || ch == '_') {
      result.push_back(ch);
    } else {
      result.push_back('_');
    }
  }
  return result;
}

std::string HostdDiskRuntimeSupport::ManagedDiskImagePath(
    const naim::DiskSpec& disk,
    const std::string& storage_root,
    const std::optional<std::string>& runtime_root) const {
  if (disk.kind == naim::DiskKind::PlaneShared) {
    const std::filesystem::path base(
        path_support_.RebaseManagedPath(
            std::string(HostdDesiredStatePathSupport::kDefaultManagedStorageRoot) +
                "/disk-images",
            storage_root,
            runtime_root,
            std::nullopt));
    return (
        base /
        SanitizeDiskPathComponent(disk.plane_name) /
        "shared" /
        (SanitizeDiskPathComponent(disk.name) + ".img"))
        .string();
  }

  const std::filesystem::path base(
      path_support_.RebaseManagedPath(
          std::string(HostdDesiredStatePathSupport::kDefaultManagedStorageRoot) +
              "/disk-images",
          storage_root,
          runtime_root,
          naim::IsNodeLocalDiskKind(disk.kind) ? std::optional<std::string>(disk.node_name)
                                                : std::nullopt));
  return (
      base /
      SanitizeDiskPathComponent(disk.plane_name) /
      SanitizeDiskPathComponent(disk.node_name) /
      (SanitizeDiskPathComponent(disk.name) + ".img"))
      .string();
}

void HostdDiskRuntimeSupport::EnsureDiskDirectory(
    const std::string& path,
    const std::string& disk_key) const {
  std::filesystem::create_directories(path);
  file_support_.WriteTextFile(
      (std::filesystem::path(path) / ".naim-disk-info").string(),
      "disk=" + disk_key + "\nmanaged_by=naim-hostd\n");
}

void HostdDiskRuntimeSupport::RemoveDiskDirectory(
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

bool HostdDiskRuntimeSupport::HostCanManageRealDisks() const {
  return naim::platform::HasElevatedPrivileges();
}

std::string HostdDiskRuntimeSupport::NormalizeManagedPath(const std::string& path) const {
  std::error_code error;
  const auto normalized = std::filesystem::weakly_canonical(path, error);
  if (!error) {
    return normalized.string();
  }
  return std::filesystem::path(path).lexically_normal().string();
}

std::string HostdDiskRuntimeSupport::NormalizeLoopImagePath(
    const std::string& image_path) const {
  return NormalizeManagedPath(image_path);
}

std::string HostdDiskRuntimeSupport::NormalizeMountPointPath(
    const std::string& mount_point) const {
  return NormalizeManagedPath(mount_point);
}

std::optional<std::string> HostdDiskRuntimeSupport::DetectExistingLoopDevice(
    const std::string& image_path) const {
  const std::array<std::string, 2> candidates = {
      image_path,
      NormalizeLoopImagePath(image_path),
  };
  for (const auto& candidate : candidates) {
    if (candidate.empty()) {
      continue;
    }
    const std::string output =
        command_support_.RunCommandCapture(
            "/usr/sbin/losetup -j " + command_support_.ShellQuote(candidate) +
            " 2>/dev/null || true");
    const std::string trimmed = command_support_.Trim(output);
    if (trimmed.empty()) {
      continue;
    }
    const auto colon = trimmed.find(':');
    if (colon == std::string::npos) {
      continue;
    }
    return trimmed.substr(0, colon);
  }
  return std::nullopt;
}

std::string HostdDiskRuntimeSupport::RequireLoopDeviceForImage(
    const std::string& image_path) const {
  if (const auto existing = DetectExistingLoopDevice(image_path); existing.has_value()) {
    return *existing;
  }
  const std::string attach_path = NormalizeLoopImagePath(image_path);
  const std::string output =
      command_support_.RunCommandCapture(
          "/usr/sbin/losetup --find --show " + command_support_.ShellQuote(attach_path) +
          " 2>/dev/null");
  const std::string loop_device = command_support_.Trim(output);
  if (loop_device.empty()) {
    throw std::runtime_error("failed to attach loop device for image '" + image_path + "'");
  }
  return loop_device;
}

std::string HostdDiskRuntimeSupport::DetectFilesystemTypeForDevice(
    const std::string& device_path) const {
  return command_support_.Trim(
      command_support_.RunCommandCapture(
          "/usr/sbin/blkid -o value -s TYPE " + command_support_.ShellQuote(device_path) +
          " 2>/dev/null || true"));
}

bool HostdDiskRuntimeSupport::IsPathMounted(const std::string& mount_point) const {
  if (command_support_.RunCommandOk(
          "/usr/bin/mountpoint -q " + command_support_.ShellQuote(mount_point) +
          " >/dev/null 2>&1")) {
    return true;
  }
  const std::string normalized_mount_point = NormalizeMountPointPath(mount_point);
  return normalized_mount_point != mount_point &&
         command_support_.RunCommandOk(
             "/usr/bin/mountpoint -q " +
             command_support_.ShellQuote(normalized_mount_point) + " >/dev/null 2>&1");
}

std::optional<std::string> HostdDiskRuntimeSupport::CurrentMountSource(
    const std::string& mount_point) const {
  const std::array<std::string, 2> candidates = {
      mount_point,
      NormalizeMountPointPath(mount_point),
  };
  std::ifstream input("/proc/self/mounts");
  if (!input.is_open()) {
    return std::nullopt;
  }

  std::string source;
  std::string target;
  std::string fs_type;
  while (input >> source >> target >> fs_type) {
    std::string rest_of_line;
    std::getline(input, rest_of_line);
    for (const auto& candidate : candidates) {
      if (!candidate.empty() && target == candidate) {
        return source;
      }
    }
  }
  return std::nullopt;
}

void HostdDiskRuntimeSupport::CreateSparseImageFile(
    const std::string& image_path,
    int size_gb) const {
  const auto parent = std::filesystem::path(image_path).parent_path();
  if (!parent.empty()) {
    std::filesystem::create_directories(parent);
  }
  if (!std::filesystem::exists(image_path)) {
    std::ofstream create(image_path, std::ios::binary);
    if (!create.is_open()) {
      throw std::runtime_error("failed to create disk image '" + image_path + "'");
    }
  }

  const std::uintmax_t size_bytes =
      static_cast<std::uintmax_t>(std::max(size_gb, 1)) * 1024ULL * 1024ULL * 1024ULL;
  std::filesystem::resize_file(image_path, size_bytes);
}

bool HostdDiskRuntimeSupport::IsSharedManagedDiskImagePath(
    const std::string& image_path) const {
  return image_path.find("/disk-images/") != std::string::npos &&
         image_path.find("/shared/") != std::string::npos;
}

naim::DiskRuntimeState HostdDiskRuntimeSupport::BuildDiskRuntimeState(
    const naim::DiskSpec& disk,
    const std::string& runtime_state,
    const std::string& status_message) const {
  naim::DiskRuntimeState state;
  state.disk_name = disk.name;
  state.plane_name = disk.plane_name;
  state.node_name = disk.node_name;
  state.filesystem_type = "directory";
  state.mount_point = disk.host_path;
  state.runtime_state = runtime_state;
  state.status_message = status_message;
  return state;
}

naim::DiskRuntimeState HostdDiskRuntimeSupport::EnsureRealDiskMount(
    const naim::DiskSpec& disk,
    const std::string& storage_root,
    const std::optional<std::string>& runtime_root) const {
  naim::DiskRuntimeState runtime_state;
  runtime_state.disk_name = disk.name;
  runtime_state.plane_name = disk.plane_name;
  runtime_state.node_name = disk.node_name;
  runtime_state.image_path = ManagedDiskImagePath(disk, storage_root, runtime_root);
  runtime_state.mount_point = disk.host_path;

  const bool image_preexisting = std::filesystem::exists(runtime_state.image_path);
  CreateSparseImageFile(runtime_state.image_path, disk.size_gb);
  runtime_state.runtime_state = "image-created";

  runtime_state.loop_device = RequireLoopDeviceForImage(runtime_state.image_path);
  runtime_state.attached_at = "attached";
  runtime_state.runtime_state = "attached";

  runtime_state.filesystem_type = DetectFilesystemTypeForDevice(runtime_state.loop_device);
  if (runtime_state.filesystem_type.empty() && image_preexisting &&
      disk.kind == naim::DiskKind::PlaneShared) {
    for (int attempt = 0; attempt < 15; ++attempt) {
      std::this_thread::sleep_for(std::chrono::seconds(2));
      runtime_state.filesystem_type = DetectFilesystemTypeForDevice(runtime_state.loop_device);
      if (!runtime_state.filesystem_type.empty()) {
        break;
      }
    }
  }
  if (runtime_state.filesystem_type.empty()) {
    if (!command_support_.RunCommandOk(
            "/usr/sbin/mkfs.ext4 -F " +
            command_support_.ShellQuote(runtime_state.loop_device) +
            " >/dev/null 2>&1")) {
      throw std::runtime_error(
          "failed to format disk image '" + runtime_state.image_path + "'");
    }
    runtime_state.filesystem_type = "ext4";
  }
  runtime_state.runtime_state = "formatted";

  std::filesystem::create_directories(runtime_state.mount_point);
  if (IsPathMounted(runtime_state.mount_point)) {
    const auto current_source = CurrentMountSource(runtime_state.mount_point);
    if (!current_source.has_value() || *current_source != runtime_state.loop_device) {
      throw std::runtime_error(
          "mount point '" + runtime_state.mount_point +
          "' is already mounted by a different source");
    }
  } else {
    const std::string mount_command =
        "/usr/bin/mount " + command_support_.ShellQuote(runtime_state.loop_device) + " " +
        command_support_.ShellQuote(runtime_state.mount_point) + " >/dev/null 2>&1";
    bool mounted = false;
    for (int attempt = 0; attempt < 5; ++attempt) {
      if (command_support_.RunCommandOk(mount_command)) {
        mounted = true;
        break;
      }
      const auto current_source = CurrentMountSource(runtime_state.mount_point);
      if (IsPathMounted(runtime_state.mount_point) &&
          current_source.has_value() &&
          *current_source == runtime_state.loop_device) {
        mounted = true;
        break;
      }
      if (attempt < 4) {
        std::this_thread::sleep_for(std::chrono::milliseconds(250));
      }
    }
    if (!mounted) {
      throw std::runtime_error(
          "failed to mount loop device '" + runtime_state.loop_device +
          "' at '" + runtime_state.mount_point + "'");
    }
  }

  file_support_.WriteTextFile(
      (std::filesystem::path(runtime_state.mount_point) / ".naim-disk-info").string(),
      "disk=" + disk.name + "@" + disk.node_name + "\nmanaged_by=naim-hostd\nrealized=mounted\n");
  runtime_state.mounted_at = "mounted";
  runtime_state.runtime_state = "mounted";
  runtime_state.status_message = "real mounted disk lifecycle applied by hostd";
  return runtime_state;
}

naim::DiskRuntimeState HostdDiskRuntimeSupport::InspectRealDiskRuntime(
    const naim::DiskSpec& disk,
    const std::string& storage_root,
    const std::optional<std::string>& runtime_root) const {
  naim::DiskRuntimeState runtime_state;
  runtime_state.disk_name = disk.name;
  runtime_state.plane_name = disk.plane_name;
  runtime_state.node_name = disk.node_name;
  runtime_state.image_path = ManagedDiskImagePath(disk, storage_root, runtime_root);
  runtime_state.mount_point = disk.host_path;

  const bool image_exists = std::filesystem::exists(runtime_state.image_path);
  const auto loop_device =
      image_exists ? DetectExistingLoopDevice(runtime_state.image_path) : std::nullopt;
  const bool mounted = IsPathMounted(runtime_state.mount_point);
  const auto mount_source =
      mounted ? CurrentMountSource(runtime_state.mount_point) : std::nullopt;

  if (loop_device.has_value()) {
    runtime_state.loop_device = *loop_device;
    runtime_state.filesystem_type = DetectFilesystemTypeForDevice(*loop_device);
  }

  if (mounted) {
    runtime_state.mounted_at = "mounted";
  }
  if (loop_device.has_value()) {
    runtime_state.attached_at = "attached";
  }

  if (mounted &&
      (!loop_device.has_value() || !mount_source.has_value() || *mount_source != *loop_device)) {
    runtime_state.runtime_state = "drifted";
    runtime_state.status_message = "mount exists but does not match managed loop device";
    return runtime_state;
  }
  if (mounted && loop_device.has_value()) {
    runtime_state.runtime_state = "mounted";
    runtime_state.status_message = "real mounted disk runtime verified by hostd";
    return runtime_state;
  }
  if (loop_device.has_value() && !runtime_state.filesystem_type.empty()) {
    runtime_state.runtime_state = "formatted";
    runtime_state.status_message = "loop device attached but mount missing";
    return runtime_state;
  }
  if (loop_device.has_value()) {
    runtime_state.runtime_state = "attached";
    runtime_state.status_message = "loop device attached";
    return runtime_state;
  }
  if (image_exists) {
    runtime_state.runtime_state = "image-created";
    runtime_state.status_message = "disk image exists but is not attached";
    return runtime_state;
  }
  runtime_state.runtime_state = "missing";
  runtime_state.status_message = "managed disk artifacts missing";
  return runtime_state;
}

std::optional<naim::DiskSpec> HostdDiskRuntimeSupport::FindDiskInStateByKey(
    const std::optional<naim::DesiredState>& state,
    const std::string& disk_key) const {
  if (!state.has_value()) {
    return std::nullopt;
  }
  for (const auto& disk : state->disks) {
    if (disk.name + "@" + disk.node_name == disk_key) {
      return disk;
    }
  }
  return std::nullopt;
}

std::pair<std::string, std::string> HostdDiskRuntimeSupport::SplitDiskKey(
    const std::string& disk_key) const {
  const auto at = disk_key.find('@');
  if (at == std::string::npos) {
    return {disk_key, ""};
  }
  return {disk_key.substr(0, at), disk_key.substr(at + 1)};
}

naim::DiskRuntimeState HostdDiskRuntimeSupport::EnsureDesiredDiskRuntimeState(
    const naim::DiskSpec& disk,
    const std::string& disk_key,
    const std::string& storage_root,
    const std::optional<std::string>& runtime_root) const {
  if (HostCanManageRealDisks()) {
    const auto inspected_state = InspectRealDiskRuntime(disk, storage_root, runtime_root);
    if (inspected_state.runtime_state == "mounted") {
      return inspected_state;
    }
    if (inspected_state.runtime_state == "drifted") {
      throw std::runtime_error(
          "managed disk drift detected for '" + disk_key + "': " +
          inspected_state.status_message);
    }
    return EnsureRealDiskMount(disk, storage_root, runtime_root);
  }

  EnsureDiskDirectory(disk.host_path, disk_key);
  return BuildDiskRuntimeState(
      disk,
      "directory-backed-fallback",
      "real disk lifecycle unavailable; hostd is not running with root privileges");
}

void HostdDiskRuntimeSupport::PersistDiskRuntimeStateForDesiredDisks(
    HostdBackend* backend,
    const naim::DesiredState& desired_node_state,
    const std::string& storage_root,
    const std::optional<std::string>& runtime_root,
    const std::string& status_message) const {
  if (backend == nullptr) {
    return;
  }
  for (const auto& disk : desired_node_state.disks) {
    auto realized_state =
        EnsureDesiredDiskRuntimeState(
            disk,
            disk.name + "@" + disk.node_name,
            storage_root,
            runtime_root);
    realized_state.status_message = status_message;
    backend->UpsertDiskRuntimeState(realized_state);
  }
}

void HostdDiskRuntimeSupport::EnsureDesiredDisksReady(
    HostdBackend* backend,
    const naim::DesiredState& desired_node_state,
    const std::string& storage_root,
    const std::optional<std::string>& runtime_root) const {
  for (const auto& disk : desired_node_state.disks) {
    const auto realized_state =
        EnsureDesiredDiskRuntimeState(
            disk,
            disk.name + "@" + disk.node_name,
            storage_root,
            runtime_root);
    if (backend != nullptr) {
      backend->UpsertDiskRuntimeState(realized_state);
    }
  }
}

void HostdDiskRuntimeSupport::PersistDiskRuntimeStateForRemovedDisks(
    HostdBackend* backend,
    const std::optional<naim::DesiredState>& previous_state,
    const naim::NodeExecutionPlan& execution_plan) const {
  if (backend == nullptr) {
    return;
  }
  for (const auto& operation : execution_plan.operations) {
    if (operation.kind != naim::HostOperationKind::RemoveDisk) {
      continue;
    }
    const auto [disk_name, disk_node_name] = SplitDiskKey(operation.target);
    const auto existing_state = backend->LoadDiskRuntimeState(disk_name, disk_node_name);
    if (existing_state.has_value() && existing_state->runtime_state == "removed") {
      continue;
    }
    const auto removed_disk = FindDiskInStateByKey(previous_state, operation.target);
    if (!removed_disk.has_value()) {
      continue;
    }
    auto runtime_state =
        BuildDiskRuntimeState(*removed_disk, "removed", "runtime path removed by hostd");
    runtime_state.filesystem_type = "";
    runtime_state.mount_point = operation.details;
    backend->UpsertDiskRuntimeState(runtime_state);
  }
}

void HostdDiskRuntimeSupport::RemoveRealDiskMount(
    const naim::DiskRuntimeState& runtime_state,
    const std::optional<std::string>& runtime_root) const {
  bool shared_image_removal_deferred = false;
  std::optional<std::string> mounted_source;
  if (!runtime_state.mount_point.empty() && IsPathMounted(runtime_state.mount_point)) {
    mounted_source = CurrentMountSource(runtime_state.mount_point);
    if (!command_support_.RunCommandOk(
            "/usr/bin/umount " + command_support_.ShellQuote(runtime_state.mount_point) +
            " >/dev/null 2>&1")) {
      throw std::runtime_error(
          "failed to unmount managed disk at '" + runtime_state.mount_point + "'");
    }
  }

  std::optional<std::string> loop_device = runtime_state.loop_device;
  if (!loop_device.has_value() || loop_device->empty()) {
    if (mounted_source.has_value() && mounted_source->rfind("/dev/loop", 0) == 0) {
      loop_device = mounted_source;
    }
  }

  if (loop_device.has_value() && !loop_device->empty()) {
    if (!runtime_state.image_path.empty()) {
      const auto still_attached = DetectExistingLoopDevice(runtime_state.image_path);
      if (still_attached.has_value()) {
        loop_device = still_attached;
      }
    }
    if (loop_device.has_value() && !loop_device->empty()) {
      if (!command_support_.RunCommandOk(
              "/usr/sbin/losetup -d " + command_support_.ShellQuote(*loop_device) +
              " >/dev/null 2>&1")) {
        const bool loop_device_missing = !std::filesystem::exists(*loop_device);
        const auto still_attached =
            runtime_state.image_path.empty()
                ? std::nullopt
                : DetectExistingLoopDevice(runtime_state.image_path);
        if (loop_device_missing || !still_attached.has_value()) {
          loop_device.reset();
        } else if (
            IsSharedManagedDiskImagePath(runtime_state.image_path) &&
            still_attached.has_value() &&
            (!runtime_state.mount_point.empty() && !IsPathMounted(runtime_state.mount_point))) {
          shared_image_removal_deferred = true;
        } else {
          throw std::runtime_error(
              "failed to detach loop device '" + *loop_device + "'");
        }
      }
    }
  }

  if (!runtime_state.mount_point.empty()) {
    RemoveDiskDirectory(runtime_state.mount_point, runtime_root);
  }

  if (!runtime_state.image_path.empty() && !shared_image_removal_deferred) {
    const std::filesystem::path image_path(runtime_state.image_path);
    if (!runtime_root.has_value() || IsUnderRoot(image_path, runtime_root)) {
      std::error_code error;
      std::filesystem::remove(image_path, error);
      if (error) {
        throw std::runtime_error(
            "failed to remove managed disk image '" + runtime_state.image_path +
            "': " + error.message());
      }
    }
  }
}

}  // namespace naim::hostd
