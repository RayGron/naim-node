#include <filesystem>
#include <iostream>
#include <stdexcept>
#include <string>

#include "app/hostd_command_support.h"
#include "app/hostd_desired_state_path_support.h"
#include "app/hostd_disk_runtime_support.h"
#include "app/hostd_file_support.h"

namespace {

void Expect(bool condition, const std::string& message) {
  if (!condition) {
    throw std::runtime_error(message);
  }
}

naim::DiskSpec BuildDisk(
    const std::string& plane_name,
    const std::string& node_name,
    const std::string& disk_name,
    const std::string& host_path) {
  naim::DiskSpec disk;
  disk.name = disk_name;
  disk.plane_name = plane_name;
  disk.node_name = node_name;
  disk.host_path = host_path;
  disk.kind = naim::DiskKind::PlaneShared;
  disk.size_gb = 2;
  return disk;
}

}  // namespace

int main() {
  try {
    const naim::hostd::HostdCommandSupport command_support;
    const naim::hostd::HostdDesiredStatePathSupport path_support;
    const naim::hostd::HostdFileSupport file_support;
    const naim::hostd::HostdDiskRuntimeSupport support(
        command_support,
        path_support,
        file_support);

    {
      const auto [disk_name, node_name] = support.SplitDiskKey("shared@node-a");
      Expect(disk_name == "shared", "SplitDiskKey should return disk name");
      Expect(node_name == "node-a", "SplitDiskKey should return node name");
    }

    {
      naim::DesiredState state;
      state.plane_name = "plane-a";
      state.disks.push_back(BuildDisk("plane-a", "node-a", "shared", "/tmp/shared"));
      const auto disk = support.FindDiskInStateByKey(state, "shared@node-a");
      Expect(disk.has_value(), "FindDiskInStateByKey should find disk");
      Expect(disk->name == "shared", "FindDiskInStateByKey should preserve name");
    }

    {
      namespace fs = std::filesystem;
      const fs::path temp_root =
          fs::temp_directory_path() / "naim-hostd-disk-runtime-support-tests";
      std::error_code cleanup_error;
      fs::remove_all(temp_root, cleanup_error);
      fs::create_directories(temp_root);
      const auto disk = BuildDisk(
          "plane-b",
          "node-b",
          "shared",
          (temp_root / "runtime-disk").string());
      const auto state = support.EnsureDesiredDiskRuntimeState(
          disk,
          "shared@node-b",
          "/storage",
          temp_root.string());
      Expect(
          state.runtime_state == "directory-backed-fallback" ||
              state.runtime_state == "mounted",
          "EnsureDesiredDiskRuntimeState should realize disk runtime");
      Expect(fs::exists(temp_root / "runtime-disk"), "realized disk path should exist");
      fs::remove_all(temp_root, cleanup_error);
    }

    std::cout << "ok: hostd-disk-runtime-support-split-key\n";
    std::cout << "ok: hostd-disk-runtime-support-find-disk\n";
    std::cout << "ok: hostd-disk-runtime-support-realize-disk\n";
    return 0;
  } catch (const std::exception& ex) {
    std::cerr << ex.what() << '\n';
    return 1;
  }
}
