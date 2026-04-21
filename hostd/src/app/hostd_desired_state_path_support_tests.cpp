#include <iostream>
#include <stdexcept>
#include <string>

#include "app/hostd_desired_state_path_support.h"

namespace {

void Expect(bool condition, const std::string& message) {
  if (!condition) {
    throw std::runtime_error(message);
  }
}

naim::DiskSpec BuildSharedDisk() {
  naim::DiskSpec disk;
  disk.name = "shared";
  disk.plane_name = "plane-a";
  disk.node_name = "node-a";
  disk.kind = naim::DiskKind::PlaneShared;
  disk.host_path = "/var/lib/naim/disks/plane-a/shared";
  disk.container_path = "/workspace/shared";
  return disk;
}

naim::DesiredState BuildState() {
  naim::DesiredState state;
  state.plane_name = "plane-a";
  state.control_root = "/workspace/shared/control/plane-a";
  state.disks.push_back(BuildSharedDisk());

  naim::InstanceSpec infer;
  infer.name = "infer-a";
  infer.role = naim::InstanceRole::Infer;
  infer.node_name = "node-a";
  state.instances.push_back(infer);
  return state;
}

}  // namespace

int main() {
  try {
    const naim::hostd::HostdDesiredStatePathSupport support;

    {
      auto state = BuildState();
      state.disks.front().host_path = "/var/lib/naim/disks/plane-a/shared";
      const auto rebased =
          support.RebaseStateForRuntimeRoot(state, "/mnt/storage", "/sandbox/runtime");
      Expect(
          rebased.disks.front().host_path ==
              "/sandbox/runtime/mnt/storage/disks/plane-a/shared",
          "RebaseStateForRuntimeRoot should rebase managed disk paths");
    }

    {
      const auto control_path = support.ControlFilePathForNode(BuildState(), "node-a", "file.json");
      Expect(control_path.has_value(), "ControlFilePathForNode should resolve");
      Expect(
          *control_path == "/var/lib/naim/disks/plane-a/shared/control/plane-a/file.json",
          "ControlFilePathForNode should map into shared disk control root");
    }

    {
      const auto runtime_status = support.RuntimeStatusPathForNode(BuildState(), "node-a");
      Expect(runtime_status.has_value(), "RuntimeStatusPathForNode should resolve");
      Expect(
          *runtime_status ==
              "/var/lib/naim/disks/plane-a/shared/control/plane-a/infer/infer-a/runtime-status.json",
          "RuntimeStatusPathForNode should prefer infer runtime status file");
    }

    {
      const auto shared_path = support.SharedDiskHostPathForContainerPath(
          BuildSharedDisk(),
          "/workspace/shared/models/gguf/model.gguf",
          "models/fallback.gguf");
      Expect(
          shared_path == "/var/lib/naim/disks/plane-a/shared/models/gguf/model.gguf",
          "SharedDiskHostPathForContainerPath should preserve relative path");
    }

    std::cout << "ok: hostd-desired-state-path-support-rebase-state\n";
    std::cout << "ok: hostd-desired-state-path-support-control-file\n";
    std::cout << "ok: hostd-desired-state-path-support-runtime-status\n";
    std::cout << "ok: hostd-desired-state-path-support-shared-disk-path\n";
    return 0;
  } catch (const std::exception& ex) {
    std::cerr << ex.what() << '\n';
    return 1;
  }
}
