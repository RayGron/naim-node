#include <filesystem>
#include <fstream>
#include <iostream>
#include <stdexcept>
#include <string>

#include "app/hostd_bootstrap_model_artifact_support.h"

namespace {

void Expect(bool condition, const std::string& message) {
  if (!condition) {
    throw std::runtime_error(message);
  }
}

naim::DesiredState BuildState(const std::string& shared_root) {
  naim::DesiredState state;
  state.plane_name = "plane-a";
  state.control_root = "/workspace/shared/control/plane-a";
  state.inference.primary_infer_node = "node-primary";

  naim::DiskSpec disk;
  disk.name = "plane-a-shared";
  disk.plane_name = "plane-a";
  disk.node_name = "node-a";
  disk.kind = naim::DiskKind::PlaneShared;
  disk.host_path = shared_root;
  disk.container_path = "/workspace/shared";
  state.disks.push_back(disk);

  naim::NodeInventory primary_node;
  primary_node.name = "node-primary";
  state.nodes.push_back(primary_node);
  return state;
}

}  // namespace

int main() {
  try {
    namespace fs = std::filesystem;

    const naim::hostd::HostdDesiredStatePathSupport path_support;
    const naim::hostd::HostdBootstrapModelArtifactSupport support(path_support);

    {
      auto state = BuildState("/var/lib/naim/disks/plane-a/shared");
      naim::BootstrapModelSpec bootstrap_model;
      bootstrap_model.model_id = "model-a";
      bootstrap_model.source_url = "https://example.com/models/model-a.gguf?download=1";
      state.bootstrap_model = bootstrap_model;
      const auto artifacts = support.BuildArtifacts(state, "node-a");
      Expect(artifacts.size() == 1, "BuildArtifacts should create one artifact");
      Expect(
          artifacts.front().target_host_path ==
              "/var/lib/naim/disks/plane-a/shared/models/gguf/model-a.gguf",
          "BuildArtifacts should resolve target file from URL");
      Expect(
          support.TargetPath(state, "node-a") == artifacts.front().target_host_path,
          "TargetPath should return first artifact target");
    }

    {
      const fs::path temp_root =
          fs::temp_directory_path() / "naim-hostd-bootstrap-model-artifact-support-tests";
      std::error_code cleanup_error;
      fs::remove_all(temp_root, cleanup_error);
      fs::create_directories(temp_root / "hf-model");
      {
        std::ofstream output(temp_root / "hf-model" / "config.json");
        output << "{}";
      }
      Expect(
          naim::hostd::HostdBootstrapModelArtifactSupport::LooksLikeRecognizedModelDirectory(
              (temp_root / "hf-model").string()),
          "LooksLikeRecognizedModelDirectory should detect config.json");
      fs::remove_all(temp_root, cleanup_error);
    }

    {
      auto state = BuildState("/var/lib/naim/disks/plane-a/shared");
      Expect(
          support.SharedModelBootstrapOwnerNode(state) == "node-primary",
          "SharedModelBootstrapOwnerNode should prefer primary infer node");
    }

    std::cout << "ok: hostd-bootstrap-model-artifact-support-build-artifacts\n";
    std::cout << "ok: hostd-bootstrap-model-artifact-support-recognized-directory\n";
    std::cout << "ok: hostd-bootstrap-model-artifact-support-owner-node\n";
    return 0;
  } catch (const std::exception& ex) {
    std::cerr << ex.what() << '\n';
    return 1;
  }
}
