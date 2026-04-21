#include <filesystem>
#include <fstream>
#include <iostream>
#include <stdexcept>
#include <string>

#include "app/hostd_bootstrap_active_model_support.h"

namespace {

void Expect(bool condition, const std::string& message) {
  if (!condition) {
    throw std::runtime_error(message);
  }
}

std::string ReadFile(const std::filesystem::path& path) {
  std::ifstream input(path, std::ios::binary);
  if (!input.is_open()) {
    throw std::runtime_error("failed to open file: " + path.string());
  }
  return std::string(
      std::istreambuf_iterator<char>(input),
      std::istreambuf_iterator<char>());
}

naim::DesiredState BuildState(const std::string& shared_root) {
  naim::DesiredState state;
  state.plane_name = "plane-a";
  state.control_root = "/workspace/shared/control/plane-a";

  naim::DiskSpec disk;
  disk.name = "plane-a-shared";
  disk.plane_name = "plane-a";
  disk.node_name = "node-a";
  disk.kind = naim::DiskKind::PlaneShared;
  disk.host_path = shared_root;
  disk.container_path = "/workspace/shared";
  state.disks.push_back(disk);

  naim::NodeInventory node;
  node.name = "node-a";
  state.nodes.push_back(node);

  naim::InstanceSpec infer;
  infer.name = "infer-a";
  infer.role = naim::InstanceRole::Infer;
  infer.node_name = "node-a";
  state.instances.push_back(infer);

  naim::BootstrapModelSpec bootstrap_model;
  bootstrap_model.model_id = "model-a";
  state.bootstrap_model = bootstrap_model;
  return state;
}

}  // namespace

int main() {
  try {
    namespace fs = std::filesystem;

    const naim::hostd::HostdDesiredStatePathSupport path_support;
    const naim::hostd::HostdFileSupport file_support;
    const naim::hostd::HostdBootstrapModelArtifactSupport artifact_support(path_support);
    const naim::hostd::HostdBootstrapActiveModelSupport support(
        path_support,
        file_support,
        artifact_support);

    const fs::path temp_root =
        fs::temp_directory_path() / "naim-hostd-bootstrap-active-model-support-tests";
    std::error_code cleanup_error;
    fs::remove_all(temp_root, cleanup_error);
    fs::create_directories(temp_root / "shared");

    auto state = BuildState((temp_root / "shared").string());
    support.WriteBootstrapActiveModel(
        state,
        "node-a",
        (temp_root / "shared" / "models" / "gguf" / "model.gguf").string());

    const fs::path active_model_path =
        temp_root / "shared" / "control" / "plane-a" / "active-model.json";
    Expect(fs::exists(active_model_path), "WriteBootstrapActiveModel should create active-model");
    const auto contents = ReadFile(active_model_path);
    Expect(
        contents.find("\"runtime_model_path\": \"/workspace/shared/models/gguf/model.gguf\"") !=
            std::string::npos,
        "WriteBootstrapActiveModel should store runtime model path");
    Expect(
        support.ActiveModelPathForNode(state, "node-a") == active_model_path.string(),
        "ActiveModelPathForNode should resolve control file location");

    fs::remove_all(temp_root, cleanup_error);

    std::cout << "ok: hostd-bootstrap-active-model-support-write-active-model\n";
    std::cout << "ok: hostd-bootstrap-active-model-support-active-model-path\n";
    return 0;
  } catch (const std::exception& ex) {
    std::cerr << ex.what() << '\n';
    return 1;
  }
}
