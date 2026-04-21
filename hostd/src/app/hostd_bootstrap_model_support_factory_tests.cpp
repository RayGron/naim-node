#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <map>
#include <optional>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include <nlohmann/json.hpp>

#include "app/hostd_bootstrap_model_support_factory.h"
#include "backend/hostd_backend.h"
#include "naim/security/crypto_utils.h"

namespace {

void Expect(bool condition, const std::string& message) {
  if (!condition) {
    throw std::runtime_error(message);
  }
}

naim::DesiredState BuildBootstrapState(
    const std::string& shared_root,
    const std::string& source_model_path) {
  naim::DesiredState state;
  state.plane_name = "plane-a";
  state.control_root = "/workspace/shared/control/plane-a";
  naim::BootstrapModelSpec bootstrap_model;
  bootstrap_model.model_id = "model-a";
  bootstrap_model.materialization_mode = "reference";
  bootstrap_model.local_path = source_model_path;
  state.bootstrap_model = bootstrap_model;

  naim::NodeInventory node;
  node.name = "node-a";
  state.nodes.push_back(node);

  naim::DiskSpec disk;
  disk.name = "plane-a-shared";
  disk.plane_name = "plane-a";
  disk.node_name = "node-a";
  disk.kind = naim::DiskKind::PlaneShared;
  disk.host_path = shared_root;
  disk.container_path = "/workspace/shared";
  state.disks.push_back(disk);

  naim::InstanceSpec infer;
  infer.name = "infer-a";
  infer.role = naim::InstanceRole::Infer;
  infer.node_name = "node-a";
  state.instances.push_back(infer);
  return state;
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

std::string BuildManifestCanonicalText(std::vector<nlohmann::json> files) {
  std::sort(files.begin(), files.end(), [](const nlohmann::json& lhs, const nlohmann::json& rhs) {
    const int lhs_root = lhs.value("root_index", 0);
    const int rhs_root = rhs.value("root_index", 0);
    if (lhs_root != rhs_root) {
      return lhs_root < rhs_root;
    }
    return lhs.value("relative_path", std::string{}) <
           rhs.value("relative_path", std::string{});
  });
  std::string canonical = "naim-model-manifest-v1\n";
  for (const auto& file : files) {
    canonical += "file ";
    canonical += std::to_string(file.value("root_index", 0));
    canonical += " ";
    canonical += file.value("relative_path", std::string{});
    canonical += " ";
    canonical += std::to_string(file.value("size_bytes", std::uintmax_t{0}));
    canonical += " ";
    canonical += file.value("sha256", std::string{});
    canonical += "\n";
  }
  return canonical;
}

class RelayHostdBackend final : public naim::hostd::HostdBackend {
 public:
  explicit RelayHostdBackend(std::string artifact_contents)
      : artifacts_({{"/storage/models/remote.gguf", std::move(artifact_contents)}}) {}
  explicit RelayHostdBackend(std::map<std::string, std::string> artifacts)
      : artifacts_(std::move(artifacts)) {}

  std::optional<naim::HostAssignment> ClaimNextHostAssignment(
      const std::string&) override {
    return std::nullopt;
  }

  bool TransitionClaimedHostAssignment(
      int,
      naim::HostAssignmentStatus,
      const std::string&) override {
    return true;
  }

  bool UpdateHostAssignmentProgress(
      int,
      const nlohmann::json& progress) override {
    progress_updates.push_back(progress);
    return true;
  }

  nlohmann::json RequestModelArtifactChunk(
      const std::string& requester_node_name,
      const std::string& source_node_name,
      const std::string& source_path,
      std::uintmax_t offset,
      std::uintmax_t max_bytes) override {
    Expect(requester_node_name == "node-a", "relay requester should be compute node");
    Expect(source_node_name == "storage-a", "relay source should be storage node");
    Expect(artifacts_.count(source_path) != 0, "relay source path should exist");
    chunk_requests_[next_assignment_id_] = Request{source_path, offset, max_bytes};
    return nlohmann::json{
        {"status", "queued"},
        {"assignment_id", next_assignment_id_++},
    };
  }

  nlohmann::json LoadModelArtifactChunk(
      const std::string& requester_node_name,
      int assignment_id) override {
    Expect(requester_node_name == "node-a", "relay poll requester should be compute node");
    const auto request = chunk_requests_.at(assignment_id);
    const auto& artifact_contents = artifacts_.at(request.source_path);
    const std::uintmax_t safe_offset =
        std::min<std::uintmax_t>(request.offset, artifact_contents.size());
    const std::uintmax_t bytes_left = artifact_contents.size() - safe_offset;
    const std::uintmax_t bytes_to_read = std::min(request.max_bytes, bytes_left);
    const auto begin = artifact_contents.begin() + static_cast<std::ptrdiff_t>(safe_offset);
    const auto end = begin + static_cast<std::ptrdiff_t>(bytes_to_read);
    const std::vector<unsigned char> bytes(begin, end);
    const std::uintmax_t next_offset = safe_offset + bytes_to_read;
    return nlohmann::json{
        {"status", "applied"},
        {"status_message", "ok"},
        {"progress",
         nlohmann::json{
             {"phase", "chunk-ready"},
             {"offset", safe_offset},
             {"next_offset", next_offset},
             {"bytes_total", artifact_contents.size()},
             {"eof", next_offset >= artifact_contents.size()},
             {"bytes_base64", naim::EncodeBytesBase64(bytes)},
         }},
    };
  }

  nlohmann::json RequestModelArtifactManifest(
      const std::string& requester_node_name,
      const std::string& source_node_name,
      const std::vector<std::string>& source_paths) override {
    Expect(requester_node_name == "node-a", "manifest requester should be compute node");
    Expect(source_node_name == "storage-a", "manifest source should be storage node");
    manifest_requests_[next_assignment_id_] = source_paths;
    return nlohmann::json{
        {"status", "queued"},
        {"assignment_id", next_assignment_id_++},
    };
  }

  nlohmann::json LoadModelArtifactManifest(
      const std::string& requester_node_name,
      int assignment_id) override {
    Expect(requester_node_name == "node-a", "manifest poll requester should be compute node");
    const auto source_paths = manifest_requests_.at(assignment_id);
    nlohmann::json roots = nlohmann::json::array();
    nlohmann::json files = nlohmann::json::array();
    std::uintmax_t total_size = 0;
    for (std::size_t root_index = 0; root_index < source_paths.size(); ++root_index) {
      const auto& source_path = source_paths[root_index];
      const auto exact = artifacts_.find(source_path);
      if (exact != artifacts_.end()) {
        roots.push_back(
            nlohmann::json{
                {"source_path", source_path},
                {"kind", "file"},
                {"root_index", root_index},
            });
        total_size += exact->second.size();
        files.push_back(
            nlohmann::json{
                {"root_index", root_index},
                {"source_path", source_path},
                {"relative_path", std::filesystem::path(source_path).filename().string()},
                {"size_bytes", exact->second.size()},
                {"sha256", naim::ComputeSha256Hex(exact->second)},
            });
        continue;
      }

      roots.push_back(
          nlohmann::json{
              {"source_path", source_path},
              {"kind", "directory"},
              {"root_index", root_index},
          });
      const std::string prefix = source_path + "/";
      for (const auto& [artifact_path, contents] : artifacts_) {
        if (artifact_path.rfind(prefix, 0) != 0) {
          continue;
        }
        total_size += contents.size();
        files.push_back(
            nlohmann::json{
                {"root_index", root_index},
                {"source_path", artifact_path},
                {"relative_path", artifact_path.substr(prefix.size())},
                {"size_bytes", contents.size()},
                {"sha256", naim::ComputeSha256Hex(contents)},
            });
      }
    }
    const std::string manifest_sha256 =
        naim::ComputeSha256Hex(BuildManifestCanonicalText(files.get<std::vector<nlohmann::json>>()));
    return nlohmann::json{
        {"status", "applied"},
        {"status_message", "ok"},
        {"progress",
         nlohmann::json{
             {"phase", "manifest-ready"},
             {"roots", roots},
             {"files", files},
             {"bytes_total", total_size},
             {"manifest_algorithm", "naim-model-manifest-v1"},
             {"manifest_sha256", manifest_sha256},
         }},
    };
  }

  nlohmann::json RequestFileTransferTicket(
      const std::string&,
      const std::string&,
      const std::vector<std::string>&) override {
    return nlohmann::json{{"status", "not_available"}};
  }

  nlohmann::json ValidateFileTransferTicket(
      const std::string&,
      const std::string&) override {
    return nlohmann::json{{"status", "denied"}};
  }

  nlohmann::json RequestFileUploadTicket(
      const std::string&,
      const std::string&,
      const std::string&,
      const std::string&,
      std::uintmax_t,
      bool) override {
    return nlohmann::json{{"status", "not_available"}};
  }

  nlohmann::json ValidateFileUploadTicket(
      const std::string&,
      const std::string&) override {
    return nlohmann::json{{"status", "denied"}};
  }

  void UpsertHostObservation(const naim::HostObservation&) override {}
  void AppendEvent(const naim::EventRecord&) override {}
  void UpsertDiskRuntimeState(const naim::DiskRuntimeState&) override {}
  std::optional<naim::DiskRuntimeState> LoadDiskRuntimeState(
      const std::string&,
      const std::string&) override {
    return std::nullopt;
  }

  std::vector<nlohmann::json> progress_updates;

 private:
  struct Request {
    std::string source_path;
    std::uintmax_t offset = 0;
    std::uintmax_t max_bytes = 0;
  };

  std::map<std::string, std::string> artifacts_;
  int next_assignment_id_ = 1;
  std::map<int, Request> chunk_requests_;
  std::map<int, std::vector<std::string>> manifest_requests_;
};

}  // namespace

int main() {
  try {
    namespace fs = std::filesystem;

    const naim::hostd::HostdDesiredStatePathSupport path_support;
    const naim::hostd::HostdCommandSupport command_support;
    const naim::hostd::HostdFileSupport file_support;
    const naim::hostd::HostdReportingSupport reporting_support;
    const naim::hostd::HostdBootstrapModelSupportFactory factory(
        path_support,
        command_support,
        file_support,
        reporting_support);

    const fs::path temp_root =
        fs::temp_directory_path() / "naim-hostd-bootstrap-model-support-factory-tests";
    std::error_code cleanup_error;
    fs::remove_all(temp_root, cleanup_error);
    fs::create_directories(temp_root / "shared");
    fs::create_directories(temp_root / "models");

    const fs::path source_model_path = temp_root / "models" / "model.gguf";
    {
      std::ofstream output(source_model_path, std::ios::binary | std::ios::trunc);
      output << "gguf";
    }

    {
      auto state = BuildBootstrapState((temp_root / "shared").string(), source_model_path.string());
      auto support = factory.Create();
      support.BootstrapPlaneModelIfNeeded(state, "node-a", nullptr, std::nullopt);

      const fs::path active_model_path =
          temp_root / "shared" / "control" / "plane-a" / "active-model.json";
      Expect(fs::exists(active_model_path), "factory-created support should write active-model");
      const auto contents = ReadFile(active_model_path);
      Expect(
          contents.find(source_model_path.string()) != std::string::npos,
          "active-model should reference configured source path");
      Expect(
          contents.find("\"model_id\": \"model-a\"") != std::string::npos,
          "active-model should contain model id");
    }

    {
      fs::create_directories(temp_root / "shared-remote");
      auto state = BuildBootstrapState(
          (temp_root / "shared-remote").string(),
          "/storage/models/remote.gguf");
      state.bootstrap_model->source_node_name = "storage-a";
      auto support = factory.Create();
      RelayHostdBackend backend("remote-model-data");
      support.BootstrapPlaneModelIfNeeded(state, "node-a", &backend, 77);

      const fs::path target_model_path =
          temp_root / "shared-remote" / "models" / "gguf" / "remote.gguf";
      Expect(fs::exists(target_model_path), "relay bootstrap should create target model");
      Expect(
          ReadFile(target_model_path) == "remote-model-data",
          "relay bootstrap should preserve remote artifact contents");
      const fs::path active_model_path =
          temp_root / "shared-remote" / "control" / "plane-a" / "active-model.json";
      const auto contents = ReadFile(active_model_path);
      Expect(
          contents.find(target_model_path.string()) != std::string::npos,
          "active-model should reference relayed model in shared disk");
      Expect(
          !backend.progress_updates.empty(),
          "relay bootstrap should publish assignment progress");
    }

    {
      fs::create_directories(temp_root / "shared-multipart");
      auto state = BuildBootstrapState(
          (temp_root / "shared-multipart").string(),
          "/storage/models/llama-00001-of-00002.gguf");
      state.bootstrap_model->source_node_name = "storage-a";
      state.bootstrap_model->source_paths = {
          "/storage/models/llama-00001-of-00002.gguf",
          "/storage/models/llama-00002-of-00002.gguf",
      };
      auto support = factory.Create();
      RelayHostdBackend backend(
          std::map<std::string, std::string>{
              {"/storage/models/llama-00001-of-00002.gguf", "part-one"},
              {"/storage/models/llama-00002-of-00002.gguf", "part-two"},
          });
      support.BootstrapPlaneModelIfNeeded(state, "node-a", &backend, 78);

      const fs::path first_part =
          temp_root / "shared-multipart" / "models" / "gguf" / "llama-00001-of-00002.gguf";
      const fs::path second_part =
          temp_root / "shared-multipart" / "models" / "gguf" / "llama-00002-of-00002.gguf";
      Expect(ReadFile(first_part) == "part-one", "relay bootstrap should write first multipart part");
      Expect(ReadFile(second_part) == "part-two", "relay bootstrap should write second multipart part");
      const fs::path active_model_path =
          temp_root / "shared-multipart" / "control" / "plane-a" / "active-model.json";
      Expect(
          ReadFile(active_model_path).find(first_part.string()) != std::string::npos,
          "active-model should reference first multipart part");
    }

    {
      fs::create_directories(temp_root / "shared-directory");
      auto state = BuildBootstrapState(
          (temp_root / "shared-directory").string(),
          "/storage/models/hf-qwen");
      state.bootstrap_model->source_node_name = "storage-a";
      state.bootstrap_model->source_paths = {"/storage/models/hf-qwen"};
      auto support = factory.Create();
      RelayHostdBackend backend(
          std::map<std::string, std::string>{
              {"/storage/models/hf-qwen/config.json", "{}"},
              {"/storage/models/hf-qwen/nested/tokenizer.json", "tokens"},
          });
      support.BootstrapPlaneModelIfNeeded(state, "node-a", &backend, 79);

      const fs::path target_root =
          temp_root / "shared-directory" / "models" / "gguf" / "hf-qwen";
      Expect(
          ReadFile(target_root / "config.json") == "{}",
          "relay bootstrap should write directory root file");
      Expect(
          ReadFile(target_root / "nested" / "tokenizer.json") == "tokens",
          "relay bootstrap should preserve directory relative paths");
      const fs::path active_model_path =
          temp_root / "shared-directory" / "control" / "plane-a" / "active-model.json";
      Expect(
          ReadFile(active_model_path).find(target_root.string()) != std::string::npos,
          "active-model should reference relayed directory");
    }

    {
      auto state = BuildBootstrapState((temp_root / "shared").string(), source_model_path.string());
      state.bootstrap_model.reset();
      auto support = factory.Create();
      support.BootstrapPlaneModelIfNeeded(state, "node-a", nullptr, std::nullopt);

      const fs::path active_model_path =
          temp_root / "shared" / "control" / "plane-a" / "active-model.json";
      Expect(
          !fs::exists(active_model_path),
          "factory-created support should remove active-model when bootstrap config is absent");
    }

    fs::remove_all(temp_root, cleanup_error);

    std::cout << "ok: hostd-bootstrap-model-support-factory-reference-mode\n";
    std::cout << "ok: hostd-bootstrap-model-support-factory-controller-relay\n";
    std::cout << "ok: hostd-bootstrap-model-support-factory-controller-relay-multipart\n";
    std::cout << "ok: hostd-bootstrap-model-support-factory-controller-relay-directory\n";
    std::cout << "ok: hostd-bootstrap-model-support-factory-remove-active-model\n";
    return 0;
  } catch (const std::exception& ex) {
    std::cerr << ex.what() << '\n';
    return 1;
  }
}
