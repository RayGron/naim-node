#include <filesystem>
#include <fstream>
#include <iostream>
#include <stdexcept>
#include <string>

#include "app/hostd_bootstrap_transfer_support.h"

namespace {

void Expect(bool condition, const std::string& message) {
  if (!condition) {
    throw std::runtime_error(message);
  }
}

class RecordingHostdBackend final : public naim::hostd::HostdBackend {
 public:
  std::optional<int> updated_assignment_id;
  nlohmann::json updated_progress = nlohmann::json::object();

  std::optional<naim::HostAssignment> ClaimNextHostAssignment(
      const std::string&) override {
    return std::nullopt;
  }

  bool TransitionClaimedHostAssignment(
      int,
      naim::HostAssignmentStatus,
      const std::string&) override {
    return false;
  }

  bool UpdateHostAssignmentProgress(
      int assignment_id,
      const nlohmann::json& progress) override {
    updated_assignment_id = assignment_id;
    updated_progress = progress;
    return true;
  }

  nlohmann::json RequestModelArtifactChunk(
      const std::string&,
      const std::string&,
      const std::string&,
      std::uintmax_t,
      std::uintmax_t) override {
    return nlohmann::json::object();
  }

  nlohmann::json LoadModelArtifactChunk(
      const std::string&,
      int) override {
    return nlohmann::json::object();
  }

  nlohmann::json RequestModelArtifactManifest(
      const std::string&,
      const std::string&,
      const std::vector<std::string>&) override {
    return nlohmann::json::object();
  }

  nlohmann::json LoadModelArtifactManifest(
      const std::string&,
      int) override {
    return nlohmann::json::object();
  }

  void UpsertHostObservation(const naim::HostObservation&) override {}
  void AppendEvent(const naim::EventRecord&) override {}
  void UpsertDiskRuntimeState(const naim::DiskRuntimeState&) override {}
  std::optional<naim::DiskRuntimeState> LoadDiskRuntimeState(
      const std::string&,
      const std::string&) override {
    return std::nullopt;
  }
};

std::string ReadFile(const std::filesystem::path& path) {
  std::ifstream input(path, std::ios::binary);
  if (!input.is_open()) {
    throw std::runtime_error("failed to open file: " + path.string());
  }
  return std::string(
      std::istreambuf_iterator<char>(input),
      std::istreambuf_iterator<char>());
}

}  // namespace

int main() {
  try {
    namespace fs = std::filesystem;

    const naim::hostd::HostdCommandSupport command_support;
    const naim::hostd::HostdFileSupport file_support;
    const naim::hostd::HostdReportingSupport reporting_support;
    const naim::hostd::HostdBootstrapTransferSupport support(
        command_support,
        file_support,
        reporting_support);

    const fs::path temp_root =
        fs::temp_directory_path() / "naim-hostd-bootstrap-transfer-support-tests";
    std::error_code cleanup_error;
    fs::remove_all(temp_root, cleanup_error);
    fs::create_directories(temp_root);

    {
      const fs::path source_file = temp_root / "sha256.txt";
      {
        std::ofstream output(source_file, std::ios::binary | std::ios::trunc);
        output << "abc";
      }
      Expect(
          support.FileSizeIfExists(source_file.string()).value_or(0) == 3,
          "FileSizeIfExists should read regular file size");
      Expect(
          support.CheckFileSha256Hex(
              source_file.string(),
              "BA7816BF8F01CFEA414140DE5DAE2223B00361A396177A9CB410FF61F20015AD"),
          "CheckFileSha256Hex should compare case-insensitively");
    }

    {
      const fs::path source_root = temp_root / "source-dir";
      fs::create_directories(source_root / "nested");
      {
        std::ofstream output(source_root / "root.bin", std::ios::binary | std::ios::trunc);
        output << "1234";
      }
      {
        std::ofstream output(source_root / "nested" / "child.bin", std::ios::binary | std::ios::trunc);
        output << "567";
      }
      Expect(
          support.FileSizeIfExists(source_root.string()).value_or(0) == 7,
          "FileSizeIfExists should sum directory contents");
    }

    {
      const fs::path source_file = temp_root / "copy-source.gguf";
      const fs::path target_file = temp_root / "target" / "copy-target.gguf";
      {
        std::ofstream output(source_file, std::ios::binary | std::ios::trunc);
        output << "bootstrap-model";
      }
      RecordingHostdBackend backend;
      support.CopyFileWithProgress(
          source_file.string(),
          target_file.string(),
          &backend,
          42,
          "plane-a",
          "node-a");
      Expect(fs::exists(target_file), "CopyFileWithProgress should create target file");
      Expect(
          ReadFile(target_file) == "bootstrap-model",
          "CopyFileWithProgress should preserve file contents");
      Expect(
          backend.updated_assignment_id.has_value() && *backend.updated_assignment_id == 42,
          "CopyFileWithProgress should publish assignment progress");
      Expect(
          backend.updated_progress.at("phase").get<std::string>() == "acquiring-model",
          "CopyFileWithProgress should publish acquiring-model phase");
    }

    {
      const fs::path source_root = temp_root / "copy-source-dir";
      const fs::path target_root = temp_root / "target-dir" / "model";
      fs::create_directories(source_root / "nested");
      {
        std::ofstream output(source_root / "config.json", std::ios::binary | std::ios::trunc);
        output << "{}";
      }
      {
        std::ofstream output(source_root / "nested" / "weights.gguf", std::ios::binary | std::ios::trunc);
        output << "gguf-data";
      }
      RecordingHostdBackend backend;
      support.CopyFileWithProgress(
          source_root.string(),
          target_root.string(),
          &backend,
          7,
          "plane-a",
          "node-a",
          1,
          2,
          5,
          20);
      Expect(
          fs::exists(target_root / "config.json") && fs::exists(target_root / "nested" / "weights.gguf"),
          "CopyFileWithProgress should copy directory trees");
      Expect(
          backend.updated_progress.at("bytes_total").get<std::uintmax_t>() == 20,
          "CopyFileWithProgress should keep aggregate total");
    }

    fs::remove_all(temp_root, cleanup_error);

    std::cout << "ok: hostd-bootstrap-transfer-support-file-size-and-sha256\n";
    std::cout << "ok: hostd-bootstrap-transfer-support-copy-file-progress\n";
    std::cout << "ok: hostd-bootstrap-transfer-support-copy-directory-progress\n";
    return 0;
  } catch (const std::exception& ex) {
    std::cerr << ex.what() << '\n';
    return 1;
  }
}
