#include <arpa/inet.h>
#include <chrono>
#include <ctime>
#include <cctype>
#include <cstdio>
#include <cstring>
#include <dlfcn.h>
#include <errno.h>
#include <filesystem>
#include <fstream>
#include <exception>
#include <iomanip>
#include <iostream>
#include <map>
#include <memory>
#include <netdb.h>
#include <netinet/in.h>
#include <optional>
#include <stdexcept>
#include <string>
#include <cstdlib>
#include <sstream>
#include <sys/socket.h>
#include <set>
#include <sys/statvfs.h>
#include <sys/types.h>
#include <thread>
#include <future>
#include <unistd.h>
#include <vector>
#include <array>
#include <algorithm>

#include <nlohmann/json.hpp>
#include <sodium.h>

#include "comet/compose_renderer.h"
#include "comet/crypto_utils.h"
#include "comet/demo_state.h"
#include "comet/execution_plan.h"
#include "comet/infer_runtime_config.h"
#include "comet/models.h"
#include "comet/planner.h"
#include "comet/runtime_status.h"
#include "comet/sqlite_store.h"
#include "comet/state_json.h"

namespace {

using nlohmann::json;

struct CometNodeConfig {
  std::string storage_root = "/var/lib/comet";
};

constexpr const char* kDefaultManagedStorageRoot = "/var/lib/comet";
constexpr const char* kDefaultNodeConfigRelativePath = "config/comet-node-config.json";

std::string DefaultDbPath() {
  return (std::filesystem::path("var") / "controller.sqlite").string();
}

std::string DefaultArtifactsRoot() {
  return (std::filesystem::path("var") / "artifacts").string();
}

std::string DefaultStateRoot() {
  return (std::filesystem::path("var") / "hostd-state").string();
}

enum class ComposeMode {
  Skip,
  Exec,
};

class HostdBackend;

std::string ResolvedDockerCommand();
std::string ShellQuote(const std::string& value);
bool RunCommandOk(const std::string& command);
std::vector<comet::RuntimeProcessStatus> LoadLocalInstanceRuntimeStatuses(
    const std::string& state_root,
    const std::string& node_name,
    const std::optional<std::string>& plane_name = std::nullopt);

void PrintUsage() {
  std::cout
      << "Usage:\n"
      << "  comet-hostd show-demo-ops --node <node-name> [--config <path>]\n"
      << "  comet-hostd show-state-ops --node <node-name> [--db <path>] [--artifacts-root <path>] [--runtime-root <path>] [--state-root <path>] [--config <path>]\n"
      << "  comet-hostd show-local-state --node <node-name> [--state-root <path>]\n"
      << "  comet-hostd show-runtime-status --node <node-name> [--state-root <path>]\n"
      << "  comet-hostd report-observed-state --node <node-name> [--db <path> | --controller <url>] [--host-private-key <path>] [--controller-fingerprint <sha256>] [--state-root <path>]\n"
      << "  comet-hostd apply-state-ops --node <node-name> [--db <path>] [--artifacts-root <path>] [--runtime-root <path>] [--state-root <path>] [--compose-mode skip|exec] [--config <path>]\n"
      << "  comet-hostd apply-next-assignment --node <node-name> [--db <path> | --controller <url>] [--host-private-key <path>] [--controller-fingerprint <sha256>] [--runtime-root <path>] [--state-root <path>] [--compose-mode skip|exec] [--config <path>]\n";
}

std::optional<std::string> ParseNodeArg(int argc, char** argv) {
  for (int index = 2; index < argc; ++index) {
    const std::string arg = argv[index];
    if (arg == "--node" && index + 1 < argc) {
      return std::string(argv[index + 1]);
    }
  }
  return std::nullopt;
}

std::optional<std::string> ParseDbArg(int argc, char** argv) {
  for (int index = 2; index < argc; ++index) {
    const std::string arg = argv[index];
    if (arg == "--db" && index + 1 < argc) {
      return std::string(argv[index + 1]);
    }
  }
  return std::nullopt;
}

std::optional<std::string> ParseControllerArg(int argc, char** argv) {
  for (int index = 2; index < argc; ++index) {
    const std::string arg = argv[index];
    if (arg == "--controller" && index + 1 < argc) {
      return std::string(argv[index + 1]);
    }
  }
  return std::nullopt;
}

std::optional<std::string> ParseArtifactsRootArg(int argc, char** argv) {
  for (int index = 2; index < argc; ++index) {
    const std::string arg = argv[index];
    if (arg == "--artifacts-root" && index + 1 < argc) {
      return std::string(argv[index + 1]);
    }
  }
  return std::nullopt;
}

std::optional<std::string> ParseRuntimeRootArg(int argc, char** argv) {
  for (int index = 2; index < argc; ++index) {
    const std::string arg = argv[index];
    if (arg == "--runtime-root" && index + 1 < argc) {
      return std::string(argv[index + 1]);
    }
  }
  return std::nullopt;
}

std::optional<std::string> ParseComposeModeArg(int argc, char** argv) {
  for (int index = 2; index < argc; ++index) {
    const std::string arg = argv[index];
    if (arg == "--compose-mode" && index + 1 < argc) {
      return std::string(argv[index + 1]);
    }
  }
  return std::nullopt;
}

std::optional<std::string> ParseStateRootArg(int argc, char** argv) {
  for (int index = 2; index < argc; ++index) {
    const std::string arg = argv[index];
    if (arg == "--state-root" && index + 1 < argc) {
      return std::string(argv[index + 1]);
    }
  }
  return std::nullopt;
}

std::optional<std::string> ParseHostPrivateKeyArg(int argc, char** argv) {
  for (int index = 2; index < argc; ++index) {
    const std::string arg = argv[index];
    if (arg == "--host-private-key" && index + 1 < argc) {
      return std::string(argv[index + 1]);
    }
  }
  return std::nullopt;
}

std::optional<std::string> ParseControllerFingerprintArg(int argc, char** argv) {
  for (int index = 2; index < argc; ++index) {
    const std::string arg = argv[index];
    if (arg == "--controller-fingerprint" && index + 1 < argc) {
      return std::string(argv[index + 1]);
    }
  }
  return std::nullopt;
}

std::optional<std::string> ParseConfigArg(int argc, char** argv) {
  for (int index = 2; index < argc; ++index) {
    const std::string arg = argv[index];
    if (arg == "--config" && index + 1 < argc) {
      return std::string(argv[index + 1]);
    }
  }
  return std::nullopt;
}

std::string ResolveDbPath(const std::optional<std::string>& db_arg) {
  return db_arg.value_or(DefaultDbPath());
}

std::string ResolveArtifactsRoot(const std::optional<std::string>& artifacts_root_arg) {
  return artifacts_root_arg.value_or(DefaultArtifactsRoot());
}

std::string ResolveStateRoot(const std::optional<std::string>& state_root_arg) {
  return state_root_arg.value_or(DefaultStateRoot());
}

ComposeMode ResolveComposeMode(const std::optional<std::string>& compose_mode_arg) {
  if (!compose_mode_arg.has_value() || *compose_mode_arg == "skip") {
    return ComposeMode::Skip;
  }
  if (*compose_mode_arg == "exec") {
    return ComposeMode::Exec;
  }
  throw std::runtime_error("unsupported compose mode '" + *compose_mode_arg + "'");
}

std::optional<std::filesystem::path> FindNodeConfigPath(const char* argv0) {
  std::vector<std::filesystem::path> candidates;
  candidates.emplace_back(std::filesystem::current_path() / kDefaultNodeConfigRelativePath);

  if (argv0 != nullptr && *argv0 != '\0') {
    std::error_code error;
    const std::filesystem::path executable_path = std::filesystem::absolute(argv0, error);
    if (!error) {
      std::filesystem::path current = executable_path.parent_path();
      for (int depth = 0; depth < 4 && !current.empty(); ++depth) {
        candidates.emplace_back(current / kDefaultNodeConfigRelativePath);
        current = current.parent_path();
      }
    }
  }

  for (const auto& candidate : candidates) {
    std::error_code error;
    if (std::filesystem::exists(candidate, error) && !error) {
      return candidate.lexically_normal();
    }
  }
  return std::nullopt;
}

CometNodeConfig LoadCometNodeConfig(
    const std::optional<std::string>& config_arg,
    const char* argv0) {
  std::optional<std::filesystem::path> config_path;
  bool explicit_path = false;

  if (config_arg.has_value()) {
    config_path = *config_arg;
    explicit_path = true;
  } else if (const char* env_path = std::getenv("COMET_NODE_CONFIG_PATH");
             env_path != nullptr && *env_path != '\0') {
    config_path = env_path;
    explicit_path = true;
  } else {
    config_path = FindNodeConfigPath(argv0);
  }

  if (!config_path.has_value()) {
    return {};
  }

  if (!std::filesystem::exists(*config_path)) {
    if (explicit_path) {
      throw std::runtime_error(
          "comet node config file not found: " + config_path->string());
    }
    return {};
  }

  std::ifstream input(*config_path);
  if (!input.is_open()) {
    throw std::runtime_error(
        "failed to open comet node config file '" + config_path->string() + "'");
  }

  const json value = json::parse(input, nullptr, true, true);
  if (!value.is_object()) {
    throw std::runtime_error(
        "comet node config must be a JSON object: " + config_path->string());
  }

  CometNodeConfig config;
  if (value.contains("paths")) {
    if (!value.at("paths").is_object()) {
      throw std::runtime_error(
          "comet node config field 'paths' must be an object: " + config_path->string());
    }
    const auto& paths = value.at("paths");
    if (paths.contains("storage_root")) {
      if (!paths.at("storage_root").is_string()) {
        throw std::runtime_error(
            "comet node config field 'paths.storage_root' must be a string: " +
            config_path->string());
      }
      config.storage_root = paths.at("storage_root").get<std::string>();
    }
  } else if (value.contains("storage_root")) {
    if (!value.at("storage_root").is_string()) {
      throw std::runtime_error(
          "comet node config field 'storage_root' must be a string: " +
          config_path->string());
    }
    config.storage_root = value.at("storage_root").get<std::string>();
  }

  if (config.storage_root.empty()) {
    throw std::runtime_error(
        "comet node config storage_root must not be empty: " + config_path->string());
  }

  const std::filesystem::path storage_root_path(config.storage_root);
  if (!storage_root_path.is_absolute()) {
    throw std::runtime_error(
        "comet node config storage_root must be an absolute path: " +
        config_path->string());
  }

  config.storage_root = storage_root_path.lexically_normal().string();
  return config;
}

bool HasPathPrefix(
    const std::filesystem::path& path,
    const std::filesystem::path& prefix) {
  const auto path_text = path.lexically_normal().generic_string();
  const auto prefix_text = prefix.lexically_normal().generic_string();
  return path_text == prefix_text ||
         (path_text.size() > prefix_text.size() &&
          path_text.compare(0, prefix_text.size(), prefix_text) == 0 &&
          path_text[prefix_text.size()] == '/');
}

std::string RebaseManagedStorageRoot(
    const std::string& path,
    const std::string& storage_root) {
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

std::string RebaseManagedPath(
    const std::string& path,
    const std::string& storage_root,
    const std::optional<std::string>& runtime_root,
    const std::optional<std::string>& node_name = std::nullopt) {
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

comet::DesiredState RebaseStateForRuntimeRoot(
    comet::DesiredState state,
    const std::string& storage_root,
    const std::optional<std::string>& runtime_root) {
  if (!runtime_root.has_value() &&
      storage_root == std::string(kDefaultManagedStorageRoot)) {
    return state;
  }

  for (auto& disk : state.disks) {
    const bool node_local_disk =
        disk.kind == comet::DiskKind::InferPrivate ||
        disk.kind == comet::DiskKind::WorkerPrivate;
    disk.host_path = RebaseManagedPath(
        disk.host_path,
        storage_root,
        runtime_root,
        node_local_disk ? std::optional<std::string>(disk.node_name) : std::nullopt);
  }
  return state;
}

comet::NodeExecutionPlan FindNodeExecutionPlan(
    const std::vector<comet::NodeExecutionPlan>& plans,
    const std::string& node_name) {
  for (const auto& plan : plans) {
    if (plan.node_name == node_name) {
      return plan;
    }
  }
  throw std::runtime_error("node '" + node_name + "' not found in execution plan");
}

bool StateHasNode(const comet::DesiredState& state, const std::string& node_name) {
  for (const auto& node : state.nodes) {
    if (node.name == node_name) {
      return true;
    }
  }
  return false;
}

std::string ComposePathForNode(
    const std::string& artifacts_root,
    const std::string& plane_name,
    const std::string& node_name) {
  return (std::filesystem::path(artifacts_root) / plane_name / node_name / "docker-compose.yml")
      .string();
}

bool NodeHasInferInstance(const comet::DesiredState& state, const std::string& node_name) {
  for (const auto& instance : state.instances) {
    if (instance.node_name == node_name && instance.role == comet::InstanceRole::Infer) {
      return true;
    }
  }
  return false;
}

const comet::DiskSpec* FindSharedDiskForNode(
    const comet::DesiredState& state,
    const std::string& node_name) {
  for (const auto& disk : state.disks) {
    if (disk.node_name == node_name && disk.kind == comet::DiskKind::PlaneShared) {
      return &disk;
    }
  }
  return nullptr;
}

std::optional<std::string> InferRuntimeConfigPathForNode(
    const comet::DesiredState& state,
    const std::string& node_name);

std::optional<std::string> ControlFilePathForNode(
    const comet::DesiredState& state,
    const std::string& node_name,
    const std::string& file_name) {
  if (!NodeHasInferInstance(state, node_name)) {
    return std::nullopt;
  }

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
    } else if (control_text.size() > shared_text.size() &&
               control_text.compare(0, shared_text.size(), shared_text) == 0 &&
               control_text[shared_text.size()] == '/') {
      relative_control_path = control_root.lexically_relative(shared_container_path);
    }
  }
  if (relative_control_path.empty()) {
    relative_control_path = std::filesystem::path("control") / state.plane_name;
  }

  return (
      std::filesystem::path(shared_disk->host_path) /
      relative_control_path /
      file_name)
      .string();
}

std::optional<std::string> InferRuntimeConfigPathForNode(
    const comet::DesiredState& state,
    const std::string& node_name) {
  return ControlFilePathForNode(state, node_name, "infer-runtime.json");
}

std::optional<std::string> RuntimeStatusPathForNode(
    const comet::DesiredState& state,
    const std::string& node_name) {
  return ControlFilePathForNode(state, node_name, "runtime-status.json");
}

comet::NodeExecutionPlan ResolveNodeExecutionPlan(
    const std::vector<comet::NodeExecutionPlan>& plans,
    const std::optional<comet::DesiredState>& current_state,
    const comet::DesiredState& desired_state,
    const std::string& node_name,
    const std::string& artifacts_root) {
  for (const auto& plan : plans) {
    if (plan.node_name == node_name) {
      return plan;
    }
  }

  const bool in_current = current_state.has_value() && StateHasNode(*current_state, node_name);
  const bool in_desired = StateHasNode(desired_state, node_name);
  if (!in_current && !in_desired) {
    throw std::runtime_error("node '" + node_name + "' not found in execution plan");
  }

  const std::string plane_name = in_desired ? desired_state.plane_name : current_state->plane_name;
  comet::NodeExecutionPlan plan;
  plan.plane_name = plane_name;
  plan.node_name = node_name;
  plan.compose_file_path = ComposePathForNode(artifacts_root, plane_name, node_name);
  return plan;
}

comet::NodeComposePlan RequireNodeComposePlan(
    const comet::DesiredState& state,
    const std::string& node_name) {
  const auto plan = comet::FindNodeComposePlan(state, node_name);
  if (!plan.has_value()) {
    throw std::runtime_error("node '" + node_name + "' not found in compose plan");
  }
  return *plan;
}

void WriteTextFile(const std::string& path, const std::string& contents) {
  const std::filesystem::path file_path(path);
  if (file_path.has_parent_path()) {
    std::filesystem::create_directories(file_path.parent_path());
  }

  std::ofstream out(path, std::ios::binary | std::ios::trunc);
  if (!out.is_open()) {
    throw std::runtime_error("failed to open file for write: " + path);
  }
  out << contents;
  if (!out.good()) {
    throw std::runtime_error("failed to write file: " + path);
  }
}

bool IsUnderRoot(
    const std::filesystem::path& path,
    const std::optional<std::string>& runtime_root) {
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

std::string SanitizeDiskPathComponent(const std::string& value) {
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

std::string ManagedDiskImagePath(
    const comet::DiskSpec& disk,
    const std::string& storage_root,
    const std::optional<std::string>& runtime_root) {
  if (disk.kind == comet::DiskKind::PlaneShared) {
    const std::filesystem::path base(
        RebaseManagedPath(
            std::string(kDefaultManagedStorageRoot) + "/disk-images",
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

  const bool node_local_disk =
      disk.kind == comet::DiskKind::InferPrivate ||
      disk.kind == comet::DiskKind::WorkerPrivate;
  const std::filesystem::path base(
      RebaseManagedPath(
          std::string(kDefaultManagedStorageRoot) + "/disk-images",
          storage_root,
          runtime_root,
          node_local_disk ? std::optional<std::string>(disk.node_name) : std::nullopt));
  return (
      base /
      SanitizeDiskPathComponent(disk.plane_name) /
      SanitizeDiskPathComponent(disk.node_name) /
      (SanitizeDiskPathComponent(disk.name) + ".img"))
      .string();
}

void EnsureDiskDirectory(const std::string& path, const std::string& disk_key) {
  std::filesystem::create_directories(path);
  WriteTextFile(
      (std::filesystem::path(path) / ".comet-disk-info").string(),
      "disk=" + disk_key + "\nmanaged_by=comet-hostd\n");
}

void RemoveDiskDirectory(
    const std::string& path,
    const std::optional<std::string>& runtime_root) {
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
    const std::string helper_image = "comet/base-runtime:dev";
    const std::string docker = ResolvedDockerCommand();
    if (!RunCommandOk(
            docker + " image inspect " + ShellQuote(helper_image) + " >/dev/null 2>&1")) {
      RunCommandOk(docker + " pull " + ShellQuote(helper_image) + " >/dev/null 2>&1");
    }
    const std::string helper_command =
        docker + " run --rm --user 0:0" +
        " -v " + ShellQuote(parent.string() + ":/cleanup-parent") +
        " --entrypoint /bin/rm " + ShellQuote(helper_image) +
        " -rf -- " + ShellQuote("/cleanup-parent/" + disk_path.filename().string());
    if (RunCommandOk(helper_command)) {
      error.clear();
      std::filesystem::remove_all(disk_path, error);
    }
  }
  if (error) {
    throw std::runtime_error(
        "failed to remove managed disk path '" + path + "': " + error.message());
  }
}

void RemoveFileIfExists(const std::string& path) {
  std::error_code error;
  std::filesystem::remove(path, error);
  if (error) {
    throw std::runtime_error("failed to remove file '" + path + "': " + error.message());
  }
}

void EnsureParentDirectory(const std::string& path) {
  const std::filesystem::path file_path(path);
  if (file_path.has_parent_path()) {
    std::filesystem::create_directories(file_path.parent_path());
  }
}

void RemoveStateFileIfExists(const std::string& path) {
  RemoveFileIfExists(path);
}

std::string LocalPlaneRoot(
    const std::string& state_root,
    const std::string& node_name,
    const std::string& plane_name) {
  return (std::filesystem::path(state_root) / node_name / "planes" / plane_name).string();
}

std::string LocalGenerationPath(const std::string& state_root, const std::string& node_name) {
  return (std::filesystem::path(state_root) / node_name / "applied-generation.txt").string();
}

std::string LocalPlaneGenerationPath(
    const std::string& state_root,
    const std::string& node_name,
    const std::string& plane_name) {
  return (std::filesystem::path(LocalPlaneRoot(state_root, node_name, plane_name)) /
          "applied-generation.txt")
      .string();
}

std::vector<std::string> ListLocalPlaneNames(
    const std::string& state_root,
    const std::string& node_name) {
  std::vector<std::string> result;
  const std::filesystem::path planes_root =
      std::filesystem::path(state_root) / node_name / "planes";
  if (!std::filesystem::exists(planes_root)) {
    return result;
  }
  for (const auto& entry : std::filesystem::directory_iterator(planes_root)) {
    if (!entry.is_directory()) {
      continue;
    }
    result.push_back(entry.path().filename().string());
  }
  std::sort(result.begin(), result.end());
  return result;
}

std::optional<int> LoadGenerationFromPath(const std::string& path) {
  if (!std::filesystem::exists(path)) {
    return std::nullopt;
  }

  std::ifstream input(path);
  if (!input.is_open()) {
    throw std::runtime_error("failed to open generation file: " + path);
  }

  int generation = 0;
  input >> generation;
  if (!input.good() && !input.eof()) {
    throw std::runtime_error("failed to parse generation file: " + path);
  }
  return generation;
}

std::optional<int> LoadLocalAppliedGeneration(
    const std::string& state_root,
    const std::string& node_name,
    const std::optional<std::string>& plane_name = std::nullopt) {
  if (plane_name.has_value()) {
    return LoadGenerationFromPath(LocalPlaneGenerationPath(state_root, node_name, *plane_name));
  }

  const std::string aggregate_path = LocalGenerationPath(state_root, node_name);
  if (std::filesystem::exists(aggregate_path)) {
    return LoadGenerationFromPath(aggregate_path);
  }

  const auto plane_names = ListLocalPlaneNames(state_root, node_name);
  std::optional<int> generation;
  for (const auto& current_plane_name : plane_names) {
    const auto plane_generation =
        LoadGenerationFromPath(LocalPlaneGenerationPath(state_root, node_name, current_plane_name));
    if (!plane_generation.has_value()) {
      continue;
    }
    generation = generation.has_value() ? std::max(*generation, *plane_generation)
                                        : *plane_generation;
  }
  return generation;
}

void SaveLocalAppliedGeneration(
    const std::string& state_root,
    const std::string& node_name,
    int generation,
    const std::optional<std::string>& plane_name = std::nullopt) {
  const std::string path = plane_name.has_value()
                               ? LocalPlaneGenerationPath(state_root, node_name, *plane_name)
                               : LocalGenerationPath(state_root, node_name);
  const std::filesystem::path file_path(path);
  if (file_path.has_parent_path()) {
    std::filesystem::create_directories(file_path.parent_path());
  }

  std::ofstream output(path, std::ios::binary | std::ios::trunc);
  if (!output.is_open()) {
    throw std::runtime_error("failed to open generation file for write: " + path);
  }
  output << generation << "\n";
  if (!output.good()) {
    throw std::runtime_error("failed to write generation file: " + path);
  }
}

void RunComposeCommand(
    const std::string& compose_file_path,
    const std::string& subcommand,
    ComposeMode compose_mode) {
  if (compose_mode == ComposeMode::Skip) {
    return;
  }

  std::string effective_subcommand = subcommand;
  if (subcommand == "up -d") {
    effective_subcommand += " --remove-orphans";
  }
  const std::string command =
      ResolvedDockerCommand() + " compose -f '" + compose_file_path + "' " + effective_subcommand;
  const int rc = std::system(command.c_str());
  if (rc != 0) {
    throw std::runtime_error(
        "compose command failed with exit code " + std::to_string(rc) + ": " + command);
  }
}

void PrintOperationApplied(
    const comet::HostOperation& operation,
    const std::string& status) {
  std::cout << "[" << status << "] " << comet::ToString(operation.kind)
            << " " << operation.target;
  if (!operation.details.empty()) {
    std::cout << " :: " << operation.details;
  }
  std::cout << "\n";
}

std::string RuntimeConfigSummary(const comet::DesiredState& state) {
  std::ostringstream out;
  out << "gpu_nodes=" << state.runtime_gpu_nodes.size()
      << " primary_infer_node=" << state.inference.primary_infer_node
      << " gateway=" << state.gateway.listen_host << ":" << state.gateway.listen_port;
  return out.str();
}

bool NodeHasInferInstance(const comet::DesiredState& state) {
  for (const auto& instance : state.instances) {
    if (instance.role == comet::InstanceRole::Infer) {
      return true;
    }
  }
  return false;
}

std::string LocalStatePath(const std::string& state_root, const std::string& node_name) {
  return (std::filesystem::path(state_root) / node_name / "applied-state.json").string();
}

std::string LocalPlaneStatePath(
    const std::string& state_root,
    const std::string& node_name,
    const std::string& plane_name) {
  return (std::filesystem::path(LocalPlaneRoot(state_root, node_name, plane_name)) /
          "applied-state.json")
      .string();
}

std::optional<comet::DesiredState> LoadStateFromPath(const std::string& path) {
  return comet::LoadDesiredStateJson(path);
}

comet::DesiredState MergeLocalAppliedStates(const std::vector<comet::DesiredState>& states) {
  if (states.empty()) {
    throw std::runtime_error("cannot merge empty local state list");
  }

  comet::DesiredState merged = states.front();
  if (states.size() > 1) {
    merged.plane_name.clear();
    merged.plane_shared_disk_name.clear();
  }
  merged.runtime_gpu_nodes.clear();
  merged.nodes.clear();
  merged.disks.clear();
  merged.instances.clear();

  std::set<std::string> seen_nodes;
  std::set<std::string> seen_runtime_gpu_nodes;
  for (const auto& state : states) {
    for (const auto& node : state.nodes) {
      if (seen_nodes.insert(node.name).second) {
        merged.nodes.push_back(node);
      }
    }
    for (const auto& runtime_gpu_node : state.runtime_gpu_nodes) {
      const std::string key = runtime_gpu_node.node_name + ":" + runtime_gpu_node.gpu_device;
      if (seen_runtime_gpu_nodes.insert(key).second) {
        merged.runtime_gpu_nodes.push_back(runtime_gpu_node);
      }
    }
    merged.disks.insert(merged.disks.end(), state.disks.begin(), state.disks.end());
    merged.instances.insert(merged.instances.end(), state.instances.begin(), state.instances.end());
  }
  return merged;
}

std::vector<comet::DesiredState> LoadAllLocalAppliedStates(
    const std::string& state_root,
    const std::string& node_name) {
  std::vector<comet::DesiredState> result;
  const auto plane_names = ListLocalPlaneNames(state_root, node_name);
  for (const auto& plane_name : plane_names) {
    const auto plane_state =
        LoadStateFromPath(LocalPlaneStatePath(state_root, node_name, plane_name));
    if (plane_state.has_value()) {
      result.push_back(*plane_state);
    }
  }
  if (!result.empty()) {
    return result;
  }

  const auto aggregate_state = LoadStateFromPath(LocalStatePath(state_root, node_name));
  if (aggregate_state.has_value()) {
    result.push_back(*aggregate_state);
  }
  return result;
}

std::optional<comet::DesiredState> LoadLocalAppliedState(
    const std::string& state_root,
    const std::string& node_name,
    const std::optional<std::string>& plane_name = std::nullopt) {
  if (plane_name.has_value()) {
    return LoadStateFromPath(LocalPlaneStatePath(state_root, node_name, *plane_name));
  }

  const auto states = LoadAllLocalAppliedStates(state_root, node_name);
  if (states.empty()) {
    return std::nullopt;
  }
  return MergeLocalAppliedStates(states);
}

void RewriteAggregateLocalState(const std::string& state_root, const std::string& node_name) {
  std::vector<comet::DesiredState> states;
  for (const auto& plane_name : ListLocalPlaneNames(state_root, node_name)) {
    const auto plane_state =
        LoadStateFromPath(LocalPlaneStatePath(state_root, node_name, plane_name));
    if (plane_state.has_value()) {
      states.push_back(*plane_state);
    }
  }
  if (states.empty()) {
    RemoveStateFileIfExists(LocalStatePath(state_root, node_name));
    return;
  }
  comet::SaveDesiredStateJson(
      MergeLocalAppliedStates(states),
      LocalStatePath(state_root, node_name));
}

void RewriteAggregateLocalGeneration(
    const std::string& state_root,
    const std::string& node_name) {
  std::optional<int> generation;
  for (const auto& plane_name : ListLocalPlaneNames(state_root, node_name)) {
    const auto plane_generation =
        LoadGenerationFromPath(LocalPlaneGenerationPath(state_root, node_name, plane_name));
    if (!plane_generation.has_value()) {
      continue;
    }
    generation = generation.has_value() ? std::max(*generation, *plane_generation)
                                        : *plane_generation;
  }
  if (generation.has_value()) {
    SaveLocalAppliedGeneration(state_root, node_name, *generation, std::nullopt);
  } else {
    RemoveStateFileIfExists(LocalGenerationPath(state_root, node_name));
  }
}

std::optional<comet::RuntimeStatus> LoadLocalRuntimeStatus(
    const std::string& state_root,
    const std::string& node_name,
    const std::optional<std::string>& plane_name = std::nullopt) {
  if (plane_name.has_value()) {
    const auto local_state = LoadLocalAppliedState(state_root, node_name, plane_name);
    if (!local_state.has_value()) {
      return std::nullopt;
    }
    const auto runtime_status_path = RuntimeStatusPathForNode(*local_state, node_name);
    if (!runtime_status_path.has_value()) {
      return std::nullopt;
    }
    return comet::LoadRuntimeStatusJson(*runtime_status_path);
  }

  for (const auto& local_state : LoadAllLocalAppliedStates(state_root, node_name)) {
    const auto runtime_status_path = RuntimeStatusPathForNode(local_state, node_name);
    if (!runtime_status_path.has_value()) {
      continue;
    }
    const auto runtime_status = comet::LoadRuntimeStatusJson(*runtime_status_path);
    if (runtime_status.has_value()) {
      return runtime_status;
    }
  }
  return std::nullopt;
}

void SaveLocalAppliedState(
    const std::string& state_root,
    const std::string& node_name,
    const comet::DesiredState& state,
    const std::optional<std::string>& plane_name = std::nullopt) {
  const std::string effective_plane_name =
      plane_name.has_value() ? *plane_name : state.plane_name;
  const std::string path = plane_name.has_value()
                               ? LocalPlaneStatePath(state_root, node_name, effective_plane_name)
                               : LocalStatePath(state_root, node_name);
  comet::SaveDesiredStateJson(state, path);
}

void RemoveLocalAppliedPlaneState(
    const std::string& state_root,
    const std::string& node_name,
    const std::string& plane_name) {
  RemoveStateFileIfExists(LocalPlaneStatePath(state_root, node_name, plane_name));
  RemoveStateFileIfExists(LocalPlaneGenerationPath(state_root, node_name, plane_name));
  std::error_code error;
  std::filesystem::remove(
      std::filesystem::path(LocalPlaneRoot(state_root, node_name, plane_name)),
      error);
  if (error) {
    throw std::runtime_error(
        "failed to remove plane state root for plane '" + plane_name + "': " + error.message());
  }
}

void WaitForLocalRuntimeStatus(
    const std::string& state_root,
    const std::string& node_name,
    const std::optional<std::string>& plane_name,
    std::chrono::seconds timeout) {
  const auto deadline = std::chrono::steady_clock::now() + timeout;
  while (std::chrono::steady_clock::now() < deadline) {
    if (LoadLocalRuntimeStatus(state_root, node_name, plane_name).has_value()) {
      return;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(250));
  }
}

std::size_t ExpectedRuntimeStatusCountForNode(
    const comet::DesiredState& desired_node_state,
    const std::string& node_name) {
  std::size_t count = 0;
  for (const auto& instance : desired_node_state.instances) {
    if (instance.node_name == node_name) {
      ++count;
    }
  }
  return count;
}

void WaitForLocalInstanceRuntimeStatuses(
    const std::string& state_root,
    const std::string& node_name,
    const std::optional<std::string>& plane_name,
    std::size_t expected_count,
    std::chrono::seconds timeout) {
  if (expected_count == 0) {
    return;
  }
  const auto deadline = std::chrono::steady_clock::now() + timeout;
  while (std::chrono::steady_clock::now() < deadline) {
    const auto statuses = LoadLocalInstanceRuntimeStatuses(state_root, node_name, plane_name);
    if (statuses.size() >= expected_count) {
      return;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(250));
  }
}

void PrintLocalStateSummary(
    const comet::DesiredState& state,
    const std::string& state_path,
    const std::string& node_name,
    const std::optional<int>& generation) {
  std::cout << "hostd local state for node=" << node_name << "\n";
  std::cout << "state_path=" << state_path << "\n";
  if (generation.has_value()) {
    std::cout << "applied_generation=" << *generation << "\n";
  }
  std::cout << "plane=" << state.plane_name << "\n";
  std::cout << "disks=" << state.disks.size() << "\n";
  std::cout << "instances=" << state.instances.size() << "\n";
  for (const auto& disk : state.disks) {
    std::cout << "  - disk=" << disk.name
              << " kind=" << comet::ToString(disk.kind)
              << " host_path=" << disk.host_path
              << " realized=directory-backed"
              << " exists=" << (std::filesystem::exists(disk.host_path) ? "yes" : "no")
              << "\n";
  }
  for (const auto& instance : state.instances) {
    std::cout << "  - " << instance.name << " role=" << comet::ToString(instance.role)
              << " image=" << instance.image << "\n";
  }
}

std::string RequireSingleNodeName(const comet::DesiredState& state) {
  if (state.nodes.empty()) {
    throw std::runtime_error("desired node state is empty");
  }
  return state.nodes.front().name;
}

void PrintAssignmentSummary(const comet::HostAssignment& assignment) {
  std::cout << "assignment_id=" << assignment.id << "\n";
  std::cout << "assignment_node=" << assignment.node_name << "\n";
  std::cout << "assignment_plane=" << assignment.plane_name << "\n";
  std::cout << "assignment_generation=" << assignment.desired_generation << "\n";
  std::cout << "assignment_attempt=" << assignment.attempt_count
            << "/" << assignment.max_attempts << "\n";
  std::cout << "assignment_type=" << assignment.assignment_type << "\n";
  std::cout << "assignment_status=" << comet::ToString(assignment.status) << "\n";
  std::cout << "assignment_artifacts_root=" << assignment.artifacts_root << "\n";
}

std::string Trim(const std::string& value) {
  std::size_t start = 0;
  while (start < value.size() &&
         std::isspace(static_cast<unsigned char>(value[start])) != 0) {
    ++start;
  }
  std::size_t end = value.size();
  while (end > start &&
         std::isspace(static_cast<unsigned char>(value[end - 1])) != 0) {
    --end;
  }
  return value.substr(start, end - start);
}

std::vector<std::string> SplitCsvRow(const std::string& line) {
  std::vector<std::string> result;
  std::string current;
  bool in_quotes = false;
  for (char ch : line) {
    if (ch == '"') {
      in_quotes = !in_quotes;
      continue;
    }
    if (ch == ',' && !in_quotes) {
      result.push_back(Trim(current));
      current.clear();
      continue;
    }
    current.push_back(ch);
  }
  result.push_back(Trim(current));
  return result;
}

std::string RunCommandCapture(const std::string& command) {
  std::array<char, 512> buffer{};
  std::string output;
  FILE* pipe = popen(command.c_str(), "r");
  if (pipe == nullptr) {
    return output;
  }
  while (fgets(buffer.data(), static_cast<int>(buffer.size()), pipe) != nullptr) {
    output.append(buffer.data());
  }
  pclose(pipe);
  return output;
}

std::string ShellQuote(const std::string& value) {
  std::string quoted = "'";
  for (char ch : value) {
    if (ch == '\'') {
      quoted += "'\"'\"'";
    } else {
      quoted.push_back(ch);
    }
  }
  quoted += "'";
  return quoted;
}

bool RunCommandOk(const std::string& command);

bool HasSuffix(const std::string& value, const std::string& suffix) {
  return value.size() >= suffix.size() &&
         value.compare(value.size() - suffix.size(), suffix.size(), suffix) == 0;
}

std::string NormalizeLowercase(std::string value) {
  std::transform(value.begin(), value.end(), value.begin(), [](unsigned char ch) {
    return static_cast<char>(std::tolower(ch));
  });
  return value;
}

std::string CurrentHostPlatform() {
#if defined(_WIN32)
  return "windows";
#elif defined(__APPLE__)
  return "macos";
#elif defined(__linux__)
  return "linux";
#else
  return "unknown";
#endif
}

const comet::NodeInventory* FindNodeInventory(
    const comet::DesiredState& desired_node_state,
    const std::string& node_name) {
  for (const auto& node : desired_node_state.nodes) {
    if (node.name == node_name) {
      return &node;
    }
  }
  return nullptr;
}

bool NodeUsesGpuRuntime(
    const comet::DesiredState& desired_node_state,
    const std::string& node_name) {
  for (const auto& runtime_gpu_node : desired_node_state.runtime_gpu_nodes) {
    if (runtime_gpu_node.enabled && runtime_gpu_node.node_name == node_name) {
      return true;
    }
  }
  for (const auto& instance : desired_node_state.instances) {
    if (instance.node_name == node_name && instance.gpu_device.has_value() &&
        !instance.gpu_device->empty()) {
      return true;
    }
  }
  if (const auto* node = FindNodeInventory(desired_node_state, node_name);
      node != nullptr && !node->gpu_devices.empty()) {
    return true;
  }
  return false;
}

bool NodeUsesManagedRuntimeServices(
    const comet::DesiredState& desired_node_state,
    const std::string& node_name) {
  for (const auto& instance : desired_node_state.instances) {
    if (instance.node_name == node_name) {
      return true;
    }
  }
  return false;
}

void ValidateDesiredNodeStateForCurrentHost(
    const comet::DesiredState& desired_node_state,
    ComposeMode compose_mode) {
  if (compose_mode != ComposeMode::Exec) {
    return;
  }

  if (desired_node_state.nodes.empty()) {
    throw std::runtime_error("desired node state is empty");
  }
  const std::string node_name = desired_node_state.nodes.front().name;
  const std::string host_platform = CurrentHostPlatform();
  if (const auto* node = FindNodeInventory(desired_node_state, node_name);
      node != nullptr && !node->platform.empty() && node->platform != host_platform) {
    throw std::runtime_error(
        "node '" + node_name + "' targets platform '" + node->platform +
        "', but hostd is running on '" + host_platform + "'");
  }

  if (host_platform == "macos" &&
      NodeUsesManagedRuntimeServices(desired_node_state, node_name) &&
      NodeUsesGpuRuntime(desired_node_state, node_name)) {
    throw std::runtime_error(
        "node '" + node_name +
        "' requests Linux/NVIDIA GPU runtime, but hostd compose exec is unsupported on macOS");
  }
}

std::optional<std::filesystem::path> FindRepoRootFromPath(std::filesystem::path current) {
  while (!current.empty()) {
    if (std::filesystem::exists(current / "scripts" / "build-runtime-images.sh") &&
        std::filesystem::exists(current / "runtime" / "base" / "Dockerfile")) {
      return current;
    }
    const auto parent = current.parent_path();
    if (parent.empty() || parent == current) {
      break;
    }
    current = parent;
  }
  return std::nullopt;
}

std::optional<std::filesystem::path> DetectCometRepoRoot() {
  try {
    if (const auto from_cwd = FindRepoRootFromPath(std::filesystem::current_path());
        from_cwd.has_value()) {
      return from_cwd;
    }
  } catch (...) {
  }

  std::array<char, 4096> buffer{};
  const auto size = ::readlink("/proc/self/exe", buffer.data(), buffer.size() - 1);
  if (size <= 0) {
    return std::nullopt;
  }
  buffer[static_cast<std::size_t>(size)] = '\0';
  return FindRepoRootFromPath(std::filesystem::path(buffer.data()).parent_path());
}

bool LocalRuntimeBinaryExists(
    const std::filesystem::path& repo_root,
    const std::string& image) {
  if (image == "comet/infer-runtime:dev") {
    return std::filesystem::exists(repo_root / "build" / "linux" / "x64" / "comet-inferctl");
  }
  if (image == "comet/worker-runtime:dev") {
    return std::filesystem::exists(repo_root / "build" / "linux" / "x64" / "comet-workerd");
  }
  return true;
}

void EnsureLocalRuntimeBinary(
    const std::filesystem::path& repo_root,
    const std::string& image) {
  if (LocalRuntimeBinaryExists(repo_root, image)) {
    return;
  }

  const std::filesystem::path build_script = repo_root / "scripts" / "build-target.sh";
  if (!std::filesystem::exists(build_script)) {
    throw std::runtime_error(
        "runtime image requires local binary, but build-target.sh is unavailable");
  }

  const std::string command =
      "cd " + ShellQuote(repo_root.string()) +
      " && " + ShellQuote(build_script.string()) + " linux x64 Debug";
  if (!RunCommandOk(command)) {
    throw std::runtime_error(
        "failed to auto-build local comet binaries required for " + image);
  }

  if (!LocalRuntimeBinaryExists(repo_root, image)) {
    throw std::runtime_error(
        "local runtime binary is still missing after build for " + image);
  }
}

bool DockerImageExists(const std::string& image) {
  return RunCommandOk(
      ResolvedDockerCommand() + " image inspect " + ShellQuote(image) + " >/dev/null 2>&1");
}

void BuildCometRuntimeImage(
    const std::filesystem::path& repo_root,
    const std::string& image) {
  const std::string repo_root_quoted = ShellQuote(repo_root.string());
  const std::string docker = ResolvedDockerCommand();

  auto build_base = [&]() {
    const std::string command =
        docker + " build -f " + ShellQuote((repo_root / "runtime" / "base" / "Dockerfile").string()) +
        " -t " + ShellQuote("comet/base-runtime:dev") + " " + repo_root_quoted;
    if (!RunCommandOk(command)) {
      throw std::runtime_error("failed to auto-build comet/base-runtime:dev");
    }
  };

  auto build_runtime = [&](const std::string& dockerfile, const std::string& target_image) {
    const std::string command =
        docker + " build " +
        " -f " + ShellQuote((repo_root / dockerfile).string()) +
        " -t " + ShellQuote(target_image) + " " + repo_root_quoted;
    if (!RunCommandOk(command)) {
      throw std::runtime_error("failed to auto-build " + target_image);
    }
  };

  if (image == "comet/base-runtime:dev") {
    build_base();
    return;
  }
  if (image == "comet/infer-runtime:dev") {
    if (!DockerImageExists("comet/base-runtime:dev")) {
      build_base();
    }
    EnsureLocalRuntimeBinary(repo_root, image);
    build_runtime("runtime/infer/Dockerfile", image);
    return;
  }
  if (image == "comet/worker-runtime:dev") {
    if (!DockerImageExists("comet/base-runtime:dev")) {
      build_base();
    }
    EnsureLocalRuntimeBinary(repo_root, image);
    build_runtime("runtime/worker/Dockerfile", image);
    return;
  }
  if (image == "comet/web-ui:dev") {
    const std::string command =
        docker + " build -f " +
        ShellQuote((repo_root / "runtime" / "web-ui" / "Dockerfile").string()) +
        " -t " + ShellQuote(image) + " " + repo_root_quoted;
    if (!RunCommandOk(command)) {
      throw std::runtime_error("failed to auto-build " + image);
    }
    return;
  }

  throw std::runtime_error("unsupported auto-build image '" + image + "'");
}

void EnsureRuntimeImageAvailable(const std::string& image) {
  static std::set<std::string> ensured_images;
  if (image.empty() || ensured_images.count(image) > 0 || DockerImageExists(image)) {
    if (!image.empty()) {
      ensured_images.insert(image);
    }
    return;
  }

  if (image.rfind("comet/", 0) == 0 && HasSuffix(image, ":dev")) {
    if (const auto repo_root = DetectCometRepoRoot(); repo_root.has_value()) {
      BuildCometRuntimeImage(*repo_root, image);
      if (DockerImageExists(image)) {
        ensured_images.insert(image);
        return;
      }
    }
  }

  if (!RunCommandOk(ResolvedDockerCommand() + " pull " + ShellQuote(image))) {
    throw std::runtime_error(
        "required runtime image is unavailable locally and auto-build/pull failed: " + image);
  }
  if (!DockerImageExists(image)) {
    throw std::runtime_error("required runtime image is still unavailable after pull: " + image);
  }
  ensured_images.insert(image);
}

void EnsureComposeImagesAvailable(const comet::NodeComposePlan& compose_plan, ComposeMode compose_mode) {
  if (compose_mode != ComposeMode::Exec) {
    return;
  }
  std::set<std::string> images;
  for (const auto& service : compose_plan.services) {
    if (!service.image.empty()) {
      images.insert(service.image);
    }
  }
  for (const auto& image : images) {
    EnsureRuntimeImageAvailable(image);
  }
}

json BuildAssignmentProgressPayload(
    const std::string& phase,
    const std::string& title,
    const std::string& detail,
    int percent,
    const std::string& plane_name,
    const std::string& node_name,
    const std::optional<std::uintmax_t>& bytes_done = std::nullopt,
    const std::optional<std::uintmax_t>& bytes_total = std::nullopt) {
  json payload{
      {"phase", phase},
      {"title", title},
      {"detail", detail},
      {"percent", std::max(0, std::min(100, percent))},
      {"plane_name", plane_name},
      {"node_name", node_name},
  };
  if (bytes_done.has_value()) {
    payload["bytes_done"] = *bytes_done;
  }
  if (bytes_total.has_value()) {
    payload["bytes_total"] = *bytes_total;
  }
  return payload;
}

void PublishAssignmentProgress(
    HostdBackend* backend,
    const std::optional<int>& assignment_id,
    const json& progress);

const comet::DiskSpec& RequirePlaneSharedDiskForNode(
    const comet::DesiredState& state,
    const std::string& node_name) {
  for (const auto& disk : state.disks) {
    if (disk.node_name == node_name && disk.kind == comet::DiskKind::PlaneShared) {
      return disk;
    }
  }
  throw std::runtime_error(
      "plane '" + state.plane_name + "' is missing a plane-shared disk for node '" + node_name + "'");
}

std::string SharedDiskHostPathForContainerPath(
    const comet::DiskSpec& shared_disk,
    const std::string& container_path,
    const std::string& fallback_relative_path) {
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
    } else if (requested_text.size() > shared_text.size() &&
               requested_text.compare(0, shared_text.size(), shared_text) == 0 &&
               requested_text[shared_text.size()] == '/') {
      relative_path = requested_path.lexically_relative(shared_container_path);
    }
  }
  return (std::filesystem::path(shared_disk.host_path) / relative_path).string();
}

std::string FilenameFromUrl(const std::string& source_url) {
  const auto query = source_url.find_first_of("?#");
  const std::string without_query =
      query == std::string::npos ? source_url : source_url.substr(0, query);
  const std::string filename = std::filesystem::path(without_query).filename().string();
  if (filename.empty()) {
    throw std::runtime_error("failed to infer filename from bootstrap model URL: " + source_url);
  }
  return filename;
}

struct BootstrapModelArtifact {
  std::optional<std::string> local_path;
  std::optional<std::string> source_url;
  std::string target_host_path;
};

std::vector<BootstrapModelArtifact> BuildBootstrapModelArtifacts(
    const comet::DesiredState& state,
    const std::string& node_name) {
  const auto& shared_disk = RequirePlaneSharedDiskForNode(state, node_name);
  const bool use_vllm_runtime = state.inference.runtime_engine == "vllm";
  const std::filesystem::path target_root =
      SharedDiskHostPathForContainerPath(
          shared_disk,
          use_vllm_runtime ? state.inference.model_cache_dir : state.inference.gguf_cache_dir,
          use_vllm_runtime ? "models/cache" : "models/gguf");
  std::vector<BootstrapModelArtifact> artifacts;
  std::string filename = use_vllm_runtime ? "model" : "model.gguf";
  if (!state.bootstrap_model.has_value()) {
    artifacts.push_back(BootstrapModelArtifact{
        std::nullopt,
        std::nullopt,
        (target_root / filename).string(),
    });
    return artifacts;
  }

  const auto& bootstrap_model = *state.bootstrap_model;
  if (!bootstrap_model.source_urls.empty()) {
    artifacts.reserve(bootstrap_model.source_urls.size());
    for (const auto& source_url : bootstrap_model.source_urls) {
      artifacts.push_back(BootstrapModelArtifact{
          std::nullopt,
          source_url,
          (target_root / FilenameFromUrl(source_url)).string(),
      });
    }
    return artifacts;
  }

  if (bootstrap_model.target_filename.has_value() && !bootstrap_model.target_filename->empty()) {
    filename = *bootstrap_model.target_filename;
  } else if (bootstrap_model.local_path.has_value() && !bootstrap_model.local_path->empty()) {
    filename = std::filesystem::path(*bootstrap_model.local_path).filename().string();
  } else if (bootstrap_model.source_url.has_value() && !bootstrap_model.source_url->empty()) {
    filename = FilenameFromUrl(*bootstrap_model.source_url);
  }
  if (filename.empty()) {
    filename = use_vllm_runtime ? "model" : "model.gguf";
  }
  artifacts.push_back(BootstrapModelArtifact{
      bootstrap_model.local_path,
      bootstrap_model.source_url,
      (target_root / filename).string(),
  });
  return artifacts;
}

std::string BootstrapModelTargetPath(
    const comet::DesiredState& state,
    const std::string& node_name) {
  const auto artifacts = BuildBootstrapModelArtifacts(state, node_name);
  if (artifacts.empty()) {
    throw std::runtime_error(
        "failed to resolve bootstrap model target path for plane '" + state.plane_name + "'");
  }
  return artifacts.front().target_host_path;
}

std::string ActiveModelPathForNode(
    const comet::DesiredState& state,
    const std::string& node_name) {
  const auto active_model_path = ControlFilePathForNode(state, node_name, "active-model.json");
  if (!active_model_path.has_value()) {
    throw std::runtime_error(
        "plane '" + state.plane_name + "' is missing infer control path for node '" + node_name + "'");
  }
  return *active_model_path;
}

std::string BootstrapRuntimeModelPath(
    const comet::DesiredState& state,
    const std::string& target_host_path) {
  const auto& shared_disk = RequirePlaneSharedDiskForNode(state, RequireSingleNodeName(state));
  const std::filesystem::path target_path(target_host_path);
  const std::filesystem::path shared_root(shared_disk.host_path);
  std::string relative = target_path.lexically_relative(shared_root).generic_string();
  if (relative.empty() || relative == ".") {
    relative = target_path.filename().string();
  }
  return (std::filesystem::path(shared_disk.container_path) / relative).generic_string();
}

std::optional<std::uintmax_t> FileSizeIfExists(const std::string& path) {
  std::error_code error;
  if (!std::filesystem::exists(path, error) || error) {
    return std::nullopt;
  }
  if (std::filesystem::is_directory(path, error)) {
    if (error) {
      return std::nullopt;
    }
    std::uintmax_t total = 0;
    for (const auto& entry : std::filesystem::recursive_directory_iterator(path, error)) {
      if (error) {
        return std::nullopt;
      }
      if (!entry.is_regular_file(error)) {
        if (error) {
          return std::nullopt;
        }
        continue;
      }
      total += entry.file_size(error);
      if (error) {
        return std::nullopt;
      }
    }
    return total;
  }
  const auto size = std::filesystem::file_size(path, error);
  if (error) {
    return std::nullopt;
  }
  return size;
}

std::string ComputeFileSha256Hex(const std::string& path) {
  comet::InitializeCrypto();
  std::ifstream input(path, std::ios::binary);
  if (!input.is_open()) {
    throw std::runtime_error("failed to open file for sha256: " + path);
  }
  crypto_hash_sha256_state context;
  crypto_hash_sha256_init(&context);
  std::array<char, 1024 * 1024> buffer{};
  while (input.good()) {
    input.read(buffer.data(), static_cast<std::streamsize>(buffer.size()));
    const auto count = input.gcount();
    if (count > 0) {
      crypto_hash_sha256_update(
          &context,
          reinterpret_cast<const unsigned char*>(buffer.data()),
          static_cast<unsigned long long>(count));
    }
  }
  std::array<unsigned char, crypto_hash_sha256_BYTES> digest{};
  crypto_hash_sha256_final(&context, digest.data());
  std::ostringstream out;
  out << std::hex << std::setfill('0');
  for (unsigned char byte : digest) {
    out << std::setw(2) << static_cast<int>(byte);
  }
  return out.str();
}

std::optional<std::uintmax_t> ProbeContentLength(const std::string& source_url) {
  const std::string output = RunCommandCapture(
      "/usr/bin/curl -fsSLI " + ShellQuote(source_url) + " 2>/dev/null || true");
  std::optional<std::uintmax_t> content_length;
  std::istringstream input(output);
  std::string line;
  while (std::getline(input, line)) {
    const std::string trimmed = Trim(line);
    if (trimmed.empty()) {
      continue;
    }
    const auto colon = trimmed.find(':');
    if (colon == std::string::npos) {
      continue;
    }
    const std::string key = NormalizeLowercase(Trim(trimmed.substr(0, colon)));
    if (key != "content-length") {
      continue;
    }
    try {
      content_length = static_cast<std::uintmax_t>(std::stoull(Trim(trimmed.substr(colon + 1))));
    } catch (...) {
      content_length = std::nullopt;
    }
  }
  return content_length;
}

void CopyFileWithProgress(
    const std::string& source_path,
    const std::string& target_path,
    HostdBackend* backend,
    const std::optional<int>& assignment_id,
    const std::string& plane_name,
    const std::string& node_name,
    std::size_t part_index = 0,
    std::size_t part_count = 1,
    std::uintmax_t aggregate_prefix = 0,
    const std::optional<std::uintmax_t>& aggregate_total = std::nullopt) {
  if (std::filesystem::is_directory(source_path)) {
    const auto total_size = FileSizeIfExists(source_path);
    const std::filesystem::path source_root(source_path);
    const std::filesystem::path target_root(target_path);
    const std::filesystem::path temp_root = target_root.string() + ".partdir";
    std::filesystem::remove_all(temp_root);
    std::filesystem::create_directories(temp_root);
    std::uintmax_t copied = 0;
    for (const auto& entry : std::filesystem::recursive_directory_iterator(source_root)) {
      const auto relative = entry.path().lexically_relative(source_root);
      const auto temp_target = temp_root / relative;
      if (entry.is_directory()) {
        std::filesystem::create_directories(temp_target);
        continue;
      }
      if (!entry.is_regular_file()) {
        continue;
      }
      EnsureParentDirectory(temp_target.string());
      std::ifstream input(entry.path(), std::ios::binary);
      if (!input.is_open()) {
        throw std::runtime_error(
            "failed to open bootstrap model source file: " + entry.path().string());
      }
      std::ofstream output(temp_target, std::ios::binary | std::ios::trunc);
      if (!output.is_open()) {
        throw std::runtime_error(
            "failed to open bootstrap model target file: " + temp_target.string());
      }
      std::array<char, 1024 * 1024> buffer{};
      while (input.good()) {
        input.read(buffer.data(), static_cast<std::streamsize>(buffer.size()));
        const auto count = input.gcount();
        if (count <= 0) {
          break;
        }
        output.write(buffer.data(), count);
        copied += static_cast<std::uintmax_t>(count);
        const std::uintmax_t overall_done = aggregate_prefix + copied;
        int percent = 40;
        if (aggregate_total.has_value() && *aggregate_total > 0) {
          percent =
              20 + static_cast<int>((static_cast<double>(overall_done) / *aggregate_total) * 40.0);
        } else if (total_size.has_value() && *total_size > 0) {
          percent = 20 + static_cast<int>((static_cast<double>(copied) / *total_size) * 40.0);
        }
        PublishAssignmentProgress(
            backend,
            assignment_id,
            BuildAssignmentProgressPayload(
                "acquiring-model",
                "Acquiring model",
                part_count > 1
                    ? ("Copying bootstrap model part " + std::to_string(part_index + 1) + "/" +
                       std::to_string(part_count) + " into the plane shared disk.")
                    : "Copying bootstrap model into the plane shared disk.",
                percent,
                plane_name,
                node_name,
                aggregate_total.has_value() ? std::optional<std::uintmax_t>(overall_done)
                                            : std::optional<std::uintmax_t>(copied),
                aggregate_total.has_value() ? aggregate_total : total_size));
      }
      output.close();
      if (!output.good()) {
        throw std::runtime_error(
            "failed to write bootstrap model target file: " + temp_target.string());
      }
    }
    std::filesystem::remove_all(target_root);
    EnsureParentDirectory(target_root.string());
    std::filesystem::rename(temp_root, target_root);
    return;
  }

  const auto total_size = FileSizeIfExists(source_path);
  EnsureParentDirectory(target_path);
  const std::string temp_path = target_path + ".part";
  std::ifstream input(source_path, std::ios::binary);
  if (!input.is_open()) {
    throw std::runtime_error("failed to open bootstrap model source: " + source_path);
  }
  std::ofstream output(temp_path, std::ios::binary | std::ios::trunc);
  if (!output.is_open()) {
    throw std::runtime_error("failed to open bootstrap model target: " + temp_path);
  }
  std::array<char, 1024 * 1024> buffer{};
  std::uintmax_t copied = 0;
  while (input.good()) {
    input.read(buffer.data(), static_cast<std::streamsize>(buffer.size()));
    const auto count = input.gcount();
    if (count <= 0) {
      break;
    }
    output.write(buffer.data(), count);
    copied += static_cast<std::uintmax_t>(count);
    const std::uintmax_t overall_done = aggregate_prefix + copied;
    int percent = 40;
    if (aggregate_total.has_value() && *aggregate_total > 0) {
      percent = 20 + static_cast<int>((static_cast<double>(overall_done) / *aggregate_total) * 40.0);
    } else if (total_size.has_value() && *total_size > 0) {
      percent = 20 + static_cast<int>((static_cast<double>(copied) / *total_size) * 40.0);
    }
    PublishAssignmentProgress(
        backend,
        assignment_id,
        BuildAssignmentProgressPayload(
            "acquiring-model",
            "Acquiring model",
            part_count > 1
                ? ("Copying bootstrap model part " + std::to_string(part_index + 1) + "/" +
                   std::to_string(part_count) + " into the plane shared disk.")
                : "Copying bootstrap model into the plane shared disk.",
            percent,
            plane_name,
            node_name,
            aggregate_total.has_value() ? std::optional<std::uintmax_t>(overall_done)
                                        : std::optional<std::uintmax_t>(copied),
            aggregate_total.has_value() ? aggregate_total : total_size));
  }
  output.close();
  if (!output.good()) {
    throw std::runtime_error("failed to write bootstrap model target: " + temp_path);
  }
  std::filesystem::rename(temp_path, target_path);
}

void DownloadFileWithProgress(
    const std::string& source_url,
    const std::string& target_path,
    HostdBackend* backend,
    const std::optional<int>& assignment_id,
    const std::string& plane_name,
    const std::string& node_name,
    std::size_t part_index = 0,
    std::size_t part_count = 1,
    std::uintmax_t aggregate_prefix = 0,
    const std::optional<std::uintmax_t>& aggregate_total = std::nullopt) {
  EnsureParentDirectory(target_path);
  const std::string temp_path = target_path + ".part";
  std::filesystem::remove(temp_path);
  const auto content_length = ProbeContentLength(source_url);
  auto future = std::async(
      std::launch::async,
      [command = "/usr/bin/curl -fL --silent --show-error --output " + ShellQuote(temp_path) +
                     " " + ShellQuote(source_url)]() {
        return std::system(command.c_str());
      });
  while (future.wait_for(std::chrono::milliseconds(500)) != std::future_status::ready) {
    const auto bytes_done = FileSizeIfExists(temp_path).value_or(0);
    const std::uintmax_t overall_done = aggregate_prefix + bytes_done;
    int percent = 40;
    if (aggregate_total.has_value() && *aggregate_total > 0) {
      percent = 20 + static_cast<int>((static_cast<double>(overall_done) / *aggregate_total) * 40.0);
    } else if (content_length.has_value() && *content_length > 0) {
      percent = 20 + static_cast<int>((static_cast<double>(bytes_done) / *content_length) * 40.0);
    }
    PublishAssignmentProgress(
        backend,
        assignment_id,
        BuildAssignmentProgressPayload(
            "acquiring-model",
            "Acquiring model",
            part_count > 1
                ? ("Downloading bootstrap model part " + std::to_string(part_index + 1) + "/" +
                   std::to_string(part_count) + " into the plane shared disk.")
                : "Downloading bootstrap model into the plane shared disk.",
            percent,
            plane_name,
            node_name,
            aggregate_total.has_value() ? std::optional<std::uintmax_t>(overall_done)
                                        : std::optional<std::uintmax_t>(bytes_done),
            aggregate_total.has_value() ? aggregate_total : content_length));
  }
  const int rc = future.get();
  if (rc != 0) {
    throw std::runtime_error("failed to download bootstrap model from " + source_url);
  }
  std::filesystem::rename(temp_path, target_path);
}

void WriteBootstrapActiveModel(
    const comet::DesiredState& state,
    const std::string& node_name,
    const std::string& target_host_path) {
  const auto& bootstrap_model = *state.bootstrap_model;
  const std::string runtime_model_path = BootstrapRuntimeModelPath(state, target_host_path);
  WriteTextFile(
      ActiveModelPathForNode(state, node_name),
      json{
          {"version", 1},
          {"plane_name", state.plane_name},
          {"model_id", bootstrap_model.model_id},
          {"served_model_name",
           bootstrap_model.served_model_name.has_value()
               ? *bootstrap_model.served_model_name
               : bootstrap_model.model_id},
          {"cached_local_model_path", target_host_path},
          {"cached_runtime_model_path", runtime_model_path},
          {"runtime_model_path", runtime_model_path},
      }
          .dump(2));
}

void BootstrapPlaneModelIfNeeded(
    const comet::DesiredState& state,
    const std::string& node_name,
    HostdBackend* backend,
    const std::optional<int>& assignment_id) {
  if (state.instances.empty()) {
    return;
  }

  const auto& shared_disk = RequirePlaneSharedDiskForNode(state, node_name);
  if (!std::filesystem::exists(shared_disk.host_path)) {
    throw std::runtime_error(
        "plane shared disk path does not exist after ensure-disk: " + shared_disk.host_path);
  }

  PublishAssignmentProgress(
      backend,
      assignment_id,
      BuildAssignmentProgressPayload(
          "ensuring-shared-disk",
          "Ensuring shared disk",
          "Plane shared disk is mounted and ready for model/bootstrap data.",
          12,
          state.plane_name,
          node_name));

  const std::string active_model_path = ActiveModelPathForNode(state, node_name);
  if (!state.bootstrap_model.has_value()) {
    RemoveFileIfExists(active_model_path);
    return;
  }

  const auto& bootstrap_model = *state.bootstrap_model;
  const std::string target_path = BootstrapModelTargetPath(state, node_name);
  const auto artifacts = BuildBootstrapModelArtifacts(state, node_name);
  bool already_present = !artifacts.empty();
  std::optional<std::uintmax_t> aggregate_total = std::uintmax_t{0};
  for (const auto& artifact : artifacts) {
    if (!std::filesystem::exists(artifact.target_host_path)) {
      already_present = false;
    }
    std::optional<std::uintmax_t> expected_size;
    if (artifact.local_path.has_value() && !artifact.local_path->empty()) {
      expected_size = FileSizeIfExists(*artifact.local_path);
      const auto target_size = FileSizeIfExists(artifact.target_host_path);
      if (!expected_size.has_value() || !target_size.has_value() || *expected_size != *target_size) {
        already_present = false;
      }
    } else if (artifact.source_url.has_value() && !artifact.source_url->empty()) {
      expected_size = ProbeContentLength(*artifact.source_url);
      const auto target_size = FileSizeIfExists(artifact.target_host_path);
      if (!expected_size.has_value() || !target_size.has_value() || *expected_size != *target_size) {
        already_present = false;
      }
    } else if (!std::filesystem::exists(artifact.target_host_path)) {
      already_present = false;
    }
    if (!expected_size.has_value()) {
      aggregate_total = std::nullopt;
    } else if (aggregate_total.has_value()) {
      *aggregate_total += *expected_size;
    }
  }
  if (already_present && bootstrap_model.sha256.has_value() && artifacts.size() == 1) {
    if (std::filesystem::is_directory(target_path)) {
      throw std::runtime_error(
          "bootstrap_model.sha256 is not supported for directory-based bootstrap models");
    }
    PublishAssignmentProgress(
        backend,
        assignment_id,
        BuildAssignmentProgressPayload(
            "verifying-model",
            "Verifying model",
            "Checking the existing shared-disk model checksum.",
            68,
            state.plane_name,
            node_name));
    already_present = NormalizeLowercase(ComputeFileSha256Hex(target_path)) ==
                      NormalizeLowercase(*bootstrap_model.sha256);
  } else if (bootstrap_model.sha256.has_value() && artifacts.size() > 1) {
    throw std::runtime_error(
        "bootstrap_model.sha256 is not supported with multipart bootstrap_model.source_urls");
  }

  if (!already_present) {
    std::uintmax_t aggregate_prefix = 0;
    for (std::size_t index = 0; index < artifacts.size(); ++index) {
      const auto& artifact = artifacts[index];
      bool artifact_present = std::filesystem::exists(artifact.target_host_path);
      std::optional<std::uintmax_t> artifact_size = FileSizeIfExists(artifact.target_host_path);
      if (artifact.local_path.has_value() && !artifact.local_path->empty()) {
        const auto source_size = FileSizeIfExists(*artifact.local_path);
        artifact_present = artifact_present && source_size.has_value() && artifact_size.has_value() &&
                           *source_size == *artifact_size;
        if (!artifact_present) {
          CopyFileWithProgress(
              *artifact.local_path,
              artifact.target_host_path,
              backend,
              assignment_id,
              state.plane_name,
              node_name,
              index,
              artifacts.size(),
              aggregate_prefix,
              aggregate_total);
          artifact_size = FileSizeIfExists(artifact.target_host_path);
        }
      } else if (artifact.source_url.has_value() && !artifact.source_url->empty()) {
        const auto remote_size = ProbeContentLength(*artifact.source_url);
        artifact_present = artifact_present && remote_size.has_value() && artifact_size.has_value() &&
                           *remote_size == *artifact_size;
        if (!artifact_present) {
          DownloadFileWithProgress(
              *artifact.source_url,
              artifact.target_host_path,
              backend,
              assignment_id,
              state.plane_name,
              node_name,
              index,
              artifacts.size(),
              aggregate_prefix,
              aggregate_total);
          artifact_size = FileSizeIfExists(artifact.target_host_path);
        }
      }
      if (artifact_size.has_value()) {
        aggregate_prefix += *artifact_size;
      }
    }
  }

  if (bootstrap_model.sha256.has_value() && artifacts.size() == 1) {
    if (std::filesystem::is_directory(target_path)) {
      throw std::runtime_error(
          "bootstrap_model.sha256 is not supported for directory-based bootstrap models");
    }
    PublishAssignmentProgress(
        backend,
        assignment_id,
        BuildAssignmentProgressPayload(
            "verifying-model",
            "Verifying model",
            "Verifying the model checksum in the shared disk.",
            72,
            state.plane_name,
            node_name));
    if (NormalizeLowercase(ComputeFileSha256Hex(target_path)) !=
        NormalizeLowercase(*bootstrap_model.sha256)) {
      throw std::runtime_error(
          "bootstrap model checksum mismatch for " + target_path);
    }
  }

  const bool has_source =
      (bootstrap_model.local_path.has_value() && !bootstrap_model.local_path->empty()) ||
      (bootstrap_model.source_url.has_value() && !bootstrap_model.source_url->empty()) ||
      !bootstrap_model.source_urls.empty();
  if (!has_source && !std::filesystem::exists(target_path)) {
    RemoveFileIfExists(active_model_path);
    return;
  }

  PublishAssignmentProgress(
      backend,
      assignment_id,
      BuildAssignmentProgressPayload(
          "activating-model",
          "Activating model",
          "Writing active-model.json for infer and worker runtime.",
          80,
          state.plane_name,
          node_name));
  WriteBootstrapActiveModel(state, node_name, target_path);
}

bool RunCommandOk(const std::string& command) {
  return std::system(command.c_str()) == 0;
}

bool HostCanManageRealDisks() {
  return geteuid() == 0;
}

std::optional<std::string> DetectExistingLoopDevice(const std::string& image_path) {
  const std::string output =
      RunCommandCapture("/usr/sbin/losetup -j " + ShellQuote(image_path) + " 2>/dev/null || true");
  const std::string trimmed = Trim(output);
  if (trimmed.empty()) {
    return std::nullopt;
  }
  const auto colon = trimmed.find(':');
  if (colon == std::string::npos) {
    return std::nullopt;
  }
  return trimmed.substr(0, colon);
}

std::string RequireLoopDeviceForImage(const std::string& image_path) {
  if (const auto existing = DetectExistingLoopDevice(image_path); existing.has_value()) {
    return *existing;
  }
  const std::string output =
      RunCommandCapture(
          "/usr/sbin/losetup --find --show " + ShellQuote(image_path) + " 2>/dev/null");
  const std::string loop_device = Trim(output);
  if (loop_device.empty()) {
    throw std::runtime_error("failed to attach loop device for image '" + image_path + "'");
  }
  return loop_device;
}

std::string DetectFilesystemTypeForDevice(const std::string& device_path) {
  return Trim(
      RunCommandCapture(
          "/usr/sbin/blkid -o value -s TYPE " + ShellQuote(device_path) + " 2>/dev/null || true"));
}

bool IsPathMounted(const std::string& mount_point) {
  return RunCommandOk(
      "/usr/bin/mountpoint -q " + ShellQuote(mount_point) + " >/dev/null 2>&1");
}

std::optional<std::string> CurrentMountSource(const std::string& mount_point) {
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
    if (target == mount_point) {
      return source;
    }
  }
  return std::nullopt;
}

void CreateSparseImageFile(const std::string& image_path, int size_gb) {
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

comet::DiskRuntimeState EnsureRealDiskMount(
    const comet::DiskSpec& disk,
    const std::string& storage_root,
    const std::optional<std::string>& runtime_root) {
  comet::DiskRuntimeState runtime_state;
  runtime_state.disk_name = disk.name;
  runtime_state.plane_name = disk.plane_name;
  runtime_state.node_name = disk.node_name;
  runtime_state.image_path = ManagedDiskImagePath(disk, storage_root, runtime_root);
  runtime_state.mount_point = disk.host_path;

  CreateSparseImageFile(runtime_state.image_path, disk.size_gb);
  runtime_state.runtime_state = "image-created";

  runtime_state.loop_device = RequireLoopDeviceForImage(runtime_state.image_path);
  runtime_state.attached_at = "attached";
  runtime_state.runtime_state = "attached";

  runtime_state.filesystem_type = DetectFilesystemTypeForDevice(runtime_state.loop_device);
  if (runtime_state.filesystem_type.empty()) {
    if (!RunCommandOk(
            "/usr/sbin/mkfs.ext4 -F " + ShellQuote(runtime_state.loop_device) +
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
        "/usr/bin/mount " + ShellQuote(runtime_state.loop_device) + " " +
        ShellQuote(runtime_state.mount_point) + " >/dev/null 2>&1";
    bool mounted = false;
    for (int attempt = 0; attempt < 5; ++attempt) {
      if (RunCommandOk(mount_command)) {
        mounted = true;
        break;
      }
      const auto current_source = CurrentMountSource(runtime_state.mount_point);
      if (IsPathMounted(runtime_state.mount_point) &&
          current_source.has_value() &&
          *current_source == runtime_state.loop_device) {
        // Some WSL/DrvFs-backed mount paths can report a non-zero exit code even
        // when the loop device is already mounted as requested. Trust the
        // actual mount table in that case.
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

  WriteTextFile(
      (std::filesystem::path(runtime_state.mount_point) / ".comet-disk-info").string(),
      "disk=" + disk.name + "@" + disk.node_name + "\nmanaged_by=comet-hostd\nrealized=mounted\n");
  runtime_state.mounted_at = "mounted";
  runtime_state.runtime_state = "mounted";
  runtime_state.status_message = "real mounted disk lifecycle applied by hostd";
  return runtime_state;
}

comet::DiskRuntimeState InspectRealDiskRuntime(
    const comet::DiskSpec& disk,
    const std::string& storage_root,
    const std::optional<std::string>& runtime_root) {
  comet::DiskRuntimeState runtime_state;
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

  if (mounted && (!loop_device.has_value() || !mount_source.has_value() || *mount_source != *loop_device)) {
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

std::string ResolvedDockerCommand() {
  static const std::string resolved = []() -> std::string {
    if (std::system("docker version >/dev/null 2>&1") == 0) {
      return "docker";
    }
    const std::string windows_docker =
        "/mnt/c/Program Files/Docker/Docker/resources/bin/docker.exe";
    if (std::filesystem::exists(windows_docker) &&
        std::system(("'" + windows_docker + "' version >/dev/null 2>&1").c_str()) == 0) {
      return "'" + windows_docker + "'";
    }
    return "docker";
  }();
  return resolved;
}

std::string CurrentTimestampString() {
  const std::time_t now = std::time(nullptr);
  std::tm tm{};
#if defined(_WIN32)
  localtime_s(&tm, &now);
#else
  localtime_r(&now, &tm);
#endif
  char buffer[32];
  if (std::strftime(buffer, sizeof(buffer), "%Y-%m-%d %H:%M:%S", &tm) == 0) {
    return {};
  }
  return buffer;
}

std::optional<std::tm> ParseDisplayTimestamp(const std::string& value) {
  if (value.empty()) {
    return std::nullopt;
  }
  for (const char* format : {"%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%dT%H:%M:%S"}) {
    std::tm tm{};
    std::istringstream input(value);
    input >> std::get_time(&tm, format);
    if (!input.fail()) {
      return tm;
    }
  }
  return std::nullopt;
}

std::string FormatDisplayTimestamp(const std::string& value) {
  const auto parsed = ParseDisplayTimestamp(value);
  if (!parsed.has_value()) {
    return value.empty() ? "(empty)" : value;
  }
  std::ostringstream output;
  output << std::put_time(&*parsed, "%d/%m/%Y %H:%M:%S");
  return output.str();
}

std::string SerializeEventPayload(const json& payload) {
  return payload.dump();
}

class HostdBackend {
 public:
  virtual ~HostdBackend() = default;

  virtual std::optional<comet::HostAssignment> ClaimNextHostAssignment(
      const std::string& node_name) = 0;
  virtual bool TransitionClaimedHostAssignment(
      int assignment_id,
      comet::HostAssignmentStatus status,
      const std::string& status_message) = 0;
  virtual bool UpdateHostAssignmentProgress(
      int assignment_id,
      const json& progress) = 0;
  virtual void UpsertHostObservation(const comet::HostObservation& observation) = 0;
  virtual void AppendEvent(const comet::EventRecord& event) = 0;
  virtual void UpsertDiskRuntimeState(const comet::DiskRuntimeState& state) = 0;
  virtual std::optional<comet::DiskRuntimeState> LoadDiskRuntimeState(
      const std::string& disk_name,
      const std::string& node_name) = 0;
};

struct HttpResponse {
  int status_code = 200;
  std::string content_type = "application/json";
  std::string body;
};

struct ControllerEndpointTarget {
  std::string raw;
  std::string host;
  int port = 18080;
  std::string base_path;
};

std::string Trim(const std::string& text);

std::string Lowercase(std::string value) {
  std::transform(value.begin(), value.end(), value.begin(), [](unsigned char ch) {
    return static_cast<char>(std::tolower(ch));
  });
  return value;
}

std::string UrlEncode(const std::string& value) {
  std::ostringstream out;
  out.fill('0');
  out << std::hex << std::uppercase;
  for (const unsigned char ch : value) {
    if (std::isalnum(ch) || ch == '-' || ch == '_' || ch == '.' || ch == '~') {
      out << static_cast<char>(ch);
    } else {
      out << '%' << std::setw(2) << static_cast<int>(ch);
    }
  }
  return out.str();
}

ControllerEndpointTarget ParseControllerTarget(const std::string& raw_target) {
  std::string target = Trim(raw_target);
  if (target.empty()) {
    throw std::runtime_error("empty controller target");
  }

  ControllerEndpointTarget parsed;
  parsed.raw = target;
  if (target.rfind("http://", 0) == 0) {
    target = target.substr(7);
  } else if (target.rfind("https://", 0) == 0) {
    throw std::runtime_error("https controller targets are not supported yet");
  }

  const std::size_t slash = target.find('/');
  if (slash != std::string::npos) {
    parsed.base_path = target.substr(slash);
    target = target.substr(0, slash);
    if (parsed.base_path == "/") {
      parsed.base_path.clear();
    }
  }

  const std::size_t colon = target.rfind(':');
  if (colon != std::string::npos) {
    parsed.host = target.substr(0, colon);
    parsed.port = std::stoi(target.substr(colon + 1));
  } else {
    parsed.host = target;
  }
  if (parsed.host.empty()) {
    throw std::runtime_error("invalid controller target '" + raw_target + "'");
  }
  return parsed;
}

HttpResponse ParseHttpResponse(const std::string& response_text) {
  HttpResponse response;
  const std::size_t headers_end = response_text.find("\r\n\r\n");
  const std::string header_text =
      headers_end == std::string::npos ? response_text : response_text.substr(0, headers_end);
  response.body =
      headers_end == std::string::npos ? std::string{} : response_text.substr(headers_end + 4);

  const std::size_t line_end = header_text.find("\r\n");
  const std::string first_line =
      line_end == std::string::npos ? header_text : header_text.substr(0, line_end);
  std::stringstream stream(first_line);
  std::string http_version;
  stream >> http_version >> response.status_code;

  std::size_t offset = line_end == std::string::npos ? header_text.size() : line_end + 2;
  while (offset < header_text.size()) {
    const std::size_t next = header_text.find("\r\n", offset);
    const std::string line = header_text.substr(
        offset,
        next == std::string::npos ? std::string::npos : next - offset);
    const std::size_t colon = line.find(':');
    if (colon != std::string::npos) {
      const std::string key = Lowercase(Trim(line.substr(0, colon)));
      const std::string value = Trim(line.substr(colon + 1));
      if (key == "content-type") {
        response.content_type = value;
      }
    }
    if (next == std::string::npos) {
      break;
    }
    offset = next + 2;
  }
  return response;
}

HttpResponse SendControllerHttpRequest(
    const ControllerEndpointTarget& target,
    const std::string& method,
    const std::string& path_and_query,
    const std::string& body = "",
    const std::map<std::string, std::string>& headers = {}) {
  addrinfo hints{};
  hints.ai_family = AF_UNSPEC;
  hints.ai_socktype = SOCK_STREAM;
  addrinfo* results = nullptr;
  const std::string port_text = std::to_string(target.port);
  const int lookup = getaddrinfo(target.host.c_str(), port_text.c_str(), &hints, &results);
  if (lookup != 0) {
    throw std::runtime_error(
        "failed to resolve controller target '" + target.raw + "': " + gai_strerror(lookup));
  }

  int fd = -1;
  for (addrinfo* candidate = results; candidate != nullptr; candidate = candidate->ai_next) {
    fd = socket(candidate->ai_family, candidate->ai_socktype, candidate->ai_protocol);
    if (fd < 0) {
      continue;
    }
    if (connect(fd, candidate->ai_addr, candidate->ai_addrlen) == 0) {
      break;
    }
    close(fd);
    fd = -1;
  }
  freeaddrinfo(results);
  if (fd < 0) {
    throw std::runtime_error("failed to connect to controller target '" + target.raw + "'");
  }

  const std::string request_path = target.base_path + path_and_query;
  std::ostringstream request;
  request << method << " " << request_path << " HTTP/1.1\r\n";
  request << "Host: " << target.host << ":" << target.port << "\r\n";
  request << "Connection: close\r\n";
  for (const auto& [key, value] : headers) {
    request << key << ": " << value << "\r\n";
  }
  if (!body.empty()) {
    request << "Content-Type: application/json\r\n";
    request << "Content-Length: " << body.size() << "\r\n";
  }
  request << "\r\n";
  request << body;

  const std::string request_text = request.str();
  const char* data = request_text.c_str();
  std::size_t remaining = request_text.size();
  while (remaining > 0) {
    const ssize_t written = send(fd, data, remaining, 0);
    if (written <= 0) {
      const std::string error = std::strerror(errno);
      close(fd);
      throw std::runtime_error("failed to write HTTP request: " + error);
    }
    data += written;
    remaining -= static_cast<std::size_t>(written);
  }

  std::string response_text;
  std::array<char, 8192> buffer{};
  while (true) {
    const ssize_t read_count = recv(fd, buffer.data(), buffer.size(), 0);
    if (read_count < 0) {
      const std::string error = std::strerror(errno);
      close(fd);
      throw std::runtime_error("failed to read HTTP response: " + error);
    }
    if (read_count == 0) {
      break;
    }
    response_text.append(buffer.data(), static_cast<std::size_t>(read_count));
  }
  close(fd);
  return ParseHttpResponse(response_text);
}

json SendControllerJsonRequest(
    const ControllerEndpointTarget& target,
    const std::string& method,
    const std::string& path_and_query,
    const json& body = json(),
    const std::map<std::string, std::string>& headers = {}) {
  const HttpResponse response =
      SendControllerHttpRequest(
          target,
          method,
          path_and_query,
          body.is_null() ? "" : body.dump(),
          headers);
  const json payload = response.body.empty() ? json::object() : json::parse(response.body);
  if (response.status_code >= 400) {
    throw std::runtime_error(
        payload.contains("error") && payload["error"].is_object()
            ? payload["error"].value("message", "controller request failed")
            : "controller request failed with status " + std::to_string(response.status_code));
  }
  return payload;
}

comet::HostAssignment ParseAssignmentPayload(const json& payload) {
  comet::HostAssignment assignment;
  assignment.id = payload.value("id", 0);
  assignment.node_name = payload.value("node_name", std::string{});
  assignment.plane_name = payload.value("plane_name", std::string{});
  assignment.desired_generation = payload.value("desired_generation", 0);
  assignment.attempt_count = payload.value("attempt_count", 0);
  assignment.max_attempts = payload.value("max_attempts", 3);
  assignment.assignment_type = payload.value("assignment_type", std::string{});
  assignment.desired_state_json = payload.value("desired_state_json", std::string{});
  assignment.artifacts_root = payload.value("artifacts_root", std::string{});
  assignment.status =
      comet::ParseHostAssignmentStatus(payload.value("status", std::string("pending")));
  assignment.status_message = payload.value("status_message", std::string{});
  if (payload.contains("progress") && !payload.at("progress").is_null()) {
    assignment.progress_json = payload.at("progress").dump();
  }
  return assignment;
}

json BuildHostObservationPayload(const comet::HostObservation& observation) {
  return json{
      {"node_name", observation.node_name},
      {"plane_name", observation.plane_name},
      {"applied_generation",
       observation.applied_generation.has_value() ? json(*observation.applied_generation)
                                                  : json(nullptr)},
      {"last_assignment_id",
       observation.last_assignment_id.has_value() ? json(*observation.last_assignment_id)
                                                  : json(nullptr)},
      {"status", comet::ToString(observation.status)},
      {"status_message", observation.status_message},
      {"observed_state_json", observation.observed_state_json},
      {"runtime_status_json", observation.runtime_status_json},
      {"instance_runtime_json", observation.instance_runtime_json},
      {"gpu_telemetry_json", observation.gpu_telemetry_json},
      {"disk_telemetry_json", observation.disk_telemetry_json},
      {"network_telemetry_json", observation.network_telemetry_json},
      {"cpu_telemetry_json", observation.cpu_telemetry_json},
      {"heartbeat_at", observation.heartbeat_at},
  };
}

json BuildDiskRuntimeStatePayload(const comet::DiskRuntimeState& state) {
  return json{
      {"disk_name", state.disk_name},
      {"plane_name", state.plane_name},
      {"node_name", state.node_name},
      {"image_path", state.image_path},
      {"filesystem_type", state.filesystem_type},
      {"loop_device", state.loop_device},
      {"mount_point", state.mount_point},
      {"runtime_state", state.runtime_state},
      {"attached_at", state.attached_at},
      {"mounted_at", state.mounted_at},
      {"last_verified_at", state.last_verified_at},
      {"status_message", state.status_message},
      {"updated_at", state.updated_at},
  };
}

comet::DiskRuntimeState ParseDiskRuntimeStatePayload(const json& payload) {
  comet::DiskRuntimeState state;
  state.disk_name = payload.value("disk_name", std::string{});
  state.plane_name = payload.value("plane_name", std::string{});
  state.node_name = payload.value("node_name", std::string{});
  state.image_path = payload.value("image_path", std::string{});
  state.filesystem_type = payload.value("filesystem_type", std::string{});
  state.loop_device = payload.value("loop_device", std::string{});
  state.mount_point = payload.value("mount_point", std::string{});
  state.runtime_state = payload.value("runtime_state", std::string{});
  state.attached_at = payload.value("attached_at", std::string{});
  state.mounted_at = payload.value("mounted_at", std::string{});
  state.last_verified_at = payload.value("last_verified_at", std::string{});
  state.status_message = payload.value("status_message", std::string{});
  state.updated_at = payload.value("updated_at", std::string{});
  return state;
}

class LocalDbHostdBackend final : public HostdBackend {
 public:
  explicit LocalDbHostdBackend(std::string db_path) : store_(std::move(db_path)) {
    store_.Initialize();
  }

  std::optional<comet::HostAssignment> ClaimNextHostAssignment(
      const std::string& node_name) override {
    return store_.ClaimNextHostAssignment(node_name);
  }

  bool TransitionClaimedHostAssignment(
      int assignment_id,
      comet::HostAssignmentStatus status,
      const std::string& status_message) override {
    return store_.TransitionClaimedHostAssignment(assignment_id, status, status_message);
  }

  bool UpdateHostAssignmentProgress(
      int assignment_id,
      const json& progress) override {
    return store_.UpdateHostAssignmentProgress(assignment_id, progress.dump());
  }

  void UpsertHostObservation(const comet::HostObservation& observation) override {
    store_.UpsertHostObservation(observation);
  }

  void AppendEvent(const comet::EventRecord& event) override {
    store_.AppendEvent(event);
  }

  void UpsertDiskRuntimeState(const comet::DiskRuntimeState& state) override {
    store_.UpsertDiskRuntimeState(state);
  }

  std::optional<comet::DiskRuntimeState> LoadDiskRuntimeState(
      const std::string& disk_name,
      const std::string& node_name) override {
    return store_.LoadDiskRuntimeState(disk_name, node_name);
  }

 private:
  comet::ControllerStore store_;
};

class HttpHostdBackend final : public HostdBackend {
 public:
  HttpHostdBackend(
      std::string controller_url,
      std::string private_key_base64,
      std::string trusted_controller_fingerprint)
      : target_(ParseControllerTarget(std::move(controller_url))),
        private_key_base64_(std::move(private_key_base64)),
        trusted_controller_fingerprint_(std::move(trusted_controller_fingerprint)) {}

  std::optional<comet::HostAssignment> ClaimNextHostAssignment(
      const std::string& node_name) override {
    EnsureSession(node_name, "claiming next assignment");
    const json payload = SendEncryptedControllerJsonRequest(
        "/api/v1/hostd/assignments/next",
        json{{"node_name", node_name}},
        "assignments/next");
    if (!payload.contains("assignment") || payload["assignment"].is_null()) {
      return std::nullopt;
    }
    return ParseAssignmentPayload(payload["assignment"]);
  }

  bool TransitionClaimedHostAssignment(
      int assignment_id,
      comet::HostAssignmentStatus status,
      const std::string& status_message) override {
    if (status == comet::HostAssignmentStatus::Applied) {
      SendEncryptedControllerJsonRequest(
          "/api/v1/hostd/assignments/" + std::to_string(assignment_id) + "/applied",
          json{{"status_message", status_message}},
          "assignments/" + std::to_string(assignment_id) + "/applied");
      return true;
    }
    if (status == comet::HostAssignmentStatus::Pending || status == comet::HostAssignmentStatus::Failed) {
      SendEncryptedControllerJsonRequest(
          "/api/v1/hostd/assignments/" + std::to_string(assignment_id) + "/failed",
          json{
              {"status_message", status_message},
              {"retry", status == comet::HostAssignmentStatus::Pending},
          },
          "assignments/" + std::to_string(assignment_id) + "/failed");
      return true;
    }
    throw std::runtime_error("unsupported remote assignment transition");
  }

  bool UpdateHostAssignmentProgress(
      int assignment_id,
      const json& progress) override {
    SendEncryptedControllerJsonRequest(
        "/api/v1/hostd/assignments/" + std::to_string(assignment_id) + "/progress",
        progress,
        "assignments/" + std::to_string(assignment_id) + "/progress");
    return true;
  }

  void UpsertHostObservation(const comet::HostObservation& observation) override {
    EnsureSession(observation.node_name, "uploading observation");
    SendEncryptedControllerJsonRequest(
        "/api/v1/hostd/observations",
        BuildHostObservationPayload(observation),
        "observations/upsert");
  }

  void AppendEvent(const comet::EventRecord& event) override {
    if (!event.node_name.empty()) {
      EnsureSession(event.node_name, "appending event");
    }
    SendEncryptedControllerJsonRequest(
        "/api/v1/hostd/events",
        json{
            {"plane_name", event.plane_name},
            {"node_name", event.node_name},
            {"worker_name", event.worker_name},
            {"assignment_id", event.assignment_id.has_value() ? json(*event.assignment_id)
                                                              : json(nullptr)},
            {"rollout_action_id",
             event.rollout_action_id.has_value() ? json(*event.rollout_action_id) : json(nullptr)},
            {"category", event.category},
            {"event_type", event.event_type},
            {"severity", event.severity},
            {"message", event.message},
            {"payload_json", event.payload_json},
        },
        "events/append");
  }

  void UpsertDiskRuntimeState(const comet::DiskRuntimeState& state) override {
    EnsureSession(state.node_name, "upserting disk runtime state");
    SendEncryptedControllerJsonRequest(
        "/api/v1/hostd/disk-runtime-state",
        BuildDiskRuntimeStatePayload(state),
        "disk-runtime-state/upsert");
  }

  std::optional<comet::DiskRuntimeState> LoadDiskRuntimeState(
      const std::string& disk_name,
      const std::string& node_name) override {
    EnsureSession(node_name, "loading disk runtime state");
    const json payload = SendEncryptedControllerJsonRequest(
        "/api/v1/hostd/disk-runtime-state/load",
        json{{"disk_name", disk_name}, {"node_name", node_name}},
        "disk-runtime-state/load");
    if (!payload.contains("runtime_state") || payload["runtime_state"].is_null()) {
      return std::nullopt;
    }
    return ParseDiskRuntimeStatePayload(payload["runtime_state"]);
  }

 private:
  static constexpr std::uint64_t SessionRekeyMessageLimit() {
    return 64;
  }

  std::string BuildRequestAad(
      const std::string& message_type,
      std::uint64_t sequence_number) const {
    return "request\n" + message_type + "\n" + session_node_name_ + "\n" +
           std::to_string(sequence_number);
  }

  std::string BuildResponseAad(
      const std::string& message_type,
      std::uint64_t sequence_number) const {
    return "response\n" + message_type + "\n" + session_node_name_ + "\n" +
           std::to_string(sequence_number);
  }

  json SendEncryptedControllerJsonRequest(
      const std::string& path,
      const json& payload,
      const std::string& message_type) {
    if (session_token_.empty()) {
      throw std::runtime_error("missing host session token");
    }
    host_sequence_ += 1;
    const comet::EncryptedEnvelope envelope = comet::EncryptEnvelopeBase64(
        payload.dump(),
        session_token_,
        BuildRequestAad(message_type, host_sequence_));
    const json response = SendControllerJsonRequest(
        target_,
        "POST",
        path,
        json{
            {"encrypted", true},
            {"sequence_number", host_sequence_},
            {"nonce", envelope.nonce_base64},
            {"ciphertext", envelope.ciphertext_base64},
        },
        SessionHeaders());
    if (!response.value("encrypted", false)) {
      return response;
    }
    const std::uint64_t controller_sequence =
        response.value("sequence_number", static_cast<std::uint64_t>(0));
    if (controller_sequence <= controller_sequence_) {
      throw std::runtime_error("stale or replayed controller session response");
    }
    const comet::EncryptedEnvelope response_envelope{
        response.value("nonce", std::string{}),
        response.value("ciphertext", std::string{}),
    };
    const std::string decrypted = comet::DecryptEnvelopeBase64(
        response_envelope,
        session_token_,
        BuildResponseAad(message_type, controller_sequence));
    controller_sequence_ = controller_sequence;
    return decrypted.empty() ? json::object() : json::parse(decrypted);
  }

  void EnsureSession(const std::string& node_name, const std::string& status_message) {
    if (!session_token_.empty() &&
        (host_sequence_ >= SessionRekeyMessageLimit() ||
         controller_sequence_ >= SessionRekeyMessageLimit())) {
      session_token_.clear();
      session_node_name_.clear();
      host_sequence_ = 0;
      controller_sequence_ = 0;
    }
    if (!session_token_.empty() && session_node_name_ == node_name) {
      try {
        SendEncryptedControllerJsonRequest(
            "/api/v1/hostd/session/heartbeat",
            json{
                {"node_name", node_name},
                {"session_state", "connected"},
                {"status_message", status_message}},
            "session/heartbeat");
        return;
      } catch (const std::exception&) {
        session_token_.clear();
        session_node_name_.clear();
        host_sequence_ = 0;
        controller_sequence_ = 0;
      }
    }
    const std::string nonce = comet::RandomTokenBase64(24);
    const std::string timestamp = std::to_string(std::time(nullptr));
    const std::string message =
        "hostd-session-open\n" + node_name + "\n" + timestamp + "\n" + nonce;
    const std::string signature =
        comet::SignDetachedBase64(message, private_key_base64_);
    const json response = SendControllerJsonRequest(
        target_,
        "POST",
        "/api/v1/hostd/session/open",
        json{
            {"node_name", node_name},
            {"timestamp", timestamp},
            {"nonce", nonce},
            {"signature", signature},
            {"status_message", status_message},
        });
    const std::string controller_fingerprint =
        response.value("controller_public_key_fingerprint", std::string{});
    if (!trusted_controller_fingerprint_.empty() &&
        controller_fingerprint != trusted_controller_fingerprint_) {
      throw std::runtime_error("controller fingerprint mismatch during host session open");
    }
    session_token_ = response.value("session_token", std::string{});
    session_node_name_ = node_name;
    host_sequence_ = 0;
    controller_sequence_ =
        response.value("controller_sequence", static_cast<std::uint64_t>(0));
    SendEncryptedControllerJsonRequest(
        "/api/v1/hostd/session/heartbeat",
        json{{"node_name", node_name}, {"session_state", "connected"}, {"status_message", status_message}},
        "session/heartbeat");
  }

  std::map<std::string, std::string> SessionHeaders() const {
    if (session_token_.empty()) {
      return {};
    }
    return {
        {"X-Comet-Host-Session", session_token_},
        {"X-Comet-Host-Node", session_node_name_},
    };
  }

  ControllerEndpointTarget target_;
  std::string private_key_base64_;
  std::string trusted_controller_fingerprint_;
  std::string session_token_;
  std::string session_node_name_;
  std::uint64_t host_sequence_ = 0;
  std::uint64_t controller_sequence_ = 0;
};

void PublishAssignmentProgress(
    HostdBackend* backend,
    const std::optional<int>& assignment_id,
    const json& progress) {
  if (backend == nullptr || !assignment_id.has_value()) {
    return;
  }
  backend->UpdateHostAssignmentProgress(*assignment_id, progress);
}

std::unique_ptr<HostdBackend> CreateHostdBackend(
    const std::optional<std::string>& db_path,
    const std::optional<std::string>& controller_url,
    const std::optional<std::string>& host_private_key_path,
    const std::optional<std::string>& controller_fingerprint) {
  if (controller_url.has_value() && !controller_url->empty()) {
    if (!host_private_key_path.has_value() || host_private_key_path->empty()) {
      throw std::runtime_error("--host-private-key is required for remote host-agent mode");
    }
    std::ifstream input(*host_private_key_path);
    if (!input.is_open()) {
      throw std::runtime_error("failed to read host private key '" + *host_private_key_path + "'");
    }
    std::stringstream buffer;
    buffer << input.rdbuf();
    return std::make_unique<HttpHostdBackend>(
        *controller_url,
        Trim(buffer.str()),
        controller_fingerprint.value_or(""));
  }
  return std::make_unique<LocalDbHostdBackend>(db_path.value_or(DefaultDbPath()));
}

void AppendHostdEvent(
    HostdBackend& backend,
    const std::string& category,
    const std::string& event_type,
    const std::string& message,
    const json& payload = json::object(),
    const std::string& plane_name = "",
    const std::string& node_name = "",
    const std::string& worker_name = "",
    const std::optional<int>& assignment_id = std::nullopt,
    const std::optional<int>& rollout_action_id = std::nullopt,
    const std::string& severity = "info") {
  backend.AppendEvent(comet::EventRecord{
      0,
      plane_name,
      node_name,
      worker_name,
      assignment_id,
      rollout_action_id,
      category,
      event_type,
      severity,
      message,
      SerializeEventPayload(payload),
      "",
  });
}

std::optional<std::string> CurrentMountFilesystemType(const std::string& mount_point) {
  std::ifstream input("/proc/mounts");
  if (!input.is_open()) {
    return std::nullopt;
  }

  std::string source;
  std::string target;
  std::string fs_type;
  while (input >> source >> target >> fs_type) {
    std::string rest_of_line;
    std::getline(input, rest_of_line);
    if (target == mount_point) {
      return fs_type;
    }
  }
  return std::nullopt;
}

struct BlockDeviceIoStats {
  std::uint64_t read_ios = 0;
  std::uint64_t read_sectors = 0;
  std::uint64_t write_ios = 0;
  std::uint64_t write_sectors = 0;
  std::uint64_t io_in_progress = 0;
  std::uint64_t io_time_ms = 0;
  std::uint64_t weighted_io_time_ms = 0;
};

std::optional<std::string> BlockDeviceNameFromPath(const std::string& device_path) {
  if (device_path.empty()) {
    return std::nullopt;
  }
  const auto device_name = std::filesystem::path(device_path).filename().string();
  if (device_name.empty()) {
    return std::nullopt;
  }
  return device_name;
}

std::optional<bool> ReadBlockDeviceReadOnly(const std::string& device_path) {
  const auto device_name = BlockDeviceNameFromPath(device_path);
  if (!device_name.has_value()) {
    return std::nullopt;
  }
  std::ifstream input("/sys/class/block/" + *device_name + "/ro");
  if (!input.is_open()) {
    return std::nullopt;
  }
  int value = 0;
  if (!(input >> value)) {
    return std::nullopt;
  }
  return value != 0;
}

std::optional<std::uint64_t> ReadBlockDeviceIoErrorCount(const std::string& device_path) {
  const auto device_name = BlockDeviceNameFromPath(device_path);
  if (!device_name.has_value()) {
    return std::nullopt;
  }
  const std::array<std::filesystem::path, 2> candidates{
      std::filesystem::path("/sys/class/block") / *device_name / "ioerr_cnt",
      std::filesystem::path("/sys/class/block") / *device_name / "device" / "ioerr_cnt",
  };
  for (const auto& candidate : candidates) {
    std::ifstream input(candidate);
    if (!input.is_open()) {
      continue;
    }
    std::uint64_t value = 0;
    if (input >> value) {
      return value;
    }
  }
  return std::nullopt;
}

std::optional<BlockDeviceIoStats> ReadBlockDeviceIoStats(
    const std::string& device_path) {
  const auto device_name = BlockDeviceNameFromPath(device_path);
  if (!device_name.has_value()) {
    return std::nullopt;
  }
  std::ifstream input("/sys/class/block/" + *device_name + "/stat");
  if (!input.is_open()) {
    return std::nullopt;
  }

  BlockDeviceIoStats stats;
  std::uint64_t reads_merged = 0;
  std::uint64_t read_time_ms = 0;
  std::uint64_t writes_merged = 0;
  std::uint64_t write_time_ms = 0;
  if (!(input >> stats.read_ios >> reads_merged >> stats.read_sectors >> read_time_ms >>
        stats.write_ios >> writes_merged >> stats.write_sectors >> write_time_ms >>
        stats.io_in_progress >> stats.io_time_ms >> stats.weighted_io_time_ms)) {
    return std::nullopt;
  }
  return stats;
}

comet::DiskTelemetrySnapshot CollectDiskTelemetry(
    const comet::DesiredState& state,
    const std::string& node_name) {
  comet::DiskTelemetrySnapshot snapshot;
  snapshot.contract_version = 1;
  snapshot.source = "statvfs";
  snapshot.collected_at = CurrentTimestampString();

  for (const auto& disk : state.disks) {
    if (disk.node_name != node_name) {
      continue;
    }

    comet::DiskTelemetryRecord record;
    record.disk_name = disk.name;
    record.plane_name = disk.plane_name;
    record.node_name = disk.node_name;
    record.mount_point = disk.host_path;
    record.runtime_state = std::filesystem::exists(disk.host_path) ? "present" : "missing";
    record.health = std::filesystem::exists(disk.host_path) ? "ok" : "missing";
    if (record.health == "missing") {
      record.fault_count += 1;
      record.fault_reasons.push_back("host-path-missing");
    }

    if (IsPathMounted(disk.host_path)) {
      record.mounted = true;
      record.runtime_state = "mounted";
      const auto mount_fs = CurrentMountFilesystemType(disk.host_path);
      if (mount_fs.has_value()) {
        record.filesystem_type = *mount_fs;
      }
      const auto mount_source = CurrentMountSource(disk.host_path);
      if (mount_source.has_value()) {
        record.mount_source = *mount_source;
      } else {
        record.warning_count += 1;
        record.fault_reasons.push_back("mount-source-unavailable");
        if (record.health == "ok") {
          record.health = "degraded";
        }
      }
      if (mount_source.has_value() && mount_source->rfind("/dev/", 0) == 0) {
        if (const auto read_only = ReadBlockDeviceReadOnly(*mount_source);
            read_only.has_value()) {
          record.read_only = *read_only;
          if (*read_only) {
            record.warning_count += 1;
            record.fault_reasons.push_back("read-only-device");
            if (record.health == "ok") {
              record.health = "degraded";
            }
          }
        }
        if (const auto io_stats = ReadBlockDeviceIoStats(*mount_source);
            io_stats.has_value()) {
          record.perf_counters_available = true;
          record.read_ios = io_stats->read_ios;
          record.write_ios = io_stats->write_ios;
          record.read_bytes = io_stats->read_sectors * 512ULL;
          record.write_bytes = io_stats->write_sectors * 512ULL;
          record.io_in_progress = static_cast<int>(io_stats->io_in_progress);
          record.io_time_ms = io_stats->io_time_ms;
          record.weighted_io_time_ms = io_stats->weighted_io_time_ms;
        } else {
          record.status_message =
              record.status_message.empty()
                  ? "block io stats unavailable"
                  : record.status_message + "; block io stats unavailable";
          record.warning_count += 1;
          record.fault_reasons.push_back("block-io-stats-unavailable");
          if (record.health == "ok") {
            record.health = "degraded";
          }
        }
        if (const auto io_error_count = ReadBlockDeviceIoErrorCount(*mount_source);
            io_error_count.has_value()) {
          record.io_error_counters_available = true;
          record.io_error_count = *io_error_count;
          if (*io_error_count > 0) {
            record.fault_count += 1;
            record.fault_reasons.push_back("io-error-count-nonzero");
            if (record.health == "ok") {
              record.health = "degraded";
            }
          }
        }
      }
    }

    struct statvfs stats {};
    if (statvfs(disk.host_path.c_str(), &stats) == 0) {
      const std::uint64_t block_size = static_cast<std::uint64_t>(stats.f_frsize);
      record.total_bytes = static_cast<std::uint64_t>(stats.f_blocks) * block_size;
      record.free_bytes = static_cast<std::uint64_t>(stats.f_bavail) * block_size;
      record.used_bytes =
          record.total_bytes >= record.free_bytes ? (record.total_bytes - record.free_bytes) : 0;
      if (record.health == "missing") {
        record.health = "ok";
      }
      if (record.runtime_state == "missing") {
        record.runtime_state = "available";
      }
    } else {
      record.status_message =
          record.status_message.empty()
              ? "statvfs unavailable"
              : record.status_message + "; statvfs unavailable";
      record.fault_count += 1;
      record.fault_reasons.push_back("statvfs-unavailable");
      if (record.health == "ok") {
        record.health = "degraded";
      }
    }

    snapshot.items.push_back(std::move(record));
  }

  return snapshot;
}

std::optional<std::string> ReadTrimmedFile(const std::filesystem::path& path) {
  std::ifstream input(path);
  if (!input.is_open()) {
    return std::nullopt;
  }
  std::string value;
  std::getline(input, value);
  while (!value.empty() && (value.back() == '\n' || value.back() == '\r' || value.back() == ' ')) {
    value.pop_back();
  }
  return value;
}

std::uint64_t ReadUint64FileOrZero(const std::filesystem::path& path) {
  std::ifstream input(path);
  std::uint64_t value = 0;
  if (!input.is_open()) {
    return 0;
  }
  input >> value;
  return value;
}

struct CpuSample {
  std::uint64_t idle = 0;
  std::uint64_t total = 0;
};

std::optional<CpuSample> ReadCpuSample() {
  std::ifstream input("/proc/stat");
  if (!input.is_open()) {
    return std::nullopt;
  }

  std::string cpu_label;
  CpuSample sample;
  std::uint64_t user = 0;
  std::uint64_t nice = 0;
  std::uint64_t system = 0;
  std::uint64_t idle = 0;
  std::uint64_t iowait = 0;
  std::uint64_t irq = 0;
  std::uint64_t softirq = 0;
  std::uint64_t steal = 0;
  input >> cpu_label >> user >> nice >> system >> idle >> iowait >> irq >> softirq >> steal;
  if (!input.good() || cpu_label != "cpu") {
    return std::nullopt;
  }
  sample.idle = idle + iowait;
  sample.total = user + nice + system + idle + iowait + irq + softirq + steal;
  return sample;
}

std::optional<std::array<double, 3>> ReadLoadAverage() {
  std::ifstream input("/proc/loadavg");
  if (!input.is_open()) {
    return std::nullopt;
  }
  std::array<double, 3> load{0.0, 0.0, 0.0};
  input >> load[0] >> load[1] >> load[2];
  if (!input.good()) {
    return std::nullopt;
  }
  return load;
}

comet::CpuTelemetrySnapshot CollectCpuTelemetry() {
  comet::CpuTelemetrySnapshot snapshot;
  snapshot.contract_version = 1;
  snapshot.source = "procfs";
  snapshot.collected_at = CurrentTimestampString();
  snapshot.core_count = static_cast<int>(std::thread::hardware_concurrency());

  const auto first = ReadCpuSample();
  std::this_thread::sleep_for(std::chrono::milliseconds(150));
  const auto second = ReadCpuSample();
  if (first.has_value() && second.has_value() && second->total > first->total) {
    const auto total_delta = static_cast<double>(second->total - first->total);
    const auto idle_delta = static_cast<double>(second->idle - first->idle);
    snapshot.utilization_pct =
        total_delta > 0.0 ? std::max(0.0, 100.0 * (1.0 - (idle_delta / total_delta))) : 0.0;
  } else {
    snapshot.degraded = true;
    snapshot.source = "procfs-unavailable";
  }

  if (const auto load = ReadLoadAverage(); load.has_value()) {
    snapshot.loadavg_1m = (*load)[0];
    snapshot.loadavg_5m = (*load)[1];
    snapshot.loadavg_15m = (*load)[2];
  } else {
    snapshot.degraded = true;
  }

  std::ifstream meminfo("/proc/meminfo");
  if (meminfo.is_open()) {
    std::string key;
    std::uint64_t value = 0;
    std::string unit;
    std::uint64_t total_kb = 0;
    std::uint64_t available_kb = 0;
    while (meminfo >> key >> value >> unit) {
      if (key == "MemTotal:") {
        total_kb = value;
      } else if (key == "MemAvailable:") {
        available_kb = value;
      }
    }
    snapshot.total_memory_bytes = total_kb * 1024ULL;
    snapshot.available_memory_bytes = available_kb * 1024ULL;
    snapshot.used_memory_bytes =
        snapshot.total_memory_bytes >= snapshot.available_memory_bytes
            ? (snapshot.total_memory_bytes - snapshot.available_memory_bytes)
            : 0;
  } else {
    snapshot.degraded = true;
  }

  return snapshot;
}

comet::NetworkTelemetrySnapshot CollectNetworkTelemetry() {
  comet::NetworkTelemetrySnapshot snapshot;
  snapshot.contract_version = 1;
  snapshot.source = "sysfs";
  snapshot.collected_at = CurrentTimestampString();

  const std::filesystem::path net_root("/sys/class/net");
  if (!std::filesystem::exists(net_root)) {
    snapshot.degraded = true;
    snapshot.source = "unavailable";
    return snapshot;
  }

  for (const auto& entry : std::filesystem::directory_iterator(net_root)) {
    if (!entry.is_directory() && !entry.is_symlink()) {
      continue;
    }
    comet::NetworkInterfaceTelemetry interface;
    interface.interface_name = entry.path().filename().string();
    interface.oper_state =
        ReadTrimmedFile(entry.path() / "operstate").value_or(std::string{"unknown"});
    const auto carrier = ReadTrimmedFile(entry.path() / "carrier");
    if (carrier.has_value()) {
      interface.link_state = (*carrier == "1") ? "up" : "down";
    } else {
      interface.link_state = interface.oper_state;
    }
    interface.rx_bytes = ReadUint64FileOrZero(entry.path() / "statistics" / "rx_bytes");
    interface.tx_bytes = ReadUint64FileOrZero(entry.path() / "statistics" / "tx_bytes");
    interface.loopback = interface.interface_name == "lo";
    snapshot.interfaces.push_back(std::move(interface));
  }

  std::sort(
      snapshot.interfaces.begin(),
      snapshot.interfaces.end(),
      [](const auto& left, const auto& right) { return left.interface_name < right.interface_name; });
  return snapshot;
}

std::optional<std::string> WorkerRuntimeStatusPathForInstance(
    const comet::DesiredState& state,
    const comet::InstanceSpec& instance) {
  if (instance.role != comet::InstanceRole::Worker) {
    return std::nullopt;
  }
  for (const auto& disk : state.disks) {
    if (disk.kind == comet::DiskKind::WorkerPrivate &&
        disk.owner_name == instance.name &&
        disk.node_name == instance.node_name) {
      return (std::filesystem::path(disk.host_path) / "worker-runtime-status.json").string();
    }
  }
  return std::nullopt;
}

comet::RuntimeProcessStatus ToProcessStatus(
    comet::RuntimeStatus status,
    const comet::InstanceSpec& instance) {
  if (status.instance_name.empty()) {
    status.instance_name = instance.name;
  }
  if (status.instance_role.empty()) {
    status.instance_role = comet::ToString(instance.role);
  }
  if (status.node_name.empty()) {
    status.node_name = instance.node_name;
  }
  if (status.gpu_device.empty() && instance.gpu_device.has_value()) {
    status.gpu_device = *instance.gpu_device;
  }
  comet::RuntimeProcessStatus process;
  process.instance_name = status.instance_name;
  process.instance_role = status.instance_role;
  process.node_name = status.node_name;
  process.model_path = status.model_path.empty() ? status.cached_local_model_path : status.model_path;
  process.gpu_device = status.gpu_device;
  process.runtime_phase = status.runtime_phase;
  process.started_at = status.started_at;
  process.last_activity_at = status.last_activity_at;
  process.runtime_pid = status.runtime_pid;
  process.engine_pid = status.engine_pid;
  process.ready = status.ready || status.launch_ready || status.inference_ready;
  return process;
}

std::vector<comet::RuntimeProcessStatus> LoadLocalInstanceRuntimeStatuses(
    const std::string& state_root,
    const std::string& node_name,
    const std::optional<std::string>& plane_name) {
  std::vector<comet::RuntimeProcessStatus> result;
  const auto local_states = plane_name.has_value()
                                ? [&]() {
                                    std::vector<comet::DesiredState> states;
                                    const auto state =
                                        LoadLocalAppliedState(state_root, node_name, plane_name);
                                    if (state.has_value()) {
                                      states.push_back(*state);
                                    }
                                    return states;
                                  }()
                                : LoadAllLocalAppliedStates(state_root, node_name);
  for (const auto& local_state : local_states) {
    for (const auto& instance : local_state.instances) {
      if (instance.node_name != node_name) {
        continue;
      }
      std::optional<std::string> status_path;
      if (instance.role == comet::InstanceRole::Infer) {
        status_path = RuntimeStatusPathForNode(local_state, node_name);
      } else {
        status_path = WorkerRuntimeStatusPathForInstance(local_state, instance);
      }
      if (!status_path.has_value() || !std::filesystem::exists(*status_path)) {
        continue;
      }
      const auto status = comet::LoadRuntimeStatusJson(*status_path);
      if (!status.has_value()) {
        continue;
      }
      result.push_back(ToProcessStatus(*status, instance));
    }
  }
  return result;
}

std::optional<std::string> ResolveComposeContainerIdForService(const std::string& service_name) {
  const std::string output =
      RunCommandCapture(
          ResolvedDockerCommand() +
          " ps --filter label=com.docker.compose.service=" + service_name +
          " --format '{{.ID}}' 2>/dev/null");
  std::istringstream input(output);
  std::string container_id;
  while (std::getline(input, container_id)) {
    container_id = Trim(container_id);
    if (!container_id.empty()) {
      return container_id;
    }
  }
  return std::nullopt;
}

std::optional<int> ResolveServiceHostPid(const std::string& service_name) {
  const auto container_id = ResolveComposeContainerIdForService(service_name);
  if (!container_id.has_value()) {
    return std::nullopt;
  }
  const std::string output =
      RunCommandCapture(
          ResolvedDockerCommand() + " top " + *container_id + " -eo pid,comm,args 2>/dev/null");
  std::istringstream input(output);
  std::string line;
  bool first = true;
  std::optional<int> fallback_pid;
  while (std::getline(input, line)) {
    if (first) {
      first = false;
      continue;
    }
    line = Trim(line);
    if (line.empty()) {
      continue;
    }
    std::istringstream row(line);
    int pid = 0;
    std::string comm;
    row >> pid >> comm;
    std::string args;
    std::getline(row, args);
    args = Trim(args);
    if (pid <= 0) {
      continue;
    }
    if (args.find("comet-workerd") != std::string::npos ||
        args.find("comet-inferctl") != std::string::npos) {
      return pid;
    }
    if (comm != "tini" && comm != "bash" && comm != "sh" && !fallback_pid.has_value()) {
      fallback_pid = pid;
    }
  }
  return fallback_pid;
}

void ResolveInstanceHostPids(std::vector<comet::RuntimeProcessStatus>* statuses) {
  if (statuses == nullptr) {
    return;
  }
  for (auto& status : *statuses) {
    const auto host_pid = ResolveServiceHostPid(status.instance_name);
    if (host_pid.has_value()) {
      status.runtime_pid = *host_pid;
      status.engine_pid = *host_pid;
    }
  }
}

std::optional<std::string> ParseTaggedValue(
    const std::string& text,
    const std::string& key) {
  const std::string needle = key + "=";
  const std::size_t begin = text.find(needle);
  if (begin == std::string::npos) {
    return std::nullopt;
  }
  std::size_t end = text.find(' ', begin + needle.size());
  if (end == std::string::npos) {
    end = text.size();
  }
  return text.substr(begin + needle.size(), end - (begin + needle.size()));
}

std::vector<std::string> ParseTaggedCsv(
    const std::string& text,
    const std::string& key) {
  const auto value = ParseTaggedValue(text, key);
  if (!value.has_value()) {
    return {};
  }
  std::vector<std::string> items;
  std::stringstream stream(*value);
  std::string item;
  while (std::getline(stream, item, ',')) {
    item = Trim(item);
    if (!item.empty()) {
      items.push_back(item);
    }
  }
  return items;
}

std::map<std::string, int> CaptureServiceHostPids(const std::vector<std::string>& service_names) {
  std::map<std::string, int> result;
  for (const auto& service_name : service_names) {
    const auto pid = ResolveServiceHostPid(service_name);
    if (pid.has_value()) {
      result.emplace(service_name, *pid);
    }
  }
  return result;
}

struct NvmlMemoryInfo {
  unsigned long long total = 0;
  unsigned long long free = 0;
  unsigned long long used = 0;
};

struct NvmlUtilizationInfo {
  unsigned int gpu = 0;
  unsigned int memory = 0;
};

std::optional<comet::GpuTelemetrySnapshot> TryCollectGpuTelemetryWithNvml(
    const comet::DesiredState& state,
    const std::string& node_name) {
  void* lib = dlopen("libnvidia-ml.so.1", RTLD_LAZY);
  if (lib == nullptr) {
    return std::nullopt;
  }

  using nvmlReturn_t = int;
  using nvmlDevice_t = void*;
  constexpr nvmlReturn_t kNvmlSuccess = 0;
  using NvmlInitFn = nvmlReturn_t (*)();
  using NvmlShutdownFn = nvmlReturn_t (*)();
  using NvmlGetHandleFn = nvmlReturn_t (*)(unsigned int, nvmlDevice_t*);
  using NvmlMemoryInfoFn = nvmlReturn_t (*)(nvmlDevice_t, NvmlMemoryInfo*);
  using NvmlUtilizationFn = nvmlReturn_t (*)(nvmlDevice_t, NvmlUtilizationInfo*);

  const auto init = reinterpret_cast<NvmlInitFn>(dlsym(lib, "nvmlInit_v2"));
  const auto shutdown = reinterpret_cast<NvmlShutdownFn>(dlsym(lib, "nvmlShutdown"));
  const auto get_handle =
      reinterpret_cast<NvmlGetHandleFn>(dlsym(lib, "nvmlDeviceGetHandleByIndex_v2"));
  const auto get_memory =
      reinterpret_cast<NvmlMemoryInfoFn>(dlsym(lib, "nvmlDeviceGetMemoryInfo"));
  const auto get_utilization =
      reinterpret_cast<NvmlUtilizationFn>(dlsym(lib, "nvmlDeviceGetUtilizationRates"));
  if (init == nullptr || shutdown == nullptr || get_handle == nullptr ||
      get_memory == nullptr || get_utilization == nullptr) {
    dlclose(lib);
    return std::nullopt;
  }

  if (init() != kNvmlSuccess) {
    dlclose(lib);
    return std::nullopt;
  }

  comet::GpuTelemetrySnapshot snapshot;
  snapshot.degraded = false;
  snapshot.source = "nvml";
  for (const auto& node : state.nodes) {
    if (node.name != node_name) {
      continue;
    }
    for (const auto& gpu_device : node.gpu_devices) {
      unsigned int index = 0;
      try {
        index = static_cast<unsigned int>(std::stoul(gpu_device));
      } catch (const std::exception&) {
        continue;
      }
      nvmlDevice_t handle = nullptr;
      if (get_handle(index, &handle) != kNvmlSuccess || handle == nullptr) {
        continue;
      }
      NvmlMemoryInfo memory{};
      NvmlUtilizationInfo utilization{};
      if (get_memory(handle, &memory) != kNvmlSuccess ||
          get_utilization(handle, &utilization) != kNvmlSuccess) {
        continue;
      }
      comet::GpuDeviceTelemetry device;
      device.gpu_device = gpu_device;
      device.total_vram_mb = static_cast<int>(memory.total / (1024 * 1024));
      device.used_vram_mb = static_cast<int>(memory.used / (1024 * 1024));
      device.free_vram_mb = static_cast<int>(memory.free / (1024 * 1024));
      device.gpu_utilization_pct = static_cast<int>(utilization.gpu);
      snapshot.devices.push_back(std::move(device));
    }
  }

  shutdown();
  dlclose(lib);
  return snapshot;
}

void PopulateGpuProcessesFromNvidiaSmi(
    comet::GpuTelemetrySnapshot* snapshot,
    const std::vector<comet::RuntimeProcessStatus>& instance_statuses) {
  if (snapshot == nullptr) {
    return;
  }
  std::map<int, std::string> pid_to_instance_name;
  for (const auto& status : instance_statuses) {
    if (status.engine_pid > 0) {
      pid_to_instance_name[status.engine_pid] = status.instance_name;
    }
    if (status.runtime_pid > 0) {
      pid_to_instance_name[status.runtime_pid] = status.instance_name;
    }
  }

  std::map<std::string, std::string> uuid_to_gpu_device;
  {
    const std::string output =
        RunCommandCapture(
            "nvidia-smi --query-gpu=index,uuid --format=csv,noheader,nounits 2>/dev/null");
    std::istringstream input(output);
    std::string line;
    while (std::getline(input, line)) {
      const auto columns = SplitCsvRow(line);
      if (columns.size() >= 2) {
        uuid_to_gpu_device[columns[1]] = columns[0];
      }
    }
  }

  const std::string output =
      RunCommandCapture(
          "nvidia-smi --query-compute-apps=gpu_uuid,pid,used_gpu_memory "
          "--format=csv,noheader,nounits 2>/dev/null");
  std::istringstream input(output);
  std::string line;
  while (std::getline(input, line)) {
    const auto columns = SplitCsvRow(line);
    if (columns.size() < 3) {
      continue;
    }
    const auto gpu_it = uuid_to_gpu_device.find(columns[0]);
    if (gpu_it == uuid_to_gpu_device.end()) {
      continue;
    }
    int pid = 0;
    int used_vram_mb = 0;
    try {
      pid = std::stoi(columns[1]);
      used_vram_mb = std::stoi(columns[2]);
    } catch (const std::exception&) {
      continue;
    }
    for (auto& device : snapshot->devices) {
      if (device.gpu_device != gpu_it->second) {
        continue;
      }
      comet::GpuProcessTelemetry process;
      process.pid = pid;
      process.used_vram_mb = used_vram_mb;
      const auto owner_it = pid_to_instance_name.find(pid);
      if (owner_it != pid_to_instance_name.end()) {
        process.instance_name = owner_it->second;
      }
      device.processes.push_back(std::move(process));
      break;
    }
  }
}

std::optional<comet::GpuTelemetrySnapshot> TryCollectGpuTelemetryWithNvidiaSmi(
    const comet::DesiredState& state,
    const std::string& node_name,
    const std::vector<comet::RuntimeProcessStatus>& instance_statuses) {
  const std::string output =
      RunCommandCapture(
          "nvidia-smi --query-gpu=index,memory.total,memory.used,memory.free,utilization.gpu "
          "--format=csv,noheader,nounits 2>/dev/null");
  if (output.empty()) {
    return std::nullopt;
  }

  std::set<std::string> allowed_devices;
  for (const auto& node : state.nodes) {
    if (node.name != node_name) {
      continue;
    }
    allowed_devices.insert(node.gpu_devices.begin(), node.gpu_devices.end());
  }

  comet::GpuTelemetrySnapshot snapshot;
  snapshot.degraded = true;
  snapshot.source = "nvidia-smi";
  std::istringstream input(output);
  std::string line;
  while (std::getline(input, line)) {
    const auto columns = SplitCsvRow(line);
    if (columns.size() < 5 || allowed_devices.find(columns[0]) == allowed_devices.end()) {
      continue;
    }
    try {
      comet::GpuDeviceTelemetry device;
      device.gpu_device = columns[0];
      device.total_vram_mb = std::stoi(columns[1]);
      device.used_vram_mb = std::stoi(columns[2]);
      device.free_vram_mb = std::stoi(columns[3]);
      device.gpu_utilization_pct = std::stoi(columns[4]);
      snapshot.devices.push_back(std::move(device));
    } catch (const std::exception&) {
      continue;
    }
  }
  PopulateGpuProcessesFromNvidiaSmi(&snapshot, instance_statuses);
  return snapshot;
}

comet::GpuTelemetrySnapshot CollectGpuTelemetry(
    const comet::DesiredState& state,
    const std::string& node_name,
    const std::vector<comet::RuntimeProcessStatus>& instance_statuses) {
  const bool disable_nvml =
      std::getenv("COMET_DISABLE_NVML") != nullptr &&
      std::string(std::getenv("COMET_DISABLE_NVML")) != "0";
  if (!disable_nvml) {
    if (const auto nvml_snapshot = TryCollectGpuTelemetryWithNvml(state, node_name);
        nvml_snapshot.has_value()) {
      comet::GpuTelemetrySnapshot snapshot = *nvml_snapshot;
      PopulateGpuProcessesFromNvidiaSmi(&snapshot, instance_statuses);
      snapshot.contract_version = 1;
      snapshot.collected_at = CurrentTimestampString();
      return snapshot;
    }
  }
  if (const auto smi_snapshot =
          TryCollectGpuTelemetryWithNvidiaSmi(state, node_name, instance_statuses);
      smi_snapshot.has_value()) {
    comet::GpuTelemetrySnapshot snapshot = *smi_snapshot;
    snapshot.contract_version = 1;
    snapshot.collected_at = CurrentTimestampString();
    return snapshot;
  }
  comet::GpuTelemetrySnapshot snapshot;
  snapshot.contract_version = 1;
  snapshot.degraded = true;
  snapshot.source = "unavailable";
  snapshot.collected_at = CurrentTimestampString();
  return snapshot;
}

bool IsContainerAbsentForService(const std::string& service_name) {
  return !ResolveComposeContainerIdForService(service_name).has_value();
}

bool VerifyEvictionAssignment(
    const comet::DesiredState& local_state,
    const std::string& node_name,
    const std::string& state_root,
    const std::string& status_message,
    const std::map<std::string, int>& victim_host_pids) {
  const auto victim_names = ParseTaggedCsv(status_message, "victims");
  const auto target_gpu = ParseTaggedValue(status_message, "target_gpu");
  const int required_memory_cap_mb =
      ParseTaggedValue(status_message, "required_memory_cap_mb").has_value()
          ? std::stoi(*ParseTaggedValue(status_message, "required_memory_cap_mb"))
          : 0;
  constexpr int kReserveMemoryMb = 1024;
  constexpr int kStableSamples = 3;
  const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(30);
  int stable_samples = 0;

  while (std::chrono::steady_clock::now() < deadline) {
    bool victims_gone = true;
    for (const auto& victim_name : victim_names) {
      if (!IsContainerAbsentForService(victim_name)) {
        victims_gone = false;
        break;
      }
    }

    auto instance_statuses = LoadLocalInstanceRuntimeStatuses(state_root, node_name);
    ResolveInstanceHostPids(&instance_statuses);
    const auto telemetry = CollectGpuTelemetry(local_state, node_name, instance_statuses);

    bool victims_released_gpu = true;
    bool memory_ready = target_gpu.has_value() ? false : true;
    for (const auto& device : telemetry.devices) {
      if (target_gpu.has_value() && device.gpu_device == *target_gpu) {
        memory_ready =
            device.free_vram_mb >= required_memory_cap_mb + kReserveMemoryMb;
      }
      for (const auto& process : device.processes) {
        if (std::find(victim_names.begin(), victim_names.end(), process.instance_name) !=
            victim_names.end()) {
          victims_released_gpu = false;
        }
        for (const auto& [victim_name, victim_pid] : victim_host_pids) {
          (void)victim_name;
          if (process.pid == victim_pid) {
            victims_released_gpu = false;
          }
        }
      }
    }

    if (victims_gone && victims_released_gpu && memory_ready) {
      ++stable_samples;
      if (stable_samples >= kStableSamples) {
        return true;
      }
    } else {
      stable_samples = 0;
    }
    std::this_thread::sleep_for(std::chrono::seconds(2));
  }

  return false;
}

comet::HostObservation BuildObservedStateSnapshot(
    const std::string& node_name,
    const std::string& state_root,
    comet::HostObservationStatus status,
    const std::string& status_message,
    const std::optional<int>& last_assignment_id = std::nullopt) {
  comet::HostObservation observation;
  observation.node_name = node_name;
  observation.status = status;
  observation.status_message = status_message;
  observation.last_assignment_id = last_assignment_id;
  observation.applied_generation = LoadLocalAppliedGeneration(state_root, node_name);

  const auto local_state = LoadLocalAppliedState(state_root, node_name);
  if (local_state.has_value()) {
    observation.plane_name = local_state->plane_name;
    observation.observed_state_json = comet::SerializeDesiredStateJson(*local_state);
  }
  const auto runtime_status = LoadLocalRuntimeStatus(state_root, node_name);
  if (runtime_status.has_value()) {
    observation.runtime_status_json = comet::SerializeRuntimeStatusJson(*runtime_status);
  }
  auto instance_statuses = LoadLocalInstanceRuntimeStatuses(state_root, node_name);
  ResolveInstanceHostPids(&instance_statuses);
  if (!instance_statuses.empty()) {
    observation.instance_runtime_json =
        comet::SerializeRuntimeStatusListJson(instance_statuses);
  }
  if (local_state.has_value()) {
    observation.gpu_telemetry_json =
        comet::SerializeGpuTelemetryJson(
            CollectGpuTelemetry(*local_state, node_name, instance_statuses));
    observation.disk_telemetry_json =
        comet::SerializeDiskTelemetryJson(CollectDiskTelemetry(*local_state, node_name));
  }
  observation.network_telemetry_json =
      comet::SerializeNetworkTelemetryJson(CollectNetworkTelemetry());
  observation.cpu_telemetry_json =
      comet::SerializeCpuTelemetryJson(CollectCpuTelemetry());

  return observation;
}

void ReportObservedState(
    HostdBackend& backend,
    const comet::HostObservation& observation,
    const std::string& source_label) {
  backend.UpsertHostObservation(observation);
  AppendHostdEvent(
      backend,
      "host-observation",
      "reported",
      source_label,
      json{
          {"status", comet::ToString(observation.status)},
          {"applied_generation",
           observation.applied_generation.has_value()
               ? json(*observation.applied_generation)
               : json(nullptr)},
          {"last_assignment_id",
           observation.last_assignment_id.has_value()
               ? json(*observation.last_assignment_id)
               : json(nullptr)},
      },
      observation.plane_name,
      observation.node_name,
      "",
      observation.last_assignment_id);

  std::cout << source_label << "\n";
  std::cout << "backend=hostd-control\n";
  std::cout << "node=" << observation.node_name << "\n";
  std::cout << "status=" << comet::ToString(observation.status) << "\n";
  if (!observation.plane_name.empty()) {
    std::cout << "plane=" << observation.plane_name << "\n";
  }
  if (observation.applied_generation.has_value()) {
    std::cout << "applied_generation=" << *observation.applied_generation << "\n";
  }
  if (observation.last_assignment_id.has_value()) {
    std::cout << "last_assignment_id=" << *observation.last_assignment_id << "\n";
  }
  if (!observation.status_message.empty()) {
    std::cout << "message=" << observation.status_message << "\n";
  }
}

bool IsDesiredNodeStateEmpty(const comet::DesiredState& state) {
  return state.disks.empty() && state.instances.empty();
}

void ShowDesiredNodeOps(
    const comet::DesiredState& desired_node_state,
    const std::string& artifacts_root,
    const std::optional<std::string>& runtime_root,
    const std::string& state_root,
    const std::string& source_label,
    const std::optional<int>& desired_generation);

std::optional<comet::DiskSpec> FindDiskInStateByKey(
    const std::optional<comet::DesiredState>& state,
    const std::string& disk_key);

comet::DiskRuntimeState BuildDiskRuntimeState(
    const comet::DiskSpec& disk,
    const std::string& runtime_state,
    const std::string& status_message);

comet::DiskRuntimeState EnsureDesiredDiskRuntimeState(
    const comet::DiskSpec& disk,
    const std::string& disk_key,
    const std::string& storage_root,
    const std::optional<std::string>& runtime_root);

std::pair<std::string, std::string> SplitDiskKey(const std::string& disk_key);

void RemoveRealDiskMount(
    const comet::DiskRuntimeState& runtime_state,
    const std::optional<std::string>& runtime_root);

void ApplyNodePlan(
    const comet::NodeExecutionPlan& plan,
    const comet::DesiredState& desired_node_state,
    const comet::NodeComposePlan& compose_plan,
    const std::string& storage_root,
    const std::optional<std::string>& runtime_root,
    ComposeMode compose_mode,
    const std::optional<int>& assignment_id,
    HostdBackend* backend) {
  std::cout << "applying node=" << plan.node_name << "\n";
  std::cout << "compose=" << plan.compose_file_path << "\n";

  auto apply_operation = [&](const comet::HostOperation& operation) {
    switch (operation.kind) {
      case comet::HostOperationKind::EnsureDisk: {
        const auto disk =
            FindDiskInStateByKey(std::optional<comet::DesiredState>(desired_node_state), operation.target);
        if (!disk.has_value()) {
          throw std::runtime_error(
              "missing desired disk for ensure operation '" + operation.target + "'");
        }
        const auto realized_state =
            EnsureDesiredDiskRuntimeState(
                *disk,
                operation.target,
                storage_root,
                runtime_root);
        if (backend != nullptr) {
          backend->UpsertDiskRuntimeState(realized_state);
        }
        PrintOperationApplied(operation, "applied");
        break;
      }
      case comet::HostOperationKind::RemoveDisk: {
        const auto [disk_name, disk_node_name] = SplitDiskKey(operation.target);
        const auto runtime_state =
            backend == nullptr ? std::nullopt
                               : backend->LoadDiskRuntimeState(disk_name, disk_node_name);
        bool removed = false;
        const bool mounted_now = IsPathMounted(operation.details);
        if (HostCanManageRealDisks() &&
            (mounted_now ||
             (runtime_state.has_value() &&
              (runtime_state->runtime_state == "mounted" ||
               !runtime_state->loop_device.empty() ||
               !runtime_state->image_path.empty())))) {
          comet::DiskRuntimeState effective_state;
          if (runtime_state.has_value()) {
            effective_state = *runtime_state;
          } else {
            effective_state.disk_name = disk_name;
            effective_state.plane_name = plan.plane_name;
            effective_state.node_name = disk_node_name;
            effective_state.mount_point = operation.details;
            effective_state.runtime_state = "mounted";
            effective_state.status_message = "runtime state recovered from live mount";
          }
          if (effective_state.plane_name.empty()) {
            effective_state.plane_name = plan.plane_name;
          }
          if (effective_state.node_name.empty()) {
            effective_state.node_name = disk_node_name;
          }
          RemoveRealDiskMount(effective_state, runtime_root);
          removed = true;
          auto removed_state = effective_state;
          removed_state.runtime_state = "removed";
          removed_state.status_message = "managed disk detached and removed by hostd";
          removed_state.loop_device.clear();
          removed_state.mount_point = operation.details;
          if (runtime_root.has_value()) {
            removed_state.image_path.clear();
          }
          if (backend != nullptr) {
            backend->UpsertDiskRuntimeState(removed_state);
          }
        } else {
          RemoveDiskDirectory(operation.details, runtime_root);
          removed = runtime_root.has_value();
        }
        PrintOperationApplied(
            operation,
            removed ? "applied" : "skipped");
        break;
      }
      case comet::HostOperationKind::EnsureService:
      case comet::HostOperationKind::RemoveService:
        PrintOperationApplied(operation, "planned");
        break;
      case comet::HostOperationKind::WriteInferRuntimeConfig:
        PublishAssignmentProgress(
            backend,
            assignment_id,
            BuildAssignmentProgressPayload(
                "rendering-runtime",
                "Rendering runtime",
                "Writing infer runtime configuration.",
                84,
                desired_node_state.plane_name,
                plan.node_name));
        EnsureParentDirectory(operation.target);
        WriteTextFile(operation.target, comet::RenderInferRuntimeConfigJson(desired_node_state));
        PrintOperationApplied(operation, "applied");
        break;
      case comet::HostOperationKind::RemoveInferRuntimeConfig:
        RemoveFileIfExists(operation.target);
        PrintOperationApplied(operation, "applied");
        break;
      case comet::HostOperationKind::WriteComposeFile:
        PublishAssignmentProgress(
            backend,
            assignment_id,
            BuildAssignmentProgressPayload(
                "rendering-runtime",
                "Rendering runtime",
                "Writing docker compose plan for the node.",
                88,
                desired_node_state.plane_name,
                plan.node_name));
        WriteTextFile(operation.target, comet::RenderComposeYaml(compose_plan));
        PrintOperationApplied(operation, "applied");
        break;
      case comet::HostOperationKind::RemoveComposeFile:
        RemoveFileIfExists(operation.target);
        PrintOperationApplied(operation, "applied");
        break;
      case comet::HostOperationKind::ComposeUp:
        PublishAssignmentProgress(
            backend,
            assignment_id,
            BuildAssignmentProgressPayload(
                "starting-runtime",
                "Starting runtime",
                "Starting infer and worker services on the node.",
                92,
                desired_node_state.plane_name,
                plan.node_name));
        EnsureComposeImagesAvailable(compose_plan, compose_mode);
        RunComposeCommand(operation.target, "up -d", compose_mode);
        PrintOperationApplied(
            operation,
            compose_mode == ComposeMode::Exec ? "applied" : "skipped");
        break;
      case comet::HostOperationKind::ComposeDown:
        RunComposeCommand(operation.target, "down", compose_mode);
        PrintOperationApplied(
            operation,
            compose_mode == ComposeMode::Exec ? "applied" : "skipped");
        break;
    }
  };

  for (const auto& operation : plan.operations) {
    if (operation.kind == comet::HostOperationKind::ComposeDown ||
        operation.kind == comet::HostOperationKind::RemoveComposeFile) {
      apply_operation(operation);
    }
  }
  for (const auto& operation : plan.operations) {
    if (operation.kind == comet::HostOperationKind::ComposeDown ||
        operation.kind == comet::HostOperationKind::RemoveComposeFile ||
        operation.kind == comet::HostOperationKind::ComposeUp) {
      continue;
    }
    apply_operation(operation);
  }
  for (const auto& operation : plan.operations) {
    if (operation.kind == comet::HostOperationKind::ComposeUp) {
      apply_operation(operation);
    }
  }
}

void ShowDemoOps(
    const std::string& node_name,
    const std::string& storage_root,
    const std::optional<std::string>& runtime_root) {
  const comet::DesiredState state =
      RebaseStateForRuntimeRoot(comet::BuildDemoState(), storage_root, runtime_root);
  const auto plan = FindNodeExecutionPlan(
      comet::BuildNodeExecutionPlans(
          std::nullopt,
          state,
          DefaultArtifactsRoot()),
      node_name);

  std::cout << "hostd demo ops for node=" << plan.node_name << "\n";
  std::cout << comet::RenderNodeExecutionPlans({plan});
}

void ShowStateOps(
    const std::string& db_path,
    const std::string& node_name,
    const std::string& artifacts_root,
    const std::string& storage_root,
    const std::optional<std::string>& runtime_root,
    const std::string& state_root) {
  comet::ControllerStore store(db_path);
  store.Initialize();
  const auto state = store.LoadDesiredState();
  if (!state.has_value()) {
    throw std::runtime_error("no desired state found in db '" + db_path + "'");
  }

  const comet::DesiredState rebased_full_state =
      RebaseStateForRuntimeRoot(*state, storage_root, runtime_root);
  const comet::DesiredState desired_node_state =
      comet::SliceDesiredStateForNode(rebased_full_state, node_name);
  const auto desired_generation = store.LoadDesiredGeneration();

  std::cout << "db=" << db_path << "\n";
  ShowDesiredNodeOps(
      desired_node_state,
      artifacts_root,
      runtime_root,
      state_root,
      "hostd desired ops",
      desired_generation);
}

void ShowDesiredNodeOps(
    const comet::DesiredState& desired_node_state,
    const std::string& artifacts_root,
    const std::optional<std::string>& runtime_root,
    const std::string& state_root,
    const std::string& source_label,
    const std::optional<int>& desired_generation) {
  const std::string node_name = RequireSingleNodeName(desired_node_state);
  const auto current_local_state =
      LoadLocalAppliedState(state_root, node_name, desired_node_state.plane_name);
  const auto applied_generation =
      LoadLocalAppliedGeneration(state_root, node_name, desired_node_state.plane_name);
  const auto plan = ResolveNodeExecutionPlan(
      comet::BuildNodeExecutionPlans(current_local_state, desired_node_state, artifacts_root),
      current_local_state,
      desired_node_state,
      node_name,
      artifacts_root);

  std::cout << source_label << " for node=" << plan.node_name << "\n";
  std::cout << "artifacts_root=" << artifacts_root << "\n";
  std::cout << "state_path="
            << LocalPlaneStatePath(state_root, node_name, desired_node_state.plane_name) << "\n";
  if (desired_generation.has_value()) {
    std::cout << "desired_generation=" << *desired_generation << "\n";
  }
  if (applied_generation.has_value()) {
    std::cout << "applied_generation=" << *applied_generation << "\n";
  }
  if (runtime_root.has_value()) {
    std::cout << "runtime_root=" << *runtime_root << "\n";
  }
  if (const auto runtime_config_path = InferRuntimeConfigPathForNode(desired_node_state, node_name)) {
    std::cout << "infer_runtime_config=" << *runtime_config_path << "\n";
    std::cout << "infer_runtime_summary=" << RuntimeConfigSummary(desired_node_state) << "\n";
  }
  std::cout << comet::RenderNodeExecutionPlans({plan});
}

void ShowLocalState(
    const std::string& node_name,
    const std::string& state_root) {
  const auto local_state = LoadLocalAppliedState(state_root, node_name);
  if (!local_state.has_value()) {
    std::cout << "hostd local state for node=" << node_name << "\n";
    std::cout << "state_path=" << LocalStatePath(state_root, node_name) << "\n";
    const auto generation = LoadLocalAppliedGeneration(state_root, node_name);
    if (generation.has_value()) {
      std::cout << "applied_generation=" << *generation << "\n";
    }
    std::cout << "state: empty\n";
    return;
  }

  PrintLocalStateSummary(
      *local_state,
      LocalStatePath(state_root, node_name),
      node_name,
      LoadLocalAppliedGeneration(state_root, node_name));
}

void ShowRuntimeStatus(
    const std::string& node_name,
    const std::string& state_root) {
  const auto local_state = LoadLocalAppliedState(state_root, node_name);
  std::cout << "hostd runtime status for node=" << node_name << "\n";
  std::cout << "state_path=" << LocalStatePath(state_root, node_name) << "\n";
  if (!local_state.has_value()) {
    std::cout << "runtime_status: unavailable (no local applied state)\n";
    return;
  }

  const auto runtime_status = LoadLocalRuntimeStatus(state_root, node_name);
  if (!runtime_status.has_value()) {
    std::cout << "runtime_status: empty\n";
    return;
  }

  std::cout << "plane=" << runtime_status->plane_name << "\n";
  std::cout << "control_root=" << runtime_status->control_root << "\n";
  std::cout << "primary_infer_node=" << runtime_status->primary_infer_node << "\n";
  std::cout << "runtime_backend="
            << (runtime_status->runtime_backend.empty()
                    ? "(empty)"
                    : runtime_status->runtime_backend)
            << "\n";
  std::cout << "runtime_phase="
            << (runtime_status->runtime_phase.empty() ? "(empty)" : runtime_status->runtime_phase)
            << "\n";
  std::cout << "supervisor_pid=" << runtime_status->supervisor_pid << "\n";
  std::cout << "started_at="
            << FormatDisplayTimestamp(runtime_status->started_at)
            << "\n";
  std::cout << "enabled_gpu_nodes=" << runtime_status->enabled_gpu_nodes << "\n";
  std::cout << "registry_entries=" << runtime_status->registry_entries << "\n";
  std::cout << "active_model="
            << (runtime_status->active_model_id.empty()
                    ? "(empty)"
                    : runtime_status->active_model_id)
            << " served="
            << (runtime_status->active_served_model_name.empty()
                    ? "(empty)"
                    : runtime_status->active_served_model_name)
            << "\n";
  std::cout << "runtime_profile="
            << (runtime_status->active_runtime_profile.empty()
                    ? "(empty)"
                    : runtime_status->active_runtime_profile)
            << "\n";
  std::cout << "gateway_listen=" << runtime_status->gateway_listen << "\n";
  std::cout << "upstream_models_url=" << runtime_status->upstream_models_url << "\n";
  std::cout << "inference_health_url=" << runtime_status->inference_health_url << "\n";
  std::cout << "gateway_health_url=" << runtime_status->gateway_health_url << "\n";
  std::cout << "active_model_ready=" << (runtime_status->active_model_ready ? "yes" : "no")
            << "\n";
  std::cout << "gateway_plan_ready=" << (runtime_status->gateway_plan_ready ? "yes" : "no")
            << "\n";
  std::cout << "inference_ready=" << (runtime_status->inference_ready ? "yes" : "no") << "\n";
  std::cout << "gateway_ready=" << (runtime_status->gateway_ready ? "yes" : "no") << "\n";
  std::cout << "launch_ready=" << (runtime_status->launch_ready ? "yes" : "no") << "\n";
}

void ReportLocalObservedState(
    const std::optional<std::string>& db_path,
    const std::optional<std::string>& controller_url,
    const std::optional<std::string>& host_private_key_path,
    const std::optional<std::string>& controller_fingerprint,
    const std::string& node_name,
    const std::string& state_root) {
  auto backend = CreateHostdBackend(
      db_path,
      controller_url,
      host_private_key_path,
      controller_fingerprint);
  ReportObservedState(
      *backend,
      BuildObservedStateSnapshot(
          node_name,
          state_root,
          comet::HostObservationStatus::Idle,
          "manual heartbeat"),
      "hostd report-observed-state");
}

std::optional<comet::DiskSpec> FindDiskInStateByKey(
    const std::optional<comet::DesiredState>& state,
    const std::string& disk_key) {
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

std::pair<std::string, std::string> SplitDiskKey(const std::string& disk_key) {
  const auto at = disk_key.find('@');
  if (at == std::string::npos) {
    return {disk_key, ""};
  }
  return {disk_key.substr(0, at), disk_key.substr(at + 1)};
}

comet::DiskRuntimeState BuildDiskRuntimeState(
    const comet::DiskSpec& disk,
    const std::string& runtime_state,
    const std::string& status_message) {
  comet::DiskRuntimeState state;
  state.disk_name = disk.name;
  state.plane_name = disk.plane_name;
  state.node_name = disk.node_name;
  state.filesystem_type = "directory";
  state.mount_point = disk.host_path;
  state.runtime_state = runtime_state;
  state.status_message = status_message;
  return state;
}

comet::DiskRuntimeState EnsureDesiredDiskRuntimeState(
    const comet::DiskSpec& disk,
    const std::string& disk_key,
    const std::string& storage_root,
    const std::optional<std::string>& runtime_root) {
  if (HostCanManageRealDisks()) {
    const auto inspected_state = InspectRealDiskRuntime(disk, storage_root, runtime_root);
    if (inspected_state.runtime_state == "mounted") {
      return inspected_state;
    }
    if (inspected_state.runtime_state == "drifted") {
      throw std::runtime_error(
          "managed disk drift detected for '" + disk_key + "': " + inspected_state.status_message);
    }
    return EnsureRealDiskMount(disk, storage_root, runtime_root);
  }

  EnsureDiskDirectory(disk.host_path, disk_key);
  return BuildDiskRuntimeState(
      disk,
      "directory-backed-fallback",
      "real disk lifecycle unavailable; hostd is not running with root privileges");
}

void PersistDiskRuntimeStateForDesiredDisks(
    HostdBackend* backend,
    const comet::DesiredState& desired_node_state,
    const std::string& storage_root,
    const std::optional<std::string>& runtime_root,
    const std::string& status_message) {
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

void EnsureDesiredDisksReady(
    HostdBackend* backend,
    const comet::DesiredState& desired_node_state,
    const std::string& storage_root,
    const std::optional<std::string>& runtime_root) {
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

void PersistDiskRuntimeStateForRemovedDisks(
    HostdBackend* backend,
    const std::optional<comet::DesiredState>& previous_state,
    const comet::NodeExecutionPlan& execution_plan) {
  if (backend == nullptr) {
    return;
  }
  for (const auto& operation : execution_plan.operations) {
    if (operation.kind != comet::HostOperationKind::RemoveDisk) {
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

void RemoveRealDiskMount(
    const comet::DiskRuntimeState& runtime_state,
    const std::optional<std::string>& runtime_root) {
  std::optional<std::string> mounted_source;
  if (!runtime_state.mount_point.empty() && IsPathMounted(runtime_state.mount_point)) {
    mounted_source = CurrentMountSource(runtime_state.mount_point);
    if (!RunCommandOk(
            "/usr/bin/umount " + ShellQuote(runtime_state.mount_point) + " >/dev/null 2>&1")) {
      throw std::runtime_error(
          "failed to unmount managed disk at '" + runtime_state.mount_point + "'");
    }
  }

  std::optional<std::string> loop_device = runtime_state.loop_device;
  if (!loop_device.has_value() || loop_device->empty()) {
    if (mounted_source.has_value() &&
        mounted_source->rfind("/dev/loop", 0) == 0) {
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
      if (!RunCommandOk(
              "/usr/sbin/losetup -d " + ShellQuote(*loop_device) + " >/dev/null 2>&1")) {
        throw std::runtime_error(
            "failed to detach loop device '" + *loop_device + "'");
      }
    }
  }

  if (!runtime_state.mount_point.empty()) {
    RemoveDiskDirectory(runtime_state.mount_point, runtime_root);
  }

  if (!runtime_state.image_path.empty()) {
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

void ApplyDesiredNodeState(
    const comet::DesiredState& desired_node_state,
    const std::string& artifacts_root,
    const std::string& storage_root,
    const std::optional<std::string>& runtime_root,
    const std::string& state_root,
    ComposeMode compose_mode,
    const std::string& source_label,
    const std::optional<int>& desired_generation,
    const std::optional<int>& assignment_id,
    HostdBackend* backend) {
  const std::string node_name = RequireSingleNodeName(desired_node_state);
  const std::string& plane_name = desired_node_state.plane_name;
  const auto current_local_state = LoadLocalAppliedState(state_root, node_name, plane_name);
  const auto applied_generation = LoadLocalAppliedGeneration(state_root, node_name, plane_name);
  const auto execution_plan = ResolveNodeExecutionPlan(
      comet::BuildNodeExecutionPlans(current_local_state, desired_node_state, artifacts_root),
      current_local_state,
      desired_node_state,
      node_name,
      artifacts_root);
  const auto compose_plan = RequireNodeComposePlan(desired_node_state, node_name);

  std::cout << source_label << "\n";
  std::cout << "artifacts_root=" << artifacts_root << "\n";
  std::cout << "state_path=" << LocalPlaneStatePath(state_root, node_name, plane_name) << "\n";
  if (desired_generation.has_value()) {
    std::cout << "desired_generation=" << *desired_generation << "\n";
  }
  if (applied_generation.has_value()) {
    std::cout << "applied_generation=" << *applied_generation << "\n";
  }
  if (runtime_root.has_value()) {
    std::cout << "runtime_root=" << *runtime_root << "\n";
  }
  if (const auto runtime_config_path = InferRuntimeConfigPathForNode(desired_node_state, node_name)) {
    std::cout << "infer_runtime_config=" << *runtime_config_path << "\n";
    std::cout << "infer_runtime_summary=" << RuntimeConfigSummary(desired_node_state) << "\n";
  }
  std::cout << "compose_mode="
            << (compose_mode == ComposeMode::Exec ? "exec" : "skip") << "\n";

  ValidateDesiredNodeStateForCurrentHost(desired_node_state, compose_mode);

  if (execution_plan.operations.empty()) {
    std::cout << "no local changes for node=" << node_name << "\n";
    PersistDiskRuntimeStateForDesiredDisks(
        backend,
        desired_node_state,
        storage_root,
        runtime_root,
        "disk runtime verified by hostd");
    if (IsDesiredNodeStateEmpty(desired_node_state)) {
      RemoveLocalAppliedPlaneState(state_root, node_name, plane_name);
    } else {
      SaveLocalAppliedState(state_root, node_name, desired_node_state, plane_name);
      if (desired_generation.has_value()) {
        SaveLocalAppliedGeneration(state_root, node_name, *desired_generation, plane_name);
      }
    }
    RewriteAggregateLocalState(state_root, node_name);
    RewriteAggregateLocalGeneration(state_root, node_name);
    PublishAssignmentProgress(
        backend,
        assignment_id,
        BuildAssignmentProgressPayload(
            "completed",
            "Assignment complete",
            "No local changes were required for the node.",
            100,
            plane_name,
            node_name));
    return;
  }

  EnsureDesiredDisksReady(backend, desired_node_state, storage_root, runtime_root);
  BootstrapPlaneModelIfNeeded(desired_node_state, node_name, backend, assignment_id);

  ApplyNodePlan(
      execution_plan,
      desired_node_state,
      compose_plan,
      storage_root,
      runtime_root,
      compose_mode,
      assignment_id,
      backend);
  PersistDiskRuntimeStateForRemovedDisks(backend, current_local_state, execution_plan);
  PersistDiskRuntimeStateForDesiredDisks(
      backend,
      desired_node_state,
      storage_root,
      runtime_root,
      "disk runtime applied by hostd");
  if (IsDesiredNodeStateEmpty(desired_node_state)) {
    RemoveLocalAppliedPlaneState(state_root, node_name, plane_name);
    RewriteAggregateLocalState(state_root, node_name);
    RewriteAggregateLocalGeneration(state_root, node_name);
    return;
  }
  SaveLocalAppliedState(state_root, node_name, desired_node_state, plane_name);
  if (desired_generation.has_value()) {
    SaveLocalAppliedGeneration(state_root, node_name, *desired_generation, plane_name);
  }
  RewriteAggregateLocalState(state_root, node_name);
  RewriteAggregateLocalGeneration(state_root, node_name);
  if (compose_mode == ComposeMode::Exec) {
    PublishAssignmentProgress(
        backend,
        assignment_id,
        BuildAssignmentProgressPayload(
            "waiting-runtime-ready",
            "Waiting for runtime readiness",
            "Runtime was started; waiting for infer and worker observation to converge.",
            97,
            plane_name,
            node_name));
    if (NodeHasInferInstance(desired_node_state)) {
      WaitForLocalRuntimeStatus(state_root, node_name, plane_name, std::chrono::seconds(20));
    }
    WaitForLocalInstanceRuntimeStatuses(
        state_root,
        node_name,
        plane_name,
        ExpectedRuntimeStatusCountForNode(desired_node_state, node_name),
        std::chrono::seconds(20));
  }
  PublishAssignmentProgress(
      backend,
      assignment_id,
      BuildAssignmentProgressPayload(
          "completed",
          "Assignment complete",
          "Desired runtime state was applied on the node.",
          100,
          plane_name,
          node_name));
}

void ApplyStateOps(
    const std::string& db_path,
    const std::string& node_name,
    const std::string& artifacts_root,
    const std::string& storage_root,
    const std::optional<std::string>& runtime_root,
    const std::string& state_root,
    ComposeMode compose_mode) {
  comet::ControllerStore store(db_path);
  store.Initialize();
  LocalDbHostdBackend backend(db_path);
  const auto state = store.LoadDesiredState();
  if (!state.has_value()) {
    throw std::runtime_error("no desired state found in db '" + db_path + "'");
  }
  const auto desired_generation = store.LoadDesiredGeneration();

  const comet::DesiredState rebased_full_state =
      RebaseStateForRuntimeRoot(*state, storage_root, runtime_root);
  const comet::DesiredState desired_node_state =
      comet::SliceDesiredStateForNode(rebased_full_state, node_name);

  std::cout << "db=" << db_path << "\n";
  try {
    ApplyDesiredNodeState(
        desired_node_state,
        artifacts_root,
        storage_root,
        runtime_root,
        state_root,
        compose_mode,
        "hostd apply-state-ops",
        desired_generation,
        std::nullopt,
        &backend);
    ReportObservedState(
        backend,
        BuildObservedStateSnapshot(
            node_name,
            state_root,
            comet::HostObservationStatus::Applied,
            desired_generation.has_value()
                ? "applied desired generation " + std::to_string(*desired_generation)
                : "applied desired state"),
        "hostd observed-state-update");
  } catch (const std::exception& error) {
    ReportObservedState(
        backend,
        BuildObservedStateSnapshot(
            node_name,
            state_root,
            comet::HostObservationStatus::Failed,
            error.what()),
        "hostd observed-state-update");
    throw;
  }
}

void ApplyNextAssignment(
    const std::optional<std::string>& db_path,
    const std::optional<std::string>& controller_url,
    const std::optional<std::string>& host_private_key_path,
    const std::optional<std::string>& controller_fingerprint,
    const std::string& node_name,
    const std::string& storage_root,
    const std::optional<std::string>& runtime_root,
    const std::string& state_root,
    ComposeMode compose_mode) {
  auto backend = CreateHostdBackend(
      db_path,
      controller_url,
      host_private_key_path,
      controller_fingerprint);
  const auto assignment = backend->ClaimNextHostAssignment(node_name);
  if (!assignment.has_value()) {
    std::cout << "no pending assignments for node=" << node_name << "\n";
    return;
  }

  std::cout << "hostd apply-next-assignment\n";
  if (controller_url.has_value()) {
    std::cout << "controller=" << *controller_url << "\n";
  } else {
    std::cout << "db=" << db_path.value_or(DefaultDbPath()) << "\n";
  }
  PrintAssignmentSummary(*assignment);
  if (runtime_root.has_value()) {
    std::cout << "runtime_root=" << *runtime_root << "\n";
  }
  std::cout << "state_root=" << state_root << "\n";
  std::cout << "compose_mode="
            << (compose_mode == ComposeMode::Exec ? "exec" : "skip") << "\n";
  const std::string assignment_context =
      assignment->status_message.empty() ? "" : " [" + assignment->status_message + "]";

  try {
    if (assignment->assignment_type != "apply-node-state" &&
        assignment->assignment_type != "drain-node-state" &&
        assignment->assignment_type != "stop-plane-state" &&
        assignment->assignment_type != "delete-plane-state" &&
        assignment->assignment_type != "evict-workers") {
      throw std::runtime_error(
          "unsupported assignment type '" + assignment->assignment_type + "'");
    }

    const comet::DesiredState rebased_state = RebaseStateForRuntimeRoot(
        comet::DeserializeDesiredStateJson(assignment->desired_state_json),
        storage_root,
        runtime_root);
    const bool is_drain_assignment = assignment->assignment_type == "drain-node-state";
    const bool is_stop_assignment = assignment->assignment_type == "stop-plane-state";
    const bool is_delete_assignment = assignment->assignment_type == "delete-plane-state";
    const bool is_eviction_assignment = assignment->assignment_type == "evict-workers";
    const auto victim_names =
        is_eviction_assignment ? ParseTaggedCsv(assignment->status_message, "victims")
                               : std::vector<std::string>{};
    const auto victim_host_pids =
        is_eviction_assignment ? CaptureServiceHostPids(victim_names)
                               : std::map<std::string, int>{};
    const std::string applying_status_message =
        (is_drain_assignment ? "draining node for desired generation "
                             : (is_stop_assignment
                                    ? "stopping plane state for desired generation "
                             : (is_delete_assignment
                                    ? "deleting plane state for desired generation "
                             : (is_eviction_assignment
                                    ? "evicting rollout workers for desired generation "
                                    : "applying desired generation ")))) +
        std::to_string(assignment->desired_generation) + assignment_context;
    const std::string apply_trace_label =
        is_drain_assignment
            ? "hostd drain-assignment-ops"
            : (is_stop_assignment
                   ? "hostd stop-plane-assignment-ops"
                   : (is_delete_assignment
                          ? "hostd delete-plane-assignment-ops"
                          : (is_eviction_assignment
                                 ? "hostd eviction-assignment-ops"
                                 : "hostd apply-assignment-ops")));
    ReportObservedState(
        *backend,
        BuildObservedStateSnapshot(
            node_name,
            state_root,
            comet::HostObservationStatus::Applying,
            applying_status_message,
            assignment->id),
        "hostd observed-state-update");
    ApplyDesiredNodeState(
        rebased_state,
        assignment->artifacts_root,
        storage_root,
        runtime_root,
        state_root,
        compose_mode,
        apply_trace_label,
        assignment->desired_generation,
        assignment->id,
        backend.get());
    if (is_eviction_assignment &&
        compose_mode == ComposeMode::Exec &&
        !VerifyEvictionAssignment(
            rebased_state,
            node_name,
            state_root,
            assignment->status_message,
            victim_host_pids)) {
      throw std::runtime_error(
          "eviction verification timed out; gpu resources were not released");
    }
    ReportObservedState(
        *backend,
        BuildObservedStateSnapshot(
            node_name,
            state_root,
            comet::HostObservationStatus::Applied,
            (is_drain_assignment ? "drained node for desired generation "
                                 : (is_stop_assignment
                                        ? "stopped plane state for desired generation "
                                 : (is_eviction_assignment
                                        ? "evicted rollout workers for desired generation "
                                        : "applied desired generation "))) +
                std::to_string(assignment->desired_generation) + assignment_context,
            assignment->id),
        "hostd observed-state-update");
    if (!backend->TransitionClaimedHostAssignment(
        assignment->id,
        comet::HostAssignmentStatus::Applied,
        (is_drain_assignment ? "drained node for desired generation "
                             : (is_stop_assignment
                                    ? "stopped plane state for desired generation "
                             : (is_eviction_assignment
                                    ? "evicted rollout workers for desired generation "
                                    : "applied desired generation "))) +
            std::to_string(assignment->desired_generation) +
            assignment_context +
            " on attempt " + std::to_string(assignment->attempt_count) + "/" +
            std::to_string(assignment->max_attempts))) {
      std::cout << "assignment transition skipped for id=" << assignment->id
                << " because it is no longer claimed\n";
    }
    AppendHostdEvent(
        *backend,
        "host-assignment",
        "applied",
        "applied assignment on node " + node_name,
        json{
            {"assignment_type", assignment->assignment_type},
            {"desired_generation", assignment->desired_generation},
            {"attempt_count", assignment->attempt_count},
            {"max_attempts", assignment->max_attempts},
        },
        assignment->plane_name,
        node_name,
        "",
        assignment->id);
  } catch (const std::exception& error) {
    const std::string error_message = error.what();
    PublishAssignmentProgress(
        backend.get(),
        assignment->id,
        BuildAssignmentProgressPayload(
            "failed",
            "Assignment failed",
            error_message,
            100,
            assignment->plane_name,
            node_name));
    ReportObservedState(
        *backend,
        BuildObservedStateSnapshot(
            node_name,
            state_root,
            comet::HostObservationStatus::Failed,
            error_message,
            assignment->id),
        "hostd observed-state-update");
    if (assignment->attempt_count < assignment->max_attempts) {
      if (!backend->TransitionClaimedHostAssignment(
              assignment->id,
              comet::HostAssignmentStatus::Pending,
              "attempt " + std::to_string(assignment->attempt_count) + "/" +
                  std::to_string(assignment->max_attempts) + " failed: " +
                  error_message + assignment_context)) {
        std::cout << "assignment retry transition skipped for id=" << assignment->id
                  << " because it is no longer claimed\n";
      }
    } else {
      if (!backend->TransitionClaimedHostAssignment(
              assignment->id,
              comet::HostAssignmentStatus::Failed,
              "attempt " + std::to_string(assignment->attempt_count) + "/" +
                  std::to_string(assignment->max_attempts) + " exhausted: " +
                  error_message + assignment_context)) {
        std::cout << "assignment failure transition skipped for id=" << assignment->id
                  << " because it is no longer claimed\n";
      }
    }
    AppendHostdEvent(
        *backend,
        "host-assignment",
        "failed",
        error_message,
        json{
            {"assignment_type", assignment->assignment_type},
            {"desired_generation", assignment->desired_generation},
            {"attempt_count", assignment->attempt_count},
            {"max_attempts", assignment->max_attempts},
        },
        assignment->plane_name,
        node_name,
        "",
        assignment->id,
        std::nullopt,
        "error");
    throw;
  }
}

}  // namespace

int main(int argc, char** argv) {
  if (argc < 2) {
    PrintUsage();
    return 1;
  }

  const std::string command = argv[1];
  const auto node_name = ParseNodeArg(argc, argv);
  const auto config_path = ParseConfigArg(argc, argv);
  const auto controller_url = ParseControllerArg(argc, argv);
  const auto host_private_key_path = ParseHostPrivateKeyArg(argc, argv);
  if (!node_name.has_value()) {
    std::cerr << "error: --node is required\n";
    return 1;
  }

  try {
    const CometNodeConfig node_config = LoadCometNodeConfig(config_path, argv[0]);

    if (command == "show-demo-ops") {
      ShowDemoOps(*node_name, node_config.storage_root, ParseRuntimeRootArg(argc, argv));
      return 0;
    }

    if (command == "show-state-ops") {
      ShowStateOps(
          ResolveDbPath(ParseDbArg(argc, argv)),
          *node_name,
          ResolveArtifactsRoot(ParseArtifactsRootArg(argc, argv)),
          node_config.storage_root,
          ParseRuntimeRootArg(argc, argv),
          ResolveStateRoot(ParseStateRootArg(argc, argv)));
      return 0;
    }

    if (command == "show-local-state") {
      ShowLocalState(*node_name, ResolveStateRoot(ParseStateRootArg(argc, argv)));
      return 0;
    }

    if (command == "show-runtime-status") {
      ShowRuntimeStatus(*node_name, ResolveStateRoot(ParseStateRootArg(argc, argv)));
      return 0;
    }

    if (command == "report-observed-state") {
      ReportLocalObservedState(
          ParseDbArg(argc, argv),
          controller_url,
          host_private_key_path,
          ParseControllerFingerprintArg(argc, argv),
          *node_name,
          ResolveStateRoot(ParseStateRootArg(argc, argv)));
      return 0;
    }

    if (command == "apply-state-ops") {
      ApplyStateOps(
          ResolveDbPath(ParseDbArg(argc, argv)),
          *node_name,
          ResolveArtifactsRoot(ParseArtifactsRootArg(argc, argv)),
          node_config.storage_root,
          ParseRuntimeRootArg(argc, argv),
          ResolveStateRoot(ParseStateRootArg(argc, argv)),
          ResolveComposeMode(ParseComposeModeArg(argc, argv)));
      return 0;
    }

    if (command == "apply-next-assignment") {
      ApplyNextAssignment(
          ParseDbArg(argc, argv),
          controller_url,
          host_private_key_path,
          ParseControllerFingerprintArg(argc, argv),
          *node_name,
          node_config.storage_root,
          ParseRuntimeRootArg(argc, argv),
          ResolveStateRoot(ParseStateRootArg(argc, argv)),
          ResolveComposeMode(ParseComposeModeArg(argc, argv)));
      return 0;
    }
  } catch (const std::exception& error) {
    std::cerr << "error: " << error.what() << "\n";
    return 1;
  }

  PrintUsage();
  return 1;
}
