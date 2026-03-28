#include "app/hostd_app.h"

#include "app/hostd_bootstrap_model_support.h"
#include "app/hostd_composition_root.h"
#include "app/hostd_controller_transport_support.h"
#include "app/hostd_local_state_support.h"
#include "app/hostd_telemetry_support.h"
#include "backend/hostd_backend.h"
#include "backend/hostd_backend_factory.h"
#include "backend/http_hostd_backend_support.h"
#include "backend/local_db_hostd_backend.h"
#include "cli/hostd_cli.h"
#include "cli/hostd_command_line.h"
#include "config/node_config_loader.h"
#include "observation/hostd_observation_service.h"
#include "state_apply/hostd_assignment_service.h"

#include <chrono>
#include <ctime>
#include <cctype>
#include <cstdio>
#include <cstring>
#include <errno.h>
#include <filesystem>
#include <fstream>
#include <exception>
#include <iomanip>
#include <iostream>
#include <map>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <cstdlib>
#include <sstream>
#include <set>
#include <thread>
#include <future>
#include <vector>
#include <array>
#include <algorithm>

#if !defined(_WIN32)
#include <dlfcn.h>
#include <sys/statvfs.h>
#endif

#include <nlohmann/json.hpp>
#include <sodium.h>

#include "comet/planning/compose_renderer.h"
#include "comet/security/crypto_utils.h"
#include "comet/state/demo_state.h"
#include "comet/planning/execution_plan.h"
#include "comet/runtime/infer_runtime_config.h"
#include "comet/state/models.h"
#include "comet/core/platform_compat.h"
#include "comet/planning/planner.h"
#include "comet/runtime/runtime_status.h"
#include "comet/state/sqlite_store.h"
#include "comet/state/state_json.h"

namespace {

using nlohmann::json;
using ComposeMode = comet::hostd::ComposeMode;
using HostdBackend = comet::hostd::HostdBackend;

constexpr const char* kDefaultManagedStorageRoot = "/var/lib/comet";

std::string DefaultArtifactsRoot() {
  return (std::filesystem::path("var") / "artifacts").string();
}

std::string ResolvedDockerCommand();
std::string ShellQuote(const std::string& value);
bool RunCommandOk(const std::string& command);
std::vector<comet::RuntimeProcessStatus> LoadLocalInstanceRuntimeStatuses(
    const std::string& state_root,
    const std::string& node_name,
    const std::optional<std::string>& plane_name = std::nullopt);
std::string SharedDiskHostPathForContainerPath(
    const comet::DiskSpec& shared_disk,
    const std::string& container_path,
    const std::string& fallback_relative_path);

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
        disk.kind == comet::DiskKind::WorkerPrivate ||
        disk.kind == comet::DiskKind::AppPrivate;
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
      disk.kind == comet::DiskKind::WorkerPrivate ||
      disk.kind == comet::DiskKind::AppPrivate;
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

std::string LocalPlaneRoot(
    const std::string& state_root,
    const std::string& node_name,
    const std::string& plane_name) {
  return comet::hostd::local_state_support::LocalPlaneRoot(state_root, node_name, plane_name);
}

std::optional<int> LoadLocalAppliedGeneration(
    const std::string& state_root,
    const std::string& node_name,
    const std::optional<std::string>& plane_name = std::nullopt) {
  return comet::hostd::local_state_support::LoadLocalAppliedGeneration(
      state_root,
      node_name,
      plane_name);
}

void SaveLocalAppliedGeneration(
    const std::string& state_root,
    const std::string& node_name,
    int generation,
    const std::optional<std::string>& plane_name = std::nullopt) {
  comet::hostd::local_state_support::SaveLocalAppliedGeneration(
      state_root,
      node_name,
      generation,
      plane_name);
}

std::string RunCommandCapture(const std::string& command);

bool ComposeProjectHasContainers(const std::string& compose_file_path) {
  const std::string command =
      ResolvedDockerCommand() + " compose -f '" + compose_file_path + "' ps -a --quiet 2>/dev/null";
  const std::string output = RunCommandCapture(command);
  return output.find_first_not_of(" \t\r\n") != std::string::npos;
}

std::string PlaneMeshNetworkName(const std::string& plane_name) {
  return "comet-" + plane_name + "-mesh";
}

void EnsureComposeMeshNetworkAvailable(
    const comet::NodeComposePlan& compose_plan,
    ComposeMode compose_mode) {
  if (compose_mode != ComposeMode::Exec) {
    return;
  }
  const std::string network_name = PlaneMeshNetworkName(compose_plan.plane_name);
  const std::string inspect_command =
      ResolvedDockerCommand() + " network inspect " + ShellQuote(network_name) + " >/dev/null 2>&1";
  if (RunCommandOk(inspect_command)) {
    return;
  }
  const std::string create_command =
      ResolvedDockerCommand() + " network create " + ShellQuote(network_name) + " >/dev/null";
  if (!RunCommandOk(create_command)) {
    throw std::runtime_error("failed to create compose mesh network: " + network_name);
  }
}

bool IsSharedManagedDiskImagePath(const std::string& image_path) {
  return image_path.find("/disk-images/") != std::string::npos &&
         image_path.find("/shared/") != std::string::npos;
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
    if (subcommand == "down" && !ComposeProjectHasContainers(compose_file_path)) {
      return;
    }
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
  return comet::hostd::local_state_support::LocalStatePath(state_root, node_name);
}

std::string LocalPlaneStatePath(
    const std::string& state_root,
    const std::string& node_name,
    const std::string& plane_name) {
  return comet::hostd::local_state_support::LocalPlaneStatePath(
      state_root,
      node_name,
      plane_name);
}

std::optional<comet::DesiredState> LoadLocalAppliedState(
    const std::string& state_root,
    const std::string& node_name,
    const std::optional<std::string>& plane_name = std::nullopt) {
  return comet::hostd::local_state_support::LoadLocalAppliedState(
      state_root,
      node_name,
      plane_name);
}

void RewriteAggregateLocalState(const std::string& state_root, const std::string& node_name) {
  comet::hostd::local_state_support::RewriteAggregateLocalState(state_root, node_name);
}

void RewriteAggregateLocalGeneration(
    const std::string& state_root,
    const std::string& node_name) {
  comet::hostd::local_state_support::RewriteAggregateLocalGeneration(state_root, node_name);
}

std::optional<comet::RuntimeStatus> LoadLocalRuntimeStatus(
    const std::string& state_root,
    const std::string& node_name,
    const std::optional<std::string>& plane_name = std::nullopt) {
  return comet::hostd::local_state_support::LoadLocalRuntimeStatus(
      state_root,
      node_name,
      [](const comet::DesiredState& state, const std::string& current_node_name) {
        return RuntimeStatusPathForNode(state, current_node_name);
      },
      plane_name);
}

void SaveLocalAppliedState(
    const std::string& state_root,
    const std::string& node_name,
    const comet::DesiredState& state,
    const std::optional<std::string>& plane_name = std::nullopt) {
  comet::hostd::local_state_support::SaveLocalAppliedState(
      state_root,
      node_name,
      state,
      plane_name);
}

void RemoveLocalAppliedPlaneState(
    const std::string& state_root,
    const std::string& node_name,
    const std::string& plane_name) {
  comet::hostd::local_state_support::RemoveLocalAppliedPlaneState(
      state_root,
      node_name,
      plane_name);
}

void WaitForLocalRuntimeStatus(
    const std::string& state_root,
    const std::string& node_name,
    const std::optional<std::string>& plane_name,
    std::chrono::seconds timeout) {
  comet::hostd::local_state_support::WaitForLocalRuntimeStatus(
      state_root,
      node_name,
      [](const comet::DesiredState& state, const std::string& current_node_name) {
        return RuntimeStatusPathForNode(state, current_node_name);
      },
      plane_name,
      timeout);
}

std::size_t ExpectedRuntimeStatusCountForNode(
    const comet::DesiredState& desired_node_state,
    const std::string& node_name) {
  return comet::hostd::local_state_support::ExpectedRuntimeStatusCountForNode(
      desired_node_state,
      node_name);
}

void WaitForLocalInstanceRuntimeStatuses(
    const std::string& state_root,
    const std::string& node_name,
    const std::optional<std::string>& plane_name,
    std::size_t expected_count,
    std::chrono::seconds timeout) {
  comet::hostd::local_state_support::WaitForLocalInstanceRuntimeStatuses(
      state_root,
      node_name,
      [](const std::string& current_state_root,
         const std::string& current_node_name,
         const std::optional<std::string>& current_plane_name) {
        return LoadLocalInstanceRuntimeStatuses(
            current_state_root,
            current_node_name,
            current_plane_name);
      },
      plane_name,
      expected_count,
      timeout);
}

void PrintLocalStateSummary(
    const comet::DesiredState& state,
    const std::string& state_path,
    const std::string& node_name,
    const std::optional<int>& generation) {
  comet::hostd::local_state_support::PrintLocalStateSummary(
      state,
      state_path,
      node_name,
      generation);
}

std::string RequireSingleNodeName(const comet::DesiredState& state) {
  return comet::hostd::local_state_support::RequireSingleNodeName(state);
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

std::string RunCommandCapture(const std::string& command) {
  std::array<char, 512> buffer{};
  std::string output;
  FILE* pipe = comet::platform::OpenPipe(command.c_str(), "r");
  if (pipe == nullptr) {
    return output;
  }
  while (fgets(buffer.data(), static_cast<int>(buffer.size()), pipe) != nullptr) {
    output.append(buffer.data());
  }
  comet::platform::ClosePipe(pipe);
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
    if (instance.node_name == node_name &&
        (instance.role == comet::InstanceRole::Worker ||
         (instance.gpu_device.has_value() && !instance.gpu_device->empty()))) {
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

  const std::string executable_path = comet::platform::ExecutablePath();
  if (executable_path.empty()) {
    return std::nullopt;
  }
  return FindRepoRootFromPath(std::filesystem::path(executable_path).parent_path());
}

std::optional<std::filesystem::path> ResolvePlaneOwnedPath(
    const comet::DesiredState& state,
    const std::string& relative_path) {
  if (relative_path.empty()) {
    return std::nullopt;
  }

  const std::filesystem::path input(relative_path);
  if (input.is_absolute()) {
    if (std::filesystem::exists(input)) {
      return input.lexically_normal();
    }
    return std::nullopt;
  }

  std::vector<std::filesystem::path> candidates;
  try {
    candidates.push_back(std::filesystem::current_path() / input);
  } catch (...) {
  }

  if (const auto comet_repo_root = DetectCometRepoRoot(); comet_repo_root.has_value()) {
    candidates.push_back(*comet_repo_root / input);
    candidates.push_back(comet_repo_root->parent_path() / state.plane_name / input);
  }

  for (const auto& candidate : candidates) {
    std::error_code error;
    if (std::filesystem::exists(candidate, error) && !error) {
      return candidate.lexically_normal();
    }
  }
  return std::nullopt;
}

bool NodeHasAppInstance(
    const comet::DesiredState& desired_node_state,
    const std::string& node_name) {
  for (const auto& instance : desired_node_state.instances) {
    if (instance.node_name == node_name && instance.role == comet::InstanceRole::App) {
      return true;
    }
  }
  return false;
}

bool ShouldRunPostDeployScript(
    const comet::DesiredState& desired_node_state,
    const std::string& node_name) {
  if (!desired_node_state.post_deploy_script.has_value() ||
      desired_node_state.post_deploy_script->empty()) {
    return false;
  }
  if (!desired_node_state.inference.primary_infer_node.empty()) {
    return desired_node_state.inference.primary_infer_node == node_name;
  }
  if (NodeHasAppInstance(desired_node_state, node_name)) {
    return true;
  }
  return !desired_node_state.nodes.empty() && desired_node_state.nodes.front().name == node_name;
}

json BuildAssignmentProgressPayload(
    const std::string& phase,
    const std::string& title,
    const std::string& detail,
    int percent,
    const std::string& plane_name,
    const std::string& node_name,
    const std::optional<std::uintmax_t>& bytes_done,
    const std::optional<std::uintmax_t>& bytes_total);

void PublishAssignmentProgress(
    HostdBackend* backend,
    const std::optional<int>& assignment_id,
    const json& progress);

std::string TailTextFile(const std::string& path, std::size_t max_bytes = 4096) {
  std::ifstream input(path, std::ios::binary);
  if (!input.is_open()) {
    return {};
  }
  input.seekg(0, std::ios::end);
  const auto size = static_cast<std::size_t>(input.tellg());
  const auto read_size = std::min(size, max_bytes);
  input.seekg(static_cast<std::streamoff>(size - read_size), std::ios::beg);
  std::string text(read_size, '\0');
  input.read(text.data(), static_cast<std::streamsize>(read_size));
  return text;
}

void RunPostDeployScriptIfNeeded(
    const comet::DesiredState& desired_node_state,
    const std::string& node_name,
    const std::string& artifacts_root,
    const std::string& storage_root,
    const std::optional<std::string>& runtime_root,
    const std::string& state_root,
    const std::optional<int>& desired_generation,
    const std::optional<int>& assignment_id,
    HostdBackend* backend) {
  if (!ShouldRunPostDeployScript(desired_node_state, node_name)) {
    return;
  }

  const auto script_path =
      ResolvePlaneOwnedPath(desired_node_state, *desired_node_state.post_deploy_script);
  if (!script_path.has_value()) {
    throw std::runtime_error(
        "post_deploy_script was configured but could not be resolved: " +
        *desired_node_state.post_deploy_script);
  }

  const std::filesystem::path plane_root(LocalPlaneRoot(
      state_root,
      node_name,
      desired_node_state.plane_name));
  std::filesystem::create_directories(plane_root);
  const std::string log_path = (plane_root / "post-deploy.log").string();

  PublishAssignmentProgress(
      backend,
      assignment_id,
      BuildAssignmentProgressPayload(
          "running-post-deploy",
          "Running post-deploy hook",
          "Executing plane post_deploy_script after runtime readiness.",
          99,
          desired_node_state.plane_name,
          node_name,
          std::nullopt,
          std::nullopt));

  std::ostringstream command;
  command << "cd " << ShellQuote(script_path->parent_path().string()) << " && "
          << "COMET_PLANE_NAME=" << ShellQuote(desired_node_state.plane_name) << " "
          << "COMET_NODE_NAME=" << ShellQuote(node_name) << " "
          << "COMET_ARTIFACTS_ROOT=" << ShellQuote(artifacts_root) << " "
          << "COMET_STORAGE_ROOT=" << ShellQuote(storage_root) << " "
          << "COMET_STATE_ROOT=" << ShellQuote(state_root) << " "
          << "COMET_RUNTIME_ROOT="
          << ShellQuote(runtime_root.has_value() ? *runtime_root : std::string()) << " "
          << "COMET_POST_DEPLOY_LOG=" << ShellQuote(log_path) << " "
          << "COMET_DESIRED_GENERATION="
          << ShellQuote(desired_generation.has_value()
                            ? std::to_string(*desired_generation)
                            : std::string()) << " "
          << "COMET_ASSIGNMENT_ID="
          << ShellQuote(
                 assignment_id.has_value() ? std::to_string(*assignment_id) : std::string())
          << " "
          << ShellQuote(script_path->string()) << " >" << ShellQuote(log_path) << " 2>&1";
  const int rc = std::system(command.str().c_str());
  if (rc != 0) {
    const std::string tail = TailTextFile(log_path);
    throw std::runtime_error(
        "post_deploy_script failed with exit code " + std::to_string(rc) +
        (tail.empty() ? std::string()
                      : std::string(": ") + Trim(tail)));
  }
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

comet::hostd::HostdBootstrapModelSupport MakeHostdBootstrapModelSupport() {
  return comet::hostd::HostdBootstrapModelSupport(
      comet::hostd::HostdBootstrapModelSupport::Deps{
          [](const comet::DesiredState& state, const std::string& node_name)
              -> const comet::DiskSpec* {
            for (const auto& disk : state.disks) {
              if (disk.node_name == node_name &&
                  disk.kind == comet::DiskKind::PlaneShared) {
                return &disk;
              }
            }
            return nullptr;
          },
          [](const comet::DiskSpec& shared_disk,
              const std::string& container_path,
              const std::string& fallback_relative_path) {
            return SharedDiskHostPathForContainerPath(
                shared_disk,
                container_path,
                fallback_relative_path);
          },
          [](const comet::DesiredState& state,
              const std::string& node_name,
              const std::string& filename) {
            return ControlFilePathForNode(state, node_name, filename);
          },
          [](const comet::DesiredState& state) {
            return RequireSingleNodeName(state);
          },
          [](const std::string& command) {
            return RunCommandCapture(command);
          },
          [](const std::string& value) {
            return ShellQuote(value);
          },
          [](const std::string& value) {
            return NormalizeLowercase(value);
          },
          [](const std::string& value) {
            return Trim(value);
          },
          [](const std::string& path, const std::string& contents) {
            WriteTextFile(path, contents);
          },
          [](const std::string& path) {
            RemoveFileIfExists(path);
          },
          [](const std::string& path) {
            EnsureParentDirectory(path);
          },
          [](const std::string& phase,
              const std::string& title,
              const std::string& detail,
              int percent,
              const std::string& plane_name,
              const std::string& node_name,
              const std::optional<std::uintmax_t>& bytes_done,
              const std::optional<std::uintmax_t>& bytes_total) {
            return BuildAssignmentProgressPayload(
                phase,
                title,
                detail,
                percent,
                plane_name,
                node_name,
                bytes_done,
                bytes_total);
          },
          [](HostdBackend* backend,
              const std::optional<int>& assignment_id,
              const json& progress) {
            PublishAssignmentProgress(backend, assignment_id, progress);
          },
      });
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

bool RunCommandOk(const std::string& command) {
  return std::system(command.c_str()) == 0;
}

bool HostCanManageRealDisks() {
  return comet::platform::HasElevatedPrivileges();
}

std::string NormalizeManagedPath(const std::string& path) {
  std::error_code error;
  const auto normalized = std::filesystem::weakly_canonical(path, error);
  if (!error) {
    return normalized.string();
  }
  return std::filesystem::path(path).lexically_normal().string();
}

std::string NormalizeLoopImagePath(const std::string& image_path) {
  return NormalizeManagedPath(image_path);
}

std::string NormalizeMountPointPath(const std::string& mount_point) {
  return NormalizeManagedPath(mount_point);
}

std::optional<std::string> DetectExistingLoopDevice(const std::string& image_path) {
  const std::array<std::string, 2> candidates = {
      image_path,
      NormalizeLoopImagePath(image_path),
  };
  for (const auto& candidate : candidates) {
    if (candidate.empty()) {
      continue;
    }
    const std::string output =
        RunCommandCapture("/usr/sbin/losetup -j " + ShellQuote(candidate) + " 2>/dev/null || true");
    const std::string trimmed = Trim(output);
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

std::string RequireLoopDeviceForImage(const std::string& image_path) {
  if (const auto existing = DetectExistingLoopDevice(image_path); existing.has_value()) {
    return *existing;
  }
  const std::string attach_path = NormalizeLoopImagePath(image_path);
  const std::string output =
      RunCommandCapture(
          "/usr/sbin/losetup --find --show " + ShellQuote(attach_path) + " 2>/dev/null");
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
  if (RunCommandOk(
          "/usr/bin/mountpoint -q " + ShellQuote(mount_point) + " >/dev/null 2>&1")) {
    return true;
  }
  const std::string normalized_mount_point = NormalizeMountPointPath(mount_point);
  return normalized_mount_point != mount_point &&
         RunCommandOk(
             "/usr/bin/mountpoint -q " + ShellQuote(normalized_mount_point) +
             " >/dev/null 2>&1");
}

std::optional<std::string> CurrentMountSource(const std::string& mount_point) {
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

  const bool image_preexisting = std::filesystem::exists(runtime_state.image_path);
  CreateSparseImageFile(runtime_state.image_path, disk.size_gb);
  runtime_state.runtime_state = "image-created";

  runtime_state.loop_device = RequireLoopDeviceForImage(runtime_state.image_path);
  runtime_state.attached_at = "attached";
  runtime_state.runtime_state = "attached";

  runtime_state.filesystem_type = DetectFilesystemTypeForDevice(runtime_state.loop_device);
  if (runtime_state.filesystem_type.empty() && image_preexisting &&
      disk.kind == comet::DiskKind::PlaneShared) {
    for (int attempt = 0; attempt < 15; ++attempt) {
      std::this_thread::sleep_for(std::chrono::seconds(2));
      runtime_state.filesystem_type = DetectFilesystemTypeForDevice(runtime_state.loop_device);
      if (!runtime_state.filesystem_type.empty()) {
        break;
      }
    }
  }
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

void PublishAssignmentProgress(
    HostdBackend* backend,
    const std::optional<int>& assignment_id,
    const json& progress) {
  if (backend == nullptr || !assignment_id.has_value()) {
    return;
  }
  backend->UpdateHostAssignmentProgress(*assignment_id, progress);
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

  std::vector<comet::RuntimeProcessStatus> LoadLocalInstanceRuntimeStatuses(
    const std::string& state_root,
    const std::string& node_name,
    const std::optional<std::string>& plane_name) {
  return comet::hostd::telemetry_support::LoadLocalInstanceRuntimeStatuses(
      state_root,
      node_name,
      plane_name);
}

std::vector<std::string> ParseTaggedCsv(
    const std::string& text,
    const std::string& key) {
  return comet::hostd::telemetry_support::ParseTaggedCsv(text, key);
}

std::map<std::string, int> CaptureServiceHostPids(const std::vector<std::string>& service_names) {
  return comet::hostd::telemetry_support::CaptureServiceHostPids(service_names);
}

bool VerifyEvictionAssignment(
    const comet::DesiredState& local_state,
    const std::string& node_name,
    const std::string& state_root,
    const std::string& status_message,
    const std::map<std::string, int>& victim_host_pids) {
  return comet::hostd::telemetry_support::VerifyEvictionAssignment(
      local_state,
      node_name,
      state_root,
      status_message,
      victim_host_pids);
}

comet::HostObservation BuildObservedStateSnapshot(
    const std::string& node_name,
    const std::string& state_root,
    comet::HostObservationStatus status,
    const std::string& status_message,
    const std::optional<int>& last_assignment_id = std::nullopt) {
  return comet::hostd::telemetry_support::BuildObservedStateSnapshot(
      node_name,
      state_root,
      status,
      status_message,
      last_assignment_id);
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
        const auto disk_key = SplitDiskKey(operation.target);
        const std::string& disk_name = disk_key.first;
        const std::string& disk_node_name = disk_key.second;
        const auto runtime_state =
            backend == nullptr ? std::nullopt
                               : backend->LoadDiskRuntimeState(disk_name, disk_node_name);
        const bool is_plane_shared_disk = std::any_of(
            desired_node_state.disks.begin(),
            desired_node_state.disks.end(),
            [&](const comet::DiskSpec& disk) {
              return disk.name == disk_name &&
                     disk.node_name == disk_node_name &&
                     disk.kind == comet::DiskKind::PlaneShared;
            });
        const bool delegated_shared_remove =
            is_plane_shared_disk &&
            disk_node_name != desired_node_state.inference.primary_infer_node;
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
          removed = true;
          if (!delegated_shared_remove) {
            RemoveRealDiskMount(effective_state, runtime_root);
          }
          auto removed_state = effective_state;
          removed_state.runtime_state = "removed";
          removed_state.status_message =
              delegated_shared_remove
                  ? "plane-shared disk removal delegated to primary infer node"
                  : "managed disk detached and removed by hostd";
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
        EnsureComposeMeshNetworkAvailable(compose_plan, compose_mode);
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
  bool shared_image_removal_deferred = false;
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
  static const comet::hostd::HostdBootstrapModelSupport bootstrap_model_support =
      MakeHostdBootstrapModelSupport();
  bootstrap_model_support.BootstrapPlaneModelIfNeeded(
      desired_node_state,
      node_name,
      backend,
      assignment_id);

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
      WaitForLocalRuntimeStatus(state_root, node_name, plane_name, std::chrono::seconds(300));
    }
    WaitForLocalInstanceRuntimeStatuses(
        state_root,
        node_name,
        plane_name,
        ExpectedRuntimeStatusCountForNode(desired_node_state, node_name),
        std::chrono::seconds(300));
    RunPostDeployScriptIfNeeded(
        desired_node_state,
        node_name,
        artifacts_root,
        storage_root,
        runtime_root,
        state_root,
        desired_generation,
        assignment_id,
        backend);
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

}  // namespace

namespace comet::hostd::appsupport {

nlohmann::json SendControllerJsonRequest(
    const std::string& controller_url,
    const std::string& method,
    const std::string& path,
    const nlohmann::json& payload,
    const std::map<std::string, std::string>& headers) {
  return controller_transport_support::SendControllerJsonRequest(
      controller_url,
      method,
      path,
      payload,
      headers);
}

comet::HostAssignment ParseAssignmentPayload(const nlohmann::json& payload) {
  return controller_transport_support::ParseAssignmentPayload(payload);
}

nlohmann::json BuildHostObservationPayload(const comet::HostObservation& observation) {
  return controller_transport_support::BuildHostObservationPayload(observation);
}

nlohmann::json BuildDiskRuntimeStatePayload(const comet::DiskRuntimeState& state) {
  return controller_transport_support::BuildDiskRuntimeStatePayload(state);
}

comet::DiskRuntimeState ParseDiskRuntimeStatePayload(const nlohmann::json& payload) {
  return controller_transport_support::ParseDiskRuntimeStatePayload(payload);
}

std::string Trim(const std::string& value) {
  return controller_transport_support::Trim(value);
}

void ShowLocalState(const std::string& node_name, const std::string& state_root) {
  ::ShowLocalState(node_name, state_root);
}

void ShowRuntimeStatus(const std::string& node_name, const std::string& state_root) {
  ::ShowRuntimeStatus(node_name, state_root);
}

comet::HostObservation BuildObservedStateSnapshot(
    const std::string& node_name,
    const std::string& state_root,
    comet::HostObservationStatus status,
    const std::string& status_message,
    const std::optional<int>& assignment_id) {
  return ::BuildObservedStateSnapshot(
      node_name,
      state_root,
      status,
      status_message,
      assignment_id);
}

void AppendHostdEvent(
    HostdBackend& backend,
    const std::string& category,
    const std::string& event_type,
    const std::string& message,
    const nlohmann::json& payload,
    const std::string& plane_name,
    const std::string& node_name,
    const std::string& worker_name,
    const std::optional<int>& assignment_id,
    const std::optional<int>& rollout_action_id,
    const std::string& severity) {
  ::AppendHostdEvent(
      backend,
      category,
      event_type,
      message,
      payload,
      plane_name,
      node_name,
      worker_name,
      assignment_id,
      rollout_action_id,
      severity);
}

comet::DesiredState RebaseStateForRuntimeRoot(
    comet::DesiredState state,
    const std::string& storage_root,
    const std::optional<std::string>& runtime_root) {
  return ::RebaseStateForRuntimeRoot(std::move(state), storage_root, runtime_root);
}

nlohmann::json BuildAssignmentProgressPayload(
    const std::string& phase,
    const std::string& phase_label,
    const std::string& message,
    int progress_percent,
    const std::string& plane_name,
    const std::string& node_name) {
  return ::BuildAssignmentProgressPayload(
      phase,
      phase_label,
      message,
      progress_percent,
      plane_name,
      node_name);
}

void PublishAssignmentProgress(
    HostdBackend* backend,
    const std::optional<int>& assignment_id,
    const nlohmann::json& progress) {
  ::PublishAssignmentProgress(backend, assignment_id, progress);
}

std::vector<std::string> ParseTaggedCsv(
    const std::string& tagged_message,
    const std::string& tag) {
  return ::ParseTaggedCsv(tagged_message, tag);
}

std::map<std::string, int> CaptureServiceHostPids(
    const std::vector<std::string>& service_names) {
  return ::CaptureServiceHostPids(service_names);
}

bool VerifyEvictionAssignment(
    const comet::DesiredState& desired_state,
    const std::string& node_name,
    const std::string& state_root,
    const std::string& tagged_message,
    const std::map<std::string, int>& expected_victim_host_pids) {
  return ::VerifyEvictionAssignment(
      desired_state,
      node_name,
      state_root,
      tagged_message,
      expected_victim_host_pids);
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
  ::ApplyDesiredNodeState(
      desired_node_state,
      artifacts_root,
      storage_root,
      runtime_root,
      state_root,
      compose_mode,
      source_label,
      desired_generation,
      assignment_id,
      backend);
}

void ShowDemoOps(
    const std::string& node_name,
    const std::string& storage_root,
    const std::optional<std::string>& runtime_root) {
  ::ShowDemoOps(node_name, storage_root, runtime_root);
}

void ShowStateOps(
    const std::string& db_path,
    const std::string& node_name,
    const std::string& artifacts_root,
    const std::string& storage_root,
    const std::optional<std::string>& runtime_root,
    const std::string& state_root) {
  ::ShowStateOps(
      db_path,
      node_name,
      artifacts_root,
      storage_root,
      runtime_root,
      state_root);
}

}  // namespace comet::hostd::appsupport

namespace comet::hostd {

HostdApp::HostdApp(int argc, char** argv) : argc_(argc), argv_(argv) {}

int HostdApp::Run() {
  comet::hostd::HostdCompositionRoot composition_root;
  return composition_root.Run(argc_, argv_);
}

}  // namespace comet::hostd
