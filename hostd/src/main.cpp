#include <chrono>
#include <cctype>
#include <cstdio>
#include <dlfcn.h>
#include <filesystem>
#include <fstream>
#include <exception>
#include <iostream>
#include <map>
#include <optional>
#include <stdexcept>
#include <string>
#include <cstdlib>
#include <sstream>
#include <set>
#include <thread>
#include <vector>
#include <array>
#include <algorithm>

#include <nlohmann/json.hpp>

#include "comet/compose_renderer.h"
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

std::string ResolvedDockerCommand();
std::vector<comet::RuntimeProcessStatus> LoadLocalInstanceRuntimeStatuses(
    const std::string& state_root,
    const std::string& node_name);

void PrintUsage() {
  std::cout
      << "Usage:\n"
      << "  comet-hostd show-demo-ops --node <node-name>\n"
      << "  comet-hostd show-state-ops --node <node-name> [--db <path>] [--artifacts-root <path>] [--runtime-root <path>] [--state-root <path>]\n"
      << "  comet-hostd show-local-state --node <node-name> [--state-root <path>]\n"
      << "  comet-hostd show-runtime-status --node <node-name> [--state-root <path>]\n"
      << "  comet-hostd report-observed-state --node <node-name> [--db <path>] [--state-root <path>]\n"
      << "  comet-hostd apply-state-ops --node <node-name> [--db <path>] [--artifacts-root <path>] [--runtime-root <path>] [--state-root <path>] [--compose-mode skip|exec]\n"
      << "  comet-hostd apply-next-assignment --node <node-name> [--db <path>] [--runtime-root <path>] [--state-root <path>] [--compose-mode skip|exec]\n";
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

std::string RebaseManagedPath(
    const std::string& path,
    const std::optional<std::string>& runtime_root,
    const std::optional<std::string>& node_name = std::nullopt) {
  if (!runtime_root.has_value()) {
    return path;
  }

  const std::filesystem::path original(path);
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
    const std::optional<std::string>& runtime_root) {
  if (!runtime_root.has_value()) {
    return state;
  }

  for (auto& disk : state.disks) {
    const bool node_local_disk =
        disk.kind == comet::DiskKind::InferPrivate ||
        disk.kind == comet::DiskKind::WorkerPrivate;
    disk.host_path = RebaseManagedPath(
        disk.host_path,
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

  return path_text == root_text ||
         (path_text.size() > root_text.size() &&
          path_text.compare(0, root_text.size(), root_text) == 0 &&
          path_text[root_text.size()] == '/');
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

std::string LocalGenerationPath(const std::string& state_root, const std::string& node_name) {
  return (std::filesystem::path(state_root) / node_name / "applied-generation.txt").string();
}

std::optional<int> LoadLocalAppliedGeneration(
    const std::string& state_root,
    const std::string& node_name) {
  const std::string path = LocalGenerationPath(state_root, node_name);
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

void SaveLocalAppliedGeneration(
    const std::string& state_root,
    const std::string& node_name,
    int generation) {
  const std::string path = LocalGenerationPath(state_root, node_name);
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

std::optional<comet::DesiredState> LoadLocalAppliedState(
    const std::string& state_root,
    const std::string& node_name) {
  return comet::LoadDesiredStateJson(LocalStatePath(state_root, node_name));
}

std::optional<comet::RuntimeStatus> LoadLocalRuntimeStatus(
    const std::string& state_root,
    const std::string& node_name) {
  const auto local_state = LoadLocalAppliedState(state_root, node_name);
  if (!local_state.has_value()) {
    return std::nullopt;
  }
  const auto runtime_status_path = RuntimeStatusPathForNode(*local_state, node_name);
  if (!runtime_status_path.has_value()) {
    return std::nullopt;
  }
  return comet::LoadRuntimeStatusJson(*runtime_status_path);
}

void SaveLocalAppliedState(
    const std::string& state_root,
    const std::string& node_name,
    const comet::DesiredState& state) {
  comet::SaveDesiredStateJson(state, LocalStatePath(state_root, node_name));
}

void WaitForLocalRuntimeStatus(
    const std::string& state_root,
    const std::string& node_name,
    std::chrono::seconds timeout) {
  const auto deadline = std::chrono::steady_clock::now() + timeout;
  while (std::chrono::steady_clock::now() < deadline) {
    if (LoadLocalRuntimeStatus(state_root, node_name).has_value()) {
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
    std::size_t expected_count,
    std::chrono::seconds timeout) {
  if (expected_count == 0) {
    return;
  }
  const auto deadline = std::chrono::steady_clock::now() + timeout;
  while (std::chrono::steady_clock::now() < deadline) {
    const auto statuses = LoadLocalInstanceRuntimeStatuses(state_root, node_name);
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
    const std::string& node_name) {
  std::vector<comet::RuntimeProcessStatus> result;
  const auto local_state = LoadLocalAppliedState(state_root, node_name);
  if (!local_state.has_value()) {
    return result;
  }

  for (const auto& instance : local_state->instances) {
    if (instance.node_name != node_name) {
      continue;
    }
    std::optional<std::string> status_path;
    if (instance.role == comet::InstanceRole::Infer) {
      status_path = RuntimeStatusPathForNode(*local_state, node_name);
    } else {
      status_path = WorkerRuntimeStatusPathForInstance(*local_state, instance);
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
  if (const auto nvml_snapshot = TryCollectGpuTelemetryWithNvml(state, node_name);
      nvml_snapshot.has_value()) {
    comet::GpuTelemetrySnapshot snapshot = *nvml_snapshot;
    PopulateGpuProcessesFromNvidiaSmi(&snapshot, instance_statuses);
    return snapshot;
  }
  if (const auto smi_snapshot =
          TryCollectGpuTelemetryWithNvidiaSmi(state, node_name, instance_statuses);
      smi_snapshot.has_value()) {
    return *smi_snapshot;
  }
  comet::GpuTelemetrySnapshot snapshot;
  snapshot.degraded = true;
  snapshot.source = "unavailable";
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
  }

  return observation;
}

void ReportObservedState(
    const std::string& db_path,
    const comet::HostObservation& observation,
    const std::string& source_label) {
  comet::ControllerStore store(db_path);
  store.Initialize();
  store.UpsertHostObservation(observation);

  std::cout << source_label << "\n";
  std::cout << "db=" << db_path << "\n";
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

void ApplyNodePlan(
    const comet::NodeExecutionPlan& plan,
    const comet::DesiredState& desired_node_state,
    const comet::NodeComposePlan& compose_plan,
    const std::optional<std::string>& runtime_root,
    ComposeMode compose_mode) {
  std::cout << "applying node=" << plan.node_name << "\n";
  std::cout << "compose=" << plan.compose_file_path << "\n";

  for (const auto& operation : plan.operations) {
    switch (operation.kind) {
      case comet::HostOperationKind::EnsureDisk:
        EnsureDiskDirectory(operation.details, operation.target);
        PrintOperationApplied(operation, "applied");
        break;
      case comet::HostOperationKind::RemoveDisk:
        RemoveDiskDirectory(operation.details, runtime_root);
        PrintOperationApplied(
            operation,
            runtime_root.has_value() ? "applied" : "skipped");
        break;
      case comet::HostOperationKind::EnsureService:
      case comet::HostOperationKind::RemoveService:
        PrintOperationApplied(operation, "planned");
        break;
      case comet::HostOperationKind::WriteInferRuntimeConfig:
        EnsureParentDirectory(operation.target);
        WriteTextFile(operation.target, comet::RenderInferRuntimeConfigJson(desired_node_state));
        PrintOperationApplied(operation, "applied");
        break;
      case comet::HostOperationKind::RemoveInferRuntimeConfig:
        RemoveFileIfExists(operation.target);
        PrintOperationApplied(operation, "applied");
        break;
      case comet::HostOperationKind::WriteComposeFile:
        WriteTextFile(operation.target, comet::RenderComposeYaml(compose_plan));
        PrintOperationApplied(operation, "applied");
        break;
      case comet::HostOperationKind::RemoveComposeFile:
        RemoveFileIfExists(operation.target);
        PrintOperationApplied(operation, "applied");
        break;
      case comet::HostOperationKind::ComposeUp:
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
  }
}

void ShowDemoOps(
    const std::string& node_name,
    const std::optional<std::string>& runtime_root) {
  const comet::DesiredState state =
      RebaseStateForRuntimeRoot(comet::BuildDemoState(), runtime_root);
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
    const std::optional<std::string>& runtime_root,
    const std::string& state_root) {
  comet::ControllerStore store(db_path);
  store.Initialize();
  const auto state = store.LoadDesiredState();
  if (!state.has_value()) {
    throw std::runtime_error("no desired state found in db '" + db_path + "'");
  }

  const comet::DesiredState rebased_full_state =
      RebaseStateForRuntimeRoot(*state, runtime_root);
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
  const auto current_local_state = LoadLocalAppliedState(state_root, node_name);
  const auto applied_generation = LoadLocalAppliedGeneration(state_root, node_name);
  const auto plan = ResolveNodeExecutionPlan(
      comet::BuildNodeExecutionPlans(current_local_state, desired_node_state, artifacts_root),
      current_local_state,
      desired_node_state,
      node_name,
      artifacts_root);

  std::cout << source_label << " for node=" << plan.node_name << "\n";
  std::cout << "artifacts_root=" << artifacts_root << "\n";
  std::cout << "state_path=" << LocalStatePath(state_root, node_name) << "\n";
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

  const auto runtime_status_path = RuntimeStatusPathForNode(*local_state, node_name);
  if (!runtime_status_path.has_value()) {
    std::cout << "runtime_status: unavailable (node has no infer instance)\n";
    return;
  }

  std::cout << "runtime_status_path=" << *runtime_status_path << "\n";
  const auto runtime_status = comet::LoadRuntimeStatusJson(*runtime_status_path);
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
            << (runtime_status->started_at.empty() ? "(empty)" : runtime_status->started_at)
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
    const std::string& db_path,
    const std::string& node_name,
    const std::string& state_root) {
  ReportObservedState(
      db_path,
      BuildObservedStateSnapshot(
          node_name,
          state_root,
          comet::HostObservationStatus::Idle,
          "manual heartbeat"),
      "hostd report-observed-state");
}

void ApplyDesiredNodeState(
    const comet::DesiredState& desired_node_state,
    const std::string& artifacts_root,
    const std::optional<std::string>& runtime_root,
    const std::string& state_root,
    ComposeMode compose_mode,
    const std::string& source_label,
    const std::optional<int>& desired_generation) {
  const std::string node_name = RequireSingleNodeName(desired_node_state);
  const auto current_local_state = LoadLocalAppliedState(state_root, node_name);
  const auto applied_generation = LoadLocalAppliedGeneration(state_root, node_name);
  const auto execution_plan = ResolveNodeExecutionPlan(
      comet::BuildNodeExecutionPlans(current_local_state, desired_node_state, artifacts_root),
      current_local_state,
      desired_node_state,
      node_name,
      artifacts_root);
  const auto compose_plan = RequireNodeComposePlan(desired_node_state, node_name);

  std::cout << source_label << "\n";
  std::cout << "artifacts_root=" << artifacts_root << "\n";
  std::cout << "state_path=" << LocalStatePath(state_root, node_name) << "\n";
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

  if (execution_plan.operations.empty()) {
    std::cout << "no local changes for node=" << node_name << "\n";
    if (IsDesiredNodeStateEmpty(desired_node_state)) {
      RemoveStateFileIfExists(LocalStatePath(state_root, node_name));
      RemoveStateFileIfExists(LocalGenerationPath(state_root, node_name));
    } else {
      SaveLocalAppliedState(state_root, node_name, desired_node_state);
      if (desired_generation.has_value()) {
        SaveLocalAppliedGeneration(state_root, node_name, *desired_generation);
      }
    }
    return;
  }

  ApplyNodePlan(
      execution_plan,
      desired_node_state,
      compose_plan,
      runtime_root,
      compose_mode);
  if (IsDesiredNodeStateEmpty(desired_node_state)) {
    RemoveStateFileIfExists(LocalStatePath(state_root, node_name));
    RemoveStateFileIfExists(LocalGenerationPath(state_root, node_name));
    return;
  }
  SaveLocalAppliedState(state_root, node_name, desired_node_state);
  if (desired_generation.has_value()) {
    SaveLocalAppliedGeneration(state_root, node_name, *desired_generation);
  }
  if (compose_mode == ComposeMode::Exec) {
    if (NodeHasInferInstance(desired_node_state)) {
      WaitForLocalRuntimeStatus(state_root, node_name, std::chrono::seconds(20));
    }
    WaitForLocalInstanceRuntimeStatuses(
        state_root,
        node_name,
        ExpectedRuntimeStatusCountForNode(desired_node_state, node_name),
        std::chrono::seconds(20));
  }
}

void ApplyStateOps(
    const std::string& db_path,
    const std::string& node_name,
    const std::string& artifacts_root,
    const std::optional<std::string>& runtime_root,
    const std::string& state_root,
    ComposeMode compose_mode) {
  comet::ControllerStore store(db_path);
  store.Initialize();
  const auto state = store.LoadDesiredState();
  if (!state.has_value()) {
    throw std::runtime_error("no desired state found in db '" + db_path + "'");
  }
  const auto desired_generation = store.LoadDesiredGeneration();

  const comet::DesiredState rebased_full_state =
      RebaseStateForRuntimeRoot(*state, runtime_root);
  const comet::DesiredState desired_node_state =
      comet::SliceDesiredStateForNode(rebased_full_state, node_name);

  std::cout << "db=" << db_path << "\n";
  try {
    ApplyDesiredNodeState(
        desired_node_state,
        artifacts_root,
        runtime_root,
        state_root,
        compose_mode,
        "hostd apply-state-ops",
        desired_generation);
    ReportObservedState(
        db_path,
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
        db_path,
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
    const std::string& db_path,
    const std::string& node_name,
    const std::optional<std::string>& runtime_root,
    const std::string& state_root,
    ComposeMode compose_mode) {
  comet::ControllerStore store(db_path);
  store.Initialize();
  const auto assignment = store.ClaimNextHostAssignment(node_name);
  if (!assignment.has_value()) {
    std::cout << "no pending assignments for node=" << node_name << "\n";
    return;
  }

  std::cout << "hostd apply-next-assignment\n";
  std::cout << "db=" << db_path << "\n";
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
        assignment->assignment_type != "evict-workers") {
      throw std::runtime_error(
          "unsupported assignment type '" + assignment->assignment_type + "'");
    }

    const comet::DesiredState rebased_state = RebaseStateForRuntimeRoot(
        comet::DeserializeDesiredStateJson(assignment->desired_state_json),
        runtime_root);
    const bool is_drain_assignment = assignment->assignment_type == "drain-node-state";
    const bool is_eviction_assignment = assignment->assignment_type == "evict-workers";
    const auto victim_names =
        is_eviction_assignment ? ParseTaggedCsv(assignment->status_message, "victims")
                               : std::vector<std::string>{};
    const auto victim_host_pids =
        is_eviction_assignment ? CaptureServiceHostPids(victim_names)
                               : std::map<std::string, int>{};
    ReportObservedState(
        db_path,
        BuildObservedStateSnapshot(
            node_name,
            state_root,
            comet::HostObservationStatus::Applying,
            (is_drain_assignment ? "draining node for desired generation "
                                 : (is_eviction_assignment
                                        ? "evicting rollout workers for desired generation "
                                        : "applying desired generation ")) +
                std::to_string(assignment->desired_generation) + assignment_context,
            assignment->id),
        "hostd observed-state-update");
    ApplyDesiredNodeState(
        rebased_state,
        assignment->artifacts_root,
        runtime_root,
        state_root,
        compose_mode,
        is_drain_assignment
            ? "hostd drain-assignment-ops"
            : (is_eviction_assignment
                   ? "hostd eviction-assignment-ops"
                   : "hostd apply-assignment-ops"),
        assignment->desired_generation);
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
        db_path,
        BuildObservedStateSnapshot(
            node_name,
            state_root,
            comet::HostObservationStatus::Applied,
            (is_drain_assignment ? "drained node for desired generation "
                                 : (is_eviction_assignment
                                        ? "evicted rollout workers for desired generation "
                                        : "applied desired generation ")) +
                std::to_string(assignment->desired_generation) + assignment_context,
            assignment->id),
        "hostd observed-state-update");
    if (!store.TransitionClaimedHostAssignment(
        assignment->id,
        comet::HostAssignmentStatus::Applied,
        (is_drain_assignment ? "drained node for desired generation "
                             : (is_eviction_assignment
                                    ? "evicted rollout workers for desired generation "
                                    : "applied desired generation ")) +
            std::to_string(assignment->desired_generation) +
            assignment_context +
            " on attempt " + std::to_string(assignment->attempt_count) + "/" +
            std::to_string(assignment->max_attempts))) {
      std::cout << "assignment transition skipped for id=" << assignment->id
                << " because it is no longer claimed\n";
    }
  } catch (const std::exception& error) {
    const std::string error_message = error.what();
    ReportObservedState(
        db_path,
        BuildObservedStateSnapshot(
            node_name,
            state_root,
            comet::HostObservationStatus::Failed,
            error_message,
            assignment->id),
        "hostd observed-state-update");
    if (assignment->attempt_count < assignment->max_attempts) {
      if (!store.TransitionClaimedHostAssignment(
              assignment->id,
              comet::HostAssignmentStatus::Pending,
              "attempt " + std::to_string(assignment->attempt_count) + "/" +
                  std::to_string(assignment->max_attempts) + " failed: " +
                  error_message + assignment_context)) {
        std::cout << "assignment retry transition skipped for id=" << assignment->id
                  << " because it is no longer claimed\n";
      }
    } else {
      if (!store.TransitionClaimedHostAssignment(
              assignment->id,
              comet::HostAssignmentStatus::Failed,
              "attempt " + std::to_string(assignment->attempt_count) + "/" +
                  std::to_string(assignment->max_attempts) + " exhausted: " +
                  error_message + assignment_context)) {
        std::cout << "assignment failure transition skipped for id=" << assignment->id
                  << " because it is no longer claimed\n";
      }
    }
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
  if (!node_name.has_value()) {
    std::cerr << "error: --node is required\n";
    return 1;
  }

  try {
    if (command == "show-demo-ops") {
      ShowDemoOps(*node_name, ParseRuntimeRootArg(argc, argv));
      return 0;
    }

    if (command == "show-state-ops") {
      ShowStateOps(
          ResolveDbPath(ParseDbArg(argc, argv)),
          *node_name,
          ResolveArtifactsRoot(ParseArtifactsRootArg(argc, argv)),
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
          ResolveDbPath(ParseDbArg(argc, argv)),
          *node_name,
          ResolveStateRoot(ParseStateRootArg(argc, argv)));
      return 0;
    }

    if (command == "apply-state-ops") {
      ApplyStateOps(
          ResolveDbPath(ParseDbArg(argc, argv)),
          *node_name,
          ResolveArtifactsRoot(ParseArtifactsRootArg(argc, argv)),
          ParseRuntimeRootArg(argc, argv),
          ResolveStateRoot(ParseStateRootArg(argc, argv)),
          ResolveComposeMode(ParseComposeModeArg(argc, argv)));
      return 0;
    }

    if (command == "apply-next-assignment") {
      ApplyNextAssignment(
          ResolveDbPath(ParseDbArg(argc, argv)),
          *node_name,
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
