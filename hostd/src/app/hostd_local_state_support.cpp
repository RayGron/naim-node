#include "app/hostd_local_state_support.h"

#include <algorithm>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <set>
#include <stdexcept>
#include <thread>

#include "comet/state/state_json.h"

namespace comet::hostd::local_state_support {

namespace {

void RemoveStateFileIfExists(const std::string& path) {
  std::error_code error;
  std::filesystem::remove(path, error);
  if (error) {
    throw std::runtime_error("failed to remove state file: " + path + ": " + error.message());
  }
}

}  // namespace

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
    if (entry.is_directory()) {
      result.push_back(entry.path().filename().string());
    }
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
    const std::optional<std::string>& plane_name) {
  if (plane_name.has_value()) {
    return LoadGenerationFromPath(LocalPlaneGenerationPath(state_root, node_name, *plane_name));
  }
  const std::string aggregate_path = LocalGenerationPath(state_root, node_name);
  if (std::filesystem::exists(aggregate_path)) {
    return LoadGenerationFromPath(aggregate_path);
  }
  std::optional<int> generation;
  for (const auto& current_plane_name : ListLocalPlaneNames(state_root, node_name)) {
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
    const std::optional<std::string>& plane_name) {
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
  for (const auto& plane_name : ListLocalPlaneNames(state_root, node_name)) {
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
    const std::optional<std::string>& plane_name) {
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
    const RuntimeStatusPathResolver& runtime_status_path_resolver,
    const std::optional<std::string>& plane_name) {
  if (plane_name.has_value()) {
    const auto local_state = LoadLocalAppliedState(state_root, node_name, plane_name);
    if (!local_state.has_value()) {
      return std::nullopt;
    }
    const auto runtime_status_path = runtime_status_path_resolver(*local_state, node_name);
    if (!runtime_status_path.has_value()) {
      return std::nullopt;
    }
    return comet::LoadRuntimeStatusJson(*runtime_status_path);
  }

  for (const auto& local_state : LoadAllLocalAppliedStates(state_root, node_name)) {
    const auto runtime_status_path = runtime_status_path_resolver(local_state, node_name);
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
    const std::optional<std::string>& plane_name) {
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
    const RuntimeStatusPathResolver& runtime_status_path_resolver,
    const std::optional<std::string>& plane_name,
    std::chrono::seconds timeout) {
  const auto deadline = std::chrono::steady_clock::now() + timeout;
  while (std::chrono::steady_clock::now() < deadline) {
    if (const auto runtime_status =
            LoadLocalRuntimeStatus(state_root, node_name, runtime_status_path_resolver, plane_name);
        runtime_status.has_value() &&
        runtime_status->ready &&
        runtime_status->launch_ready &&
        runtime_status->inference_ready &&
        (runtime_status->gateway_health_url.empty() || runtime_status->gateway_ready)) {
      return;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(250));
  }
  throw std::runtime_error(
      "timed out waiting for plane runtime readiness on node '" + node_name + "'");
}

namespace {
bool InstanceProducesRuntimeStatus(const comet::InstanceSpec& instance) {
  return instance.role == comet::InstanceRole::Infer ||
         instance.role == comet::InstanceRole::Worker;
}
}  // namespace

std::size_t ExpectedRuntimeStatusCountForNode(
    const comet::DesiredState& desired_node_state,
    const std::string& node_name) {
  std::size_t count = 0;
  for (const auto& instance : desired_node_state.instances) {
    if (instance.node_name == node_name && InstanceProducesRuntimeStatus(instance)) {
      ++count;
    }
  }
  return count;
}

void WaitForLocalInstanceRuntimeStatuses(
    const std::string& state_root,
    const std::string& node_name,
    const InstanceRuntimeStatusLoader& status_loader,
    const std::optional<std::string>& plane_name,
    std::size_t expected_count,
    std::chrono::seconds timeout) {
  if (expected_count == 0) {
    return;
  }
  const auto deadline = std::chrono::steady_clock::now() + timeout;
  while (std::chrono::steady_clock::now() < deadline) {
    const auto statuses = status_loader(state_root, node_name, plane_name);
    std::size_t ready_count = 0;
    for (const auto& status : statuses) {
      if (status.ready &&
          (status.runtime_phase == "running" || status.runtime_phase == "ready")) {
        ++ready_count;
      }
    }
    if (ready_count >= expected_count) {
      return;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(250));
  }
  throw std::runtime_error(
      "timed out waiting for instance runtime readiness on node '" + node_name + "'");
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

}  // namespace comet::hostd::local_state_support
