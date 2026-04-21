#include "app/hostd_local_state_repository.h"

#include <algorithm>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <set>
#include <sstream>
#include <stdexcept>

#include "naim/state/state_json.h"

namespace naim::hostd {

HostdLocalStateRepository::HostdLocalStateRepository(
    const HostdLocalStatePathSupport& path_support)
    : path_support_(path_support) {}

std::optional<int> HostdLocalStateRepository::LoadLocalAppliedGeneration(
    const std::string& state_root,
    const std::string& node_name,
    const std::optional<std::string>& plane_name) const {
  if (plane_name.has_value()) {
    return LoadGenerationFromPath(
        path_support_.LocalPlaneGenerationPath(state_root, node_name, *plane_name));
  }
  const std::string aggregate_path = path_support_.LocalGenerationPath(state_root, node_name);
  if (std::filesystem::exists(aggregate_path)) {
    return LoadGenerationFromPath(aggregate_path);
  }
  std::optional<int> generation;
  for (const auto& current_plane_name : path_support_.ListLocalPlaneNames(state_root, node_name)) {
    const auto plane_generation = LoadGenerationFromPath(
        path_support_.LocalPlaneGenerationPath(state_root, node_name, current_plane_name));
    if (!plane_generation.has_value()) {
      continue;
    }
    generation = generation.has_value() ? std::max(*generation, *plane_generation)
                                        : *plane_generation;
  }
  return generation;
}

void HostdLocalStateRepository::SaveLocalAppliedGeneration(
    const std::string& state_root,
    const std::string& node_name,
    int generation,
    const std::optional<std::string>& plane_name) const {
  const std::string path = plane_name.has_value()
                               ? path_support_.LocalPlaneGenerationPath(
                                     state_root,
                                     node_name,
                                     *plane_name)
                               : path_support_.LocalGenerationPath(state_root, node_name);
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

std::vector<naim::DesiredState> HostdLocalStateRepository::LoadAllLocalAppliedStates(
    const std::string& state_root,
    const std::string& node_name) const {
  std::vector<naim::DesiredState> result;
  for (const auto& plane_name : path_support_.ListLocalPlaneNames(state_root, node_name)) {
    const auto plane_state = LoadStateFromPath(
        path_support_.LocalPlaneStatePath(state_root, node_name, plane_name));
    if (plane_state.has_value()) {
      result.push_back(*plane_state);
    }
  }
  if (!result.empty()) {
    return result;
  }
  const auto aggregate_state =
      LoadStateFromPath(path_support_.LocalStatePath(state_root, node_name));
  if (aggregate_state.has_value()) {
    result.push_back(*aggregate_state);
  }
  return result;
}

std::optional<naim::DesiredState> HostdLocalStateRepository::LoadLocalAppliedState(
    const std::string& state_root,
    const std::string& node_name,
    const std::optional<std::string>& plane_name) const {
  if (plane_name.has_value()) {
    return LoadStateFromPath(
        path_support_.LocalPlaneStatePath(state_root, node_name, *plane_name));
  }
  const auto states = LoadAllLocalAppliedStates(state_root, node_name);
  if (states.empty()) {
    return std::nullopt;
  }
  return MergeLocalAppliedStates(states);
}

void HostdLocalStateRepository::RewriteAggregateLocalState(
    const std::string& state_root,
    const std::string& node_name) const {
  std::vector<naim::DesiredState> states;
  for (const auto& plane_name : path_support_.ListLocalPlaneNames(state_root, node_name)) {
    const auto plane_state = LoadStateFromPath(
        path_support_.LocalPlaneStatePath(state_root, node_name, plane_name));
    if (plane_state.has_value()) {
      states.push_back(*plane_state);
    }
  }
  if (states.empty()) {
    RemoveStateFileIfExists(path_support_.LocalStatePath(state_root, node_name));
    return;
  }
  WriteLocalStateFile(
      MergeLocalAppliedStates(states),
      path_support_.LocalStatePath(state_root, node_name));
}

void HostdLocalStateRepository::RewriteAggregateLocalGeneration(
    const std::string& state_root,
    const std::string& node_name) const {
  std::optional<int> generation;
  for (const auto& plane_name : path_support_.ListLocalPlaneNames(state_root, node_name)) {
    const auto plane_generation = LoadGenerationFromPath(
        path_support_.LocalPlaneGenerationPath(state_root, node_name, plane_name));
    if (!plane_generation.has_value()) {
      continue;
    }
    generation = generation.has_value() ? std::max(*generation, *plane_generation)
                                        : *plane_generation;
  }
  if (generation.has_value()) {
    SaveLocalAppliedGeneration(state_root, node_name, *generation, std::nullopt);
  } else {
    RemoveStateFileIfExists(path_support_.LocalGenerationPath(state_root, node_name));
  }
}

void HostdLocalStateRepository::SaveLocalAppliedState(
    const std::string& state_root,
    const std::string& node_name,
    const naim::DesiredState& state,
    const std::optional<std::string>& plane_name) const {
  const std::string effective_plane_name =
      plane_name.has_value() ? *plane_name : state.plane_name;
  const std::string path =
      plane_name.has_value()
          ? path_support_.LocalPlaneStatePath(state_root, node_name, effective_plane_name)
          : path_support_.LocalStatePath(state_root, node_name);
  WriteLocalStateFile(state, path);
}

void HostdLocalStateRepository::RemoveLocalAppliedPlaneState(
    const std::string& state_root,
    const std::string& node_name,
    const std::string& plane_name) const {
  const std::filesystem::path plane_root(
      path_support_.LocalPlaneRoot(state_root, node_name, plane_name));
  if (!std::filesystem::exists(plane_root)) {
    return;
  }
  std::error_code error;
  std::filesystem::remove_all(plane_root, error);
  if (error) {
    throw std::runtime_error(
        "failed to remove plane state root for plane '" + plane_name + "': " + error.message());
  }
}

void HostdLocalStateRepository::PrintLocalStateSummary(
    const naim::DesiredState& state,
    const std::string& state_path,
    const std::string& node_name,
    const std::optional<int>& generation) const {
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
              << " kind=" << naim::ToString(disk.kind)
              << " host_path=" << disk.host_path
              << " realized=directory-backed"
              << " exists=" << (std::filesystem::exists(disk.host_path) ? "yes" : "no")
              << "\n";
  }
  for (const auto& instance : state.instances) {
    std::cout << "  - " << instance.name << " role=" << naim::ToString(instance.role)
              << " image=" << instance.image << "\n";
  }
}

std::string HostdLocalStateRepository::RequireSingleNodeName(
    const naim::DesiredState& state) const {
  if (state.nodes.empty()) {
    throw std::runtime_error("desired node state is empty");
  }
  const std::string node_name = state.nodes.front().name;
  if (node_name.empty()) {
    throw std::runtime_error("desired node state has an empty node name");
  }
  for (const auto& node : state.nodes) {
    if (node.name != node_name) {
      throw std::runtime_error(
          "desired node state contains multiple node entries; expected a node-sliced state");
    }
  }
  for (const auto& disk : state.disks) {
    if (!disk.node_name.empty() && disk.node_name != node_name) {
      throw std::runtime_error(
          "desired node state contains disk '" + disk.name + "' for node '" + disk.node_name +
          "' while applying node '" + node_name + "'");
    }
  }
  for (const auto& instance : state.instances) {
    if (!instance.node_name.empty() && instance.node_name != node_name) {
      throw std::runtime_error(
          "desired node state contains instance '" + instance.name + "' for node '" +
          instance.node_name + "' while applying node '" + node_name + "'");
    }
  }
  return node_name;
}

std::optional<int> HostdLocalStateRepository::LoadGenerationFromPath(
    const std::string& path) const {
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

std::optional<naim::DesiredState> HostdLocalStateRepository::LoadStateFromPath(
    const std::string& path) const {
  if (!std::filesystem::exists(path)) {
    return std::nullopt;
  }
  std::ifstream input(path, std::ios::binary);
  if (!input.is_open()) {
    throw std::runtime_error("failed to open local state file: " + path);
  }
  std::ostringstream buffer;
  buffer << input.rdbuf();
  if (!input.good() && !input.eof()) {
    throw std::runtime_error("failed to read local state file: " + path);
  }
  return naim::DeserializeDesiredStateJson(buffer.str());
}

naim::DesiredState HostdLocalStateRepository::MergeLocalAppliedStates(
    const std::vector<naim::DesiredState>& states) const {
  if (states.empty()) {
    throw std::runtime_error("cannot merge empty local state list");
  }

  naim::DesiredState merged = states.front();
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

void HostdLocalStateRepository::RemoveStateFileIfExists(const std::string& path) const {
  std::error_code error;
  std::filesystem::remove(path, error);
  if (error) {
    throw std::runtime_error("failed to remove state file: " + path + ": " + error.message());
  }
}

void HostdLocalStateRepository::WriteLocalStateFile(
    const naim::DesiredState& state,
    const std::string& path) const {
  const std::filesystem::path file_path(path);
  if (file_path.has_parent_path()) {
    std::filesystem::create_directories(file_path.parent_path());
  }

  std::ofstream output(path, std::ios::binary | std::ios::trunc);
  if (!output.is_open()) {
    throw std::runtime_error("failed to open local state file for write: " + path);
  }

  output << naim::SerializeDesiredStateJson(state) << "\n";
  if (!output.good()) {
    throw std::runtime_error("failed to write local state file: " + path);
  }
}

}  // namespace naim::hostd
