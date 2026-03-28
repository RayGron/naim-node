#include "runtime/infer_control_support.h"

#include <algorithm>
#include <filesystem>
#include <fstream>
#include <set>
#include <stdexcept>

namespace comet::infer::control_support {

namespace {

json LoadJsonFile(const fs::path& path) {
  std::ifstream input(path);
  if (!input.is_open()) {
    throw std::runtime_error("failed to open json file: " + path.string());
  }
  json value;
  input >> value;
  return value;
}

json LoadJsonOrDefault(const fs::path& path, json fallback) {
  if (!fs::exists(path)) {
    return fallback;
  }
  return LoadJsonFile(path);
}

template <typename T>
T Require(const json& object, const char* key, const char* context) {
  if (!object.contains(key)) {
    throw std::runtime_error(
        std::string("missing required key '") + key + "' in " + context);
  }
  return object.at(key).get<T>();
}

}  // namespace

json LoadProfiles(const std::string& path_str) {
  const fs::path path(path_str);
  if (!fs::exists(path)) {
    throw std::runtime_error("runtime profiles not found: " + path.string());
  }
  return LoadJsonFile(path);
}

RuntimeProfile ResolveProfile(const json& profiles_json, const std::string& name) {
  const json profiles = Require<json>(profiles_json, "profiles", "root");
  if (!profiles.contains(name) || !profiles.at(name).is_object()) {
    throw std::runtime_error("unknown runtime profile: " + name);
  }
  const json profile_json = profiles.at(name);
  RuntimeProfile profile;
  profile.name = name;
  profile.llama_args = profile_json.value("llama_args", std::vector<std::string>{});
  if (profile_json.contains("ctx_size")) {
    profile.ctx_size = profile_json.at("ctx_size").get<int>();
  }
  if (profile_json.contains("batch_size")) {
    profile.batch_size = profile_json.at("batch_size").get<int>();
  }
  if (profile_json.contains("gpu_layers")) {
    profile.gpu_layers = profile_json.at("gpu_layers").get<int>();
  }
  return profile;
}

ControlPaths BuildControlPaths(const RuntimeConfig& config) {
  const fs::path root(config.control_root);
  return ControlPaths{
      root,
      root / "runtime-assets.json",
      root / "model-cache-registry.json",
      root / "active-model.json",
      root / "gateway-plan.json",
      root / "runtime-status.json",
      root / "worker-upstream.json",
      root / "worker-group",
  };
}

json LoadWorkerUpstreamContract(const RuntimeConfig& config) {
  return LoadJsonOrDefault(BuildControlPaths(config).worker_upstream_path, json::object());
}

json LoadWorkerGroupStatus(const RuntimeConfig& config) {
  const ControlPaths paths = BuildControlPaths(config);
  json result = {
      {"group_id", config.worker_group.value("group_id", std::string{})},
      {"data_parallel_mode", config.data_parallel_mode},
      {"expected_workers", config.worker_group.value("expected_workers", 0)},
      {"members", json::array()},
  };
  if (!fs::exists(paths.worker_group_dir) || !fs::is_directory(paths.worker_group_dir)) {
    return result;
  }
  std::set<std::string> configured_members;
  for (const auto& member : config.worker_group.value("members", json::array())) {
    if (member.is_object()) {
      const std::string member_name = member.value("name", std::string{});
      if (!member_name.empty()) {
        configured_members.insert(member_name);
      }
    }
  }
  std::vector<fs::path> files;
  for (const auto& entry : fs::directory_iterator(paths.worker_group_dir)) {
    if (entry.is_regular_file() && entry.path().extension() == ".json") {
      files.push_back(entry.path());
    }
  }
  std::sort(files.begin(), files.end());
  for (const auto& path : files) {
    const json member = LoadJsonOrDefault(path, json::object());
    if (!member.is_object()) {
      continue;
    }
    const std::string member_name = member.value("instance_name", std::string{});
    if (!configured_members.empty() && !member_name.empty() &&
        configured_members.count(member_name) == 0) {
      continue;
    }
    result["members"].push_back(member);
  }
  return result;
}

std::vector<fs::path> RuntimeDirs(const RuntimeConfig& config) {
  return {
      fs::path(config.control_root),
      fs::path(config.models_root),
      fs::path(config.model_cache_dir),
      fs::path(config.gguf_cache_dir),
      fs::path(config.infer_log_dir),
  };
}

json LoadActiveModel(const RuntimeConfig& config) {
  return LoadJsonOrDefault(BuildControlPaths(config).active_model_path, json::object());
}

json LoadGatewayPlan(const RuntimeConfig& config) {
  return LoadJsonOrDefault(BuildControlPaths(config).gateway_plan_path, json::object());
}

json LoadRegistry(const RuntimeConfig& config) {
  return LoadJsonOrDefault(
      BuildControlPaths(config).registry_path,
      json{{"version", 1}, {"entries", json::array()}});
}

json LoadRuntimeAssetsStatus(const RuntimeConfig& config) {
  return LoadJsonOrDefault(BuildControlPaths(config).runtime_assets_path, json::object());
}

int EnabledGpuNodeCount(const RuntimeConfig& config) {
  int enabled = 0;
  for (const auto& gpu_node : config.gpu_nodes) {
    if (!gpu_node.is_object()) {
      continue;
    }
    if (gpu_node.value("enabled", true)) {
      ++enabled;
    }
  }
  return enabled;
}

}  // namespace comet::infer::control_support
