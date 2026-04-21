#include "app/infer_model_cache_support.h"

#include <algorithm>
#include <cctype>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <stdexcept>
#include <string>
#include <vector>

#include <nlohmann/json.hpp>

namespace naim::infer::model_cache_support {

namespace fs = std::filesystem;
using control_support::BuildControlPaths;
using control_support::EnabledGpuNodeCount;
using control_support::LoadRegistry;
using control_support::LoadActiveModel;
using control_support::RuntimeProfile;
using nlohmann::json;

namespace {

[[noreturn]] void Throw(const std::string& message) {
  throw std::runtime_error(message);
}

std::string ExpandUserPath(const std::string& value) {
  if (value.empty() || value[0] != '~') {
    return value;
  }
  const char* home = std::getenv("HOME");
  if (home == nullptr || std::strlen(home) == 0) {
    return value;
  }
  if (value == "~") {
    return home;
  }
  if (value.size() > 1 && value[1] == '/') {
    return std::string(home) + value.substr(1);
  }
  return value;
}

void SaveJsonFile(const fs::path& path, const json& value) {
  const auto parent = path.parent_path();
  if (!parent.empty()) {
    fs::create_directories(parent);
  }
  std::ofstream output(path);
  if (!output.is_open()) {
    Throw("failed to write json file: " + path.string());
  }
  output << value.dump(2) << "\n";
}

std::string ToLowerCopy(std::string value) {
  std::transform(
      value.begin(),
      value.end(),
      value.begin(),
      [](unsigned char ch) { return static_cast<char>(std::tolower(ch)); });
  return value;
}

std::string SafeServedModelName(const std::string& model_id) {
  std::string sanitized = ToLowerCopy(model_id);
  std::replace(sanitized.begin(), sanitized.end(), '/', '-');
  return sanitized;
}

std::string Join(const std::vector<std::string>& values, const std::string& delimiter) {
  std::string joined;
  for (std::size_t index = 0; index < values.size(); ++index) {
    if (index > 0) {
      joined += delimiter;
    }
    joined += values[index];
  }
  return joined;
}

bool HasArgument(const std::vector<std::string>& args, const std::string& flag) {
  return std::find(args.begin(), args.end(), flag) != args.end();
}

json FindCachedEntry(
    const json& registry,
    const std::string& alias,
    const std::string& local_model_path) {
  for (const auto& entry : registry.value("entries", json::array())) {
    if (entry.value("alias", std::string{}) == alias ||
        entry.value("local_model_path", std::string{}) == local_model_path) {
      return entry;
    }
  }
  return json::object();
}

}  // namespace

void PreloadModel(
    const RuntimeConfig& config,
    const InferCommandLineOptions& args,
    bool apply) {
  if (args.alias.empty() || args.source_model_id.empty() || args.local_model_path.empty()) {
    Throw("preload-model requires --alias, --source-model-id, and --local-model-path");
  }
  const fs::path local_model_path(ExpandUserPath(args.local_model_path));
  const std::string runtime_model_path =
      args.runtime_model_path.empty() ? local_model_path.string() : args.runtime_model_path;
  const fs::path marker_root =
      local_model_path.has_extension() ? local_model_path.parent_path() : local_model_path;
  const fs::path marker_path = marker_root / ".naim-model-cache.json";
  const auto paths = BuildControlPaths(config);
  json registry = LoadRegistry(config);
  const json existing = FindCachedEntry(registry, args.alias, local_model_path.string());

  json payload{
      {"alias", args.alias},
      {"source_model_id", args.source_model_id},
      {"local_model_path", local_model_path.string()},
      {"runtime_model_path", runtime_model_path},
      {"marker_path", marker_path.string()},
      {"status", "prepared"},
  };
  std::cout << "preload-model-plan:\n";
  std::cout << "  alias=" << args.alias << "\n";
  std::cout << "  source_model_id=" << args.source_model_id << "\n";
  std::cout << "  local_model_path=" << local_model_path.string() << "\n";
  std::cout << "  runtime_model_path=" << runtime_model_path << "\n";
  std::cout << "  registry_path=" << paths.registry_path.string() << "\n";
  std::cout << "  marker_path=" << marker_path.string() << "\n";
  std::cout << "  registry_entry=" << (existing.empty() ? "missing" : "present") << "\n";
  if (!apply) {
    return;
  }

  if (local_model_path.has_extension()) {
    if (marker_root.empty()) {
      Throw("local model file path must have a parent directory");
    }
    fs::create_directories(marker_root);
  } else {
    fs::create_directories(local_model_path);
  }
  SaveJsonFile(marker_path, payload);

  json next_entries = json::array();
  for (const auto& entry : registry.value("entries", json::array())) {
    if (entry.value("alias", std::string{}) != args.alias) {
      next_entries.push_back(entry);
    }
  }
  next_entries.push_back(payload);
  registry = json{{"version", 1}, {"plane_name", config.plane_name}, {"entries", next_entries}};
  SaveJsonFile(paths.registry_path, registry);
}

int CacheStatus(
    const RuntimeConfig& config,
    const InferCommandLineOptions& args) {
  if (args.alias.empty() || args.local_model_path.empty()) {
    Throw("cache-status requires --alias and --local-model-path");
  }
  const fs::path local_model_path(ExpandUserPath(args.local_model_path));
  const fs::path marker_root =
      local_model_path.has_extension() ? local_model_path.parent_path() : local_model_path;
  const fs::path marker_path = marker_root / ".naim-model-cache.json";
  const json registry = LoadRegistry(config);
  const json entry = FindCachedEntry(registry, args.alias, local_model_path.string());
  std::cout << "cache-status:\n";
  std::cout << "  alias=" << args.alias << "\n";
  std::cout << "  local_model_path=" << local_model_path.string() << "\n";
  std::cout << "  path_exists=" << (fs::exists(local_model_path) ? "yes" : "no") << "\n";
  std::cout << "  marker_exists=" << (fs::exists(marker_path) ? "yes" : "no") << "\n";
  std::cout << "  registry=" << (entry.empty() ? "missing" : "present") << "\n";
  if (!entry.empty()) {
    std::cout << "  source_model_id=" << entry.value("source_model_id", std::string{}) << "\n";
    std::cout << "  runtime_model_path=" << entry.value("runtime_model_path", std::string{})
              << "\n";
    std::cout << "  status=" << entry.value("status", std::string{"unknown"}) << "\n";
  }
  return (!entry.empty() || fs::exists(marker_path) || fs::exists(local_model_path)) ? 0 : 1;
}

void SwitchModel(
    const RuntimeConfig& config,
    const RuntimeProfile& profile,
    const InferCommandLineOptions& args,
    bool apply) {
  if (args.model_id.empty()) {
    Throw("switch-model requires --model-id");
  }
  if (args.tp <= 0 || args.pp <= 0) {
    Throw("--tp and --pp must be positive");
  }
  const int required_slots = args.tp * args.pp;
  const int available_slots = EnabledGpuNodeCount(config);
  if (available_slots < required_slots) {
    Throw("insufficient enabled gpu nodes");
  }

  const json registry = LoadRegistry(config);
  json cached_entry = json::object();
  for (const auto& entry : registry.value("entries", json::array())) {
    if (entry.value("alias", std::string{}) == args.model_id ||
        entry.value("source_model_id", std::string{}) == args.model_id) {
      cached_entry = entry;
      break;
    }
  }

  const std::string served_model_name =
      args.served_model_name.empty() ? SafeServedModelName(args.model_id) : args.served_model_name;
  const json current_active_model = LoadActiveModel(config);
  std::vector<std::string> llama_args = profile.llama_args;
  if (current_active_model.value("turboquant_enabled", false)) {
    const std::string cache_type_k =
        current_active_model.value("active_cache_type_k", std::string{});
    const std::string cache_type_v =
        current_active_model.value("active_cache_type_v", std::string{});
    if (!cache_type_k.empty() && !HasArgument(llama_args, "--cache-type-k")) {
      llama_args.push_back("--cache-type-k");
      llama_args.push_back(cache_type_k);
    }
    if (!cache_type_v.empty() && !HasArgument(llama_args, "--cache-type-v")) {
      llama_args.push_back("--cache-type-v");
      llama_args.push_back(cache_type_v);
    }
  }
  json payload{
      {"model_id", args.model_id},
      {"served_model_name", served_model_name},
      {"tensor_parallel_size", args.tp},
      {"pipeline_parallel_size", args.pp},
      {"required_gpu_slots", required_slots},
      {"available_gpu_slots", available_slots},
      {"gpu_memory_utilization", args.gpu_memory_utilization},
      {"runtime_profile", profile.name},
      {"primary_infer_node", config.primary_infer_node},
      {"llama_port", config.llama_port},
      {"cached_local_model_path", cached_entry.value("local_model_path", std::string{})},
      {"cached_runtime_model_path",
       cached_entry.value(
           "runtime_model_path",
           cached_entry.value("local_model_path", std::string{}))},
      {"llama_args", llama_args},
  };
  if (current_active_model.value("turboquant_enabled", false)) {
    payload["turboquant_enabled"] = true;
    payload["active_cache_type_k"] =
        current_active_model.value("active_cache_type_k", std::string{});
    payload["active_cache_type_v"] =
        current_active_model.value("active_cache_type_v", std::string{});
  }

  std::cout << "switch-model-plan:\n";
  for (const auto& key : {"model_id",
                          "served_model_name",
                          "tensor_parallel_size",
                          "pipeline_parallel_size",
                          "required_gpu_slots",
                          "available_gpu_slots",
                          "gpu_memory_utilization",
                          "runtime_profile",
                          "primary_infer_node",
                          "llama_port",
                          "cached_local_model_path",
                          "cached_runtime_model_path"}) {
    std::cout << "  " << key << "=" << payload.at(key) << "\n";
  }
  const std::vector<std::string> extra_args =
      payload.at("llama_args").get<std::vector<std::string>>();
  std::cout << "  llama_args=" << (extra_args.empty() ? "(empty)" : Join(extra_args, ","))
            << "\n";
  for (const auto& gpu_node : config.gpu_nodes) {
    if (!gpu_node.value("enabled", true)) {
      continue;
    }
    std::cout << "  gpu_node="
              << gpu_node.value("node_name", std::string{}) << "::"
              << gpu_node.value("name", std::string{}) << "::"
              << gpu_node.value("gpu_device", std::string{}) << " fraction="
              << gpu_node.value("gpu_fraction", 0.0) << "\n";
  }

  if (!apply) {
    return;
  }
  json active_model = json{{"version", 1}, {"plane_name", config.plane_name}};
  active_model.update(payload);
  SaveJsonFile(BuildControlPaths(config).active_model_path, active_model);
}

}  // namespace naim::infer::model_cache_support
