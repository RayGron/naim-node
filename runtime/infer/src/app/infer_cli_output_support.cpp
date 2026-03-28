#include "app/infer_cli_output_support.h"

#include <algorithm>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <stdexcept>
#include <string>
#include <vector>

namespace comet::infer::cli_output_support {

namespace fs = std::filesystem;
using control_support::BuildControlPaths;
using control_support::EnabledGpuNodeCount;
using control_support::LoadRuntimeAssetsStatus;
using control_support::ResolveProfile;
using control_support::RuntimeDirs;
using control_support::RuntimeProfile;
using nlohmann::json;

namespace {

template <typename T>
T Require(const json& object, const char* key, const char* context) {
  if (!object.contains(key)) {
    throw std::runtime_error(
        std::string("missing required key '") + key + "' in " + context);
  }
  return object.at(key).get<T>();
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

void SaveJsonFile(const fs::path& path, const json& value) {
  if (!path.parent_path().empty()) {
    fs::create_directories(path.parent_path());
  }
  std::ofstream output(path);
  if (!output.is_open()) {
    throw std::runtime_error("failed to open json file for writing: " + path.string());
  }
  output << value.dump(2) << "\n";
}

}  // namespace

void PrintConfigSummary(const RuntimeConfig& config) {
  std::cout << "infer runtime config: OK\n";
  std::cout << "plane_name=" << config.plane_name << "\n";
  std::cout << "control_root=" << config.control_root << "\n";
  std::cout << "controller_url=" << config.controller_url << "\n";
  std::cout << "runtime_engine=" << config.runtime_engine << "\n";
  std::cout << "gpu_node_count=" << config.gpu_nodes.size() << "\n";
  std::cout << "enabled_gpu_node_count=" << EnabledGpuNodeCount(config) << "\n";
  std::cout << "primary_infer_node=" << config.primary_infer_node << "\n";
  std::cout << "models_root=" << config.models_root << "\n";
  std::cout << "api_port=" << config.api_port << "\n";
  std::cout << "gateway=" << config.gateway_listen_host << ":" << config.gateway_listen_port
            << "\n";
}

void PrintPrepareRuntime(const RuntimeConfig& config, bool apply) {
  const char* verb = apply ? "created" : "would-create";
  for (const auto& path : RuntimeDirs(config)) {
    std::cout << verb << "=" << path.string() << "\n";
    if (apply) {
      fs::create_directories(path);
    }
  }
}

void PrintListProfiles(const json& profiles_json) {
  std::cout << "runtime profiles:\n";
  const json profiles = Require<json>(profiles_json, "profiles", "root");
  std::vector<std::string> names;
  for (auto it = profiles.begin(); it != profiles.end(); ++it) {
    names.push_back(it.key());
  }
  std::sort(names.begin(), names.end());
  for (const auto& name : names) {
    const RuntimeProfile profile = ResolveProfile(profiles_json, name);
    std::cout << "  - " << name << "\n";
    std::cout << "    llama_args="
              << (profile.llama_args.empty() ? "(empty)" : Join(profile.llama_args, ","))
              << "\n";
    if (profile.ctx_size.has_value()) {
      std::cout << "    ctx_size=" << *profile.ctx_size << "\n";
    }
    if (profile.batch_size.has_value()) {
      std::cout << "    batch_size=" << *profile.batch_size << "\n";
    }
    if (profile.gpu_layers.has_value()) {
      std::cout << "    gpu_layers=" << *profile.gpu_layers << "\n";
    }
  }
}

void BootstrapRuntime(
    const RuntimeConfig& config,
    const RuntimeProfile& profile,
    bool apply) {
  std::cout << "bootstrap-runtime:\n";
  std::cout << "  profile=" << profile.name << "\n";
  std::cout << "  plane=" << config.plane_name << "\n";
  std::cout << "  control_root=" << config.control_root << "\n";
  std::cout << "  models_root=" << config.models_root << "\n";
  std::cout << "  gguf_cache_dir=" << config.gguf_cache_dir << "\n";
  std::cout << "  runtime_mode=llama-library\n";
  std::cout << "  llama_args="
            << (profile.llama_args.empty() ? "(empty)" : Join(profile.llama_args, ","))
            << "\n";
  if (profile.ctx_size.has_value()) {
    std::cout << "  ctx_size=" << *profile.ctx_size << "\n";
  }
  if (profile.batch_size.has_value()) {
    std::cout << "  batch_size=" << *profile.batch_size << "\n";
  }
  if (profile.gpu_layers.has_value()) {
    std::cout << "  gpu_layers=" << *profile.gpu_layers << "\n";
  }
  if (apply) {
    SaveJsonFile(
        BuildControlPaths(config).runtime_assets_path,
        json{
            {"version", 1},
            {"plane_name", config.plane_name},
            {"runtime_profile", profile.name},
            {"runtime_mode", "llama-library"},
            {"llama_args", profile.llama_args},
            {"models_root", config.models_root},
            {"gguf_cache_dir", config.gguf_cache_dir},
        });
  }
  PrintPrepareRuntime(config, apply);
}

int PrintRuntimeAssetsStatus(const RuntimeConfig& config) {
  const json payload = LoadRuntimeAssetsStatus(config);
  if (payload.empty()) {
    std::cout << "(empty)\n";
    return 1;
  }
  std::cout << "runtime-assets-status:\n";
  std::cout << "  runtime_profile="
            << payload.value("runtime_profile", std::string{"(empty)"}) << "\n";
  std::cout << "  runtime_mode="
            << payload.value("runtime_mode", std::string{"llama-library"}) << "\n";
  std::cout << "  models_root="
            << payload.value("models_root", std::string{"(empty)"}) << "\n";
  std::cout << "  gguf_cache_dir="
            << payload.value("gguf_cache_dir", std::string{"(empty)"}) << "\n";
  std::cout << "  llama_args="
            << Join(payload.value("llama_args", std::vector<std::string>{}), ",") << "\n";
  return 0;
}

void PrintLaunchPlan(const RuntimeConfig& config) {
  std::cout << "launch-plan:\n";
  const json worker_group = config.worker_group;
  std::cout << "  runtime_engine=" << config.runtime_engine << "\n";
  std::cout << "  primary-infer=node:" << config.primary_infer_node
            << " api_port:" << config.api_port
            << " ctx_size:" << config.llama_ctx_size
            << " threads:" << config.llama_threads
            << " gpu_layers:" << config.llama_gpu_layers
            << " net_if:" << config.net_if << "\n";
  for (const auto& serving_worker : config.serving_workers) {
    if (!serving_worker.value("enabled", true)) {
      continue;
    }
    const std::string node_name = serving_worker.value("node_name", std::string{});
    const std::string name = serving_worker.value("name", std::string{});
    const std::string gpu_device = serving_worker.value("gpu_device", std::string{});
    const double gpu_fraction = serving_worker.value("gpu_fraction", 0.0);
    const bool colocated_with_primary_infer =
        serving_worker.value("colocated_with_primary_infer", node_name == config.primary_infer_node);
    std::cout << "  serving-worker=node:" << node_name << " worker:" << name
              << " gpu:" << gpu_device << " fraction:" << gpu_fraction
              << " colocated_with_primary_infer:"
              << (colocated_with_primary_infer ? "yes" : "no") << "\n";
  }
  if (worker_group.is_object()) {
    std::cout << "  worker-group=id:" << worker_group.value("group_id", std::string{})
              << " backend:" << worker_group.value("distributed_backend", std::string{"vllm"})
              << " expected_workers:" << worker_group.value("expected_workers", 0)
              << " rendezvous_port:" << worker_group.value("rendezvous_port", 29500) << "\n";
  }
  if (config.runtime_engine == "vllm") {
    std::cout << "  vllm=head:" << config.primary_infer_node << " port:" << config.api_port
              << " model_cache_dir:" << config.model_cache_dir
              << " log_dir:" << config.infer_log_dir << "\n";
  } else {
    std::cout << "  llama.cpp=head:" << config.primary_infer_node << " port:" << config.llama_port
              << " gguf_cache_dir:" << config.gguf_cache_dir
              << " log_dir:" << config.infer_log_dir << "\n";
  }
  std::cout << "  gateway=listen:" << config.gateway_listen_host << ":"
            << config.gateway_listen_port
            << " server_name:" << config.gateway_server_name << "\n";
}

}  // namespace comet::infer::cli_output_support
