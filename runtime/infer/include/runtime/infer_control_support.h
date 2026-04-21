#pragma once

#include <filesystem>
#include <optional>
#include <string>
#include <vector>

#include <nlohmann/json.hpp>

#include "runtime/infer_runtime_types.h"

namespace naim::infer::control_support {

namespace fs = std::filesystem;
using nlohmann::json;

struct RuntimeProfile {
  std::string name;
  std::vector<std::string> llama_args;
  std::optional<int> ctx_size;
  std::optional<int> batch_size;
  std::optional<int> gpu_layers;
};

struct ControlPaths {
  fs::path control_root;
  fs::path runtime_assets_path;
  fs::path registry_path;
  fs::path active_model_path;
  fs::path gateway_plan_path;
  fs::path prewarmed_replicas_path;
  fs::path runtime_status_path;
  fs::path worker_group_dir;
};

json LoadProfiles(const std::string& path_str);
RuntimeProfile ResolveProfile(const json& profiles_json, const std::string& name);
ControlPaths BuildControlPaths(const RuntimeConfig& config);
json LoadWorkerGroupStatus(const RuntimeConfig& config);
std::vector<fs::path> RuntimeDirs(const RuntimeConfig& config);
json LoadActiveModel(const RuntimeConfig& config);
json LoadGatewayPlan(const RuntimeConfig& config);
json LoadRegistry(const RuntimeConfig& config);
json LoadRuntimeAssetsStatus(const RuntimeConfig& config);
int EnabledGpuNodeCount(const RuntimeConfig& config);

}  // namespace naim::infer::control_support
