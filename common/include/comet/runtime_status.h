#pragma once

#include <optional>
#include <string>
#include <vector>

namespace comet {

struct RuntimeProcessStatus {
  std::string instance_name;
  std::string instance_role;
  std::string node_name;
  std::string model_path;
  std::string gpu_device;
  std::string runtime_phase;
  std::string started_at;
  std::string last_activity_at;
  int runtime_pid = 0;
  int engine_pid = 0;
  bool ready = false;
};

struct GpuProcessTelemetry {
  int pid = 0;
  int used_vram_mb = 0;
  std::string instance_name = "unknown";
};

struct GpuDeviceTelemetry {
  std::string gpu_device;
  int total_vram_mb = 0;
  int used_vram_mb = 0;
  int free_vram_mb = 0;
  int gpu_utilization_pct = 0;
  std::vector<GpuProcessTelemetry> processes;
};

struct GpuTelemetrySnapshot {
  bool degraded = false;
  std::string source;
  std::vector<GpuDeviceTelemetry> devices;
};

struct RuntimeStatus {
  std::string plane_name;
  std::string control_root;
  std::string controller_url;
  std::string primary_infer_node;
  std::string instance_name;
  std::string instance_role;
  std::string node_name;
  std::string runtime_backend;
  std::string runtime_phase;
  int enabled_gpu_nodes = 0;
  int registry_entries = 0;
  int supervisor_pid = 0;
  int runtime_pid = 0;
  int engine_pid = 0;
  std::vector<std::string> aliases;
  std::string active_model_id;
  std::string active_served_model_name;
  std::string active_runtime_profile;
  std::string cached_local_model_path;
  std::string model_path;
  std::string gpu_device;
  std::string gateway_listen;
  std::string upstream_models_url;
  std::string inference_health_url;
  std::string gateway_health_url;
  std::string started_at;
  std::string last_activity_at;
  bool ready = false;
  bool active_model_ready = false;
  bool gateway_plan_ready = false;
  bool inference_ready = false;
  bool gateway_ready = false;
  bool launch_ready = false;
};

std::string SerializeRuntimeStatusJson(const RuntimeStatus& status);
RuntimeStatus DeserializeRuntimeStatusJson(const std::string& json_text);
std::string SerializeRuntimeStatusListJson(const std::vector<RuntimeProcessStatus>& statuses);
std::vector<RuntimeProcessStatus> DeserializeRuntimeStatusListJson(const std::string& json_text);
std::string SerializeGpuTelemetryJson(const GpuTelemetrySnapshot& snapshot);
GpuTelemetrySnapshot DeserializeGpuTelemetryJson(const std::string& json_text);

std::optional<RuntimeStatus> LoadRuntimeStatusJson(const std::string& path);
void SaveRuntimeStatusJson(const RuntimeStatus& status, const std::string& path);

}  // namespace comet
