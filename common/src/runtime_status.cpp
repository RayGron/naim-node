#include "comet/runtime_status.h"

#include <filesystem>
#include <fstream>
#include <stdexcept>

#include <nlohmann/json.hpp>

namespace comet {

namespace {

using nlohmann::json;

json ToJson(const RuntimeProcessStatus& status) {
  return json{
      {"instance_name", status.instance_name},
      {"instance_role", status.instance_role},
      {"node_name", status.node_name},
      {"model_path", status.model_path},
      {"gpu_device", status.gpu_device},
      {"runtime_phase", status.runtime_phase},
      {"started_at", status.started_at},
      {"last_activity_at", status.last_activity_at},
      {"runtime_pid", status.runtime_pid},
      {"engine_pid", status.engine_pid},
      {"ready", status.ready},
  };
}

RuntimeProcessStatus RuntimeProcessStatusFromJson(const json& value) {
  RuntimeProcessStatus status;
  status.instance_name = value.value("instance_name", std::string{});
  status.instance_role = value.value("instance_role", std::string{});
  status.node_name = value.value("node_name", std::string{});
  status.model_path = value.value("model_path", std::string{});
  status.gpu_device = value.value("gpu_device", std::string{});
  status.runtime_phase = value.value("runtime_phase", std::string{});
  status.started_at = value.value("started_at", std::string{});
  status.last_activity_at = value.value("last_activity_at", std::string{});
  status.runtime_pid = value.value("runtime_pid", 0);
  status.engine_pid = value.value("engine_pid", 0);
  status.ready = value.value("ready", false);
  return status;
}

json ToJson(const GpuProcessTelemetry& process) {
  return json{
      {"pid", process.pid},
      {"used_vram_mb", process.used_vram_mb},
      {"instance_name", process.instance_name},
  };
}

GpuProcessTelemetry GpuProcessTelemetryFromJson(const json& value) {
  GpuProcessTelemetry process;
  process.pid = value.value("pid", 0);
  process.used_vram_mb = value.value("used_vram_mb", 0);
  process.instance_name = value.value("instance_name", std::string{"unknown"});
  return process;
}

json ToJson(const GpuDeviceTelemetry& device) {
  json processes = json::array();
  for (const auto& process : device.processes) {
    processes.push_back(ToJson(process));
  }
  return json{
      {"gpu_device", device.gpu_device},
      {"total_vram_mb", device.total_vram_mb},
      {"used_vram_mb", device.used_vram_mb},
      {"free_vram_mb", device.free_vram_mb},
      {"gpu_utilization_pct", device.gpu_utilization_pct},
      {"processes", std::move(processes)},
  };
}

GpuDeviceTelemetry GpuDeviceTelemetryFromJson(const json& value) {
  GpuDeviceTelemetry device;
  device.gpu_device = value.value("gpu_device", std::string{});
  device.total_vram_mb = value.value("total_vram_mb", 0);
  device.used_vram_mb = value.value("used_vram_mb", 0);
  device.free_vram_mb = value.value("free_vram_mb", 0);
  device.gpu_utilization_pct = value.value("gpu_utilization_pct", 0);
  for (const auto& process : value.value("processes", json::array())) {
    if (process.is_object()) {
      device.processes.push_back(GpuProcessTelemetryFromJson(process));
    }
  }
  return device;
}

json ToJson(const GpuTelemetrySnapshot& snapshot) {
  json devices = json::array();
  for (const auto& device : snapshot.devices) {
    devices.push_back(ToJson(device));
  }
  return json{
      {"degraded", snapshot.degraded},
      {"source", snapshot.source},
      {"devices", std::move(devices)},
  };
}

GpuTelemetrySnapshot GpuTelemetrySnapshotFromJson(const json& value) {
  GpuTelemetrySnapshot snapshot;
  snapshot.degraded = value.value("degraded", false);
  snapshot.source = value.value("source", std::string{});
  for (const auto& device : value.value("devices", json::array())) {
    if (device.is_object()) {
      snapshot.devices.push_back(GpuDeviceTelemetryFromJson(device));
    }
  }
  return snapshot;
}

json ToJson(const RuntimeStatus& status) {
  return json{
      {"plane_name", status.plane_name},
      {"control_root", status.control_root},
      {"controller_url", status.controller_url},
      {"primary_infer_node", status.primary_infer_node},
      {"instance_name", status.instance_name},
      {"instance_role", status.instance_role},
      {"node_name", status.node_name},
      {"runtime_backend", status.runtime_backend},
      {"runtime_phase", status.runtime_phase},
      {"enabled_gpu_nodes", status.enabled_gpu_nodes},
      {"registry_entries", status.registry_entries},
      {"supervisor_pid", status.supervisor_pid},
      {"runtime_pid", status.runtime_pid},
      {"engine_pid", status.engine_pid},
      {"aliases", status.aliases},
      {"active_model_id", status.active_model_id},
      {"active_served_model_name", status.active_served_model_name},
      {"active_runtime_profile", status.active_runtime_profile},
      {"cached_local_model_path", status.cached_local_model_path},
      {"model_path", status.model_path},
      {"gpu_device", status.gpu_device},
      {"gateway_listen", status.gateway_listen},
      {"upstream_models_url", status.upstream_models_url},
      {"inference_health_url", status.inference_health_url},
      {"gateway_health_url", status.gateway_health_url},
      {"started_at", status.started_at},
      {"last_activity_at", status.last_activity_at},
      {"ready", status.ready},
      {"active_model_ready", status.active_model_ready},
      {"gateway_plan_ready", status.gateway_plan_ready},
      {"inference_ready", status.inference_ready},
      {"gateway_ready", status.gateway_ready},
      {"launch_ready", status.launch_ready},
  };
}

RuntimeStatus RuntimeStatusFromJson(const json& value) {
  RuntimeStatus status;
  status.plane_name = value.value("plane_name", std::string{});
  status.control_root = value.value("control_root", std::string{});
  status.controller_url = value.value("controller_url", std::string{});
  status.primary_infer_node = value.value("primary_infer_node", std::string{});
  status.instance_name = value.value("instance_name", std::string{});
  status.instance_role = value.value("instance_role", std::string{});
  status.node_name = value.value("node_name", std::string{});
  status.runtime_backend = value.value("runtime_backend", std::string{});
  status.runtime_phase = value.value("runtime_phase", std::string{});
  status.enabled_gpu_nodes = value.value("enabled_gpu_nodes", 0);
  status.registry_entries = value.value("registry_entries", 0);
  status.supervisor_pid = value.value("supervisor_pid", 0);
  status.runtime_pid = value.value("runtime_pid", 0);
  status.engine_pid = value.value("engine_pid", 0);
  status.aliases = value.value("aliases", std::vector<std::string>{});
  status.active_model_id = value.value("active_model_id", std::string{});
  status.active_served_model_name = value.value("active_served_model_name", std::string{});
  status.active_runtime_profile = value.value("active_runtime_profile", std::string{});
  status.cached_local_model_path = value.value("cached_local_model_path", std::string{});
  status.model_path = value.value("model_path", std::string{});
  status.gpu_device = value.value("gpu_device", std::string{});
  status.gateway_listen = value.value("gateway_listen", std::string{});
  status.upstream_models_url = value.value("upstream_models_url", std::string{});
  status.inference_health_url = value.value("inference_health_url", std::string{});
  status.gateway_health_url = value.value("gateway_health_url", std::string{});
  status.started_at = value.value("started_at", std::string{});
  status.last_activity_at = value.value("last_activity_at", std::string{});
  status.ready = value.value("ready", false);
  status.active_model_ready = value.value("active_model_ready", false);
  status.gateway_plan_ready = value.value("gateway_plan_ready", false);
  status.inference_ready = value.value("inference_ready", false);
  status.gateway_ready = value.value("gateway_ready", false);
  status.launch_ready = value.value("launch_ready", false);
  return status;
}

}  // namespace

std::string SerializeRuntimeStatusJson(const RuntimeStatus& status) {
  return ToJson(status).dump(2);
}

RuntimeStatus DeserializeRuntimeStatusJson(const std::string& json_text) {
  return RuntimeStatusFromJson(json::parse(json_text));
}

std::string SerializeRuntimeStatusListJson(const std::vector<RuntimeProcessStatus>& statuses) {
  json payload = json::array();
  for (const auto& status : statuses) {
    payload.push_back(ToJson(status));
  }
  return payload.dump(2);
}

std::vector<RuntimeProcessStatus> DeserializeRuntimeStatusListJson(const std::string& json_text) {
  const json value = json::parse(json_text);
  std::vector<RuntimeProcessStatus> statuses;
  for (const auto& entry : value) {
    if (entry.is_object()) {
      statuses.push_back(RuntimeProcessStatusFromJson(entry));
    }
  }
  return statuses;
}

std::string SerializeGpuTelemetryJson(const GpuTelemetrySnapshot& snapshot) {
  return ToJson(snapshot).dump(2);
}

GpuTelemetrySnapshot DeserializeGpuTelemetryJson(const std::string& json_text) {
  return GpuTelemetrySnapshotFromJson(json::parse(json_text));
}

std::optional<RuntimeStatus> LoadRuntimeStatusJson(const std::string& path) {
  if (!std::filesystem::exists(path)) {
    return std::nullopt;
  }

  std::ifstream input(path);
  if (!input.is_open()) {
    throw std::runtime_error("failed to open runtime status file: " + path);
  }

  json value;
  input >> value;
  return RuntimeStatusFromJson(value);
}

void SaveRuntimeStatusJson(const RuntimeStatus& status, const std::string& path) {
  const std::filesystem::path file_path(path);
  if (file_path.has_parent_path()) {
    std::filesystem::create_directories(file_path.parent_path());
  }

  std::ofstream output(path, std::ios::binary | std::ios::trunc);
  if (!output.is_open()) {
    throw std::runtime_error("failed to open runtime status file for write: " + path);
  }

  output << SerializeRuntimeStatusJson(status) << "\n";
  if (!output.good()) {
    throw std::runtime_error("failed to write runtime status file: " + path);
  }
}

}  // namespace comet
