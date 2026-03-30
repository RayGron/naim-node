#include "comet/runtime/runtime_status.h"

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
      {"temperature_c", device.temperature_c},
      {"temperature_available", device.temperature_available},
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
  device.temperature_c = value.value("temperature_c", 0);
  device.temperature_available = value.value("temperature_available", false);
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
      {"contract_version", snapshot.contract_version},
      {"degraded", snapshot.degraded},
      {"source", snapshot.source},
      {"collected_at", snapshot.collected_at},
      {"devices", std::move(devices)},
  };
}

GpuTelemetrySnapshot GpuTelemetrySnapshotFromJson(const json& value) {
  GpuTelemetrySnapshot snapshot;
  snapshot.contract_version = value.value("contract_version", 1);
  snapshot.degraded = value.value("degraded", false);
  snapshot.source = value.value("source", std::string{});
  snapshot.collected_at = value.value("collected_at", std::string{});
  for (const auto& device : value.value("devices", json::array())) {
    if (device.is_object()) {
      snapshot.devices.push_back(GpuDeviceTelemetryFromJson(device));
    }
  }
  return snapshot;
}

json ToJson(const DiskTelemetryRecord& record) {
  return json{
      {"disk_name", record.disk_name},
      {"plane_name", record.plane_name},
      {"node_name", record.node_name},
      {"mount_point", record.mount_point},
      {"mount_source", record.mount_source},
      {"filesystem_type", record.filesystem_type},
      {"runtime_state", record.runtime_state},
      {"health", record.health},
      {"status_message", record.status_message},
      {"total_bytes", record.total_bytes},
      {"used_bytes", record.used_bytes},
      {"free_bytes", record.free_bytes},
      {"read_ios", record.read_ios},
      {"write_ios", record.write_ios},
      {"read_bytes", record.read_bytes},
      {"write_bytes", record.write_bytes},
      {"io_time_ms", record.io_time_ms},
      {"weighted_io_time_ms", record.weighted_io_time_ms},
      {"io_error_count", record.io_error_count},
      {"io_in_progress", record.io_in_progress},
      {"warning_count", record.warning_count},
      {"fault_count", record.fault_count},
      {"read_only", record.read_only},
      {"mounted", record.mounted},
      {"perf_counters_available", record.perf_counters_available},
      {"io_error_counters_available", record.io_error_counters_available},
      {"fault_reasons", record.fault_reasons},
  };
}

DiskTelemetryRecord DiskTelemetryRecordFromJson(const json& value) {
  DiskTelemetryRecord record;
  record.disk_name = value.value("disk_name", std::string{});
  record.plane_name = value.value("plane_name", std::string{});
  record.node_name = value.value("node_name", std::string{});
  record.mount_point = value.value("mount_point", std::string{});
  record.mount_source = value.value("mount_source", std::string{});
  record.filesystem_type = value.value("filesystem_type", std::string{});
  record.runtime_state = value.value("runtime_state", std::string{});
  record.health = value.value("health", std::string{});
  record.status_message = value.value("status_message", std::string{});
  record.total_bytes = value.value("total_bytes", static_cast<std::uint64_t>(0));
  record.used_bytes = value.value("used_bytes", static_cast<std::uint64_t>(0));
  record.free_bytes = value.value("free_bytes", static_cast<std::uint64_t>(0));
  record.read_ios = value.value("read_ios", static_cast<std::uint64_t>(0));
  record.write_ios = value.value("write_ios", static_cast<std::uint64_t>(0));
  record.read_bytes = value.value("read_bytes", static_cast<std::uint64_t>(0));
  record.write_bytes = value.value("write_bytes", static_cast<std::uint64_t>(0));
  record.io_time_ms = value.value("io_time_ms", static_cast<std::uint64_t>(0));
  record.weighted_io_time_ms =
      value.value("weighted_io_time_ms", static_cast<std::uint64_t>(0));
  record.io_error_count = value.value("io_error_count", static_cast<std::uint64_t>(0));
  record.io_in_progress = value.value("io_in_progress", 0);
  record.warning_count = value.value("warning_count", 0);
  record.fault_count = value.value("fault_count", 0);
  record.read_only = value.value("read_only", false);
  record.mounted = value.value("mounted", false);
  record.perf_counters_available = value.value("perf_counters_available", false);
  record.io_error_counters_available = value.value("io_error_counters_available", false);
  for (const auto& item : value.value("fault_reasons", json::array())) {
    if (item.is_string()) {
      record.fault_reasons.push_back(item.get<std::string>());
    }
  }
  return record;
}

json ToJson(const DiskTelemetrySnapshot& snapshot) {
  json items = json::array();
  for (const auto& item : snapshot.items) {
    items.push_back(ToJson(item));
  }
  return json{
      {"contract_version", snapshot.contract_version},
      {"degraded", snapshot.degraded},
      {"source", snapshot.source},
      {"collected_at", snapshot.collected_at},
      {"items", std::move(items)},
  };
}

DiskTelemetrySnapshot DiskTelemetrySnapshotFromJson(const json& value) {
  DiskTelemetrySnapshot snapshot;
  snapshot.contract_version = value.value("contract_version", 1);
  snapshot.degraded = value.value("degraded", false);
  snapshot.source = value.value("source", std::string{});
  snapshot.collected_at = value.value("collected_at", std::string{});
  for (const auto& item : value.value("items", json::array())) {
    if (item.is_object()) {
      snapshot.items.push_back(DiskTelemetryRecordFromJson(item));
    }
  }
  return snapshot;
}

json ToJson(const NetworkInterfaceTelemetry& interface) {
  return json{
      {"interface_name", interface.interface_name},
      {"oper_state", interface.oper_state},
      {"link_state", interface.link_state},
      {"rx_bytes", interface.rx_bytes},
      {"tx_bytes", interface.tx_bytes},
      {"loopback", interface.loopback},
  };
}

NetworkInterfaceTelemetry NetworkInterfaceTelemetryFromJson(const json& value) {
  NetworkInterfaceTelemetry interface;
  interface.interface_name = value.value("interface_name", std::string{});
  interface.oper_state = value.value("oper_state", std::string{});
  interface.link_state = value.value("link_state", std::string{});
  interface.rx_bytes = value.value("rx_bytes", static_cast<std::uint64_t>(0));
  interface.tx_bytes = value.value("tx_bytes", static_cast<std::uint64_t>(0));
  interface.loopback = value.value("loopback", false);
  return interface;
}

json ToJson(const NetworkTelemetrySnapshot& snapshot) {
  json interfaces = json::array();
  for (const auto& interface : snapshot.interfaces) {
    interfaces.push_back(ToJson(interface));
  }
  return json{
      {"contract_version", snapshot.contract_version},
      {"degraded", snapshot.degraded},
      {"source", snapshot.source},
      {"collected_at", snapshot.collected_at},
      {"interfaces", std::move(interfaces)},
  };
}

NetworkTelemetrySnapshot NetworkTelemetrySnapshotFromJson(const json& value) {
  NetworkTelemetrySnapshot snapshot;
  snapshot.contract_version = value.value("contract_version", 1);
  snapshot.degraded = value.value("degraded", false);
  snapshot.source = value.value("source", std::string{});
  snapshot.collected_at = value.value("collected_at", std::string{});
  for (const auto& interface : value.value("interfaces", json::array())) {
    if (interface.is_object()) {
      snapshot.interfaces.push_back(NetworkInterfaceTelemetryFromJson(interface));
    }
  }
  return snapshot;
}

json ToJson(const CpuTelemetrySnapshot& snapshot) {
  return json{
      {"contract_version", snapshot.contract_version},
      {"degraded", snapshot.degraded},
      {"source", snapshot.source},
      {"collected_at", snapshot.collected_at},
      {"core_count", snapshot.core_count},
      {"utilization_pct", snapshot.utilization_pct},
      {"loadavg_1m", snapshot.loadavg_1m},
      {"loadavg_5m", snapshot.loadavg_5m},
      {"loadavg_15m", snapshot.loadavg_15m},
      {"temperature_c", snapshot.temperature_c},
      {"max_temperature_c", snapshot.max_temperature_c},
      {"temperature_available", snapshot.temperature_available},
      {"total_memory_bytes", snapshot.total_memory_bytes},
      {"available_memory_bytes", snapshot.available_memory_bytes},
      {"used_memory_bytes", snapshot.used_memory_bytes},
  };
}

CpuTelemetrySnapshot CpuTelemetrySnapshotFromJson(const json& value) {
  CpuTelemetrySnapshot snapshot;
  snapshot.contract_version = value.value("contract_version", 1);
  snapshot.degraded = value.value("degraded", false);
  snapshot.source = value.value("source", std::string{});
  snapshot.collected_at = value.value("collected_at", std::string{});
  snapshot.core_count = value.value("core_count", 0);
  snapshot.utilization_pct = value.value("utilization_pct", 0.0);
  snapshot.loadavg_1m = value.value("loadavg_1m", 0.0);
  snapshot.loadavg_5m = value.value("loadavg_5m", 0.0);
  snapshot.loadavg_15m = value.value("loadavg_15m", 0.0);
  snapshot.temperature_c = value.value("temperature_c", 0.0);
  snapshot.max_temperature_c = value.value("max_temperature_c", 0.0);
  snapshot.temperature_available = value.value("temperature_available", false);
  snapshot.total_memory_bytes =
      value.value("total_memory_bytes", static_cast<std::uint64_t>(0));
  snapshot.available_memory_bytes =
      value.value("available_memory_bytes", static_cast<std::uint64_t>(0));
  snapshot.used_memory_bytes =
      value.value("used_memory_bytes", static_cast<std::uint64_t>(0));
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
      {"data_parallel_mode", status.data_parallel_mode},
      {"data_parallel_lb_mode", status.data_parallel_lb_mode},
      {"runtime_backend", status.runtime_backend},
      {"runtime_phase", status.runtime_phase},
      {"enabled_gpu_nodes", status.enabled_gpu_nodes},
      {"data_parallel_size", status.data_parallel_size},
      {"data_parallel_size_local_max", status.data_parallel_size_local_max},
      {"replica_groups_expected", status.replica_groups_expected},
      {"replica_groups_ready", status.replica_groups_ready},
      {"replica_groups_degraded", status.replica_groups_degraded},
      {"api_endpoints_expected", status.api_endpoints_expected},
      {"api_endpoints_ready", status.api_endpoints_ready},
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
      {"rpc_endpoint", status.rpc_endpoint},
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
  status.data_parallel_mode = value.value("data_parallel_mode", std::string("off"));
  status.data_parallel_lb_mode =
      value.value("data_parallel_lb_mode", std::string("external"));
  status.runtime_backend = value.value("runtime_backend", std::string{});
  status.runtime_phase = value.value("runtime_phase", std::string{});
  status.enabled_gpu_nodes = value.value("enabled_gpu_nodes", 0);
  status.data_parallel_size = value.value("data_parallel_size", 0);
  status.data_parallel_size_local_max = value.value("data_parallel_size_local_max", 0);
  status.replica_groups_expected = value.value("replica_groups_expected", 0);
  status.replica_groups_ready = value.value("replica_groups_ready", 0);
  status.replica_groups_degraded = value.value("replica_groups_degraded", 0);
  status.api_endpoints_expected = value.value("api_endpoints_expected", 0);
  status.api_endpoints_ready = value.value("api_endpoints_ready", 0);
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
  status.rpc_endpoint = value.value("rpc_endpoint", std::string{});
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
  return RuntimeStatusJsonCodec().Serialize(status);
}

RuntimeStatus DeserializeRuntimeStatusJson(const std::string& json_text) {
  return RuntimeStatusJsonCodec().Deserialize(json_text);
}

std::string SerializeRuntimeStatusListJson(const std::vector<RuntimeProcessStatus>& statuses) {
  return RuntimeStatusJsonCodec().SerializeList(statuses);
}

std::vector<RuntimeProcessStatus> DeserializeRuntimeStatusListJson(const std::string& json_text) {
  return RuntimeStatusJsonCodec().DeserializeList(json_text);
}

std::string GpuTelemetryJsonCodec::Serialize(const GpuTelemetrySnapshot& snapshot) const {
  return ToJson(snapshot).dump(2);
}

GpuTelemetrySnapshot GpuTelemetryJsonCodec::Deserialize(const std::string& json_text) const {
  return GpuTelemetrySnapshotFromJson(json::parse(json_text));
}

std::string DiskTelemetryJsonCodec::Serialize(const DiskTelemetrySnapshot& snapshot) const {
  return ToJson(snapshot).dump(2);
}

DiskTelemetrySnapshot DiskTelemetryJsonCodec::Deserialize(const std::string& json_text) const {
  return DiskTelemetrySnapshotFromJson(json::parse(json_text));
}

std::string NetworkTelemetryJsonCodec::Serialize(const NetworkTelemetrySnapshot& snapshot) const {
  return ToJson(snapshot).dump(2);
}

NetworkTelemetrySnapshot NetworkTelemetryJsonCodec::Deserialize(
    const std::string& json_text) const {
  return NetworkTelemetrySnapshotFromJson(json::parse(json_text));
}

std::string CpuTelemetryJsonCodec::Serialize(const CpuTelemetrySnapshot& snapshot) const {
  return ToJson(snapshot).dump(2);
}

CpuTelemetrySnapshot CpuTelemetryJsonCodec::Deserialize(const std::string& json_text) const {
  return CpuTelemetrySnapshotFromJson(json::parse(json_text));
}

std::string RuntimeStatusJsonCodec::Serialize(const RuntimeStatus& status) const {
  return ToJson(status).dump(2);
}

RuntimeStatus RuntimeStatusJsonCodec::Deserialize(const std::string& json_text) const {
  return RuntimeStatusFromJson(json::parse(json_text));
}

std::string RuntimeStatusJsonCodec::SerializeList(
    const std::vector<RuntimeProcessStatus>& statuses) const {
  json payload = json::array();
  for (const auto& status : statuses) {
    payload.push_back(ToJson(status));
  }
  return payload.dump(2);
}

std::vector<RuntimeProcessStatus> RuntimeStatusJsonCodec::DeserializeList(
    const std::string& json_text) const {
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
  return GpuTelemetryJsonCodec().Serialize(snapshot);
}

GpuTelemetrySnapshot DeserializeGpuTelemetryJson(const std::string& json_text) {
  return GpuTelemetryJsonCodec().Deserialize(json_text);
}

std::string SerializeDiskTelemetryJson(const DiskTelemetrySnapshot& snapshot) {
  return DiskTelemetryJsonCodec().Serialize(snapshot);
}

DiskTelemetrySnapshot DeserializeDiskTelemetryJson(const std::string& json_text) {
  return DiskTelemetryJsonCodec().Deserialize(json_text);
}

std::string SerializeNetworkTelemetryJson(const NetworkTelemetrySnapshot& snapshot) {
  return NetworkTelemetryJsonCodec().Serialize(snapshot);
}

NetworkTelemetrySnapshot DeserializeNetworkTelemetryJson(const std::string& json_text) {
  return NetworkTelemetryJsonCodec().Deserialize(json_text);
}

std::string SerializeCpuTelemetryJson(const CpuTelemetrySnapshot& snapshot) {
  return CpuTelemetryJsonCodec().Serialize(snapshot);
}

CpuTelemetrySnapshot DeserializeCpuTelemetryJson(const std::string& json_text) {
  return CpuTelemetryJsonCodec().Deserialize(json_text);
}

std::optional<RuntimeStatus> RuntimeStatusFileStore::Load(const std::string& path) const {
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

void RuntimeStatusFileStore::Save(const RuntimeStatus& status, const std::string& path) const {
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

std::optional<RuntimeStatus> LoadRuntimeStatusJson(const std::string& path) {
  return RuntimeStatusFileStore().Load(path);
}

void SaveRuntimeStatusJson(const RuntimeStatus& status, const std::string& path) {
  RuntimeStatusFileStore().Save(status, path);
}

}  // namespace comet
