#pragma once

#include <cstdint>
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
  int temperature_c = 0;
  bool temperature_available = false;
  std::vector<GpuProcessTelemetry> processes;
};

struct GpuTelemetrySnapshot {
  int contract_version = 1;
  bool degraded = false;
  std::string source;
  std::string collected_at;
  std::vector<GpuDeviceTelemetry> devices;
};

struct DiskTelemetryRecord {
  std::string disk_name;
  std::string plane_name;
  std::string node_name;
  std::string mount_point;
  std::string mount_source;
  std::string filesystem_type;
  std::string runtime_state;
  std::string health;
  std::string status_message;
  std::uint64_t total_bytes = 0;
  std::uint64_t used_bytes = 0;
  std::uint64_t free_bytes = 0;
  std::uint64_t read_ios = 0;
  std::uint64_t write_ios = 0;
  std::uint64_t read_bytes = 0;
  std::uint64_t write_bytes = 0;
  std::uint64_t io_time_ms = 0;
  std::uint64_t weighted_io_time_ms = 0;
  std::uint64_t io_error_count = 0;
  int io_in_progress = 0;
  int warning_count = 0;
  int fault_count = 0;
  bool read_only = false;
  bool mounted = false;
  bool perf_counters_available = false;
  bool io_error_counters_available = false;
  std::vector<std::string> fault_reasons;
};

struct DiskTelemetrySnapshot {
  int contract_version = 1;
  bool degraded = false;
  std::string source;
  std::string collected_at;
  std::vector<DiskTelemetryRecord> items;
};

struct NetworkInterfaceTelemetry {
  std::string interface_name;
  std::string oper_state;
  std::string link_state;
  std::uint64_t rx_bytes = 0;
  std::uint64_t tx_bytes = 0;
  bool loopback = false;
};

struct NetworkTelemetrySnapshot {
  int contract_version = 1;
  bool degraded = false;
  std::string source;
  std::string collected_at;
  std::vector<NetworkInterfaceTelemetry> interfaces;
};

struct CpuTelemetrySnapshot {
  int contract_version = 1;
  bool degraded = false;
  std::string source;
  std::string collected_at;
  int core_count = 0;
  double utilization_pct = 0.0;
  double loadavg_1m = 0.0;
  double loadavg_5m = 0.0;
  double loadavg_15m = 0.0;
  double temperature_c = 0.0;
  double max_temperature_c = 0.0;
  bool temperature_available = false;
  std::uint64_t total_memory_bytes = 0;
  std::uint64_t available_memory_bytes = 0;
  std::uint64_t used_memory_bytes = 0;
};

struct RuntimeStatus {
  std::string plane_name;
  std::string control_root;
  std::string controller_url;
  std::string primary_infer_node;
  std::string instance_name;
  std::string instance_role;
  std::string node_name;
  std::string data_parallel_mode = "off";
  std::string data_parallel_lb_mode = "external";
  std::string runtime_backend;
  std::string runtime_phase;
  int enabled_gpu_nodes = 0;
  int data_parallel_size = 0;
  int data_parallel_size_local_max = 0;
  int replica_groups_expected = 0;
  int replica_groups_ready = 0;
  int replica_groups_degraded = 0;
  int api_endpoints_expected = 0;
  int api_endpoints_ready = 0;
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

class RuntimeStatusJsonCodec {
 public:
  std::string Serialize(const RuntimeStatus& status) const;
  RuntimeStatus Deserialize(const std::string& json_text) const;
  std::string SerializeList(const std::vector<RuntimeProcessStatus>& statuses) const;
  std::vector<RuntimeProcessStatus> DeserializeList(const std::string& json_text) const;
};

class GpuTelemetryJsonCodec {
 public:
  std::string Serialize(const GpuTelemetrySnapshot& snapshot) const;
  GpuTelemetrySnapshot Deserialize(const std::string& json_text) const;
};

class DiskTelemetryJsonCodec {
 public:
  std::string Serialize(const DiskTelemetrySnapshot& snapshot) const;
  DiskTelemetrySnapshot Deserialize(const std::string& json_text) const;
};

class NetworkTelemetryJsonCodec {
 public:
  std::string Serialize(const NetworkTelemetrySnapshot& snapshot) const;
  NetworkTelemetrySnapshot Deserialize(const std::string& json_text) const;
};

class CpuTelemetryJsonCodec {
 public:
  std::string Serialize(const CpuTelemetrySnapshot& snapshot) const;
  CpuTelemetrySnapshot Deserialize(const std::string& json_text) const;
};

class RuntimeStatusFileStore {
 public:
  std::optional<RuntimeStatus> Load(const std::string& path) const;
  void Save(const RuntimeStatus& status, const std::string& path) const;
};

std::string SerializeRuntimeStatusJson(const RuntimeStatus& status);
RuntimeStatus DeserializeRuntimeStatusJson(const std::string& json_text);
std::string SerializeRuntimeStatusListJson(const std::vector<RuntimeProcessStatus>& statuses);
std::vector<RuntimeProcessStatus> DeserializeRuntimeStatusListJson(const std::string& json_text);
std::string SerializeGpuTelemetryJson(const GpuTelemetrySnapshot& snapshot);
GpuTelemetrySnapshot DeserializeGpuTelemetryJson(const std::string& json_text);
std::string SerializeDiskTelemetryJson(const DiskTelemetrySnapshot& snapshot);
DiskTelemetrySnapshot DeserializeDiskTelemetryJson(const std::string& json_text);
std::string SerializeNetworkTelemetryJson(const NetworkTelemetrySnapshot& snapshot);
NetworkTelemetrySnapshot DeserializeNetworkTelemetryJson(const std::string& json_text);
std::string SerializeCpuTelemetryJson(const CpuTelemetrySnapshot& snapshot);
CpuTelemetrySnapshot DeserializeCpuTelemetryJson(const std::string& json_text);

std::optional<RuntimeStatus> LoadRuntimeStatusJson(const std::string& path);
void SaveRuntimeStatusJson(const RuntimeStatus& status, const std::string& path);

}  // namespace comet
