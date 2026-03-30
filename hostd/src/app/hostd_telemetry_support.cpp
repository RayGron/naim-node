#include "app/hostd_telemetry_support.h"

#include <algorithm>
#include <array>
#include <cctype>
#include <chrono>
#include <cstdlib>
#include <ctime>
#include <filesystem>
#include <fstream>
#include <map>
#include <optional>
#include <set>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#if !defined(_WIN32)
#include <dlfcn.h>
#include <sys/statvfs.h>
#endif

#include "app/hostd_controller_transport_support.h"
#include "app/hostd_local_state_support.h"
#include "comet/core/platform_compat.h"
#include "comet/runtime/infer_runtime_config.h"
#include "comet/runtime/runtime_status.h"
#include "comet/state/state_json.h"

namespace comet::hostd::telemetry_support {

namespace fs = std::filesystem;
using controller_transport_support::Trim;

namespace {

std::string CurrentTimestampString() {
  const std::time_t now = std::time(nullptr);
  std::tm tm{};
#if defined(_WIN32)
  localtime_s(&tm, &now);
#else
  localtime_r(&now, &tm);
#endif
  char buffer[32];
  if (std::strftime(buffer, sizeof(buffer), "%Y-%m-%d %H:%M:%S", &tm) == 0) {
    return {};
  }
  return buffer;
}

std::string RunCommandCapture(const std::string& command) {
  std::array<char, 512> buffer{};
  std::string output;
  FILE* pipe = comet::platform::OpenPipe(command.c_str(), "r");
  if (pipe == nullptr) {
    return output;
  }
  while (fgets(buffer.data(), static_cast<int>(buffer.size()), pipe) != nullptr) {
    output.append(buffer.data());
  }
  comet::platform::ClosePipe(pipe);
  return output;
}

bool RunCommandOk(const std::string& command) {
  return std::system(command.c_str()) == 0;
}

std::string ShellQuote(const std::string& value) {
  std::string quoted = "'";
  for (char ch : value) {
    if (ch == '\'') {
      quoted += "'\"'\"'";
    } else {
      quoted.push_back(ch);
    }
  }
  quoted += "'";
  return quoted;
}

std::string ResolvedDockerCommand() {
  static const std::string resolved = []() -> std::string {
    if (std::system("docker version >/dev/null 2>&1") == 0) {
      return "docker";
    }
    const std::string windows_docker =
        "/mnt/c/Program Files/Docker/Docker/resources/bin/docker.exe";
    if (fs::exists(windows_docker) &&
        std::system(("'" + windows_docker + "' version >/dev/null 2>&1").c_str()) == 0) {
      return "'" + windows_docker + "'";
    }
    return "docker";
  }();
  return resolved;
}

std::string NormalizeManagedPath(const std::string& path) {
  std::error_code error;
  const auto normalized = fs::weakly_canonical(path, error);
  if (!error) {
    return normalized.string();
  }
  return fs::path(path).lexically_normal().string();
}

std::string NormalizeMountPointPath(const std::string& mount_point) {
  return NormalizeManagedPath(mount_point);
}

bool IsPathMounted(const std::string& mount_point) {
  if (RunCommandOk(
          "/usr/bin/mountpoint -q " + ShellQuote(mount_point) + " >/dev/null 2>&1")) {
    return true;
  }
  const std::string normalized_mount_point = NormalizeMountPointPath(mount_point);
  return normalized_mount_point != mount_point &&
         RunCommandOk(
             "/usr/bin/mountpoint -q " + ShellQuote(normalized_mount_point) +
             " >/dev/null 2>&1");
}

std::optional<std::string> CurrentMountSource(const std::string& mount_point) {
  const std::array<std::string, 2> candidates = {
      mount_point,
      NormalizeMountPointPath(mount_point),
  };
  std::ifstream input("/proc/self/mounts");
  if (!input.is_open()) {
    return std::nullopt;
  }

  std::string source;
  std::string target;
  std::string fs_type;
  while (input >> source >> target >> fs_type) {
    std::string rest_of_line;
    std::getline(input, rest_of_line);
    for (const auto& candidate : candidates) {
      if (!candidate.empty() && target == candidate) {
        return source;
      }
    }
  }
  return std::nullopt;
}

std::optional<std::string> CurrentMountFilesystemType(const std::string& mount_point) {
  std::ifstream input("/proc/mounts");
  if (!input.is_open()) {
    return std::nullopt;
  }

  std::string source;
  std::string target;
  std::string fs_type;
  while (input >> source >> target >> fs_type) {
    std::string rest_of_line;
    std::getline(input, rest_of_line);
    if (target == mount_point) {
      return fs_type;
    }
  }
  return std::nullopt;
}

struct BlockDeviceIoStats {
  std::uint64_t read_ios = 0;
  std::uint64_t read_sectors = 0;
  std::uint64_t write_ios = 0;
  std::uint64_t write_sectors = 0;
  std::uint64_t io_in_progress = 0;
  std::uint64_t io_time_ms = 0;
  std::uint64_t weighted_io_time_ms = 0;
};

std::optional<std::string> BlockDeviceNameFromPath(const std::string& device_path) {
  if (device_path.empty()) {
    return std::nullopt;
  }
  const auto device_name = fs::path(device_path).filename().string();
  if (device_name.empty()) {
    return std::nullopt;
  }
  return device_name;
}

std::optional<bool> ReadBlockDeviceReadOnly(const std::string& device_path) {
  const auto device_name = BlockDeviceNameFromPath(device_path);
  if (!device_name.has_value()) {
    return std::nullopt;
  }
  std::ifstream input("/sys/class/block/" + *device_name + "/ro");
  if (!input.is_open()) {
    return std::nullopt;
  }
  int value = 0;
  if (!(input >> value)) {
    return std::nullopt;
  }
  return value != 0;
}

std::optional<std::uint64_t> ReadBlockDeviceIoErrorCount(const std::string& device_path) {
  const auto device_name = BlockDeviceNameFromPath(device_path);
  if (!device_name.has_value()) {
    return std::nullopt;
  }
  const std::array<fs::path, 2> candidates{
      fs::path("/sys/class/block") / *device_name / "ioerr_cnt",
      fs::path("/sys/class/block") / *device_name / "device" / "ioerr_cnt",
  };
  for (const auto& candidate : candidates) {
    std::ifstream input(candidate);
    if (!input.is_open()) {
      continue;
    }
    std::uint64_t value = 0;
    if (input >> value) {
      return value;
    }
  }
  return std::nullopt;
}

std::optional<BlockDeviceIoStats> ReadBlockDeviceIoStats(const std::string& device_path) {
  const auto device_name = BlockDeviceNameFromPath(device_path);
  if (!device_name.has_value()) {
    return std::nullopt;
  }
  std::ifstream input("/sys/class/block/" + *device_name + "/stat");
  if (!input.is_open()) {
    return std::nullopt;
  }

  BlockDeviceIoStats stats;
  std::uint64_t reads_merged = 0;
  std::uint64_t read_time_ms = 0;
  std::uint64_t writes_merged = 0;
  std::uint64_t write_time_ms = 0;
  if (!(input >> stats.read_ios >> reads_merged >> stats.read_sectors >> read_time_ms >>
        stats.write_ios >> writes_merged >> stats.write_sectors >> write_time_ms >>
        stats.io_in_progress >> stats.io_time_ms >> stats.weighted_io_time_ms)) {
    return std::nullopt;
  }
  return stats;
}

const comet::DiskSpec* FindSharedDiskForNode(
    const comet::DesiredState& state,
    const std::string& node_name) {
  for (const auto& disk : state.disks) {
    if (disk.node_name == node_name && disk.kind == comet::DiskKind::PlaneShared) {
      return &disk;
    }
  }
  return nullptr;
}

std::optional<std::string> ControlFilePathForNode(
    const comet::DesiredState& state,
    const std::string& node_name,
    const std::string& file_name) {
  const auto* shared_disk = FindSharedDiskForNode(state, node_name);
  if (shared_disk == nullptr) {
    return std::nullopt;
  }

  const fs::path control_root(state.control_root);
  const fs::path shared_container_path(shared_disk->container_path);
  fs::path relative_control_path;
  if (!state.control_root.empty() && control_root.is_absolute() &&
      shared_container_path.is_absolute()) {
    const auto control_text = control_root.generic_string();
    const auto shared_text = shared_container_path.generic_string();
    if (control_text == shared_text) {
      relative_control_path = ".";
    } else if (
        control_text.size() > shared_text.size() &&
        control_text.compare(0, shared_text.size(), shared_text) == 0 &&
        control_text[shared_text.size()] == '/') {
      relative_control_path = control_root.lexically_relative(shared_container_path);
    }
  }
  if (relative_control_path.empty()) {
    relative_control_path = fs::path("control") / state.plane_name;
  }

  return (fs::path(shared_disk->host_path) / relative_control_path / file_name).string();
}

std::optional<std::string> RuntimeStatusPathForNode(
    const comet::DesiredState& state,
    const std::string& node_name) {
  for (const auto& instance : state.instances) {
    if (instance.role == comet::InstanceRole::Infer &&
        instance.node_name == node_name &&
        !instance.name.empty()) {
      return ControlFilePathForNode(
          state,
          node_name,
          comet::InferRuntimeStatusRelativePath(instance.name));
    }
  }
  return ControlFilePathForNode(state, node_name, "runtime-status.json");
}

std::optional<std::string> InferRuntimeStatusPathForInstance(
    const comet::DesiredState& state,
    const comet::InstanceSpec& instance) {
  if (instance.role != comet::InstanceRole::Infer || instance.name.empty()) {
    return std::nullopt;
  }
  return ControlFilePathForNode(
      state,
      instance.node_name,
      comet::InferRuntimeStatusRelativePath(instance.name));
}

std::optional<std::string> WorkerRuntimeStatusPathForInstance(
    const comet::DesiredState& state,
    const comet::InstanceSpec& instance) {
  if (instance.role != comet::InstanceRole::Worker) {
    return std::nullopt;
  }
  for (const auto& disk : state.disks) {
    if (disk.kind == comet::DiskKind::WorkerPrivate && disk.owner_name == instance.name &&
        disk.node_name == instance.node_name) {
      return (fs::path(disk.host_path) / "worker-runtime-status.json").string();
    }
  }
  return std::nullopt;
}

comet::RuntimeProcessStatus ToProcessStatus(
    comet::RuntimeStatus status,
    const comet::InstanceSpec& instance) {
  if (status.instance_name.empty()) {
    status.instance_name = instance.name;
  }
  if (status.instance_role.empty()) {
    status.instance_role = comet::ToString(instance.role);
  }
  if (status.node_name.empty()) {
    status.node_name = instance.node_name;
  }
  if (status.gpu_device.empty() && instance.gpu_device.has_value()) {
    status.gpu_device = *instance.gpu_device;
  }
  comet::RuntimeProcessStatus process;
  process.instance_name = status.instance_name;
  process.instance_role = status.instance_role;
  process.node_name = status.node_name;
  process.model_path = status.model_path.empty() ? status.cached_local_model_path : status.model_path;
  process.gpu_device = status.gpu_device;
  process.runtime_phase = status.runtime_phase;
  process.started_at = status.started_at;
  process.last_activity_at = status.last_activity_at;
  process.runtime_pid = status.runtime_pid;
  process.engine_pid = status.engine_pid;
  process.ready = status.ready || status.launch_ready || status.inference_ready;
  return process;
}

std::vector<std::string> SplitCsvRow(const std::string& line) {
  std::vector<std::string> result;
  std::stringstream stream(line);
  std::string current;
  while (std::getline(stream, current, ',')) {
    result.push_back(Trim(current));
  }
  return result;
}

std::optional<std::string> ResolveComposeContainerIdForService(const std::string& service_name) {
  const std::string output =
      RunCommandCapture(
          ResolvedDockerCommand() +
          " ps --filter label=com.docker.compose.service=" + service_name +
          " --format '{{.ID}}' 2>/dev/null");
  std::istringstream input(output);
  std::string container_id;
  while (std::getline(input, container_id)) {
    container_id = Trim(container_id);
    if (!container_id.empty()) {
      return container_id;
    }
  }
  return std::nullopt;
}

std::optional<int> ResolveServiceHostPid(const std::string& service_name) {
  const auto container_id = ResolveComposeContainerIdForService(service_name);
  if (!container_id.has_value()) {
    return std::nullopt;
  }
  const std::string output =
      RunCommandCapture(
          ResolvedDockerCommand() + " top " + *container_id + " -eo pid,comm,args 2>/dev/null");
  std::istringstream input(output);
  std::string line;
  bool first = true;
  std::optional<int> fallback_pid;
  while (std::getline(input, line)) {
    if (first) {
      first = false;
      continue;
    }
    line = Trim(line);
    if (line.empty()) {
      continue;
    }
    std::istringstream row(line);
    int pid = 0;
    std::string comm;
    row >> pid >> comm;
    std::string args;
    std::getline(row, args);
    args = Trim(args);
    if (pid <= 0) {
      continue;
    }
    if (args.find("comet-workerd") != std::string::npos ||
        args.find("comet-inferctl") != std::string::npos) {
      return pid;
    }
    if (comm != "tini" && comm != "bash" && comm != "sh" && !fallback_pid.has_value()) {
      fallback_pid = pid;
    }
  }
  return fallback_pid;
}

void ResolveInstanceHostPids(std::vector<comet::RuntimeProcessStatus>* statuses) {
  if (statuses == nullptr) {
    return;
  }
  for (auto& status : *statuses) {
    const auto host_pid = ResolveServiceHostPid(status.instance_name);
    if (host_pid.has_value()) {
      status.runtime_pid = *host_pid;
      status.engine_pid = *host_pid;
    }
  }
}

std::optional<std::string> ParseTaggedValue(const std::string& text, const std::string& key) {
  const std::string needle = key + "=";
  const std::size_t begin = text.find(needle);
  if (begin == std::string::npos) {
    return std::nullopt;
  }
  std::size_t end = text.find(' ', begin + needle.size());
  if (end == std::string::npos) {
    end = text.size();
  }
  return text.substr(begin + needle.size(), end - (begin + needle.size()));
}

struct NvmlMemoryInfo {
  unsigned long long total = 0;
  unsigned long long free = 0;
  unsigned long long used = 0;
};

struct NvmlUtilizationInfo {
  unsigned int gpu = 0;
  unsigned int memory = 0;
};

std::optional<comet::GpuTelemetrySnapshot> TryCollectGpuTelemetryWithNvml(
    const comet::DesiredState& state,
    const std::string& node_name) {
#if defined(_WIN32)
  (void)state;
  (void)node_name;
  return std::nullopt;
#else
  (void)state;
  (void)node_name;
  void* lib = dlopen("libnvidia-ml.so.1", RTLD_LAZY);
  if (lib == nullptr) {
    return std::nullopt;
  }

  using nvmlReturn_t = int;
  using nvmlDevice_t = void*;
  constexpr nvmlReturn_t kNvmlSuccess = 0;
  using NvmlInitFn = nvmlReturn_t (*)();
  using NvmlShutdownFn = nvmlReturn_t (*)();
  using NvmlGetCountFn = nvmlReturn_t (*)(unsigned int*);
  using NvmlGetHandleFn = nvmlReturn_t (*)(unsigned int, nvmlDevice_t*);
  using NvmlMemoryInfoFn = nvmlReturn_t (*)(nvmlDevice_t, NvmlMemoryInfo*);
  using NvmlUtilizationFn = nvmlReturn_t (*)(nvmlDevice_t, NvmlUtilizationInfo*);
  using NvmlTemperatureFn = nvmlReturn_t (*)(nvmlDevice_t, unsigned int, unsigned int*);
  constexpr unsigned int kNvmlTemperatureGpu = 0;

  const auto init = reinterpret_cast<NvmlInitFn>(dlsym(lib, "nvmlInit_v2"));
  const auto shutdown = reinterpret_cast<NvmlShutdownFn>(dlsym(lib, "nvmlShutdown"));
  const auto get_count =
      reinterpret_cast<NvmlGetCountFn>(dlsym(lib, "nvmlDeviceGetCount_v2"));
  const auto get_handle =
      reinterpret_cast<NvmlGetHandleFn>(dlsym(lib, "nvmlDeviceGetHandleByIndex_v2"));
  const auto get_memory =
      reinterpret_cast<NvmlMemoryInfoFn>(dlsym(lib, "nvmlDeviceGetMemoryInfo"));
  const auto get_utilization =
      reinterpret_cast<NvmlUtilizationFn>(dlsym(lib, "nvmlDeviceGetUtilizationRates"));
  const auto get_temperature =
      reinterpret_cast<NvmlTemperatureFn>(dlsym(lib, "nvmlDeviceGetTemperature"));
  if (init == nullptr || shutdown == nullptr || get_count == nullptr || get_handle == nullptr ||
      get_memory == nullptr || get_utilization == nullptr) {
    dlclose(lib);
    return std::nullopt;
  }

  if (init() != kNvmlSuccess) {
    dlclose(lib);
    return std::nullopt;
  }

  comet::GpuTelemetrySnapshot snapshot;
  snapshot.degraded = false;
  snapshot.source = "nvml";
  unsigned int device_count = 0;
  if (get_count(&device_count) != kNvmlSuccess) {
    shutdown();
    dlclose(lib);
    return std::nullopt;
  }
  for (unsigned int index = 0; index < device_count; ++index) {
    nvmlDevice_t handle = nullptr;
    if (get_handle(index, &handle) != kNvmlSuccess || handle == nullptr) {
      continue;
    }
    NvmlMemoryInfo memory{};
    NvmlUtilizationInfo utilization{};
    if (get_memory(handle, &memory) != kNvmlSuccess ||
        get_utilization(handle, &utilization) != kNvmlSuccess) {
      continue;
    }
    comet::GpuDeviceTelemetry device;
    device.gpu_device = std::to_string(index);
    device.total_vram_mb = static_cast<int>(memory.total / (1024 * 1024));
    device.used_vram_mb = static_cast<int>(memory.used / (1024 * 1024));
    device.free_vram_mb = static_cast<int>(memory.free / (1024 * 1024));
    device.gpu_utilization_pct = static_cast<int>(utilization.gpu);
    unsigned int temperature_c = 0;
    if (get_temperature != nullptr &&
        get_temperature(handle, kNvmlTemperatureGpu, &temperature_c) == kNvmlSuccess) {
      device.temperature_c = static_cast<int>(temperature_c);
      device.temperature_available = true;
    }
    snapshot.devices.push_back(std::move(device));
  }

  shutdown();
  dlclose(lib);
  return snapshot;
#endif
}

void PopulateGpuProcessesFromNvidiaSmi(
    comet::GpuTelemetrySnapshot* snapshot,
    const std::vector<comet::RuntimeProcessStatus>& instance_statuses) {
  if (snapshot == nullptr) {
    return;
  }
  std::map<int, std::string> pid_to_instance_name;
  for (const auto& status : instance_statuses) {
    if (status.engine_pid > 0) {
      pid_to_instance_name[status.engine_pid] = status.instance_name;
    }
    if (status.runtime_pid > 0) {
      pid_to_instance_name[status.runtime_pid] = status.instance_name;
    }
  }

  std::map<std::string, std::string> uuid_to_gpu_device;
  {
    const std::string output =
        RunCommandCapture(
            "nvidia-smi --query-gpu=index,uuid --format=csv,noheader,nounits 2>/dev/null");
    std::istringstream input(output);
    std::string line;
    while (std::getline(input, line)) {
      const auto columns = SplitCsvRow(line);
      if (columns.size() >= 2) {
        uuid_to_gpu_device[columns[1]] = columns[0];
      }
    }
  }

  const std::string output =
      RunCommandCapture(
          "nvidia-smi --query-compute-apps=gpu_uuid,pid,used_gpu_memory "
          "--format=csv,noheader,nounits 2>/dev/null");
  std::istringstream input(output);
  std::string line;
  while (std::getline(input, line)) {
    const auto columns = SplitCsvRow(line);
    if (columns.size() < 3) {
      continue;
    }
    const auto gpu_it = uuid_to_gpu_device.find(columns[0]);
    if (gpu_it == uuid_to_gpu_device.end()) {
      continue;
    }
    int pid = 0;
    int used_vram_mb = 0;
    try {
      pid = std::stoi(columns[1]);
      used_vram_mb = std::stoi(columns[2]);
    } catch (const std::exception&) {
      continue;
    }
    for (auto& device : snapshot->devices) {
      if (device.gpu_device != gpu_it->second) {
        continue;
      }
      comet::GpuProcessTelemetry process;
      process.pid = pid;
      process.used_vram_mb = used_vram_mb;
      const auto owner_it = pid_to_instance_name.find(pid);
      if (owner_it != pid_to_instance_name.end()) {
        process.instance_name = owner_it->second;
      }
      device.processes.push_back(std::move(process));
      break;
    }
  }
}

std::optional<comet::GpuTelemetrySnapshot> TryCollectGpuTelemetryWithNvidiaSmi(
    const comet::DesiredState& state,
    const std::string& node_name,
    const std::vector<comet::RuntimeProcessStatus>& instance_statuses) {
  (void)state;
  (void)node_name;
  const std::string output =
      RunCommandCapture(
          "nvidia-smi --query-gpu=index,memory.total,memory.used,memory.free,utilization.gpu,temperature.gpu "
          "--format=csv,noheader,nounits 2>/dev/null");
  if (output.empty()) {
    return std::nullopt;
  }

  comet::GpuTelemetrySnapshot snapshot;
  snapshot.degraded = true;
  snapshot.source = "nvidia-smi";
  std::istringstream input(output);
  std::string line;
  while (std::getline(input, line)) {
    const auto columns = SplitCsvRow(line);
    if (columns.size() < 5) {
      continue;
    }
    try {
      comet::GpuDeviceTelemetry device;
      device.gpu_device = columns[0];
      device.total_vram_mb = std::stoi(columns[1]);
      device.used_vram_mb = std::stoi(columns[2]);
      device.free_vram_mb = std::stoi(columns[3]);
      device.gpu_utilization_pct = std::stoi(columns[4]);
      if (columns.size() >= 6 && columns[5] != "[N/A]" && columns[5] != "N/A" &&
          !columns[5].empty()) {
        device.temperature_c = std::stoi(columns[5]);
        device.temperature_available = true;
      }
      snapshot.devices.push_back(std::move(device));
    } catch (const std::exception&) {
      continue;
    }
  }
  PopulateGpuProcessesFromNvidiaSmi(&snapshot, instance_statuses);
  return snapshot;
}

comet::GpuTelemetrySnapshot CollectGpuTelemetry(
    const comet::DesiredState& state,
    const std::string& node_name,
    const std::vector<comet::RuntimeProcessStatus>& instance_statuses) {
  const bool disable_nvml =
      std::getenv("COMET_DISABLE_NVML") != nullptr &&
      std::string(std::getenv("COMET_DISABLE_NVML")) != "0";
  if (!disable_nvml) {
    if (const auto nvml_snapshot = TryCollectGpuTelemetryWithNvml(state, node_name);
        nvml_snapshot.has_value()) {
      comet::GpuTelemetrySnapshot snapshot = *nvml_snapshot;
      PopulateGpuProcessesFromNvidiaSmi(&snapshot, instance_statuses);
      snapshot.contract_version = 1;
      snapshot.collected_at = CurrentTimestampString();
      return snapshot;
    }
  }
  if (const auto smi_snapshot =
          TryCollectGpuTelemetryWithNvidiaSmi(state, node_name, instance_statuses);
      smi_snapshot.has_value()) {
    comet::GpuTelemetrySnapshot snapshot = *smi_snapshot;
    snapshot.contract_version = 1;
    snapshot.collected_at = CurrentTimestampString();
    return snapshot;
  }
  comet::GpuTelemetrySnapshot snapshot;
  snapshot.contract_version = 1;
  snapshot.degraded = true;
  snapshot.source = "unavailable";
  snapshot.collected_at = CurrentTimestampString();
  return snapshot;
}

comet::DiskTelemetrySnapshot CollectDiskTelemetry(
    const comet::DesiredState& state,
    const std::string& node_name) {
  comet::DiskTelemetrySnapshot snapshot;
  snapshot.contract_version = 1;
#if defined(_WIN32)
  snapshot.source = "filesystem::space";
#else
  snapshot.source = "statvfs";
#endif
  snapshot.collected_at = CurrentTimestampString();

  for (const auto& disk : state.disks) {
    if (disk.node_name != node_name) {
      continue;
    }

    comet::DiskTelemetryRecord record;
    record.disk_name = disk.name;
    record.plane_name = disk.plane_name;
    record.node_name = disk.node_name;
    record.mount_point = disk.host_path;
    record.runtime_state = fs::exists(disk.host_path) ? "present" : "missing";
    record.health = fs::exists(disk.host_path) ? "ok" : "missing";
    if (record.health == "missing") {
      record.fault_count += 1;
      record.fault_reasons.push_back("host-path-missing");
    }

    if (IsPathMounted(disk.host_path)) {
      record.mounted = true;
      record.runtime_state = "mounted";
      const auto mount_fs = CurrentMountFilesystemType(disk.host_path);
      if (mount_fs.has_value()) {
        record.filesystem_type = *mount_fs;
      }
      const auto mount_source = CurrentMountSource(disk.host_path);
      if (mount_source.has_value()) {
        record.mount_source = *mount_source;
      } else {
        record.warning_count += 1;
        record.fault_reasons.push_back("mount-source-unavailable");
        if (record.health == "ok") {
          record.health = "degraded";
        }
      }
      if (mount_source.has_value() && mount_source->rfind("/dev/", 0) == 0) {
        if (const auto read_only = ReadBlockDeviceReadOnly(*mount_source);
            read_only.has_value()) {
          record.read_only = *read_only;
          if (*read_only) {
            record.warning_count += 1;
            record.fault_reasons.push_back("read-only-device");
            if (record.health == "ok") {
              record.health = "degraded";
            }
          }
        }
        if (const auto io_stats = ReadBlockDeviceIoStats(*mount_source); io_stats.has_value()) {
          record.perf_counters_available = true;
          record.read_ios = io_stats->read_ios;
          record.write_ios = io_stats->write_ios;
          record.read_bytes = io_stats->read_sectors * 512ULL;
          record.write_bytes = io_stats->write_sectors * 512ULL;
          record.io_in_progress = static_cast<int>(io_stats->io_in_progress);
          record.io_time_ms = io_stats->io_time_ms;
          record.weighted_io_time_ms = io_stats->weighted_io_time_ms;
        } else {
          record.status_message =
              record.status_message.empty()
                  ? "block io stats unavailable"
                  : record.status_message + "; block io stats unavailable";
          record.warning_count += 1;
          record.fault_reasons.push_back("block-io-stats-unavailable");
          if (record.health == "ok") {
            record.health = "degraded";
          }
        }
        if (const auto io_error_count = ReadBlockDeviceIoErrorCount(*mount_source);
            io_error_count.has_value()) {
          record.io_error_counters_available = true;
          record.io_error_count = *io_error_count;
          if (*io_error_count > 0) {
            record.fault_count += 1;
            record.fault_reasons.push_back("io-error-count-nonzero");
            if (record.health == "ok") {
              record.health = "degraded";
            }
          }
        }
      }
    }

#if defined(_WIN32)
    std::error_code space_error;
    const auto space_info = fs::space(disk.host_path, space_error);
    if (!space_error) {
      record.total_bytes = space_info.capacity;
      record.free_bytes = space_info.available;
      record.used_bytes =
          record.total_bytes >= record.free_bytes ? (record.total_bytes - record.free_bytes) : 0;
      if (record.health == "missing") {
        record.health = "ok";
      }
      if (record.runtime_state == "missing") {
        record.runtime_state = "available";
      }
    } else {
      record.status_message =
          record.status_message.empty() ? "filesystem::space unavailable"
                                        : record.status_message + "; filesystem::space unavailable";
      record.fault_count += 1;
      record.fault_reasons.push_back("filesystem-space-unavailable");
      if (record.health == "ok") {
        record.health = "degraded";
      }
    }
#else
    struct statvfs stats {};
    if (statvfs(disk.host_path.c_str(), &stats) == 0) {
      const std::uint64_t block_size = static_cast<std::uint64_t>(stats.f_frsize);
      record.total_bytes = static_cast<std::uint64_t>(stats.f_blocks) * block_size;
      record.free_bytes = static_cast<std::uint64_t>(stats.f_bavail) * block_size;
      record.used_bytes =
          record.total_bytes >= record.free_bytes ? (record.total_bytes - record.free_bytes) : 0;
      if (record.health == "missing") {
        record.health = "ok";
      }
      if (record.runtime_state == "missing") {
        record.runtime_state = "available";
      }
    } else {
      record.status_message =
          record.status_message.empty() ? "statvfs unavailable"
                                        : record.status_message + "; statvfs unavailable";
      record.fault_count += 1;
      record.fault_reasons.push_back("statvfs-unavailable");
      if (record.health == "ok") {
        record.health = "degraded";
      }
    }
#endif

    snapshot.items.push_back(std::move(record));
  }

  return snapshot;
}

std::optional<std::string> ReadTrimmedFile(const fs::path& path) {
  std::ifstream input(path);
  if (!input.is_open()) {
    return std::nullopt;
  }
  std::string value;
  std::getline(input, value);
  while (!value.empty() &&
         (value.back() == '\n' || value.back() == '\r' || value.back() == ' ')) {
    value.pop_back();
  }
  return value;
}

std::uint64_t ReadUint64FileOrZero(const fs::path& path) {
  std::ifstream input(path);
  std::uint64_t value = 0;
  if (!input.is_open()) {
    return 0;
  }
  input >> value;
  return value;
}

std::string Lowercase(std::string value) {
  std::transform(
      value.begin(),
      value.end(),
      value.begin(),
      [](unsigned char ch) { return static_cast<char>(std::tolower(ch)); });
  return value;
}

bool ContainsAnyToken(const std::string& text, const std::initializer_list<const char*> tokens) {
  for (const char* token : tokens) {
    if (text.find(token) != std::string::npos) {
      return true;
    }
  }
  return false;
}

std::optional<double> ReadTemperatureCelsius(const fs::path& path) {
  std::ifstream input(path);
  if (!input.is_open()) {
    return std::nullopt;
  }
  double value = 0.0;
  if (!(input >> value)) {
    return std::nullopt;
  }
  if (value > 1000.0) {
    value /= 1000.0;
  }
  if (value < -50.0 || value > 200.0) {
    return std::nullopt;
  }
  return value;
}

std::vector<double> CollectCpuTemperatureSamples() {
  std::vector<double> samples;

  const fs::path hwmon_root("/sys/class/hwmon");
  if (fs::exists(hwmon_root)) {
    for (const auto& hwmon_entry : fs::directory_iterator(hwmon_root)) {
      if (!hwmon_entry.is_directory() && !hwmon_entry.is_symlink()) {
        continue;
      }
      const std::string hwmon_name =
          Lowercase(ReadTrimmedFile(hwmon_entry.path() / "name").value_or(std::string{}));
      const bool hwmon_cpu_related = ContainsAnyToken(
          hwmon_name,
          {"coretemp", "k10temp", "zenpower", "cpu", "package", "acpitz", "soc"});
      for (const auto& sensor_entry : fs::directory_iterator(hwmon_entry.path())) {
        const std::string file_name = sensor_entry.path().filename().string();
        if (file_name.rfind("temp", 0) != 0 ||
            file_name.find("_input") == std::string::npos) {
          continue;
        }
        const std::string label_file =
            file_name.substr(0, file_name.find("_input")) + "_label";
        const std::string label = Lowercase(
            ReadTrimmedFile(hwmon_entry.path() / label_file).value_or(std::string{}));
        const bool label_cpu_related = ContainsAnyToken(
            label,
            {"package", "cpu", "core", "tctl", "tdie", "ccd", "soc"});
        if (!hwmon_cpu_related && !label_cpu_related) {
          continue;
        }
        const auto sample = ReadTemperatureCelsius(sensor_entry.path());
        if (sample.has_value()) {
          samples.push_back(*sample);
        }
      }
    }
  }

  if (!samples.empty()) {
    return samples;
  }

  const fs::path thermal_root("/sys/class/thermal");
  if (fs::exists(thermal_root)) {
    for (const auto& zone_entry : fs::directory_iterator(thermal_root)) {
      const std::string zone_name = zone_entry.path().filename().string();
      if (zone_name.rfind("thermal_zone", 0) != 0) {
        continue;
      }
      const std::string zone_type = Lowercase(
          ReadTrimmedFile(zone_entry.path() / "type").value_or(std::string{}));
      if (!ContainsAnyToken(zone_type, {"cpu", "pkg", "x86", "acpitz", "soc"})) {
        continue;
      }
      const auto sample = ReadTemperatureCelsius(zone_entry.path() / "temp");
      if (sample.has_value()) {
        samples.push_back(*sample);
      }
    }
  }

  return samples;
}

struct CpuSample {
  std::uint64_t idle = 0;
  std::uint64_t total = 0;
};

std::optional<CpuSample> ReadCpuSample() {
  std::ifstream input("/proc/stat");
  if (!input.is_open()) {
    return std::nullopt;
  }

  std::string cpu_label;
  CpuSample sample;
  std::uint64_t user = 0;
  std::uint64_t nice = 0;
  std::uint64_t system = 0;
  std::uint64_t idle = 0;
  std::uint64_t iowait = 0;
  std::uint64_t irq = 0;
  std::uint64_t softirq = 0;
  std::uint64_t steal = 0;
  input >> cpu_label >> user >> nice >> system >> idle >> iowait >> irq >> softirq >> steal;
  if (!input.good() || cpu_label != "cpu") {
    return std::nullopt;
  }
  sample.idle = idle + iowait;
  sample.total = user + nice + system + idle + iowait + irq + softirq + steal;
  return sample;
}

std::optional<std::array<double, 3>> ReadLoadAverage() {
  std::ifstream input("/proc/loadavg");
  if (!input.is_open()) {
    return std::nullopt;
  }
  std::array<double, 3> load{0.0, 0.0, 0.0};
  input >> load[0] >> load[1] >> load[2];
  if (!input.good()) {
    return std::nullopt;
  }
  return load;
}

comet::CpuTelemetrySnapshot CollectCpuTelemetry() {
  comet::CpuTelemetrySnapshot snapshot;
  snapshot.contract_version = 1;
  snapshot.source = "procfs";
  snapshot.collected_at = CurrentTimestampString();
  snapshot.core_count = static_cast<int>(std::thread::hardware_concurrency());

  const auto first = ReadCpuSample();
  std::this_thread::sleep_for(std::chrono::milliseconds(150));
  const auto second = ReadCpuSample();
  if (first.has_value() && second.has_value() && second->total > first->total) {
    const auto total_delta = static_cast<double>(second->total - first->total);
    const auto idle_delta = static_cast<double>(second->idle - first->idle);
    snapshot.utilization_pct =
        total_delta > 0.0 ? std::max(0.0, 100.0 * (1.0 - (idle_delta / total_delta))) : 0.0;
  } else {
    snapshot.degraded = true;
    snapshot.source = "procfs-unavailable";
  }

  if (const auto load = ReadLoadAverage(); load.has_value()) {
    snapshot.loadavg_1m = (*load)[0];
    snapshot.loadavg_5m = (*load)[1];
    snapshot.loadavg_15m = (*load)[2];
  } else {
    snapshot.degraded = true;
  }

  const auto temperature_samples = CollectCpuTemperatureSamples();
  if (!temperature_samples.empty()) {
    double total_temperature = 0.0;
    double max_temperature = temperature_samples.front();
    for (double sample : temperature_samples) {
      total_temperature += sample;
      max_temperature = std::max(max_temperature, sample);
    }
    snapshot.temperature_available = true;
    snapshot.temperature_c =
        total_temperature / static_cast<double>(temperature_samples.size());
    snapshot.max_temperature_c = max_temperature;
  }

  std::ifstream meminfo("/proc/meminfo");
  if (meminfo.is_open()) {
    std::string key;
    std::uint64_t value = 0;
    std::string unit;
    std::uint64_t total_kb = 0;
    std::uint64_t available_kb = 0;
    while (meminfo >> key >> value >> unit) {
      if (key == "MemTotal:") {
        total_kb = value;
      } else if (key == "MemAvailable:") {
        available_kb = value;
      }
    }
    snapshot.total_memory_bytes = total_kb * 1024ULL;
    snapshot.available_memory_bytes = available_kb * 1024ULL;
    snapshot.used_memory_bytes =
        snapshot.total_memory_bytes >= snapshot.available_memory_bytes
            ? (snapshot.total_memory_bytes - snapshot.available_memory_bytes)
            : 0;
  } else {
    snapshot.degraded = true;
  }

  return snapshot;
}

comet::NetworkTelemetrySnapshot CollectNetworkTelemetry() {
  comet::NetworkTelemetrySnapshot snapshot;
  snapshot.contract_version = 1;
  snapshot.source = "sysfs";
  snapshot.collected_at = CurrentTimestampString();

  const fs::path net_root("/sys/class/net");
  if (!fs::exists(net_root)) {
    snapshot.degraded = true;
    snapshot.source = "unavailable";
    return snapshot;
  }

  for (const auto& entry : fs::directory_iterator(net_root)) {
    if (!entry.is_directory() && !entry.is_symlink()) {
      continue;
    }
    comet::NetworkInterfaceTelemetry interface;
    interface.interface_name = entry.path().filename().string();
    interface.oper_state =
        ReadTrimmedFile(entry.path() / "operstate").value_or(std::string{"unknown"});
    const auto carrier = ReadTrimmedFile(entry.path() / "carrier");
    if (carrier.has_value()) {
      interface.link_state = (*carrier == "1") ? "up" : "down";
    } else {
      interface.link_state = interface.oper_state;
    }
    interface.rx_bytes = ReadUint64FileOrZero(entry.path() / "statistics" / "rx_bytes");
    interface.tx_bytes = ReadUint64FileOrZero(entry.path() / "statistics" / "tx_bytes");
    interface.loopback = interface.interface_name == "lo";
    snapshot.interfaces.push_back(std::move(interface));
  }

  std::sort(
      snapshot.interfaces.begin(),
      snapshot.interfaces.end(),
      [](const auto& left, const auto& right) { return left.interface_name < right.interface_name; });
  return snapshot;
}

bool IsContainerAbsentForService(const std::string& service_name) {
  return !ResolveComposeContainerIdForService(service_name).has_value();
}

}  // namespace

std::vector<comet::RuntimeProcessStatus> LoadLocalInstanceRuntimeStatuses(
    const std::string& state_root,
    const std::string& node_name,
    const std::optional<std::string>& plane_name) {
  std::vector<comet::RuntimeProcessStatus> result;
  const auto local_states = plane_name.has_value()
                                ? [&]() {
                                    std::vector<comet::DesiredState> states;
                                    const auto state = local_state_support::LoadLocalAppliedState(
                                        state_root,
                                        node_name,
                                        plane_name);
                                    if (state.has_value()) {
                                      states.push_back(*state);
                                    }
                                    return states;
                                  }()
                                : local_state_support::LoadAllLocalAppliedStates(state_root, node_name);
  for (const auto& local_state : local_states) {
    for (const auto& instance : local_state.instances) {
      if (instance.node_name != node_name) {
        continue;
      }
      std::optional<std::string> status_path;
      if (instance.role == comet::InstanceRole::Infer) {
        status_path = InferRuntimeStatusPathForInstance(local_state, instance);
      } else {
        status_path = WorkerRuntimeStatusPathForInstance(local_state, instance);
      }
      if (!status_path.has_value() || !fs::exists(*status_path)) {
        continue;
      }
      const auto status = comet::LoadRuntimeStatusJson(*status_path);
      if (!status.has_value()) {
        continue;
      }
      result.push_back(ToProcessStatus(*status, instance));
    }
  }
  return result;
}

std::vector<std::string> ParseTaggedCsv(const std::string& text, const std::string& key) {
  const auto value = ParseTaggedValue(text, key);
  if (!value.has_value()) {
    return {};
  }
  std::vector<std::string> items;
  std::stringstream stream(*value);
  std::string item;
  while (std::getline(stream, item, ',')) {
    item = Trim(item);
    if (!item.empty()) {
      items.push_back(item);
    }
  }
  return items;
}

std::map<std::string, int> CaptureServiceHostPids(const std::vector<std::string>& service_names) {
  std::map<std::string, int> result;
  for (const auto& service_name : service_names) {
    const auto pid = ResolveServiceHostPid(service_name);
    if (pid.has_value()) {
      result.emplace(service_name, *pid);
    }
  }
  return result;
}

bool VerifyEvictionAssignment(
    const comet::DesiredState& local_state,
    const std::string& node_name,
    const std::string& state_root,
    const std::string& status_message,
    const std::map<std::string, int>& victim_host_pids) {
  const auto victim_names = ParseTaggedCsv(status_message, "victims");
  const auto target_gpu = ParseTaggedValue(status_message, "target_gpu");
  const int required_memory_cap_mb =
      ParseTaggedValue(status_message, "required_memory_cap_mb").has_value()
          ? std::stoi(*ParseTaggedValue(status_message, "required_memory_cap_mb"))
          : 0;
  constexpr int kReserveMemoryMb = 1024;
  constexpr int kStableSamples = 3;
  const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(30);
  int stable_samples = 0;

  while (std::chrono::steady_clock::now() < deadline) {
    bool victims_gone = true;
    for (const auto& victim_name : victim_names) {
      if (!IsContainerAbsentForService(victim_name)) {
        victims_gone = false;
        break;
      }
    }

    auto instance_statuses = LoadLocalInstanceRuntimeStatuses(state_root, node_name);
    ResolveInstanceHostPids(&instance_statuses);
    const auto telemetry = CollectGpuTelemetry(local_state, node_name, instance_statuses);

    bool victims_released_gpu = true;
    bool memory_ready = target_gpu.has_value() ? false : true;
    for (const auto& device : telemetry.devices) {
      if (target_gpu.has_value() && device.gpu_device == *target_gpu) {
        memory_ready = device.free_vram_mb >= required_memory_cap_mb + kReserveMemoryMb;
      }
      for (const auto& process : device.processes) {
        if (std::find(victim_names.begin(), victim_names.end(), process.instance_name) !=
            victim_names.end()) {
          victims_released_gpu = false;
        }
        for (const auto& [victim_name, victim_pid] : victim_host_pids) {
          (void)victim_name;
          if (process.pid == victim_pid) {
            victims_released_gpu = false;
          }
        }
      }
    }

    if (victims_gone && victims_released_gpu && memory_ready) {
      ++stable_samples;
      if (stable_samples >= kStableSamples) {
        return true;
      }
    } else {
      stable_samples = 0;
    }
    std::this_thread::sleep_for(std::chrono::seconds(2));
  }

  return false;
}

comet::HostObservation BuildObservedStateSnapshot(
    const std::string& node_name,
    const std::string& state_root,
    comet::HostObservationStatus status,
    const std::string& status_message,
    const std::optional<int>& last_assignment_id) {
  comet::HostObservation observation;
  observation.node_name = node_name;
  observation.status = status;
  observation.status_message = status_message;
  observation.last_assignment_id = last_assignment_id;
  observation.applied_generation = local_state_support::LoadLocalAppliedGeneration(state_root, node_name);

  const auto local_state = local_state_support::LoadLocalAppliedState(state_root, node_name);
  if (local_state.has_value()) {
    observation.plane_name = local_state->plane_name;
    observation.observed_state_json = comet::SerializeDesiredStateJson(*local_state);
  }
  const auto runtime_status = local_state_support::LoadLocalRuntimeStatus(
      state_root,
      node_name,
      [](const comet::DesiredState& state, const std::string& current_node_name) {
        return RuntimeStatusPathForNode(state, current_node_name);
      });
  if (runtime_status.has_value()) {
    observation.runtime_status_json = comet::SerializeRuntimeStatusJson(*runtime_status);
  }
  auto instance_statuses = LoadLocalInstanceRuntimeStatuses(state_root, node_name);
  ResolveInstanceHostPids(&instance_statuses);
  if (!instance_statuses.empty()) {
    observation.instance_runtime_json = comet::SerializeRuntimeStatusListJson(instance_statuses);
  }
  if (local_state.has_value()) {
    observation.gpu_telemetry_json =
        comet::SerializeGpuTelemetryJson(
            CollectGpuTelemetry(*local_state, node_name, instance_statuses));
    observation.disk_telemetry_json =
        comet::SerializeDiskTelemetryJson(CollectDiskTelemetry(*local_state, node_name));
  }
  observation.network_telemetry_json =
      comet::SerializeNetworkTelemetryJson(CollectNetworkTelemetry());
  observation.cpu_telemetry_json =
      comet::SerializeCpuTelemetryJson(CollectCpuTelemetry());

  return observation;
}

}  // namespace comet::hostd::telemetry_support
