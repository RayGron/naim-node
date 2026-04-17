#include "app/hostd_system_telemetry_collector.h"

#include <algorithm>
#include <array>
#include <cctype>
#include <chrono>
#include <cstdlib>
#include <ctime>
#include <filesystem>
#include <fstream>
#include <initializer_list>
#include <map>
#include <optional>
#include <sstream>
#include <string>
#include <thread>

#include <nlohmann/json.hpp>

#if !defined(_WIN32)
#include <dlfcn.h>
#include <sys/statvfs.h>
#endif

namespace naim::hostd {

namespace fs = std::filesystem;

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

struct NvmlMemoryInfo {
  unsigned long long total = 0;
  unsigned long long free = 0;
  unsigned long long used = 0;
};

struct NvmlUtilizationInfo {
  unsigned int gpu = 0;
  unsigned int memory = 0;
};

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

std::map<std::string, std::vector<std::string>> LoadInterfaceAddresses(
    const HostdCommandSupport& command_support) {
  std::map<std::string, std::vector<std::string>> addresses_by_interface;
#if defined(_WIN32)
  (void)command_support;
#else
  const std::string output =
      command_support.RunCommandCapture("ip -o -4 addr show 2>/dev/null || true");
  std::istringstream lines(output);
  std::string line;
  while (std::getline(lines, line)) {
    std::istringstream fields(line);
    std::string index;
    std::string interface_name;
    std::string family;
    std::string address;
    fields >> index >> interface_name >> family >> address;
    if (interface_name.empty() || family != "inet" || address.empty()) {
      continue;
    }
    if (!interface_name.empty() && interface_name.back() == ':') {
      interface_name.pop_back();
    }
    addresses_by_interface[interface_name].push_back(address);
  }
#endif
  return addresses_by_interface;
}

std::vector<naim::PeerDiscoveryTelemetry> LoadPeerDiscoveryTelemetry(
    const std::string& state_root) {
  std::vector<naim::PeerDiscoveryTelemetry> peers;
  if (state_root.empty()) {
    return peers;
  }
  const fs::path peer_path = fs::path(state_root) / "peer-discovery.json";
  std::ifstream input(peer_path);
  if (!input.is_open()) {
    return peers;
  }
  const auto payload = nlohmann::json::parse(input, nullptr, false);
  if (payload.is_discarded() || !payload.is_object()) {
    return peers;
  }
  for (const auto& value : payload.value("peers", nlohmann::json::array())) {
    if (!value.is_object()) {
      continue;
    }
    naim::PeerDiscoveryTelemetry peer;
    peer.peer_node_name = value.value("peer_node_name", std::string{});
    peer.peer_endpoint = value.value("peer_endpoint", std::string{});
    peer.local_interface = value.value("local_interface", std::string{});
    peer.remote_address = value.value("remote_address", std::string{});
    peer.seen_udp = value.value("seen_udp", false);
    peer.tcp_reachable = value.value("tcp_reachable", false);
    peer.rtt_ms = value.value("rtt_ms", 0);
    peer.last_seen_at = value.value("last_seen_at", std::string{});
    peer.last_probe_at = value.value("last_probe_at", std::string{});
    if (!peer.peer_node_name.empty()) {
      peers.push_back(std::move(peer));
    }
  }
  return peers;
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

}  // namespace

naim::GpuTelemetrySnapshot HostdSystemTelemetryCollector::CollectGpuTelemetry(
    const naim::DesiredState& state,
    const std::string& node_name,
    const std::vector<naim::RuntimeProcessStatus>& instance_statuses) const {
  auto split_csv_row = [this](const std::string& line) {
    std::vector<std::string> result;
    std::stringstream stream(line);
    std::string current;
    while (std::getline(stream, current, ',')) {
      result.push_back(command_support_.Trim(current));
    }
    return result;
  };

  auto populate_gpu_processes_from_nvidia_smi =
      [&](naim::GpuTelemetrySnapshot* snapshot) {
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
          const std::string output = command_support_.RunCommandCapture(
              "nvidia-smi --query-gpu=index,uuid --format=csv,noheader,nounits 2>/dev/null");
          std::istringstream input(output);
          std::string line;
          while (std::getline(input, line)) {
            const auto columns = split_csv_row(line);
            if (columns.size() >= 2) {
              uuid_to_gpu_device[columns[1]] = columns[0];
            }
          }
        }

        const std::string output = command_support_.RunCommandCapture(
            "nvidia-smi --query-compute-apps=gpu_uuid,pid,used_gpu_memory "
            "--format=csv,noheader,nounits 2>/dev/null");
        std::istringstream input(output);
        std::string line;
        while (std::getline(input, line)) {
          const auto columns = split_csv_row(line);
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
            naim::GpuProcessTelemetry process;
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
      };

  auto try_collect_gpu_telemetry_with_nvml = [&]() -> std::optional<naim::GpuTelemetrySnapshot> {
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

    naim::GpuTelemetrySnapshot snapshot;
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
      naim::GpuDeviceTelemetry device;
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
  };

  auto try_collect_gpu_telemetry_with_nvidia_smi =
      [&]() -> std::optional<naim::GpuTelemetrySnapshot> {
        (void)state;
        (void)node_name;
        const std::string output = command_support_.RunCommandCapture(
            "nvidia-smi --query-gpu=index,memory.total,memory.used,memory.free,utilization.gpu,temperature.gpu "
            "--format=csv,noheader,nounits 2>/dev/null");
        if (output.empty()) {
          return std::nullopt;
        }

        naim::GpuTelemetrySnapshot snapshot;
        snapshot.degraded = true;
        snapshot.source = "nvidia-smi";
        std::istringstream input(output);
        std::string line;
        while (std::getline(input, line)) {
          const auto columns = split_csv_row(line);
          if (columns.size() < 5) {
            continue;
          }
          try {
            naim::GpuDeviceTelemetry device;
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
        populate_gpu_processes_from_nvidia_smi(&snapshot);
        return snapshot;
      };

  const bool disable_nvml =
      std::getenv("NAIM_DISABLE_NVML") != nullptr &&
      std::string(std::getenv("NAIM_DISABLE_NVML")) != "0";
  if (!disable_nvml) {
    if (const auto nvml_snapshot = try_collect_gpu_telemetry_with_nvml();
        nvml_snapshot.has_value()) {
      naim::GpuTelemetrySnapshot snapshot = *nvml_snapshot;
      populate_gpu_processes_from_nvidia_smi(&snapshot);
      snapshot.contract_version = 1;
      snapshot.collected_at = CurrentTimestampString();
      return snapshot;
    }
  }
  if (const auto smi_snapshot = try_collect_gpu_telemetry_with_nvidia_smi();
      smi_snapshot.has_value()) {
    naim::GpuTelemetrySnapshot snapshot = *smi_snapshot;
    snapshot.contract_version = 1;
    snapshot.collected_at = CurrentTimestampString();
    return snapshot;
  }
  naim::GpuTelemetrySnapshot snapshot;
  snapshot.contract_version = 1;
  snapshot.degraded = true;
  snapshot.source = "unavailable";
  snapshot.collected_at = CurrentTimestampString();
  return snapshot;
}

naim::DiskTelemetrySnapshot HostdSystemTelemetryCollector::CollectDiskTelemetry(
    const naim::DesiredState& state,
    const std::string& node_name) const {
  naim::DiskTelemetrySnapshot snapshot;
  snapshot.contract_version = 1;
#if defined(_WIN32)
  snapshot.source = "filesystem::space";
#else
  snapshot.source = "statvfs";
#endif
  snapshot.collected_at = CurrentTimestampString();

  auto is_path_mounted = [this](const std::string& mount_point) {
    if (command_support_.RunCommandOk(
            "/usr/bin/mountpoint -q " + command_support_.ShellQuote(mount_point) +
            " >/dev/null 2>&1")) {
      return true;
    }
    const std::string normalized_mount_point = NormalizeMountPointPath(mount_point);
    return normalized_mount_point != mount_point &&
           command_support_.RunCommandOk(
               "/usr/bin/mountpoint -q " + command_support_.ShellQuote(normalized_mount_point) +
               " >/dev/null 2>&1");
  };

  auto current_mount_source = [&](const std::string& mount_point) -> std::optional<std::string> {
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
  };

  auto current_mount_filesystem_type =
      [](const std::string& mount_point) -> std::optional<std::string> {
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
  };

  for (const auto& disk : state.disks) {
    if (disk.node_name != node_name) {
      continue;
    }

    naim::DiskTelemetryRecord record;
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

    if (is_path_mounted(disk.host_path)) {
      record.mounted = true;
      record.runtime_state = "mounted";
      const auto mount_fs = current_mount_filesystem_type(disk.host_path);
      if (mount_fs.has_value()) {
        record.filesystem_type = *mount_fs;
      }
      const auto mount_source = current_mount_source(disk.host_path);
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

naim::DiskTelemetryRecord HostdSystemTelemetryCollector::BuildStorageRootTelemetry(
    const std::string& node_name,
    const std::string& storage_root) const {
  naim::DiskTelemetryRecord record;
  record.disk_name = "storage-root";
  record.node_name = node_name;
  record.mount_point = storage_root;
  record.runtime_state = fs::exists(storage_root) ? "present" : "missing";
  record.health = fs::exists(storage_root) ? "ok" : "missing";
  if (record.health == "missing") {
    record.fault_count += 1;
    record.fault_reasons.push_back("storage-root-missing");
  }

  auto is_path_mounted = [this](const std::string& mount_point) {
    if (command_support_.RunCommandOk(
            "/usr/bin/mountpoint -q " + command_support_.ShellQuote(mount_point) +
            " >/dev/null 2>&1")) {
      return true;
    }
    const std::string normalized_mount_point = NormalizeMountPointPath(mount_point);
    return normalized_mount_point != mount_point &&
           command_support_.RunCommandOk(
               "/usr/bin/mountpoint -q " + command_support_.ShellQuote(normalized_mount_point) +
               " >/dev/null 2>&1");
  };

  auto current_mount_source = [&](const std::string& mount_point) -> std::optional<std::string> {
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
  };

  auto current_mount_filesystem_type =
      [](const std::string& mount_point) -> std::optional<std::string> {
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
  };

  if (is_path_mounted(storage_root)) {
    record.mounted = true;
    record.runtime_state = "mounted";
    if (const auto mount_fs = current_mount_filesystem_type(storage_root); mount_fs.has_value()) {
      record.filesystem_type = *mount_fs;
    }
    if (const auto mount_source = current_mount_source(storage_root); mount_source.has_value()) {
      record.mount_source = *mount_source;
      if (mount_source->rfind("/dev/", 0) == 0) {
        if (const auto read_only = ReadBlockDeviceReadOnly(*mount_source);
            read_only.has_value()) {
          record.read_only = *read_only;
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
        }
        if (const auto io_error_count = ReadBlockDeviceIoErrorCount(*mount_source);
            io_error_count.has_value()) {
          record.io_error_counters_available = true;
          record.io_error_count = *io_error_count;
        }
      }
    }
  }

#if defined(_WIN32)
  std::error_code space_error;
  const auto space_info = fs::space(storage_root, space_error);
  if (!space_error) {
    record.total_bytes = space_info.capacity;
    record.free_bytes = space_info.available;
    record.used_bytes =
        record.total_bytes >= record.free_bytes ? (record.total_bytes - record.free_bytes) : 0;
  }
#else
  struct statvfs stats {};
  if (statvfs(storage_root.c_str(), &stats) == 0) {
    const std::uint64_t block_size = static_cast<std::uint64_t>(stats.f_frsize);
    record.total_bytes = static_cast<std::uint64_t>(stats.f_blocks) * block_size;
    record.free_bytes = static_cast<std::uint64_t>(stats.f_bavail) * block_size;
    record.used_bytes =
        record.total_bytes >= record.free_bytes ? (record.total_bytes - record.free_bytes) : 0;
  }
#endif

  return record;
}

naim::CpuTelemetrySnapshot HostdSystemTelemetryCollector::CollectCpuTelemetry() const {
  naim::CpuTelemetrySnapshot snapshot;
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

naim::NetworkTelemetrySnapshot HostdSystemTelemetryCollector::CollectNetworkTelemetry(
    const std::string& state_root) const {
  naim::NetworkTelemetrySnapshot snapshot;
  snapshot.contract_version = 1;
  snapshot.source = "sysfs";
  snapshot.collected_at = CurrentTimestampString();
  snapshot.peer_discovery = LoadPeerDiscoveryTelemetry(state_root);

  const fs::path net_root("/sys/class/net");
  if (!fs::exists(net_root)) {
    snapshot.degraded = true;
    snapshot.source = "unavailable";
    return snapshot;
  }

  const auto addresses_by_interface = LoadInterfaceAddresses(command_support_);
  for (const auto& entry : fs::directory_iterator(net_root)) {
    if (!entry.is_directory() && !entry.is_symlink()) {
      continue;
    }
    naim::NetworkInterfaceTelemetry interface;
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
    if (const auto address_it = addresses_by_interface.find(interface.interface_name);
        address_it != addresses_by_interface.end()) {
      interface.addresses = address_it->second;
    }
    snapshot.interfaces.push_back(std::move(interface));
  }

  std::sort(
      snapshot.interfaces.begin(),
      snapshot.interfaces.end(),
      [](const auto& left, const auto& right) { return left.interface_name < right.interface_name; });
  return snapshot;
}

}  // namespace naim::hostd
