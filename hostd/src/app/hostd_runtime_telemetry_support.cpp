#include "app/hostd_runtime_telemetry_support.h"

#include <algorithm>
#include <chrono>
#include <filesystem>
#include <map>
#include <optional>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#include "app/hostd_local_state_path_support.h"
#include "app/hostd_local_state_repository.h"
#include "app/hostd_system_telemetry_collector.h"
#include "naim/state/state_json.h"

namespace naim::hostd {

namespace fs = std::filesystem;

namespace {

const HostdLocalStatePathSupport& LocalStatePathSupport() {
  static const HostdLocalStatePathSupport path_support;
  return path_support;
}

const HostdLocalStateRepository& LocalStateRepository() {
  static const HostdLocalStateRepository repository(LocalStatePathSupport());
  return repository;
}

}  // namespace

std::vector<naim::RuntimeProcessStatus> HostdRuntimeTelemetrySupport::LoadLocalInstanceRuntimeStatuses(
    const std::string& state_root,
    const std::string& node_name,
    const std::optional<std::string>& plane_name) const {
  std::vector<naim::RuntimeProcessStatus> result;
  const auto local_states = plane_name.has_value()
                                ? [&]() {
                                    std::vector<naim::DesiredState> states;
                                    const auto state = LocalStateRepository().LoadLocalAppliedState(
                                        state_root,
                                        node_name,
                                        plane_name);
                                    if (state.has_value()) {
                                      states.push_back(*state);
                                    }
                                    return states;
                                  }()
                                : LocalStateRepository().LoadAllLocalAppliedStates(
                                      state_root,
                                      node_name);
  for (const auto& local_state : local_states) {
    for (const auto& instance : local_state.instances) {
      if (instance.node_name != node_name) {
        continue;
      }
      const auto status_path = WorkerRuntimeStatusPathForInstance(local_state, instance);
      const auto infer_status_path =
          instance.role == naim::InstanceRole::Infer
              ? path_support_.InferRuntimeStatusPathForInstance(local_state, instance)
              : std::nullopt;
      const auto selected_path =
          infer_status_path.has_value() ? infer_status_path : status_path;
      if (!selected_path.has_value() || !fs::exists(*selected_path)) {
        continue;
      }
      const auto status = naim::LoadRuntimeStatusJson(*selected_path);
      if (!status.has_value()) {
        continue;
      }
      result.push_back(ToProcessStatus(*status, instance));
    }
  }
  return result;
}

std::vector<std::string> HostdRuntimeTelemetrySupport::ParseTaggedCsv(
    const std::string& text,
    const std::string& key) const {
  const auto value = ParseTaggedValue(text, key);
  if (!value.has_value()) {
    return {};
  }
  std::vector<std::string> items;
  std::stringstream stream(*value);
  std::string item;
  while (std::getline(stream, item, ',')) {
    item = command_support_.Trim(item);
    if (!item.empty()) {
      items.push_back(item);
    }
  }
  return items;
}

std::map<std::string, int> HostdRuntimeTelemetrySupport::CaptureServiceHostPids(
    const std::vector<std::string>& service_names) const {
  std::map<std::string, int> result;
  for (const auto& service_name : service_names) {
    const auto pid = ResolveServiceHostPid(service_name);
    if (pid.has_value()) {
      result.emplace(service_name, *pid);
    }
  }
  return result;
}

bool HostdRuntimeTelemetrySupport::VerifyEvictionAssignment(
    const naim::DesiredState& local_state,
    const std::string& node_name,
    const std::string& state_root,
    const std::string& status_message,
    const std::map<std::string, int>& victim_host_pids) const {
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
  const HostdSystemTelemetryCollector system_telemetry_collector;

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
    const auto telemetry =
        system_telemetry_collector.CollectGpuTelemetry(local_state, node_name, instance_statuses);

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

void HostdRuntimeTelemetrySupport::ResolveInstanceHostPids(
    std::vector<naim::RuntimeProcessStatus>* statuses) const {
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

std::optional<std::string> HostdRuntimeTelemetrySupport::WorkerRuntimeStatusPathForInstance(
    const naim::DesiredState& state,
    const naim::InstanceSpec& instance) const {
  if (instance.role != naim::InstanceRole::Worker &&
      instance.role != naim::InstanceRole::Skills) {
    return std::nullopt;
  }
  for (const auto& disk : state.disks) {
    const bool matching_disk_kind =
        (instance.role == naim::InstanceRole::Worker &&
         disk.kind == naim::DiskKind::WorkerPrivate) ||
        (instance.role == naim::InstanceRole::Skills &&
         disk.kind == naim::DiskKind::SkillsPrivate);
    if (matching_disk_kind && disk.owner_name == instance.name &&
        disk.node_name == instance.node_name) {
      return (fs::path(disk.host_path) /
              (instance.role == naim::InstanceRole::Skills
                   ? "skills-runtime-status.json"
                   : "worker-runtime-status.json"))
          .string();
    }
  }
  return std::nullopt;
}

naim::RuntimeProcessStatus HostdRuntimeTelemetrySupport::ToProcessStatus(
    naim::RuntimeStatus status,
    const naim::InstanceSpec& instance) const {
  if (status.instance_name.empty()) {
    status.instance_name = instance.name;
  }
  if (status.instance_role.empty()) {
    status.instance_role = naim::ToString(instance.role);
  }
  if (status.node_name.empty()) {
    status.node_name = instance.node_name;
  }
  if (status.gpu_device.empty() && instance.gpu_device.has_value()) {
    status.gpu_device = *instance.gpu_device;
  }
  naim::RuntimeProcessStatus process;
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

std::optional<std::string> HostdRuntimeTelemetrySupport::ParseTaggedValue(
    const std::string& text,
    const std::string& key) const {
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

std::vector<std::string> HostdRuntimeTelemetrySupport::SplitCsvRow(
    const std::string& line) const {
  std::vector<std::string> result;
  std::stringstream stream(line);
  std::string current;
  while (std::getline(stream, current, ',')) {
    result.push_back(command_support_.Trim(current));
  }
  return result;
}

std::optional<std::string> HostdRuntimeTelemetrySupport::ResolveComposeContainerIdForService(
    const std::string& service_name) const {
  const std::string output = command_support_.RunCommandCapture(
      command_support_.ResolvedDockerCommand() +
      " ps --filter label=com.docker.compose.service=" + service_name +
      " --format '{{.ID}}' 2>/dev/null");
  std::istringstream input(output);
  std::string container_id;
  while (std::getline(input, container_id)) {
    container_id = command_support_.Trim(container_id);
    if (!container_id.empty()) {
      return container_id;
    }
  }
  return std::nullopt;
}

std::optional<int> HostdRuntimeTelemetrySupport::ResolveServiceHostPid(
    const std::string& service_name) const {
  const auto container_id = ResolveComposeContainerIdForService(service_name);
  if (!container_id.has_value()) {
    return std::nullopt;
  }
  const std::string output = command_support_.RunCommandCapture(
      command_support_.ResolvedDockerCommand() + " top " + *container_id +
      " -eo pid,comm,args 2>/dev/null");
  std::istringstream input(output);
  std::string line;
  bool first = true;
  std::optional<int> fallback_pid;
  while (std::getline(input, line)) {
    if (first) {
      first = false;
      continue;
    }
    line = command_support_.Trim(line);
    if (line.empty()) {
      continue;
    }
    std::istringstream row(line);
    int pid = 0;
    std::string comm;
    row >> pid >> comm;
    std::string args;
    std::getline(row, args);
    args = command_support_.Trim(args);
    if (pid <= 0) {
      continue;
    }
    if (args.find("naim-workerd") != std::string::npos ||
        args.find("naim-inferctl") != std::string::npos) {
      return pid;
    }
    if (comm != "tini" && comm != "bash" && comm != "sh" && !fallback_pid.has_value()) {
      fallback_pid = pid;
    }
  }
  return fallback_pid;
}

bool HostdRuntimeTelemetrySupport::IsContainerAbsentForService(
    const std::string& service_name) const {
  return !ResolveComposeContainerIdForService(service_name).has_value();
}

}  // namespace naim::hostd
