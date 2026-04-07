#include "worker_status_service.h"

#include "comet/runtime/runtime_status.h"

#include <chrono>
#include <ctime>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <sstream>
#include <system_error>
#include <unistd.h>

namespace fs = std::filesystem;

namespace comet::worker {

std::string WorkerStatusService::CurrentTimestamp() const {
  const auto now = std::chrono::system_clock::now();
  const std::time_t time = std::chrono::system_clock::to_time_t(now);
  std::tm tm{};
  gmtime_r(&time, &tm);
  std::ostringstream out;
  out << std::put_time(&tm, "%Y-%m-%dT%H:%M:%SZ");
  return out.str();
}

void WorkerStatusService::MarkStarting(
    const WorkerConfig& config,
    const std::string& started_at,
    const std::optional<std::string>& model_path) const {
  TouchReadyFile(false);
  WriteStatus(
      BuildStatus(config, "starting", false, started_at, CurrentTimestamp(), model_path.value_or("")),
      config.status_path);
}

void WorkerStatusService::MarkWaitingForModel(
    const WorkerConfig& config,
    const std::string& started_at,
    const std::optional<std::string>& model_path) const {
  TouchReadyFile(false);
  WriteStatus(
      BuildStatus(config, "waiting-for-model", false, started_at, "", model_path.value_or("")),
      config.status_path);
}

void WorkerStatusService::MarkRunning(
    const WorkerConfig& config,
    const std::string& started_at,
    const std::string& model_path) const {
  TouchReadyFile(true);
  WriteStatus(
      BuildStatus(config, "running", true, started_at, CurrentTimestamp(), model_path),
      config.status_path);
}

void WorkerStatusService::MarkFailed(
    const WorkerConfig& config,
    const std::string& started_at,
    const std::optional<std::string>& model_path) const {
  TouchReadyFile(false);
  WriteStatus(
      BuildStatus(
          config,
          "failed",
          false,
          started_at,
          CurrentTimestamp(),
          model_path.value_or("")),
      config.status_path);
}

void WorkerStatusService::MarkStopped(
    const WorkerConfig& config,
    const std::string& started_at,
    const std::optional<std::string>& model_path) const {
  TouchReadyFile(false);
  WriteStatus(
      BuildStatus(
          config,
          "stopped",
          false,
          started_at,
          CurrentTimestamp(),
          model_path.value_or("")),
      config.status_path);
}

void WorkerStatusService::TouchReadyFile(bool ready) {
  const fs::path ready_path("/tmp/comet-ready");
  if (ready) {
    fs::create_directories(ready_path.parent_path());
    std::ofstream(ready_path) << "ready\n";
  } else {
    std::error_code error;
    fs::remove(ready_path, error);
  }
}

comet::RuntimeStatus WorkerStatusService::BuildStatus(
    const WorkerConfig& config,
    const std::string& phase,
    bool ready,
    const std::string& started_at,
    const std::string& last_activity_at,
    const std::string& model_path) {
  comet::RuntimeStatus status;
  status.plane_name = config.plane_name;
  status.control_root = config.control_root;
  status.instance_name = config.instance_name;
  status.instance_role = config.instance_role;
  status.node_name = config.node_name;
  status.runtime_backend =
      config.boot_mode == "llama-rpc" ? "llama-rpc-worker" : "comet-worker";
  status.runtime_phase = phase;
  status.runtime_pid = static_cast<int>(getpid());
  status.engine_pid = static_cast<int>(getpid());
  status.started_at = started_at;
  status.last_activity_at = last_activity_at;
  status.model_path = model_path;
  status.cached_local_model_path = model_path;
  status.gpu_device = config.gpu_device;
  status.enabled_gpu_nodes = config.gpu_device.empty() ? 0 : 1;
  status.rpc_endpoint = config.rpc_endpoint;
  status.ready = ready;
  status.inference_ready = ready;
  status.launch_ready = ready;
  return status;
}

void WorkerStatusService::WriteStatus(
    const comet::RuntimeStatus& status,
    const std::string& path) {
  comet::SaveRuntimeStatusJson(status, path);
}

}  // namespace comet::worker
