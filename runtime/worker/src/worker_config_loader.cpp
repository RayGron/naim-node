#include "worker_config_loader.h"

#include <algorithm>
#include <cctype>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <memory>
#include <string>

#include <cstdio>

namespace naim::worker {

namespace {

std::string ResolveVisibleGpuDevice() {
  const char* visible_devices = std::getenv("NVIDIA_VISIBLE_DEVICES");
  if (visible_devices == nullptr || std::strlen(visible_devices) == 0) {
    return "";
  }

  const std::string devices(visible_devices);
  if (devices == "none" || devices == "void" || devices == "all") {
    return "";
  }

  const auto comma = devices.find(',');
  const std::string first = devices.substr(0, comma);
  const auto begin = std::find_if_not(first.begin(), first.end(), [](unsigned char ch) {
    return std::isspace(ch) != 0;
  });
  const auto end = std::find_if_not(first.rbegin(), first.rend(), [](unsigned char ch) {
    return std::isspace(ch) != 0;
  }).base();
  if (begin >= end) {
    return "";
  }
  return std::string(begin, end);
}

std::string ResolveGpuDeviceFromNvidiaSmi() {
  std::unique_ptr<FILE, decltype(&pclose)> pipe(
      popen("nvidia-smi --query-gpu=index --format=csv,noheader 2>/dev/null", "r"),
      pclose);
  if (!pipe) {
    return "";
  }

  char buffer[128];
  if (fgets(buffer, sizeof(buffer), pipe.get()) == nullptr) {
    return "";
  }

  std::string gpu_index(buffer);
  gpu_index.erase(std::remove(gpu_index.begin(), gpu_index.end(), '\n'), gpu_index.end());
  gpu_index.erase(std::remove(gpu_index.begin(), gpu_index.end(), '\r'), gpu_index.end());
  const auto begin = std::find_if_not(gpu_index.begin(), gpu_index.end(), [](unsigned char ch) {
    return std::isspace(ch) != 0;
  });
  const auto end = std::find_if_not(gpu_index.rbegin(), gpu_index.rend(), [](unsigned char ch) {
    return std::isspace(ch) != 0;
  }).base();
  if (begin >= end) {
    return "";
  }
  return std::string(begin, end);
}

}  // namespace

WorkerConfig WorkerConfigLoader::Load() const {
  WorkerConfig config;
  config.plane_name = GetEnvOr("NAIM_PLANE_NAME", "unknown");
  config.instance_name = GetEnvOr("NAIM_INSTANCE_NAME", "worker");
  config.instance_role = GetEnvOr("NAIM_INSTANCE_ROLE", "worker");
  config.node_name = GetEnvOr("NAIM_NODE_NAME");
  config.control_root = GetEnvOr("NAIM_CONTROL_ROOT");
  config.shared_disk_path = GetEnvOr("NAIM_SHARED_DISK_PATH", "/naim/shared");
  config.private_disk_path = GetEnvOr("NAIM_PRIVATE_DISK_PATH", "/naim/private");
  config.status_path = GetEnvOr(
      "NAIM_WORKER_RUNTIME_STATUS_PATH",
      (std::filesystem::path(config.private_disk_path) / "worker-runtime-status.json").string());
  config.model_path = GetEnvOr("NAIM_WORKER_MODEL_PATH");
  config.gpu_device =
      GetEnvOr("NAIM_GPU_DEVICE", GetEnvOr("NAIM_WORKER_GPU_DEVICE", ResolveVisibleGpuDevice()));
  if (config.gpu_device.empty()) {
    config.gpu_device = ResolveGpuDeviceFromNvidiaSmi();
  }
  config.boot_mode = GetEnvOr("NAIM_WORKER_BOOT_MODE", "llama-idle");
  config.distributed_backend = GetEnvOr("NAIM_DISTRIBUTED_BACKEND", "local");
  config.rpc_host = GetEnvOr("NAIM_WORKER_RPC_HOST", "0.0.0.0");
  config.rpc_port = GetEnvIntOr("NAIM_WORKER_RPC_PORT", 50052);
  config.rpc_endpoint = GetEnvOr("NAIM_WORKER_RPC_ENDPOINT");
  config.rpc_device = GetEnvOr("NAIM_WORKER_RPC_DEVICE", GetEnvOr("NAIM_RPC_SERVER_DEVICE"));
  config.llama_ctx_size = GetEnvIntOr("NAIM_WORKER_CTX_SIZE", 2048);
  config.llama_threads = GetEnvIntOr("NAIM_WORKER_THREADS", 2);
  config.llama_gpu_layers = GetEnvIntOr("NAIM_LLAMA_GPU_LAYERS", 99);
  config.graceful_stop_timeout_sec =
      GetEnvIntOr("NAIM_WORKER_GRACEFUL_STOP_TIMEOUT_SEC", 15);
  return config;
}

std::string WorkerConfigLoader::GetEnvOr(const char* name, const std::string& fallback) {
  const char* value = std::getenv(name);
  if (value == nullptr || std::strlen(value) == 0) {
    return fallback;
  }
  return value;
}

int WorkerConfigLoader::GetEnvIntOr(const char* name, int fallback) {
  const char* value = std::getenv(name);
  if (value == nullptr || std::strlen(value) == 0) {
    return fallback;
  }
  return std::stoi(value);
}

}  // namespace naim::worker
