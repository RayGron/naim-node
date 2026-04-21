#pragma once

#include <string>

namespace naim::worker {

struct WorkerConfig {
  std::string plane_name;
  std::string instance_name;
  std::string instance_role = "worker";
  std::string node_name;
  std::string control_root;
  std::string shared_disk_path;
  std::string private_disk_path;
  std::string status_path;
  std::string model_path;
  std::string gpu_device;
  std::string boot_mode = "llama-idle";
  std::string distributed_backend = "local";
  std::string rpc_host = "0.0.0.0";
  std::string rpc_endpoint;
  std::string rpc_device;
  int rpc_port = 50052;
  int llama_ctx_size = 2048;
  int llama_threads = 2;
  int llama_gpu_layers = 99;
  int graceful_stop_timeout_sec = 15;
};

}  // namespace naim::worker
