#pragma once

#include <string>
#include <vector>

#include <nlohmann/json.hpp>

namespace comet::infer {

struct RuntimeConfig {
  nlohmann::json raw;
  std::string plane_name;
  std::string instance_name;
  std::string control_root;
  std::string controller_url;
  std::string primary_infer_node;
  std::string runtime_engine = "llama.cpp";
  std::string distributed_backend = "local";
  std::string data_parallel_mode = "off";
  std::string data_parallel_lb_mode = "external";
  int api_server_count = 0;
  nlohmann::json worker_group = nlohmann::json::object();
  std::string net_if;
  std::string models_root;
  std::string model_cache_dir;
  std::string gguf_cache_dir;
  std::string infer_log_dir;
  int api_port = 8000;
  int llama_port = 8000;
  int max_model_len = 8192;
  int max_num_seqs = 16;
  double gpu_memory_utilization = 0.9;
  int llama_ctx_size = 8192;
  int llama_threads = 8;
  int llama_gpu_layers = 99;
  std::string gateway_listen_host;
  int gateway_listen_port = 80;
  std::string gateway_server_name;
  std::vector<std::string> replica_upstreams;
  nlohmann::json gpu_nodes = nlohmann::json::array();
  nlohmann::json serving_workers = nlohmann::json::array();
};

}  // namespace comet::infer
