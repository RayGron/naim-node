#include "comet/infer_runtime_config.h"

#include <stdexcept>
#include <utility>

#include <nlohmann/json.hpp>

namespace comet {

namespace {

using nlohmann::json;

const InstanceSpec& RequireInferInstance(const DesiredState& state) {
  for (const auto& instance : state.instances) {
    if (instance.role == InstanceRole::Infer) {
      return instance;
    }
  }
  throw std::runtime_error("desired state does not contain an infer instance");
}

json BuildGpuNodesJson(const DesiredState& state) {
  json gpu_nodes = json::array();
  if (!state.runtime_gpu_nodes.empty()) {
    for (const auto& gpu_node : state.runtime_gpu_nodes) {
      json gpu_node_json = {
          {"name", gpu_node.name},
          {"node_name", gpu_node.node_name},
          {"gpu_device", gpu_node.gpu_device},
          {"placement_mode", ToString(gpu_node.placement_mode)},
          {"share_mode", ToString(gpu_node.share_mode)},
          {"gpu_fraction", gpu_node.gpu_fraction},
          {"priority", gpu_node.priority},
          {"preemptible", gpu_node.preemptible},
          {"enabled", gpu_node.enabled},
      };
      if (gpu_node.memory_cap_mb.has_value()) {
        gpu_node_json["memory_cap_mb"] = *gpu_node.memory_cap_mb;
      }
      gpu_nodes.push_back(std::move(gpu_node_json));
    }
    return gpu_nodes;
  }

  for (const auto& instance : state.instances) {
    if (instance.role != InstanceRole::Worker) {
      continue;
    }
    json gpu_node_json = {
        {"name", instance.name},
        {"node_name", instance.node_name},
        {"gpu_device", instance.gpu_device.value_or("")},
        {"placement_mode", ToString(instance.placement_mode)},
        {"share_mode", ToString(instance.share_mode)},
        {"gpu_fraction", instance.gpu_fraction},
        {"priority", instance.priority},
        {"preemptible", instance.preemptible},
        {"enabled", true},
    };
    if (instance.memory_cap_mb.has_value()) {
      gpu_node_json["memory_cap_mb"] = *instance.memory_cap_mb;
    }
    gpu_nodes.push_back(std::move(gpu_node_json));
  }
  return gpu_nodes;
}

}  // namespace

std::string RenderInferRuntimeConfigJson(const DesiredState& state) {
  const auto& infer = RequireInferInstance(state);

  const json value = {
      {"comet_version", "0.1.0"},
      {"plane",
       {
           {"name", state.plane_name},
           {"control_root", state.control_root},
       }},
      {"control",
       {
           {"root", state.control_root},
           {"controller_url", infer.environment.count("COMET_CONTROLLER_URL") == 0
                                  ? "http://controller.internal:8080"
                                  : infer.environment.at("COMET_CONTROLLER_URL")},
       }},
      {"gpu_nodes", BuildGpuNodesJson(state)},
      {"inference",
       {
           {"primary_infer_node", state.inference.primary_infer_node},
           {"net_if", state.inference.net_if},
           {"models_root", state.inference.models_root},
           {"gguf_cache_dir", state.inference.gguf_cache_dir},
           {"infer_log_dir", state.inference.infer_log_dir},
           {"llama_port", state.inference.llama_port},
           {"llama_ctx_size", state.inference.llama_ctx_size},
           {"llama_threads", state.inference.llama_threads},
           {"llama_gpu_layers", state.inference.llama_gpu_layers},
           {"inference_healthcheck_retries", state.inference.inference_healthcheck_retries},
           {"inference_healthcheck_interval_sec",
            state.inference.inference_healthcheck_interval_sec},
       }},
      {"gateway",
       {
           {"listen_host", state.gateway.listen_host},
           {"listen_port", state.gateway.listen_port},
           {"server_name", state.gateway.server_name},
       }},
  };

  return value.dump(2);
}

}  // namespace comet
