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

json BuildServingWorkersJson(const DesiredState& state) {
  json serving_workers = json::array();
  if (!state.runtime_gpu_nodes.empty()) {
    for (const auto& gpu_node : state.runtime_gpu_nodes) {
      json worker_json = {
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
        worker_json["memory_cap_mb"] = *gpu_node.memory_cap_mb;
      }
      worker_json["colocated_with_primary_infer"] =
          gpu_node.node_name == state.inference.primary_infer_node;
      serving_workers.push_back(std::move(worker_json));
    }
    return serving_workers;
  }

  for (const auto& instance : state.instances) {
    if (instance.role != InstanceRole::Worker) {
      continue;
    }
    json worker_json = {
        {"name", instance.name},
        {"node_name", instance.node_name},
        {"gpu_device", instance.gpu_device.value_or("")},
        {"placement_mode", ToString(instance.placement_mode)},
        {"share_mode", ToString(instance.share_mode)},
        {"gpu_fraction", instance.gpu_fraction},
        {"priority", instance.priority},
        {"preemptible", instance.preemptible},
        {"enabled", true},
        {"colocated_with_primary_infer",
         instance.node_name == state.inference.primary_infer_node},
    };
    if (instance.memory_cap_mb.has_value()) {
      worker_json["memory_cap_mb"] = *instance.memory_cap_mb;
    }
    serving_workers.push_back(std::move(worker_json));
  }
  return serving_workers;
}

json BuildWorkerGroupJson(const DesiredState& state) {
  json members = json::array();
  for (const auto& member : state.worker_group.members) {
    json item = {
        {"name", member.name},
        {"node_name", member.node_name},
        {"gpu_device", member.gpu_device},
        {"rank", member.rank},
        {"gpu_fraction", member.gpu_fraction},
        {"share_mode", ToString(member.share_mode)},
        {"priority", member.priority},
        {"preemptible", member.preemptible},
        {"enabled", member.enabled},
        {"leader", member.leader},
    };
    if (member.memory_cap_mb.has_value()) {
      item["memory_cap_mb"] = *member.memory_cap_mb;
    }
    members.push_back(std::move(item));
  }
  return {
      {"group_id", state.worker_group.group_id},
      {"infer_instance_name", state.worker_group.infer_instance_name},
      {"distributed_backend", state.worker_group.distributed_backend},
      {"rendezvous_host", state.worker_group.rendezvous_host},
      {"rendezvous_port", state.worker_group.rendezvous_port},
      {"expected_workers", state.worker_group.expected_workers},
      {"worker_selection_policy", state.worker_group.worker_selection_policy},
      {"members", std::move(members)},
  };
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
      {"serving_workers", BuildServingWorkersJson(state)},
      {"inference",
       {
           {"primary_infer_node", state.inference.primary_infer_node},
           {"runtime_engine", state.inference.runtime_engine},
           {"worker_group_id", state.inference.worker_group_id},
           {"distributed_backend", state.inference.distributed_backend},
           {"worker_selection_policy", state.inference.worker_selection_policy},
           {"net_if", state.inference.net_if},
           {"models_root", state.inference.models_root},
           {"model_cache_dir", state.inference.model_cache_dir},
           {"runtime_log_dir", state.inference.runtime_log_dir},
           {"api_port", state.inference.api_port},
           {"max_model_len", state.inference.max_model_len},
           {"tensor_parallel_size", state.inference.tensor_parallel_size},
           {"pipeline_parallel_size", state.inference.pipeline_parallel_size},
           {"max_num_seqs", state.inference.max_num_seqs},
           {"gpu_memory_utilization", state.inference.gpu_memory_utilization},
           {"enforce_eager", state.inference.enforce_eager},
           {"gguf_cache_dir", state.inference.gguf_cache_dir},
           {"infer_log_dir", state.inference.infer_log_dir},
           {"llama_port", state.inference.llama_port},
           {"llama_ctx_size", state.inference.llama_ctx_size},
           {"llama_threads", state.inference.llama_threads},
           {"llama_gpu_layers", state.inference.llama_gpu_layers},
           {"inference_healthcheck_retries", state.inference.inference_healthcheck_retries},
           {"inference_healthcheck_interval_sec",
            state.inference.inference_healthcheck_interval_sec},
           {"rendezvous_port", state.inference.rendezvous_port},
       }},
      {"worker_group", BuildWorkerGroupJson(state)},
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
