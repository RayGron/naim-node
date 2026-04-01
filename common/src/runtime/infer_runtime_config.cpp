#include "comet/runtime/infer_runtime_config.h"

#include <filesystem>
#include <stdexcept>
#include <set>
#include <utility>

#include <nlohmann/json.hpp>

#include "comet/state/worker_group_topology.h"

namespace comet {

namespace {

using nlohmann::json;

bool WorkerAssignedToInfer(
    const WorkerGroupMemberSpec& member,
    const std::string& infer_instance_name) {
  return member.enabled &&
         (member.infer_instance_name.empty() || member.infer_instance_name == infer_instance_name);
}

std::vector<std::string> SplitCommaSeparated(std::string_view text) {
  std::vector<std::string> result;
  std::string current;
  for (const char ch : text) {
    if (ch == ',') {
      if (!current.empty()) {
        result.push_back(current);
        current.clear();
      }
      continue;
    }
    if (ch == ' ' || ch == '\t' || ch == '\n' || ch == '\r') {
      continue;
    }
    current.push_back(ch);
  }
  if (!current.empty()) {
    result.push_back(current);
  }
  return result;
}

int ResolveInferPort(
    const InstanceSpec& infer,
    const std::string& env_name,
    int fallback) {
  const auto it = infer.environment.find(env_name);
  if (it == infer.environment.end() || it->second.empty()) {
    return fallback;
  }
  return std::stoi(it->second);
}

const InstanceSpec& RequireInferInstanceByName(
    const DesiredState& state,
    const std::string& infer_instance_name) {
  for (const auto& instance : state.instances) {
    if (instance.role == InstanceRole::Infer &&
        (infer_instance_name.empty() || instance.name == infer_instance_name)) {
      return instance;
    }
  }
  if (infer_instance_name.empty()) {
    throw std::runtime_error("desired state does not contain an infer instance");
  }
  throw std::runtime_error(
      "desired state does not contain infer instance '" + infer_instance_name + "'");
}

const InstanceSpec& RequireDefaultInferInstance(const DesiredState& state) {
  for (const auto& instance : state.instances) {
    if (instance.role == InstanceRole::Infer) {
      return instance;
    }
  }
  throw std::runtime_error("desired state does not contain an infer instance");
}

std::set<std::string> SelectedWorkerNames(
    const DesiredState& state,
    const std::string& infer_instance_name) {
  std::set<std::string> names;
  for (const auto& member : state.worker_group.members) {
    if (WorkerAssignedToInfer(member, infer_instance_name)) {
      names.insert(member.name);
    }
  }
  return names;
}

json BuildGpuNodesJson(const DesiredState& state, const std::string& infer_instance_name) {
  const std::set<std::string> selected_workers = SelectedWorkerNames(state, infer_instance_name);
  json gpu_nodes = json::array();
  if (!state.runtime_gpu_nodes.empty()) {
    for (const auto& gpu_node : state.runtime_gpu_nodes) {
      if (!selected_workers.empty() && selected_workers.count(gpu_node.name) == 0) {
        continue;
      }
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
    if (!selected_workers.empty() && selected_workers.count(instance.name) == 0) {
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

json BuildServingWorkersJson(const DesiredState& state, const std::string& infer_instance_name) {
  const auto& infer = RequireInferInstanceByName(state, infer_instance_name);
  const std::set<std::string> selected_workers = SelectedWorkerNames(state, infer_instance_name);
  json serving_workers = json::array();
  if (!state.runtime_gpu_nodes.empty()) {
    for (const auto& gpu_node : state.runtime_gpu_nodes) {
      if (!selected_workers.empty() && selected_workers.count(gpu_node.name) == 0) {
        continue;
      }
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
          gpu_node.node_name == infer.node_name;
      serving_workers.push_back(std::move(worker_json));
    }
    return serving_workers;
  }

  for (const auto& instance : state.instances) {
    if (instance.role != InstanceRole::Worker) {
      continue;
    }
    if (!selected_workers.empty() && selected_workers.count(instance.name) == 0) {
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
         instance.node_name == infer.node_name},
    };
    if (instance.memory_cap_mb.has_value()) {
      worker_json["memory_cap_mb"] = *instance.memory_cap_mb;
    }
    serving_workers.push_back(std::move(worker_json));
  }
  return serving_workers;
}

json BuildWorkerGroupJson(const DesiredState& state, const std::string& infer_instance_name) {
  json members = json::array();
  const auto& infer = RequireInferInstanceByName(state, infer_instance_name);
  int selected_member_count = 0;
  for (const auto& member : state.worker_group.members) {
    if (!WorkerAssignedToInfer(member, infer_instance_name)) {
      continue;
    }
    const bool colocated_with_primary_infer =
        member.node_name == infer.node_name;
    const int rpc_port =
        state.inference.runtime_engine == "llama.cpp" &&
                state.inference.distributed_backend == "llama_rpc"
            ? StableLlamaRpcWorkerPort(state.plane_name, member.name)
            : member.rpc_port;
    std::string rpc_endpoint;
    if (rpc_port > 0) {
      if (colocated_with_primary_infer && !member.name.empty()) {
        rpc_endpoint = member.name + ":" + std::to_string(rpc_port);
      } else if (!member.node_name.empty()) {
        rpc_endpoint = member.node_name + ":" + std::to_string(rpc_port);
      }
    }
    json item = {
        {"name", member.name},
        {"infer_instance_name", member.infer_instance_name},
        {"node_name", member.node_name},
        {"gpu_device", member.gpu_device},
        {"rank", member.rank},
        {"replica_group_id", member.replica_group_id},
        {"replica_index", member.replica_index},
        {"replica_size", member.replica_size},
        {"replica_leader", member.replica_leader},
        {"data_parallel_rank", member.data_parallel_rank},
        {"data_parallel_size", member.data_parallel_size},
        {"data_parallel_size_local", member.data_parallel_size_local},
        {"data_parallel_start_rank", member.data_parallel_start_rank},
        {"data_parallel_api_endpoint", member.data_parallel_api_endpoint},
        {"data_parallel_head_address", member.data_parallel_head_address},
        {"data_parallel_rpc_port", member.data_parallel_rpc_port},
        {"rpc_port", rpc_port},
        {"rpc_endpoint", rpc_endpoint},
        {"colocated_with_primary_infer", colocated_with_primary_infer},
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
    ++selected_member_count;
  }
  return {
      {"group_id", state.worker_group.group_id},
      {"infer_instance_name",
       infer_instance_name.empty() ? state.worker_group.infer_instance_name : infer_instance_name},
      {"distributed_backend", state.worker_group.distributed_backend},
      {"rendezvous_host", state.worker_group.rendezvous_host},
      {"rendezvous_port", state.worker_group.rendezvous_port},
      {"expected_workers", selected_member_count > 0 ? selected_member_count
                                                     : state.worker_group.expected_workers},
      {"worker_selection_policy", state.worker_group.worker_selection_policy},
      {"members", std::move(members)},
  };
}

}  // namespace

std::string InferRuntimeConfigRelativePath(const std::string& infer_instance_name) {
  if (infer_instance_name.empty()) {
    return "infer-runtime.json";
  }
  return (std::filesystem::path("infer") / infer_instance_name / "infer-runtime.json").string();
}

std::string InferRuntimeConfigControlPath(
    const std::string& control_root,
    const std::string& infer_instance_name) {
  if (control_root.empty()) {
    return InferRuntimeConfigRelativePath(infer_instance_name);
  }
  return (std::filesystem::path(control_root) / InferRuntimeConfigRelativePath(infer_instance_name))
      .string();
}

std::string InferRuntimeStatusRelativePath(const std::string& infer_instance_name) {
  if (infer_instance_name.empty()) {
    return "runtime-status.json";
  }
  return (std::filesystem::path("infer") / infer_instance_name / "runtime-status.json").string();
}

std::string InferRuntimeStatusControlPath(
    const std::string& control_root,
    const std::string& infer_instance_name) {
  if (control_root.empty()) {
    return InferRuntimeStatusRelativePath(infer_instance_name);
  }
  return (std::filesystem::path(control_root) / InferRuntimeStatusRelativePath(infer_instance_name))
      .string();
}

std::string RenderInferRuntimeConfigJson(const DesiredState& state) {
  const auto& infer = RequireDefaultInferInstance(state);
  return RenderInferRuntimeConfigJsonForInstance(state, infer.name);
}

std::string RenderInferRuntimeConfigJsonForInstance(
    const DesiredState& state,
    const std::string& infer_instance_name) {
  const auto& infer = RequireInferInstanceByName(state, infer_instance_name);
  const int api_port = ResolveInferPort(infer, "COMET_INFERENCE_PORT", state.inference.api_port);
  const int gateway_listen_port =
      ResolveInferPort(infer, "COMET_GATEWAY_PORT", state.gateway.listen_port);
  const int llama_port = ResolveInferPort(infer, "COMET_LLAMA_PORT", state.inference.llama_port);
  const std::vector<std::string> replica_upstreams =
      infer.environment.count("COMET_REPLICA_UPSTREAMS") == 0
          ? std::vector<std::string>{}
          : SplitCommaSeparated(infer.environment.at("COMET_REPLICA_UPSTREAMS"));

  const json value = {
      {"comet_version", "0.1.0"},
      {"instance",
       {
           {"name", infer.name},
       }},
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
      {"gpu_nodes", BuildGpuNodesJson(state, infer.name)},
      {"serving_workers", BuildServingWorkersJson(state, infer.name)},
      {"inference",
       {
           {"primary_infer_node", infer.node_name},
           {"runtime_engine", state.inference.runtime_engine},
           {"data_parallel_mode", state.inference.data_parallel_mode},
           {"data_parallel_lb_mode", state.inference.data_parallel_lb_mode},
           {"api_server_count", state.inference.api_server_count},
           {"worker_group_id", state.inference.worker_group_id},
           {"distributed_backend", state.inference.distributed_backend},
           {"worker_selection_policy", state.inference.worker_selection_policy},
           {"net_if", state.inference.net_if},
           {"models_root", state.inference.models_root},
           {"model_cache_dir", state.inference.model_cache_dir},
           {"runtime_log_dir", state.inference.runtime_log_dir},
           {"api_port", api_port},
           {"max_model_len", state.inference.max_model_len},
           {"tensor_parallel_size", state.inference.tensor_parallel_size},
           {"pipeline_parallel_size", state.inference.pipeline_parallel_size},
           {"max_num_seqs", state.inference.max_num_seqs},
           {"gpu_memory_utilization", state.inference.gpu_memory_utilization},
           {"enforce_eager", state.inference.enforce_eager},
           {"gguf_cache_dir", state.inference.gguf_cache_dir},
           {"infer_log_dir", state.inference.infer_log_dir},
           {"llama_port", llama_port},
           {"llama_ctx_size", state.inference.llama_ctx_size},
           {"llama_threads", state.inference.llama_threads},
           {"llama_gpu_layers", state.inference.llama_gpu_layers},
           {"inference_healthcheck_retries", state.inference.inference_healthcheck_retries},
           {"inference_healthcheck_interval_sec",
            state.inference.inference_healthcheck_interval_sec},
           {"rendezvous_port", state.inference.rendezvous_port},
       }},
      {"worker_group", BuildWorkerGroupJson(state, infer.name)},
      {"replica_upstreams", replica_upstreams},
      {"gateway",
       {
           {"listen_host", state.gateway.listen_host},
           {"listen_port", gateway_listen_port},
           {"server_name", state.gateway.server_name},
       }},
  };

  return value.dump(2);
}

}  // namespace comet
