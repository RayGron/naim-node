#include "interaction/interaction_runtime_support_service.h"

#include <algorithm>
#include <map>

#include "comet/state/worker_group_topology.h"

namespace {

std::string NormalizeInteractionHost(const std::string& host) {
  if (host == "0.0.0.0" || host == "::" || host.empty()) {
    return "127.0.0.1";
  }
  return host;
}

struct ReplicaGroupSummary {
  int expected_replica_groups = 0;
  int ready_replica_groups = 0;
  int degraded_replica_groups = 0;
  int ready_worker_members = 0;
  int expected_worker_members = 0;
  int expected_api_endpoints = 0;
  int ready_api_endpoints = 0;
  int data_parallel_size = 0;
  int data_parallel_size_local_max = 0;
};

std::string HybridReplicaGroupKey(const comet::WorkerGroupMemberSpec& member) {
  const std::string node_name = member.node_name.empty() ? std::string("unknown-node")
                                                         : member.node_name;
  return "hybrid-node-" + node_name + "-start-" +
         std::to_string(std::max(0, member.data_parallel_start_rank));
}

ReplicaGroupSummary SummarizeReplicaGroups(
    const comet::DesiredState& desired_state,
    const std::vector<comet::RuntimeProcessStatus>& instance_statuses) {
  const bool llama_rpc_runtime =
      desired_state.inference.runtime_engine == "llama.cpp" &&
      desired_state.inference.distributed_backend == "llama_rpc";
  const bool hybrid_data_parallel =
      comet::DataParallelEnabled(desired_state.inference) &&
      desired_state.inference.data_parallel_lb_mode == comet::kDataParallelLbModeHybrid;
  ReplicaGroupSummary summary;
  if (llama_rpc_runtime) {
    std::map<std::string, std::pair<int, int>> groups;
    for (const auto& member : desired_state.worker_group.members) {
      if (!member.enabled) {
        continue;
      }
      ++summary.expected_worker_members;
      const std::string group_key =
          member.infer_instance_name.empty() ? desired_state.worker_group.infer_instance_name
                                             : member.infer_instance_name;
      auto& group = groups[group_key];
      ++group.first;
      const auto status_it = std::find_if(
          instance_statuses.begin(),
          instance_statuses.end(),
          [&](const comet::RuntimeProcessStatus& status) {
            return status.instance_name == member.name;
          });
      if (status_it != instance_statuses.end() && status_it->ready) {
        ++summary.ready_worker_members;
        ++group.second;
      }
    }
    summary.expected_replica_groups = static_cast<int>(groups.size());
    for (const auto& [_, group] : groups) {
      if (group.second >= group.first && group.first > 0) {
        ++summary.ready_replica_groups;
      } else {
        ++summary.degraded_replica_groups;
      }
    }
    return summary;
  }

  std::map<std::string, std::pair<int, int>> groups;
  for (const auto& member : desired_state.worker_group.members) {
    if (!member.enabled) {
      continue;
    }
    ++summary.expected_worker_members;
    const std::string key = hybrid_data_parallel
                                ? HybridReplicaGroupKey(member)
                                : (member.replica_group_id.empty() ? std::string("replica-0")
                                                                   : member.replica_group_id);
    auto& group = groups[key];
    group.first = std::max(
        group.first,
        hybrid_data_parallel ? 1 : std::max(1, member.replica_size));

    const auto status_it = std::find_if(
        instance_statuses.begin(),
        instance_statuses.end(),
        [&](const comet::RuntimeProcessStatus& status) {
          return status.instance_name == member.name;
        });
    if (member.data_parallel_api_endpoint) {
      ++summary.expected_api_endpoints;
    }
    if (status_it != instance_statuses.end() && status_it->ready) {
      ++summary.ready_worker_members;
      if (member.data_parallel_api_endpoint) {
        ++summary.ready_api_endpoints;
      }
      if (!hybrid_data_parallel || member.data_parallel_api_endpoint) {
        ++group.second;
      }
    }
    summary.data_parallel_size =
        std::max(summary.data_parallel_size, member.data_parallel_size);
    summary.data_parallel_size_local_max =
        std::max(summary.data_parallel_size_local_max, member.data_parallel_size_local);
  }

  summary.expected_replica_groups = static_cast<int>(groups.size());
  for (const auto& [_, group] : groups) {
    if (group.second >= std::max(1, group.first)) {
      ++summary.ready_replica_groups;
    } else {
      ++summary.degraded_replica_groups;
    }
  }
  return summary;
}

}  // namespace

namespace comet::controller {

std::optional<ControllerEndpointTarget>
InteractionRuntimeSupportService::ParseInteractionTarget(
    const std::string& gateway_listen,
    int fallback_port) const {
  std::string host = "127.0.0.1";
  int port = fallback_port;
  if (!gateway_listen.empty()) {
    const std::size_t colon = gateway_listen.rfind(':');
    if (colon != std::string::npos) {
      host = NormalizeInteractionHost(gateway_listen.substr(0, colon));
      port = std::stoi(gateway_listen.substr(colon + 1));
    }
  }
  if (port <= 0) {
    return std::nullopt;
  }
  return ParseControllerEndpointTarget(host + ":" + std::to_string(port));
}

std::optional<std::string> InteractionRuntimeSupportService::FindInferInstanceName(
    const comet::DesiredState& desired_state) const {
  for (const auto& instance : desired_state.instances) {
    if (instance.role == comet::InstanceRole::Infer &&
        instance.plane_name == desired_state.plane_name) {
      return instance.name;
    }
  }
  return std::nullopt;
}

std::vector<std::string> InteractionRuntimeSupportService::FindWorkerInstanceNames(
    const comet::DesiredState& desired_state) const {
  std::vector<std::string> names;
  for (const auto& instance : desired_state.instances) {
    if (instance.role == comet::InstanceRole::Worker &&
        instance.plane_name == desired_state.plane_name) {
      names.push_back(instance.name);
    }
  }
  return names;
}

std::optional<comet::RuntimeProcessStatus>
InteractionRuntimeSupportService::FindInstanceRuntimeStatus(
    const std::vector<comet::RuntimeProcessStatus>& statuses,
    const std::string& instance_name) const {
  for (const auto& status : statuses) {
    if (status.instance_name == instance_name) {
      return status;
    }
  }
  return std::nullopt;
}

bool InteractionRuntimeSupportService::ProbeControllerTargetOk(
    const std::optional<ControllerEndpointTarget>& target,
    const std::string& path) const {
  if (!target.has_value()) {
    return false;
  }
  try {
    const HttpResponse response = SendControllerHttpRequest(*target, "GET", path);
    return response.status_code >= 200 && response.status_code < 300;
  } catch (const std::exception&) {
    return false;
  }
}

std::optional<comet::RuntimeStatus>
InteractionRuntimeSupportService::BuildPlaneScopedRuntimeStatus(
    const comet::DesiredState& desired_state,
    const comet::HostObservation& observation,
    const std::function<std::vector<comet::RuntimeProcessStatus>(
        const comet::HostObservation&)>& parse_instance_runtime_statuses) const {
  const auto infer_instance_name = FindInferInstanceName(desired_state);
  if (!infer_instance_name.has_value()) {
    return std::nullopt;
  }

  const auto instance_statuses = parse_instance_runtime_statuses(observation);
  const auto infer_status =
      FindInstanceRuntimeStatus(instance_statuses, *infer_instance_name);
  if (!infer_status.has_value()) {
    return std::nullopt;
  }

  comet::RuntimeStatus runtime;
  if (!observation.runtime_status_json.empty()) {
    try {
      const auto parsed =
          comet::DeserializeRuntimeStatusJson(observation.runtime_status_json);
      if (parsed.plane_name == desired_state.plane_name &&
          parsed.instance_name == *infer_instance_name) {
        runtime = parsed;
      }
    } catch (const std::exception&) {
    }
  }

  const auto worker_instance_names = FindWorkerInstanceNames(desired_state);
  const ReplicaGroupSummary replica_summary =
      SummarizeReplicaGroups(desired_state, instance_statuses);

  runtime.plane_name = desired_state.plane_name;
  runtime.control_root = desired_state.control_root;
  runtime.primary_infer_node = desired_state.inference.primary_infer_node;
  runtime.instance_name = infer_status->instance_name;
  runtime.instance_role = infer_status->instance_role;
  runtime.node_name = infer_status->node_name;
  runtime.data_parallel_mode = desired_state.inference.data_parallel_mode;
  runtime.data_parallel_lb_mode = desired_state.inference.data_parallel_lb_mode;
  runtime.data_parallel_size = replica_summary.data_parallel_size;
  runtime.data_parallel_size_local_max = replica_summary.data_parallel_size_local_max;
  if (desired_state.inference.runtime_engine == "llama.cpp" &&
      desired_state.inference.distributed_backend == "llama_rpc") {
    runtime.runtime_backend = "llama-rpc-head";
  } else {
    runtime.runtime_backend = desired_state.inference.runtime_engine;
  }
  runtime.runtime_phase = infer_status->runtime_phase;
  runtime.replica_groups_expected = replica_summary.expected_replica_groups;
  runtime.replica_groups_ready = replica_summary.ready_replica_groups;
  runtime.replica_groups_degraded = replica_summary.degraded_replica_groups;
  runtime.api_endpoints_expected = replica_summary.expected_api_endpoints;
  runtime.api_endpoints_ready = replica_summary.ready_api_endpoints;
  runtime.enabled_gpu_nodes = static_cast<int>(worker_instance_names.size());
  runtime.registry_entries = replica_summary.ready_worker_members;
  runtime.runtime_pid = infer_status->runtime_pid;
  runtime.engine_pid = infer_status->engine_pid;
  runtime.supervisor_pid = infer_status->runtime_pid;
  if (desired_state.turboquant.has_value() && desired_state.turboquant->enabled) {
    runtime.turboquant_enabled = true;
    runtime.active_cache_type_k =
        desired_state.turboquant->cache_type_k.value_or("planar3");
    runtime.active_cache_type_v =
        desired_state.turboquant->cache_type_v.value_or("f16");
  }
  if (desired_state.bootstrap_model.has_value()) {
    runtime.active_model_id = desired_state.bootstrap_model->model_id;
    runtime.active_served_model_name =
        desired_state.bootstrap_model->served_model_name.value_or(std::string{});
  }
  runtime.cached_local_model_path = infer_status->model_path;
  runtime.model_path = infer_status->model_path;
  runtime.gpu_device = infer_status->gpu_device;
  runtime.started_at = infer_status->started_at;
  runtime.last_activity_at = infer_status->last_activity_at;
  runtime.gateway_listen =
      "0.0.0.0:" + std::to_string(desired_state.gateway.listen_port);
  runtime.gateway_health_url =
      "http://127.0.0.1:" + std::to_string(desired_state.gateway.listen_port) +
      "/health";
  runtime.upstream_models_url =
      "http://127.0.0.1:" + std::to_string(desired_state.gateway.listen_port) +
      "/v1/models";
  runtime.inference_health_url = runtime.gateway_health_url;
  runtime.active_model_ready = true;
  runtime.gateway_plan_ready = true;
  const auto target =
      ParseInteractionTarget(runtime.gateway_listen, desired_state.gateway.listen_port);
  runtime.gateway_ready = ProbeControllerTargetOk(target, "/health");
  runtime.inference_ready = ProbeControllerTargetOk(target, "/v1/models");
  const bool replica_topology_ready =
      replica_summary.expected_replica_groups == 0 ||
      (desired_state.inference.runtime_engine == "llama.cpp" &&
       desired_state.inference.distributed_backend == "llama_rpc"
           ? replica_summary.ready_worker_members >= replica_summary.expected_worker_members
           : replica_summary.ready_replica_groups >= replica_summary.expected_replica_groups);
  runtime.launch_ready =
      runtime.gateway_ready &&
      runtime.inference_ready &&
      replica_topology_ready;
  runtime.ready =
      runtime.active_model_ready && runtime.gateway_ready &&
      runtime.inference_ready && runtime.launch_ready;
  return runtime;
}

int InteractionRuntimeSupportService::CountReadyWorkerMembers(
    comet::ControllerStore& store,
    const comet::DesiredState& desired_state,
    const std::function<std::vector<comet::RuntimeProcessStatus>(
        const comet::HostObservation&)>& parse_instance_runtime_statuses) const {
  int ready_workers = 0;
  for (const auto& worker_name : FindWorkerInstanceNames(desired_state)) {
    const auto instance_it = std::find_if(
        desired_state.instances.begin(),
        desired_state.instances.end(),
        [&](const comet::InstanceSpec& instance) {
          return instance.name == worker_name &&
                 instance.role == comet::InstanceRole::Worker;
        });
    if (instance_it == desired_state.instances.end() ||
        instance_it->node_name.empty()) {
      continue;
    }
    const auto observation = store.LoadHostObservation(instance_it->node_name);
    if (!observation.has_value()) {
      continue;
    }
    const auto instance_statuses = parse_instance_runtime_statuses(*observation);
    const auto worker_status =
        FindInstanceRuntimeStatus(instance_statuses, worker_name);
    if (worker_status.has_value() && worker_status->ready) {
      ++ready_workers;
    }
  }
  return ready_workers;
}

}  // namespace comet::controller
