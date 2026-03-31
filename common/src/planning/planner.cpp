#include "comet/planning/planner.h"

#include <algorithm>
#include <cstdint>
#include <sstream>
#include <stdexcept>

#include "comet/state/worker_group_topology.h"

namespace comet {

namespace {

bool UsesVllmRuntime(const DesiredState& state) {
  return state.inference.runtime_engine == "vllm";
}

bool UsesLlamaRpcRuntime(const DesiredState& state) {
  return state.inference.runtime_engine == "llama.cpp" &&
         state.inference.distributed_backend == "llama_rpc";
}

std::optional<ComposeVolume> BuildDirectModelCacheVolume(const DesiredState& state) {
  if ((!UsesVllmRuntime(state) && !UsesLlamaRpcRuntime(state)) ||
      !state.bootstrap_model.has_value() || !state.bootstrap_model->local_path.has_value() ||
      state.bootstrap_model->materialization_mode != "reference") {
    return std::nullopt;
  }
  const std::string& local_path = *state.bootstrap_model->local_path;
  if (local_path.empty() || local_path.front() != '/') {
    return std::nullopt;
  }
  return ComposeVolume{local_path, local_path, true};
}

constexpr int kWorkerPublishedPortBase = 20000;
constexpr int kWorkerPublishedPortSpan = 20000;
constexpr int kWorkerInternalPortBase = 30000;
constexpr int kWorkerInternalPortSpan = 10000;

std::vector<std::string> DefaultComposeExtraHosts() {
  return {
      "host.docker.internal:host-gateway",
      "controller.internal:host-gateway",
  };
}

uint32_t StableWorkerPortHash(const std::string& value) {
  uint32_t hash = 2166136261u;
  for (unsigned char ch : value) {
    hash ^= static_cast<uint32_t>(ch);
    hash *= 16777619u;
  }
  return hash;
}

std::optional<int> IntEnvValue(
    const std::map<std::string, std::string>& environment,
    const std::string& key) {
  const auto it = environment.find(key);
  if (it == environment.end() || it->second.empty()) {
    return std::nullopt;
  }
  return std::stoi(it->second);
}

int InferApiPort(const DesiredState& state, const InstanceSpec& instance) {
  return IntEnvValue(instance.environment, "COMET_INFERENCE_PORT").value_or(state.inference.api_port);
}

int InferGatewayPort(const DesiredState& state, const InstanceSpec& instance) {
  return IntEnvValue(instance.environment, "COMET_GATEWAY_PORT").value_or(state.gateway.listen_port);
}

int WorkerRpcPort(const WorkerGroupMemberSpec* worker_group_member) {
  return worker_group_member != nullptr && worker_group_member->rpc_port > 0
             ? worker_group_member->rpc_port
             : 50052;
}

int WorkerPublishedHostPort(
    const DesiredState& state,
    const InstanceSpec& instance) {
  const uint32_t offset =
      StableWorkerPortHash(state.plane_name + ":" + instance.name) % kWorkerPublishedPortSpan;
  return kWorkerPublishedPortBase + static_cast<int>(offset);
}

int WorkerInternalRuntimePort(
    const DesiredState& state,
    const InstanceSpec& instance) {
  const uint32_t offset = StableWorkerPortHash(
                              state.plane_name + ":" + instance.name + ":internal") %
      kWorkerInternalPortSpan;
  return kWorkerInternalPortBase + static_cast<int>(offset);
}

bool IsManagedInferPublishedPort(
    const DesiredState& state,
    const InstanceSpec& instance,
    const PublishedPort& port) {
  if (instance.role != InstanceRole::Infer || port.host_port != port.container_port) {
    return false;
  }
  const int infer_api_port = InferApiPort(state, instance);
  const int infer_gateway_port = InferGatewayPort(state, instance);
  return port.host_port == infer_api_port || port.host_port == infer_gateway_port ||
         port.host_port == state.inference.api_port ||
         port.host_port == state.gateway.listen_port;
}

bool IsManagedLlamaRpcWorkerPublishedPort(
    const DesiredState& state,
    const InstanceSpec& instance,
    const WorkerGroupMemberSpec* worker_group_member,
    const PublishedPort& port) {
  if (!UsesLlamaRpcRuntime(state) || instance.role != InstanceRole::Worker ||
      port.host_port != port.container_port) {
    return false;
  }
  return port.host_port == WorkerRpcPort(worker_group_member) || port.host_port == 50052;
}

bool ContainsPublishedPort(
    const std::vector<PublishedPort>& ports,
    const PublishedPort& needle) {
  return std::find_if(
             ports.begin(),
             ports.end(),
             [&](const PublishedPort& port) {
               return port.host_ip == needle.host_ip &&
                      port.host_port == needle.host_port &&
                      port.container_port == needle.container_port;
             }) != ports.end();
}

void AppendUniquePublishedPort(
    std::vector<PublishedPort>* ports,
    const PublishedPort& port) {
  if (ports == nullptr || ContainsPublishedPort(*ports, port)) {
    return;
  }
  ports->push_back(port);
}

const DiskSpec& FindDiskByName(
    const std::vector<DiskSpec>& disks,
    const std::string& node_name,
    const std::string& disk_name) {
  const auto it = std::find_if(
      disks.begin(),
      disks.end(),
      [&](const DiskSpec& disk) {
        return disk.name == disk_name && disk.node_name == node_name;
      });
  if (it == disks.end()) {
    throw std::runtime_error("missing disk '" + disk_name + "' for node '" + node_name + "'");
  }
  return *it;
}

const WorkerGroupMemberSpec* FindWorkerGroupMember(
    const DesiredState& state,
    const std::string& instance_name) {
  const auto it = std::find_if(
      state.worker_group.members.begin(),
      state.worker_group.members.end(),
      [&](const WorkerGroupMemberSpec& member) { return member.name == instance_name; });
  if (it == state.worker_group.members.end()) {
    return nullptr;
  }
  return &*it;
}

const WorkerGroupMemberSpec* FindLeaderWorkerGroupMember(const DesiredState& state) {
  const auto it = std::find_if(
      state.worker_group.members.begin(),
      state.worker_group.members.end(),
      [&](const WorkerGroupMemberSpec& member) { return member.leader; });
  if (it != state.worker_group.members.end()) {
    return &*it;
  }
  if (!state.worker_group.members.empty()) {
    return &state.worker_group.members.front();
  }
  return nullptr;
}

const WorkerGroupMemberSpec* FindReplicaLeaderWorkerGroupMember(
    const DesiredState& state,
    const WorkerGroupMemberSpec& member) {
  const auto it = std::find_if(
      state.worker_group.members.begin(),
      state.worker_group.members.end(),
      [&](const WorkerGroupMemberSpec& candidate) {
        return candidate.enabled &&
               candidate.replica_group_id == member.replica_group_id &&
               candidate.replica_leader;
      });
  if (it != state.worker_group.members.end()) {
    return &*it;
  }
  return FindLeaderWorkerGroupMember(state);
}

const WorkerGroupMemberSpec* FindHybridApiEndpointWorkerGroupMember(
    const DesiredState& state,
    const WorkerGroupMemberSpec& member) {
  const auto it = std::find_if(
      state.worker_group.members.begin(),
      state.worker_group.members.end(),
      [&](const WorkerGroupMemberSpec& candidate) {
        return candidate.enabled && candidate.data_parallel_api_endpoint &&
               candidate.node_name == member.node_name &&
               candidate.data_parallel_start_rank == member.data_parallel_start_rank;
      });
  if (it != state.worker_group.members.end()) {
    return &*it;
  }
  return FindReplicaLeaderWorkerGroupMember(state, member);
}

std::vector<const WorkerGroupMemberSpec*> FindHybridLocalWorkerGroupMembers(
    const DesiredState& state,
    const WorkerGroupMemberSpec& member) {
  std::vector<const WorkerGroupMemberSpec*> matches;
  if (!HybridDataParallelEnabled(state.inference)) {
    return matches;
  }
  for (const auto& candidate : state.worker_group.members) {
    if (!candidate.enabled) {
      continue;
    }
    if (candidate.node_name != member.node_name) {
      continue;
    }
    if (candidate.data_parallel_start_rank != member.data_parallel_start_rank) {
      continue;
    }
    matches.push_back(&candidate);
  }
  std::sort(
      matches.begin(),
      matches.end(),
      [](const WorkerGroupMemberSpec* lhs, const WorkerGroupMemberSpec* rhs) {
        if (lhs->data_parallel_rank != rhs->data_parallel_rank) {
          return lhs->data_parallel_rank < rhs->data_parallel_rank;
        }
        return lhs->name < rhs->name;
      });
  return matches;
}

std::vector<std::string> CollectHybridLocalGpuDevices(
    const DesiredState& state,
    const WorkerGroupMemberSpec& member) {
  std::vector<std::string> devices;
  for (const auto* local_member : FindHybridLocalWorkerGroupMembers(state, member)) {
    if (!local_member->gpu_device.empty()) {
      devices.push_back(local_member->gpu_device);
    }
  }
  return devices;
}

std::vector<std::string> CollectHybridLocalMemberNames(
    const DesiredState& state,
    const WorkerGroupMemberSpec& member) {
  std::vector<std::string> names;
  for (const auto* local_member : FindHybridLocalWorkerGroupMembers(state, member)) {
    names.push_back(local_member->name);
  }
  return names;
}

std::string JoinStrings(const std::vector<std::string>& values) {
  std::ostringstream out;
  for (std::size_t index = 0; index < values.size(); ++index) {
    if (index > 0) {
      out << ",";
    }
    out << values[index];
  }
  return out.str();
}

std::string BuildLocalGpuOrdinals(std::size_t count) {
  std::ostringstream out;
  for (std::size_t index = 0; index < count; ++index) {
    if (index > 0) {
      out << ",";
    }
    out << index;
  }
  return out.str();
}

bool ShouldRenderWorkerInstance(
    const DesiredState& state,
    const InstanceSpec& instance) {
  if (!UsesVllmRuntime(state) || instance.role != InstanceRole::Worker ||
      !HybridDataParallelEnabled(state.inference)) {
    return true;
  }
  const auto* worker_group_member = FindWorkerGroupMember(state, instance.name);
  if (worker_group_member == nullptr) {
    return true;
  }
  return worker_group_member->data_parallel_api_endpoint;
}

ComposeService BuildComposeService(
    const InstanceSpec& instance,
    const std::vector<DiskSpec>& disks,
    const std::vector<InstanceSpec>& node_instances,
    const DesiredState& state) {
  ComposeService service;
  service.name = instance.name;
  service.image = instance.image;
  service.command = instance.command;
  service.extra_hosts = DefaultComposeExtraHosts();
  const bool use_vllm = UsesVllmRuntime(state);
  const bool use_llama_rpc = UsesLlamaRpcRuntime(state);
  if (use_vllm && instance.role == InstanceRole::Worker &&
      service.image == "comet/worker-runtime:dev") {
    service.image = "comet/worker-vllm-runtime:dev";
  }
  for (const auto& dependency : instance.depends_on) {
    if (use_vllm && instance.role == InstanceRole::Worker) {
      continue;
    }
    const auto dependency_it = std::find_if(
        node_instances.begin(),
        node_instances.end(),
        [&](const InstanceSpec& candidate) { return candidate.name == dependency; });
    if (dependency_it != node_instances.end()) {
      service.depends_on.push_back(dependency);
    }
  }
  service.environment = instance.environment;
  service.environment["COMET_PLANE_NAME"] = state.plane_name;
  service.environment["COMET_PLANE_PROTECTED"] = state.protected_plane ? "1" : "0";
  if (use_vllm) {
    const std::string data_parallel_mode = CanonicalDataParallelMode(state.inference);
    if (instance.role == InstanceRole::Infer) {
      service.environment["COMET_INFER_RUNTIME_BACKEND"] = "worker-vllm";
      service.environment["COMET_DATA_PARALLEL_MODE"] = data_parallel_mode;
      service.environment["COMET_DATA_PARALLEL_LB_MODE"] =
          state.inference.data_parallel_lb_mode;
      service.environment["COMET_WORKER_GROUP_ID"] = state.worker_group.group_id;
      service.environment["COMET_WORKER_GROUP_EXPECTED_SIZE"] =
          std::to_string(std::max(0, state.worker_group.expected_workers));
      service.environment["COMET_DISTRIBUTED_BACKEND"] =
          state.worker_group.distributed_backend;
      service.environment["COMET_WORKER_SELECTION_POLICY"] =
          state.worker_group.worker_selection_policy;
      service.environment["COMET_RENDEZVOUS_HOST"] =
          state.worker_group.rendezvous_host.empty()
              ? state.worker_group.infer_instance_name
              : state.worker_group.rendezvous_host;
      service.environment["COMET_RENDEZVOUS_PORT"] =
          std::to_string(state.worker_group.rendezvous_port);
    } else if (instance.role == InstanceRole::Worker) {
      const auto* worker_group_member = FindWorkerGroupMember(state, instance.name);
      const auto* leader_worker_group_member =
          worker_group_member != nullptr
              ? FindReplicaLeaderWorkerGroupMember(state, *worker_group_member)
              : FindLeaderWorkerGroupMember(state);
      const auto* routable_worker_group_member =
          worker_group_member != nullptr && HybridDataParallelEnabled(state.inference)
              ? FindHybridApiEndpointWorkerGroupMember(state, *worker_group_member)
              : leader_worker_group_member;
      const std::vector<std::string> hybrid_local_gpu_devices =
          worker_group_member != nullptr && HybridDataParallelEnabled(state.inference)
              ? CollectHybridLocalGpuDevices(state, *worker_group_member)
              : std::vector<std::string>{};
      const int published_host_port = WorkerPublishedHostPort(state, instance);
      const int internal_runtime_port = WorkerInternalRuntimePort(state, instance);
      const bool worker_group_leader =
          worker_group_member != nullptr && worker_group_member->leader;
      const bool native_data_parallel = NativeDataParallelEnabled(state.inference);
      const int api_server_count =
          state.inference.api_server_count > 0
              ? state.inference.api_server_count
              : (HybridDataParallelEnabled(state.inference)
                     ? std::max(
                           1,
                           worker_group_member != nullptr
                               ? worker_group_member->data_parallel_size_local
                               : 1)
                     : 0);
      const int replica_world_size =
          worker_group_member != nullptr ? std::max(1, worker_group_member->replica_size)
                                         : std::max(1, state.worker_group.expected_workers);
      const bool data_parallel_api_endpoint =
          worker_group_member != nullptr
              ? worker_group_member->data_parallel_api_endpoint
              : worker_group_leader;
      const bool distributed_runtime =
          replica_world_size > 1;
      service.environment["COMET_WORKER_BOOT_MODE"] = "vllm-openai";
      service.environment["COMET_VLLM_PORT"] = std::to_string(state.inference.api_port);
      service.environment["COMET_VLLM_TENSOR_PARALLEL_SIZE"] =
          std::to_string(std::max(1, state.inference.tensor_parallel_size));
      service.environment["COMET_VLLM_PIPELINE_PARALLEL_SIZE"] =
          std::to_string(std::max(1, state.inference.pipeline_parallel_size));
      service.environment["COMET_VLLM_MAX_MODEL_LEN"] =
          std::to_string(std::max(1, state.inference.max_model_len));
      service.environment["COMET_VLLM_MAX_NUM_SEQS"] =
          std::to_string(std::max(1, state.inference.max_num_seqs));
      service.environment["COMET_VLLM_GPU_MEMORY_UTILIZATION"] =
          std::to_string(state.inference.gpu_memory_utilization);
      service.environment["COMET_VLLM_ENFORCE_EAGER"] =
          state.inference.enforce_eager ? "1" : "0";
      service.environment["COMET_VLLM_DOWNLOAD_DIR"] = state.inference.model_cache_dir;
      service.environment["COMET_DATA_PARALLEL_MODE"] = data_parallel_mode;
      service.environment["COMET_DATA_PARALLEL_LB_MODE"] =
          state.inference.data_parallel_lb_mode;
      service.environment["COMET_VLLM_API_SERVER_COUNT"] =
          std::to_string(std::max(0, api_server_count));
      service.environment["COMET_WORKER_GROUP_ID"] = state.worker_group.group_id;
      service.environment["COMET_WORKER_GROUP_WORLD_SIZE"] =
          std::to_string(replica_world_size);
      service.environment["COMET_DISTRIBUTED_BACKEND"] =
          state.worker_group.distributed_backend;
      service.environment["COMET_WORKER_SELECTION_POLICY"] =
          state.worker_group.worker_selection_policy;
      service.environment["COMET_RENDEZVOUS_HOST"] =
          state.worker_group.rendezvous_host.empty()
              ? state.worker_group.infer_instance_name
              : state.worker_group.rendezvous_host;
      service.environment["COMET_RENDEZVOUS_PORT"] =
          std::to_string(state.worker_group.rendezvous_port);
      service.environment["COMET_WORKER_ADVERTISED_BASE_URL"] =
          data_parallel_api_endpoint
              ? "http://" + instance.name + ":" + std::to_string(state.inference.api_port)
              : "";
      service.environment["VLLM_HOST_IP"] = instance.name;
      if (!native_data_parallel) {
        service.environment["VLLM_PORT"] = std::to_string(internal_runtime_port);
      }
      service.environment["COMET_VLLM_DISTRIBUTED_RUNTIME"] =
          distributed_runtime ? "1" : "0";
      service.environment["COMET_VLLM_DISTRIBUTED_EXECUTOR_BACKEND"] = "mp";
      service.environment["COMET_VLLM_DISTRIBUTED_MASTER_ADDR"] =
          leader_worker_group_member != nullptr ? leader_worker_group_member->name : instance.name;
      service.environment["COMET_VLLM_DISTRIBUTED_MASTER_PORT"] =
          std::to_string(state.worker_group.rendezvous_port);
      service.environment["COMET_VLLM_DISTRIBUTED_NNODES"] =
          std::to_string(replica_world_size);
      service.environment["COMET_VLLM_DISTRIBUTED_NODE_RANK"] = "0";
      service.environment["COMET_VLLM_HEADLESS"] = "0";
      service.environment["COMET_WORKER_LEADER_API_BASE_URL"] =
          "http://" +
          (routable_worker_group_member != nullptr ? routable_worker_group_member->name : instance.name) +
          ":" + std::to_string(state.inference.api_port);
      if (worker_group_member != nullptr) {
        service.environment["COMET_WORKER_GROUP_RANK"] =
            std::to_string(worker_group_member->rank);
        service.environment["COMET_WORKER_GROUP_LEADER"] =
            worker_group_member->leader ? "1" : "0";
        service.environment["COMET_WORKER_REPLICA_GROUP_ID"] =
            worker_group_member->replica_group_id;
        service.environment["COMET_WORKER_REPLICA_INDEX"] =
            std::to_string(worker_group_member->replica_index);
        service.environment["COMET_WORKER_REPLICA_SIZE"] =
            std::to_string(std::max(1, worker_group_member->replica_size));
        service.environment["COMET_WORKER_REPLICA_LEADER"] =
            worker_group_member->replica_leader ? "1" : "0";
        service.environment["COMET_VLLM_DATA_PARALLEL_SIZE"] =
            std::to_string(std::max(1, worker_group_member->data_parallel_size));
        service.environment["COMET_VLLM_DATA_PARALLEL_RANK"] =
            std::to_string(std::max(0, worker_group_member->data_parallel_rank));
        service.environment["COMET_VLLM_DATA_PARALLEL_SIZE_LOCAL"] =
            std::to_string(std::max(1, worker_group_member->data_parallel_size_local));
        service.environment["COMET_VLLM_DATA_PARALLEL_START_RANK"] =
            std::to_string(std::max(0, worker_group_member->data_parallel_start_rank));
        service.environment["COMET_VLLM_DATA_PARALLEL_ADDRESS"] =
            worker_group_member->data_parallel_head_address.empty()
                ? service.environment["COMET_RENDEZVOUS_HOST"]
                : worker_group_member->data_parallel_head_address;
        service.environment["COMET_VLLM_DATA_PARALLEL_RPC_PORT"] =
            std::to_string(
                worker_group_member->data_parallel_rpc_port > 0
                    ? worker_group_member->data_parallel_rpc_port
                    : state.worker_group.rendezvous_port + 100);
        service.environment["COMET_VLLM_DATA_PARALLEL_API_ENDPOINT"] =
            worker_group_member->data_parallel_api_endpoint ? "1" : "0";
        service.environment["COMET_VLLM_DATA_PARALLEL_EXTERNAL_LB"] =
            (native_data_parallel &&
             state.inference.data_parallel_lb_mode == kDataParallelLbModeExternal)
                ? "1"
                : "0";
        service.environment["COMET_VLLM_DATA_PARALLEL_HYBRID_LB"] =
            (native_data_parallel &&
             state.inference.data_parallel_lb_mode == kDataParallelLbModeHybrid)
                ? "1"
                : "0";
        service.environment["COMET_VLLM_DISTRIBUTED_NODE_RANK"] =
            std::to_string(std::max(0, worker_group_member->rank));
        service.environment["COMET_VLLM_HEADLESS"] =
            (distributed_runtime &&
             (!worker_group_member->leader ||
              (HybridDataParallelEnabled(state.inference) &&
               !worker_group_member->data_parallel_api_endpoint)))
                ? "1"
                : "0";
        if (HybridDataParallelEnabled(state.inference) && data_parallel_api_endpoint &&
            !hybrid_local_gpu_devices.empty()) {
          service.environment["COMET_HYBRID_LOCAL_MEMBER_NAMES"] =
              JoinStrings(CollectHybridLocalMemberNames(state, *worker_group_member));
          service.environment["COMET_LOCAL_GPU_ORDINALS"] =
              BuildLocalGpuOrdinals(hybrid_local_gpu_devices.size());
        }
      }
      if (state.bootstrap_model.has_value() &&
          state.bootstrap_model->served_model_name.has_value() &&
          !state.bootstrap_model->served_model_name->empty()) {
        service.environment["COMET_VLLM_SERVED_MODEL_NAME"] =
            *state.bootstrap_model->served_model_name;
      }
      if (state.bootstrap_model.has_value()) {
        if (state.bootstrap_model->local_path.has_value() &&
            !state.bootstrap_model->local_path->empty()) {
          service.environment["COMET_WORKER_MODEL_PATH"] =
              *state.bootstrap_model->local_path;
        } else if (!state.bootstrap_model->model_id.empty()) {
          service.environment["COMET_WORKER_MODEL_PATH"] =
              state.bootstrap_model->model_id;
        }
      }
      if (data_parallel_api_endpoint || !distributed_runtime) {
        AppendUniquePublishedPort(
            &service.published_ports,
            PublishedPort{"127.0.0.1", published_host_port, state.inference.api_port});
      }
      if (distributed_runtime) {
        service.shm_size = "1gb";
      }
    }
  } else if (use_llama_rpc && instance.role == InstanceRole::Worker) {
    const auto* worker_group_member = FindWorkerGroupMember(state, instance.name);
    const int rpc_port = WorkerRpcPort(worker_group_member);
    AppendUniquePublishedPort(
        &service.published_ports,
        PublishedPort{"0.0.0.0", rpc_port, rpc_port});
  }
  service.labels = instance.labels;
  const auto* worker_group_member =
      instance.role == InstanceRole::Worker ? FindWorkerGroupMember(state, instance.name) : nullptr;
  for (const auto& port : instance.published_ports) {
    if (IsManagedInferPublishedPort(state, instance, port) ||
        IsManagedLlamaRpcWorkerPublishedPort(state, instance, worker_group_member, port)) {
      continue;
    }
    AppendUniquePublishedPort(&service.published_ports, port);
  }
  service.gpu_device = instance.gpu_device;
  if (use_vllm && instance.role == InstanceRole::Worker &&
      HybridDataParallelEnabled(state.inference)) {
    if (const auto* worker_group_member = FindWorkerGroupMember(state, instance.name);
        worker_group_member != nullptr && worker_group_member->data_parallel_api_endpoint) {
      service.gpu_devices = CollectHybridLocalGpuDevices(state, *worker_group_member);
      if (!service.gpu_devices.empty()) {
        service.gpu_device = service.gpu_devices.front();
      }
    }
  }
  if (!service.gpu_device.has_value() && instance.role == InstanceRole::Infer && !use_vllm &&
      !use_llama_rpc) {
    const auto local_gpu_worker = std::find_if(
        node_instances.begin(),
        node_instances.end(),
        [&](const InstanceSpec& candidate) {
          return candidate.role == InstanceRole::Worker &&
                 candidate.gpu_device.has_value();
        });
    if (local_gpu_worker != node_instances.end()) {
      service.gpu_device = local_gpu_worker->gpu_device;
    }
  }
  if (service.gpu_device.has_value()) {
    service.use_nvidia_runtime = true;
    service.environment["NVIDIA_DRIVER_CAPABILITIES"] = "compute,utility";
    if (use_vllm && instance.role == InstanceRole::Worker) {
      // Docker device filtering scopes the container down to the selected GPUs.
      // Inside the container, vLLM should address those devices via local ordinals.
      service.environment["NVIDIA_VISIBLE_DEVICES"] = "all";
      const auto local_gpu_ordinals = service.environment.find("COMET_LOCAL_GPU_ORDINALS");
      service.environment["CUDA_VISIBLE_DEVICES"] =
          local_gpu_ordinals != service.environment.end() ? local_gpu_ordinals->second : "0";
    } else {
      service.environment["NVIDIA_VISIBLE_DEVICES"] = *service.gpu_device;
    }
    service.security_opts.push_back("apparmor=unconfined");
  } else if ((use_vllm || use_llama_rpc) && instance.role == InstanceRole::Infer) {
    // vLLM-backed and llama.cpp RPC-backed infer binaries can link against CUDA-enabled
    // llama.cpp objects even when the infer role itself does not own a GPU. Expose the
    // driver libraries without claiming a device on infer-only hosts.
    service.use_nvidia_runtime = true;
    service.environment["NVIDIA_VISIBLE_DEVICES"] = "none";
    service.environment["NVIDIA_DRIVER_CAPABILITIES"] = "compute,utility";
    service.security_opts.push_back("apparmor=unconfined");
  }
  service.healthcheck = instance.role == InstanceRole::Infer
                            ? "CMD-SHELL /runtime/infer/inferctl.sh probe-url "
                              "http://127.0.0.1:$${COMET_GATEWAY_PORT:-80}/health || "
                              "/runtime/infer/inferctl.sh probe-url "
                              "http://127.0.0.1:$${COMET_INFERENCE_PORT:-8000}/health"
                            : (instance.role == InstanceRole::App
                                   ? "CMD-SHELL curl -fsS http://127.0.0.1:$${PORT:-8080}/health >/dev/null"
                                   : (instance.role == InstanceRole::Skills
                                          ? "CMD-SHELL curl -fsS http://127.0.0.1:$${COMET_SKILLS_PORT:-18120}/health >/dev/null"
                                          : "CMD-SHELL test -f /tmp/comet-ready"));
  if (instance.role == InstanceRole::Infer) {
    const int infer_api_port = InferApiPort(state, instance);
    const int infer_gateway_port = InferGatewayPort(state, instance);
    AppendUniquePublishedPort(
        &service.published_ports,
        PublishedPort{"127.0.0.1", infer_api_port, infer_api_port});
    AppendUniquePublishedPort(
        &service.published_ports,
        PublishedPort{"127.0.0.1", infer_gateway_port, infer_gateway_port});
  }

  const auto& shared_disk =
      FindDiskByName(disks, instance.node_name, instance.shared_disk_name);
  const auto& private_disk =
      FindDiskByName(disks, instance.node_name, instance.private_disk_name);

  service.volumes.push_back(
      ComposeVolume{shared_disk.host_path, shared_disk.container_path, false});
  service.volumes.push_back(
      ComposeVolume{private_disk.host_path, private_disk.container_path, false});
  if (const auto direct_model_cache = BuildDirectModelCacheVolume(state); direct_model_cache.has_value()) {
    service.volumes.push_back(*direct_model_cache);
  }

  return service;
}

}  // namespace

std::vector<NodeComposePlan> BuildNodeComposePlans(const DesiredState& state) {
  std::vector<NodeComposePlan> plans;
  plans.reserve(state.nodes.size());

  for (const auto& node : state.nodes) {
    NodeComposePlan plan;
    plan.plane_name = state.plane_name;
    plan.node_name = node.name;
    std::vector<InstanceSpec> node_instances;

    for (const auto& disk : state.disks) {
      if (disk.node_name == node.name) {
        plan.disks.push_back(disk);
      }
    }

    for (const auto& instance : state.instances) {
      if (instance.node_name == node.name) {
        node_instances.push_back(instance);
      }
    }

    for (const auto& instance : node_instances) {
      if (!ShouldRenderWorkerInstance(state, instance)) {
        continue;
      }
      plan.services.push_back(BuildComposeService(instance, state.disks, node_instances, state));
    }

    plans.push_back(std::move(plan));
  }

  return plans;
}

std::optional<NodeComposePlan> FindNodeComposePlan(
    const DesiredState& state,
    const std::string& node_name) {
  const auto plans = BuildNodeComposePlans(state);
  const auto it = std::find_if(
      plans.begin(),
      plans.end(),
      [&](const NodeComposePlan& plan) { return plan.node_name == node_name; });
  if (it == plans.end()) {
    return std::nullopt;
  }
  return *it;
}

std::string ToString(InstanceRole role) {
  switch (role) {
    case InstanceRole::Infer:
      return "infer";
    case InstanceRole::Worker:
      return "worker";
    case InstanceRole::App:
      return "app";
    case InstanceRole::Skills:
      return "skills";
  }
  return "unknown";
}

std::string ToString(DiskKind kind) {
  switch (kind) {
    case DiskKind::PlaneShared:
      return "plane-shared";
    case DiskKind::InferPrivate:
      return "infer-private";
    case DiskKind::WorkerPrivate:
      return "worker-private";
    case DiskKind::AppPrivate:
      return "app-private";
    case DiskKind::SkillsPrivate:
      return "skills-private";
  }
  return "unknown";
}

std::string ToString(GpuShareMode mode) {
  switch (mode) {
    case GpuShareMode::Exclusive:
      return "exclusive";
    case GpuShareMode::Shared:
      return "shared";
    case GpuShareMode::BestEffort:
      return "best-effort";
  }
  return "unknown";
}

GpuShareMode ParseGpuShareMode(const std::string& value) {
  if (value == "exclusive") {
    return GpuShareMode::Exclusive;
  }
  if (value == "shared") {
    return GpuShareMode::Shared;
  }
  if (value == "best-effort") {
    return GpuShareMode::BestEffort;
  }
  throw std::runtime_error("unknown gpu share mode '" + value + "'");
}

std::string ToString(PlacementMode mode) {
  switch (mode) {
    case PlacementMode::Manual:
      return "manual";
    case PlacementMode::Auto:
      return "auto";
    case PlacementMode::Movable:
      return "movable";
  }
  return "unknown";
}

PlacementMode ParsePlacementMode(const std::string& value) {
  if (value == "manual") {
    return PlacementMode::Manual;
  }
  if (value == "auto") {
    return PlacementMode::Auto;
  }
  if (value == "movable") {
    return PlacementMode::Movable;
  }
  throw std::runtime_error("unknown placement mode '" + value + "'");
}

std::string ToString(PlaneMode mode) {
  switch (mode) {
    case PlaneMode::Compute:
      return "compute";
    case PlaneMode::Llm:
      return "llm";
  }
  return "compute";
}

PlaneMode ParsePlaneMode(const std::string& value) {
  if (value == "compute" || value.empty()) {
    return PlaneMode::Compute;
  }
  if (value == "llm") {
    return PlaneMode::Llm;
  }
  throw std::runtime_error("unknown plane mode '" + value + "'");
}

std::string ToString(HostExecutionMode mode) {
  switch (mode) {
    case HostExecutionMode::InferOnly:
      return "infer-only";
    case HostExecutionMode::WorkerOnly:
      return "worker-only";
    case HostExecutionMode::Mixed:
      return "mixed";
  }
  return "mixed";
}

HostExecutionMode ParseHostExecutionMode(const std::string& value) {
  if (value == "infer-only") {
    return HostExecutionMode::InferOnly;
  }
  if (value == "worker-only") {
    return HostExecutionMode::WorkerOnly;
  }
  if (value == "mixed" || value.empty()) {
    return HostExecutionMode::Mixed;
  }
  throw std::runtime_error("unknown host execution mode '" + value + "'");
}

}  // namespace comet
