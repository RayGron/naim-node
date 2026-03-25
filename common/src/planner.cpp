#include "comet/planner.h"

#include <algorithm>
#include <cstdint>
#include <stdexcept>

namespace comet {

namespace {

bool UsesVllmRuntime(const DesiredState& state) {
  return state.inference.runtime_engine == "vllm";
}

constexpr int kWorkerPublishedPortBase = 20000;
constexpr int kWorkerPublishedPortSpan = 20000;
constexpr int kWorkerInternalPortBase = 30000;
constexpr int kWorkerInternalPortSpan = 10000;

uint32_t StableWorkerPortHash(const std::string& value) {
  uint32_t hash = 2166136261u;
  for (unsigned char ch : value) {
    hash ^= static_cast<uint32_t>(ch);
    hash *= 16777619u;
  }
  return hash;
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

ComposeService BuildComposeService(
    const InstanceSpec& instance,
    const std::vector<DiskSpec>& disks,
    const std::vector<InstanceSpec>& node_instances,
    const DesiredState& state) {
  ComposeService service;
  service.name = instance.name;
  service.image = instance.image;
  service.command = instance.command;
  const bool use_vllm = UsesVllmRuntime(state);
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
  if (use_vllm) {
    if (instance.role == InstanceRole::Infer) {
      service.environment["COMET_INFER_RUNTIME_BACKEND"] = "worker-vllm";
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
      const auto* leader_worker_group_member = FindLeaderWorkerGroupMember(state);
      const int published_host_port = WorkerPublishedHostPort(state, instance);
      const int internal_runtime_port = WorkerInternalRuntimePort(state, instance);
      const bool worker_group_leader =
          worker_group_member != nullptr && worker_group_member->leader;
      const bool distributed_runtime =
          std::max(0, state.worker_group.expected_workers) > 1;
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
      service.environment["COMET_WORKER_GROUP_ID"] = state.worker_group.group_id;
      service.environment["COMET_WORKER_GROUP_WORLD_SIZE"] =
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
      service.environment["COMET_WORKER_ADVERTISED_BASE_URL"] =
          worker_group_leader
              ? "http://" + instance.name + ":" + std::to_string(state.inference.api_port)
              : "";
      service.environment["VLLM_HOST_IP"] = instance.name;
      service.environment["VLLM_PORT"] = std::to_string(internal_runtime_port);
      service.environment["COMET_VLLM_DISTRIBUTED_RUNTIME"] =
          distributed_runtime ? "1" : "0";
      service.environment["COMET_VLLM_DISTRIBUTED_EXECUTOR_BACKEND"] = "mp";
      service.environment["COMET_VLLM_DISTRIBUTED_MASTER_ADDR"] =
          leader_worker_group_member != nullptr ? leader_worker_group_member->name : instance.name;
      service.environment["COMET_VLLM_DISTRIBUTED_MASTER_PORT"] =
          std::to_string(state.worker_group.rendezvous_port);
      service.environment["COMET_VLLM_DISTRIBUTED_NNODES"] =
          std::to_string(std::max(1, state.worker_group.expected_workers));
      service.environment["COMET_VLLM_DISTRIBUTED_NODE_RANK"] = "0";
      service.environment["COMET_VLLM_HEADLESS"] = "0";
      service.environment["COMET_WORKER_LEADER_API_BASE_URL"] =
          "http://" +
          (leader_worker_group_member != nullptr ? leader_worker_group_member->name : instance.name) +
          ":" + std::to_string(state.inference.api_port);
      if (worker_group_member != nullptr) {
        service.environment["COMET_WORKER_GROUP_RANK"] =
            std::to_string(worker_group_member->rank);
        service.environment["COMET_WORKER_GROUP_LEADER"] =
            worker_group_member->leader ? "1" : "0";
        service.environment["COMET_VLLM_DISTRIBUTED_NODE_RANK"] =
            std::to_string(std::max(0, worker_group_member->rank));
        service.environment["COMET_VLLM_HEADLESS"] =
            distributed_runtime && !worker_group_member->leader ? "1" : "0";
      }
      if (state.bootstrap_model.has_value() &&
          state.bootstrap_model->served_model_name.has_value() &&
          !state.bootstrap_model->served_model_name->empty()) {
        service.environment["COMET_VLLM_SERVED_MODEL_NAME"] =
            *state.bootstrap_model->served_model_name;
      }
      if (worker_group_leader || !distributed_runtime) {
        service.published_ports.push_back(
            PublishedPort{"0.0.0.0", published_host_port, state.inference.api_port});
      }
      if (distributed_runtime) {
        service.shm_size = "1gb";
      }
    }
  }
  service.labels = instance.labels;
  service.gpu_device = instance.gpu_device;
  if (!service.gpu_device.has_value() && instance.role == InstanceRole::Infer) {
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
      // Docker device filtering already scopes the container to the selected GPU.
      // Inside the container, vLLM should address that device as local ordinal 0.
      service.environment["NVIDIA_VISIBLE_DEVICES"] = "all";
      service.environment["CUDA_VISIBLE_DEVICES"] = "0";
    } else {
      service.environment["NVIDIA_VISIBLE_DEVICES"] = *service.gpu_device;
    }
    service.security_opts.push_back("apparmor=unconfined");
  } else if (use_vllm && instance.role == InstanceRole::Infer) {
    // vLLM-backed infer still links against CUDA-enabled binaries, so expose driver
    // libraries without claiming a device on infer-only hosts.
    service.use_nvidia_runtime = true;
    service.environment["NVIDIA_VISIBLE_DEVICES"] = "none";
    service.environment["NVIDIA_DRIVER_CAPABILITIES"] = "compute,utility";
    service.security_opts.push_back("apparmor=unconfined");
  }
  service.healthcheck = instance.role == InstanceRole::Infer
                            ? "CMD-SHELL /runtime/infer/inferctl.sh probe-url "
                              "http://127.0.0.1:${COMET_INFERENCE_PORT:-8000}/health"
                            : "CMD-SHELL test -f /tmp/comet-ready";
  if (instance.role == InstanceRole::Infer) {
    service.published_ports.push_back(
        PublishedPort{"127.0.0.1", state.inference.api_port, state.inference.api_port});
    service.published_ports.push_back(
        PublishedPort{"127.0.0.1", state.gateway.listen_port, state.gateway.listen_port});
  }

  const auto& shared_disk =
      FindDiskByName(disks, instance.node_name, instance.shared_disk_name);
  const auto& private_disk =
      FindDiskByName(disks, instance.node_name, instance.private_disk_name);

  service.volumes.push_back(
      ComposeVolume{shared_disk.host_path, shared_disk.container_path, false});
  service.volumes.push_back(
      ComposeVolume{private_disk.host_path, private_disk.container_path, false});

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
