#include "naim/planning/planner.h"

#include <algorithm>
#include <cstdlib>
#include <cstdint>
#include <sstream>
#include <stdexcept>

#include "naim/state/worker_group_topology.h"

namespace naim {

namespace {

bool UsesLlamaRpcRuntime(const DesiredState& state) {
  return state.inference.runtime_engine == "llama.cpp" &&
         state.inference.distributed_backend == "llama_rpc";
}

std::optional<ComposeVolume> BuildDirectModelCacheVolume(
    const DesiredState& state,
    const InstanceSpec& instance) {
  if (!UsesLlamaRpcRuntime(state) ||
      instance.role == InstanceRole::Browsing ||
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

std::vector<std::string> DefaultComposeExtraHosts() {
  const char* internal_host = std::getenv("NAIM_CONTROLLER_INTERNAL_HOST");
  const std::string controller_alias =
      internal_host != nullptr && *internal_host != '\0'
          ? "controller.internal:" + std::string(internal_host)
          : "controller.internal:host-gateway";
  return {
      "host.docker.internal:host-gateway",
      controller_alias,
  };
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
  return IntEnvValue(instance.environment, "NAIM_INFERENCE_PORT").value_or(state.inference.api_port);
}

int InferGatewayPort(const DesiredState& state, const InstanceSpec& instance) {
  return IntEnvValue(instance.environment, "NAIM_GATEWAY_PORT").value_or(state.gateway.listen_port);
}

int WorkerRpcPort(const WorkerGroupMemberSpec* worker_group_member) {
  return worker_group_member != nullptr && worker_group_member->rpc_port > 0
             ? worker_group_member->rpc_port
             : 50052;
}

int ManagedLlamaRpcWorkerPort(
    const DesiredState& state,
    const InstanceSpec& instance,
    const WorkerGroupMemberSpec* worker_group_member) {
  if (UsesLlamaRpcRuntime(state) && instance.role == InstanceRole::Worker) {
    return StableLlamaRpcWorkerPort(state.plane_name, instance.name);
  }
  return WorkerRpcPort(worker_group_member);
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
  return port.host_port == ManagedLlamaRpcWorkerPort(state, instance, worker_group_member) ||
         port.host_port == 50052;
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

std::vector<std::string> LocalWorkerGpuDevices(const std::vector<InstanceSpec>& node_instances) {
  std::vector<std::string> gpu_devices;
  for (const auto& candidate : node_instances) {
    if (candidate.role != InstanceRole::Worker || !candidate.gpu_device.has_value() ||
        candidate.gpu_device->empty()) {
      continue;
    }
    if (std::find(gpu_devices.begin(), gpu_devices.end(), *candidate.gpu_device) !=
        gpu_devices.end()) {
      continue;
    }
    gpu_devices.push_back(*candidate.gpu_device);
  }
  return gpu_devices;
}

std::string JoinStrings(const std::vector<std::string>& values, std::string_view delimiter) {
  std::ostringstream joined;
  for (std::size_t index = 0; index < values.size(); ++index) {
    if (index > 0) {
      joined << delimiter;
    }
    joined << values[index];
  }
  return joined.str();
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
  const bool use_llama_rpc = UsesLlamaRpcRuntime(state);
  for (const auto& dependency : instance.depends_on) {
    const auto dependency_it = std::find_if(
        node_instances.begin(),
        node_instances.end(),
        [&](const InstanceSpec& candidate) { return candidate.name == dependency; });
    if (dependency_it != node_instances.end()) {
      service.depends_on.push_back(dependency);
    }
  }
  service.environment = instance.environment;
  service.environment["NAIM_PLANE_NAME"] = state.plane_name;
  service.environment["NAIM_PLANE_PROTECTED"] = state.protected_plane ? "1" : "0";
  if (use_llama_rpc && instance.role == InstanceRole::Worker) {
    const auto* worker_group_member = FindWorkerGroupMember(state, instance.name);
    const int rpc_port = ManagedLlamaRpcWorkerPort(state, instance, worker_group_member);
    service.environment["NAIM_WORKER_RPC_PORT"] = std::to_string(rpc_port);
    service.environment["NAIM_WORKER_RPC_HOST"] = "0.0.0.0";
    service.environment["NAIM_WORKER_RPC_ENDPOINT"] =
        instance.name + ":" + std::to_string(rpc_port);
    AppendUniquePublishedPort(
        &service.published_ports,
        PublishedPort{"0.0.0.0", rpc_port, rpc_port});
  }
  if (instance.role == InstanceRole::Browsing) {
    service.privileged = true;
    service.security_opts.push_back("no-new-privileges:true");
    service.security_opts.push_back("apparmor=unconfined");
    service.security_opts.push_back("seccomp=unconfined");
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
  if (use_llama_rpc && instance.role == InstanceRole::Infer) {
    service.gpu_devices = LocalWorkerGpuDevices(node_instances);
    if (!service.gpu_device.has_value() && !service.gpu_devices.empty()) {
      service.gpu_device = service.gpu_devices.front();
    }
  }
  if (!service.gpu_device.has_value() && instance.role == InstanceRole::Infer &&
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
    service.environment["NVIDIA_VISIBLE_DEVICES"] =
        service.gpu_devices.empty() ? *service.gpu_device
                                    : JoinStrings(service.gpu_devices, ",");
    service.security_opts.push_back("apparmor=unconfined");
  } else if (use_llama_rpc && instance.role == InstanceRole::Infer) {
    service.use_nvidia_runtime = true;
    service.environment["NVIDIA_VISIBLE_DEVICES"] = "none";
    service.environment["NVIDIA_DRIVER_CAPABILITIES"] = "compute,utility";
    service.security_opts.push_back("apparmor=unconfined");
  }
  service.healthcheck = instance.role == InstanceRole::Infer
                            ? "CMD-SHELL /runtime/infer/inferctl.sh probe-url "
                              "http://127.0.0.1:$${NAIM_GATEWAY_PORT:-80}/health || "
                              "/runtime/infer/inferctl.sh probe-url "
                              "http://127.0.0.1:$${NAIM_INFERENCE_PORT:-8000}/health"
                            : (instance.role == InstanceRole::App
                                   ? "CMD-SHELL curl -fsS http://127.0.0.1:$${PORT:-8080}/health >/dev/null"
                                   : (instance.role == InstanceRole::Skills
                                          ? "CMD-SHELL test -f /tmp/naim-ready"
                                          : "CMD-SHELL test -f /tmp/naim-ready"));
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

  if (!instance.shared_disk_name.empty()) {
    const auto& shared_disk =
        FindDiskByName(disks, instance.node_name, instance.shared_disk_name);
    service.volumes.push_back(
        ComposeVolume{shared_disk.host_path, shared_disk.container_path, false});
  }
  if (!instance.private_disk_name.empty()) {
    const auto& private_disk =
        FindDiskByName(disks, instance.node_name, instance.private_disk_name);
    service.volumes.push_back(
        ComposeVolume{private_disk.host_path, private_disk.container_path, false});
  }
  if (const auto direct_model_cache = BuildDirectModelCacheVolume(state, instance);
      direct_model_cache.has_value()) {
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
    case InstanceRole::Browsing:
      return "webgateway";
    case InstanceRole::Interaction:
      return "interaction";
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
    case DiskKind::BrowsingPrivate:
      return "webgateway-private";
    case DiskKind::InteractionPrivate:
      return "interaction-private";
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

}  // namespace naim
