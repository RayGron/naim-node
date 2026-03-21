#include "comet/planner.h"

#include <algorithm>
#include <stdexcept>

namespace comet {

namespace {

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

ComposeService BuildComposeService(
    const InstanceSpec& instance,
    const std::vector<DiskSpec>& disks,
    const std::vector<InstanceSpec>& node_instances) {
  ComposeService service;
  service.name = instance.name;
  service.image = instance.image;
  service.command = instance.command;
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
    service.environment["NVIDIA_VISIBLE_DEVICES"] = *service.gpu_device;
    service.environment["NVIDIA_DRIVER_CAPABILITIES"] = "compute,utility";
  }
  service.healthcheck = instance.role == InstanceRole::Infer
                            ? "CMD-SHELL /runtime/infer/inferctl.sh probe-url "
                              "http://127.0.0.1:${COMET_INFERENCE_PORT:-8000}/health"
                            : "CMD-SHELL test -f /tmp/comet-ready";

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
      plan.services.push_back(BuildComposeService(instance, state.disks, node_instances));
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

}  // namespace comet
