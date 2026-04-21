#include "naim/state/demo_state.h"

#include <utility>

#include "naim/runtime/infer_runtime_config.h"

namespace naim {

namespace {

DiskSpec MakeDisk(
    std::string name,
    DiskKind kind,
    std::string plane_name,
    std::string owner_name,
    std::string node_name,
    std::string host_path,
    std::string container_path,
    int size_gb) {
  DiskSpec disk;
  disk.name = std::move(name);
  disk.kind = kind;
  disk.plane_name = std::move(plane_name);
  disk.owner_name = std::move(owner_name);
  disk.node_name = std::move(node_name);
  disk.host_path = std::move(host_path);
  disk.container_path = std::move(container_path);
  disk.size_gb = size_gb;
  return disk;
}

InstanceSpec MakeInfer() {
  InstanceSpec instance;
  instance.name = "infer-main";
  instance.role = InstanceRole::Infer;
  instance.plane_name = "alpha";
  instance.node_name = "node-a";
  instance.image = "naim/infer-runtime:dev";
  instance.command = "/runtime/bin/naim-inferctl container-boot";
  instance.private_disk_name = "infer-main-private";
  instance.shared_disk_name = "plane-alpha-shared";
  instance.environment = {
      {"NAIM_PLANE_NAME", "alpha"},
      {"NAIM_INSTANCE_NAME", "infer-main"},
      {"NAIM_INSTANCE_ROLE", "infer"},
      {"NAIM_NODE_NAME", "node-a"},
      {"NAIM_INFER_RUNTIME_BACKEND", "auto"},
      {"NAIM_CONTROLLER_URL", "http://controller.internal:8080"},
      {"NAIM_CONTROL_ROOT", "/naim/shared/control/alpha"},
      {"NAIM_INFER_RUNTIME_CONFIG",
       InferRuntimeConfigControlPath("/naim/shared/control/alpha", "infer-main")},
      {"NAIM_INFERENCE_PORT", "8000"},
      {"NAIM_GATEWAY_PORT", "8080"},
      {"NAIM_LLAMA_PORT", "8000"},
      {"NAIM_SHARED_DISK_PATH", "/naim/shared"},
      {"NAIM_PRIVATE_DISK_PATH", "/naim/private"},
  };
  instance.labels = {
      {"naim.plane", "alpha"},
      {"naim.role", "infer"},
      {"naim.node", "node-a"},
  };
  instance.private_disk_size_gb = 80;
  return instance;
}

InstanceSpec MakeWorker(
    std::string name,
    std::string node_name,
    std::string gpu_device,
    double gpu_fraction,
    GpuShareMode share_mode,
    int priority,
    bool preemptible,
    std::optional<int> memory_cap_mb,
    std::vector<std::string> depends_on) {
  InstanceSpec instance;
  instance.name = std::move(name);
  instance.role = InstanceRole::Worker;
  instance.plane_name = "alpha";
  instance.node_name = std::move(node_name);
  instance.image = "naim/worker-runtime:dev";
  instance.command = "/runtime/bin/naim-workerd";
  instance.private_disk_name = instance.name + "-private";
  instance.shared_disk_name = "plane-alpha-shared";
  instance.depends_on = std::move(depends_on);
  instance.environment = {
      {"NAIM_PLANE_NAME", "alpha"},
      {"NAIM_INSTANCE_NAME", instance.name},
      {"NAIM_INSTANCE_ROLE", "worker"},
      {"NAIM_NODE_NAME", instance.node_name},
      {"NAIM_GPU_DEVICE", gpu_device},
      {"NAIM_WORKER_BOOT_MODE", "llama-load"},
      {"NAIM_CONTROL_ROOT", "/naim/shared/control/alpha"},
      {"NAIM_SHARED_DISK_PATH", "/naim/shared"},
      {"NAIM_PRIVATE_DISK_PATH", "/naim/private"},
      {"NAIM_WORKER_RUNTIME_STATUS_PATH", "/naim/private/worker-runtime-status.json"},
  };
  instance.labels = {
      {"naim.plane", "alpha"},
      {"naim.role", "worker"},
      {"naim.node", instance.node_name},
  };
  instance.gpu_device = std::move(gpu_device);
  instance.placement_mode = PlacementMode::Manual;
  instance.share_mode = share_mode;
  instance.gpu_fraction = gpu_fraction;
  instance.priority = priority;
  instance.preemptible = preemptible;
  instance.memory_cap_mb = memory_cap_mb;
  instance.private_disk_size_gb = 40;
  return instance;
}

}  // namespace

DesiredState BuildDemoState() {
  DesiredState state;
  state.plane_name = "alpha";
  state.plane_shared_disk_name = "plane-alpha-shared";
  state.control_root = "/naim/shared/control/alpha";
  state.inference.primary_infer_node = "node-a";
  state.gateway.listen_host = "0.0.0.0";
  state.gateway.listen_port = 8080;
  state.gateway.server_name = "alpha.local";

  NodeInventory node_a;
  node_a.name = "node-a";
  node_a.platform = "linux";
  node_a.execution_mode = HostExecutionMode::Mixed;
  node_a.gpu_devices = {"0", "1"};
  node_a.gpu_memory_mb = {{"0", 24576}, {"1", 24576}};
  state.nodes.push_back(std::move(node_a));

  NodeInventory node_b;
  node_b.name = "node-b";
  node_b.platform = "linux";
  node_b.execution_mode = HostExecutionMode::Mixed;
  node_b.gpu_devices = {"0"};
  node_b.gpu_memory_mb = {{"0", 24576}};
  state.nodes.push_back(std::move(node_b));

  state.disks = {
      MakeDisk(
          "plane-alpha-shared",
          DiskKind::PlaneShared,
          "alpha",
          "alpha",
          "node-a",
          "/var/lib/naim/disks/planes/alpha/shared",
          "/naim/shared",
          200),
      MakeDisk(
          "infer-main-private",
          DiskKind::InferPrivate,
          "alpha",
          "infer-main",
          "node-a",
          "/var/lib/naim/disks/instances/infer-main/private",
          "/naim/private",
          80),
      MakeDisk(
          "worker-a-private",
          DiskKind::WorkerPrivate,
          "alpha",
          "worker-a",
          "node-a",
          "/var/lib/naim/disks/instances/worker-a/private",
          "/naim/private",
          40),
      MakeDisk(
          "worker-b-private",
          DiskKind::WorkerPrivate,
          "alpha",
          "worker-b",
          "node-b",
          "/var/lib/naim/disks/instances/worker-b/private",
          "/naim/private",
          40),
      MakeDisk(
          "plane-alpha-shared",
          DiskKind::PlaneShared,
          "alpha",
          "alpha",
          "node-b",
          "/var/lib/naim/disks/planes/alpha/shared",
          "/naim/shared",
          200),
  };

  state.instances = {
      MakeInfer(),
      MakeWorker(
          "worker-a",
          "node-a",
          "0",
          1.0,
          GpuShareMode::Exclusive,
          200,
          false,
          16384,
          {"infer-main"}),
      MakeWorker(
          "worker-b",
          "node-b",
          "0",
          0.5,
          GpuShareMode::Shared,
          100,
          true,
          8192,
          {}),
  };

  state.runtime_gpu_nodes = {
      RuntimeGpuNode{"worker-a", "node-a", "0", PlacementMode::Manual, GpuShareMode::Exclusive, 1.0, 200, false, 16384, true},
      RuntimeGpuNode{"worker-b", "node-b", "0", PlacementMode::Manual, GpuShareMode::Shared, 0.5, 100, true, 8192, true},
  };

  return state;
}

}  // namespace naim
