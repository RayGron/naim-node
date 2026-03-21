#include "comet/state_json.h"

#include <filesystem>
#include <fstream>
#include <stdexcept>

#include <nlohmann/json.hpp>

namespace comet {

namespace {

using nlohmann::json;

DiskKind ParseDiskKind(const std::string& value) {
  if (value == "plane-shared") {
    return DiskKind::PlaneShared;
  }
  if (value == "infer-private") {
    return DiskKind::InferPrivate;
  }
  if (value == "worker-private") {
    return DiskKind::WorkerPrivate;
  }
  throw std::runtime_error("unknown disk kind '" + value + "'");
}

InstanceRole ParseInstanceRole(const std::string& value) {
  if (value == "infer") {
    return InstanceRole::Infer;
  }
  if (value == "worker") {
    return InstanceRole::Worker;
  }
  throw std::runtime_error("unknown instance role '" + value + "'");
}

json ToJson(const NodeInventory& node) {
  return json{
      {"name", node.name},
      {"platform", node.platform},
      {"gpu_devices", node.gpu_devices},
      {"gpu_memory_mb", node.gpu_memory_mb},
  };
}

json ToJson(const RuntimeGpuNode& gpu_node) {
  json result = json{
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
    result["memory_cap_mb"] = *gpu_node.memory_cap_mb;
  }
  return result;
}

json ToJson(const DiskSpec& disk) {
  return json{
      {"name", disk.name},
      {"kind", ToString(disk.kind)},
      {"plane_name", disk.plane_name},
      {"owner_name", disk.owner_name},
      {"node_name", disk.node_name},
      {"host_path", disk.host_path},
      {"container_path", disk.container_path},
      {"size_gb", disk.size_gb},
  };
}

json ToJson(const InstanceSpec& instance) {
  json result = json{
      {"name", instance.name},
      {"role", ToString(instance.role)},
      {"plane_name", instance.plane_name},
      {"node_name", instance.node_name},
      {"image", instance.image},
      {"command", instance.command},
      {"private_disk_name", instance.private_disk_name},
      {"shared_disk_name", instance.shared_disk_name},
      {"depends_on", instance.depends_on},
      {"environment", instance.environment},
      {"labels", instance.labels},
      {"placement_mode", ToString(instance.placement_mode)},
      {"share_mode", ToString(instance.share_mode)},
      {"gpu_fraction", instance.gpu_fraction},
      {"priority", instance.priority},
      {"preemptible", instance.preemptible},
      {"private_disk_size_gb", instance.private_disk_size_gb},
  };
  if (instance.gpu_device.has_value()) {
    result["gpu_device"] = *instance.gpu_device;
  }
  if (instance.memory_cap_mb.has_value()) {
    result["memory_cap_mb"] = *instance.memory_cap_mb;
  }
  return result;
}

NodeInventory NodeInventoryFromJson(const json& value) {
  NodeInventory node;
  node.name = value.at("name").get<std::string>();
  node.platform = value.at("platform").get<std::string>();
  node.gpu_devices = value.value("gpu_devices", std::vector<std::string>{});
  node.gpu_memory_mb = value.value("gpu_memory_mb", std::map<std::string, int>{});
  return node;
}

RuntimeGpuNode RuntimeGpuNodeFromJson(const json& value) {
  RuntimeGpuNode gpu_node;
  gpu_node.name = value.at("name").get<std::string>();
  gpu_node.node_name = value.at("node_name").get<std::string>();
  gpu_node.gpu_device = value.value("gpu_device", std::string{});
  gpu_node.placement_mode =
      ParsePlacementMode(value.value("placement_mode", std::string("manual")));
  gpu_node.share_mode =
      ParseGpuShareMode(value.value("share_mode", std::string("exclusive")));
  gpu_node.gpu_fraction = value.value("gpu_fraction", 0.0);
  gpu_node.priority = value.value("priority", 100);
  gpu_node.preemptible = value.value("preemptible", false);
  if (value.contains("memory_cap_mb") && !value.at("memory_cap_mb").is_null()) {
    gpu_node.memory_cap_mb = value.at("memory_cap_mb").get<int>();
  }
  gpu_node.enabled = value.value("enabled", true);
  return gpu_node;
}

DiskSpec DiskSpecFromJson(const json& value) {
  DiskSpec disk;
  disk.name = value.at("name").get<std::string>();
  disk.kind = ParseDiskKind(value.at("kind").get<std::string>());
  disk.plane_name = value.at("plane_name").get<std::string>();
  disk.owner_name = value.at("owner_name").get<std::string>();
  disk.node_name = value.at("node_name").get<std::string>();
  disk.host_path = value.at("host_path").get<std::string>();
  disk.container_path = value.at("container_path").get<std::string>();
  disk.size_gb = value.at("size_gb").get<int>();
  return disk;
}

InstanceSpec InstanceSpecFromJson(const json& value) {
  InstanceSpec instance;
  instance.name = value.at("name").get<std::string>();
  instance.role = ParseInstanceRole(value.at("role").get<std::string>());
  instance.plane_name = value.at("plane_name").get<std::string>();
  instance.node_name = value.at("node_name").get<std::string>();
  instance.image = value.at("image").get<std::string>();
  instance.command = value.at("command").get<std::string>();
  instance.private_disk_name = value.at("private_disk_name").get<std::string>();
  instance.shared_disk_name = value.at("shared_disk_name").get<std::string>();
  instance.depends_on = value.value("depends_on", std::vector<std::string>{});
  instance.environment = value.value("environment", std::map<std::string, std::string>{});
  instance.labels = value.value("labels", std::map<std::string, std::string>{});
  if (value.contains("gpu_device") && !value.at("gpu_device").is_null()) {
    instance.gpu_device = value.at("gpu_device").get<std::string>();
  }
  instance.placement_mode =
      ParsePlacementMode(value.value("placement_mode", std::string("manual")));
  instance.share_mode =
      ParseGpuShareMode(value.value("share_mode", std::string("exclusive")));
  instance.gpu_fraction = value.value("gpu_fraction", 0.0);
  instance.priority = value.value("priority", 100);
  instance.preemptible = value.value("preemptible", false);
  if (value.contains("memory_cap_mb") && !value.at("memory_cap_mb").is_null()) {
    instance.memory_cap_mb = value.at("memory_cap_mb").get<int>();
  }
  instance.private_disk_size_gb = value.value("private_disk_size_gb", 0);
  return instance;
}

json DesiredStateToJson(const DesiredState& state) {
  json result = {
      {"plane_name", state.plane_name},
      {"plane_shared_disk_name", state.plane_shared_disk_name},
      {"control_root", state.control_root},
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
      {"runtime_gpu_nodes", json::array()},
      {"nodes", json::array()},
      {"disks", json::array()},
      {"instances", json::array()},
  };

  for (const auto& gpu_node : state.runtime_gpu_nodes) {
    result["runtime_gpu_nodes"].push_back(ToJson(gpu_node));
  }
  for (const auto& node : state.nodes) {
    result["nodes"].push_back(ToJson(node));
  }
  for (const auto& disk : state.disks) {
    result["disks"].push_back(ToJson(disk));
  }
  for (const auto& instance : state.instances) {
    result["instances"].push_back(ToJson(instance));
  }

  return result;
}

DesiredState DesiredStateFromJson(const json& value) {
  DesiredState state;
  state.plane_name = value.at("plane_name").get<std::string>();
  state.plane_shared_disk_name = value.at("plane_shared_disk_name").get<std::string>();
  state.control_root =
      value.value("control_root", "/comet/shared/control/" + state.plane_name);

  if (value.contains("inference") && value.at("inference").is_object()) {
    const auto& inference = value.at("inference");
    state.inference.primary_infer_node =
        inference.value("primary_infer_node", state.inference.primary_infer_node);
    state.inference.net_if = inference.value("net_if", state.inference.net_if);
    state.inference.models_root =
        inference.value("models_root", state.inference.models_root);
    state.inference.gguf_cache_dir =
        inference.value("gguf_cache_dir", state.inference.gguf_cache_dir);
    state.inference.infer_log_dir =
        inference.value("infer_log_dir", state.inference.infer_log_dir);
    state.inference.llama_port =
        inference.value("llama_port", state.inference.llama_port);
    state.inference.llama_ctx_size =
        inference.value("llama_ctx_size", state.inference.llama_ctx_size);
    state.inference.llama_threads =
        inference.value("llama_threads", state.inference.llama_threads);
    state.inference.llama_gpu_layers =
        inference.value("llama_gpu_layers", state.inference.llama_gpu_layers);
    state.inference.inference_healthcheck_retries = inference.value(
        "inference_healthcheck_retries",
        state.inference.inference_healthcheck_retries);
    state.inference.inference_healthcheck_interval_sec = inference.value(
        "inference_healthcheck_interval_sec",
        state.inference.inference_healthcheck_interval_sec);
  }

  if (value.contains("gateway") && value.at("gateway").is_object()) {
    const auto& gateway = value.at("gateway");
    state.gateway.listen_host =
        gateway.value("listen_host", state.gateway.listen_host);
    state.gateway.listen_port =
        gateway.value("listen_port", state.gateway.listen_port);
    state.gateway.server_name =
        gateway.value("server_name", state.gateway.server_name);
  }

  for (const auto& gpu_node : value.value("runtime_gpu_nodes", json::array())) {
    state.runtime_gpu_nodes.push_back(RuntimeGpuNodeFromJson(gpu_node));
  }
  for (const auto& node : value.value("nodes", json::array())) {
    state.nodes.push_back(NodeInventoryFromJson(node));
  }
  for (const auto& disk : value.value("disks", json::array())) {
    state.disks.push_back(DiskSpecFromJson(disk));
  }
  for (const auto& instance : value.value("instances", json::array())) {
    state.instances.push_back(InstanceSpecFromJson(instance));
  }

  return state;
}

}  // namespace

DesiredState SliceDesiredStateForNode(
    const DesiredState& state,
    const std::string& node_name) {
  DesiredState result;
  result.plane_name = state.plane_name;
  result.plane_shared_disk_name = state.plane_shared_disk_name;
  result.control_root = state.control_root;
  result.inference = state.inference;
  result.gateway = state.gateway;
  result.runtime_gpu_nodes = state.runtime_gpu_nodes;

  for (const auto& node : state.nodes) {
    if (node.name == node_name) {
      result.nodes.push_back(node);
    }
  }
  for (const auto& disk : state.disks) {
    if (disk.node_name == node_name) {
      result.disks.push_back(disk);
    }
  }
  for (const auto& instance : state.instances) {
    if (instance.node_name == node_name) {
      result.instances.push_back(instance);
    }
  }

  if (result.nodes.empty()) {
    throw std::runtime_error("node '" + node_name + "' not found in desired state");
  }

  return result;
}

std::string SerializeDesiredStateJson(const DesiredState& state) {
  return DesiredStateToJson(state).dump(2);
}

DesiredState DeserializeDesiredStateJson(const std::string& json_text) {
  return DesiredStateFromJson(json::parse(json_text));
}

std::optional<DesiredState> LoadDesiredStateJson(const std::string& path) {
  if (!std::filesystem::exists(path)) {
    return std::nullopt;
  }

  std::ifstream input(path);
  if (!input.is_open()) {
    throw std::runtime_error("failed to open desired state file: " + path);
  }

  json value;
  input >> value;
  return DesiredStateFromJson(value);
}

void SaveDesiredStateJson(const DesiredState& state, const std::string& path) {
  const std::filesystem::path file_path(path);
  if (file_path.has_parent_path()) {
    std::filesystem::create_directories(file_path.parent_path());
  }

  std::ofstream output(path, std::ios::binary | std::ios::trunc);
  if (!output.is_open()) {
    throw std::runtime_error("failed to open desired state file for write: " + path);
  }

  output << SerializeDesiredStateJson(state) << "\n";
  if (!output.good()) {
    throw std::runtime_error("failed to write desired state file: " + path);
  }
}

}  // namespace comet
