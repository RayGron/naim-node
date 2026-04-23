#include "naim/state/state_json_runtime_codecs.h"

#include <stdexcept>

namespace naim {

namespace {

using nlohmann::json;

}  // namespace

DiskKind StateJsonRuntimeCodecs::ParseDiskKind(const std::string& value) {
  if (value == "plane-shared") {
    return DiskKind::PlaneShared;
  }
  if (value == "infer-private") {
    return DiskKind::InferPrivate;
  }
  if (value == "worker-private") {
    return DiskKind::WorkerPrivate;
  }
  if (value == "app-private") {
    return DiskKind::AppPrivate;
  }
  if (value == "skills-private") {
    return DiskKind::SkillsPrivate;
  }
  if (value == "browsing-private") {
    return DiskKind::BrowsingPrivate;
  }
  if (value == "webgateway-private") {
    return DiskKind::BrowsingPrivate;
  }
  if (value == "interaction-private") {
    return DiskKind::InteractionPrivate;
  }
  throw std::runtime_error("unknown disk kind '" + value + "'");
}

InstanceRole StateJsonRuntimeCodecs::ParseInstanceRole(
    const std::string& value) {
  if (value == "infer") {
    return InstanceRole::Infer;
  }
  if (value == "worker") {
    return InstanceRole::Worker;
  }
  if (value == "app") {
    return InstanceRole::App;
  }
  if (value == "skills") {
    return InstanceRole::Skills;
  }
  if (value == "browsing") {
    return InstanceRole::Browsing;
  }
  if (value == "webgateway") {
    return InstanceRole::Browsing;
  }
  if (value == "interaction") {
    return InstanceRole::Interaction;
  }
  throw std::runtime_error("unknown instance role '" + value + "'");
}

json StateJsonRuntimeCodecs::ToJson(const PublishedPort& port) {
  return json{
      {"host_ip", port.host_ip},
      {"host_port", port.host_port},
      {"container_port", port.container_port},
  };
}

json StateJsonRuntimeCodecs::ToJson(const NodeInventory& node) {
  return json{
      {"name", node.name},
      {"platform", node.platform},
      {"execution_mode", naim::ToString(node.execution_mode)},
      {"gpu_memory_mb", node.gpu_memory_mb},
  };
}

json StateJsonRuntimeCodecs::ToJson(const WorkerGroupMemberSpec& member) {
  json result = {
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
      {"rpc_port", member.rpc_port},
      {"gpu_fraction", member.gpu_fraction},
      {"share_mode", naim::ToString(member.share_mode)},
      {"priority", member.priority},
      {"preemptible", member.preemptible},
      {"enabled", member.enabled},
      {"leader", member.leader},
  };
  if (member.memory_cap_mb.has_value()) {
    result["memory_cap_mb"] = *member.memory_cap_mb;
  }
  return result;
}

json StateJsonRuntimeCodecs::ToJson(const WorkerGroupSpec& group) {
  json result = {
      {"group_id", group.group_id},
      {"infer_instance_name", group.infer_instance_name},
      {"distributed_backend", group.distributed_backend},
      {"rendezvous_host", group.rendezvous_host},
      {"rendezvous_port", group.rendezvous_port},
      {"expected_workers", group.expected_workers},
      {"worker_selection_policy", group.worker_selection_policy},
      {"members", json::array()},
  };
  for (const auto& member : group.members) {
    result["members"].push_back(ToJson(member));
  }
  return result;
}

json StateJsonRuntimeCodecs::ToJson(const RuntimeGpuNode& gpu_node) {
  json result = json{
      {"name", gpu_node.name},
      {"node_name", gpu_node.node_name},
      {"gpu_device", gpu_node.gpu_device},
      {"placement_mode", naim::ToString(gpu_node.placement_mode)},
      {"share_mode", naim::ToString(gpu_node.share_mode)},
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

json StateJsonRuntimeCodecs::ToJson(const DiskSpec& disk) {
  return json{
      {"name", disk.name},
      {"kind", naim::ToString(disk.kind)},
      {"plane_name", disk.plane_name},
      {"owner_name", disk.owner_name},
      {"node_name", disk.node_name},
      {"host_path", disk.host_path},
      {"container_path", disk.container_path},
      {"size_gb", disk.size_gb},
  };
}

json StateJsonRuntimeCodecs::ToJson(const InstanceSpec& instance) {
  json result = json{
      {"name", instance.name},
      {"role", naim::ToString(instance.role)},
      {"plane_name", instance.plane_name},
      {"node_name", instance.node_name},
      {"image", instance.image},
      {"command", instance.command},
      {"private_disk_name", instance.private_disk_name},
      {"shared_disk_name", instance.shared_disk_name},
      {"depends_on", instance.depends_on},
      {"environment", instance.environment},
      {"labels", instance.labels},
      {"published_ports", json::array()},
      {"placement_mode", naim::ToString(instance.placement_mode)},
      {"share_mode", naim::ToString(instance.share_mode)},
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
  for (const auto& port : instance.published_ports) {
    result["published_ports"].push_back(ToJson(port));
  }
  return result;
}

PublishedPort StateJsonRuntimeCodecs::PublishedPortFromJson(
    const json& value) {
  PublishedPort port;
  port.host_ip = value.value("host_ip", port.host_ip);
  port.host_port = value.value("host_port", port.host_port);
  port.container_port = value.value("container_port", port.container_port);
  return port;
}

NodeInventory StateJsonRuntimeCodecs::NodeInventoryFromJson(
    const json& value) {
  NodeInventory node;
  node.name = value.at("name").get<std::string>();
  node.platform = value.at("platform").get<std::string>();
  node.execution_mode = ParseHostExecutionMode(
      value.value("execution_mode", std::string("mixed")));
  node.gpu_devices = value.value("gpu_devices", std::vector<std::string>{});
  node.gpu_memory_mb =
      value.value("gpu_memory_mb", std::map<std::string, int>{});
  if (node.gpu_devices.empty()) {
    for (const auto& [gpu_device, _] : node.gpu_memory_mb) {
      node.gpu_devices.push_back(gpu_device);
    }
  }
  return node;
}

WorkerGroupMemberSpec StateJsonRuntimeCodecs::WorkerGroupMemberSpecFromJson(
    const json& value) {
  WorkerGroupMemberSpec member;
  member.name = value.at("name").get<std::string>();
  member.infer_instance_name = value.value("infer_instance_name", std::string{});
  member.node_name = value.at("node_name").get<std::string>();
  member.gpu_device = value.value("gpu_device", std::string{});
  member.rank = value.value("rank", 0);
  member.replica_group_id = value.value("replica_group_id", std::string{});
  member.replica_index = value.value("replica_index", 0);
  member.replica_size = value.value("replica_size", 1);
  member.replica_leader = value.value("replica_leader", value.value("leader", false));
  member.data_parallel_rank = value.value("data_parallel_rank", member.replica_index);
  member.data_parallel_size = value.value("data_parallel_size", 1);
  member.data_parallel_size_local = value.value("data_parallel_size_local", 1);
  member.data_parallel_start_rank =
      value.value("data_parallel_start_rank", member.data_parallel_rank);
  member.data_parallel_api_endpoint =
      value.value("data_parallel_api_endpoint", member.replica_leader);
  member.data_parallel_head_address =
      value.value("data_parallel_head_address", std::string{});
  member.data_parallel_rpc_port = value.value("data_parallel_rpc_port", 0);
  member.rpc_port = value.value("rpc_port", member.data_parallel_rpc_port);
  member.gpu_fraction = value.value("gpu_fraction", 0.0);
  member.share_mode =
      ParseGpuShareMode(value.value("share_mode", std::string("exclusive")));
  member.priority = value.value("priority", 100);
  member.preemptible = value.value("preemptible", false);
  member.enabled = value.value("enabled", true);
  member.leader = value.value("leader", false);
  if (value.contains("memory_cap_mb") && !value.at("memory_cap_mb").is_null()) {
    member.memory_cap_mb = value.at("memory_cap_mb").get<int>();
  }
  return member;
}

WorkerGroupSpec StateJsonRuntimeCodecs::WorkerGroupSpecFromJson(
    const json& value) {
  WorkerGroupSpec group;
  group.group_id = value.value("group_id", std::string{});
  group.infer_instance_name = value.value("infer_instance_name", std::string{});
  group.distributed_backend =
      value.value("distributed_backend", std::string("llama_rpc"));
  group.rendezvous_host = value.value("rendezvous_host", std::string{});
  group.rendezvous_port = value.value("rendezvous_port", 29500);
  group.expected_workers = value.value("expected_workers", 0);
  group.worker_selection_policy =
      value.value("worker_selection_policy", std::string("prefer-free-then-share"));
  for (const auto& member : value.value("members", json::array())) {
    if (member.is_object()) {
      group.members.push_back(WorkerGroupMemberSpecFromJson(member));
    }
  }
  return group;
}

RuntimeGpuNode StateJsonRuntimeCodecs::RuntimeGpuNodeFromJson(
    const json& value) {
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

DiskSpec StateJsonRuntimeCodecs::DiskSpecFromJson(const json& value) {
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

InstanceSpec StateJsonRuntimeCodecs::InstanceSpecFromJson(
    const json& value) {
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
  instance.environment =
      value.value("environment", std::map<std::string, std::string>{});
  instance.labels = value.value("labels", std::map<std::string, std::string>{});
  if (value.contains("published_ports") && value.at("published_ports").is_array()) {
    for (const auto& port : value.at("published_ports")) {
      if (port.is_object()) {
        instance.published_ports.push_back(PublishedPortFromJson(port));
      }
    }
  }
  if (value.contains("gpu_device") && !value.at("gpu_device").is_null()) {
    instance.gpu_device = value.at("gpu_device").get<std::string>();
  }
  instance.placement_mode =
      value.contains("placement_mode")
          ? ParsePlacementMode(value.at("placement_mode").get<std::string>())
          : (instance.role == InstanceRole::Worker &&
                     (!instance.gpu_device.has_value() || instance.gpu_device->empty())
                 ? PlacementMode::Auto
                 : PlacementMode::Manual);
  instance.share_mode =
      ParseGpuShareMode(value.value("share_mode", std::string("exclusive")));
  instance.gpu_fraction =
      value.value("gpu_fraction", instance.role == InstanceRole::Worker ? 1.0 : 0.0);
  instance.priority = value.value("priority", 100);
  instance.preemptible = value.value("preemptible", false);
  if (value.contains("memory_cap_mb") && !value.at("memory_cap_mb").is_null()) {
    instance.memory_cap_mb = value.at("memory_cap_mb").get<int>();
  }
  instance.private_disk_size_gb = value.value("private_disk_size_gb", 0);
  return instance;
}

}  // namespace naim
