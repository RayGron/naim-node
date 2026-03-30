#include "comet/state/state_json.h"

#include <cstring>
#include <filesystem>
#include <fstream>
#include <set>
#include <stdexcept>

#include <nlohmann/json.hpp>

#include "comet/state/desired_state_v2_renderer.h"
#include "comet/state/desired_state_v2_projector.h"
#include "comet/state/worker_group_topology.h"

namespace comet {

namespace {

using nlohmann::json;

void NormalizeInferenceSettings(InferenceRuntimeSettings* settings);

bool IsDesiredStateV2(const json& value) {
  return value.is_object() && value.value("version", 0) == 2;
}


void NormalizeInferenceSettings(InferenceRuntimeSettings* settings) {
  if (settings == nullptr) {
    return;
  }
  if (settings->worker_group_id.empty()) {
    settings->worker_group_id = "default-worker-group";
  }
  if (settings->distributed_backend.empty()) {
    settings->distributed_backend =
        settings->runtime_engine == "llama.cpp" ? "llama_rpc" : "local";
  }
  if (settings->data_parallel_mode.empty()) {
    settings->data_parallel_mode = kDataParallelModeOff;
  }
  if (settings->data_parallel_mode == kDataParallelModeAutoReplicas) {
    settings->data_parallel_mode = kDataParallelModeVllmNative;
  }
  if (settings->data_parallel_lb_mode.empty()) {
    settings->data_parallel_lb_mode = kDataParallelLbModeExternal;
  }
  if (settings->worker_selection_policy.empty()) {
    settings->worker_selection_policy = "prefer-free-then-share";
  }
  if (settings->model_cache_dir.empty()) {
    settings->model_cache_dir = settings->gguf_cache_dir;
  }
  if (settings->gguf_cache_dir.empty()) {
    settings->gguf_cache_dir = settings->model_cache_dir;
  }
  if (settings->runtime_log_dir.empty()) {
    settings->runtime_log_dir = settings->infer_log_dir;
  }
  if (settings->infer_log_dir.empty()) {
    settings->infer_log_dir = settings->runtime_log_dir;
  }
  if (settings->api_port <= 0) {
    settings->api_port = settings->llama_port;
  }
  if (settings->llama_port <= 0) {
    settings->llama_port = settings->api_port;
  }
  if (settings->max_model_len <= 0) {
    settings->max_model_len = settings->llama_ctx_size;
  }
  if (settings->llama_ctx_size <= 0) {
    settings->llama_ctx_size = settings->max_model_len;
  }
}

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
  if (value == "app-private") {
    return DiskKind::AppPrivate;
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
  if (value == "app") {
    return InstanceRole::App;
  }
  throw std::runtime_error("unknown instance role '" + value + "'");
}

json ToJson(const PublishedPort& port) {
  return json{
      {"host_ip", port.host_ip},
      {"host_port", port.host_port},
      {"container_port", port.container_port},
  };
}

json ToJson(const NodeInventory& node) {
  return json{
      {"name", node.name},
      {"platform", node.platform},
      {"execution_mode", ToString(node.execution_mode)},
      {"gpu_memory_mb", node.gpu_memory_mb},
  };
}

json ToJson(const WorkerGroupMemberSpec& member) {
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
      {"share_mode", ToString(member.share_mode)},
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

json ToJson(const WorkerGroupSpec& group) {
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

json ToJson(const BootstrapModelSpec& bootstrap_model) {
  json result = {
      {"model_id", bootstrap_model.model_id},
      {"materialization_mode", bootstrap_model.materialization_mode},
  };
  if (bootstrap_model.served_model_name.has_value()) {
    result["served_model_name"] = *bootstrap_model.served_model_name;
  }
  if (bootstrap_model.local_path.has_value()) {
    result["local_path"] = *bootstrap_model.local_path;
  }
  if (bootstrap_model.source_url.has_value()) {
    result["source_url"] = *bootstrap_model.source_url;
  }
  if (!bootstrap_model.source_urls.empty()) {
    result["source_urls"] = bootstrap_model.source_urls;
  }
  if (bootstrap_model.target_filename.has_value()) {
    result["target_filename"] = *bootstrap_model.target_filename;
  }
  if (bootstrap_model.sha256.has_value()) {
    result["sha256"] = *bootstrap_model.sha256;
  }
  return result;
}

json ToJson(const InteractionSettings& interaction) {
  json result = {
      {"default_response_language", interaction.default_response_language},
      {"supported_response_languages", interaction.supported_response_languages},
      {"follow_user_language", interaction.follow_user_language},
  };
  if (interaction.system_prompt.has_value()) {
    result["system_prompt"] = *interaction.system_prompt;
  }
  if (interaction.analysis_system_prompt.has_value()) {
    result["analysis_system_prompt"] = *interaction.analysis_system_prompt;
  }
  if (interaction.completion_policy.has_value()) {
    json completion_policy = {
        {"response_mode", interaction.completion_policy->response_mode},
        {"max_tokens", interaction.completion_policy->max_tokens},
        {"max_continuations", interaction.completion_policy->max_continuations},
        {"max_total_completion_tokens",
         interaction.completion_policy->max_total_completion_tokens},
        {"max_elapsed_time_ms", interaction.completion_policy->max_elapsed_time_ms},
    };
    if (interaction.completion_policy->target_completion_tokens.has_value()) {
      completion_policy["target_completion_tokens"] =
          *interaction.completion_policy->target_completion_tokens;
    }
    if (interaction.completion_policy->semantic_goal.has_value()) {
      completion_policy["semantic_goal"] = *interaction.completion_policy->semantic_goal;
    }
    result["completion_policy"] = std::move(completion_policy);
  }
  if (interaction.long_completion_policy.has_value()) {
    json long_completion_policy = {
        {"response_mode", interaction.long_completion_policy->response_mode},
        {"max_tokens", interaction.long_completion_policy->max_tokens},
        {"max_continuations", interaction.long_completion_policy->max_continuations},
        {"max_total_completion_tokens",
         interaction.long_completion_policy->max_total_completion_tokens},
        {"max_elapsed_time_ms", interaction.long_completion_policy->max_elapsed_time_ms},
    };
    if (interaction.long_completion_policy->target_completion_tokens.has_value()) {
      long_completion_policy["target_completion_tokens"] =
          *interaction.long_completion_policy->target_completion_tokens;
    }
    if (interaction.long_completion_policy->semantic_goal.has_value()) {
      long_completion_policy["semantic_goal"] =
          *interaction.long_completion_policy->semantic_goal;
    }
    result["long_completion_policy"] = std::move(long_completion_policy);
  }
  if (interaction.analysis_completion_policy.has_value()) {
    json analysis_completion_policy = {
        {"response_mode", interaction.analysis_completion_policy->response_mode},
        {"max_tokens", interaction.analysis_completion_policy->max_tokens},
        {"max_continuations", interaction.analysis_completion_policy->max_continuations},
        {"max_total_completion_tokens",
         interaction.analysis_completion_policy->max_total_completion_tokens},
        {"max_elapsed_time_ms", interaction.analysis_completion_policy->max_elapsed_time_ms},
    };
    if (interaction.analysis_completion_policy->target_completion_tokens.has_value()) {
      analysis_completion_policy["target_completion_tokens"] =
          *interaction.analysis_completion_policy->target_completion_tokens;
    }
    if (interaction.analysis_completion_policy->semantic_goal.has_value()) {
      analysis_completion_policy["semantic_goal"] =
          *interaction.analysis_completion_policy->semantic_goal;
    }
    result["analysis_completion_policy"] = std::move(analysis_completion_policy);
  }
  if (interaction.analysis_long_completion_policy.has_value()) {
    json analysis_long_completion_policy = {
        {"response_mode", interaction.analysis_long_completion_policy->response_mode},
        {"max_tokens", interaction.analysis_long_completion_policy->max_tokens},
        {"max_continuations",
         interaction.analysis_long_completion_policy->max_continuations},
        {"max_total_completion_tokens",
         interaction.analysis_long_completion_policy->max_total_completion_tokens},
        {"max_elapsed_time_ms",
         interaction.analysis_long_completion_policy->max_elapsed_time_ms},
    };
    if (interaction.analysis_long_completion_policy->target_completion_tokens
            .has_value()) {
      analysis_long_completion_policy["target_completion_tokens"] =
          *interaction.analysis_long_completion_policy->target_completion_tokens;
    }
    if (interaction.analysis_long_completion_policy->semantic_goal.has_value()) {
      analysis_long_completion_policy["semantic_goal"] =
          *interaction.analysis_long_completion_policy->semantic_goal;
    }
    result["analysis_long_completion_policy"] =
        std::move(analysis_long_completion_policy);
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
      {"published_ports", json::array()},
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
  for (const auto& port : instance.published_ports) {
    result["published_ports"].push_back(ToJson(port));
  }
  return result;
}

PublishedPort PublishedPortFromJson(const json& value) {
  PublishedPort port;
  port.host_ip = value.value("host_ip", port.host_ip);
  port.host_port = value.value("host_port", port.host_port);
  port.container_port = value.value("container_port", port.container_port);
  return port;
}

NodeInventory NodeInventoryFromJson(const json& value) {
  NodeInventory node;
  node.name = value.at("name").get<std::string>();
  node.platform = value.at("platform").get<std::string>();
  node.execution_mode =
      ParseHostExecutionMode(value.value("execution_mode", std::string("mixed")));
  node.gpu_devices = value.value("gpu_devices", std::vector<std::string>{});
  node.gpu_memory_mb = value.value("gpu_memory_mb", std::map<std::string, int>{});
  if (node.gpu_devices.empty()) {
    for (const auto& [gpu_device, _] : node.gpu_memory_mb) {
      node.gpu_devices.push_back(gpu_device);
    }
  }
  return node;
}

WorkerGroupMemberSpec WorkerGroupMemberSpecFromJson(const json& value) {
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

WorkerGroupSpec WorkerGroupSpecFromJson(const json& value) {
  WorkerGroupSpec group;
  group.group_id = value.value("group_id", std::string{});
  group.infer_instance_name = value.value("infer_instance_name", std::string{});
  group.distributed_backend = value.value("distributed_backend", std::string("llama_rpc"));
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

BootstrapModelSpec BootstrapModelSpecFromJson(const json& value) {
  BootstrapModelSpec bootstrap_model;
  bootstrap_model.model_id = value.value("model_id", std::string{});
  bootstrap_model.materialization_mode =
      value.value("materialization_mode", bootstrap_model.materialization_mode);
  if (value.contains("served_model_name") && !value.at("served_model_name").is_null()) {
    bootstrap_model.served_model_name = value.at("served_model_name").get<std::string>();
  }
  if (value.contains("local_path") && !value.at("local_path").is_null()) {
    bootstrap_model.local_path = value.at("local_path").get<std::string>();
  }
  if (value.contains("source_url") && !value.at("source_url").is_null()) {
    bootstrap_model.source_url = value.at("source_url").get<std::string>();
  }
  if (value.contains("source_urls") && value.at("source_urls").is_array()) {
    bootstrap_model.source_urls = value.at("source_urls").get<std::vector<std::string>>();
  }
  if (value.contains("target_filename") && !value.at("target_filename").is_null()) {
    bootstrap_model.target_filename = value.at("target_filename").get<std::string>();
  }
  if (value.contains("sha256") && !value.at("sha256").is_null()) {
    bootstrap_model.sha256 = value.at("sha256").get<std::string>();
  }
  return bootstrap_model;
}

InteractionSettings InteractionSettingsFromJson(const json& value) {
  InteractionSettings interaction;
  if (value.contains("system_prompt") && !value.at("system_prompt").is_null()) {
    interaction.system_prompt = value.at("system_prompt").get<std::string>();
  }
  if (value.contains("analysis_system_prompt") &&
      !value.at("analysis_system_prompt").is_null()) {
    interaction.analysis_system_prompt =
        value.at("analysis_system_prompt").get<std::string>();
  }
  interaction.default_response_language =
      value.value("default_response_language", interaction.default_response_language);
  interaction.supported_response_languages =
      value.value("supported_response_languages", std::vector<std::string>{});
  interaction.follow_user_language =
      value.value("follow_user_language", interaction.follow_user_language);
  if (value.contains("completion_policy") && value.at("completion_policy").is_object()) {
    InteractionSettings::CompletionPolicy completion_policy;
    const auto& policy_value = value.at("completion_policy");
    completion_policy.response_mode =
        policy_value.value("response_mode", completion_policy.response_mode);
    completion_policy.max_tokens =
        policy_value.value("max_tokens", completion_policy.max_tokens);
    if (policy_value.contains("target_completion_tokens") &&
        !policy_value.at("target_completion_tokens").is_null()) {
      completion_policy.target_completion_tokens =
          policy_value.at("target_completion_tokens").get<int>();
    }
    completion_policy.max_continuations =
        policy_value.value("max_continuations", completion_policy.max_continuations);
    completion_policy.max_total_completion_tokens = policy_value.value(
        "max_total_completion_tokens",
        completion_policy.max_total_completion_tokens);
    completion_policy.max_elapsed_time_ms =
        policy_value.value("max_elapsed_time_ms", completion_policy.max_elapsed_time_ms);
    if (policy_value.contains("semantic_goal") &&
        !policy_value.at("semantic_goal").is_null()) {
      completion_policy.semantic_goal = policy_value.at("semantic_goal").get<std::string>();
    }
    interaction.completion_policy = std::move(completion_policy);
  }
  if (value.contains("long_completion_policy") && value.at("long_completion_policy").is_object()) {
    InteractionSettings::CompletionPolicy completion_policy;
    const auto& policy_value = value.at("long_completion_policy");
    completion_policy.response_mode =
        policy_value.value("response_mode", completion_policy.response_mode);
    completion_policy.max_tokens =
        policy_value.value("max_tokens", completion_policy.max_tokens);
    if (policy_value.contains("target_completion_tokens") &&
        !policy_value.at("target_completion_tokens").is_null()) {
      completion_policy.target_completion_tokens =
          policy_value.at("target_completion_tokens").get<int>();
    }
    completion_policy.max_continuations =
        policy_value.value("max_continuations", completion_policy.max_continuations);
    completion_policy.max_total_completion_tokens = policy_value.value(
        "max_total_completion_tokens",
        completion_policy.max_total_completion_tokens);
    completion_policy.max_elapsed_time_ms =
        policy_value.value("max_elapsed_time_ms", completion_policy.max_elapsed_time_ms);
    if (policy_value.contains("semantic_goal") &&
        !policy_value.at("semantic_goal").is_null()) {
      completion_policy.semantic_goal = policy_value.at("semantic_goal").get<std::string>();
    }
    interaction.long_completion_policy = std::move(completion_policy);
  }
  if (value.contains("analysis_completion_policy") &&
      value.at("analysis_completion_policy").is_object()) {
    InteractionSettings::CompletionPolicy completion_policy;
    const auto& policy_value = value.at("analysis_completion_policy");
    completion_policy.response_mode =
        policy_value.value("response_mode", completion_policy.response_mode);
    completion_policy.max_tokens =
        policy_value.value("max_tokens", completion_policy.max_tokens);
    if (policy_value.contains("target_completion_tokens") &&
        !policy_value.at("target_completion_tokens").is_null()) {
      completion_policy.target_completion_tokens =
          policy_value.at("target_completion_tokens").get<int>();
    }
    completion_policy.max_continuations =
        policy_value.value("max_continuations", completion_policy.max_continuations);
    completion_policy.max_total_completion_tokens = policy_value.value(
        "max_total_completion_tokens",
        completion_policy.max_total_completion_tokens);
    completion_policy.max_elapsed_time_ms =
        policy_value.value("max_elapsed_time_ms", completion_policy.max_elapsed_time_ms);
    if (policy_value.contains("semantic_goal") &&
        !policy_value.at("semantic_goal").is_null()) {
      completion_policy.semantic_goal =
          policy_value.at("semantic_goal").get<std::string>();
    }
    interaction.analysis_completion_policy = std::move(completion_policy);
  }
  if (value.contains("analysis_long_completion_policy") &&
      value.at("analysis_long_completion_policy").is_object()) {
    InteractionSettings::CompletionPolicy completion_policy;
    const auto& policy_value = value.at("analysis_long_completion_policy");
    completion_policy.response_mode =
        policy_value.value("response_mode", completion_policy.response_mode);
    completion_policy.max_tokens =
        policy_value.value("max_tokens", completion_policy.max_tokens);
    if (policy_value.contains("target_completion_tokens") &&
        !policy_value.at("target_completion_tokens").is_null()) {
      completion_policy.target_completion_tokens =
          policy_value.at("target_completion_tokens").get<int>();
    }
    completion_policy.max_continuations =
        policy_value.value("max_continuations", completion_policy.max_continuations);
    completion_policy.max_total_completion_tokens = policy_value.value(
        "max_total_completion_tokens",
        completion_policy.max_total_completion_tokens);
    completion_policy.max_elapsed_time_ms =
        policy_value.value("max_elapsed_time_ms", completion_policy.max_elapsed_time_ms);
    if (policy_value.contains("semantic_goal") &&
        !policy_value.at("semantic_goal").is_null()) {
      completion_policy.semantic_goal =
          policy_value.at("semantic_goal").get<std::string>();
    }
    interaction.analysis_long_completion_policy = std::move(completion_policy);
  }
  return interaction;
}

std::string ResolvePlacementTargetAliasValue(
    const std::optional<std::string>& placement_target) {
  if (!placement_target.has_value() || placement_target->empty()) {
    return "";
  }
  if (*placement_target == "local") {
    return "local-hostd";
  }
  constexpr const char* kNodePrefix = "node:";
  if (placement_target->rfind(kNodePrefix, 0) == 0) {
    const std::string node_name = placement_target->substr(std::strlen(kNodePrefix));
    if (node_name.empty()) {
      throw std::runtime_error("placement_target node alias is empty");
    }
    return node_name;
  }
  throw std::runtime_error(
      "unsupported placement_target '" + *placement_target +
      "' (expected 'local' or 'node:<name>')");
}

std::set<std::string> CollectPlacementReferencedNodes(const DesiredState& state) {
  std::set<std::string> referenced_nodes;
  if (!state.inference.primary_infer_node.empty()) {
    referenced_nodes.insert(state.inference.primary_infer_node);
  }
  for (const auto& member : state.worker_group.members) {
    if (!member.node_name.empty()) {
      referenced_nodes.insert(member.node_name);
    }
  }
  for (const auto& node : state.nodes) {
    if (!node.name.empty()) {
      referenced_nodes.insert(node.name);
    }
  }
  for (const auto& disk : state.disks) {
    if (!disk.node_name.empty()) {
      referenced_nodes.insert(disk.node_name);
    }
  }
  for (const auto& instance : state.instances) {
    if (!instance.node_name.empty()) {
      referenced_nodes.insert(instance.node_name);
    }
  }
  for (const auto& gpu_node : state.runtime_gpu_nodes) {
    if (!gpu_node.node_name.empty()) {
      referenced_nodes.insert(gpu_node.node_name);
    }
  }
  return referenced_nodes;
}

json DesiredStateToJson(const DesiredState& state) {
  json result = {
      {"plane_name", state.plane_name},
      {"plane_shared_disk_name", state.plane_shared_disk_name},
      {"control_root", state.control_root},
      {"plane_mode", ToString(state.plane_mode)},
      {"protected", state.protected_plane},
      {"inference",
       {
           {"primary_infer_node", state.inference.primary_infer_node},
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
      {"worker_group", ToJson(state.worker_group)},
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
  if (state.post_deploy_script.has_value()) {
    result["post_deploy_script"] = *state.post_deploy_script;
  }
  if (state.bootstrap_model.has_value()) {
    result["bootstrap_model"] = ToJson(*state.bootstrap_model);
  }
  if (state.interaction.has_value()) {
    result["interaction"] = ToJson(*state.interaction);
  }

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
  if (IsDesiredStateV2(value)) {
    return DesiredStateV2Renderer::Render(value);
  }
  DesiredState state;
  state.plane_name = value.at("plane_name").get<std::string>();
  state.plane_shared_disk_name = value.at("plane_shared_disk_name").get<std::string>();
  state.control_root =
      value.value("control_root", "/comet/shared/control/" + state.plane_name);
  state.plane_mode = ParsePlaneMode(value.value("plane_mode", std::string("compute")));
  state.protected_plane = value.value("protected", state.protected_plane);
  if (value.contains("post_deploy_script") && !value.at("post_deploy_script").is_null()) {
    state.post_deploy_script = value.at("post_deploy_script").get<std::string>();
  }
  if (value.contains("placement_target") && !value.at("placement_target").is_null()) {
    state.placement_target = value.at("placement_target").get<std::string>();
  }
  if (value.contains("bootstrap_model") && value.at("bootstrap_model").is_object()) {
    state.bootstrap_model = BootstrapModelSpecFromJson(value.at("bootstrap_model"));
  }
  if (value.contains("interaction") && value.at("interaction").is_object()) {
    state.interaction = InteractionSettingsFromJson(value.at("interaction"));
  }

  if (value.contains("inference") && value.at("inference").is_object()) {
    const auto& inference = value.at("inference");
    state.inference.primary_infer_node =
        inference.value("primary_infer_node", state.inference.primary_infer_node);
    state.inference.runtime_engine =
        inference.value("runtime_engine", state.inference.runtime_engine);
    state.inference.data_parallel_mode =
        inference.value("data_parallel_mode", state.inference.data_parallel_mode);
    state.inference.data_parallel_lb_mode =
        inference.value("data_parallel_lb_mode", state.inference.data_parallel_lb_mode);
    state.inference.api_server_count =
        inference.value("api_server_count", state.inference.api_server_count);
    state.inference.worker_group_id =
        inference.value("worker_group_id", state.inference.worker_group_id);
    state.inference.distributed_backend =
        inference.value("distributed_backend", state.inference.distributed_backend);
    state.inference.worker_selection_policy =
        inference.value("worker_selection_policy", state.inference.worker_selection_policy);
    state.inference.net_if = inference.value("net_if", state.inference.net_if);
    state.inference.models_root =
        inference.value("models_root", state.inference.models_root);
    state.inference.model_cache_dir =
        inference.value("model_cache_dir", state.inference.model_cache_dir);
    state.inference.runtime_log_dir =
        inference.value("runtime_log_dir", state.inference.runtime_log_dir);
    state.inference.api_port =
        inference.value(
            "api_port",
            inference.value("llama_port", state.inference.api_port));
    state.inference.max_model_len =
        inference.value(
            "max_model_len",
            inference.value("llama_ctx_size", state.inference.max_model_len));
    state.inference.tensor_parallel_size =
        inference.value("tensor_parallel_size", state.inference.tensor_parallel_size);
    state.inference.pipeline_parallel_size =
        inference.value("pipeline_parallel_size", state.inference.pipeline_parallel_size);
    state.inference.max_num_seqs =
        inference.value("max_num_seqs", state.inference.max_num_seqs);
    state.inference.gpu_memory_utilization =
        inference.value(
            "gpu_memory_utilization",
            state.inference.gpu_memory_utilization);
    state.inference.enforce_eager =
        inference.value("enforce_eager", state.inference.enforce_eager);
    state.inference.gguf_cache_dir =
        inference.value(
            "gguf_cache_dir",
            inference.value("model_cache_dir", state.inference.gguf_cache_dir));
    state.inference.infer_log_dir =
        inference.value(
            "infer_log_dir",
            inference.value("runtime_log_dir", state.inference.infer_log_dir));
    state.inference.llama_port =
        inference.value(
            "llama_port",
            inference.value("api_port", state.inference.llama_port));
    state.inference.llama_ctx_size =
        inference.value(
            "llama_ctx_size",
            inference.value("max_model_len", state.inference.llama_ctx_size));
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
    state.inference.rendezvous_port =
        inference.value("rendezvous_port", state.inference.rendezvous_port);
  }
  NormalizeInferenceSettings(&state.inference);
  if (value.contains("worker_group") && value.at("worker_group").is_object()) {
    state.worker_group = WorkerGroupSpecFromJson(value.at("worker_group"));
  }
  if (state.worker_group.group_id.empty()) {
    state.worker_group.group_id = state.inference.worker_group_id;
  }
  if (state.worker_group.group_id.empty()) {
    state.worker_group.group_id = state.plane_name + "-workers";
  }
  if (state.worker_group.distributed_backend.empty()) {
    state.worker_group.distributed_backend = state.inference.distributed_backend;
  }
  if (state.worker_group.worker_selection_policy.empty()) {
    state.worker_group.worker_selection_policy = state.inference.worker_selection_policy;
  }
  if (state.worker_group.rendezvous_port <= 0) {
    state.worker_group.rendezvous_port = state.inference.rendezvous_port;
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

  if (state.worker_group.members.empty()) {
    if (!state.runtime_gpu_nodes.empty()) {
      int next_rank = 0;
      for (const auto& gpu_node : state.runtime_gpu_nodes) {
        WorkerGroupMemberSpec member;
        member.name = gpu_node.name;
        member.node_name = gpu_node.node_name;
        member.gpu_device = gpu_node.gpu_device;
        member.rank = next_rank++;
        member.gpu_fraction = gpu_node.gpu_fraction;
        member.share_mode = gpu_node.share_mode;
        member.priority = gpu_node.priority;
        member.preemptible = gpu_node.preemptible;
        member.memory_cap_mb = gpu_node.memory_cap_mb;
        member.enabled = gpu_node.enabled;
        member.leader = member.rank == 0;
        state.worker_group.members.push_back(std::move(member));
      }
    } else {
      int next_rank = 0;
      for (const auto& instance : state.instances) {
        if (instance.role != InstanceRole::Worker) {
          continue;
        }
        WorkerGroupMemberSpec member;
        member.name = instance.name;
        member.node_name = instance.node_name;
        member.gpu_device = instance.gpu_device.value_or("");
        if (const auto rpc_port_it = instance.environment.find("COMET_WORKER_RPC_PORT");
            rpc_port_it != instance.environment.end() && !rpc_port_it->second.empty()) {
          member.rpc_port = std::stoi(rpc_port_it->second);
        }
        member.rank = next_rank++;
        member.gpu_fraction = instance.gpu_fraction;
        member.share_mode = instance.share_mode;
        member.priority = instance.priority;
        member.preemptible = instance.preemptible;
        member.memory_cap_mb = instance.memory_cap_mb;
        member.enabled = true;
        member.leader = member.rank == 0;
        state.worker_group.members.push_back(std::move(member));
      }
    }
  }
  if (state.worker_group.expected_workers <= 0) {
    state.worker_group.expected_workers =
        DefaultWorkersPerReplica(
            state.inference,
            EligibleWorkerMemberCount(state.worker_group));
  }
  if (state.worker_group.infer_instance_name.empty()) {
    for (const auto& instance : state.instances) {
      if (instance.role == InstanceRole::Infer) {
        state.worker_group.infer_instance_name = instance.name;
        break;
      }
    }
  }
  if (state.worker_group.rendezvous_host.empty()) {
    state.worker_group.rendezvous_host = state.worker_group.infer_instance_name;
  }
  for (auto& member : state.worker_group.members) {
    if (member.infer_instance_name.empty()) {
      member.infer_instance_name = state.worker_group.infer_instance_name;
    }
  }
  ValidateReplicaPacking(state.inference, state.worker_group);
  AssignReplicaTopology(state.inference, &state.worker_group);

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
  result.plane_mode = state.plane_mode;
  result.protected_plane = state.protected_plane;
  result.post_deploy_script = state.post_deploy_script;
  result.placement_target = state.placement_target;
  result.bootstrap_model = state.bootstrap_model;
  result.interaction = state.interaction;
  result.inference = state.inference;
  result.worker_group = state.worker_group;
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

DesiredState ResolvePlacementTargetAliases(DesiredState state) {
  const std::string resolved_target = ResolvePlacementTargetAliasValue(state.placement_target);
  if (resolved_target.empty()) {
    return state;
  }

  const auto referenced_nodes = CollectPlacementReferencedNodes(state);
  if (referenced_nodes.size() > 1) {
    throw std::runtime_error(
        "plane-level placement_target supports only single-node desired states");
  }

  if (!state.inference.primary_infer_node.empty()) {
    state.inference.primary_infer_node = resolved_target;
  }
  for (auto& node : state.nodes) {
    node.name = resolved_target;
  }
  for (auto& disk : state.disks) {
    disk.node_name = resolved_target;
  }
  for (auto& instance : state.instances) {
    instance.node_name = resolved_target;
  }
  for (auto& gpu_node : state.runtime_gpu_nodes) {
    gpu_node.node_name = resolved_target;
  }
  for (auto& member : state.worker_group.members) {
    member.node_name = resolved_target;
  }
  return state;
}

std::string SerializeDesiredStateJson(const DesiredState& state) {
  return DesiredStateToJson(state).dump(2);
}

std::string SerializeDesiredStateV2Json(const DesiredState& state) {
  return DesiredStateV2Projector::Project(state).dump(2);
}

DesiredState DeserializeDesiredStateJson(const std::string& json_text) {
  DesiredState state = DesiredStateFromJson(json::parse(json_text));
  try {
    state = ResolvePlacementTargetAliases(std::move(state));
  } catch (const std::runtime_error& error) {
    if (state.placement_target.has_value() &&
        std::string_view(error.what()) ==
            "plane-level placement_target supports only single-node desired states") {
      state.placement_target.reset();
    } else {
      throw;
    }
  }
  state.placement_target.reset();
  return state;
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
  DesiredState state = DesiredStateFromJson(value);
  try {
    state = ResolvePlacementTargetAliases(std::move(state));
  } catch (const std::runtime_error& error) {
    if (state.placement_target.has_value() &&
        std::string_view(error.what()) ==
            "plane-level placement_target supports only single-node desired states") {
      state.placement_target.reset();
    } else {
      throw;
    }
  }
  state.placement_target.reset();
  return state;
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
