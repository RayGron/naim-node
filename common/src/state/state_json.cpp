#include "naim/state/state_json.h"

#include <cstring>
#include <filesystem>
#include <fstream>
#include <set>
#include <stdexcept>
#include <string_view>
#include <utility>

#include <nlohmann/json.hpp>

#include "naim/state/desired_state_v2_projector.h"
#include "naim/state/desired_state_v2_renderer.h"
#include "naim/state/state_json_runtime_codecs.h"
#include "naim/state/state_json_settings_codecs.h"
#include "naim/state/worker_group_topology.h"

namespace naim {

namespace {

using nlohmann::json;
using RuntimeCodecs = StateJsonRuntimeCodecs;
using SettingsCodecs = StateJsonSettingsCodecs;

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

json ToJson(const TurboQuantFeatureSpec& turboquant) {
  json result = {
      {"enabled", turboquant.enabled},
  };
  if (turboquant.cache_type_k.has_value()) {
    result["cache_type_k"] = *turboquant.cache_type_k;
  }
  if (turboquant.cache_type_v.has_value()) {
    result["cache_type_v"] = *turboquant.cache_type_v;
  }
  return result;
}

json ToJson(const ContextCompressionFeatureSpec& context_compression) {
  return {
      {"enabled", context_compression.enabled},
      {"mode", context_compression.mode},
      {"target", context_compression.target},
      {"memory_priority", context_compression.memory_priority},
  };
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
      {"worker_group", RuntimeCodecs::ToJson(state.worker_group)},
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
  if (state.placement_target.has_value()) {
    result["placement_target"] = *state.placement_target;
  }
  if (state.bootstrap_model.has_value()) {
    result["bootstrap_model"] = SettingsCodecs::ToJson(*state.bootstrap_model);
  }
  if (state.interaction.has_value()) {
    result["interaction"] = SettingsCodecs::ToJson(*state.interaction);
  }
  if (state.skills.has_value()) {
    result["skills"] = SettingsCodecs::ToJson(*state.skills);
  }
  if (state.browsing.has_value()) {
    result["webgateway"] = SettingsCodecs::ToJson(*state.browsing);
  }
  if (state.knowledge.has_value()) {
    result["knowledge"] = SettingsCodecs::ToJson(*state.knowledge);
  }
  if (state.turboquant.has_value() || state.context_compression.has_value()) {
    json features = json::object();
    if (state.turboquant.has_value()) {
      features["turboquant"] = ToJson(*state.turboquant);
    }
    if (state.context_compression.has_value()) {
      features["context_compression"] = ToJson(*state.context_compression);
    }
    result["features"] = std::move(features);
  }
  if (state.app_host.has_value()) {
    result["app_host"] = SettingsCodecs::ToJson(*state.app_host);
  }

  for (const auto& gpu_node : state.runtime_gpu_nodes) {
    result["runtime_gpu_nodes"].push_back(RuntimeCodecs::ToJson(gpu_node));
  }
  for (const auto& node : state.nodes) {
    result["nodes"].push_back(RuntimeCodecs::ToJson(node));
  }
  for (const auto& disk : state.disks) {
    result["disks"].push_back(RuntimeCodecs::ToJson(disk));
  }
  for (const auto& instance : state.instances) {
    result["instances"].push_back(RuntimeCodecs::ToJson(instance));
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
      value.value("control_root", "/naim/shared/control/" + state.plane_name);
  state.plane_mode = ParsePlaneMode(value.value("plane_mode", std::string("compute")));
  state.protected_plane = value.value("protected", state.protected_plane);
  if (value.contains("post_deploy_script") && !value.at("post_deploy_script").is_null()) {
    state.post_deploy_script = value.at("post_deploy_script").get<std::string>();
  }
  if (value.contains("placement_target") && !value.at("placement_target").is_null()) {
    state.placement_target = value.at("placement_target").get<std::string>();
  }
  if (value.contains("bootstrap_model") && value.at("bootstrap_model").is_object()) {
    state.bootstrap_model =
        SettingsCodecs::BootstrapModelSpecFromJson(value.at("bootstrap_model"));
  }
  if (value.contains("interaction") && value.at("interaction").is_object()) {
    state.interaction =
        SettingsCodecs::InteractionSettingsFromJson(value.at("interaction"));
  }
  if (value.contains("skills") && value.at("skills").is_object()) {
    state.skills = SettingsCodecs::SkillsSettingsFromJson(value.at("skills"));
  }
  const auto* browsing_block =
      value.contains("webgateway") && value.at("webgateway").is_object()
          ? &value.at("webgateway")
          : (value.contains("browsing") && value.at("browsing").is_object()
                 ? &value.at("browsing")
                 : nullptr);
  if (browsing_block != nullptr) {
    state.browsing = SettingsCodecs::BrowsingSettingsFromJson(*browsing_block);
  }
  if (value.contains("knowledge") && value.at("knowledge").is_object()) {
    state.knowledge = SettingsCodecs::KnowledgeSettingsFromJson(value.at("knowledge"));
  }
  if (value.contains("features") && value.at("features").is_object()) {
    const auto& features = value.at("features");
    if (features.contains("turboquant") && features.at("turboquant").is_object()) {
      TurboQuantFeatureSpec turboquant;
      const auto& turboquant_json = features.at("turboquant");
      turboquant.enabled = turboquant_json.value("enabled", turboquant.enabled);
      if (turboquant_json.contains("cache_type_k") &&
          !turboquant_json.at("cache_type_k").is_null()) {
        turboquant.cache_type_k = turboquant_json.at("cache_type_k").get<std::string>();
      }
      if (turboquant_json.contains("cache_type_v") &&
          !turboquant_json.at("cache_type_v").is_null()) {
        turboquant.cache_type_v = turboquant_json.at("cache_type_v").get<std::string>();
      }
      state.turboquant = std::move(turboquant);
    }
    if (features.contains("context_compression") &&
        features.at("context_compression").is_object()) {
      ContextCompressionFeatureSpec context_compression;
      const auto& context_compression_json = features.at("context_compression");
      context_compression.enabled = context_compression_json.value(
          "enabled",
          context_compression.enabled);
      context_compression.mode = context_compression_json.value(
          "mode",
          context_compression.mode);
      context_compression.target = context_compression_json.value(
          "target",
          context_compression.target);
      context_compression.memory_priority = context_compression_json.value(
          "memory_priority",
          context_compression.memory_priority);
      state.context_compression = std::move(context_compression);
    }
  }
  if (value.contains("app_host") && value.at("app_host").is_object()) {
    state.app_host =
        SettingsCodecs::ExternalAppHostConfigFromJson(value.at("app_host"));
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
    state.worker_group = RuntimeCodecs::WorkerGroupSpecFromJson(value.at("worker_group"));
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
    state.runtime_gpu_nodes.push_back(RuntimeCodecs::RuntimeGpuNodeFromJson(gpu_node));
  }
  for (const auto& node : value.value("nodes", json::array())) {
    state.nodes.push_back(RuntimeCodecs::NodeInventoryFromJson(node));
  }
  for (const auto& disk : value.value("disks", json::array())) {
    state.disks.push_back(RuntimeCodecs::DiskSpecFromJson(disk));
  }
  for (const auto& instance : value.value("instances", json::array())) {
    state.instances.push_back(RuntimeCodecs::InstanceSpecFromJson(instance));
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
        if (const auto rpc_port_it = instance.environment.find("NAIM_WORKER_RPC_PORT");
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
  result.skills = state.skills;
  result.browsing = state.browsing;
  result.turboquant = state.turboquant;
  result.context_compression = state.context_compression;
  result.app_host = state.app_host;
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
    DesiredState resolved_state = state;
    state = ResolvePlacementTargetAliases(std::move(resolved_state));
  } catch (const std::runtime_error& error) {
    if (state.placement_target.has_value() &&
        std::string_view(error.what()) ==
            "plane-level placement_target supports only single-node desired states") {
      state.placement_target.reset();
    } else {
      throw;
    }
  }
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
  if (!IsDesiredStateV2(value)) {
    throw std::runtime_error(
        "desired state file must use version=2 and desired-state.v2.json");
  }

  DesiredState state = DesiredStateV2Renderer::Render(value);
  try {
    DesiredState resolved_state = state;
    state = ResolvePlacementTargetAliases(std::move(resolved_state));
  } catch (const std::runtime_error& error) {
    if (state.placement_target.has_value() &&
        std::string_view(error.what()) ==
            "plane-level placement_target supports only single-node desired states") {
      state.placement_target.reset();
    } else {
      throw;
    }
  }
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

  output << SerializeDesiredStateV2Json(state) << "\n";
  if (!output.good()) {
    throw std::runtime_error("failed to write desired state file: " + path);
  }
}

}  // namespace naim
