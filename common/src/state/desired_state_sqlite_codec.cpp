#include "comet/state/desired_state_sqlite_codec.h"

#include <stdexcept>
#include <utility>

#include <nlohmann/json.hpp>

namespace comet {

namespace {

using nlohmann::json;

}  // namespace

std::string DesiredStateSqliteCodec::SerializeInferenceSettings(
    const InferenceRuntimeSettings& settings) {
  return json{
      {"primary_infer_node", settings.primary_infer_node},
      {"runtime_engine", settings.runtime_engine},
      {"data_parallel_mode", settings.data_parallel_mode},
      {"data_parallel_lb_mode", settings.data_parallel_lb_mode},
      {"api_server_count", settings.api_server_count},
      {"worker_group_id", settings.worker_group_id},
      {"distributed_backend", settings.distributed_backend},
      {"worker_selection_policy", settings.worker_selection_policy},
      {"net_if", settings.net_if},
      {"models_root", settings.models_root},
      {"model_cache_dir", settings.model_cache_dir},
      {"runtime_log_dir", settings.runtime_log_dir},
      {"api_port", settings.api_port},
      {"max_model_len", settings.max_model_len},
      {"tensor_parallel_size", settings.tensor_parallel_size},
      {"pipeline_parallel_size", settings.pipeline_parallel_size},
      {"max_num_seqs", settings.max_num_seqs},
      {"gpu_memory_utilization", settings.gpu_memory_utilization},
      {"enforce_eager", settings.enforce_eager},
      {"gguf_cache_dir", settings.gguf_cache_dir},
      {"infer_log_dir", settings.infer_log_dir},
      {"llama_port", settings.llama_port},
      {"llama_ctx_size", settings.llama_ctx_size},
      {"llama_threads", settings.llama_threads},
      {"llama_gpu_layers", settings.llama_gpu_layers},
      {"inference_healthcheck_retries", settings.inference_healthcheck_retries},
      {"inference_healthcheck_interval_sec", settings.inference_healthcheck_interval_sec},
      {"rendezvous_port", settings.rendezvous_port},
  }
      .dump();
}

std::string DesiredStateSqliteCodec::SerializeBootstrapModelSpec(
    const std::optional<BootstrapModelSpec>& bootstrap_model) {
  if (!bootstrap_model.has_value()) {
    return "";
  }
  json value = {
      {"model_id", bootstrap_model->model_id},
      {"materialization_mode", bootstrap_model->materialization_mode},
  };
  if (bootstrap_model->served_model_name.has_value()) {
    value["served_model_name"] = *bootstrap_model->served_model_name;
  }
  if (bootstrap_model->local_path.has_value()) {
    value["local_path"] = *bootstrap_model->local_path;
  }
  if (bootstrap_model->source_url.has_value()) {
    value["source_url"] = *bootstrap_model->source_url;
  }
  if (!bootstrap_model->source_urls.empty()) {
    value["source_urls"] = bootstrap_model->source_urls;
  }
  if (bootstrap_model->target_filename.has_value()) {
    value["target_filename"] = *bootstrap_model->target_filename;
  }
  if (bootstrap_model->sha256.has_value()) {
    value["sha256"] = *bootstrap_model->sha256;
  }
  return value.dump();
}

std::optional<BootstrapModelSpec> DesiredStateSqliteCodec::DeserializeBootstrapModelSpec(
    const std::string& json_text) {
  if (json_text.empty()) {
    return std::nullopt;
  }
  const json value = json::parse(json_text);
  if (!value.is_object()) {
    return std::nullopt;
  }
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

std::optional<InteractionSettings> DesiredStateSqliteCodec::DeserializeInteractionSettings(
    const std::string& json_text) {
  if (json_text.empty()) {
    return std::nullopt;
  }
  const json value = json::parse(json_text);
  if (!value.is_object()) {
    return std::nullopt;
  }
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

InferenceRuntimeSettings DesiredStateSqliteCodec::DeserializeInferenceSettings(
    const std::string& json_text) {
  InferenceRuntimeSettings settings;
  if (json_text.empty()) {
    return settings;
  }

  const json value = json::parse(json_text);
  settings.primary_infer_node =
      value.value("primary_infer_node", settings.primary_infer_node);
  settings.runtime_engine = value.value("runtime_engine", settings.runtime_engine);
  settings.data_parallel_mode =
      value.value("data_parallel_mode", settings.data_parallel_mode);
  settings.data_parallel_lb_mode =
      value.value("data_parallel_lb_mode", settings.data_parallel_lb_mode);
  settings.api_server_count =
      value.value("api_server_count", settings.api_server_count);
  settings.worker_group_id = value.value("worker_group_id", settings.worker_group_id);
  settings.distributed_backend =
      value.value("distributed_backend", settings.distributed_backend);
  settings.worker_selection_policy =
      value.value("worker_selection_policy", settings.worker_selection_policy);
  settings.net_if = value.value("net_if", settings.net_if);
  settings.models_root = value.value("models_root", settings.models_root);
  settings.model_cache_dir = value.value("model_cache_dir", settings.model_cache_dir);
  settings.runtime_log_dir = value.value("runtime_log_dir", settings.runtime_log_dir);
  settings.api_port = value.value("api_port", value.value("llama_port", settings.api_port));
  settings.max_model_len =
      value.value("max_model_len", value.value("llama_ctx_size", settings.max_model_len));
  settings.tensor_parallel_size =
      value.value("tensor_parallel_size", settings.tensor_parallel_size);
  settings.pipeline_parallel_size =
      value.value("pipeline_parallel_size", settings.pipeline_parallel_size);
  settings.max_num_seqs = value.value("max_num_seqs", settings.max_num_seqs);
  settings.gpu_memory_utilization =
      value.value("gpu_memory_utilization", settings.gpu_memory_utilization);
  settings.enforce_eager = value.value("enforce_eager", settings.enforce_eager);
  settings.gguf_cache_dir =
      value.value("gguf_cache_dir", value.value("model_cache_dir", settings.gguf_cache_dir));
  settings.infer_log_dir =
      value.value("infer_log_dir", value.value("runtime_log_dir", settings.infer_log_dir));
  settings.llama_port = value.value("llama_port", value.value("api_port", settings.llama_port));
  settings.llama_ctx_size =
      value.value("llama_ctx_size", value.value("max_model_len", settings.llama_ctx_size));
  settings.llama_threads = value.value("llama_threads", settings.llama_threads);
  settings.llama_gpu_layers = value.value("llama_gpu_layers", settings.llama_gpu_layers);
  settings.inference_healthcheck_retries = value.value(
      "inference_healthcheck_retries", settings.inference_healthcheck_retries);
  settings.inference_healthcheck_interval_sec = value.value(
      "inference_healthcheck_interval_sec", settings.inference_healthcheck_interval_sec);
  settings.rendezvous_port = value.value("rendezvous_port", settings.rendezvous_port);
  if (settings.model_cache_dir.empty()) {
    settings.model_cache_dir = settings.gguf_cache_dir;
  }
  if (settings.gguf_cache_dir.empty()) {
    settings.gguf_cache_dir = settings.model_cache_dir;
  }
  if (settings.runtime_log_dir.empty()) {
    settings.runtime_log_dir = settings.infer_log_dir;
  }
  if (settings.infer_log_dir.empty()) {
    settings.infer_log_dir = settings.runtime_log_dir;
  }
  if (settings.api_port <= 0) {
    settings.api_port = settings.llama_port;
  }
  if (settings.llama_port <= 0) {
    settings.llama_port = settings.api_port;
  }
  if (settings.max_model_len <= 0) {
    settings.max_model_len = settings.llama_ctx_size;
  }
  if (settings.llama_ctx_size <= 0) {
    settings.llama_ctx_size = settings.max_model_len;
  }
  return settings;
}

std::string DesiredStateSqliteCodec::SerializeGatewaySettings(const GatewaySettings& settings) {
  return json{
      {"listen_host", settings.listen_host},
      {"listen_port", settings.listen_port},
      {"server_name", settings.server_name},
  }
      .dump();
}

std::string DesiredStateSqliteCodec::SerializeRuntimeGpuNodes(
    const std::vector<RuntimeGpuNode>& gpu_nodes) {
  json value = json::array();
  for (const auto& gpu_node : gpu_nodes) {
    json gpu_node_json = {
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
      gpu_node_json["memory_cap_mb"] = *gpu_node.memory_cap_mb;
    }
    value.push_back(std::move(gpu_node_json));
  }
  return value.dump();
}

std::vector<RuntimeGpuNode> DesiredStateSqliteCodec::DeserializeRuntimeGpuNodes(
    const std::string& json_text) {
  std::vector<RuntimeGpuNode> gpu_nodes;
  if (json_text.empty()) {
    return gpu_nodes;
  }

  const json value = json::parse(json_text);
  for (const auto& item : value) {
    gpu_nodes.push_back(
        RuntimeGpuNode{
            item.value("name", std::string{}),
            item.value("node_name", std::string{}),
            item.value("gpu_device", std::string{}),
            ParsePlacementMode(item.value("placement_mode", std::string("manual"))),
            ParseGpuShareMode(item.value("share_mode", std::string("exclusive"))),
            item.value("gpu_fraction", 0.0),
            item.value("priority", 100),
            item.value("preemptible", false),
            item.contains("memory_cap_mb") && !item.at("memory_cap_mb").is_null()
                ? std::optional<int>(item.at("memory_cap_mb").get<int>())
                : std::nullopt,
            item.value("enabled", true),
        });
  }
  return gpu_nodes;
}

GatewaySettings DesiredStateSqliteCodec::DeserializeGatewaySettings(const std::string& json_text) {
  GatewaySettings settings;
  if (json_text.empty()) {
    return settings;
  }

  const json value = json::parse(json_text);
  settings.listen_host = value.value("listen_host", settings.listen_host);
  settings.listen_port = value.value("listen_port", settings.listen_port);
  settings.server_name = value.value("server_name", settings.server_name);
  return settings;
}

DiskKind DesiredStateSqliteCodec::ParseDiskKind(const std::string& value) {
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

InstanceRole DesiredStateSqliteCodec::ParseInstanceRole(const std::string& value) {
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

}  // namespace comet
