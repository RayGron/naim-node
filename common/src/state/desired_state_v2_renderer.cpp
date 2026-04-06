#include "comet/state/desired_state_v2_renderer.h"

#include <algorithm>
#include <cstdint>
#include <stdexcept>
#include <string>
#include <utility>

#include "comet/runtime/infer_runtime_config.h"
#include "comet/state/desired_state_v2_validator.h"
#include "comet/state/worker_group_topology.h"

namespace comet {

namespace {

constexpr std::string_view kDesiredStateV2BundlePrefix = "bundle://";
constexpr int kDefaultSharedDiskSizeGb = 40;
constexpr int kDefaultInferPrivateDiskSizeGb = 12;
constexpr int kDefaultWorkerPrivateDiskSizeGb = 2;
constexpr int kDefaultAppPrivateDiskSizeGb = 8;
constexpr int kDefaultSkillsPrivateDiskSizeGb = 1;
constexpr int kDefaultWebGatewayPrivateDiskSizeGb = 1;
constexpr int kSkillsContainerPort = 18120;
constexpr int kSkillsPublishedPortBase = 24000;
constexpr int kSkillsPublishedPortSpan = 10000;
constexpr int kWebGatewayContainerPort = 18130;
constexpr int kWebGatewayPublishedPortBase = 34000;
constexpr int kWebGatewayPublishedPortSpan = 10000;

bool HasExplicitPrivateStorage(
    const nlohmann::json& service_json,
    const std::string& legacy_volume_key) {
  return (service_json.contains("storage") && service_json.at("storage").is_object()) ||
         (service_json.contains(legacy_volume_key) && service_json.at(legacy_volume_key).is_array() &&
          !service_json.at(legacy_volume_key).empty());
}

}  // namespace

DesiredState DesiredStateV2Renderer::Render(const nlohmann::json& value) {
  DesiredStateV2Validator::ValidateOrThrow(value);
  return DesiredStateV2Renderer(value).RenderState();
}

DesiredStateV2Renderer::DesiredStateV2Renderer(const nlohmann::json& value)
    : value_(value),
      infer_json_(
          value.contains("infer") && value.at("infer").is_object() ? value.at("infer")
                                                                    : nlohmann::json::object()),
      worker_json_(
          value.contains("worker") && value.at("worker").is_object() ? value.at("worker")
                                                                      : nlohmann::json::object()),
      resources_json_(
          value.contains("resources") && value.at("resources").is_object()
              ? value.at("resources")
              : nlohmann::json::object()),
      worker_resources_json_(
          resources_json_.contains("worker") && resources_json_.at("worker").is_object()
              ? resources_json_.at("worker")
              : nlohmann::json::object()),
      app_json_(
          value.contains("app") && value.at("app").is_object() ? value.at("app")
                                                                : nlohmann::json::object()),
      skills_json_(
          value.contains("skills") && value.at("skills").is_object() ? value.at("skills")
                                                                      : nlohmann::json::object()),
      browsing_json_(
          value.contains("webgateway") && value.at("webgateway").is_object()
              ? value.at("webgateway")
              : (value.contains("browsing") && value.at("browsing").is_object() ? value.at("browsing")
                                                                                 : nlohmann::json::object())) {}

DesiredState DesiredStateV2Renderer::RenderState() {
  RenderIdentity();
  RenderHooks();
  RenderModel();
  RenderInteraction();
  RenderRuntime();
  RenderNodeTopology();
  RenderWorkerGroup();
  RenderSharedDisk();
  RenderInferInstance();
  RenderWorkerInstances();
  RenderAppInstance();
  RenderSkillsInstance();
  RenderWebGatewayInstance();
  return state_;
}

void DesiredStateV2Renderer::RenderIdentity() {
  state_.plane_name = value_.at("plane_name").get<std::string>();
  state_.plane_shared_disk_name = BuildPlaneSharedDiskName();
  state_.control_root = "/comet/shared/control/" + state_.plane_name;
  state_.plane_mode = ParsePlaneMode(value_.value("plane_mode", std::string("llm")));
  state_.protected_plane = value_.value("protected", false);
  if (skills_json_.value("enabled", false)) {
    SkillsSettings skills_settings;
    skills_settings.enabled = true;
    if (skills_json_.contains("factory_skill_ids") &&
        skills_json_.at("factory_skill_ids").is_array()) {
      for (const auto& item : skills_json_.at("factory_skill_ids")) {
        if (item.is_string()) {
          skills_settings.factory_skill_ids.push_back(item.get<std::string>());
        }
      }
    }
    state_.skills = std::move(skills_settings);
  }
  if (browsing_json_.value("enabled", false)) {
    BrowsingSettings browsing_settings;
    browsing_settings.enabled = true;
    if (browsing_json_.contains("policy") && browsing_json_.at("policy").is_object()) {
      BrowsingPolicySettings policy;
      const auto& policy_json = browsing_json_.at("policy");
      policy.cef_enabled =
          policy_json.value("cef_enabled", policy.cef_enabled);
      policy.browser_session_enabled =
          policy_json.value("browser_session_enabled", policy.browser_session_enabled);
      policy.rendered_browser_enabled =
          policy_json.value("rendered_browser_enabled", policy.rendered_browser_enabled);
      policy.login_enabled =
          policy_json.value("login_enabled", policy.login_enabled);
      policy.response_review_enabled =
          policy_json.value("response_review_enabled", policy.response_review_enabled);
      policy.policy_version =
          policy_json.value("policy_version", policy.policy_version);
      policy.max_search_results =
          policy_json.value("max_search_results", policy.max_search_results);
      policy.max_fetch_bytes =
          policy_json.value("max_fetch_bytes", policy.max_fetch_bytes);
      if (policy_json.contains("allowed_domains") && policy_json.at("allowed_domains").is_array()) {
        for (const auto& item : policy_json.at("allowed_domains")) {
          if (item.is_string()) {
            policy.allowed_domains.push_back(item.get<std::string>());
          }
        }
      }
      if (policy_json.contains("blocked_domains") && policy_json.at("blocked_domains").is_array()) {
        for (const auto& item : policy_json.at("blocked_domains")) {
          if (item.is_string()) {
            policy.blocked_domains.push_back(item.get<std::string>());
          }
        }
      }
      if (policy_json.contains("blocked_targets") && policy_json.at("blocked_targets").is_array()) {
        for (const auto& item : policy_json.at("blocked_targets")) {
          if (item.is_string()) {
            policy.blocked_targets.push_back(item.get<std::string>());
          }
        }
      }
      browsing_settings.policy = std::move(policy);
    }
    state_.browsing = std::move(browsing_settings);
  }
}

void DesiredStateV2Renderer::RenderHooks() {
  if (!value_.contains("hooks") || !value_.at("hooks").is_object()) {
    return;
  }
  const auto& hooks = value_.at("hooks");
  if (hooks.contains("post_deploy_script") && !hooks.at("post_deploy_script").is_null()) {
    state_.post_deploy_script =
        StripBundleRefPrefix(hooks.at("post_deploy_script").get<std::string>());
  }
}

void DesiredStateV2Renderer::RenderModel() {
  if (!value_.contains("model") || !value_.at("model").is_object()) {
    return;
  }

  BootstrapModelSpec model;
  const auto& model_json = value_.at("model");
  if (model_json.contains("source") && model_json.at("source").is_object()) {
    const auto& source = model_json.at("source");
    const std::string source_type = source.value("type", "");
    if (source_type == "catalog" || source_type == "huggingface") {
      model.model_id = source.value("ref", "");
    } else if (source_type == "local") {
      model.model_id = source.value("ref", source.value("path", ""));
      if (source.contains("path") && source.at("path").is_string()) {
        model.local_path = source.at("path").get<std::string>();
      }
    } else if (source_type == "url") {
      model.model_id = source.value("ref", "");
      if (source.contains("url") && source.at("url").is_string()) {
        model.source_url = source.at("url").get<std::string>();
      }
      if (source.contains("urls") && source.at("urls").is_array()) {
        model.source_urls = source.at("urls").get<std::vector<std::string>>();
      }
    }
  }

  if (model_json.contains("materialization") && model_json.at("materialization").is_object()) {
    const auto& materialization = model_json.at("materialization");
    model.materialization_mode = materialization.value(
        "mode",
        model.local_path.has_value() ? std::string("reference") : std::string("copy"));
    if (materialization.contains("local_path") && materialization.at("local_path").is_string()) {
      model.local_path = materialization.at("local_path").get<std::string>();
    }
  }

  if (!model_json.value("served_model_name", std::string{}).empty()) {
    model.served_model_name = model_json.at("served_model_name").get<std::string>();
  }
  if (model_json.contains("target_filename") && model_json.at("target_filename").is_string()) {
    model.target_filename = model_json.at("target_filename").get<std::string>();
  }
  if (model_json.contains("sha256") && model_json.at("sha256").is_string()) {
    model.sha256 = model_json.at("sha256").get<std::string>();
  }
  state_.bootstrap_model = std::move(model);
}

void DesiredStateV2Renderer::RenderInteraction() {
  if (!value_.contains("interaction") || !value_.at("interaction").is_object()) {
    return;
  }

  InteractionSettings interaction;
  const auto& interaction_json = value_.at("interaction");
  if (interaction_json.contains("system_prompt") && interaction_json.at("system_prompt").is_string()) {
    interaction.system_prompt = interaction_json.at("system_prompt").get<std::string>();
  }
  interaction.thinking_enabled =
      interaction_json.value("thinking_enabled", interaction.thinking_enabled);
  if (interaction_json.contains("default_temperature") &&
      !interaction_json.at("default_temperature").is_null()) {
    interaction.default_temperature =
        interaction_json.at("default_temperature").get<double>();
  }
  if (interaction_json.contains("default_top_p") &&
      !interaction_json.at("default_top_p").is_null()) {
    interaction.default_top_p = interaction_json.at("default_top_p").get<double>();
  }
  interaction.default_response_language =
      interaction_json.value("default_response_language", std::string("en"));
  interaction.follow_user_language = interaction_json.value("follow_user_language", true);
  if (interaction_json.contains("supported_response_languages") &&
      interaction_json.at("supported_response_languages").is_array()) {
    interaction.supported_response_languages =
        interaction_json.at("supported_response_languages").get<std::vector<std::string>>();
  } else {
    interaction.supported_response_languages = {"en", "de", "uk", "ru"};
  }
  interaction.completion_policy = DefaultCompletionPolicy();
  interaction.long_completion_policy = DefaultLongCompletionPolicy();
  state_.interaction = std::move(interaction);
}

void DesiredStateV2Renderer::RenderRuntime() {
  const auto& runtime_json = value_.at("runtime");
  state_.inference.runtime_engine = runtime_json.value("engine", std::string("llama.cpp"));
  state_.inference.data_parallel_mode =
      runtime_json.value("data_parallel_mode", state_.inference.data_parallel_mode);
  state_.inference.data_parallel_lb_mode =
      runtime_json.value("data_parallel_lb_mode", state_.inference.data_parallel_lb_mode);
  state_.inference.distributed_backend = runtime_json.value(
      "distributed_backend",
      state_.inference.runtime_engine == "llama.cpp" ? std::string("llama_rpc")
                                                     : std::string("local"));
  state_.inference.worker_selection_policy = "prefer-free-then-share";
  state_.inference.net_if = "eth0";
  state_.inference.models_root = "/comet/shared/models";
  state_.inference.model_cache_dir = "/comet/shared/models/cache";
  state_.inference.runtime_log_dir = "/comet/shared/logs/infer";
  state_.inference.max_model_len =
      runtime_json.value("max_model_len", state_.inference.max_model_len);
  state_.inference.llama_ctx_size =
      runtime_json.value("llama_ctx_size", state_.inference.llama_ctx_size);
  state_.inference.max_num_seqs =
      runtime_json.value("max_num_seqs", state_.inference.max_num_seqs);
  state_.inference.gpu_memory_utilization =
      runtime_json.value("gpu_memory_utilization", state_.inference.gpu_memory_utilization);
  state_.inference.tensor_parallel_size =
      runtime_json.value("tensor_parallel_size", state_.inference.tensor_parallel_size);
  state_.inference.pipeline_parallel_size =
      runtime_json.value("pipeline_parallel_size", state_.inference.pipeline_parallel_size);
  state_.inference.enforce_eager =
      runtime_json.value("enforce_eager", state_.inference.enforce_eager);

  const nlohmann::json network_json =
      value_.contains("network") && value_.at("network").is_object() ? value_.at("network")
                                                                      : nlohmann::json::object();
  state_.gateway.listen_host = network_json.value("listen_host", std::string("0.0.0.0"));
  state_.gateway.listen_port = network_json.value("gateway_port", state_.gateway.listen_port);
  state_.gateway.server_name = network_json.value("server_name", state_.gateway.server_name);
  state_.inference.api_port = network_json.value("inference_port", state_.inference.api_port);
  state_.inference.rendezvous_port =
      network_json.value("rendezvous_port", state_.inference.rendezvous_port);
  NormalizeInferenceSettings();
}

void DesiredStateV2Renderer::RenderNodeTopology() {
  if (value_.contains("topology") && value_.at("topology").is_object()) {
    const auto& topology = value_.at("topology");
    if (topology.contains("nodes") && topology.at("nodes").is_array()) {
      for (const auto& node_json : topology.at("nodes")) {
        NodeInventory node;
        node.name = node_json.at("name").get<std::string>();
        node.platform = node_json.value("platform", std::string("linux"));
        node.execution_mode =
            ParseHostExecutionMode(node_json.value("execution_mode", std::string("mixed")));
        if (node_json.contains("gpu_memory_mb") && node_json.at("gpu_memory_mb").is_object()) {
          node.gpu_memory_mb =
              node_json.at("gpu_memory_mb").get<std::map<std::string, int>>();
        }
        state_.nodes.push_back(std::move(node));
      }
    }
  }
  if (!state_.nodes.empty()) {
    return;
  }
  NodeInventory node;
  node.name = "local-hostd";
  node.platform = "linux";
  node.execution_mode = HostExecutionMode::Mixed;
  state_.nodes.push_back(std::move(node));
}

void DesiredStateV2Renderer::RenderWorkerGroup() {
  state_.inference.primary_infer_node = InferEnabled() ? ResolveInferNodeName() : DefaultNodeName();
  state_.worker_group.group_id = state_.plane_name + "-workers";
  state_.worker_group.infer_instance_name = InferEnabled() ? BuildInferInstanceName() : "";
  state_.worker_group.distributed_backend = state_.inference.distributed_backend;
  state_.worker_group.rendezvous_host = state_.worker_group.infer_instance_name;
  state_.worker_group.rendezvous_port = state_.inference.rendezvous_port;
  state_.worker_group.expected_workers = ExpectedWorkers();
  state_.worker_group.worker_selection_policy = "prefer-free-then-share";
  state_.inference.worker_group_id = state_.worker_group.group_id;
}

void DesiredStateV2Renderer::RenderSharedDisk() {
  DiskSpec plane_shared_disk;
  plane_shared_disk.name = state_.plane_shared_disk_name;
  plane_shared_disk.kind = DiskKind::PlaneShared;
  plane_shared_disk.plane_name = state_.plane_name;
  plane_shared_disk.owner_name = state_.plane_name;
  plane_shared_disk.node_name = SharedDiskNodeName();
  plane_shared_disk.host_path = BuildPlaneSharedHostPath();
  plane_shared_disk.container_path = "/comet/shared";
  plane_shared_disk.size_gb = resources_json_.value("shared_disk_gb", kDefaultSharedDiskSizeGb);
  state_.disks.push_back(std::move(plane_shared_disk));
}

void DesiredStateV2Renderer::RenderInferInstance() {
  if (!InferEnabled()) {
    return;
  }

  const int infer_count =
      state_.inference.runtime_engine == "llama.cpp" &&
              state_.inference.distributed_backend == "llama_rpc"
          ? InferReplicaCount() + 1
          : 1;
  std::vector<InstanceSpec> rendered_infers;
  rendered_infers.reserve(infer_count);

  for (int infer_index = 0; infer_index < infer_count; ++infer_index) {
    InstanceSpec infer;
    const bool has_private_storage = HasExplicitPrivateStorage(infer_json_, "volumes");
    infer.name = BuildInferInstanceName(infer_index);
    infer.role = InstanceRole::Infer;
    infer.plane_name = state_.plane_name;
    infer.node_name = ResolveInferNodeName(infer_index);
    infer.image = infer_json_.value("image", std::string("comet/infer-runtime:dev"));
    infer.command =
        BuildCommandFromStartSpec(infer_json_.value("start", nlohmann::json::object()),
                                  "/runtime/bin/comet-inferctl container-boot");
    infer.private_disk_name =
        has_private_storage && InstanceNeedsPrivateDisk(infer.role) ? infer.name + "-private" : "";
    infer.shared_disk_name =
        InstanceNeedsSharedDiskMount(infer.role) ? state_.plane_shared_disk_name : "";
    infer.private_disk_size_gb = has_private_storage
                                     ? ExtractPrivateDiskSizeGb(
                                           infer_json_,
                                           kDefaultInferPrivateDiskSizeGb,
                                           "volumes")
                                     : 0;
    infer.environment = {
        {"COMET_PLANE_NAME", state_.plane_name},
        {"COMET_INSTANCE_NAME", infer.name},
        {"COMET_INSTANCE_ROLE", "infer"},
        {"COMET_INSTANCE_SUBROLE",
         infer_index == 0 && infer_count > 1 ? "aggregator" : "infer"},
        {"COMET_NODE_NAME", infer.node_name},
        {"COMET_INFER_RUNTIME_BACKEND", DefaultInferRuntimeBackend()},
        {"COMET_CONTROLLER_URL", "http://controller.internal:18080"},
        {"COMET_CONTROL_ROOT", state_.control_root},
        {"COMET_INFER_RUNTIME_CONFIG",
         InferRuntimeConfigControlPath(state_.control_root, infer.name)},
        {"COMET_INFERENCE_PORT", std::to_string(BuildInferApiPort(infer_index))},
        {"COMET_GATEWAY_PORT", std::to_string(BuildInferGatewayPort(infer_index))},
        {"COMET_LLAMA_PORT", std::to_string(BuildInferLlamaPort(infer_index))},
        {"COMET_SHARED_DISK_PATH", "/comet/shared"},
    };
    if (!infer.private_disk_name.empty()) {
      infer.environment["COMET_PRIVATE_DISK_PATH"] = "/comet/private";
    }
    if (infer_json_.contains("env") && infer_json_.at("env").is_object()) {
      const auto custom_env = infer_json_.at("env").get<std::map<std::string, std::string>>();
      for (const auto& [key, value] : custom_env) {
        infer.environment[key] = value;
      }
    }
    infer.labels = {
        {"comet.plane", state_.plane_name},
        {"comet.role", "infer"},
        {"comet.subrole", infer_index == 0 && infer_count > 1 ? "aggregator" : "infer"},
        {"comet.node", infer.node_name},
    };
    if (infer_json_.contains("publish") && infer_json_.at("publish").is_array()) {
      for (const auto& port_json : infer_json_.at("publish")) {
        infer.published_ports.push_back(PublishedPort{
            port_json.value("host_ip", std::string("127.0.0.1")),
            port_json.value("host_port", 0),
            port_json.value("container_port", 0),
        });
      }
    }
    infer_names_.push_back(infer.name);
    rendered_infers.push_back(std::move(infer));
  }

  if (rendered_infers.size() > 1 &&
      rendered_infers.front().environment.count("COMET_REPLICA_UPSTREAMS") == 0) {
    rendered_infers.front().environment["COMET_REPLICA_UPSTREAMS"] =
        BuildReplicaUpstreams(rendered_infers);
  }

  for (const auto& infer : rendered_infers) {
    state_.instances.push_back(infer);

    if (!infer.private_disk_name.empty()) {
      DiskSpec infer_private_disk;
      infer_private_disk.name = infer.private_disk_name;
      infer_private_disk.kind = DiskKind::InferPrivate;
      infer_private_disk.plane_name = state_.plane_name;
      infer_private_disk.owner_name = infer.name;
      infer_private_disk.node_name = infer.node_name;
      infer_private_disk.host_path = BuildInstancePrivateHostPath(infer.name);
      infer_private_disk.container_path =
          ExtractPrivateMountPath(infer_json_, "/comet/private", "volumes");
      infer_private_disk.size_gb = infer.private_disk_size_gb;
      state_.disks.push_back(std::move(infer_private_disk));
    }
  }
}

void DesiredStateV2Renderer::RenderWorkerInstances() {
  const int total_workers = WorkerCount();

  for (int worker_index = 0; worker_index < total_workers; ++worker_index) {
    InstanceSpec worker;
    worker.name = BuildWorkerName(worker_index, total_workers);
    worker.role = InstanceRole::Worker;
    worker.plane_name = state_.plane_name;
    worker.node_name = ResolveWorkerNodeName(worker_index);
    worker.gpu_device = ResolveWorkerGpuDevice(worker_index);
    worker.image = worker_json_.value("image", std::string("comet/worker-runtime:dev"));
    worker.command =
        BuildCommandFromStartSpec(worker_json_.value("start", nlohmann::json::object()),
                                  "/runtime/bin/comet-workerd");
    worker.private_disk_name = worker.name + "-private";
    worker.shared_disk_name = state_.plane_shared_disk_name;
    worker.placement_mode =
        ParsePlacementMode(worker_resources_json_.value("placement_mode", std::string("auto")));
    worker.share_mode =
        ParseGpuShareMode(worker_resources_json_.value("share_mode", std::string("exclusive")));
    worker.gpu_fraction = worker_resources_json_.value("gpu_fraction", 1.0);
    worker.memory_cap_mb = worker_resources_json_.contains("memory_cap_mb")
                               ? std::optional<int>(worker_resources_json_.at("memory_cap_mb").get<int>())
                               : std::nullopt;
    worker.private_disk_size_gb =
        ExtractPrivateDiskSizeGb(worker_json_, kDefaultWorkerPrivateDiskSizeGb, "volumes");
    worker.environment = {
        {"COMET_PLANE_NAME", state_.plane_name},
        {"COMET_INSTANCE_NAME", worker.name},
        {"COMET_INSTANCE_ROLE", "worker"},
        {"COMET_NODE_NAME", worker.node_name},
        {"COMET_WORKER_BOOT_MODE", DefaultWorkerBootMode()},
        {"COMET_INFER_INSTANCE_NAME", InferInstanceNameForWorker(worker_index)},
        {"COMET_CONTROL_ROOT", state_.control_root},
        {"COMET_DISTRIBUTED_BACKEND", state_.inference.distributed_backend},
        {"COMET_SHARED_DISK_PATH", "/comet/shared"},
        {"COMET_PRIVATE_DISK_PATH", "/comet/private"},
        {"COMET_WORKER_RUNTIME_STATUS_PATH", "/comet/private/worker-runtime-status.json"},
    };
    if (state_.inference.distributed_backend == "llama_rpc") {
      const int rpc_port = StableLlamaRpcWorkerPort(state_.plane_name, worker.name);
      worker.environment["COMET_WORKER_RPC_PORT"] = std::to_string(rpc_port);
      worker.environment["COMET_WORKER_RPC_HOST"] = "0.0.0.0";
      worker.environment["COMET_WORKER_RPC_ENDPOINT"] =
          worker.name + ":" + std::to_string(rpc_port);
    }
    if (worker_json_.contains("env") && worker_json_.at("env").is_object()) {
      const auto custom_env = worker_json_.at("env").get<std::map<std::string, std::string>>();
      for (const auto& [key, value] : custom_env) {
        worker.environment[key] = value;
      }
    }
    worker.labels = {
        {"comet.plane", state_.plane_name},
        {"comet.role", "worker"},
        {"comet.node", worker.node_name},
    };
    if (worker_json_.contains("publish") && worker_json_.at("publish").is_array()) {
      for (const auto& port_json : worker_json_.at("publish")) {
        worker.published_ports.push_back(PublishedPort{
            port_json.value("host_ip", std::string("127.0.0.1")),
            port_json.value("host_port", 0),
            port_json.value("container_port", 0),
        });
      }
    }
    state_.instances.push_back(worker);

    if (state_.inference.distributed_backend == "llama_rpc") {
      WorkerGroupMemberSpec member;
      member.name = worker.name;
      member.infer_instance_name = InferInstanceNameForWorker(worker_index);
      member.node_name = worker.node_name;
      member.gpu_device = worker.gpu_device.value_or("");
      member.rpc_port = std::stoi(worker.environment.at("COMET_WORKER_RPC_PORT"));
      member.rank = worker_index;
      member.replica_group_id = member.infer_instance_name.empty() ? worker.name : member.infer_instance_name;
      const int replica_groups = std::max(1, InferReplicaCount());
      const int workers_per_replica = std::max(1, total_workers / replica_groups);
      member.replica_index = std::min(replica_groups - 1, worker_index / workers_per_replica);
      member.replica_size = workers_per_replica;
      member.replica_leader = (worker_index % workers_per_replica) == 0;
      member.data_parallel_rank = member.replica_index;
      member.data_parallel_size = replica_groups;
      member.data_parallel_size_local = 1;
      member.data_parallel_start_rank = member.replica_index;
      member.data_parallel_api_endpoint = false;
      member.gpu_fraction = worker.gpu_fraction;
      member.share_mode = worker.share_mode;
      member.priority = worker.priority;
      member.preemptible = worker.preemptible;
      member.memory_cap_mb = worker.memory_cap_mb;
      member.enabled = true;
      member.leader = member.replica_leader && member.replica_index == 0;
      state_.worker_group.members.push_back(std::move(member));
    }

    DiskSpec worker_private_disk;
    worker_private_disk.name = worker.private_disk_name;
    worker_private_disk.kind = DiskKind::WorkerPrivate;
    worker_private_disk.plane_name = state_.plane_name;
    worker_private_disk.owner_name = worker.name;
    worker_private_disk.node_name = worker.node_name;
    worker_private_disk.host_path = BuildInstancePrivateHostPath(worker.name);
    worker_private_disk.container_path =
        ExtractPrivateMountPath(worker_json_, "/comet/private", "volumes");
    worker_private_disk.size_gb = worker.private_disk_size_gb;
    state_.disks.push_back(std::move(worker_private_disk));
  }
}

void DesiredStateV2Renderer::RenderAppInstance() {
  if (!value_.contains("app") || !value_.at("app").is_object() ||
      !value_.at("app").value("enabled", true)) {
    return;
  }

  InstanceSpec app;
  app.name = BuildAppInstanceName();
  app.role = InstanceRole::App;
  app.plane_name = state_.plane_name;
  app.node_name = ResolveAppNodeName();
  app.image = app_json_.value("image", std::string{});
  if (app_json_.contains("start") && app_json_.at("start").is_object()) {
    const auto& start = app_json_.at("start");
    const auto start_type = start.value("type", std::string("command"));
    if (start_type == "script") {
      app.command = BuildAppCommandFromScriptRef(start.value("script_ref", std::string{}));
    } else {
      app.command = start.value("command", std::string{});
    }
  }
  app.private_disk_name = app.name + "-private";
  app.shared_disk_name = state_.plane_shared_disk_name;
  if (!infer_names_.empty()) {
    app.depends_on.push_back(infer_names_.front());
  }
  if (app_json_.contains("env") && app_json_.at("env").is_object()) {
    app.environment = app_json_.at("env").get<std::map<std::string, std::string>>();
  }
  app.labels = {
      {"comet.plane", state_.plane_name},
      {"comet.role", "app"},
      {"comet.node", app.node_name},
  };
  if (app_json_.contains("publish") && app_json_.at("publish").is_array()) {
    for (const auto& port_json : app_json_.at("publish")) {
      app.published_ports.push_back(PublishedPort{
          port_json.value("host_ip", std::string("127.0.0.1")),
          port_json.value("host_port", 0),
          port_json.value("container_port", 0),
      });
    }
  }

  const int app_private_disk_size_gb =
      ExtractPrivateDiskSizeGb(app_json_, kDefaultAppPrivateDiskSizeGb, "volumes");
  const std::string app_private_mount_path =
      ExtractPrivateMountPath(app_json_, "/comet/private", "volumes");
  if (app_json_.contains("volumes") && app_json_.at("volumes").is_array() &&
      app_json_.at("volumes").size() > 1) {
    throw std::runtime_error("desired-state v2 currently supports at most one app volume");
  }
  app.private_disk_size_gb = app_private_disk_size_gb;
  state_.instances.push_back(app);

  DiskSpec app_private_disk;
  app_private_disk.name = app.private_disk_name;
  app_private_disk.kind = DiskKind::AppPrivate;
  app_private_disk.plane_name = state_.plane_name;
  app_private_disk.owner_name = app.name;
  app_private_disk.node_name = app.node_name;
  app_private_disk.host_path = BuildInstancePrivateHostPath(app.name);
  app_private_disk.container_path = app_private_mount_path;
  app_private_disk.size_gb = app_private_disk_size_gb;
  state_.disks.push_back(std::move(app_private_disk));
}

void DesiredStateV2Renderer::RenderSkillsInstance() {
  if (!skills_json_.value("enabled", false)) {
    return;
  }

  InstanceSpec skills;
  skills.name = BuildSkillsInstanceName();
  skills.role = InstanceRole::Skills;
  skills.plane_name = state_.plane_name;
  if (skills_json_.contains("node") && skills_json_.at("node").is_string()) {
    skills.node_name = RequireNode(skills_json_.at("node").get<std::string>(), "skills").name;
  } else {
    skills.node_name = ResolveAppNodeName();
  }
  skills.image = skills_json_.value("image", std::string("comet/skills-runtime:dev"));
  skills.command =
      BuildCommandFromStartSpec(skills_json_.value("start", nlohmann::json::object()),
                                "/runtime/bin/comet-skillsd");
  skills.private_disk_name = skills.name + "-private";
  skills.shared_disk_name =
      InstanceNeedsSharedDiskMount(skills.role) ? state_.plane_shared_disk_name : "";
  skills.private_disk_size_gb =
      ExtractPrivateDiskSizeGb(skills_json_, kDefaultSkillsPrivateDiskSizeGb, "volumes");
  skills.environment = {
      {"COMET_PLANE_NAME", state_.plane_name},
      {"COMET_INSTANCE_NAME", skills.name},
      {"COMET_INSTANCE_ROLE", "skills"},
      {"COMET_NODE_NAME", skills.node_name},
      {"COMET_PRIVATE_DISK_PATH", "/comet/private"},
      {"COMET_SKILLS_PORT", std::to_string(kSkillsContainerPort)},
      {"COMET_SKILLS_DB_PATH", "/comet/private/skills.sqlite"},
      {"COMET_SKILLS_RUNTIME_STATUS_PATH", "/comet/private/skills-runtime-status.json"},
      {"COMET_CONTROLLER_URL", "http://controller.internal:18080"},
      {"COMET_CONTROL_ROOT", state_.control_root},
  };
  if (skills_json_.contains("env") && skills_json_.at("env").is_object()) {
    const auto custom_env = skills_json_.at("env").get<std::map<std::string, std::string>>();
    for (const auto& [key, value] : custom_env) {
      skills.environment[key] = value;
    }
  }
  skills.labels = {
      {"comet.plane", state_.plane_name},
      {"comet.role", "skills"},
      {"comet.node", skills.node_name},
  };
  if (skills_json_.contains("publish") && skills_json_.at("publish").is_array()) {
    for (const auto& port_json : skills_json_.at("publish")) {
      skills.published_ports.push_back(PublishedPort{
          port_json.value("host_ip", std::string("127.0.0.1")),
          port_json.value("host_port", 0),
          port_json.value("container_port", 0),
      });
    }
  }
  if (skills.published_ports.empty()) {
    skills.published_ports.push_back(PublishedPort{
        "127.0.0.1",
        BuildSkillsHostPort(),
        kSkillsContainerPort,
    });
  }
  state_.instances.push_back(skills);

  DiskSpec skills_private_disk;
  skills_private_disk.name = skills.private_disk_name;
  skills_private_disk.kind = DiskKind::SkillsPrivate;
  skills_private_disk.plane_name = state_.plane_name;
  skills_private_disk.owner_name = skills.name;
  skills_private_disk.node_name = skills.node_name;
  skills_private_disk.host_path = BuildInstancePrivateHostPath(skills.name);
  skills_private_disk.container_path =
      ExtractPrivateMountPath(skills_json_, "/comet/private", "volumes");
  skills_private_disk.size_gb = skills.private_disk_size_gb;
  state_.disks.push_back(std::move(skills_private_disk));
}

void DesiredStateV2Renderer::RenderWebGatewayInstance() {
  if (!browsing_json_.value("enabled", false)) {
    return;
  }

  InstanceSpec browsing;
  browsing.name = BuildWebGatewayInstanceName();
  browsing.role = InstanceRole::Browsing;
  browsing.plane_name = state_.plane_name;
  if (browsing_json_.contains("node") && browsing_json_.at("node").is_string()) {
    browsing.node_name =
        RequireNode(browsing_json_.at("node").get<std::string>(), "webgateway").name;
  } else {
    browsing.node_name = ResolveAppNodeName();
  }
  browsing.image =
      browsing_json_.value("image", std::string("comet/webgateway-runtime:dev"));
  browsing.command =
      BuildCommandFromStartSpec(browsing_json_.value("start", nlohmann::json::object()),
                                "/runtime/bin/comet-webgatewayd");
  browsing.private_disk_name = browsing.name + "-private";
  browsing.shared_disk_name =
      InstanceNeedsSharedDiskMount(browsing.role) ? state_.plane_shared_disk_name : "";
  browsing.private_disk_size_gb =
      ExtractPrivateDiskSizeGb(browsing_json_, kDefaultWebGatewayPrivateDiskSizeGb, "storage");
  browsing.environment = {
      {"COMET_PLANE_NAME", state_.plane_name},
      {"COMET_INSTANCE_NAME", browsing.name},
      {"COMET_INSTANCE_ROLE", "webgateway"},
      {"COMET_NODE_NAME", browsing.node_name},
      {"COMET_PRIVATE_DISK_PATH", "/comet/private"},
      {"COMET_WEBGATEWAY_PORT", std::to_string(kWebGatewayContainerPort)},
      {"COMET_WEBGATEWAY_RUNTIME_STATUS_PATH", "/comet/private/webgateway-runtime-status.json"},
      {"COMET_WEBGATEWAY_STATE_ROOT", "/comet/private/sessions"},
      {"COMET_WEBGATEWAY_POLICY_JSON",
       browsing_json_.contains("policy") && browsing_json_.at("policy").is_object()
           ? browsing_json_.at("policy").dump()
           : nlohmann::json::object().dump()},
      {"COMET_CONTROLLER_URL", "http://controller.internal:18080"},
      {"COMET_CONTROL_ROOT", state_.control_root},
  };
  if (browsing_json_.contains("env") && browsing_json_.at("env").is_object()) {
    const auto custom_env = browsing_json_.at("env").get<std::map<std::string, std::string>>();
    for (const auto& [key, value] : custom_env) {
      browsing.environment[key] = value;
    }
  }
  browsing.labels = {
      {"comet.plane", state_.plane_name},
      {"comet.role", "webgateway"},
      {"comet.node", browsing.node_name},
  };
  if (browsing_json_.contains("publish") && browsing_json_.at("publish").is_array()) {
    for (const auto& port_json : browsing_json_.at("publish")) {
      browsing.published_ports.push_back(PublishedPort{
          port_json.value("host_ip", std::string("127.0.0.1")),
          port_json.value("host_port", 0),
          port_json.value("container_port", 0),
      });
    }
  }
  if (browsing.published_ports.empty()) {
    browsing.published_ports.push_back(PublishedPort{
        "127.0.0.1",
        BuildWebGatewayHostPort(),
        kWebGatewayContainerPort,
    });
  }
  state_.instances.push_back(browsing);

  DiskSpec browsing_private_disk;
  browsing_private_disk.name = browsing.private_disk_name;
  browsing_private_disk.kind = DiskKind::BrowsingPrivate;
  browsing_private_disk.plane_name = state_.plane_name;
  browsing_private_disk.owner_name = browsing.name;
  browsing_private_disk.node_name = browsing.node_name;
  browsing_private_disk.host_path = BuildInstancePrivateHostPath(browsing.name);
  browsing_private_disk.container_path =
      ExtractPrivateMountPath(browsing_json_, "/comet/private", "storage");
  browsing_private_disk.size_gb = browsing.private_disk_size_gb;
  state_.disks.push_back(std::move(browsing_private_disk));
}

bool DesiredStateV2Renderer::InferEnabled() const {
  return infer_json_.value(
      "enabled",
      state_.plane_mode == PlaneMode::Llm || state_.inference.runtime_engine == "llama.cpp");
}

int DesiredStateV2Renderer::InferReplicaCount() const {
  if (!(state_.inference.runtime_engine == "llama.cpp" &&
        state_.inference.distributed_backend == "llama_rpc")) {
    return 1;
  }
  return std::max(1, infer_json_.value("replicas", 1));
}

int DesiredStateV2Renderer::WorkerCount() const {
  return value_.at("runtime").value("workers", 1);
}

int DesiredStateV2Renderer::ExpectedWorkers() const {
  return std::max(1, WorkerCount());
}

std::string DesiredStateV2Renderer::ResolveInferNodeName() const {
  return ResolveInferNodeName(0);
}

std::string DesiredStateV2Renderer::ResolveInferNodeName(int infer_index) const {
  if (infer_index == 0 && infer_json_.contains("node") && infer_json_.at("node").is_string()) {
    return RequireNode(infer_json_.at("node").get<std::string>(), "infer").name;
  }
  if (InferReplicaCount() > 1 && infer_index > 0) {
    const int replica_index = infer_index - 1;
    const int workers_per_replica = std::max(1, WorkerCount() / InferReplicaCount());
    const int worker_index = std::min(WorkerCount() - 1, replica_index * workers_per_replica);
    return ResolveWorkerNodeName(worker_index);
  }
  for (const auto& node : state_.nodes) {
    if (node.execution_mode != HostExecutionMode::WorkerOnly) {
      return node.name;
    }
  }
  return DefaultNodeName();
}

std::string DesiredStateV2Renderer::ResolveAppNodeName() const {
  if (app_json_.contains("node") && app_json_.at("node").is_string()) {
    return RequireNode(app_json_.at("node").get<std::string>(), "app").name;
  }
  return ResolveInferNodeName();
}

std::string DesiredStateV2Renderer::ResolveWorkerNodeName(int worker_index) const {
  if (worker_json_.contains("assignments") && worker_json_.at("assignments").is_array() &&
      worker_index < static_cast<int>(worker_json_.at("assignments").size())) {
    const auto& assignment = worker_json_.at("assignments").at(worker_index);
    if (assignment.contains("node") && assignment.at("node").is_string()) {
      return RequireNode(assignment.at("node").get<std::string>(), "worker assignment").name;
    }
  }
  if (worker_json_.contains("node") && worker_json_.at("node").is_string()) {
    return RequireNode(worker_json_.at("node").get<std::string>(), "worker").name;
  }
  return DefaultNodeName();
}

std::optional<std::string> DesiredStateV2Renderer::ResolveWorkerGpuDevice(int worker_index) const {
  if (worker_json_.contains("assignments") && worker_json_.at("assignments").is_array() &&
      worker_index < static_cast<int>(worker_json_.at("assignments").size())) {
    const auto& assignment = worker_json_.at("assignments").at(worker_index);
    if (assignment.contains("gpu_device") && assignment.at("gpu_device").is_string()) {
      return assignment.at("gpu_device").get<std::string>();
    }
  }
  if (worker_json_.contains("gpu_device") && worker_json_.at("gpu_device").is_string()) {
    return worker_json_.at("gpu_device").get<std::string>();
  }
  return std::nullopt;
}

std::string DesiredStateV2Renderer::DefaultNodeName() const {
  if (state_.nodes.empty()) {
    throw std::runtime_error("desired-state v2 renderer requires at least one node");
  }
  return state_.nodes.front().name;
}

const NodeInventory& DesiredStateV2Renderer::RequireNode(
    const std::string& node_name,
    const char* context) const {
  for (const auto& node : state_.nodes) {
    if (node.name == node_name) {
      return node;
    }
  }
  throw std::runtime_error(
      std::string("desired-state v2 ") + context + " references unknown node '" + node_name +
      "'");
}

std::string DesiredStateV2Renderer::SharedDiskNodeName() const {
  if (InferEnabled()) {
    return ResolveInferNodeName();
  }
  return DefaultNodeName();
}

std::string DesiredStateV2Renderer::StripBundleRefPrefix(const std::string& value) const {
  if (value.rfind(kDesiredStateV2BundlePrefix.data(), 0) == 0) {
    return value.substr(kDesiredStateV2BundlePrefix.size());
  }
  return value;
}

std::string DesiredStateV2Renderer::BuildWorkerName(int index, int total_workers) const {
  if (total_workers <= 1) {
    return "worker-" + state_.plane_name;
  }
  return "worker-" + state_.plane_name + "-" + static_cast<char>('a' + index);
}

std::string DesiredStateV2Renderer::BuildPlaneSharedDiskName() const {
  return "plane-" + state_.plane_name + "-shared";
}

std::string DesiredStateV2Renderer::BuildInferInstanceName(int infer_index) const {
  if (infer_index == 0) {
    return "infer-" + state_.plane_name;
  }
  return "infer-" + state_.plane_name + "-" + static_cast<char>('a' + (infer_index - 1));
}

int DesiredStateV2Renderer::BuildInferApiPort(int infer_index) const {
  return state_.inference.api_port + infer_index;
}

int DesiredStateV2Renderer::BuildInferGatewayPort(int infer_index) const {
  return state_.gateway.listen_port + infer_index;
}

int DesiredStateV2Renderer::BuildInferLlamaPort(int infer_index) const {
  return state_.inference.llama_port + infer_index;
}

std::string DesiredStateV2Renderer::BuildReplicaUpstreams(
    const std::vector<InstanceSpec>& infer_instances) const {
  if (infer_instances.size() <= 1) {
    return {};
  }
  const std::string primary_node = infer_instances.front().node_name;
  std::vector<std::string> upstreams;
  upstreams.reserve(infer_instances.size() - 1);
  for (std::size_t index = 1; index < infer_instances.size(); ++index) {
    const auto& infer = infer_instances[index];
    const auto port_it = infer.environment.find("COMET_GATEWAY_PORT");
    const int port = port_it == infer.environment.end() ? state_.gateway.listen_port
                                                        : std::stoi(port_it->second);
    const std::string host =
        infer.node_name == primary_node ? infer.name : infer.node_name;
    upstreams.push_back("http://" + host + ":" + std::to_string(port));
  }
  std::string result;
  for (std::size_t index = 0; index < upstreams.size(); ++index) {
    if (index > 0) {
      result += ",";
    }
    result += upstreams[index];
  }
  return result;
}

std::string DesiredStateV2Renderer::InferInstanceNameForWorker(int worker_index) const {
  if (infer_names_.size() <= 1) {
    return BuildInferInstanceName();
  }
  const int workers_per_replica = std::max(1, WorkerCount() / InferReplicaCount());
  const int replica_index = std::min(InferReplicaCount() - 1, worker_index / workers_per_replica);
  const int infer_index = std::min(static_cast<int>(infer_names_.size()) - 1, replica_index + 1);
  return infer_names_.at(infer_index);
}

std::string DesiredStateV2Renderer::BuildAppInstanceName() const {
  return "app-" + state_.plane_name;
}

std::string DesiredStateV2Renderer::BuildSkillsInstanceName() const {
  return "skills-" + state_.plane_name;
}

std::string DesiredStateV2Renderer::BuildWebGatewayInstanceName() const {
  return "webgateway-" + state_.plane_name;
}

std::string DesiredStateV2Renderer::BuildPlaneSharedHostPath() const {
  return "/var/lib/comet/disks/planes/" + state_.plane_name + "/shared";
}

std::string DesiredStateV2Renderer::BuildInstancePrivateHostPath(
    const std::string& instance_name) const {
  return "/var/lib/comet/disks/instances/" + instance_name + "/private";
}

std::string DesiredStateV2Renderer::BuildAppCommandFromScriptRef(
    const std::string& script_ref) const {
  if (script_ref.empty()) {
    return {};
  }
  if (script_ref.front() == '/') {
    return script_ref;
  }
  const std::string stripped = StripBundleRefPrefix(script_ref);
  const auto slash = stripped.find_last_of('/');
  const std::string filename =
      slash == std::string::npos ? stripped : stripped.substr(slash + 1);
  return "/app/" + filename;
}

std::string DesiredStateV2Renderer::BuildCommandFromStartSpec(
    const nlohmann::json& start,
    const std::string& default_command) const {
  if (!start.is_object()) {
    return default_command;
  }
  const auto start_type = start.value("type", std::string("command"));
  if (start_type == "script") {
    return BuildAppCommandFromScriptRef(start.value("script_ref", std::string{}));
  }
  const auto command = start.value("command", std::string{});
  return command.empty() ? default_command : command;
}

int DesiredStateV2Renderer::BuildSkillsHostPort() const {
  const uint32_t offset =
      StablePortHash(state_.plane_name + ":" + BuildSkillsInstanceName()) % kSkillsPublishedPortSpan;
  return kSkillsPublishedPortBase + static_cast<int>(offset);
}

int DesiredStateV2Renderer::BuildWebGatewayHostPort() const {
  const uint32_t offset =
      StablePortHash(state_.plane_name + ":" + BuildWebGatewayInstanceName()) %
      kWebGatewayPublishedPortSpan;
  return kWebGatewayPublishedPortBase + static_cast<int>(offset);
}

std::string DesiredStateV2Renderer::DefaultInferRuntimeBackend() const {
  if (state_.inference.runtime_engine == "llama.cpp" &&
      state_.inference.distributed_backend == "llama_rpc") {
    return "llama-rpc-head";
  }
  return "auto";
}

std::string DesiredStateV2Renderer::DefaultWorkerBootMode() const {
  if (state_.inference.runtime_engine == "llama.cpp" &&
      state_.inference.distributed_backend == "llama_rpc") {
    return "llama-rpc";
  }
  return "llama-idle";
}

void DesiredStateV2Renderer::NormalizeInferenceSettings() {
  auto& settings = state_.inference;
  if (settings.worker_group_id.empty()) {
    settings.worker_group_id = "default-worker-group";
  }
  if (settings.distributed_backend.empty()) {
    settings.distributed_backend =
        settings.runtime_engine == "llama.cpp" ? "llama_rpc" : "local";
  }
  if (settings.data_parallel_mode.empty()) {
    settings.data_parallel_mode = kDataParallelModeOff;
  }
  if (settings.data_parallel_lb_mode.empty()) {
    settings.data_parallel_lb_mode = kDataParallelLbModeExternal;
  }
  if (settings.worker_selection_policy.empty()) {
    settings.worker_selection_policy = "prefer-free-then-share";
  }
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
}

int DesiredStateV2Renderer::ExtractPrivateDiskSizeGb(
    const nlohmann::json& service_json,
    int default_size_gb,
    const std::string& legacy_volume_key) const {
  if (service_json.contains("storage") && service_json.at("storage").is_object()) {
    return service_json.at("storage").value("size_gb", default_size_gb);
  }
  if (service_json.contains(legacy_volume_key) && service_json.at(legacy_volume_key).is_array() &&
      !service_json.at(legacy_volume_key).empty()) {
    return service_json.at(legacy_volume_key).at(0).value("size_gb", default_size_gb);
  }
  return default_size_gb;
}

std::string DesiredStateV2Renderer::ExtractPrivateMountPath(
    const nlohmann::json& service_json,
    const std::string& default_mount_path,
    const std::string& legacy_volume_key) const {
  if (service_json.contains("storage") && service_json.at("storage").is_object()) {
    return service_json.at("storage").value("mount_path", default_mount_path);
  }
  if (service_json.contains(legacy_volume_key) && service_json.at(legacy_volume_key).is_array() &&
      !service_json.at(legacy_volume_key).empty()) {
    return service_json.at(legacy_volume_key).at(0).value("mount_path", default_mount_path);
  }
  return default_mount_path;
}

InteractionSettings::CompletionPolicy DesiredStateV2Renderer::DefaultCompletionPolicy() const {
  InteractionSettings::CompletionPolicy policy;
  policy.response_mode = "normal";
  policy.max_tokens = 512;
  policy.max_continuations = 0;
  policy.max_total_completion_tokens = 512;
  policy.max_elapsed_time_ms = 30000;
  return policy;
}

InteractionSettings::CompletionPolicy DesiredStateV2Renderer::DefaultLongCompletionPolicy() const {
  InteractionSettings::CompletionPolicy policy;
  policy.response_mode = "very_long";
  policy.max_tokens = 1024;
  policy.max_continuations = 6;
  policy.max_total_completion_tokens = 6144;
  policy.max_elapsed_time_ms = 120000;
  policy.semantic_goal =
      "complete the requested artifact fully before emitting [[TASK_COMPLETE]]";
  return policy;
}

}  // namespace comet
