#include "naim/state/desired_state_v2_projector.h"

#include <algorithm>
#include <set>
#include <stdexcept>
#include <string>
#include <utility>

#include "naim/state/desired_state_placement_resolver.h"
#include "naim/state/desired_state_v2_projector_support.h"
#include "naim/state/worker_group_topology.h"

namespace naim {

namespace {

using ProjectorSupport = DesiredStateV2ProjectorSupport;

constexpr int kDefaultSharedDiskSizeGb = ProjectorSupport::kDefaultSharedDiskSizeGb;
constexpr std::string_view kDefaultInferImage = ProjectorSupport::kDefaultInferImage;
constexpr std::string_view kDefaultSkillsImage = ProjectorSupport::kDefaultSkillsImage;
constexpr std::string_view kDefaultWebGatewayImage =
    ProjectorSupport::kDefaultWebGatewayImage;
constexpr std::string_view kDefaultInferCommand = ProjectorSupport::kDefaultInferCommand;
constexpr std::string_view kDefaultWorkerCommand = ProjectorSupport::kDefaultWorkerCommand;
constexpr std::string_view kDefaultSkillsCommand = ProjectorSupport::kDefaultSkillsCommand;
constexpr std::string_view kDefaultWebGatewayCommand =
    ProjectorSupport::kDefaultWebGatewayCommand;

}  // namespace

nlohmann::json DesiredStateV2Projector::Project(const DesiredState& state) {
  return DesiredStateV2Projector(state).ProjectJson();
}

DesiredStateV2Projector::DesiredStateV2Projector(const DesiredState& state) : state_(state) {}

nlohmann::json DesiredStateV2Projector::ProjectJson() {
  value_ = nlohmann::json::object();
  value_["version"] = 2;
  CollectInstancesAndDisks();
  ProjectIdentity();
  ProjectPlacement();
  ProjectFeatures();
  ProjectHooks();
  ProjectModel();
  ProjectTopology();
  ProjectInteraction();
  ProjectRuntime();
  ProjectNetwork();
  ProjectInfer();
  ProjectWorker();
  ProjectApp();
  ProjectSkills();
  ProjectBrowsing();
  ProjectKnowledge();
  ProjectResources();
  return value_;
}

void DesiredStateV2Projector::CollectInstancesAndDisks() {
  infer_instance_ = FindInstance(InstanceRole::Infer);
  app_instance_ = FindInstance(InstanceRole::App);
  app_instances_ = FindInstances(InstanceRole::App);
  skills_instance_ = FindInstance(InstanceRole::Skills);
  browsing_instance_ = FindInstance(InstanceRole::Browsing);
  worker_instances_ = FindWorkerInstances();
  shared_disk_ = FindSharedDisk();
}

void DesiredStateV2Projector::ProjectIdentity() {
  value_["plane_name"] = state_.plane_name;
  value_["plane_mode"] = ToString(state_.plane_mode);
  if (state_.protected_plane) {
    value_["protected"] = true;
  }
}

void DesiredStateV2Projector::ProjectPlacement() {
  nlohmann::json placement = nlohmann::json::object();
  if (state_.placement_target.has_value() && !state_.placement_target->empty()) {
    constexpr std::string_view kNodePrefix = "node:";
    if (state_.placement_target->rfind(kNodePrefix, 0) == 0) {
      placement["execution_node"] = state_.placement_target->substr(kNodePrefix.size());
    }
  } else if (!state_.nodes.empty()) {
    placement["execution_node"] = DefaultNodeName();
  }
  if (state_.app_host.has_value()) {
    nlohmann::json app_host = {
        {"address", state_.app_host->address},
    };
    if (state_.app_host->ssh_key_path.has_value() && !state_.app_host->ssh_key_path->empty()) {
      app_host["ssh_key_path"] = *state_.app_host->ssh_key_path;
    }
    if (state_.app_host->username.has_value() && !state_.app_host->username->empty()) {
      app_host["username"] = *state_.app_host->username;
    }
    if (state_.app_host->password.has_value() && !state_.app_host->password->empty()) {
      app_host["password"] = *state_.app_host->password;
    }
    placement["app_host"] = std::move(app_host);
  }
  if (!placement.empty()) {
    value_["placement"] = std::move(placement);
  }
}

void DesiredStateV2Projector::ProjectFeatures() {
  if (!state_.turboquant.has_value() && !state_.context_compression.has_value()) {
    return;
  }
  nlohmann::json features = nlohmann::json::object();
  if (state_.turboquant.has_value()) {
    nlohmann::json turboquant = {
        {"enabled", state_.turboquant->enabled},
    };
    if (state_.turboquant->cache_type_k.has_value()) {
      turboquant["cache_type_k"] = *state_.turboquant->cache_type_k;
    }
    if (state_.turboquant->cache_type_v.has_value()) {
      turboquant["cache_type_v"] = *state_.turboquant->cache_type_v;
    }
    features["turboquant"] = std::move(turboquant);
  }
  if (state_.context_compression.has_value()) {
    features["context_compression"] = {
        {"enabled", state_.context_compression->enabled},
        {"mode", state_.context_compression->mode},
        {"target", state_.context_compression->target},
        {"memory_priority", state_.context_compression->memory_priority},
    };
  }
  value_["features"] = std::move(features);
}

void DesiredStateV2Projector::ProjectKnowledge() {
  if (!state_.knowledge.has_value() || !state_.knowledge->enabled) {
    return;
  }
  const auto& knowledge = *state_.knowledge;
  value_["knowledge"] = {
      {"enabled", knowledge.enabled},
      {"service_id", knowledge.service_id.empty() ? std::string("kv_default")
                                                   : knowledge.service_id},
      {"selection_mode",
       knowledge.selection_mode.empty() ? std::string("latest")
                                        : knowledge.selection_mode},
      {"selected_knowledge_ids", knowledge.selected_knowledge_ids},
      {"context_policy",
       {
           {"include_graph", knowledge.context_policy.include_graph},
           {"max_graph_depth", knowledge.context_policy.max_graph_depth},
           {"token_budget", knowledge.context_policy.token_budget},
       }},
  };
}

void DesiredStateV2Projector::ProjectHooks() {
  if (!state_.post_deploy_script.has_value() || state_.post_deploy_script->empty()) {
    return;
  }
  value_["hooks"] = {
      {"post_deploy_script", AddBundlePrefixIfRelative(*state_.post_deploy_script)},
  };
}

void DesiredStateV2Projector::ProjectModel() {
  if (!state_.bootstrap_model.has_value()) {
    return;
  }

  const auto& model = *state_.bootstrap_model;
  nlohmann::json model_json = nlohmann::json::object();
  model_json["source"] = ProjectModelSource();
  model_json["materialization"] = {
      {"mode", model.materialization_mode},
  };
  if (model.local_path.has_value() && !model.local_path->empty()) {
    model_json["materialization"]["local_path"] = *model.local_path;
  }
  if (model.source_node_name.has_value() && !model.source_node_name->empty()) {
    model_json["materialization"]["source_node_name"] = *model.source_node_name;
  }
  if (!model.source_paths.empty()) {
    model_json["materialization"]["source_paths"] = model.source_paths;
  }
  if (model.source_format.has_value() && !model.source_format->empty()) {
    model_json["materialization"]["source_format"] = *model.source_format;
  }
  if (model.source_quantization.has_value() && !model.source_quantization->empty()) {
    model_json["materialization"]["source_quantization"] = *model.source_quantization;
  }
  if (model.desired_output_format.has_value() && !model.desired_output_format->empty()) {
    model_json["materialization"]["desired_output_format"] = *model.desired_output_format;
  }
  if (model.quantization.has_value() && !model.quantization->empty()) {
    model_json["materialization"]["quantization"] = *model.quantization;
  }
  if (!model.keep_source) {
    model_json["materialization"]["keep_source"] = model.keep_source;
  }
  if (model.writeback_enabled) {
    model_json["materialization"]["writeback"] = {
        {"enabled", model.writeback_enabled},
        {"if_missing", model.writeback_if_missing},
    };
    if (model.writeback_target_node_name.has_value() &&
        !model.writeback_target_node_name->empty()) {
      model_json["materialization"]["writeback"]["target_node_name"] =
          *model.writeback_target_node_name;
    }
  }
  if (model.served_model_name.has_value() && !model.served_model_name->empty()) {
    model_json["served_model_name"] = *model.served_model_name;
  }
  if (model.target_filename.has_value() && !model.target_filename->empty()) {
    model_json["target_filename"] = *model.target_filename;
  }
  if (model.sha256.has_value() && !model.sha256->empty()) {
    model_json["sha256"] = *model.sha256;
  }
  value_["model"] = std::move(model_json);
}

void DesiredStateV2Projector::ProjectTopology() {
  if (!ShouldEmitTopology()) {
    return;
  }

  nlohmann::json nodes = nlohmann::json::array();
  for (const auto& node : state_.nodes) {
    nlohmann::json node_json = {
        {"name", node.name},
    };
    if (!node.platform.empty()) {
      node_json["platform"] = node.platform;
    }
    node_json["execution_mode"] = ToString(node.execution_mode);
    if (!node.gpu_memory_mb.empty()) {
      node_json["gpu_memory_mb"] = node.gpu_memory_mb;
    }
    nodes.push_back(std::move(node_json));
  }
  value_["topology"] = {{"nodes", std::move(nodes)}};
}

void DesiredStateV2Projector::ProjectInteraction() {
  if (!state_.interaction.has_value()) {
    return;
  }

  const auto& interaction = *state_.interaction;
  nlohmann::json interaction_json = nlohmann::json::object();
  if (interaction.image.has_value() && !interaction.image->empty()) {
    interaction_json["image"] = *interaction.image;
  }
  if (interaction.system_prompt.has_value() && !interaction.system_prompt->empty()) {
    interaction_json["system_prompt"] = *interaction.system_prompt;
  }
  interaction_json["thinking_enabled"] = interaction.thinking_enabled;
  if (interaction.default_temperature.has_value()) {
    interaction_json["default_temperature"] = *interaction.default_temperature;
  }
  if (interaction.default_top_p.has_value()) {
    interaction_json["default_top_p"] = *interaction.default_top_p;
  }
  if (!interaction.default_response_language.empty()) {
    interaction_json["default_response_language"] = interaction.default_response_language;
  }
  interaction_json["follow_user_language"] = interaction.follow_user_language;
  if (!interaction.supported_response_languages.empty()) {
    interaction_json["supported_response_languages"] =
        interaction.supported_response_languages;
  }
  if (!interaction_json.empty()) {
    value_["interaction"] = std::move(interaction_json);
  }
}

void DesiredStateV2Projector::ProjectRuntime() {
  nlohmann::json runtime = {
      {"engine", state_.inference.runtime_engine},
      {"workers", std::max(1, static_cast<int>(worker_instances_.size()))},
      {"distributed_backend", state_.inference.distributed_backend},
      {"max_model_len", state_.inference.max_model_len},
      {"llama_ctx_size", state_.inference.llama_ctx_size},
      {"max_num_seqs", state_.inference.max_num_seqs},
      {"gpu_memory_utilization", state_.inference.gpu_memory_utilization},
  };
  if (state_.inference.pipeline_parallel_size != 1) {
    runtime["pipeline_parallel_size"] = state_.inference.pipeline_parallel_size;
  }
  if (state_.inference.tensor_parallel_size != 1) {
    runtime["tensor_parallel_size"] = state_.inference.tensor_parallel_size;
  }
  if (state_.inference.enforce_eager) {
    runtime["enforce_eager"] = true;
  }
  value_["runtime"] = std::move(runtime);
}

void DesiredStateV2Projector::ProjectNetwork() {
  nlohmann::json network = {
      {"gateway_port", state_.gateway.listen_port},
      {"inference_port", state_.inference.api_port},
      {"server_name", state_.gateway.server_name},
  };
  if (state_.gateway.listen_host != "0.0.0.0") {
    network["listen_host"] = state_.gateway.listen_host;
  }
  if (state_.inference.rendezvous_port != 29500) {
    network["rendezvous_port"] = state_.inference.rendezvous_port;
  }
  value_["network"] = std::move(network);
}

void DesiredStateV2Projector::ProjectInfer() {
  if (infer_instance_ == nullptr) {
    return;
  }

  nlohmann::json infer = nlohmann::json::object();
  if (infer_instance_->image != kDefaultInferImage) {
    infer["image"] = infer_instance_->image;
  }
  const auto start =
      ProjectorSupport::ProjectServiceStart(*infer_instance_, std::string(kDefaultInferCommand));
  if (!start.is_null()) {
    infer["start"] = start;
  }
  const auto env = ProjectorSupport::ProjectCustomEnv(*infer_instance_, true);
  if (!env.empty()) {
    infer["env"] = env;
  }
  const auto publish = ProjectorSupport::ProjectPublishedPorts(*infer_instance_);
  if (!publish.empty()) {
    infer["publish"] = publish;
  }
  if (infer_instance_->node_name != DefaultNodeName()) {
    infer["node"] = infer_instance_->node_name;
  }
  const int replicas = InferReplicaCount();
  if (state_.plane_mode == PlaneMode::Llm && state_.inference.runtime_engine == "llama.cpp" &&
      state_.inference.distributed_backend == "llama_rpc") {
    infer["replicas"] = std::max(1, replicas);
  }
  const auto storage =
      ProjectorSupport::ProjectServiceStorage(FindDiskByName(infer_instance_->private_disk_name));
  if (!storage.is_null()) {
    infer["storage"] = storage;
  }
  if (!infer.empty()) {
    value_["infer"] = std::move(infer);
  }
}

void DesiredStateV2Projector::ProjectWorker() {
  if (worker_instances_.empty()) {
    return;
  }

  nlohmann::json worker = nlohmann::json::object();
  if (!ProjectorSupport::IsDefaultWorkerImage(worker_instances_.front()->image)) {
    worker["image"] = worker_instances_.front()->image;
  }
  const auto start =
      ProjectorSupport::ProjectServiceStart(
          *worker_instances_.front(),
          std::string(kDefaultWorkerCommand));
  if (!start.is_null()) {
    worker["start"] = start;
  }
  const auto env = ProjectorSupport::ProjectCustomEnv(*worker_instances_.front(), true);
  if (!env.empty()) {
    worker["env"] = env;
  }
  const auto publish = ProjectorSupport::ProjectPublishedPorts(*worker_instances_.front());
  if (!publish.empty()) {
    worker["publish"] = publish;
  }

  bool same_node = true;
  bool same_gpu = true;
  const std::string first_node = worker_instances_.front()->node_name;
  const std::optional<std::string> first_gpu = worker_instances_.front()->gpu_device;
  for (const auto* worker_instance : worker_instances_) {
    same_node = same_node && worker_instance->node_name == first_node;
    same_gpu = same_gpu && worker_instance->gpu_device == first_gpu;
  }

  if (worker_instances_.size() == 1 || (same_node && same_gpu)) {
    if (first_node != DefaultNodeName()) {
      worker["node"] = first_node;
    }
    if (first_gpu.has_value() && !first_gpu->empty()) {
      worker["gpu_device"] = *first_gpu;
    }
  } else {
    nlohmann::json assignments = nlohmann::json::array();
    for (const auto* worker_instance : worker_instances_) {
      nlohmann::json assignment = {
          {"node", worker_instance->node_name},
      };
      if (worker_instance->gpu_device.has_value() && !worker_instance->gpu_device->empty()) {
        assignment["gpu_device"] = *worker_instance->gpu_device;
      }
      assignments.push_back(std::move(assignment));
    }
    worker["assignments"] = std::move(assignments);
  }

  const auto storage =
      ProjectorSupport::ProjectServiceStorage(
          FindDiskByName(worker_instances_.front()->private_disk_name));
  if (!storage.is_null()) {
    worker["storage"] = storage;
  }

  if (!worker.empty()) {
    value_["worker"] = std::move(worker);
  }
}

void DesiredStateV2Projector::ProjectApp() {
  if (app_instances_.empty()) {
    value_["app"] = {{"enabled", false}};
    return;
  }

  nlohmann::json apps = nlohmann::json::array();
  const bool multi_app = app_instances_.size() > 1;
  const auto build_app_json = [&](const InstanceSpec& instance, bool primary) {
    nlohmann::json app = {
        {"enabled", true},
        {"image", instance.image},
    };
    if (multi_app || !primary) {
      app["name"] = instance.environment.contains("NAIM_APP_NAME")
                        ? instance.environment.at("NAIM_APP_NAME")
                        : instance.name;
      app["primary"] = primary;
    }
    const auto start = ProjectorSupport::ProjectServiceStart(instance, std::string{});
    if (!start.is_null()) {
      app["start"] = start;
    }
    const auto env = ProjectorSupport::ProjectCustomEnv(instance, true);
    if (!env.empty()) {
      app["env"] = env;
    }
    const auto publish = ProjectorSupport::ProjectPublishedPorts(instance);
    if (!publish.empty()) {
      app["publish"] = publish;
    }
    if (instance.node_name != DefaultNodeName()) {
      app["node"] = instance.node_name;
    }
    const auto app_disk = FindDiskByName(instance.private_disk_name);
    const auto volumes = ProjectorSupport::ProjectAppVolumes(app_disk);
    if (!volumes.empty()) {
      app["volumes"] = std::move(volumes);
    }
    return app;
  };

  const InstanceSpec* primary_app = nullptr;
  for (const auto* instance : app_instances_) {
    if (instance == nullptr) {
      continue;
    }
    const auto it = instance->environment.find("NAIM_APP_PRIMARY");
    if (it != instance->environment.end() && it->second == "true") {
      primary_app = instance;
      break;
    }
  }
  if (primary_app == nullptr) {
    primary_app = app_instances_.front();
  }

  for (const auto* instance : app_instances_) {
    if (instance == nullptr) {
      continue;
    }
    apps.push_back(build_app_json(*instance, instance == primary_app));
  }

  if (apps.size() == 1) {
    value_["app"] = apps.front();
    return;
  }
  value_["apps"] = std::move(apps);
}

void DesiredStateV2Projector::ProjectSkills() {
  if (!state_.skills.has_value() && skills_instance_ == nullptr) {
    return;
  }

  if (skills_instance_ == nullptr) {
    value_["skills"] = {{"enabled", false}};
    return;
  }

  nlohmann::json skills = {
      {"enabled", state_.skills.value_or(SkillsSettings{}).enabled},
  };
  if (state_.skills.has_value() && !state_.skills->factory_skill_ids.empty()) {
    skills["factory_skill_ids"] = state_.skills->factory_skill_ids;
  }
  if (skills_instance_->image != kDefaultSkillsImage) {
    skills["image"] = skills_instance_->image;
  }
  const auto start =
      ProjectorSupport::ProjectServiceStart(
          *skills_instance_,
          std::string(kDefaultSkillsCommand));
  if (!start.is_null()) {
    skills["start"] = start;
  }
  const auto env = ProjectorSupport::ProjectCustomEnv(*skills_instance_, true);
  if (!env.empty()) {
    skills["env"] = env;
  }
  const auto publish = ProjectorSupport::ProjectPublishedPorts(*skills_instance_);
  if (!publish.empty()) {
    skills["publish"] = publish;
  }
  if (skills_instance_->node_name != DefaultNodeName()) {
    skills["node"] = skills_instance_->node_name;
  }
  const auto storage =
      ProjectorSupport::ProjectServiceStorage(
          FindDiskByName(skills_instance_->private_disk_name));
  if (!storage.is_null()) {
    skills["storage"] = storage;
  }
  value_["skills"] = std::move(skills);
}

void DesiredStateV2Projector::ProjectBrowsing() {
  if (!state_.browsing.has_value() && browsing_instance_ == nullptr) {
    return;
  }

  if (browsing_instance_ == nullptr) {
    value_["webgateway"] = {{"enabled", false}};
    return;
  }

  nlohmann::json browsing = {
      {"enabled", state_.browsing.value_or(BrowsingSettings{}).enabled},
  };
  if (state_.browsing.has_value() && state_.browsing->policy.has_value()) {
    const auto& policy = *state_.browsing->policy;
    nlohmann::json policy_json = {
        {"browser_session_enabled", policy.browser_session_enabled},
        {"rendered_browser_enabled", policy.rendered_browser_enabled},
        {"login_enabled", policy.login_enabled},
        {"max_search_results", policy.max_search_results},
        {"max_fetch_bytes", policy.max_fetch_bytes},
    };
    if (!policy.cef_enabled) {
      policy_json["cef_enabled"] = policy.cef_enabled;
    }
    if (!policy.allowed_domains.empty()) {
      policy_json["allowed_domains"] = policy.allowed_domains;
    }
    if (!policy.blocked_domains.empty()) {
      policy_json["blocked_domains"] = policy.blocked_domains;
    }
    if (!policy.blocked_targets.empty()) {
      policy_json["blocked_targets"] = policy.blocked_targets;
    }
    if (!policy.response_review_enabled) {
      policy_json["response_review_enabled"] = policy.response_review_enabled;
    }
    if (policy.policy_version != "webgateway-v1") {
      policy_json["policy_version"] = policy.policy_version;
    }
    browsing["policy"] = std::move(policy_json);
  }
  if (browsing_instance_->image != kDefaultWebGatewayImage) {
    browsing["image"] = browsing_instance_->image;
  }
  const auto start =
      ProjectorSupport::ProjectServiceStart(
          *browsing_instance_,
          std::string(kDefaultWebGatewayCommand));
  if (!start.is_null()) {
    browsing["start"] = start;
  }
  const auto env = ProjectorSupport::ProjectCustomEnv(*browsing_instance_, true);
  if (!env.empty()) {
    browsing["env"] = env;
  }
  const auto publish = ProjectorSupport::ProjectPublishedPorts(*browsing_instance_);
  if (!publish.empty()) {
    browsing["publish"] = publish;
  }
  if (browsing_instance_->node_name != DefaultNodeName()) {
    browsing["node"] = browsing_instance_->node_name;
  }
  const auto storage =
      ProjectorSupport::ProjectServiceStorage(
          FindDiskByName(browsing_instance_->private_disk_name));
  if (!storage.is_null()) {
    browsing["storage"] = storage;
  }
  value_["webgateway"] = std::move(browsing);
}

void DesiredStateV2Projector::ProjectResources() {
  nlohmann::json resources = nlohmann::json::object();
  if (!worker_instances_.empty()) {
    const auto* worker = worker_instances_.front();
    resources["worker"] = {
        {"placement_mode", ToString(worker->placement_mode)},
        {"share_mode", ToString(worker->share_mode)},
        {"gpu_fraction", worker->gpu_fraction},
    };
    if (worker->memory_cap_mb.has_value()) {
      resources["worker"]["memory_cap_mb"] = *worker->memory_cap_mb;
    }
  }
  if (shared_disk_ != nullptr) {
    resources["shared_disk_gb"] = shared_disk_->size_gb;
  } else {
    resources["shared_disk_gb"] = kDefaultSharedDiskSizeGb;
  }
  if (!resources.empty()) {
    value_["resources"] = std::move(resources);
  }
}

bool DesiredStateV2Projector::ShouldEmitTopology() const {
  return DesiredStatePlacementResolver(state_).ShouldEmitTopology();
}

bool DesiredStateV2Projector::IsDefaultSingleNodeTopology() const {
  if (state_.nodes.size() != 1) {
    return false;
  }
  const auto& node = state_.nodes.front();
  return node.name == "local-hostd" && node.platform == "linux" &&
      node.execution_mode == HostExecutionMode::Mixed &&
      node.gpu_memory_mb.empty();
}

std::string DesiredStateV2Projector::DefaultNodeName() const {
  return DesiredStatePlacementResolver(state_).DefaultNodeName();
}

int DesiredStateV2Projector::InferReplicaCount() const {
  std::set<std::string> infer_instance_names;
  for (const auto* worker_instance : worker_instances_) {
    const auto it = worker_instance->environment.find("NAIM_INFER_INSTANCE_NAME");
    if (it == worker_instance->environment.end() || it->second.empty()) {
      continue;
    }
    infer_instance_names.insert(it->second);
  }
  return static_cast<int>(infer_instance_names.size());
}

const InstanceSpec* DesiredStateV2Projector::FindInstance(InstanceRole role) const {
  for (const auto& instance : state_.instances) {
    if (instance.role == role) {
      return &instance;
    }
  }
  return nullptr;
}

std::vector<const InstanceSpec*> DesiredStateV2Projector::FindInstances(InstanceRole role) const {
  std::vector<const InstanceSpec*> instances;
  for (const auto& instance : state_.instances) {
    if (instance.role == role) {
      instances.push_back(&instance);
    }
  }
  return instances;
}

std::vector<const InstanceSpec*> DesiredStateV2Projector::FindWorkerInstances() const {
  std::vector<const InstanceSpec*> instances;
  for (const auto& instance : state_.instances) {
    if (instance.role == InstanceRole::Worker) {
      instances.push_back(&instance);
    }
  }
  return instances;
}

const DiskSpec* DesiredStateV2Projector::FindDiskByName(const std::string& name) const {
  for (const auto& disk : state_.disks) {
    if (disk.name == name) {
      return &disk;
    }
  }
  return nullptr;
}

const DiskSpec* DesiredStateV2Projector::FindSharedDisk() const {
  if (!state_.plane_shared_disk_name.empty()) {
    return FindDiskByName(state_.plane_shared_disk_name);
  }
  for (const auto& disk : state_.disks) {
    if (disk.kind == DiskKind::PlaneShared) {
      return &disk;
    }
  }
  return nullptr;
}

nlohmann::json DesiredStateV2Projector::ProjectModelSource() const {
  const auto& model = *state_.bootstrap_model;
  nlohmann::json source = nlohmann::json::object();
  const std::string source_type = PreferredModelSourceType();
  source["type"] = source_type;
  if (!model.model_id.empty()) {
    source["ref"] = model.model_id;
  }
  if (source_type == "local" && model.local_path.has_value() && !model.local_path->empty()) {
    source["path"] = *model.local_path;
  }
  if (source_type == "url") {
    if (model.source_url.has_value() && !model.source_url->empty()) {
      source["url"] = *model.source_url;
    }
    if (!model.source_urls.empty()) {
      source["urls"] = model.source_urls;
    }
  }
  return source;
}

std::string DesiredStateV2Projector::PreferredModelSourceType() const {
  const auto& model = *state_.bootstrap_model;
  if (model.source_url.has_value() || !model.source_urls.empty()) {
    return "url";
  }
  if (model.local_path.has_value() && model.materialization_mode == "reference") {
    return "local";
  }
  if (model.materialization_mode == "prepare_on_worker") {
    return "library";
  }
  return "huggingface";
}

std::string DesiredStateV2Projector::AddBundlePrefixIfRelative(const std::string& value) const {
  if (value.empty() || value.front() == '/' || value.rfind("bundle://", 0) == 0) {
    return value;
  }
  return "bundle://" + value;
}

}  // namespace naim
