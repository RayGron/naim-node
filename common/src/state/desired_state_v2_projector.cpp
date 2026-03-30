#include "comet/state/desired_state_v2_projector.h"

#include <algorithm>
#include <set>
#include <stdexcept>
#include <string>
#include <utility>

#include "comet/state/worker_group_topology.h"

namespace comet {

namespace {

constexpr int kDefaultSharedDiskSizeGb = 40;
constexpr int kDefaultInferPrivateDiskSizeGb = 12;
constexpr int kDefaultWorkerPrivateDiskSizeGb = 12;
constexpr int kDefaultAppPrivateDiskSizeGb = 8;
constexpr std::string_view kDefaultInferImage = "comet/infer-runtime:dev";
constexpr std::string_view kDefaultVllmWorkerImage = "comet/worker-vllm-runtime:dev";
constexpr std::string_view kDefaultWorkerImage = "comet/worker-runtime:dev";
constexpr std::string_view kDefaultInferCommand = "/runtime/infer/entrypoint.sh";
constexpr std::string_view kDefaultWorkerCommand = "/runtime/worker/entrypoint.sh";

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
  ProjectHooks();
  ProjectModel();
  ProjectTopology();
  ProjectInteraction();
  ProjectRuntime();
  ProjectNetwork();
  ProjectInfer();
  ProjectWorker();
  ProjectApp();
  ProjectResources();
  return value_;
}

void DesiredStateV2Projector::CollectInstancesAndDisks() {
  infer_instance_ = FindInstance(InstanceRole::Infer);
  app_instance_ = FindInstance(InstanceRole::App);
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
  if (interaction.system_prompt.has_value() && !interaction.system_prompt->empty()) {
    interaction_json["system_prompt"] = *interaction.system_prompt;
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
      {"data_parallel_mode", state_.inference.data_parallel_mode},
      {"distributed_backend", state_.inference.distributed_backend},
      {"max_model_len", state_.inference.max_model_len},
      {"max_num_seqs", state_.inference.max_num_seqs},
      {"gpu_memory_utilization", state_.inference.gpu_memory_utilization},
  };
  if (state_.inference.data_parallel_lb_mode != kDataParallelLbModeExternal ||
      state_.inference.data_parallel_mode != kDataParallelModeOff) {
    runtime["data_parallel_lb_mode"] = state_.inference.data_parallel_lb_mode;
  }
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
  const auto start = ProjectServiceStart(*infer_instance_, std::string(kDefaultInferCommand));
  if (!start.is_null()) {
    infer["start"] = start;
  }
  const auto env = ProjectCustomEnv(*infer_instance_, true);
  if (!env.empty()) {
    infer["env"] = env;
  }
  const auto publish = ProjectPublishedPorts(*infer_instance_);
  if (!publish.empty()) {
    infer["publish"] = publish;
  }
  if (infer_instance_->node_name != DefaultNodeName()) {
    infer["node"] = infer_instance_->node_name;
  }
  const int replicas = InferReplicaCount();
  if (replicas > 1) {
    infer["replicas"] = replicas;
  }
  const auto storage = ProjectServiceStorage(FindDiskByName(infer_instance_->private_disk_name));
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
  if (!IsDefaultWorkerImage(worker_instances_.front()->image)) {
    worker["image"] = worker_instances_.front()->image;
  }
  const auto start =
      ProjectServiceStart(*worker_instances_.front(), std::string(kDefaultWorkerCommand));
  if (!start.is_null()) {
    worker["start"] = start;
  }
  const auto env = ProjectCustomEnv(*worker_instances_.front(), true);
  if (!env.empty()) {
    worker["env"] = env;
  }
  const auto publish = ProjectPublishedPorts(*worker_instances_.front());
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
      ProjectServiceStorage(FindDiskByName(worker_instances_.front()->private_disk_name));
  if (!storage.is_null()) {
    worker["storage"] = storage;
  }

  if (!worker.empty()) {
    value_["worker"] = std::move(worker);
  }
}

void DesiredStateV2Projector::ProjectApp() {
  if (app_instance_ == nullptr) {
    value_["app"] = {{"enabled", false}};
    return;
  }

  nlohmann::json app = {
      {"enabled", true},
      {"image", app_instance_->image},
  };
  const auto start = ProjectServiceStart(*app_instance_, std::string{});
  if (!start.is_null()) {
    app["start"] = start;
  }
  if (!app_instance_->environment.empty()) {
    app["env"] = app_instance_->environment;
  }
  const auto publish = ProjectPublishedPorts(*app_instance_);
  if (!publish.empty()) {
    app["publish"] = publish;
  }
  if (app_instance_->node_name != DefaultNodeName()) {
    app["node"] = app_instance_->node_name;
  }
  const auto app_disk = FindDiskByName(app_instance_->private_disk_name);
  const auto volumes = ProjectAppVolumes(app_disk);
  if (!volumes.empty()) {
    app["volumes"] = std::move(volumes);
  }
  value_["app"] = std::move(app);
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
  return !state_.nodes.empty() && !IsDefaultSingleNodeTopology();
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
  if (!state_.nodes.empty()) {
    return state_.nodes.front().name;
  }
  return "local-hostd";
}

int DesiredStateV2Projector::InferReplicaCount() const {
  std::set<std::string> infer_instance_names;
  for (const auto* worker_instance : worker_instances_) {
    const auto it = worker_instance->environment.find("COMET_INFER_INSTANCE_NAME");
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

nlohmann::json DesiredStateV2Projector::ProjectServiceStart(
    const InstanceSpec& instance,
    const std::string& default_command) const {
  if (instance.command.empty() || instance.command == default_command) {
    return nullptr;
  }
  return {
      {"type", "command"},
      {"command", instance.command},
  };
}

nlohmann::json DesiredStateV2Projector::ProjectPublishedPorts(
    const InstanceSpec& instance) const {
  if (instance.published_ports.empty()) {
    return nlohmann::json::array();
  }
  nlohmann::json ports = nlohmann::json::array();
  for (const auto& port : instance.published_ports) {
    ports.push_back({
        {"host_ip", port.host_ip},
        {"host_port", port.host_port},
        {"container_port", port.container_port},
    });
  }
  return ports;
}

nlohmann::json DesiredStateV2Projector::ProjectServiceStorage(const DiskSpec* disk) const {
  if (disk == nullptr) {
    return nullptr;
  }
  const bool default_size = disk->kind == DiskKind::InferPrivate
          ? disk->size_gb == kDefaultInferPrivateDiskSizeGb
          : disk->kind == DiskKind::WorkerPrivate
              ? disk->size_gb == kDefaultWorkerPrivateDiskSizeGb
              : disk->size_gb == kDefaultAppPrivateDiskSizeGb;
  const bool default_mount = disk->container_path == "/comet/private";
  if (default_size && default_mount) {
    return nullptr;
  }
  return {
      {"size_gb", disk->size_gb},
      {"mount_path", disk->container_path},
  };
}

nlohmann::json DesiredStateV2Projector::ProjectAppVolumes(const DiskSpec* disk) const {
  if (disk == nullptr) {
    return nlohmann::json::array();
  }
  return nlohmann::json::array(
      {{{"name", "private-data"},
        {"type", "persistent"},
        {"size_gb", disk->size_gb},
        {"mount_path", disk->container_path},
        {"access", "rw"}}});
}

std::map<std::string, std::string> DesiredStateV2Projector::ProjectCustomEnv(
    const InstanceSpec& instance,
    bool strip_comet_env) const {
  std::map<std::string, std::string> env;
  for (const auto& [key, value] : instance.environment) {
    if (strip_comet_env && key.rfind("COMET_", 0) == 0) {
      continue;
    }
    env[key] = value;
  }
  return env;
}

bool DesiredStateV2Projector::IsDefaultWorkerImage(const std::string& image) const {
  return state_.inference.runtime_engine == "vllm" ? image == kDefaultVllmWorkerImage
                                                   : image == kDefaultWorkerImage;
}

std::string DesiredStateV2Projector::PreferredModelSourceType() const {
  const auto& model = *state_.bootstrap_model;
  if (model.source_url.has_value() || !model.source_urls.empty()) {
    return "url";
  }
  if (model.local_path.has_value() && model.materialization_mode == "reference") {
    return "local";
  }
  return "huggingface";
}

std::string DesiredStateV2Projector::AddBundlePrefixIfRelative(const std::string& value) const {
  if (value.empty() || value.front() == '/' || value.rfind("bundle://", 0) == 0) {
    return value;
  }
  return "bundle://" + value;
}

}  // namespace comet
