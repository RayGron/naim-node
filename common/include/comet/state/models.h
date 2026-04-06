#pragma once

#include <algorithm>
#include <map>
#include <optional>
#include <string>
#include <vector>

namespace comet {

enum class InstanceRole {
  Infer,
  Worker,
  App,
  Skills,
  Browsing,
};

enum class DiskKind {
  PlaneShared,
  InferPrivate,
  WorkerPrivate,
  AppPrivate,
  SkillsPrivate,
  BrowsingPrivate,
};

inline bool IsPrivateDiskKind(DiskKind kind) {
  return kind == DiskKind::InferPrivate ||
         kind == DiskKind::WorkerPrivate ||
         kind == DiskKind::AppPrivate ||
         kind == DiskKind::SkillsPrivate ||
         kind == DiskKind::BrowsingPrivate;
}

inline bool IsNodeLocalDiskKind(DiskKind kind) {
  return IsPrivateDiskKind(kind);
}

inline bool InstanceNeedsSharedDiskMount(InstanceRole role) {
  return role == InstanceRole::Infer ||
         role == InstanceRole::Worker ||
         role == InstanceRole::App;
}

inline bool InstanceNeedsPrivateDisk(InstanceRole role) {
  return role == InstanceRole::Worker ||
         role == InstanceRole::App ||
         role == InstanceRole::Skills ||
         role == InstanceRole::Browsing;
}

struct PublishedPort {
  std::string host_ip = "127.0.0.1";
  int host_port = 0;
  int container_port = 0;
};

enum class GpuShareMode {
  Exclusive,
  Shared,
  BestEffort,
};

enum class PlacementMode {
  Manual,
  Auto,
  Movable,
};

enum class PlaneMode {
  Compute,
  Llm,
};

enum class HostExecutionMode {
  InferOnly,
  WorkerOnly,
  Mixed,
};

struct DiskSpec {
  std::string name;
  DiskKind kind;
  std::string plane_name;
  std::string owner_name;
  std::string node_name;
  std::string host_path;
  std::string container_path;
  int size_gb = 0;
};

struct InstanceSpec {
  std::string name;
  InstanceRole role;
  std::string plane_name;
  std::string node_name;
  std::string image;
  std::string command;
  std::string private_disk_name;
  std::string shared_disk_name;
  std::vector<std::string> depends_on;
  std::map<std::string, std::string> environment;
  std::map<std::string, std::string> labels;
  std::vector<PublishedPort> published_ports;
  std::optional<std::string> gpu_device;
  PlacementMode placement_mode = PlacementMode::Manual;
  GpuShareMode share_mode = GpuShareMode::Exclusive;
  double gpu_fraction = 0.0;
  int priority = 100;
  bool preemptible = false;
  std::optional<int> memory_cap_mb;
  int private_disk_size_gb = 0;
};

struct NodeInventory {
  std::string name;
  std::string platform;
  HostExecutionMode execution_mode = HostExecutionMode::Mixed;
  std::vector<std::string> gpu_devices;
  std::map<std::string, int> gpu_memory_mb;
};

inline std::vector<std::string> EffectiveNodeGpuDevices(const NodeInventory& node) {
  std::vector<std::string> devices;
  devices.reserve(node.gpu_memory_mb.size() + node.gpu_devices.size());

  for (const auto& [gpu_device, _] : node.gpu_memory_mb) {
    devices.push_back(gpu_device);
  }
  for (const auto& gpu_device : node.gpu_devices) {
    if (std::find(devices.begin(), devices.end(), gpu_device) == devices.end()) {
      devices.push_back(gpu_device);
    }
  }

  std::sort(devices.begin(), devices.end());
  return devices;
}

inline bool NodeHasConfiguredGpuDevices(const NodeInventory& node) {
  return !EffectiveNodeGpuDevices(node).empty();
}

struct RuntimeGpuNode {
  std::string name;
  std::string node_name;
  std::string gpu_device;
  PlacementMode placement_mode = PlacementMode::Manual;
  GpuShareMode share_mode = GpuShareMode::Exclusive;
  double gpu_fraction = 0.0;
  int priority = 100;
  bool preemptible = false;
  std::optional<int> memory_cap_mb;
  bool enabled = true;
};

struct InferenceRuntimeSettings {
  std::string primary_infer_node;
  std::string runtime_engine = "llama.cpp";
  std::string data_parallel_mode = "off";
  std::string data_parallel_lb_mode = "external";
  int api_server_count = 0;
  std::string worker_group_id;
  std::string distributed_backend = "llama_rpc";
  std::string worker_selection_policy = "prefer-free-then-share";
  std::string net_if = "eth0";
  std::string models_root = "/comet/shared/models";
  std::string model_cache_dir = "/comet/shared/models/cache";
  std::string runtime_log_dir = "/comet/shared/logs/infer";
  int api_port = 8000;
  int max_model_len = 8192;
  int tensor_parallel_size = 1;
  int pipeline_parallel_size = 1;
  int max_num_seqs = 16;
  double gpu_memory_utilization = 0.9;
  bool enforce_eager = false;
  std::string gguf_cache_dir = "/comet/shared/models/gguf";
  std::string infer_log_dir = "/comet/shared/logs/infer";
  int llama_port = 8000;
  int llama_ctx_size = 8192;
  int llama_threads = 8;
  int llama_gpu_layers = 99;
  int inference_healthcheck_retries = 300;
  int inference_healthcheck_interval_sec = 5;
  int rendezvous_port = 29500;
};

struct WorkerGroupMemberSpec {
  std::string name;
  std::string infer_instance_name;
  std::string node_name;
  std::string gpu_device;
  int rpc_port = 0;
  int rank = 0;
  std::string replica_group_id;
  int replica_index = 0;
  int replica_size = 1;
  bool replica_leader = false;
  int data_parallel_rank = 0;
  int data_parallel_size = 1;
  int data_parallel_size_local = 1;
  int data_parallel_start_rank = 0;
  bool data_parallel_api_endpoint = false;
  std::string data_parallel_head_address;
  int data_parallel_rpc_port = 0;
  double gpu_fraction = 0.0;
  GpuShareMode share_mode = GpuShareMode::Exclusive;
  int priority = 100;
  bool preemptible = false;
  std::optional<int> memory_cap_mb;
  bool enabled = true;
  bool leader = false;
};

struct WorkerGroupSpec {
  std::string group_id;
  std::string infer_instance_name;
  std::string distributed_backend = "llama_rpc";
  std::string rendezvous_host;
  int rendezvous_port = 29500;
  int expected_workers = 0;
  std::string worker_selection_policy = "prefer-free-then-share";
  std::vector<WorkerGroupMemberSpec> members;
};

struct GatewaySettings {
  std::string listen_host = "0.0.0.0";
  int listen_port = 80;
  std::string server_name = "_";
};

struct BootstrapModelSpec {
  std::string model_id;
  std::optional<std::string> served_model_name;
  std::string materialization_mode = "copy";
  std::optional<std::string> local_path;
  std::optional<std::string> source_url;
  std::vector<std::string> source_urls;
  std::optional<std::string> target_filename;
  std::optional<std::string> sha256;
};

struct InteractionSettings {
  struct CompletionPolicy {
    std::string response_mode = "normal";
    int max_tokens = 512;
    std::optional<int> target_completion_tokens;
    int max_continuations = 3;
    int max_total_completion_tokens = 1536;
    int max_elapsed_time_ms = 180000;
    std::optional<std::string> semantic_goal;
  };

  std::optional<std::string> system_prompt;
  std::optional<std::string> analysis_system_prompt;
  bool thinking_enabled = false;
  std::optional<double> default_temperature;
  std::optional<double> default_top_p;
  std::string default_response_language = "en";
  std::vector<std::string> supported_response_languages;
  bool follow_user_language = true;
  std::optional<CompletionPolicy> completion_policy;
  std::optional<CompletionPolicy> long_completion_policy;
  std::optional<CompletionPolicy> analysis_completion_policy;
  std::optional<CompletionPolicy> analysis_long_completion_policy;
};

struct SkillsSettings {
  bool enabled = false;
  std::vector<std::string> factory_skill_ids;
};

struct BrowsingPolicySettings {
  bool browser_session_enabled = false;
  bool rendered_browser_enabled = true;
  bool login_enabled = false;
  std::vector<std::string> allowed_domains;
  std::vector<std::string> blocked_domains;
  int max_search_results = 8;
  int max_fetch_bytes = 262144;
};

struct BrowsingSettings {
  bool enabled = false;
  std::optional<BrowsingPolicySettings> policy;
};

struct DesiredState {
  std::string plane_name;
  std::string plane_shared_disk_name;
  std::string control_root;
  PlaneMode plane_mode = PlaneMode::Compute;
  bool protected_plane = false;
  std::optional<std::string> post_deploy_script;
  std::optional<std::string> placement_target;
  std::optional<BootstrapModelSpec> bootstrap_model;
  std::optional<InteractionSettings> interaction;
  std::optional<SkillsSettings> skills;
  std::optional<BrowsingSettings> browsing;
  InferenceRuntimeSettings inference;
  WorkerGroupSpec worker_group;
  GatewaySettings gateway;
  std::vector<RuntimeGpuNode> runtime_gpu_nodes;
  std::vector<NodeInventory> nodes;
  std::vector<DiskSpec> disks;
  std::vector<InstanceSpec> instances;
};

struct ComposeVolume {
  std::string source;
  std::string target;
  bool read_only = false;
};

struct ComposeService {
  std::string name;
  std::string image;
  std::string command;
  std::vector<std::string> depends_on;
  std::map<std::string, std::string> environment;
  std::map<std::string, std::string> labels;
  std::vector<ComposeVolume> volumes;
  std::vector<PublishedPort> published_ports;
  std::vector<std::string> extra_hosts;
  std::vector<std::string> security_opts;
  bool privileged = false;
  bool use_nvidia_runtime = false;
  std::optional<std::string> gpu_device;
  std::vector<std::string> gpu_devices;
  std::optional<std::string> shm_size;
  std::string healthcheck;
};

struct NodeComposePlan {
  std::string plane_name;
  std::string node_name;
  std::vector<DiskSpec> disks;
  std::vector<ComposeService> services;
};

std::string ToString(InstanceRole role);
std::string ToString(DiskKind kind);
std::string ToString(GpuShareMode mode);
GpuShareMode ParseGpuShareMode(const std::string& value);
std::string ToString(PlacementMode mode);
PlacementMode ParsePlacementMode(const std::string& value);
std::string ToString(PlaneMode mode);
PlaneMode ParsePlaneMode(const std::string& value);
std::string ToString(HostExecutionMode mode);
HostExecutionMode ParseHostExecutionMode(const std::string& value);

}  // namespace comet
