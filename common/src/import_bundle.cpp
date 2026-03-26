#include "comet/import_bundle.h"
#include "comet/scheduling_policy.h"
#include "comet/state_json.h"

#include <algorithm>
#include <cmath>
#include <filesystem>
#include <fstream>
#include <map>
#include <optional>
#include <set>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include <nlohmann/json.hpp>

namespace comet {

namespace {

using json = nlohmann::json;

json ReadJsonFile(const std::filesystem::path& path) {
  std::ifstream input(path);
  if (!input.is_open()) {
    throw std::runtime_error("failed to open json file '" + path.string() + "'");
  }

  json value;
  input >> value;
  return value;
}

std::string RequiredString(const json& value, const char* key, const std::string& context) {
  if (!value.contains(key) || !value.at(key).is_string()) {
    throw std::runtime_error(context + " is missing required string field '" + key + "'");
  }
  return value.at(key).get<std::string>();
}

std::string JoinStrings(const std::vector<std::string>& values, const char* delimiter) {
  std::string result;
  for (std::size_t index = 0; index < values.size(); ++index) {
    if (index > 0) {
      result += delimiter;
    }
    result += values[index];
  }
  return result;
}

bool IsDirectFitAction(const std::string& action) {
  return action == "upgrade-to-exclusive" ||
         action == "move-equivalent" ||
         action == "move-with-current-fraction";
}

int OptionalInt(const json& value, const char* key, int default_value) {
  if (!value.contains(key)) {
    return default_value;
  }
  if (!value.at(key).is_number_integer()) {
    throw std::runtime_error(std::string("field '") + key + "' must be an integer");
  }
  return value.at(key).get<int>();
}

std::optional<int> OptionalIntOpt(const json& value, const char* key) {
  if (!value.contains(key) || value.at(key).is_null()) {
    return std::nullopt;
  }
  if (!value.at(key).is_number_integer()) {
    throw std::runtime_error(std::string("field '") + key + "' must be an integer");
  }
  return value.at(key).get<int>();
}

double OptionalDouble(const json& value, const char* key, double default_value) {
  if (!value.contains(key)) {
    return default_value;
  }
  if (!value.at(key).is_number()) {
    throw std::runtime_error(std::string("field '") + key + "' must be numeric");
  }
  return value.at(key).get<double>();
}

std::string OptionalString(const json& value, const char* key, const std::string& default_value) {
  if (!value.contains(key)) {
    return default_value;
  }
  if (!value.at(key).is_string()) {
    throw std::runtime_error(std::string("field '") + key + "' must be a string");
  }
  return value.at(key).get<std::string>();
}

std::optional<std::string> OptionalStringOpt(const json& value, const char* key) {
  if (!value.contains(key) || value.at(key).is_null()) {
    return std::nullopt;
  }
  if (!value.at(key).is_string()) {
    throw std::runtime_error(std::string("field '") + key + "' must be a string");
  }
  return value.at(key).get<std::string>();
}

bool OptionalBool(const json& value, const char* key, bool default_value) {
  if (!value.contains(key)) {
    return default_value;
  }
  if (!value.at(key).is_boolean()) {
    throw std::runtime_error(std::string("field '") + key + "' must be a boolean");
  }
  return value.at(key).get<bool>();
}

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

void ValidateNodeExists(
    const std::map<std::string, NodeInventory>& nodes_by_name,
    const std::string& node_name,
    const std::string& context) {
  if (nodes_by_name.find(node_name) == nodes_by_name.end()) {
    throw std::runtime_error(context + " references unknown node '" + node_name + "'");
  }
}

void ValidateGpuExists(
    const NodeInventory& node,
    const std::optional<std::string>& gpu_device,
    const std::string& context) {
  if (!gpu_device.has_value()) {
    return;
  }
  const auto it =
      std::find(node.gpu_devices.begin(), node.gpu_devices.end(), *gpu_device);
  if (it == node.gpu_devices.end()) {
    throw std::runtime_error(
        context + " references missing gpu '" + *gpu_device + "' on node '" + node.name + "'");
  }
}

bool NodeSupportsInstanceRole(const NodeInventory& node, InstanceRole role) {
  switch (node.execution_mode) {
    case HostExecutionMode::InferOnly:
      return role == InstanceRole::Infer;
    case HostExecutionMode::WorkerOnly:
      return role == InstanceRole::Worker;
    case HostExecutionMode::Mixed:
      return true;
  }
  return true;
}

struct PlacementUsage {
  double allocated_fraction = 0.0;
  int allocated_memory_mb = 0;
};

struct AutoPlacementDecision {
  std::string node_name;
  std::string gpu_device;
  int score = 0;
  bool idle_target = false;
  bool upgrade_to_exclusive = false;
  double allocated_fraction = 0.0;
  int allocated_memory_mb = 0;
  int node_order = 0;
  int gpu_order = 0;
};

std::string EffectiveWorkerSelectionPolicy(const DesiredState& state) {
  if (!state.worker_group.worker_selection_policy.empty()) {
    return state.worker_group.worker_selection_policy;
  }
  if (!state.inference.worker_selection_policy.empty()) {
    return state.inference.worker_selection_policy;
  }
  return "prefer-free-then-share";
}

int AutoPlacementPolicyRank(
    const std::string& policy,
    const AutoPlacementDecision& candidate) {
  if (policy == "prefer-free-then-share") {
    return candidate.idle_target ? 0 : 1;
  }
  return candidate.idle_target ? 0 : 1;
}

constexpr int kMovableRelocationThreshold = 10;

int ScoreAutoPlacementCandidate(
    const NodeInventory& node,
    const std::string& gpu_device,
    const PlacementUsage& usage,
    const InferenceRuntimeSettings& inference,
    const std::optional<std::string>& preferred_node_name,
    const std::optional<std::string>& preferred_gpu_device) {
  int score = 0;
  if (usage.allocated_fraction <= 1e-9) {
    score += 60;
  }
  if (node.name == inference.primary_infer_node) {
    score += 10;
  }
  if (preferred_node_name.has_value() && node.name == *preferred_node_name) {
    score += 20;
  }
  if (preferred_gpu_device.has_value() && gpu_device == *preferred_gpu_device) {
    score += 10;
  }
  score += static_cast<int>((1.0 - usage.allocated_fraction) * 20.0);
  const auto memory_it = node.gpu_memory_mb.find(gpu_device);
  if (memory_it != node.gpu_memory_mb.end()) {
    score += std::max(0, memory_it->second - usage.allocated_memory_mb) / 1024;
  }
  return score;
}

std::optional<AutoPlacementDecision> SelectAutoPlacement(
    const std::vector<NodeInventory>& nodes,
    const std::map<std::pair<std::string, std::string>, PlacementUsage>& placement_usage,
    const InstanceSpec& worker,
    const DesiredState& state,
    const std::optional<std::string>& requested_node_name,
    const std::optional<std::string>& requested_gpu_device,
    bool strict_node_constraint,
    bool strict_gpu_constraint) {
  std::optional<AutoPlacementDecision> best;
  std::optional<AutoPlacementDecision> current_target;
  const bool has_current_target =
      requested_node_name.has_value() && requested_gpu_device.has_value();
  const std::optional<std::string> scoring_preferred_node_name =
      worker.placement_mode == PlacementMode::Movable ? std::nullopt : requested_node_name;
  const std::optional<std::string> scoring_preferred_gpu_device =
      worker.placement_mode == PlacementMode::Movable ? std::nullopt : requested_gpu_device;
  const std::string selection_policy = EffectiveWorkerSelectionPolicy(state);

  for (std::size_t node_index = 0; node_index < nodes.size(); ++node_index) {
    const auto& node = nodes[node_index];
    if (!NodeSupportsInstanceRole(node, worker.role)) {
      continue;
    }
    if (strict_node_constraint && requested_node_name.has_value() && node.name != *requested_node_name) {
      continue;
    }

    for (std::size_t gpu_index = 0; gpu_index < node.gpu_devices.size(); ++gpu_index) {
      const auto& gpu_device = node.gpu_devices[gpu_index];
      if (strict_gpu_constraint && requested_gpu_device.has_value() &&
          gpu_device != *requested_gpu_device) {
        continue;
      }
      const auto usage_it = placement_usage.find({node.name, gpu_device});
      const PlacementUsage usage =
          usage_it == placement_usage.end() ? PlacementUsage{} : usage_it->second;
      const double free_fraction = 1.0 - usage.allocated_fraction;
      if (free_fraction + 1e-9 < worker.gpu_fraction) {
        continue;
      }

      const auto memory_it = node.gpu_memory_mb.find(gpu_device);
      if (worker.memory_cap_mb.has_value() && memory_it != node.gpu_memory_mb.end() &&
          *worker.memory_cap_mb > memory_it->second - usage.allocated_memory_mb) {
        continue;
      }

      AutoPlacementDecision candidate;
      candidate.node_name = node.name;
      candidate.gpu_device = gpu_device;
      candidate.score =
          ScoreAutoPlacementCandidate(
              node,
              gpu_device,
              usage,
              state.inference,
              scoring_preferred_node_name,
              scoring_preferred_gpu_device);
      candidate.idle_target = usage.allocated_fraction <= 1e-9;
      candidate.upgrade_to_exclusive =
          candidate.idle_target &&
          (worker.share_mode != GpuShareMode::Exclusive || worker.gpu_fraction < 1.0 - 1e-9);
      candidate.allocated_fraction = usage.allocated_fraction;
      candidate.allocated_memory_mb = usage.allocated_memory_mb;
      candidate.node_order = static_cast<int>(node_index);
      candidate.gpu_order = static_cast<int>(gpu_index);

      if (has_current_target && node.name == *requested_node_name &&
          gpu_device == *requested_gpu_device) {
        current_target = candidate;
      }

      if (!best.has_value()) {
        best = candidate;
        continue;
      }

      const int candidate_rank = AutoPlacementPolicyRank(selection_policy, candidate);
      const int best_rank = AutoPlacementPolicyRank(selection_policy, *best);
      if (candidate_rank < best_rank ||
          (candidate_rank == best_rank &&
           (candidate.allocated_fraction < best->allocated_fraction - 1e-9 ||
            (std::abs(candidate.allocated_fraction - best->allocated_fraction) <= 1e-9 &&
             (candidate.score > best->score ||
              (candidate.score == best->score &&
               (candidate.node_order < best->node_order ||
                (candidate.node_order == best->node_order &&
                 (candidate.gpu_order < best->gpu_order ||
                  (candidate.gpu_order == best->gpu_order &&
                   (candidate.node_name < best->node_name ||
                    (candidate.node_name == best->node_name &&
                     candidate.gpu_device < best->gpu_device)))))))))))) {
        best = candidate;
      }
    }
  }

  if (worker.placement_mode == PlacementMode::Movable &&
      best.has_value() &&
      current_target.has_value() &&
      (best->node_name != current_target->node_name ||
       best->gpu_device != current_target->gpu_device) &&
      best->score < current_target->score + kMovableRelocationThreshold) {
    return current_target;
  }

  return best;
}

void ReservePlacement(
    std::map<std::pair<std::string, std::string>, PlacementUsage>* placement_usage,
    const InstanceSpec& worker) {
  if (!worker.gpu_device.has_value()) {
    return;
  }
  auto& usage = (*placement_usage)[{worker.node_name, *worker.gpu_device}];
  usage.allocated_fraction += worker.gpu_fraction;
  usage.allocated_memory_mb += worker.memory_cap_mb.value_or(0);
}

std::vector<NodeInventory> ParseNodes(const json& plane_json) {
  std::vector<NodeInventory> nodes;
  std::set<std::string> node_names;

  if (!plane_json.contains("nodes")) {
    NodeInventory local_hostd;
    local_hostd.name = "local-hostd";
    local_hostd.platform = "linux";
    local_hostd.execution_mode = HostExecutionMode::Mixed;
    local_hostd.gpu_devices = {"0", "1"};
    local_hostd.gpu_memory_mb = {{"0", 24576}, {"1", 24576}};
    nodes.push_back(std::move(local_hostd));

    NodeInventory node_b;
    node_b.name = "node-b";
    node_b.platform = "linux";
    node_b.execution_mode = HostExecutionMode::Mixed;
    node_b.gpu_devices = {"0"};
    node_b.gpu_memory_mb = {{"0", 24576}};
    nodes.push_back(std::move(node_b));
    return nodes;
  }

  if (!plane_json.at("nodes").is_array()) {
    throw std::runtime_error("plane.json field 'nodes' must be an array");
  }

  for (const auto& node_json : plane_json.at("nodes")) {
    NodeInventory node;
    node.name = RequiredString(node_json, "name", "plane node");
    node.platform = OptionalString(node_json, "platform", "linux");
    node.execution_mode =
        ParseHostExecutionMode(OptionalString(node_json, "execution_mode", "mixed"));
    if (!node_names.insert(node.name).second) {
      throw std::runtime_error("plane.json contains duplicate node '" + node.name + "'");
    }

    if (node_json.contains("gpus")) {
      if (!node_json.at("gpus").is_array()) {
        throw std::runtime_error("plane node field 'gpus' must be an array");
      }
      for (const auto& gpu_json : node_json.at("gpus")) {
        if (!gpu_json.is_string()) {
          throw std::runtime_error("plane node gpu id must be a string");
        }
        node.gpu_devices.push_back(gpu_json.get<std::string>());
      }
    }

    if (node_json.contains("gpu_memory_mb")) {
      if (!node_json.at("gpu_memory_mb").is_object()) {
        throw std::runtime_error("plane node field 'gpu_memory_mb' must be an object");
      }
      for (auto it = node_json.at("gpu_memory_mb").begin(); it != node_json.at("gpu_memory_mb").end();
           ++it) {
        if (!it.value().is_number_integer()) {
          throw std::runtime_error("plane node gpu_memory_mb values must be integers");
        }
        node.gpu_memory_mb[it.key()] = it.value().get<int>();
      }
    }

    for (const auto& [gpu_device, _] : node.gpu_memory_mb) {
      const auto gpu_it = std::find(node.gpu_devices.begin(), node.gpu_devices.end(), gpu_device);
      if (gpu_it == node.gpu_devices.end()) {
        throw std::runtime_error(
            "plane node '" + node.name + "' defines gpu_memory_mb for unknown gpu '" + gpu_device + "'");
      }
    }

    nodes.push_back(std::move(node));
  }

  if (nodes.empty()) {
    throw std::runtime_error("plane.json must define at least one node");
  }

  return nodes;
}

std::optional<json> OptionalObject(const json& value, const char* key) {
  if (!value.contains(key) || value.at(key).is_null()) {
    return std::nullopt;
  }
  if (!value.at(key).is_object()) {
    throw std::runtime_error(std::string("field '") + key + "' must be an object");
  }
  return json(value.at(key));
}

void ApplyMovableSchedulerDecisions(DesiredState* state) {
  if (state == nullptr) {
    return;
  }

  const SchedulingPolicyReport final_report = EvaluateSchedulingPolicy(*state);
  for (auto& instance : state->instances) {
    if (instance.role != InstanceRole::Worker ||
        instance.placement_mode != PlacementMode::Movable) {
      continue;
    }
    instance.labels.erase("comet.placement.decision");
    instance.labels.erase("comet.placement.next_action");
    instance.labels.erase("comet.placement.next_target");
    instance.labels.erase("comet.placement.defer_reason");
    instance.labels.erase("comet.preemption.victims");
  }

  for (const auto& recommendation : final_report.placement_recommendations) {
    auto instance_it = std::find_if(
        state->instances.begin(),
        state->instances.end(),
        [&](const InstanceSpec& instance) {
          return instance.role == InstanceRole::Worker &&
                 instance.name == recommendation.worker_name &&
                 instance.placement_mode == PlacementMode::Movable;
        });
    if (instance_it == state->instances.end() || recommendation.candidates.empty()) {
      continue;
    }

    const auto& candidate = recommendation.candidates.front();
    instance_it->labels["comet.placement.decision"] =
        candidate.preemption_required ? "deferred"
                                      : (IsDirectFitAction(candidate.action) ? "proposed"
                                                                             : "hold");
    instance_it->labels["comet.placement.next_action"] = candidate.action;
    instance_it->labels["comet.placement.next_target"] =
        candidate.node_name + ":" + candidate.gpu_device;
    instance_it->labels["comet.placement.score"] = std::to_string(candidate.score);
    if (candidate.preemption_required) {
      instance_it->labels["comet.placement.defer_reason"] =
          "requires-controlled-preemption";
      if (!candidate.preemption_victims.empty()) {
        instance_it->labels["comet.preemption.victims"] =
            JoinStrings(candidate.preemption_victims, ",");
      }
    } else {
      instance_it->labels.erase("comet.placement.defer_reason");
      instance_it->labels.erase("comet.preemption.victims");
    }
  }
}

}  // namespace

DesiredState ImportPlaneBundle(const std::string& bundle_dir) {
  const std::filesystem::path bundle_path(bundle_dir);
  const json plane_json = ReadJsonFile(bundle_path / "plane.json");
  const json infer_json = ReadJsonFile(bundle_path / "infer.json");
  const std::filesystem::path workers_dir = bundle_path / "workers";

  if (!std::filesystem::exists(workers_dir) || !std::filesystem::is_directory(workers_dir)) {
    throw std::runtime_error("bundle is missing workers directory at '" + workers_dir.string() + "'");
  }

  DesiredState state;
  state.plane_name = RequiredString(plane_json, "name", "plane.json");
  state.plane_shared_disk_name = "plane-" + state.plane_name + "-shared";
  state.control_root = OptionalString(
      plane_json,
      "control_root",
      "/comet/shared/control/" + state.plane_name);
  state.plane_mode =
      ParsePlaneMode(OptionalString(plane_json, "plane_mode", "compute"));
  if (const auto placement_target = OptionalStringOpt(plane_json, "placement_target")) {
    state.placement_target = *placement_target;
  }
  if (const auto bootstrap_model = OptionalObject(plane_json, "bootstrap_model")) {
    BootstrapModelSpec spec;
    spec.model_id = OptionalString(*bootstrap_model, "model_id", "");
    spec.materialization_mode = OptionalString(
        *bootstrap_model,
        "materialization_mode",
        spec.materialization_mode);
    if (const auto served_model_name = OptionalStringOpt(*bootstrap_model, "served_model_name")) {
      spec.served_model_name = *served_model_name;
    }
    if (const auto local_path = OptionalStringOpt(*bootstrap_model, "local_path")) {
      spec.local_path = *local_path;
    }
    if (const auto source_url = OptionalStringOpt(*bootstrap_model, "source_url")) {
      spec.source_url = *source_url;
    }
    if (bootstrap_model->contains("source_urls") && (*bootstrap_model)["source_urls"].is_array()) {
      spec.source_urls = (*bootstrap_model)["source_urls"].get<std::vector<std::string>>();
    }
    if (const auto target_filename = OptionalStringOpt(*bootstrap_model, "target_filename")) {
      spec.target_filename = *target_filename;
    }
    if (const auto sha256 = OptionalStringOpt(*bootstrap_model, "sha256")) {
      spec.sha256 = *sha256;
    }
    state.bootstrap_model = std::move(spec);
  }
  if (const auto interaction = OptionalObject(plane_json, "interaction")) {
    InteractionSettings settings;
    if (const auto system_prompt = OptionalStringOpt(*interaction, "system_prompt")) {
      settings.system_prompt = *system_prompt;
    }
    if (const auto analysis_system_prompt =
            OptionalStringOpt(*interaction, "analysis_system_prompt")) {
      settings.analysis_system_prompt = *analysis_system_prompt;
    }
    settings.default_response_language =
        OptionalString(*interaction, "default_response_language", settings.default_response_language);
    if (interaction->contains("supported_response_languages") &&
        (*interaction).at("supported_response_languages").is_array()) {
      settings.supported_response_languages =
          (*interaction).at("supported_response_languages").get<std::vector<std::string>>();
    }
    settings.follow_user_language =
        interaction->value("follow_user_language", settings.follow_user_language);
    if (const auto completion_policy = OptionalObject(*interaction, "completion_policy")) {
      InteractionSettings::CompletionPolicy policy;
      policy.response_mode =
          OptionalString(*completion_policy, "response_mode", policy.response_mode);
      policy.max_tokens = OptionalInt(*completion_policy, "max_tokens", policy.max_tokens);
      if (const auto target_completion_tokens =
              OptionalIntOpt(*completion_policy, "target_completion_tokens")) {
        policy.target_completion_tokens = *target_completion_tokens;
      }
      policy.max_continuations =
          OptionalInt(*completion_policy, "max_continuations", policy.max_continuations);
      policy.max_total_completion_tokens = OptionalInt(
          *completion_policy,
          "max_total_completion_tokens",
          policy.max_total_completion_tokens);
      policy.max_elapsed_time_ms =
          OptionalInt(*completion_policy, "max_elapsed_time_ms", policy.max_elapsed_time_ms);
      if (const auto semantic_goal = OptionalStringOpt(*completion_policy, "semantic_goal")) {
        policy.semantic_goal = *semantic_goal;
      }
      settings.completion_policy = std::move(policy);
    }
    if (const auto completion_policy = OptionalObject(*interaction, "long_completion_policy")) {
      InteractionSettings::CompletionPolicy policy;
      policy.response_mode =
          OptionalString(*completion_policy, "response_mode", policy.response_mode);
      policy.max_tokens = OptionalInt(*completion_policy, "max_tokens", policy.max_tokens);
      if (const auto target_completion_tokens =
              OptionalIntOpt(*completion_policy, "target_completion_tokens")) {
        policy.target_completion_tokens = *target_completion_tokens;
      }
      policy.max_continuations =
          OptionalInt(*completion_policy, "max_continuations", policy.max_continuations);
      policy.max_total_completion_tokens = OptionalInt(
          *completion_policy,
          "max_total_completion_tokens",
          policy.max_total_completion_tokens);
      policy.max_elapsed_time_ms =
          OptionalInt(*completion_policy, "max_elapsed_time_ms", policy.max_elapsed_time_ms);
      if (const auto semantic_goal = OptionalStringOpt(*completion_policy, "semantic_goal")) {
        policy.semantic_goal = *semantic_goal;
      }
      settings.long_completion_policy = std::move(policy);
    }
    if (const auto completion_policy =
            OptionalObject(*interaction, "analysis_completion_policy")) {
      InteractionSettings::CompletionPolicy policy;
      policy.response_mode =
          OptionalString(*completion_policy, "response_mode", policy.response_mode);
      policy.max_tokens = OptionalInt(*completion_policy, "max_tokens", policy.max_tokens);
      if (const auto target_completion_tokens =
              OptionalIntOpt(*completion_policy, "target_completion_tokens")) {
        policy.target_completion_tokens = *target_completion_tokens;
      }
      policy.max_continuations =
          OptionalInt(*completion_policy, "max_continuations", policy.max_continuations);
      policy.max_total_completion_tokens = OptionalInt(
          *completion_policy,
          "max_total_completion_tokens",
          policy.max_total_completion_tokens);
      policy.max_elapsed_time_ms =
          OptionalInt(*completion_policy, "max_elapsed_time_ms", policy.max_elapsed_time_ms);
      if (const auto semantic_goal = OptionalStringOpt(*completion_policy, "semantic_goal")) {
        policy.semantic_goal = *semantic_goal;
      }
      settings.analysis_completion_policy = std::move(policy);
    }
    if (const auto completion_policy =
            OptionalObject(*interaction, "analysis_long_completion_policy")) {
      InteractionSettings::CompletionPolicy policy;
      policy.response_mode =
          OptionalString(*completion_policy, "response_mode", policy.response_mode);
      policy.max_tokens = OptionalInt(*completion_policy, "max_tokens", policy.max_tokens);
      if (const auto target_completion_tokens =
              OptionalIntOpt(*completion_policy, "target_completion_tokens")) {
        policy.target_completion_tokens = *target_completion_tokens;
      }
      policy.max_continuations =
          OptionalInt(*completion_policy, "max_continuations", policy.max_continuations);
      policy.max_total_completion_tokens = OptionalInt(
          *completion_policy,
          "max_total_completion_tokens",
          policy.max_total_completion_tokens);
      policy.max_elapsed_time_ms =
          OptionalInt(*completion_policy, "max_elapsed_time_ms", policy.max_elapsed_time_ms);
      if (const auto semantic_goal = OptionalStringOpt(*completion_policy, "semantic_goal")) {
        policy.semantic_goal = *semantic_goal;
      }
      settings.analysis_long_completion_policy = std::move(policy);
    }
    state.interaction = std::move(settings);
  }
  const int shared_disk_gb = OptionalInt(plane_json, "shared_disk_gb", 200);

  if (plane_json.contains("runtime") &&
      !plane_json.at("runtime").is_null() &&
      !plane_json.at("runtime").is_object() &&
      !plane_json.at("runtime").is_string()) {
    throw std::runtime_error("field 'runtime' must be an object or a string");
  }

  if (plane_json.contains("runtime") && plane_json.at("runtime").is_object()) {
    const auto runtime = plane_json.at("runtime");
    state.inference.primary_infer_node =
        OptionalString(runtime, "primary_infer_node", state.inference.primary_infer_node);
    state.inference.runtime_engine =
        OptionalString(runtime, "runtime_engine", state.inference.runtime_engine);
    state.inference.net_if = OptionalString(runtime, "net_if", state.inference.net_if);
    state.inference.models_root =
        OptionalString(runtime, "models_root", state.inference.models_root);
    state.inference.model_cache_dir =
        OptionalString(runtime, "model_cache_dir", state.inference.model_cache_dir);
    state.inference.runtime_log_dir =
        OptionalString(runtime, "runtime_log_dir", state.inference.runtime_log_dir);
    state.inference.api_port =
        OptionalInt(
            runtime,
            "api_port",
            OptionalInt(runtime, "llama_port", state.inference.api_port));
    state.inference.max_model_len =
        OptionalInt(
            runtime,
            "max_model_len",
            OptionalInt(runtime, "llama_ctx_size", state.inference.max_model_len));
    state.inference.tensor_parallel_size =
        OptionalInt(runtime, "tensor_parallel_size", state.inference.tensor_parallel_size);
    state.inference.pipeline_parallel_size =
        OptionalInt(runtime, "pipeline_parallel_size", state.inference.pipeline_parallel_size);
    state.inference.max_num_seqs =
        OptionalInt(runtime, "max_num_seqs", state.inference.max_num_seqs);
    state.inference.gpu_memory_utilization =
        OptionalDouble(
            runtime,
            "gpu_memory_utilization",
            state.inference.gpu_memory_utilization);
    state.inference.enforce_eager =
        OptionalBool(runtime, "enforce_eager", state.inference.enforce_eager);
    state.inference.gguf_cache_dir =
        OptionalString(
            runtime,
            "gguf_cache_dir",
            OptionalString(runtime, "model_cache_dir", state.inference.gguf_cache_dir));
    state.inference.infer_log_dir =
        OptionalString(
            runtime,
            "infer_log_dir",
            OptionalString(runtime, "runtime_log_dir", state.inference.infer_log_dir));
    state.inference.llama_port =
        OptionalInt(
            runtime,
            "llama_port",
            OptionalInt(runtime, "api_port", state.inference.llama_port));
    state.inference.llama_ctx_size =
        OptionalInt(
            runtime,
            "llama_ctx_size",
            OptionalInt(runtime, "max_model_len", state.inference.llama_ctx_size));
    state.inference.llama_threads =
        OptionalInt(runtime, "llama_threads", state.inference.llama_threads);
    state.inference.llama_gpu_layers =
        OptionalInt(runtime, "llama_gpu_layers", state.inference.llama_gpu_layers);
    state.inference.inference_healthcheck_retries = OptionalInt(
        runtime,
        "inference_healthcheck_retries",
        state.inference.inference_healthcheck_retries);
    state.inference.inference_healthcheck_interval_sec = OptionalInt(
        runtime,
        "inference_healthcheck_interval_sec",
        state.inference.inference_healthcheck_interval_sec);
    if (state.inference.model_cache_dir.empty()) {
      state.inference.model_cache_dir = state.inference.gguf_cache_dir;
    }
    if (state.inference.gguf_cache_dir.empty()) {
      state.inference.gguf_cache_dir = state.inference.model_cache_dir;
    }
    if (state.inference.runtime_log_dir.empty()) {
      state.inference.runtime_log_dir = state.inference.infer_log_dir;
    }
    if (state.inference.infer_log_dir.empty()) {
      state.inference.infer_log_dir = state.inference.runtime_log_dir;
    }
    if (state.inference.api_port <= 0) {
      state.inference.api_port = state.inference.llama_port;
    }
    if (state.inference.llama_port <= 0) {
      state.inference.llama_port = state.inference.api_port;
    }
    if (state.inference.max_model_len <= 0) {
      state.inference.max_model_len = state.inference.llama_ctx_size;
    }
    if (state.inference.llama_ctx_size <= 0) {
      state.inference.llama_ctx_size = state.inference.max_model_len;
    }
  }

  if (const auto gateway = OptionalObject(plane_json, "gateway")) {
    state.gateway.listen_host =
        OptionalString(*gateway, "listen_host", state.gateway.listen_host);
    state.gateway.listen_port =
        OptionalInt(*gateway, "listen_port", state.gateway.listen_port);
    state.gateway.server_name =
        OptionalString(*gateway, "server_name", state.gateway.server_name);
  }

  state.nodes = ParseNodes(plane_json);
  std::map<std::string, NodeInventory> nodes_by_name;
  for (const auto& node : state.nodes) {
    nodes_by_name[node.name] = node;
  }

  for (const auto& node : state.nodes) {
    state.disks.push_back(MakeDisk(
        state.plane_shared_disk_name,
        DiskKind::PlaneShared,
        state.plane_name,
        state.plane_name,
        node.name,
        "/var/lib/comet/disks/planes/" + state.plane_name + "/shared",
        "/comet/shared",
        shared_disk_gb));
  }

  const std::string infer_name = RequiredString(infer_json, "name", "infer.json");
  std::string default_infer_node_name = state.nodes.front().name;
  for (const auto& node : state.nodes) {
    if (NodeSupportsInstanceRole(node, InstanceRole::Infer)) {
      default_infer_node_name = node.name;
      break;
    }
  }
  const std::string infer_node_name =
      OptionalString(infer_json, "node", default_infer_node_name);
  ValidateNodeExists(nodes_by_name, infer_node_name, "infer.json");
  if (!NodeSupportsInstanceRole(nodes_by_name.at(infer_node_name), InstanceRole::Infer)) {
    throw std::runtime_error(
        "infer.json targets node '" + infer_node_name + "' which is not infer-capable");
  }
  if (state.inference.primary_infer_node.empty()) {
    state.inference.primary_infer_node = infer_node_name;
  }

  InstanceSpec infer;
  infer.name = infer_name;
  infer.role = InstanceRole::Infer;
  infer.plane_name = state.plane_name;
  infer.node_name = infer_node_name;
  infer.image = RequiredString(infer_json, "image", "infer.json");
  infer.command = "/runtime/infer/entrypoint.sh";
  infer.private_disk_name = infer.name + "-private";
  infer.shared_disk_name = state.plane_shared_disk_name;
  infer.private_disk_size_gb = OptionalInt(infer_json, "private_disk_gb", 80);
  const bool use_vllm_runtime = state.inference.runtime_engine == "vllm";

  infer.environment = {
      {"COMET_PLANE_NAME", state.plane_name},
      {"COMET_INSTANCE_NAME", infer.name},
      {"COMET_INSTANCE_ROLE", "infer"},
      {"COMET_NODE_NAME", infer.node_name},
      {"COMET_INFER_BOOT_MODE", "launch-runtime"},
      {"COMET_INFER_RUNTIME_BACKEND", use_vllm_runtime ? "worker-vllm" : "auto"},
      {"COMET_CONTROLLER_URL", "http://controller.internal:8080"},
      {"COMET_CONTROL_ROOT", state.control_root},
      {"COMET_INFER_RUNTIME_CONFIG", state.control_root + "/infer-runtime.json"},
      {"COMET_INFERENCE_PORT", std::to_string(state.inference.api_port)},
      {"COMET_GATEWAY_PORT", std::to_string(state.gateway.listen_port)},
      {"COMET_SHARED_DISK_PATH", "/comet/shared"},
      {"COMET_PRIVATE_DISK_PATH", "/comet/private"},
  };
  infer.labels = {
      {"comet.plane", state.plane_name},
      {"comet.role", "infer"},
      {"comet.node", infer.node_name},
  };

  state.disks.push_back(MakeDisk(
      infer.private_disk_name,
      DiskKind::InferPrivate,
      state.plane_name,
      infer.name,
      infer.node_name,
      "/var/lib/comet/disks/instances/" + infer.name + "/private",
      "/comet/private",
      infer.private_disk_size_gb));
  state.instances.push_back(infer);

  std::vector<std::filesystem::path> worker_files;
  for (const auto& entry : std::filesystem::directory_iterator(workers_dir)) {
    if (entry.is_regular_file() && entry.path().extension() == ".json") {
      worker_files.push_back(entry.path());
    }
  }
  std::sort(worker_files.begin(), worker_files.end());
  if (worker_files.empty()) {
    throw std::runtime_error("bundle must contain at least one worker json file");
  }

  std::set<std::string> instance_names;
  std::map<std::pair<std::string, std::string>, PlacementUsage> placement_usage;
  if (!instance_names.insert(infer.name).second) {
    throw std::runtime_error("duplicate instance name '" + infer.name + "'");
  }
  for (const auto& worker_file : worker_files) {
    const json worker_json = ReadJsonFile(worker_file);
    InstanceSpec worker;
    worker.name = RequiredString(worker_json, "name", worker_file.string());
    if (!instance_names.insert(worker.name).second) {
      throw std::runtime_error("duplicate instance name '" + worker.name + "'");
    }
    worker.role = InstanceRole::Worker;
    worker.plane_name = state.plane_name;
    worker.image = RequiredString(worker_json, "image", worker_file.string());
    worker.command = "/runtime/worker/entrypoint.sh";
    worker.private_disk_name = worker.name + "-private";
    worker.shared_disk_name = state.plane_shared_disk_name;
    worker.private_disk_size_gb = OptionalInt(worker_json, "private_disk_gb", 40);
    worker.gpu_fraction = OptionalDouble(worker_json, "gpu_fraction", 1.0);
    worker.gpu_device = OptionalStringOpt(worker_json, "gpu_device");
    worker.placement_mode =
        worker_json.contains("placement_mode")
            ? ParsePlacementMode(RequiredString(worker_json, "placement_mode", worker_file.string()))
            : (worker.gpu_device.has_value() ? PlacementMode::Manual : PlacementMode::Auto);
    worker.share_mode = worker_json.contains("share_mode")
                            ? ParseGpuShareMode(RequiredString(worker_json, "share_mode", worker_file.string()))
                            : (worker.gpu_fraction < 1.0 ? GpuShareMode::Shared : GpuShareMode::Exclusive);
    worker.priority = OptionalInt(worker_json, "priority", 100);
    worker.preemptible = OptionalBool(worker_json, "preemptible", false);
    worker.memory_cap_mb = OptionalIntOpt(worker_json, "memory_cap_mb");

    const std::optional<std::string> requested_node_name =
        OptionalStringOpt(worker_json, "node");
    const std::optional<std::string> requested_gpu_device = worker.gpu_device;
    const bool node_explicit = requested_node_name.has_value();
    const bool gpu_explicit = requested_gpu_device.has_value();

    if (gpu_explicit && !node_explicit) {
      throw std::runtime_error(
          worker_file.string() +
          " sets gpu_device without node; explicit gpu_device requires an explicit node");
    }

    std::string placement_origin = "manual";
    int auto_placement_score = 0;
    std::string placement_action = "manual";
    if (worker.placement_mode == PlacementMode::Manual) {
      if (!node_explicit || !gpu_explicit) {
        throw std::runtime_error(
            worker_file.string() +
            " uses placement_mode=manual but does not specify both node and gpu_device");
      }
      worker.node_name = *requested_node_name;
      ValidateNodeExists(nodes_by_name, worker.node_name, worker_file.string());
      const NodeInventory& node = nodes_by_name.at(worker.node_name);
      if (!NodeSupportsInstanceRole(node, InstanceRole::Worker)) {
        throw std::runtime_error(
            worker_file.string() + " targets node '" + worker.node_name +
            "' which is not worker-capable");
      }
      ValidateGpuExists(node, worker.gpu_device, worker_file.string());
    } else if (worker.placement_mode == PlacementMode::Movable && node_explicit && gpu_explicit) {
      worker.node_name = *requested_node_name;
      ValidateNodeExists(nodes_by_name, worker.node_name, worker_file.string());
      const NodeInventory& node = nodes_by_name.at(worker.node_name);
      if (!NodeSupportsInstanceRole(node, InstanceRole::Worker)) {
        throw std::runtime_error(
            worker_file.string() + " targets node '" + worker.node_name +
            "' which is not worker-capable");
      }
      ValidateGpuExists(node, worker.gpu_device, worker_file.string());
      placement_origin = "requested";
      placement_action = "requested";
    } else {
      const bool strict_node_constraint = worker.placement_mode == PlacementMode::Auto && node_explicit;
      const bool strict_gpu_constraint = worker.placement_mode == PlacementMode::Auto && gpu_explicit;
      const auto placement = SelectAutoPlacement(
          state.nodes,
          placement_usage,
          worker,
          state,
          requested_node_name,
          requested_gpu_device,
          strict_node_constraint,
          strict_gpu_constraint);
      if (!placement.has_value()) {
        throw std::runtime_error(
            worker_file.string() +
            " could not be auto-placed onto any node/gpu that satisfies current scheduler constraints");
      }
      worker.node_name = placement->node_name;
      worker.gpu_device = placement->gpu_device;
      placement_origin = "auto";
      auto_placement_score = placement->score;
      const bool relocated =
          requested_node_name.has_value() && requested_gpu_device.has_value() &&
          (*requested_node_name != worker.node_name || *requested_gpu_device != *worker.gpu_device);
      if (placement->upgrade_to_exclusive) {
        worker.share_mode = GpuShareMode::Exclusive;
        worker.gpu_fraction = 1.0;
        placement_action = relocated ? "relocate-and-upgrade-to-exclusive"
                                     : "upgrade-to-exclusive";
      } else {
        placement_action = relocated ? "relocate" : "auto-assign";
      }
    }

    if (worker.node_name == infer.node_name) {
      worker.depends_on.push_back(infer.name);
    }

    worker.environment = {
        {"COMET_PLANE_NAME", state.plane_name},
        {"COMET_INSTANCE_NAME", worker.name},
        {"COMET_INSTANCE_ROLE", "worker"},
        {"COMET_NODE_NAME", worker.node_name},
        {"COMET_GPU_DEVICE", worker.gpu_device.value_or("")},
        {"COMET_WORKER_BOOT_MODE", use_vllm_runtime ? "vllm-openai" : "llama-load"},
        {"COMET_CONTROL_ROOT", state.control_root},
        {"COMET_SHARED_DISK_PATH", "/comet/shared"},
        {"COMET_PRIVATE_DISK_PATH", "/comet/private"},
        {"COMET_WORKER_RUNTIME_STATUS_PATH", "/comet/private/worker-runtime-status.json"},
    };
    worker.labels = {
        {"comet.plane", state.plane_name},
        {"comet.role", "worker"},
        {"comet.node", worker.node_name},
        {"comet.placement", placement_origin},
        {"comet.placement.mode", ToString(worker.placement_mode)},
        {"comet.placement.action", placement_action},
    };
    if (placement_origin == "auto") {
      worker.labels["comet.placement.score"] = std::to_string(auto_placement_score);
    }
    if (requested_node_name.has_value()) {
      worker.labels["comet.requested.node"] = *requested_node_name;
    }
    if (requested_gpu_device.has_value()) {
      worker.labels["comet.requested.gpu"] = *requested_gpu_device;
    }

    state.disks.push_back(MakeDisk(
        worker.private_disk_name,
        DiskKind::WorkerPrivate,
        state.plane_name,
        worker.name,
        worker.node_name,
        "/var/lib/comet/disks/instances/" + worker.name + "/private",
        "/comet/private",
        worker.private_disk_size_gb));
    state.instances.push_back(worker);
    state.runtime_gpu_nodes.push_back(
        RuntimeGpuNode{
            worker.name,
            worker.node_name,
            worker.gpu_device.value_or(""),
            worker.placement_mode,
            worker.share_mode,
            worker.gpu_fraction,
            worker.priority,
            worker.preemptible,
            worker.memory_cap_mb,
            true,
        });
    ReservePlacement(&placement_usage, worker);
  }

  state.worker_group.group_id = state.inference.worker_group_id.empty()
                                    ? state.plane_name + "-workers"
                                    : state.inference.worker_group_id;
  state.worker_group.infer_instance_name = infer.name;
  state.worker_group.distributed_backend = state.inference.distributed_backend;
  state.worker_group.rendezvous_host = infer.name;
  state.worker_group.rendezvous_port = state.inference.rendezvous_port;
  state.worker_group.worker_selection_policy = state.inference.worker_selection_policy;
  state.worker_group.expected_workers = 0;
  for (const auto& worker : state.instances) {
    if (worker.role != InstanceRole::Worker) {
      continue;
    }
    WorkerGroupMemberSpec member;
    member.name = worker.name;
    member.node_name = worker.node_name;
    member.gpu_device = worker.gpu_device.value_or("");
    member.rank = state.worker_group.expected_workers++;
    member.gpu_fraction = worker.gpu_fraction;
    member.share_mode = worker.share_mode;
    member.priority = worker.priority;
    member.preemptible = worker.preemptible;
    member.memory_cap_mb = worker.memory_cap_mb;
    member.enabled = true;
    member.leader = member.rank == 0;
    state.worker_group.members.push_back(std::move(member));
  }

  ApplyMovableSchedulerDecisions(&state);
  return ResolvePlacementTargetAliases(std::move(state));
}

}  // namespace comet
