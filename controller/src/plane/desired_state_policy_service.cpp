#include "plane/desired_state_policy_service.h"

#include <algorithm>
#include <cmath>
#include <limits>
#include <stdexcept>

#include "comet/state/worker_group_topology.h"

namespace comet::controller {

void DesiredStatePolicyService::ApplyRegisteredHostExecutionModes(
    comet::ControllerStore& store,
    comet::DesiredState* desired_state) const {
  if (desired_state == nullptr) {
    return;
  }
  for (auto& node : desired_state->nodes) {
    if (const auto host = store.LoadRegisteredHost(node.name); host.has_value() &&
        !host->execution_mode.empty()) {
      node.execution_mode = comet::ParseHostExecutionMode(host->execution_mode);
    }
  }
}

std::string DesiredStatePolicyService::CurrentControllerPlatform() const {
#if defined(_WIN32)
  return "windows";
#elif defined(__APPLE__)
  return "macos";
#elif defined(__linux__)
  return "linux";
#else
  return "unknown";
#endif
}

const comet::NodeInventory* DesiredStatePolicyService::FindPlaneNodeInventory(
    const comet::DesiredState& desired_state,
    const std::string& node_name) const {
  for (const auto& node : desired_state.nodes) {
    if (node.name == node_name) {
      return &node;
    }
  }
  return nullptr;
}

bool DesiredStatePolicyService::PlaneNodeUsesGpuRuntime(
    const comet::DesiredState& desired_state,
    const std::string& node_name) const {
  for (const auto& runtime_gpu_node : desired_state.runtime_gpu_nodes) {
    if (runtime_gpu_node.enabled && runtime_gpu_node.node_name == node_name) {
      return true;
    }
  }
  for (const auto& instance : desired_state.instances) {
    if (instance.node_name == node_name &&
        (instance.role == comet::InstanceRole::Worker ||
         (instance.gpu_device.has_value() && !instance.gpu_device->empty()))) {
      return true;
    }
  }
  if (const auto* node = FindPlaneNodeInventory(desired_state, node_name);
      node != nullptr && !node->gpu_devices.empty()) {
    return true;
  }
  return false;
}

std::optional<std::string>
DesiredStatePolicyService::DescribeUnsupportedControllerLocalRuntime(
    const comet::DesiredState& desired_state,
    const std::string& node_name) const {
  if (node_name != "local-hostd" && node_name != "controller-local") {
    return std::nullopt;
  }

  const std::string controller_platform = CurrentControllerPlatform();
  if (const auto* node = FindPlaneNodeInventory(desired_state, node_name);
      node != nullptr && !node->platform.empty() && node->platform != controller_platform) {
    return "Local host '" + node_name + "' is running on '" + controller_platform +
           "', but the plane targets platform '" + node->platform + "'";
  }

  if (controller_platform == "macos" &&
      PlaneNodeUsesGpuRuntime(desired_state, node_name)) {
    return "Local host '" + node_name +
           "' is running on macOS, but this plane requires Linux/NVIDIA GPU runtime";
  }

  return std::nullopt;
}

void DesiredStatePolicyService::ValidateDesiredStateForControllerAdmission(
    const comet::DesiredState& desired_state) const {
  for (const auto& node : desired_state.nodes) {
    if (const auto detail =
            DescribeUnsupportedControllerLocalRuntime(desired_state, node.name);
        detail.has_value()) {
      throw std::invalid_argument(*detail);
    }
  }
}

bool DesiredStatePolicyService::NodeAllowsInstanceRole(
    comet::HostExecutionMode execution_mode,
    comet::InstanceRole role) const {
  switch (execution_mode) {
    case comet::HostExecutionMode::InferOnly:
      return role == comet::InstanceRole::Infer ||
             role == comet::InstanceRole::App;
    case comet::HostExecutionMode::WorkerOnly:
      return role == comet::InstanceRole::Worker;
    case comet::HostExecutionMode::Mixed:
      return true;
  }
  return true;
}

void DesiredStatePolicyService::ValidateDesiredStateExecutionModes(
    const comet::DesiredState& desired_state) const {
  std::map<std::string, comet::HostExecutionMode> node_modes;
  for (const auto& node : desired_state.nodes) {
    node_modes[node.name] = node.execution_mode;
  }
  for (const auto& instance : desired_state.instances) {
    const auto node_it = node_modes.find(instance.node_name);
    if (node_it == node_modes.end()) {
      continue;
    }
    if (!NodeAllowsInstanceRole(node_it->second, instance.role)) {
      throw std::invalid_argument(
          "instance '" + instance.name + "' role '" +
          comet::ToString(instance.role) + "' is not allowed on node '" +
          instance.node_name + "' execution_mode='" +
          comet::ToString(node_it->second) + "'");
    }
  }
}

std::string DesiredStatePolicyService::EffectiveWorkerSelectionPolicy(
    const comet::DesiredState& state) const {
  if (!state.worker_group.worker_selection_policy.empty()) {
    return state.worker_group.worker_selection_policy;
  }
  if (!state.inference.worker_selection_policy.empty()) {
    return state.inference.worker_selection_policy;
  }
  return "prefer-free-then-share";
}

int DesiredStatePolicyService::AutoPlacementPolicyRank(
    const std::string& policy,
    const AutoPlacementDecision& candidate) const {
  if (policy == "prefer-free-then-share") {
    return candidate.idle_target ? 0 : 1;
  }
  return candidate.idle_target ? 0 : 1;
}

int DesiredStatePolicyService::ScoreAutoPlacementCandidate(
    const comet::NodeInventory& node,
    const std::string& gpu_device,
    const PlacementUsage& usage,
    const comet::InferenceRuntimeSettings& inference,
    int observed_free_vram_mb,
    int observed_utilization_pct,
    const std::optional<std::string>& preferred_node_name,
    const std::optional<std::string>& preferred_gpu_device) const {
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
  if (observed_free_vram_mb >= 0) {
    score += std::max(0, observed_free_vram_mb) / 1024;
  } else {
    const auto memory_it = node.gpu_memory_mb.find(gpu_device);
    if (memory_it != node.gpu_memory_mb.end()) {
      score += std::max(0, memory_it->second - usage.allocated_memory_mb) / 1024;
    }
  }
  if (observed_utilization_pct >= 0) {
    score += std::max(0, 100 - observed_utilization_pct) / 5;
  }
  return score;
}

namespace {

bool HybridGpuAlreadyAssigned(
    const comet::DesiredState& desired_state,
    const comet::InstanceSpec& current_worker,
    const std::string& node_name,
    const std::string& gpu_device) {
  if (!comet::HybridDataParallelEnabled(desired_state.inference)) {
    return false;
  }
  for (const auto& instance : desired_state.instances) {
    if (instance.role != comet::InstanceRole::Worker) {
      continue;
    }
    if (instance.name == current_worker.name || instance.node_name != node_name ||
        !instance.gpu_device.has_value() || instance.gpu_device->empty()) {
      continue;
    }
    if (*instance.gpu_device == gpu_device) {
      return true;
    }
  }
  return false;
}

bool UsesLlamaRpcRuntime(const comet::DesiredState& desired_state) {
  return desired_state.inference.runtime_engine == "llama.cpp" &&
         desired_state.inference.distributed_backend == "llama_rpc";
}

std::string InferInstanceNameForWorker(const comet::InstanceSpec& instance) {
  const auto it = instance.environment.find("COMET_INFER_INSTANCE_NAME");
  if (it == instance.environment.end()) {
    return {};
  }
  return it->second;
}

}  // namespace

void DesiredStatePolicyService::ReservePlacement(
    std::map<std::pair<std::string, std::string>, PlacementUsage>* placement_usage,
    const comet::InstanceSpec& worker) const {
  if (placement_usage == nullptr || !worker.gpu_device.has_value() ||
      worker.gpu_device->empty()) {
    return;
  }
  auto& usage = (*placement_usage)[{worker.node_name, *worker.gpu_device}];
  usage.allocated_fraction += worker.gpu_fraction;
  usage.allocated_memory_mb += worker.memory_cap_mb.value_or(0);
}

const comet::InstanceSpec* DesiredStatePolicyService::FindInferInstance(
    const comet::DesiredState& desired_state) const {
  const auto it = std::find_if(
      desired_state.instances.begin(),
      desired_state.instances.end(),
      [](const comet::InstanceSpec& instance) {
        return instance.role == comet::InstanceRole::Infer;
      });
  return it == desired_state.instances.end() ? nullptr : &*it;
}

void DesiredStatePolicyService::RefreshDerivedWorkerMetadata(
    comet::DesiredState* desired_state) const {
  if (desired_state == nullptr) {
    return;
  }

  desired_state->runtime_gpu_nodes.clear();
  desired_state->worker_group.members.clear();

  for (const auto& instance : desired_state->instances) {
    if (instance.role != comet::InstanceRole::Worker) {
      continue;
    }

    desired_state->runtime_gpu_nodes.push_back(
        comet::RuntimeGpuNode{
            instance.name,
            instance.node_name,
            instance.gpu_device.value_or(""),
            instance.placement_mode,
            instance.share_mode,
            instance.gpu_fraction,
            instance.priority,
            instance.preemptible,
            instance.memory_cap_mb,
            true,
        });

    comet::WorkerGroupMemberSpec member;
    member.name = instance.name;
    member.infer_instance_name = InferInstanceNameForWorker(instance);
    member.node_name = instance.node_name;
    member.gpu_device = instance.gpu_device.value_or("");
    if (const auto rpc_port_it = instance.environment.find("COMET_WORKER_RPC_PORT");
        rpc_port_it != instance.environment.end() && !rpc_port_it->second.empty()) {
      member.rpc_port = std::stoi(rpc_port_it->second);
    }
    member.rank = static_cast<int>(desired_state->worker_group.members.size());
    member.gpu_fraction = instance.gpu_fraction;
    member.share_mode = instance.share_mode;
    member.priority = instance.priority;
    member.preemptible = instance.preemptible;
    member.memory_cap_mb = instance.memory_cap_mb;
    member.enabled = true;
    desired_state->worker_group.members.push_back(std::move(member));
  }

  if (desired_state->worker_group.expected_workers <= 0) {
    desired_state->worker_group.expected_workers = comet::DefaultWorkersPerReplica(
        desired_state->inference,
        comet::EligibleWorkerMemberCount(desired_state->worker_group));
  }
  if (desired_state->worker_group.infer_instance_name.empty()) {
    if (const auto* infer = FindInferInstance(*desired_state); infer != nullptr) {
      desired_state->worker_group.infer_instance_name = infer->name;
    }
  }
  if (desired_state->worker_group.rendezvous_host.empty()) {
    desired_state->worker_group.rendezvous_host =
        desired_state->worker_group.infer_instance_name;
  }
  if (UsesLlamaRpcRuntime(*desired_state)) {
    std::map<std::string, int> replica_index_by_infer;
    std::map<std::string, int> replica_size_by_infer;
    int next_replica_index = 0;
    for (const auto& member : desired_state->worker_group.members) {
      if (!member.enabled) {
        continue;
      }
      const std::string infer_name =
          member.infer_instance_name.empty() ? desired_state->worker_group.infer_instance_name
                                             : member.infer_instance_name;
      if (replica_index_by_infer.find(infer_name) == replica_index_by_infer.end()) {
        replica_index_by_infer.emplace(infer_name, next_replica_index++);
      }
      ++replica_size_by_infer[infer_name];
    }
    std::map<std::string, int> local_rank_by_infer;
    for (auto& member : desired_state->worker_group.members) {
      if (!member.enabled) {
        continue;
      }
      if (member.infer_instance_name.empty()) {
        member.infer_instance_name = desired_state->worker_group.infer_instance_name;
      }
      const int local_rank = local_rank_by_infer[member.infer_instance_name]++;
      member.rank = local_rank;
      member.replica_group_id = member.infer_instance_name;
      member.replica_index = replica_index_by_infer[member.infer_instance_name];
      member.replica_size = replica_size_by_infer[member.infer_instance_name];
      member.replica_leader = local_rank == 0;
      member.data_parallel_rank = member.replica_index;
      member.data_parallel_size = static_cast<int>(replica_index_by_infer.size());
      member.data_parallel_size_local = 1;
      member.data_parallel_start_rank = member.replica_index;
      member.data_parallel_api_endpoint = false;
      member.data_parallel_head_address =
          desired_state->worker_group.rendezvous_host.empty()
              ? desired_state->worker_group.infer_instance_name
              : desired_state->worker_group.rendezvous_host;
      member.data_parallel_rpc_port =
          desired_state->worker_group.rendezvous_port > 0
              ? desired_state->worker_group.rendezvous_port + 100
              : 29600;
      member.leader = member.replica_leader && member.replica_index == 0;
    }
    return;
  }
  comet::ValidateReplicaPacking(desired_state->inference, desired_state->worker_group);
  comet::AssignReplicaTopology(desired_state->inference, &desired_state->worker_group);
}

void DesiredStatePolicyService::ApplyObservedHostGpuInventory(
    comet::ControllerStore& store,
    comet::DesiredState* desired_state) const {
  if (desired_state == nullptr) {
    return;
  }

  const auto observations = store.LoadHostObservations();
  std::map<std::string, comet::GpuTelemetrySnapshot> telemetry_by_node;
  for (const auto& observation : observations) {
    if (const auto telemetry = runtime_support_.ParseGpuTelemetry(observation);
        telemetry.has_value()) {
      telemetry_by_node[observation.node_name] = *telemetry;
    }
  }

  for (auto& node : desired_state->nodes) {
    const auto telemetry_it = telemetry_by_node.find(node.name);
    if (telemetry_it == telemetry_by_node.end()) {
      continue;
    }
    for (const auto& device : telemetry_it->second.devices) {
      if (device.gpu_device.empty()) {
        continue;
      }
      if (std::find(node.gpu_devices.begin(), node.gpu_devices.end(), device.gpu_device) ==
          node.gpu_devices.end()) {
        node.gpu_devices.push_back(device.gpu_device);
      }
      if (device.total_vram_mb > 0) {
        node.gpu_memory_mb[device.gpu_device] = device.total_vram_mb;
      }
    }
  }
}

std::optional<DesiredStatePolicyService::AutoPlacementDecision>
DesiredStatePolicyService::SelectAutoPlacement(
    const comet::DesiredState& desired_state,
    const std::map<std::pair<std::string, std::string>, PlacementUsage>& placement_usage,
    const std::map<std::pair<std::string, std::string>, std::pair<int, int>>&
        observed_gpu_headroom,
    const comet::InstanceSpec& worker,
    const std::optional<std::string>& requested_node_name,
    const std::optional<std::string>& requested_gpu_device) const {
  std::optional<AutoPlacementDecision> best;
  const std::string selection_policy = EffectiveWorkerSelectionPolicy(desired_state);

  for (std::size_t node_index = 0; node_index < desired_state.nodes.size(); ++node_index) {
    const auto& node = desired_state.nodes[node_index];
    if (!NodeAllowsInstanceRole(node.execution_mode, worker.role)) {
      continue;
    }
    if (requested_node_name.has_value() && node.name != *requested_node_name) {
      continue;
    }
    if (node.gpu_devices.empty()) {
      continue;
    }

    for (std::size_t gpu_index = 0; gpu_index < node.gpu_devices.size(); ++gpu_index) {
      const auto& gpu_device = node.gpu_devices[gpu_index];
      if (requested_gpu_device.has_value() && gpu_device != *requested_gpu_device) {
        continue;
      }
      if (HybridGpuAlreadyAssigned(desired_state, worker, node.name, gpu_device)) {
        continue;
      }

      const auto usage_it = placement_usage.find({node.name, gpu_device});
      const PlacementUsage usage =
          usage_it == placement_usage.end() ? PlacementUsage{} : usage_it->second;
      const double free_fraction = 1.0 - usage.allocated_fraction;
      if (free_fraction + 1e-9 < worker.gpu_fraction) {
        continue;
      }

      int observed_free_vram_mb = -1;
      int observed_utilization_pct = -1;
      const auto observed_it = observed_gpu_headroom.find({node.name, gpu_device});
      if (observed_it != observed_gpu_headroom.end()) {
        observed_free_vram_mb = observed_it->second.first;
        observed_utilization_pct = observed_it->second.second;
      }

      if (worker.memory_cap_mb.has_value()) {
        const auto memory_it = node.gpu_memory_mb.find(gpu_device);
        const int capacity_free_mb =
            memory_it == node.gpu_memory_mb.end()
                ? std::numeric_limits<int>::max()
                : std::max(0, memory_it->second - usage.allocated_memory_mb);
        if (*worker.memory_cap_mb > capacity_free_mb) {
          continue;
        }
        if (observed_free_vram_mb >= 0 && *worker.memory_cap_mb > observed_free_vram_mb) {
          continue;
        }
      }

      AutoPlacementDecision candidate;
      candidate.node_name = node.name;
      candidate.gpu_device = gpu_device;
      candidate.idle_target = usage.allocated_fraction <= 1e-9;
      candidate.upgrade_to_exclusive =
          candidate.idle_target &&
          (worker.share_mode != comet::GpuShareMode::Exclusive ||
           worker.gpu_fraction < 1.0 - 1e-9);
      candidate.allocated_fraction = usage.allocated_fraction;
      candidate.allocated_memory_mb = usage.allocated_memory_mb;
      candidate.observed_free_vram_mb = observed_free_vram_mb;
      candidate.observed_utilization_pct = observed_utilization_pct;
      candidate.node_order = static_cast<int>(node_index);
      candidate.gpu_order = static_cast<int>(gpu_index);
      candidate.score = ScoreAutoPlacementCandidate(
          node,
          gpu_device,
          usage,
          desired_state.inference,
          observed_free_vram_mb,
          observed_utilization_pct,
          requested_node_name,
          requested_gpu_device);

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

  return best;
}

void DesiredStatePolicyService::ResolveDesiredStateDynamicPlacements(
    comet::ControllerStore& store,
    comet::DesiredState* desired_state) const {
  if (desired_state == nullptr) {
    return;
  }

  ApplyObservedHostGpuInventory(store, desired_state);

  std::map<std::pair<std::string, std::string>, std::pair<int, int>> observed_gpu_headroom;
  for (const auto& observation : store.LoadHostObservations()) {
    if (const auto telemetry = runtime_support_.ParseGpuTelemetry(observation);
        telemetry.has_value()) {
      for (const auto& device : telemetry->devices) {
        if (device.gpu_device.empty()) {
          continue;
        }
        observed_gpu_headroom[{observation.node_name, device.gpu_device}] = {
            device.free_vram_mb,
            device.gpu_utilization_pct,
        };
      }
    }
  }

  std::map<std::pair<std::string, std::string>, PlacementUsage> placement_usage;
  for (const auto& instance : desired_state->instances) {
    if (instance.role == comet::InstanceRole::Worker) {
      ReservePlacement(&placement_usage, instance);
    }
  }

  for (auto& instance : desired_state->instances) {
    if (instance.role != comet::InstanceRole::Worker) {
      continue;
    }

    const bool has_gpu_device =
        instance.gpu_device.has_value() && !instance.gpu_device->empty();
    if (instance.placement_mode == comet::PlacementMode::Manual && !has_gpu_device) {
      throw std::runtime_error(
          "worker '" + instance.name +
          "' uses placement_mode=manual but does not specify gpu_device");
    }
    if (has_gpu_device) {
      continue;
    }

    const std::optional<std::string> requested_node_name =
        instance.node_name.empty() ? std::nullopt
                                   : std::optional<std::string>(instance.node_name);
    const auto placement = SelectAutoPlacement(
        *desired_state,
        placement_usage,
        observed_gpu_headroom,
        instance,
        requested_node_name,
        std::nullopt);
    if (!placement.has_value()) {
      if (requested_node_name.has_value()) {
        throw std::runtime_error(
            "worker '" + instance.name +
            "' could not be assigned to any GPU on node '" +
            *requested_node_name +
            "'; wait for fresh host GPU telemetry or free capacity");
      }
      throw std::runtime_error(
          "worker '" + instance.name +
          "' could not be assigned to any GPU; wait for fresh host GPU telemetry or free capacity");
    }

    instance.node_name = placement->node_name;
    instance.gpu_device = placement->gpu_device;
    if (placement->upgrade_to_exclusive) {
      instance.share_mode = comet::GpuShareMode::Exclusive;
      instance.gpu_fraction = 1.0;
    }
    instance.environment["COMET_NODE_NAME"] = instance.node_name;
    instance.environment["COMET_GPU_DEVICE"] = *instance.gpu_device;
    instance.labels["comet.node"] = instance.node_name;
    instance.labels["comet.placement"] = "auto";
    instance.labels["comet.placement.mode"] = comet::ToString(instance.placement_mode);
    instance.labels["comet.placement.action"] =
        placement->upgrade_to_exclusive ? "upgrade-to-exclusive" : "auto-assign";
    instance.labels["comet.placement.score"] = std::to_string(placement->score);
    if (!requested_node_name.has_value()) {
      instance.labels.erase("comet.requested.node");
    } else {
      instance.labels["comet.requested.node"] = *requested_node_name;
    }
    instance.labels.erase("comet.requested.gpu");

    ReservePlacement(&placement_usage, instance);
  }

  RefreshDerivedWorkerMetadata(desired_state);
}

}  // namespace comet::controller
