#pragma once

#include <algorithm>
#include <cstdint>
#include <map>
#include <stdexcept>
#include <string>

#include "naim/state/models.h"

namespace naim {

inline constexpr const char* kDataParallelModeOff = "off";
inline constexpr const char* kDataParallelLbModeExternal = "external";
inline constexpr const char* kDataParallelLbModeHybrid = "hybrid";
inline constexpr int kLlamaRpcWorkerPublishedPortBase = 40000;
inline constexpr int kLlamaRpcWorkerPublishedPortSpan = 20000;

inline uint32_t StablePortHash(const std::string& value) {
  uint32_t hash = 2166136261u;
  for (unsigned char ch : value) {
    hash ^= static_cast<uint32_t>(ch);
    hash *= 16777619u;
  }
  return hash;
}

inline int StableLlamaRpcWorkerPort(
    const std::string& plane_name,
    const std::string& worker_name) {
  const uint32_t offset =
      StablePortHash(plane_name + ":" + worker_name + ":llama-rpc") %
      kLlamaRpcWorkerPublishedPortSpan;
  return kLlamaRpcWorkerPublishedPortBase + static_cast<int>(offset);
}

inline std::string CanonicalDataParallelMode(const InferenceRuntimeSettings& inference) {
  return inference.data_parallel_mode.empty() ? std::string(kDataParallelModeOff)
                                              : inference.data_parallel_mode;
}

inline bool DataParallelEnabled(const InferenceRuntimeSettings& inference) {
  return CanonicalDataParallelMode(inference) != kDataParallelModeOff;
}

inline bool NativeDataParallelEnabled(const InferenceRuntimeSettings& inference) {
  return DataParallelEnabled(inference);
}

inline bool HybridDataParallelEnabled(const InferenceRuntimeSettings& inference) {
  return NativeDataParallelEnabled(inference) &&
         inference.data_parallel_lb_mode == kDataParallelLbModeHybrid;
}

inline int WorkersPerReplica(const WorkerGroupSpec& worker_group) {
  return std::max(1, worker_group.expected_workers);
}

inline int EligibleWorkerMemberCount(const WorkerGroupSpec& worker_group) {
  return static_cast<int>(std::count_if(
      worker_group.members.begin(),
      worker_group.members.end(),
      [](const WorkerGroupMemberSpec& member) { return member.enabled; }));
}

inline int DefaultWorkersPerReplica(
    const InferenceRuntimeSettings&,
    int eligible_worker_members) {
  return std::max(1, eligible_worker_members);
}

inline int ExpectedReplicaGroupCount(
    const InferenceRuntimeSettings& inference,
    const WorkerGroupSpec& worker_group) {
  const int eligible_members = EligibleWorkerMemberCount(worker_group);
  if (eligible_members <= 0) {
    return 0;
  }
  if (!DataParallelEnabled(inference)) {
    return 1;
  }
  return eligible_members / WorkersPerReplica(worker_group);
}

inline std::string ReplicaGroupIdFor(
    const WorkerGroupSpec& worker_group,
    int replica_index) {
  const std::string prefix =
      worker_group.group_id.empty() ? std::string("worker-group") : worker_group.group_id;
  return prefix + "-replica-" + std::to_string(std::max(0, replica_index));
}

inline void ValidateReplicaPacking(
    const InferenceRuntimeSettings& inference,
    const WorkerGroupSpec& worker_group) {
  if (!DataParallelEnabled(inference)) {
    return;
  }
  const int eligible_members = EligibleWorkerMemberCount(worker_group);
  const int workers_per_replica = WorkersPerReplica(worker_group);
  if (eligible_members <= 0 || workers_per_replica <= 0) {
    return;
  }
  if (eligible_members % workers_per_replica != 0) {
    throw std::runtime_error(
        "data_parallel_mode=" + inference.data_parallel_mode +
        " requires eligible worker count (" +
        std::to_string(eligible_members) +
        ") to be divisible by worker_group.expected_workers (" +
        std::to_string(workers_per_replica) + ")");
  }
  if (HybridDataParallelEnabled(inference)) {
    int eligible_index = 0;
    std::string replica_node_name;
    std::map<std::string, std::map<std::string, std::string>> node_gpu_workers;
    for (const auto& member : worker_group.members) {
      if (!member.enabled) {
        continue;
      }
      if (member.node_name.empty()) {
        throw std::runtime_error(
            "data_parallel_lb_mode=hybrid requires enabled worker members to have node_name");
      }
      const int local_rank = eligible_index % workers_per_replica;
      if (local_rank == 0) {
        replica_node_name = member.node_name;
      } else if (member.node_name != replica_node_name) {
        throw std::runtime_error(
            "data_parallel_lb_mode=hybrid requires node-local replica packing; "
            "worker '" +
            member.name + "' is assigned to node '" + member.node_name +
            "' but replica started on node '" + replica_node_name + "'");
      }
      if (!member.gpu_device.empty()) {
        auto& workers_by_gpu = node_gpu_workers[member.node_name];
        const auto existing_it = workers_by_gpu.find(member.gpu_device);
        if (existing_it != workers_by_gpu.end()) {
          throw std::runtime_error(
              "data_parallel_lb_mode=hybrid requires unique gpu_device per node-local "
              "data-parallel rank; worker '" +
              member.name + "' reuses gpu '" + member.gpu_device + "' on node '" +
              member.node_name + "' already assigned to worker '" + existing_it->second + "'");
        }
        workers_by_gpu.emplace(member.gpu_device, member.name);
      }
      ++eligible_index;
    }
  }
}

inline void AssignReplicaTopology(
    const InferenceRuntimeSettings& inference,
    WorkerGroupSpec* worker_group) {
  if (worker_group == nullptr) {
    return;
  }

  const bool data_parallel = DataParallelEnabled(inference);
  const int workers_per_replica = WorkersPerReplica(*worker_group);
  const int replica_count = ExpectedReplicaGroupCount(inference, *worker_group);
  const int data_parallel_rpc_port =
      worker_group->rendezvous_port > 0 ? worker_group->rendezvous_port + 100
                                        : 29600;
  std::string data_parallel_head_address;
  for (const auto& member : worker_group->members) {
    if (!member.enabled) {
      continue;
    }
    data_parallel_head_address = member.name;
    break;
  }
  if (data_parallel_head_address.empty()) {
    data_parallel_head_address =
        worker_group->rendezvous_host.empty() ? worker_group->infer_instance_name
                                              : worker_group->rendezvous_host;
  }
  std::map<std::string, int> node_replica_counts;
  std::map<std::string, int> node_replica_start_ranks;
  if (HybridDataParallelEnabled(inference)) {
    int replica_index = 0;
    for (const auto& member : worker_group->members) {
      if (!member.enabled) {
        continue;
      }
      const int current_replica_index =
          data_parallel ? replica_index / workers_per_replica : 0;
      const int local_rank =
          data_parallel ? replica_index % workers_per_replica : replica_index;
      if (local_rank == 0) {
        auto& count = node_replica_counts[member.node_name];
        auto start_it = node_replica_start_ranks.find(member.node_name);
        if (start_it == node_replica_start_ranks.end()) {
          node_replica_start_ranks.emplace(member.node_name, current_replica_index);
        } else {
          start_it->second = std::min(start_it->second, current_replica_index);
        }
        ++count;
      }
      ++replica_index;
    }
  }
  int eligible_index = 0;
  for (auto& member : worker_group->members) {
    if (!member.enabled) {
      member.rank = 0;
      member.replica_group_id.clear();
      member.replica_index = 0;
      member.replica_size = workers_per_replica;
      member.replica_leader = false;
      member.data_parallel_rank = 0;
      member.data_parallel_size = 1;
      member.data_parallel_size_local = 1;
      member.data_parallel_start_rank = 0;
      member.data_parallel_api_endpoint = false;
      member.data_parallel_head_address.clear();
      member.data_parallel_rpc_port = 0;
      member.leader = false;
      continue;
    }

    const int replica_index =
        data_parallel ? eligible_index / workers_per_replica : 0;
    const int local_rank =
        data_parallel ? eligible_index % workers_per_replica : eligible_index;
    member.rank = local_rank;
    member.replica_group_id = ReplicaGroupIdFor(*worker_group, replica_index);
    member.replica_index = replica_index;
    member.replica_size = workers_per_replica;
    member.replica_leader = local_rank == 0;
    member.data_parallel_rank = replica_index;
    member.data_parallel_size = std::max(1, replica_count);
    member.data_parallel_size_local = 1;
    member.data_parallel_start_rank = replica_index;
    member.data_parallel_api_endpoint = member.replica_leader;
    member.data_parallel_head_address = data_parallel_head_address;
    member.data_parallel_rpc_port = data_parallel_rpc_port;
    if (HybridDataParallelEnabled(inference) && !member.node_name.empty()) {
      const auto count_it = node_replica_counts.find(member.node_name);
      if (count_it != node_replica_counts.end()) {
        member.data_parallel_size_local = std::max(1, count_it->second);
      }
      const auto start_it = node_replica_start_ranks.find(member.node_name);
      if (start_it != node_replica_start_ranks.end()) {
        member.data_parallel_start_rank = std::max(0, start_it->second);
      }
      member.data_parallel_api_endpoint =
          member.replica_leader &&
          member.data_parallel_rank == member.data_parallel_start_rank;
    }
    member.leader = member.replica_leader;
    ++eligible_index;
  }
}

}  // namespace naim
