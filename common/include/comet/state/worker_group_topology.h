#pragma once

#include <algorithm>
#include <stdexcept>
#include <string>

#include "comet/state/models.h"

namespace comet {

inline constexpr const char* kDataParallelModeOff = "off";
inline constexpr const char* kDataParallelModeAutoReplicas = "auto_replicas";

inline bool DataParallelEnabled(const InferenceRuntimeSettings& inference) {
  return inference.data_parallel_mode == kDataParallelModeAutoReplicas;
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
    const InferenceRuntimeSettings& inference,
    int eligible_worker_members) {
  if (DataParallelEnabled(inference) && inference.runtime_engine == "vllm") {
    return std::max(
        1,
        std::max(1, inference.tensor_parallel_size) *
            std::max(1, inference.pipeline_parallel_size));
  }
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
        "data_parallel_mode=auto_replicas requires eligible worker count (" +
        std::to_string(eligible_members) +
        ") to be divisible by worker_group.expected_workers (" +
        std::to_string(workers_per_replica) + ")");
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
  int eligible_index = 0;
  for (auto& member : worker_group->members) {
    if (!member.enabled) {
      member.rank = 0;
      member.replica_group_id.clear();
      member.replica_index = 0;
      member.replica_size = workers_per_replica;
      member.replica_leader = false;
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
    member.leader = member.replica_leader;
    ++eligible_index;
  }
}

}  // namespace comet
