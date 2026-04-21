#include "interaction/interaction_replica_group_summary_builder.h"

#include <algorithm>
#include <map>
#include <set>

#include "naim/state/worker_group_topology.h"

namespace naim::controller {

std::string InteractionReplicaGroupSummaryBuilder::BuildHybridReplicaGroupKey(
    const naim::WorkerGroupMemberSpec& member) const {
  const std::string node_name = member.node_name.empty() ? std::string("unknown-node")
                                                         : member.node_name;
  return "hybrid-node-" + node_name + "-start-" +
         std::to_string(std::max(0, member.data_parallel_start_rank));
}

int InteractionReplicaGroupSummaryBuilder::CountExpectedHybridApiEndpoints(
    const naim::DesiredState& desired_state) const {
  std::set<std::string> keys;
  for (const auto& member : desired_state.worker_group.members) {
    if (!member.enabled || !member.data_parallel_api_endpoint) {
      continue;
    }
    keys.insert(BuildHybridReplicaGroupKey(member));
  }
  return static_cast<int>(keys.size());
}

InteractionReplicaGroupSummary InteractionReplicaGroupSummaryBuilder::Build(
    const naim::DesiredState& desired_state,
    const std::vector<naim::RuntimeProcessStatus>& instance_statuses) const {
  const bool llama_rpc_runtime =
      desired_state.inference.runtime_engine == "llama.cpp" &&
      desired_state.inference.distributed_backend == "llama_rpc";
  const bool hybrid_data_parallel =
      naim::DataParallelEnabled(desired_state.inference) &&
      desired_state.inference.data_parallel_lb_mode == naim::kDataParallelLbModeHybrid;
  InteractionReplicaGroupSummary summary;
  if (llama_rpc_runtime) {
    std::map<std::string, std::pair<int, int>> groups;
    for (const auto& member : desired_state.worker_group.members) {
      if (!member.enabled) {
        continue;
      }
      ++summary.expected_worker_members;
      const std::string group_key =
          member.infer_instance_name.empty() ? desired_state.worker_group.infer_instance_name
                                             : member.infer_instance_name;
      auto& group = groups[group_key];
      ++group.first;
      const auto status_it = std::find_if(
          instance_statuses.begin(),
          instance_statuses.end(),
          [&](const naim::RuntimeProcessStatus& status) {
            return status.instance_name == member.name;
          });
      if (status_it != instance_statuses.end() && status_it->ready) {
        ++summary.ready_worker_members;
        ++group.second;
      }
    }
    summary.expected_replica_groups = static_cast<int>(groups.size());
    for (const auto& [_, group] : groups) {
      if (group.second >= group.first && group.first > 0) {
        ++summary.ready_replica_groups;
      } else {
        ++summary.degraded_replica_groups;
      }
    }
    return summary;
  }

  std::map<std::string, std::pair<int, int>> groups;
  for (const auto& member : desired_state.worker_group.members) {
    if (!member.enabled) {
      continue;
    }
    ++summary.expected_worker_members;
    const std::string key = hybrid_data_parallel
                                ? BuildHybridReplicaGroupKey(member)
                                : (member.replica_group_id.empty() ? std::string("replica-0")
                                                                   : member.replica_group_id);
    auto& group = groups[key];
    group.first = std::max(
        group.first,
        hybrid_data_parallel ? 1 : std::max(1, member.replica_size));

    const auto status_it = std::find_if(
        instance_statuses.begin(),
        instance_statuses.end(),
        [&](const naim::RuntimeProcessStatus& status) {
          return status.instance_name == member.name;
        });
    if (member.data_parallel_api_endpoint) {
      ++summary.expected_api_endpoints;
    }
    if (status_it != instance_statuses.end() && status_it->ready) {
      ++summary.ready_worker_members;
      if (member.data_parallel_api_endpoint) {
        ++summary.ready_api_endpoints;
      }
      if (!hybrid_data_parallel || member.data_parallel_api_endpoint) {
        ++group.second;
      }
    }
    summary.data_parallel_size =
        std::max(summary.data_parallel_size, member.data_parallel_size);
    summary.data_parallel_size_local_max =
        std::max(summary.data_parallel_size_local_max, member.data_parallel_size_local);
  }

  summary.expected_replica_groups = static_cast<int>(groups.size());
  for (const auto& [_, group] : groups) {
    if (group.second >= std::max(1, group.first)) {
      ++summary.ready_replica_groups;
    } else {
      ++summary.degraded_replica_groups;
    }
  }
  return summary;
}

}  // namespace naim::controller
