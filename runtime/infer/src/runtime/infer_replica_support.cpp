#include "runtime/infer_replica_support.h"

#include <algorithm>
#include <map>
#include <set>
#include <string>
#include <utility>

#include "runtime/infer_control_support.h"

namespace comet::infer::replica_support {

namespace {

using control_support::LoadActiveModel;
using control_support::LoadWorkerGroupStatus;
using nlohmann::json;

struct ReplicaAccumulator {
  int replica_index = 0;
  int expected_size = 0;
  int observed_members = 0;
  int ready_members = 0;
  bool leader_ready = false;
  bool model_match = true;
  std::string leader_base_url;
};

std::string ConfiguredReplicaKey(
    const json& member,
    int workers_per_replica,
    int fallback_position,
    bool hybrid_mode) {
  if (hybrid_mode) {
    const int start_rank = member.value(
        "data_parallel_start_rank",
        member.value(
            "data_parallel_rank",
            member.value("replica_index", fallback_position / std::max(1, workers_per_replica))));
    const std::string node_name = member.value("node_name", std::string{});
    if (!node_name.empty()) {
      return "hybrid-node-" + node_name + "-start-" + std::to_string(std::max(0, start_rank));
    }
    return "hybrid-start-" + std::to_string(std::max(0, start_rank));
  }
  const std::string configured = member.value("replica_group_id", std::string{});
  if (!configured.empty()) {
    return configured;
  }
  const int replica_index = member.value(
      "data_parallel_rank",
      member.value("replica_index", fallback_position / std::max(1, workers_per_replica)));
  return "replica-" + std::to_string(std::max(0, replica_index));
}

std::string ObservedReplicaKey(const json& member, int workers_per_replica, bool hybrid_mode) {
  return ConfiguredReplicaKey(
      member,
      workers_per_replica,
      member.value("rank", 0),
      hybrid_mode);
}

void AddUniqueReason(std::vector<std::string>* reasons, const std::string& reason) {
  if (reasons == nullptr || reason.empty()) {
    return;
  }
  if (std::find(reasons->begin(), reasons->end(), reason) == reasons->end()) {
    reasons->push_back(reason);
  }
}

}  // namespace

ReplicaTopology InspectReplicaTopology(const RuntimeConfig& config) {
  ReplicaTopology topology;
  topology.data_parallel_mode = config.data_parallel_mode.empty() ? "off" : config.data_parallel_mode;
  topology.data_parallel_lb_mode =
      config.data_parallel_lb_mode.empty() ? "external" : config.data_parallel_lb_mode;
  const bool llama_rpc_runtime =
      config.runtime_engine == "llama.cpp" && config.distributed_backend == "llama_rpc";
  const bool hybrid_mode =
      topology.data_parallel_mode == "vllm_native" && topology.data_parallel_lb_mode == "hybrid";

  const json configured_members = config.worker_group.value("members", json::array());
  const json observed_worker_group = LoadWorkerGroupStatus(config);
  const json active_model = LoadActiveModel(config);
  const std::string active_model_id = active_model.value("model_id", std::string{});
  topology.workers_per_replica =
      std::max(1, config.worker_group.value("expected_workers", 0));

  if (!config.replica_upstreams.empty()) {
    topology.replica_groups_expected = static_cast<int>(config.replica_upstreams.size());
    topology.replica_groups_ready = topology.replica_groups_expected;
    topology.api_endpoints_expected = topology.replica_groups_expected;
    topology.api_endpoints_ready = topology.replica_groups_ready;
    topology.ready_replica_base_urls = config.replica_upstreams;
    topology.data_parallel_size = topology.replica_groups_expected;
    topology.data_parallel_size_local_max = topology.replica_groups_expected > 0 ? 1 : 0;
    return topology;
  }

  if (llama_rpc_runtime) {
    int expected_workers = 0;
    for (const auto& member : configured_members) {
      if (!member.is_object() || !member.value("enabled", true)) {
        continue;
      }
      ++expected_workers;
    }
    for (const auto& member : observed_worker_group.value("members", json::array())) {
      if (!member.is_object()) {
        continue;
      }
      if (member.value("ready", false)) {
        ++topology.ready_worker_members;
      }
    }
    topology.replica_groups_expected = expected_workers > 0 ? 1 : 0;
    topology.replica_groups_ready =
        expected_workers > 0 && topology.ready_worker_members >= expected_workers ? 1 : 0;
    topology.replica_groups_degraded =
        topology.replica_groups_expected > topology.replica_groups_ready ? 1 : 0;
    topology.api_endpoints_expected = 0;
    topology.api_endpoints_ready = 0;
    topology.data_parallel_size = 0;
    topology.data_parallel_size_local_max = 0;
    if (topology.replica_groups_degraded > 0) {
      AddUniqueReason(&topology.degraded_reasons, "rpc_worker_group_partial");
      if (topology.ready_worker_members == 0) {
        AddUniqueReason(&topology.degraded_reasons, "no_ready_rpc_workers");
      }
    }
    return topology;
  }

  std::map<std::string, ReplicaAccumulator> expected_groups;
  int configured_position = 0;
  std::set<std::string> configured_api_endpoint_keys;
  for (const auto& member : configured_members) {
    if (!member.is_object() || !member.value("enabled", true)) {
      continue;
    }
    const std::string key =
        ConfiguredReplicaKey(
            member, topology.workers_per_replica, configured_position++, hybrid_mode);
    auto& group = expected_groups[key];
    group.replica_index = hybrid_mode
                              ? member.value(
                                    "data_parallel_start_rank",
                                    member.value(
                                        "data_parallel_rank",
                                        member.value("replica_index", group.replica_index)))
                              : member.value(
                                    "data_parallel_rank",
                                    member.value("replica_index", group.replica_index));
    group.expected_size = std::max(
        group.expected_size,
        hybrid_mode ? 1
                    : member.value("replica_size", topology.workers_per_replica));
    topology.data_parallel_size = std::max(
        topology.data_parallel_size,
        member.value("data_parallel_size", group.replica_index + 1));
    topology.data_parallel_size_local_max = std::max(
        topology.data_parallel_size_local_max,
        member.value("data_parallel_size_local", 1));
    const bool api_endpoint = topology.data_parallel_mode == "vllm_native"
                                  ? member.value(
                                        "data_parallel_api_endpoint",
                                        member.value("replica_leader", member.value("leader", false)))
                                  : member.value(
                                        "replica_leader", member.value("leader", false));
    if (api_endpoint) {
      configured_api_endpoint_keys.insert(key);
    }
  }

  std::map<std::string, ReplicaAccumulator> observed_groups;
  for (const auto& member : observed_worker_group.value("members", json::array())) {
    if (!member.is_object()) {
      continue;
    }
    const std::string key =
        ObservedReplicaKey(member, topology.workers_per_replica, hybrid_mode);
    auto& group = observed_groups[key];
    group.replica_index = hybrid_mode
                              ? member.value(
                                    "data_parallel_start_rank",
                                    member.value(
                                        "data_parallel_rank",
                                        member.value("replica_index", group.replica_index)))
                              : member.value(
                                    "data_parallel_rank",
                                    member.value("replica_index", group.replica_index));
    group.expected_size = std::max(
        group.expected_size,
        hybrid_mode ? 1
                    : member.value("replica_size", topology.workers_per_replica));
    topology.data_parallel_size = std::max(
        topology.data_parallel_size,
        member.value("data_parallel_size", group.replica_index + 1));
    topology.data_parallel_size_local_max = std::max(
        topology.data_parallel_size_local_max,
        member.value("data_parallel_size_local", 1));
    ++group.observed_members;

    const bool ready = member.value("ready", false);
    if (ready) {
      ++group.ready_members;
      ++topology.ready_worker_members;
    }

    if (!active_model_id.empty()) {
      const std::string member_model_id = member.value("active_model_id", std::string{});
      if (!member_model_id.empty() && member_model_id != active_model_id) {
        group.model_match = false;
      }
    }

    const bool api_endpoint = config.data_parallel_mode == "vllm_native"
                                  ? member.value(
                                        "data_parallel_api_endpoint",
                                        member.value("replica_leader", member.value("leader", false)))
                                  : member.value(
                                        "replica_leader", member.value("leader", false));
    if (api_endpoint) {
      group.leader_ready = ready;
      if (ready) {
        ++topology.api_endpoints_ready;
        group.leader_base_url = member.value("base_url", std::string{});
      }
    }
  }

  if (expected_groups.empty()) {
    expected_groups = observed_groups;
  }

  topology.replica_groups_expected = static_cast<int>(expected_groups.size());
  topology.api_endpoints_expected = static_cast<int>(configured_api_endpoint_keys.size());
  if (topology.data_parallel_size <= 0) {
    topology.data_parallel_size = topology.replica_groups_expected;
  }
  if (topology.data_parallel_size_local_max <= 0) {
    topology.data_parallel_size_local_max = topology.data_parallel_size > 0 ? 1 : 0;
  }
  std::vector<std::pair<std::string, ReplicaAccumulator>> ordered_groups(
      expected_groups.begin(),
      expected_groups.end());
  std::sort(
      ordered_groups.begin(),
      ordered_groups.end(),
      [](const auto& lhs, const auto& rhs) {
        if (lhs.second.replica_index != rhs.second.replica_index) {
          return lhs.second.replica_index < rhs.second.replica_index;
        }
        return lhs.first < rhs.first;
      });

  for (const auto& [key, expected_group] : ordered_groups) {
    const auto observed_it = observed_groups.find(key);
    if (observed_it == observed_groups.end()) {
      ++topology.replica_groups_degraded;
      AddUniqueReason(&topology.degraded_reasons, "replica_group_missing");
      continue;
    }

    const ReplicaAccumulator& observed = observed_it->second;
    const int expected_size = std::max(1, expected_group.expected_size);
    const bool replica_ready = observed.leader_ready &&
                               !observed.leader_base_url.empty() &&
                               observed.model_match &&
                               observed.ready_members >= expected_size;
    if (replica_ready) {
      ++topology.replica_groups_ready;
      topology.ready_replica_base_urls.push_back(observed.leader_base_url);
      continue;
    }

    ++topology.replica_groups_degraded;
    AddUniqueReason(
        &topology.degraded_reasons,
        observed.observed_members > 0 ? "replica_group_partial" : "replica_group_missing");
  }

  if (topology.replica_groups_expected > 0 && topology.replica_groups_ready == 0) {
    AddUniqueReason(&topology.degraded_reasons, "no_ready_replicas");
  }

  return topology;
}

}  // namespace comet::infer::replica_support
