#pragma once

#include <string>
#include <vector>

#include "runtime/infer_runtime_types.h"

namespace naim::infer::replica_support {

struct ReplicaTopology {
  std::string data_parallel_mode = "off";
  std::string data_parallel_lb_mode = "external";
  int workers_per_replica = 0;
  int data_parallel_size = 0;
  int data_parallel_size_local_max = 0;
  int replica_groups_expected = 0;
  int replica_groups_ready = 0;
  int replica_groups_degraded = 0;
  int api_endpoints_expected = 0;
  int api_endpoints_ready = 0;
  int ready_worker_members = 0;
  std::vector<std::string> degraded_reasons;
  std::vector<std::string> ready_replica_base_urls;
};

ReplicaTopology InspectReplicaTopology(const RuntimeConfig& config);

}  // namespace naim::infer::replica_support
