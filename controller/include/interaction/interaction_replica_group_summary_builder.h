#pragma once

#include <string>
#include <vector>

#include "naim/runtime/runtime_status.h"
#include "naim/state/models.h"

namespace naim::controller {

struct InteractionReplicaGroupSummary {
  int expected_replica_groups = 0;
  int ready_replica_groups = 0;
  int degraded_replica_groups = 0;
  int ready_worker_members = 0;
  int expected_worker_members = 0;
  int expected_api_endpoints = 0;
  int ready_api_endpoints = 0;
  int data_parallel_size = 0;
  int data_parallel_size_local_max = 0;
};

class InteractionReplicaGroupSummaryBuilder {
 public:
  std::string BuildHybridReplicaGroupKey(
      const naim::WorkerGroupMemberSpec& member) const;

  int CountExpectedHybridApiEndpoints(
      const naim::DesiredState& desired_state) const;

  InteractionReplicaGroupSummary Build(
      const naim::DesiredState& desired_state,
      const std::vector<naim::RuntimeProcessStatus>& instance_statuses) const;
};

}  // namespace naim::controller
