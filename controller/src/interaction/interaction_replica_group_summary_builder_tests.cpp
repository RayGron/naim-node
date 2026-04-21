#include <iostream>
#include <stdexcept>
#include <string>

#include "interaction/interaction_replica_group_summary_builder.h"
#include "naim/state/worker_group_topology.h"

namespace {

void Expect(bool condition, const std::string& message) {
  if (!condition) {
    throw std::runtime_error(message);
  }
}

naim::DesiredState BuildHybridDesiredState() {
  naim::DesiredState desired_state;
  desired_state.plane_name = "hybrid-plane";
  desired_state.inference.runtime_engine = "vllm";
  desired_state.inference.distributed_backend = "native";
  desired_state.inference.data_parallel_mode = "on";
  desired_state.inference.data_parallel_lb_mode = naim::kDataParallelLbModeHybrid;

  naim::WorkerGroupMemberSpec first;
  first.name = "worker-a";
  first.node_name = "node-a";
  first.data_parallel_start_rank = 0;
  first.data_parallel_size = 2;
  first.data_parallel_size_local = 1;
  first.data_parallel_api_endpoint = true;
  first.enabled = true;

  naim::WorkerGroupMemberSpec second = first;
  second.name = "worker-b";
  second.node_name = "node-b";
  second.data_parallel_start_rank = 1;

  naim::WorkerGroupMemberSpec third = first;
  third.name = "worker-c";
  third.node_name = "node-b";
  third.data_parallel_start_rank = 1;
  third.data_parallel_api_endpoint = false;

  desired_state.worker_group.members = {first, second, third};
  return desired_state;
}

void TestCountsExpectedHybridApiEndpoints() {
  const auto desired_state = BuildHybridDesiredState();
  const naim::controller::InteractionReplicaGroupSummaryBuilder builder;

  Expect(
      builder.CountExpectedHybridApiEndpoints(desired_state) == 2,
      "hybrid expected API endpoints should dedupe by node/start-rank groups");

  std::cout << "ok: hybrid-api-endpoint-count" << '\n';
}

void TestBuildsHybridReplicaSummary() {
  const auto desired_state = BuildHybridDesiredState();
  const naim::controller::InteractionReplicaGroupSummaryBuilder builder;

  const auto summary = builder.Build(
      desired_state,
      {
          naim::RuntimeProcessStatus{
              "worker-a", "worker", "node-a", "", "", "running", "", "", 11, 0, true},
          naim::RuntimeProcessStatus{
              "worker-b", "worker", "node-b", "", "", "running", "", "", 12, 0, true},
          naim::RuntimeProcessStatus{
              "worker-c", "worker", "node-b", "", "", "running", "", "", 13, 0, false},
      });

  Expect(summary.expected_worker_members == 3, "expected worker count mismatch");
  Expect(summary.ready_worker_members == 2, "ready worker count mismatch");
  Expect(summary.expected_api_endpoints == 2, "expected API endpoint count mismatch");
  Expect(summary.ready_api_endpoints == 2, "ready API endpoint count mismatch");
  Expect(summary.expected_replica_groups == 2, "expected replica group count mismatch");
  Expect(summary.ready_replica_groups == 2, "ready replica groups mismatch");
  Expect(summary.degraded_replica_groups == 0, "degraded replica groups mismatch");
  Expect(summary.data_parallel_size == 2, "data parallel size mismatch");
  Expect(summary.data_parallel_size_local_max == 1, "local max mismatch");

  std::cout << "ok: hybrid-replica-summary" << '\n';
}

}  // namespace

int main() {
  try {
    TestCountsExpectedHybridApiEndpoints();
    TestBuildsHybridReplicaSummary();
    return 0;
  } catch (const std::exception& error) {
    std::cerr << "interaction_replica_group_summary_builder_tests failed: "
              << error.what() << '\n';
    return 1;
  }
}
