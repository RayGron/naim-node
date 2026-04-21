#include <iostream>
#include <stdexcept>

#include "naim/state/worker_group_topology.h"

namespace {

void ExpectThrowsDuplicateHybridGpu() {
  naim::InferenceRuntimeSettings inference;
  inference.runtime_engine = "llama.cpp";
  inference.data_parallel_mode = "replicas";
  inference.data_parallel_lb_mode = naim::kDataParallelLbModeHybrid;

  naim::WorkerGroupSpec worker_group;
  worker_group.group_id = "hybrid-workers";
  worker_group.expected_workers = 1;

  for (int rank = 0; rank < 4; ++rank) {
    naim::WorkerGroupMemberSpec member;
    member.enabled = true;
    member.name = "worker-" + std::to_string(rank);
    member.node_name = "local-hostd";
    member.gpu_device = rank == 3 ? "0" : std::to_string(rank == 0 ? 0 : rank + 1);
    worker_group.members.push_back(member);
  }

  try {
    naim::ValidateReplicaPacking(inference, worker_group);
  } catch (const std::exception&) {
    std::cout << "ok: hybrid-duplicate-gpu-rejected\n";
    return;
  }
  throw std::runtime_error("expected hybrid duplicate gpu validation failure");
}

}  // namespace

int main() {
  try {
    ExpectThrowsDuplicateHybridGpu();
    return 0;
  } catch (const std::exception& ex) {
    std::cerr << "worker_group_topology_tests failed: " << ex.what() << '\n';
    return 1;
  }
}
