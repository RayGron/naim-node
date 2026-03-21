#pragma once

#include <string>
#include <vector>

#include "comet/models.h"

namespace comet {

struct GpuAllocationGroup {
  std::string node_name;
  std::string gpu_device;
  std::vector<std::string> worker_names;
  std::vector<std::string> exclusive_worker_names;
  std::vector<std::string> shared_worker_names;
  std::vector<std::string> best_effort_worker_names;
  std::vector<std::string> preemptible_worker_names;
  double total_fraction = 0.0;
  double remaining_fraction = 1.0;
  int highest_priority = 0;
  int lowest_priority = 0;
  int total_memory_cap_mb = 0;
  std::optional<int> memory_capacity_mb;
  std::optional<int> remaining_memory_mb;
};

struct GpuCapacitySummary {
  std::string node_name;
  std::string gpu_device;
  int worker_count = 0;
  int guaranteed_worker_count = 0;
  int best_effort_worker_count = 0;
  double allocated_fraction = 0.0;
  double free_fraction = 1.0;
  double releasable_best_effort_fraction = 0.0;
  std::optional<int> memory_capacity_mb;
  int allocated_memory_cap_mb = 0;
  int releasable_best_effort_memory_cap_mb = 0;
  std::optional<int> free_memory_mb;
  std::vector<std::string> preemptible_best_effort_workers;
  bool has_soft_share = false;
  bool is_idle = true;
};

struct PlacementCandidate {
  std::string node_name;
  std::string gpu_device;
  int score = 0;
  bool same_node = false;
  bool idle = false;
  bool fits_current_fraction = false;
  bool fits_current_with_preemption = false;
  bool fits_exclusive_fraction = false;
  bool fits_exclusive_with_preemption = false;
  bool fits_memory_cap = true;
  bool fits_memory_with_preemption = true;
  bool preemption_required = false;
  double free_fraction = 0.0;
  std::optional<int> free_memory_mb;
  std::vector<std::string> preemption_victims;
  std::string action;
};

struct WorkerPlacementRecommendation {
  std::string worker_name;
  std::string current_node_name;
  std::string current_gpu_device;
  GpuShareMode current_share_mode = GpuShareMode::Exclusive;
  double current_fraction = 0.0;
  std::optional<int> current_memory_cap_mb;
  std::vector<PlacementCandidate> candidates;
};

struct PreemptionHint {
  std::string node_name;
  std::string gpu_device;
  std::vector<std::string> victim_worker_names;
  std::string reason;
};

struct SchedulerRolloutAction {
  int step = 0;
  std::string worker_name;
  std::string action;
  std::string target_node_name;
  std::string target_gpu_device;
  std::vector<std::string> victim_worker_names;
  std::string reason;
};

struct SchedulingPolicyReport {
  std::vector<GpuAllocationGroup> allocations;
  std::vector<GpuCapacitySummary> capacities;
  std::vector<WorkerPlacementRecommendation> placement_recommendations;
  std::vector<PreemptionHint> preemption_hints;
  std::vector<SchedulerRolloutAction> rollout_actions;
  std::vector<std::string> rebalance_hints;
  std::vector<std::string> warnings;
  std::vector<std::string> errors;
};

SchedulingPolicyReport EvaluateSchedulingPolicy(const DesiredState& state);
std::string RenderSchedulingPolicyReport(const SchedulingPolicyReport& report);
void RequireSchedulingPolicy(const DesiredState& state);

}  // namespace comet
