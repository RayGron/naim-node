#pragma once

#include <map>
#include <iosfwd>
#include <optional>
#include <string>
#include <vector>

#include <nlohmann/json.hpp>

#include "naim/state/models.h"
#include "naim/state/sqlite_store.h"

enum class SchedulerRolloutPhase {
  Planned,
  EvictionEnqueued,
  EvictionApplied,
  RetryReady,
  RetryMaterialized,
  HostFailed,
  HostStale,
  RuntimeFailed,
  RolloutApplied,
};

struct RolloutLifecycleEntry {
  std::string worker_name;
  int desired_generation = 0;
  SchedulerRolloutPhase phase = SchedulerRolloutPhase::Planned;
  std::optional<int> action_id;
  std::string target_node_name;
  std::string target_gpu_device;
  std::vector<std::string> victim_worker_names;
  std::string detail;
};

struct RebalancePlanEntry {
  std::string worker_name;
  naim::PlacementMode placement_mode = naim::PlacementMode::Manual;
  std::string current_node_name;
  std::string current_gpu_device;
  std::string target_node_name;
  std::string target_gpu_device;
  std::string rebalance_class;
  std::string decision;
  std::string state;
  std::string action;
  int score = 0;
  bool preemption_required = false;
  std::vector<std::string> victim_worker_names;
  std::string gate_reason;
};

struct RebalancePolicySummary {
  int actionable_count = 0;
  int safe_direct_count = 0;
  int rollout_class_count = 0;
  int gated_count = 0;
  int blocked_active_rollout_count = 0;
  int assignment_busy_count = 0;
  int observation_gated_count = 0;
  int stable_hold_count = 0;
  int below_threshold_count = 0;
  int propose_count = 0;
  int hold_count = 0;
  int defer_count = 0;
  int no_candidate_count = 0;
  std::vector<std::string> actionable_workers;
  std::vector<std::string> safe_direct_workers;
  std::vector<std::string> rollout_class_workers;
  std::vector<std::string> gated_workers;
  std::vector<std::string> blocked_active_rollout_workers;
  std::vector<std::string> assignment_busy_workers;
  std::vector<std::string> observation_gated_workers;
  std::vector<std::string> stable_hold_workers;
  std::vector<std::string> below_threshold_workers;
  std::vector<std::string> proposed_workers;
  std::vector<std::string> held_workers;
  std::vector<std::string> deferred_workers;
  std::vector<std::string> no_candidate_workers;
};

struct RebalanceControllerGateSummary {
  bool cluster_ready = true;
  int active_rollout_count = 0;
  int blocking_assignment_count = 0;
  int unconverged_node_count = 0;
  std::vector<std::string> active_rollout_workers;
  std::vector<std::string> blocking_assignment_nodes;
  std::vector<std::string> unconverged_nodes;
};

struct RebalanceIterationBudgetSummary {
  int current_iteration = 0;
  int max_iterations = 0;
  bool exhausted = false;
};

struct RebalanceLoopStatusSummary {
  std::string state;
  std::string reason;
};

struct SchedulerRuntimeView {
  std::optional<naim::SchedulerPlaneRuntime> plane_runtime;
  std::map<std::string, naim::SchedulerWorkerRuntime> worker_runtime_by_name;
  std::map<std::string, naim::SchedulerNodeRuntime> node_runtime_by_name;
};

struct RolloutActionsViewData {
  std::string db_path;
  std::optional<std::string> plane_name;
  std::optional<std::string> node_name;
  std::optional<naim::DesiredState> desired_state;
  std::optional<int> desired_generation;
  std::vector<naim::RolloutActionRecord> actions;
  std::optional<SchedulerRuntimeView> scheduler_runtime;
  std::vector<RolloutLifecycleEntry> lifecycle;
  std::size_t gated_worker_count = 0;
  std::size_t gated_node_count = 0;
};

struct RebalancePlanViewData {
  std::string db_path;
  std::optional<std::string> plane_name;
  std::optional<std::string> node_name;
  int stale_after_seconds = 0;
  std::optional<naim::DesiredState> desired_state;
  int desired_generation = 0;
  std::vector<RebalancePlanEntry> rebalance_entries;
  RebalanceControllerGateSummary controller_gate_summary;
  RebalanceIterationBudgetSummary iteration_budget_summary;
  RebalancePolicySummary policy_summary;
  RebalanceLoopStatusSummary loop_status;
  SchedulerRuntimeView scheduler_runtime;
};

std::string ToString(SchedulerRolloutPhase phase);

class SchedulerViewService {
 public:
  nlohmann::json BuildRolloutActionsPayload(
      const RolloutActionsViewData& view) const;

  nlohmann::json BuildRebalancePlanPayload(
      const RebalancePlanViewData& view) const;

  RebalancePolicySummary BuildRebalancePolicySummary(
      const std::vector<RebalancePlanEntry>& entries) const;

  RebalanceIterationBudgetSummary BuildRebalanceIterationBudgetSummary(
      int current_iteration,
      int max_iterations) const;

  RebalanceLoopStatusSummary BuildRebalanceLoopStatusSummary(
      const RebalanceControllerGateSummary& controller_gate_summary,
      const RebalanceIterationBudgetSummary& iteration_budget_summary,
      const RebalancePolicySummary& policy_summary) const;

  void PrintRolloutLifecycleEntries(
      std::ostream& out,
      const std::vector<RolloutLifecycleEntry>& entries) const;

  void PrintRebalancePlanEntries(
      std::ostream& out,
      const std::vector<RebalancePlanEntry>& entries) const;

  void PrintRebalancePolicySummary(
      std::ostream& out,
      const RebalancePolicySummary& summary) const;

  void PrintRebalanceControllerGateSummary(
      std::ostream& out,
      const RebalanceControllerGateSummary& summary) const;

  void PrintRebalanceIterationBudgetSummary(
      std::ostream& out,
      const RebalanceIterationBudgetSummary& summary) const;

  void PrintRebalanceLoopStatusSummary(
      std::ostream& out,
      const RebalanceLoopStatusSummary& summary) const;

  void PrintSchedulerRuntimeView(
      std::ostream& out,
      const SchedulerRuntimeView& view,
      int verification_stable_samples_required) const;
};
