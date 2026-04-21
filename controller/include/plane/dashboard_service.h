#pragma once

#include <cstdint>
#include <functional>
#include <map>
#include <optional>
#include <set>
#include <string>
#include <vector>

#include <nlohmann/json.hpp>

#include "scheduler/scheduler_view_service.h"

#include "naim/state/models.h"
#include "naim/runtime/runtime_status.h"
#include "naim/planning/scheduling_policy.h"
#include "naim/state/sqlite_store.h"

namespace naim::controller {

class StateAggregateLoader;

struct StateAggregateViewData {
  std::string db_path;
  int stale_after_seconds = 0;
  std::vector<naim::PlaneRecord> planes;
  std::vector<naim::DesiredState> desired_states;
  std::optional<naim::DesiredState> desired_state;
  std::optional<int> desired_generation;
  std::vector<naim::DiskRuntimeState> disk_runtime_states;
  naim::SchedulingPolicyReport scheduling_report;
  std::vector<naim::HostObservation> observations;
  std::vector<naim::HostAssignment> assignments;
  std::vector<naim::NodeAvailabilityOverride> availability_overrides;
  SchedulerRuntimeView scheduler_runtime;
  std::vector<RolloutLifecycleEntry> rollout_lifecycle;
  std::vector<RebalancePlanEntry> rebalance_entries;
  RebalanceControllerGateSummary controller_gate_summary;
  RebalanceIterationBudgetSummary iteration_budget_summary;
  RebalancePolicySummary rebalance_policy_summary;
  RebalanceLoopStatusSummary loop_status;
};

class DashboardService {
 public:
  struct AlertSummary {
    int critical = 0;
    int warning = 0;
    int booting = 0;
    nlohmann::json items = nlohmann::json::array();

    void Push(
        const std::string& severity,
        const std::string& kind,
        const std::string& title,
        const std::string& detail,
        const std::optional<std::string>& node_name = std::nullopt,
        const std::optional<std::string>& worker_name = std::nullopt,
        const std::optional<int>& assignment_id = std::nullopt,
        const std::optional<int>& event_id = std::nullopt);

    nlohmann::json ToJson() const;
  };

  struct RuntimeFallback {
    bool available = false;
    bool launch_ready = false;
    std::string runtime_phase;
  };

  struct NodesPayload {
    nlohmann::json items = nlohmann::json::array();
    int observed_nodes = 0;
    int ready_nodes = 0;
    int not_ready_nodes = 0;
    int degraded_gpu_nodes = 0;
    std::optional<std::uint64_t> kv_cache_bytes;
    bool turboquant_enabled = false;
    std::string active_cache_type_k;
    std::string active_cache_type_v;
  };

  struct Deps {
    const StateAggregateLoader* state_aggregate_loader = nullptr;
    std::function<nlohmann::json(const naim::EventRecord&)> build_event_payload;
    std::function<std::map<std::string, naim::NodeAvailabilityOverride>(
        const std::vector<naim::NodeAvailabilityOverride>&)>
        build_availability_override_map;
    std::function<naim::NodeAvailability(
        const std::map<std::string, naim::NodeAvailabilityOverride>&,
        const std::string&)>
        resolve_node_availability;
    std::function<std::optional<long long>(const std::string&)> heartbeat_age_seconds;
    std::function<std::string(const std::optional<long long>&, int)> health_from_age;
    std::function<std::optional<naim::RuntimeStatus>(const naim::HostObservation&)>
        parse_runtime_status;
    std::function<std::optional<naim::GpuTelemetrySnapshot>(const naim::HostObservation&)>
        parse_gpu_telemetry;
  };

  explicit DashboardService(Deps deps);

  nlohmann::json BuildPayload(
      const std::string& db_path,
      int stale_after_seconds,
      const std::optional<std::string>& plane_name) const;

  static RuntimeFallback DetermineRuntimeFallback(
      const naim::HostObservation& observation,
      const std::string& node_name,
      const std::optional<std::string>& plane_name,
      const std::optional<std::string>& plane_state,
      int desired_generation,
      int desired_instance_count,
      int desired_disk_count,
      const std::string& health);

  static nlohmann::json BuildBootstrapModelPayload(
      const std::optional<naim::BootstrapModelSpec>& bootstrap_model);

  static nlohmann::json BuildPlanePayload(
      const naim::DesiredState& desired_state,
      const std::optional<int>& desired_generation,
      const std::optional<naim::PlaneRecord>& plane_record,
      int effective_plane_applied_generation);

  static nlohmann::json BuildPlanesPayload(
      const std::vector<naim::PlaneRecord>& planes,
      const std::vector<naim::DesiredState>& desired_states,
      const std::string& selected_plane_name,
      int effective_plane_applied_generation);

  static nlohmann::json BuildAssignmentsPayload(
      const std::map<std::string, naim::HostAssignment>& latest_assignments_by_node);

  static nlohmann::json BuildRolloutPayload(
      const std::vector<naim::RolloutActionRecord>& rollout_actions,
      const std::string& loop_state,
      const std::string& loop_reason);

  static nlohmann::json BuildRuntimePayload(
      int observed_nodes,
      int ready_nodes,
      int not_ready_nodes,
      int degraded_gpu_nodes,
      const std::optional<std::uint64_t>& kv_cache_bytes,
      bool turboquant_enabled,
      const std::string& active_cache_type_k,
      const std::string& active_cache_type_v);

  static nlohmann::json BuildRecentEventsPayload(
      const std::vector<naim::EventRecord>& recent_events,
      const std::function<nlohmann::json(const naim::EventRecord&)>&
          build_event_payload);

  static std::optional<std::string> ResolveSelectedPlaneState(
      const std::vector<naim::PlaneRecord>& planes,
      const std::optional<std::string>& plane_name);

  static std::map<std::string, naim::NodeInventory> BuildDashboardNodes(
      const std::optional<std::string>& plane_name,
      const naim::DesiredState& desired_state,
      const std::vector<naim::DesiredState>& desired_states);

  static NodesPayload BuildNodesPayload(
      const std::map<std::string, naim::NodeInventory>& dashboard_nodes,
      const std::map<std::string, naim::HostObservation>& observation_by_node,
      const std::map<std::string, naim::NodeAvailabilityOverride>&
          availability_override_map,
      const naim::DesiredState& desired_state,
      const std::vector<naim::DesiredState>& desired_states,
      const std::optional<std::string>& plane_name,
      const std::optional<std::string>& selected_plane_state,
      int desired_generation,
      int stale_after_seconds,
      const std::function<std::optional<long long>(const std::string&)>&
          heartbeat_age_seconds,
      const std::function<std::string(
          const std::optional<long long>&,
          int)>& health_from_age,
      const std::function<naim::NodeAvailability(
          const std::map<std::string, naim::NodeAvailabilityOverride>&,
          const std::string&)>& resolve_node_availability,
      const std::function<std::optional<naim::RuntimeStatus>(
          const naim::HostObservation&)>& parse_runtime_status,
      const std::function<std::optional<naim::GpuTelemetrySnapshot>(
          const naim::HostObservation&)>& parse_gpu_telemetry);

  static void BuildAssignmentAlerts(
      AlertSummary* alerts,
      const std::map<std::string, naim::HostAssignment>&
          latest_assignments_by_node);

  static void BuildNodeAlerts(
      AlertSummary* alerts,
      const std::map<std::string, naim::NodeInventory>& dashboard_nodes,
      const std::map<std::string, naim::HostObservation>& observation_by_node,
      const std::map<std::string, naim::NodeAvailabilityOverride>&
          availability_override_map,
      const naim::DesiredState& desired_state,
      const std::optional<std::string>& plane_name,
      const std::optional<std::string>& selected_plane_state,
      int desired_generation,
      int stale_after_seconds,
      const std::function<std::optional<long long>(const std::string&)>&
          heartbeat_age_seconds,
      const std::function<std::string(
          const std::optional<long long>&,
          int)>& health_from_age,
      const std::function<naim::NodeAvailability(
          const std::map<std::string, naim::NodeAvailabilityOverride>&,
          const std::string&)>& resolve_node_availability,
      const std::function<std::optional<naim::RuntimeStatus>(
          const naim::HostObservation&)>& parse_runtime_status,
      const std::function<std::optional<naim::GpuTelemetrySnapshot>(
          const naim::HostObservation&)>& parse_gpu_telemetry);

  static void BuildRolloutAlerts(
      AlertSummary* alerts,
      const std::vector<naim::RolloutActionRecord>& rollout_actions);

  static void BuildRecentEventAlerts(
      AlertSummary* alerts,
      const std::vector<naim::EventRecord>& recent_events);

 private:
  Deps deps_;
};

}  // namespace naim::controller
