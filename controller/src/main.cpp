#include <algorithm>
#include <ctime>
#include <cstddef>
#include <exception>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <map>
#include <optional>
#include <set>
#include <sstream>
#include <stdexcept>
#include <string>
#include <vector>

#include "comet/compose_renderer.h"
#include "comet/demo_state.h"
#include "comet/execution_plan.h"
#include "comet/import_bundle.h"
#include "comet/infer_runtime_config.h"
#include "comet/models.h"
#include "comet/planner.h"
#include "comet/reconcile.h"
#include "comet/runtime_status.h"
#include "comet/scheduling_policy.h"
#include "comet/sqlite_store.h"
#include "comet/state_json.h"

namespace {

std::string DefaultDbPath() {
  return (std::filesystem::path("var") / "controller.sqlite").string();
}

std::string DefaultArtifactsRoot() {
  return (std::filesystem::path("var") / "artifacts").string();
}

int DefaultStaleAfterSeconds() {
  return 300;
}

int MinimumSafeDirectRebalanceScore() {
  return 100;
}

int MaximumRebalanceIterationsPerGeneration() {
  return 1;
}

int WorkerMinimumResidencySeconds() {
  return 300;
}

int NodeCooldownAfterMoveSeconds() {
  return 60;
}

int VerificationStableSamplesRequired() {
  return 3;
}

int VerificationTimeoutSeconds() {
  return 45;
}

void PrintUsage() {
  std::cout
      << "Usage:\n"
      << "  comet-controller show-demo-plan\n"
      << "  comet-controller render-demo-compose [--node <node-name>]\n"
      << "  comet-controller init-db [--db <path>]\n"
      << "  comet-controller seed-demo [--db <path>]\n"
      << "  comet-controller validate-bundle --bundle <dir>\n"
      << "  comet-controller preview-bundle --bundle <dir> [--node <node-name>]\n"
      << "  comet-controller plan-bundle --bundle <dir> [--db <path>]\n"
      << "  comet-controller plan-host-ops --bundle <dir> [--db <path>] [--artifacts-root <path>] [--node <node-name>]\n"
      << "  comet-controller apply-bundle --bundle <dir> [--db <path>] [--artifacts-root <path>]\n"
      << "  comet-controller import-bundle --bundle <dir> [--db <path>]\n"
      << "  comet-controller show-host-assignments [--db <path>] [--node <node-name>]\n"
      << "  comet-controller show-host-observations [--db <path>] [--node <node-name>] [--stale-after <seconds>]\n"
      << "  comet-controller show-host-health [--db <path>] [--node <node-name>] [--stale-after <seconds>]\n"
      << "  comet-controller show-rollout-actions [--db <path>] [--node <node-name>]\n"
      << "  comet-controller show-rebalance-plan [--db <path>] [--node <node-name>]\n"
      << "  comet-controller apply-rebalance-proposal --worker <worker-name> [--db <path>] [--artifacts-root <path>]\n"
      << "  comet-controller reconcile-rebalance-proposals [--db <path>] [--artifacts-root <path>]\n"
      << "  comet-controller scheduler-tick [--db <path>] [--artifacts-root <path>]\n"
      << "  comet-controller set-rollout-action-status --id <action-id> --status <pending|acknowledged|ready-to-retry> [--message <text>] [--db <path>]\n"
      << "  comet-controller enqueue-rollout-eviction --id <action-id> [--db <path>]\n"
      << "  comet-controller reconcile-rollout-actions [--db <path>] [--artifacts-root <path>]\n"
      << "  comet-controller apply-ready-rollout-action --id <action-id> [--db <path>] [--artifacts-root <path>]\n"
      << "  comet-controller show-node-availability [--db <path>] [--node <node-name>]\n"
      << "  comet-controller set-node-availability --node <node-name> --availability <active|draining|unavailable> [--message <text>] [--db <path>]\n"
      << "  comet-controller retry-host-assignment --id <assignment-id> [--db <path>]\n"
      << "  comet-controller show-state [--db <path>]\n"
      << "  comet-controller render-infer-runtime [--db <path>]\n"
      << "  comet-controller render-compose [--db <path>] [--node <node-name>]\n";
}

std::optional<std::string> ParseNodeArg(int argc, char** argv) {
  for (int index = 2; index < argc; ++index) {
    const std::string arg = argv[index];
    if (arg == "--node" && index + 1 < argc) {
      return std::string(argv[index + 1]);
    }
  }
  return std::nullopt;
}

std::optional<std::string> ParseDbArg(int argc, char** argv) {
  for (int index = 2; index < argc; ++index) {
    const std::string arg = argv[index];
    if (arg == "--db" && index + 1 < argc) {
      return std::string(argv[index + 1]);
    }
  }
  return std::nullopt;
}

std::optional<std::string> ParseBundleArg(int argc, char** argv) {
  for (int index = 2; index < argc; ++index) {
    const std::string arg = argv[index];
    if (arg == "--bundle" && index + 1 < argc) {
      return std::string(argv[index + 1]);
    }
  }
  return std::nullopt;
}

std::optional<std::string> ParseArtifactsRootArg(int argc, char** argv) {
  for (int index = 2; index < argc; ++index) {
    const std::string arg = argv[index];
    if (arg == "--artifacts-root" && index + 1 < argc) {
      return std::string(argv[index + 1]);
    }
  }
  return std::nullopt;
}

std::optional<int> ParseIdArg(int argc, char** argv) {
  for (int index = 2; index < argc; ++index) {
    const std::string arg = argv[index];
    if (arg == "--id" && index + 1 < argc) {
      return std::stoi(argv[index + 1]);
    }
  }
  return std::nullopt;
}

std::optional<int> ParseStaleAfterArg(int argc, char** argv) {
  for (int index = 2; index < argc; ++index) {
    const std::string arg = argv[index];
    if (arg == "--stale-after" && index + 1 < argc) {
      return std::stoi(argv[index + 1]);
    }
  }
  return std::nullopt;
}

std::optional<std::string> ParseAvailabilityArg(int argc, char** argv) {
  for (int index = 2; index < argc; ++index) {
    const std::string arg = argv[index];
    if (arg == "--availability" && index + 1 < argc) {
      return std::string(argv[index + 1]);
    }
  }
  return std::nullopt;
}

std::optional<std::string> ParseMessageArg(int argc, char** argv) {
  for (int index = 2; index < argc; ++index) {
    const std::string arg = argv[index];
    if (arg == "--message" && index + 1 < argc) {
      return std::string(argv[index + 1]);
    }
  }
  return std::nullopt;
}

std::optional<std::string> ParseStatusArg(int argc, char** argv) {
  for (int index = 2; index < argc; ++index) {
    const std::string arg = argv[index];
    if (arg == "--status" && index + 1 < argc) {
      return std::string(argv[index + 1]);
    }
  }
  return std::nullopt;
}

std::optional<std::string> ParseWorkerArg(int argc, char** argv) {
  for (int index = 2; index < argc; ++index) {
    const std::string arg = argv[index];
    if (arg == "--worker" && index + 1 < argc) {
      return std::string(argv[index + 1]);
    }
  }
  return std::nullopt;
}

std::string ResolveDbPath(const std::optional<std::string>& db_arg) {
  return db_arg.value_or(DefaultDbPath());
}

std::string ResolveArtifactsRoot(const std::optional<std::string>& artifacts_root_arg) {
  return artifacts_root_arg.value_or(DefaultArtifactsRoot());
}

std::optional<comet::HostAssignment> FindLatestHostAssignmentForNode(
    const std::vector<comet::HostAssignment>& assignments,
    const std::string& node_name);

std::optional<comet::HostAssignment> FindLatestHostAssignmentForPlane(
    const std::vector<comet::HostAssignment>& assignments,
    const std::string& plane_name);

std::map<std::string, comet::NodeAvailabilityOverride> BuildAvailabilityOverrideMap(
    const std::vector<comet::NodeAvailabilityOverride>& availability_overrides);

comet::NodeAvailability ResolveNodeAvailability(
    const std::map<std::string, comet::NodeAvailabilityOverride>& availability_overrides,
    const std::string& node_name);

bool IsNodeSchedulable(comet::NodeAvailability availability);

void PrintAssignmentDispatchSummary(
    const comet::DesiredState& desired_state,
    const std::map<std::string, comet::NodeAvailabilityOverride>& availability_overrides,
    const std::vector<comet::HostObservation>& observations,
    int stale_after_seconds);

std::optional<long long> HeartbeatAgeSeconds(const std::string& heartbeat_at);

std::optional<long long> TimestampAgeSeconds(const std::string& timestamp_text);

std::string HealthFromAge(
    const std::optional<long long>& age_seconds,
    int stale_after_seconds);

std::optional<comet::RuntimeStatus> ParseRuntimeStatus(
    const comet::HostObservation& observation);

std::vector<comet::RuntimeProcessStatus> ParseInstanceRuntimeStatuses(
    const comet::HostObservation& observation);

std::optional<comet::GpuTelemetrySnapshot> ParseGpuTelemetry(
    const comet::HostObservation& observation);

std::optional<std::string> ObservedSchedulingGateReason(
    const std::vector<comet::HostObservation>& observations,
    const std::string& node_name,
    int stale_after_seconds);
std::optional<comet::GpuTelemetrySnapshot> ParseGpuTelemetry(
    const comet::HostObservation& observation);

int ReconcileRolloutActions(
    const std::string& db_path,
    const std::string& artifacts_root);

std::map<std::string, std::vector<comet::SchedulerRolloutAction>> BuildRolloutActionsByTargetNode(
    const comet::SchedulingPolicyReport& scheduling_report);

void PrintRolloutGateSummary(const comet::SchedulingPolicyReport& scheduling_report);

void PrintStateSummary(const comet::DesiredState& state) {
  std::cout << "plane: " << state.plane_name << "\n";
  std::cout << "control_root: " << state.control_root << "\n";
  std::cout << "inference:\n";
  std::cout << "  primary_infer_node=" << state.inference.primary_infer_node
            << " net_if=" << state.inference.net_if
            << " llama_port=" << state.inference.llama_port << "\n";
  std::cout << "gateway:\n";
  std::cout << "  listen=" << state.gateway.listen_host << ":" << state.gateway.listen_port
            << " server_name=" << state.gateway.server_name << "\n";
  std::cout << "nodes:\n";
  for (const auto& node : state.nodes) {
    std::cout << "  - " << node.name << " (" << node.platform << "), gpus=";
    for (std::size_t index = 0; index < node.gpu_devices.size(); ++index) {
      if (index > 0) {
        std::cout << ",";
      }
      const auto it = node.gpu_memory_mb.find(node.gpu_devices[index]);
      std::cout << node.gpu_devices[index];
      if (it != node.gpu_memory_mb.end()) {
        std::cout << "(" << it->second << "MB)";
      }
    }
    std::cout << "\n";
  }

  std::cout << "instances:\n";
  for (const auto& instance : state.instances) {
    std::cout << "  - " << instance.name
              << " role=" << comet::ToString(instance.role)
              << " node=" << instance.node_name;
    if (instance.gpu_device.has_value()) {
      std::cout << " gpu=" << *instance.gpu_device
                << " fraction=" << instance.gpu_fraction
                << " placement_mode=" << comet::ToString(instance.placement_mode)
                << " share_mode=" << comet::ToString(instance.share_mode)
                << " priority=" << instance.priority
                << " preemptible=" << (instance.preemptible ? "true" : "false");
      if (instance.memory_cap_mb.has_value()) {
        std::cout << " memory_cap_mb=" << *instance.memory_cap_mb;
      }
      const auto placement_it = instance.labels.find("comet.placement");
      if (placement_it != instance.labels.end()) {
        std::cout << " placement=" << placement_it->second;
      }
      const auto action_it = instance.labels.find("comet.placement.action");
      if (action_it != instance.labels.end()) {
        std::cout << " placement_action=" << action_it->second;
      }
      const auto score_it = instance.labels.find("comet.placement.score");
      if (score_it != instance.labels.end()) {
        std::cout << " placement_score=" << score_it->second;
      }
      const auto decision_it = instance.labels.find("comet.placement.decision");
      if (decision_it != instance.labels.end()) {
        std::cout << " placement_decision=" << decision_it->second;
      }
      const auto next_action_it = instance.labels.find("comet.placement.next_action");
      if (next_action_it != instance.labels.end()) {
        std::cout << " next_action=" << next_action_it->second;
      }
      const auto next_target_it = instance.labels.find("comet.placement.next_target");
      if (next_target_it != instance.labels.end()) {
        std::cout << " next_target=" << next_target_it->second;
      }
      const auto victims_it = instance.labels.find("comet.preemption.victims");
      if (victims_it != instance.labels.end()) {
        std::cout << " preemption_victims=" << victims_it->second;
      }
      const auto defer_reason_it = instance.labels.find("comet.placement.defer_reason");
      if (defer_reason_it != instance.labels.end()) {
        std::cout << " defer_reason=" << defer_reason_it->second;
      }
    }
    std::cout << "\n";
  }
}

void PrintSchedulerDecisionSummary(const comet::DesiredState& state) {
  bool has_decisions = false;
  for (const auto& instance : state.instances) {
    if (instance.role != comet::InstanceRole::Worker) {
      continue;
    }
    if (instance.labels.find("comet.placement.decision") == instance.labels.end()) {
      continue;
    }
    if (!has_decisions) {
      std::cout << "scheduler-decisions:\n";
      has_decisions = true;
    }

    std::cout << "  - worker=" << instance.name;
    const auto decision_it = instance.labels.find("comet.placement.decision");
    if (decision_it != instance.labels.end()) {
      std::cout << " decision=" << decision_it->second;
    }
    const auto next_action_it = instance.labels.find("comet.placement.next_action");
    if (next_action_it != instance.labels.end()) {
      std::cout << " next_action=" << next_action_it->second;
    }
    const auto next_target_it = instance.labels.find("comet.placement.next_target");
    if (next_target_it != instance.labels.end()) {
      std::cout << " next_target=" << next_target_it->second;
    }
    const auto victims_it = instance.labels.find("comet.preemption.victims");
    if (victims_it != instance.labels.end()) {
      std::cout << " victims=" << victims_it->second;
    }
    const auto defer_reason_it = instance.labels.find("comet.placement.defer_reason");
    if (defer_reason_it != instance.labels.end()) {
      std::cout << " defer_reason=" << defer_reason_it->second;
    }
    std::cout << "\n";
  }
}

std::map<std::string, std::vector<comet::SchedulerRolloutAction>> BuildRolloutActionsByTargetNode(
    const comet::SchedulingPolicyReport& scheduling_report) {
  std::map<std::string, std::vector<comet::SchedulerRolloutAction>> result;
  for (const auto& action : scheduling_report.rollout_actions) {
    result[action.target_node_name].push_back(action);
  }
  return result;
}

void PrintRolloutGateSummary(const comet::SchedulingPolicyReport& scheduling_report) {
  if (scheduling_report.rollout_actions.empty()) {
    return;
  }

  std::set<std::string> worker_names;
  std::set<std::string> node_names;
  for (const auto& action : scheduling_report.rollout_actions) {
    if (!action.worker_name.empty()) {
      worker_names.insert(action.worker_name);
    }
    if (!action.target_node_name.empty()) {
      node_names.insert(action.target_node_name);
    }
  }

  std::cout << "rollout-gates:\n";
  std::cout << "  gated_workers=" << worker_names.size()
            << " gated_nodes=" << node_names.size()
            << " deferred_actions=" << scheduling_report.rollout_actions.size() << "\n";
}

void PrintPersistedRolloutActions(
    const std::vector<comet::RolloutActionRecord>& actions) {
  std::cout << "rollout-actions:\n";
  if (actions.empty()) {
    std::cout << "  (empty)\n";
    return;
  }

  for (const auto& action : actions) {
    std::cout << "  - id=" << action.id
              << " generation=" << action.desired_generation
              << " step=" << action.step
              << " worker=" << action.worker_name
              << " action=" << action.action
              << " target=" << action.target_node_name << ":" << action.target_gpu_device
              << " status=" << comet::ToString(action.status);
    if (!action.victim_worker_names.empty()) {
      std::cout << " victims=";
      for (std::size_t index = 0; index < action.victim_worker_names.size(); ++index) {
        if (index > 0) {
          std::cout << ",";
        }
        std::cout << action.victim_worker_names[index];
      }
    }
    if (!action.reason.empty()) {
      std::cout << " reason=" << action.reason;
    }
    std::cout << "\n";
    if (!action.status_message.empty()) {
      std::cout << "    message=" << action.status_message << "\n";
    }
  }
}

std::optional<comet::RolloutActionRecord> FindRolloutActionById(
    const std::vector<comet::RolloutActionRecord>& actions,
    int action_id) {
  for (const auto& action : actions) {
    if (action.id == action_id) {
      return action;
    }
  }
  return std::nullopt;
}

void RemoveWorkerFromDesiredState(
    comet::DesiredState* state,
    const std::string& worker_name) {
  if (state == nullptr) {
    return;
  }

  state->instances.erase(
      std::remove_if(
          state->instances.begin(),
          state->instances.end(),
          [&](const comet::InstanceSpec& instance) { return instance.name == worker_name; }),
      state->instances.end());
  state->runtime_gpu_nodes.erase(
      std::remove_if(
          state->runtime_gpu_nodes.begin(),
          state->runtime_gpu_nodes.end(),
          [&](const comet::RuntimeGpuNode& gpu_node) { return gpu_node.name == worker_name; }),
      state->runtime_gpu_nodes.end());
  state->disks.erase(
      std::remove_if(
          state->disks.begin(),
          state->disks.end(),
          [&](const comet::DiskSpec& disk) {
            return disk.kind == comet::DiskKind::WorkerPrivate &&
                   disk.owner_name == worker_name;
          }),
      state->disks.end());
  for (auto& instance : state->instances) {
    instance.depends_on.erase(
        std::remove(instance.depends_on.begin(), instance.depends_on.end(), worker_name),
        instance.depends_on.end());
  }
}

void MaterializeRetryPlacementAction(
    comet::DesiredState* state,
    const comet::RolloutActionRecord& action,
    const std::vector<std::string>& victim_worker_names) {
  if (state == nullptr) {
    return;
  }

  for (const auto& victim_worker_name : victim_worker_names) {
    RemoveWorkerFromDesiredState(state, victim_worker_name);
  }

  auto instance_it = std::find_if(
      state->instances.begin(),
      state->instances.end(),
      [&](const comet::InstanceSpec& instance) {
        return instance.role == comet::InstanceRole::Worker &&
               instance.name == action.worker_name;
      });
  if (instance_it == state->instances.end()) {
    throw std::runtime_error(
        "worker '" + action.worker_name + "' not found in desired state");
  }

  instance_it->node_name = action.target_node_name;
  instance_it->gpu_device = action.target_gpu_device;
  instance_it->share_mode = comet::GpuShareMode::Exclusive;
  instance_it->gpu_fraction = 1.0;
  instance_it->labels["comet.node"] = action.target_node_name;
  instance_it->labels["comet.placement"] = "auto";
  instance_it->labels["comet.placement.action"] = "materialized-retry-placement";
  instance_it->labels["comet.placement.decision"] = "applied";
  instance_it->labels.erase("comet.placement.next_action");
  instance_it->labels.erase("comet.placement.next_target");
  instance_it->labels.erase("comet.placement.defer_reason");
  instance_it->labels.erase("comet.preemption.victims");

  auto runtime_gpu_it = std::find_if(
      state->runtime_gpu_nodes.begin(),
      state->runtime_gpu_nodes.end(),
      [&](const comet::RuntimeGpuNode& gpu_node) {
        return gpu_node.name == action.worker_name;
      });
  if (runtime_gpu_it != state->runtime_gpu_nodes.end()) {
    runtime_gpu_it->node_name = action.target_node_name;
    runtime_gpu_it->gpu_device = action.target_gpu_device;
    runtime_gpu_it->share_mode = comet::GpuShareMode::Exclusive;
    runtime_gpu_it->gpu_fraction = 1.0;
  }

  auto disk_it = std::find_if(
      state->disks.begin(),
      state->disks.end(),
      [&](const comet::DiskSpec& disk) {
        return disk.kind == comet::DiskKind::WorkerPrivate &&
               disk.owner_name == action.worker_name;
      });
  if (disk_it != state->disks.end()) {
    disk_it->node_name = action.target_node_name;
  }
}

std::string RolloutActionTag(int action_id) {
  return "rollout_action_id=" + std::to_string(action_id);
}

bool AssignmentReferencesRolloutAction(
    const comet::HostAssignment& assignment,
    int action_id) {
  return assignment.status_message.find(RolloutActionTag(action_id)) != std::string::npos;
}

std::vector<comet::HostAssignment> BuildEvictionAssignmentsForAction(
    const comet::DesiredState& desired_state,
    int desired_generation,
    const comet::RolloutActionRecord& action,
    const std::vector<comet::HostAssignment>& existing_assignments) {
  if (action.action != "evict-best-effort") {
    throw std::runtime_error(
        "rollout action id=" + std::to_string(action.id) +
        " is not an evict-best-effort action");
  }
  if (action.victim_worker_names.empty()) {
    throw std::runtime_error(
        "rollout action id=" + std::to_string(action.id) +
        " has no victim workers to evict");
  }

  std::map<std::string, std::vector<std::string>> victim_workers_by_node;
  for (const auto& victim_worker_name : action.victim_worker_names) {
    bool found = false;
    for (const auto& instance : desired_state.instances) {
      if (instance.role == comet::InstanceRole::Worker &&
          instance.name == victim_worker_name) {
        victim_workers_by_node[instance.node_name].push_back(victim_worker_name);
        found = true;
        break;
      }
    }
    if (!found) {
      throw std::runtime_error(
          "victim worker '" + victim_worker_name +
          "' not found in desired state for rollout action id=" +
          std::to_string(action.id));
    }
  }

  comet::DesiredState eviction_state = desired_state;
  int required_memory_cap_mb = 0;
  for (const auto& instance : desired_state.instances) {
    if (instance.role == comet::InstanceRole::Worker &&
        instance.name == action.worker_name) {
      required_memory_cap_mb = instance.memory_cap_mb.value_or(0);
      break;
    }
  }
  for (const auto& victim_worker_name : action.victim_worker_names) {
    RemoveWorkerFromDesiredState(&eviction_state, victim_worker_name);
  }

  const auto plane_assignment =
      FindLatestHostAssignmentForPlane(existing_assignments, desired_state.plane_name);
  std::vector<comet::HostAssignment> assignments;
  for (const auto& [node_name, victim_workers] : victim_workers_by_node) {
    comet::HostAssignment assignment;
    assignment.node_name = node_name;
    assignment.plane_name = desired_state.plane_name;
    assignment.desired_generation = desired_generation;
    assignment.assignment_type = "evict-workers";
    assignment.desired_state_json =
        comet::SerializeDesiredStateJson(
            comet::SliceDesiredStateForNode(eviction_state, node_name));
    const auto latest_assignment =
        FindLatestHostAssignmentForNode(existing_assignments, node_name);
    assignment.artifacts_root = latest_assignment.has_value()
                                    ? latest_assignment->artifacts_root
                                    : (plane_assignment.has_value()
                                           ? plane_assignment->artifacts_root
                                           : DefaultArtifactsRoot());
    assignment.status = comet::HostAssignmentStatus::Pending;
    std::ostringstream message;
    message << RolloutActionTag(action.id)
            << " evict workers for rollout worker=" << action.worker_name
            << " target_gpu=" << action.target_gpu_device
            << " required_memory_cap_mb=" << required_memory_cap_mb
            << " victims=";
    for (std::size_t index = 0; index < victim_workers.size(); ++index) {
      if (index > 0) {
        message << ",";
      }
      message << victim_workers[index];
    }
    assignment.status_message = message.str();
    assignments.push_back(std::move(assignment));
  }
  return assignments;
}

std::optional<comet::RolloutActionRecord> FindPriorRolloutActionForWorker(
    const std::vector<comet::RolloutActionRecord>& actions,
    const comet::RolloutActionRecord& action,
    const std::string& requested_action_name) {
  std::optional<comet::RolloutActionRecord> result;
  for (const auto& candidate_action : actions) {
    if (candidate_action.desired_generation != action.desired_generation ||
        candidate_action.worker_name != action.worker_name ||
        candidate_action.step >= action.step ||
        candidate_action.action != requested_action_name) {
      continue;
    }
    result = candidate_action;
  }
  return result;
}

bool AreRolloutEvictionAssignmentsApplied(
    const std::vector<comet::HostAssignment>& assignments,
    int action_id) {
  bool found = false;
  for (const auto& assignment : assignments) {
    if (assignment.assignment_type != "evict-workers" ||
        !AssignmentReferencesRolloutAction(assignment, action_id)) {
      continue;
    }
    found = true;
    if (assignment.status != comet::HostAssignmentStatus::Applied) {
      return false;
    }
  }
  return found;
}

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
  comet::PlacementMode placement_mode = comet::PlacementMode::Manual;
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
  std::optional<comet::SchedulerPlaneRuntime> plane_runtime;
  std::map<std::string, comet::SchedulerWorkerRuntime> worker_runtime_by_name;
  std::map<std::string, comet::SchedulerNodeRuntime> node_runtime_by_name;
};

void MaterializeRebalancePlanEntry(
    comet::DesiredState* state,
    const RebalancePlanEntry& entry) {
  if (state == nullptr) {
    return;
  }

  auto instance_it = std::find_if(
      state->instances.begin(),
      state->instances.end(),
      [&](const comet::InstanceSpec& instance) {
        return instance.role == comet::InstanceRole::Worker &&
               instance.name == entry.worker_name;
      });
  if (instance_it == state->instances.end()) {
    throw std::runtime_error(
        "worker '" + entry.worker_name + "' not found in desired state");
  }

  instance_it->node_name = entry.target_node_name;
  instance_it->gpu_device = entry.target_gpu_device;
  instance_it->environment["COMET_NODE_NAME"] = entry.target_node_name;
  if (!entry.target_gpu_device.empty()) {
    instance_it->environment["COMET_GPU_DEVICE"] = entry.target_gpu_device;
  } else {
    instance_it->environment.erase("COMET_GPU_DEVICE");
  }
  if (entry.action == "upgrade-to-exclusive") {
    instance_it->share_mode = comet::GpuShareMode::Exclusive;
    instance_it->gpu_fraction = 1.0;
  }
  instance_it->labels["comet.node"] = entry.target_node_name;
  instance_it->labels["comet.placement"] = "auto";
  instance_it->labels["comet.placement.action"] = "materialized-rebalance-" + entry.action;
  instance_it->labels["comet.placement.score"] = std::to_string(entry.score);
  instance_it->labels["comet.placement.decision"] = "applied";
  instance_it->labels.erase("comet.placement.next_action");
  instance_it->labels.erase("comet.placement.next_target");
  instance_it->labels.erase("comet.placement.defer_reason");
  instance_it->labels.erase("comet.preemption.victims");

  auto runtime_gpu_it = std::find_if(
      state->runtime_gpu_nodes.begin(),
      state->runtime_gpu_nodes.end(),
      [&](const comet::RuntimeGpuNode& gpu_node) {
        return gpu_node.name == entry.worker_name;
      });
  if (runtime_gpu_it != state->runtime_gpu_nodes.end()) {
    runtime_gpu_it->node_name = entry.target_node_name;
    runtime_gpu_it->gpu_device = entry.target_gpu_device;
    runtime_gpu_it->share_mode = instance_it->share_mode;
    runtime_gpu_it->gpu_fraction = instance_it->gpu_fraction;
  }

  auto disk_it = std::find_if(
      state->disks.begin(),
      state->disks.end(),
      [&](const comet::DiskSpec& disk) {
        return disk.kind == comet::DiskKind::WorkerPrivate &&
               disk.owner_name == entry.worker_name;
      });
  if (disk_it != state->disks.end()) {
    disk_it->node_name = entry.target_node_name;
  }
}

std::string ToString(SchedulerRolloutPhase phase) {
  switch (phase) {
    case SchedulerRolloutPhase::Planned:
      return "planned";
    case SchedulerRolloutPhase::EvictionEnqueued:
      return "eviction-enqueued";
    case SchedulerRolloutPhase::EvictionApplied:
      return "eviction-applied";
    case SchedulerRolloutPhase::RetryReady:
      return "retry-ready";
    case SchedulerRolloutPhase::RetryMaterialized:
      return "retry-materialized";
    case SchedulerRolloutPhase::HostFailed:
      return "host-failed";
    case SchedulerRolloutPhase::HostStale:
      return "host-stale";
    case SchedulerRolloutPhase::RuntimeFailed:
      return "runtime-failed";
    case SchedulerRolloutPhase::RolloutApplied:
      return "rollout-applied";
  }
  return "unknown";
}

bool HasRolloutEvictionAssignments(
    const std::vector<comet::HostAssignment>& assignments,
    int action_id) {
  for (const auto& assignment : assignments) {
    if (assignment.assignment_type == "evict-workers" &&
        AssignmentReferencesRolloutAction(assignment, action_id)) {
      return true;
    }
  }
  return false;
}

std::optional<comet::HostAssignment> FindLatestHostAssignmentForNodeGeneration(
    const std::vector<comet::HostAssignment>& assignments,
    const std::string& node_name,
    int desired_generation) {
  std::optional<comet::HostAssignment> result;
  for (const auto& assignment : assignments) {
    if (assignment.node_name != node_name ||
        assignment.desired_generation != desired_generation) {
      continue;
    }
    result = assignment;
  }
  return result;
}

std::optional<comet::HostObservation> FindHostObservationForNode(
    const std::vector<comet::HostObservation>& observations,
    const std::string& node_name) {
  for (const auto& observation : observations) {
    if (observation.node_name == node_name) {
      return observation;
    }
  }
  return std::nullopt;
}

std::vector<RolloutLifecycleEntry> BuildRolloutLifecycleEntries(
    const comet::DesiredState& desired_state,
    int desired_generation,
    const std::vector<comet::RolloutActionRecord>& rollout_actions,
    const std::vector<comet::HostAssignment>& assignments,
    const std::vector<comet::HostObservation>& observations) {
  std::map<std::string, std::vector<comet::RolloutActionRecord>> actions_by_worker;
  for (const auto& action : rollout_actions) {
    if (action.desired_generation == desired_generation) {
      actions_by_worker[action.worker_name].push_back(action);
    }
  }

  std::vector<RolloutLifecycleEntry> entries;
  for (auto& [worker_name, actions] : actions_by_worker) {
    std::sort(
        actions.begin(),
        actions.end(),
        [](const comet::RolloutActionRecord& left, const comet::RolloutActionRecord& right) {
          if (left.step != right.step) {
            return left.step < right.step;
          }
          return left.id < right.id;
        });

    const comet::RolloutActionRecord* evict_action = nullptr;
    const comet::RolloutActionRecord* retry_action = nullptr;
    for (const auto& action : actions) {
      if (action.action == "evict-best-effort" && evict_action == nullptr) {
        evict_action = &action;
      } else if (action.action == "retry-placement" && retry_action == nullptr) {
        retry_action = &action;
      }
    }
    if (evict_action == nullptr && retry_action == nullptr) {
      continue;
    }

    RolloutLifecycleEntry entry;
    entry.worker_name = worker_name;
    entry.desired_generation = desired_generation;
    const auto* target_action = retry_action != nullptr ? retry_action : evict_action;
    entry.target_node_name = target_action->target_node_name;
    entry.target_gpu_device = target_action->target_gpu_device;
    if (evict_action != nullptr) {
      entry.victim_worker_names = evict_action->victim_worker_names;
    }

    if (evict_action != nullptr) {
      entry.action_id = evict_action->id;
      if (evict_action->status == comet::RolloutActionStatus::Pending) {
        entry.phase = SchedulerRolloutPhase::Planned;
        entry.detail = "awaiting eviction enqueue";
      } else if (evict_action->status == comet::RolloutActionStatus::Acknowledged) {
        if (AreRolloutEvictionAssignmentsApplied(assignments, evict_action->id)) {
          entry.phase = SchedulerRolloutPhase::EvictionApplied;
          entry.detail = "eviction assignments applied";
        } else if (HasRolloutEvictionAssignments(assignments, evict_action->id)) {
          entry.phase = SchedulerRolloutPhase::EvictionEnqueued;
          entry.detail = "eviction assignments enqueued";
        } else {
          entry.phase = SchedulerRolloutPhase::EvictionEnqueued;
          entry.detail = evict_action->status_message.empty()
                             ? "eviction acknowledged"
                             : evict_action->status_message;
        }
      } else if (evict_action->status == comet::RolloutActionStatus::ReadyToRetry) {
        entry.phase = SchedulerRolloutPhase::EvictionApplied;
        entry.detail = "eviction completed";
      }
    }

    if (retry_action != nullptr &&
        retry_action->status == comet::RolloutActionStatus::ReadyToRetry) {
      entry.phase = SchedulerRolloutPhase::RetryReady;
      entry.action_id = retry_action->id;
      entry.detail = "retry placement can be materialized";
    }

    entries.push_back(std::move(entry));
  }

  for (const auto& instance : desired_state.instances) {
    if (instance.role != comet::InstanceRole::Worker) {
      continue;
    }
    const auto placement_action_it = instance.labels.find("comet.placement.action");
    const auto placement_decision_it = instance.labels.find("comet.placement.decision");
    if (placement_action_it == instance.labels.end() ||
        placement_decision_it == instance.labels.end() ||
        placement_action_it->second != "materialized-retry-placement" ||
        placement_decision_it->second != "applied") {
      continue;
    }
    if (actions_by_worker.find(instance.name) != actions_by_worker.end()) {
      continue;
    }

    RolloutLifecycleEntry entry;
    entry.worker_name = instance.name;
    entry.desired_generation = desired_generation;
    entry.phase = SchedulerRolloutPhase::RetryMaterialized;
    entry.target_node_name = instance.node_name;
    entry.target_gpu_device = instance.gpu_device.value_or("");

    const auto target_assignment =
        FindLatestHostAssignmentForNodeGeneration(
            assignments,
            instance.node_name,
            desired_generation);
    const auto target_observation =
        FindHostObservationForNode(observations, instance.node_name);
    if (target_observation.has_value() &&
        target_observation->status == comet::HostObservationStatus::Failed) {
      entry.phase = SchedulerRolloutPhase::HostFailed;
      entry.detail = "target node observation failed";
    } else if (target_observation.has_value() &&
               HealthFromAge(
                   HeartbeatAgeSeconds(target_observation->heartbeat_at),
                   DefaultStaleAfterSeconds()) == "stale") {
      entry.phase = SchedulerRolloutPhase::HostStale;
      entry.detail = "target node observation stale";
    } else if (target_observation.has_value() &&
               ParseRuntimeStatus(*target_observation).has_value() &&
               ParseRuntimeStatus(*target_observation)->runtime_phase == "failed") {
      entry.phase = SchedulerRolloutPhase::RuntimeFailed;
      entry.detail = "target runtime reported failed phase";
    } else if (target_observation.has_value() &&
               target_observation->status == comet::HostObservationStatus::Applied &&
               target_observation->applied_generation.has_value() &&
               *target_observation->applied_generation >= desired_generation) {
      entry.phase = SchedulerRolloutPhase::RolloutApplied;
      entry.detail = "target node observed desired generation applied";
    } else if (target_assignment.has_value()) {
      entry.detail =
          "target node assignment status=" + comet::ToString(target_assignment->status);
    } else {
      entry.detail = "materialized in desired state";
    }

    entries.push_back(std::move(entry));
  }

  std::sort(
      entries.begin(),
      entries.end(),
      [](const RolloutLifecycleEntry& left, const RolloutLifecycleEntry& right) {
        return left.worker_name < right.worker_name;
      });
  return entries;
}

void PrintRolloutLifecycleEntries(const std::vector<RolloutLifecycleEntry>& entries) {
  std::cout << "rollout-lifecycle:\n";
  if (entries.empty()) {
    std::cout << "  (empty)\n";
    return;
  }

  for (const auto& entry : entries) {
    std::cout << "  - worker=" << entry.worker_name
              << " generation=" << entry.desired_generation
              << " phase=" << ToString(entry.phase);
    if (entry.action_id.has_value()) {
      std::cout << " action_id=" << *entry.action_id;
    }
    if (!entry.target_node_name.empty() || !entry.target_gpu_device.empty()) {
      std::cout << " target=" << entry.target_node_name << ":" << entry.target_gpu_device;
    }
    if (!entry.victim_worker_names.empty()) {
      std::cout << " victims=";
      for (std::size_t index = 0; index < entry.victim_worker_names.size(); ++index) {
        if (index > 0) {
          std::cout << ",";
        }
        std::cout << entry.victim_worker_names[index];
      }
    }
    if (!entry.detail.empty()) {
      std::cout << " detail=" << entry.detail;
    }
    std::cout << "\n";
  }
}

std::optional<RolloutLifecycleEntry> FindRolloutLifecycleEntry(
    const std::vector<RolloutLifecycleEntry>& entries,
    const std::string& worker_name) {
  for (const auto& entry : entries) {
    if (entry.worker_name == worker_name) {
      return entry;
    }
  }
  return std::nullopt;
}

bool RolloutPhaseBlocksRebalance(SchedulerRolloutPhase phase) {
  return phase != SchedulerRolloutPhase::RolloutApplied;
}

bool HostAssignmentBlocksRebalance(const comet::HostAssignment& assignment) {
  return assignment.status == comet::HostAssignmentStatus::Pending ||
         assignment.status == comet::HostAssignmentStatus::Claimed;
}

bool NodeHasBlockingHostAssignment(
    const std::vector<comet::HostAssignment>& assignments,
    const std::string& node_name) {
  for (const auto& assignment : assignments) {
    if (assignment.node_name == node_name &&
        HostAssignmentBlocksRebalance(assignment)) {
      return true;
    }
  }
  return false;
}

RebalanceControllerGateSummary BuildRebalanceControllerGateSummary(
    const comet::DesiredState& desired_state,
    int desired_generation,
    const std::vector<comet::NodeAvailabilityOverride>& availability_overrides,
    const std::vector<RolloutLifecycleEntry>& rollout_lifecycle_entries,
    const std::vector<comet::HostAssignment>& assignments,
    const SchedulerRuntimeView& scheduler_runtime,
    const std::vector<comet::HostObservation>& observations,
    int stale_after_seconds) {
  RebalanceControllerGateSummary summary;
  std::set<std::string> active_rollout_workers;
  for (const auto& entry : rollout_lifecycle_entries) {
    if (RolloutPhaseBlocksRebalance(entry.phase)) {
      active_rollout_workers.insert(entry.worker_name);
    }
  }
  if (scheduler_runtime.plane_runtime.has_value() &&
      !scheduler_runtime.plane_runtime->active_action.empty() &&
      !scheduler_runtime.plane_runtime->active_worker_name.empty()) {
    active_rollout_workers.insert(scheduler_runtime.plane_runtime->active_worker_name);
  }

  std::set<std::string> blocking_assignment_nodes;
  for (const auto& assignment : assignments) {
    if (HostAssignmentBlocksRebalance(assignment)) {
      blocking_assignment_nodes.insert(assignment.node_name);
    }
  }

  summary.active_rollout_workers.assign(
      active_rollout_workers.begin(), active_rollout_workers.end());
  summary.blocking_assignment_nodes.assign(
      blocking_assignment_nodes.begin(), blocking_assignment_nodes.end());
  summary.active_rollout_count =
      static_cast<int>(summary.active_rollout_workers.size());
  summary.blocking_assignment_count =
      static_cast<int>(summary.blocking_assignment_nodes.size());

  const auto availability_override_map =
      BuildAvailabilityOverrideMap(availability_overrides);
  std::set<std::string> unconverged_nodes;
  for (const auto& node : desired_state.nodes) {
    if (!IsNodeSchedulable(
            ResolveNodeAvailability(availability_override_map, node.name))) {
      continue;
    }
    const auto observation = FindHostObservationForNode(observations, node.name);
    if (!observation.has_value()) {
      unconverged_nodes.insert(node.name);
      continue;
    }
    if (observation->status == comet::HostObservationStatus::Failed) {
      unconverged_nodes.insert(node.name);
      continue;
    }
    const auto age_seconds = HeartbeatAgeSeconds(observation->heartbeat_at);
    if (HealthFromAge(age_seconds, stale_after_seconds) != "online") {
      unconverged_nodes.insert(node.name);
      continue;
    }
    if (!observation->applied_generation.has_value() ||
        *observation->applied_generation != desired_generation) {
      unconverged_nodes.insert(node.name);
      continue;
    }
  }

  summary.unconverged_nodes.assign(
      unconverged_nodes.begin(), unconverged_nodes.end());
  summary.unconverged_node_count =
      static_cast<int>(summary.unconverged_nodes.size());
  summary.cluster_ready =
      summary.active_rollout_count == 0 &&
      summary.blocking_assignment_count == 0 &&
      summary.unconverged_node_count == 0;
  return summary;
}

const comet::InstanceSpec* FindWorkerInstance(
    const comet::DesiredState& state,
    const std::string& worker_name) {
  for (const auto& instance : state.instances) {
    if (instance.role == comet::InstanceRole::Worker && instance.name == worker_name) {
      return &instance;
    }
  }
  return nullptr;
}

constexpr int ComputePressureUtilizationThresholdPct() {
  return 85;
}

constexpr int ObservedMoveVramReserveMb() {
  return 1024;
}

std::optional<comet::GpuDeviceTelemetry> FindObservedGpuDeviceTelemetry(
    const std::vector<comet::HostObservation>& observations,
    const std::string& node_name,
    const std::string& gpu_device) {
  const auto observation = FindHostObservationForNode(observations, node_name);
  if (!observation.has_value()) {
    return std::nullopt;
  }
  const auto telemetry = ParseGpuTelemetry(*observation);
  if (!telemetry.has_value()) {
    return std::nullopt;
  }
  for (const auto& device : telemetry->devices) {
    if (device.gpu_device == gpu_device) {
      return device;
    }
  }
  return std::nullopt;
}

bool ObservedGpuDeviceHasForeignProcess(
    const std::vector<comet::HostObservation>& observations,
    const std::string& node_name,
    const std::string& gpu_device,
    const std::string& worker_name) {
  const auto device = FindObservedGpuDeviceTelemetry(observations, node_name, gpu_device);
  if (!device.has_value()) {
    return false;
  }
  for (const auto& process : device->processes) {
    if (process.instance_name != worker_name && process.instance_name != "unknown") {
      return true;
    }
  }
  return false;
}

std::optional<std::string> ObservedGpuPlacementGateReason(
    const std::vector<comet::HostObservation>& observations,
    const comet::InstanceSpec& worker,
    const std::string& target_node_name,
    const std::string& target_gpu_device,
    bool moving_to_different_gpu) {
  const auto device = FindObservedGpuDeviceTelemetry(observations, target_node_name, target_gpu_device);
  if (!device.has_value()) {
    return std::nullopt;
  }

  if (worker.memory_cap_mb.has_value() &&
      device->free_vram_mb < (*worker.memory_cap_mb + ObservedMoveVramReserveMb())) {
    return std::string("observed-insufficient-vram");
  }

  if (moving_to_different_gpu &&
      device->gpu_utilization_pct >= ComputePressureUtilizationThresholdPct() &&
      ObservedGpuDeviceHasForeignProcess(observations, target_node_name, target_gpu_device, worker.name)) {
    return std::string("compute-pressure");
  }

  return std::nullopt;
}

std::vector<RebalancePlanEntry> BuildRebalancePlanEntries(
    const comet::DesiredState& state,
    const comet::SchedulingPolicyReport& scheduling_report,
    const std::vector<comet::NodeAvailabilityOverride>& availability_overrides,
    const std::vector<RolloutLifecycleEntry>& rollout_lifecycle_entries,
    const std::vector<comet::HostAssignment>& assignments,
    const SchedulerRuntimeView& scheduler_runtime,
    const std::vector<comet::HostObservation>& observations,
    int stale_after_seconds,
    const std::optional<std::string>& node_name_filter = std::nullopt) {
  std::vector<RebalancePlanEntry> entries;
  for (const auto& recommendation : scheduling_report.placement_recommendations) {
    const auto* worker = FindWorkerInstance(state, recommendation.worker_name);
    if (worker == nullptr) {
      continue;
    }
    if (worker->placement_mode == comet::PlacementMode::Manual) {
      continue;
    }
    if (node_name_filter.has_value() && worker->node_name != *node_name_filter) {
      bool candidate_matches = false;
      for (const auto& candidate : recommendation.candidates) {
        if (candidate.node_name == *node_name_filter) {
          candidate_matches = true;
          break;
        }
      }
      if (!candidate_matches) {
        continue;
      }
    }

    RebalancePlanEntry entry;
    entry.worker_name = recommendation.worker_name;
    entry.placement_mode = worker->placement_mode;
    entry.current_node_name = recommendation.current_node_name;
    entry.current_gpu_device = recommendation.current_gpu_device;
    const auto availability_override_map =
        BuildAvailabilityOverrideMap(availability_overrides);
    const auto source_availability =
        ResolveNodeAvailability(availability_override_map, recommendation.current_node_name);
    const bool source_requires_exit = source_availability != comet::NodeAvailability::Active;

    const auto worker_runtime_it =
        scheduler_runtime.worker_runtime_by_name.find(recommendation.worker_name);
    if (worker_runtime_it != scheduler_runtime.worker_runtime_by_name.end() &&
        worker_runtime_it->second.manual_intervention_required) {
      entry.rebalance_class = "gated";
      entry.decision = "hold";
      entry.state = "manual-intervention-required";
      entry.gate_reason = "manual-intervention-required";
      entries.push_back(std::move(entry));
      continue;
    }

    if (scheduler_runtime.plane_runtime.has_value() &&
        !scheduler_runtime.plane_runtime->active_action.empty() &&
        scheduler_runtime.plane_runtime->active_worker_name == recommendation.worker_name) {
      entry.rebalance_class = "gated";
      entry.decision = "hold";
      entry.state = scheduler_runtime.plane_runtime->phase.empty()
                        ? "active-scheduler-action"
                        : scheduler_runtime.plane_runtime->phase;
      entry.target_node_name = scheduler_runtime.plane_runtime->target_node_name;
      entry.target_gpu_device = scheduler_runtime.plane_runtime->target_gpu_device;
      entry.gate_reason = scheduler_runtime.plane_runtime->active_action;
      entries.push_back(std::move(entry));
      continue;
    }

    if (scheduler_runtime.plane_runtime.has_value() &&
        !scheduler_runtime.plane_runtime->active_action.empty() &&
        scheduler_runtime.plane_runtime->active_worker_name != recommendation.worker_name) {
      entry.rebalance_class = "gated";
      entry.decision = "hold";
      entry.state = "active-scheduler-action";
      entry.gate_reason = scheduler_runtime.plane_runtime->active_action;
      entries.push_back(std::move(entry));
      continue;
    }

    const auto rollout_lifecycle =
        FindRolloutLifecycleEntry(rollout_lifecycle_entries, recommendation.worker_name);
    if (rollout_lifecycle.has_value() &&
        RolloutPhaseBlocksRebalance(rollout_lifecycle->phase)) {
      entry.rebalance_class = "rollout-class";
      entry.decision = "hold";
      entry.state = "active-rollout";
      entry.target_node_name = rollout_lifecycle->target_node_name;
      entry.target_gpu_device = rollout_lifecycle->target_gpu_device;
      entry.gate_reason = ToString(rollout_lifecycle->phase);
      entries.push_back(std::move(entry));
      continue;
    }

    const comet::PlacementCandidate* selected_candidate = nullptr;
    if (source_requires_exit) {
      for (const auto& candidate : recommendation.candidates) {
        if (candidate.action == "insufficient-memory" ||
            candidate.action == "insufficient-fraction") {
          continue;
        }
        const auto target_availability =
            ResolveNodeAvailability(availability_override_map, candidate.node_name);
        if (candidate.node_name != recommendation.current_node_name &&
            IsNodeSchedulable(target_availability)) {
          selected_candidate = &candidate;
          break;
        }
      }
    }
    if (selected_candidate == nullptr) {
      for (const auto& candidate : recommendation.candidates) {
        if (candidate.action == "insufficient-memory" ||
            candidate.action == "insufficient-fraction") {
          continue;
        }
        selected_candidate = &candidate;
        break;
      }
    }
    if (selected_candidate == nullptr && !recommendation.candidates.empty()) {
      selected_candidate = &recommendation.candidates.front();
    }
    if (selected_candidate == nullptr) {
      entry.rebalance_class = source_requires_exit ? "gated" : "no-candidate";
      entry.decision = "hold";
      entry.state = source_requires_exit ? "draining-source" : "no-candidate";
      entry.gate_reason =
          source_requires_exit ? "no-active-drain-target" : std::string{};
      entries.push_back(std::move(entry));
      continue;
    }

    entry.target_node_name = selected_candidate->node_name;
    entry.target_gpu_device = selected_candidate->gpu_device;
    entry.action = selected_candidate->action;
    entry.score = selected_candidate->score;
    entry.preemption_required = selected_candidate->preemption_required;
    entry.victim_worker_names = selected_candidate->preemption_victims;
    const auto target_availability =
        ResolveNodeAvailability(availability_override_map, selected_candidate->node_name);

    if (worker_runtime_it != scheduler_runtime.worker_runtime_by_name.end()) {
      const auto last_move_age = TimestampAgeSeconds(worker_runtime_it->second.last_move_at);
      if (last_move_age.has_value() &&
          *last_move_age < WorkerMinimumResidencySeconds()) {
        entry.rebalance_class = "stable";
        entry.decision = "hold";
        entry.state = "min-residency";
        entry.gate_reason =
            "min-residency(" + std::to_string(*last_move_age) + "<" +
            std::to_string(WorkerMinimumResidencySeconds()) + ")";
        entries.push_back(std::move(entry));
        continue;
      }
    }

    auto source_node_runtime_it =
        scheduler_runtime.node_runtime_by_name.find(recommendation.current_node_name);
    auto target_node_runtime_it =
        scheduler_runtime.node_runtime_by_name.find(selected_candidate->node_name);
    const auto source_move_age =
        source_node_runtime_it == scheduler_runtime.node_runtime_by_name.end()
            ? std::optional<long long>{}
            : TimestampAgeSeconds(source_node_runtime_it->second.last_move_at);
    const auto target_move_age =
        target_node_runtime_it == scheduler_runtime.node_runtime_by_name.end()
            ? std::optional<long long>{}
            : TimestampAgeSeconds(target_node_runtime_it->second.last_move_at);
    if ((source_move_age.has_value() && *source_move_age < NodeCooldownAfterMoveSeconds()) ||
        (target_move_age.has_value() && *target_move_age < NodeCooldownAfterMoveSeconds())) {
      entry.rebalance_class = "gated";
      entry.decision = "hold";
      entry.state = "cooldown";
      if (source_move_age.has_value() && target_move_age.has_value() &&
          *source_move_age < NodeCooldownAfterMoveSeconds() &&
          *target_move_age < NodeCooldownAfterMoveSeconds()) {
        entry.gate_reason = "cooldown-source-and-target";
      } else if (source_move_age.has_value() && *source_move_age < NodeCooldownAfterMoveSeconds()) {
        entry.gate_reason = "cooldown-source";
      } else {
        entry.gate_reason = "cooldown-target";
      }
      entries.push_back(std::move(entry));
      continue;
    }

    if (source_requires_exit &&
        selected_candidate->node_name == recommendation.current_node_name) {
      entry.rebalance_class = "gated";
      entry.decision = "hold";
      entry.state = "draining-source";
      entry.gate_reason = "no-active-drain-target";
      entries.push_back(std::move(entry));
      continue;
    }

    if (!IsNodeSchedulable(target_availability)) {
      entry.rebalance_class = "gated";
      entry.decision = "hold";
      entry.state = "gated-target";
      entry.gate_reason =
          target_availability == comet::NodeAvailability::Draining
              ? "draining-target"
              : "unavailable-target";
      entries.push_back(std::move(entry));
      continue;
    }

    const bool source_assignment_busy =
        NodeHasBlockingHostAssignment(assignments, recommendation.current_node_name);
    const bool target_assignment_busy =
        NodeHasBlockingHostAssignment(assignments, selected_candidate->node_name);
    if (source_assignment_busy || target_assignment_busy) {
      entry.rebalance_class = "gated";
      entry.decision = "hold";
      entry.state = "assignment-in-flight";
      if (source_assignment_busy && target_assignment_busy) {
        entry.gate_reason = "source-and-target-node-busy";
      } else if (source_assignment_busy) {
        entry.gate_reason = "source-node-busy";
      } else {
        entry.gate_reason = "target-node-busy";
      }
      entries.push_back(std::move(entry));
      continue;
    }

    const auto gate_reason =
        ObservedSchedulingGateReason(
            observations, selected_candidate->node_name, stale_after_seconds);
    if (gate_reason.has_value()) {
      entry.rebalance_class = "gated";
      entry.decision = "hold";
      entry.state = "gated-target";
      entry.gate_reason = *gate_reason;
    } else if (const auto gpu_gate_reason =
                   ObservedGpuPlacementGateReason(
                       observations,
                       *worker,
                       selected_candidate->node_name,
                       selected_candidate->gpu_device,
                       selected_candidate->node_name != recommendation.current_node_name ||
                           selected_candidate->gpu_device != recommendation.current_gpu_device);
               gpu_gate_reason.has_value()) {
      entry.rebalance_class = "gated";
      entry.decision = "hold";
      entry.state = "gated-target";
      entry.gate_reason = *gpu_gate_reason;
    } else if (selected_candidate->preemption_required) {
      entry.rebalance_class = "rollout-class";
      entry.decision = "defer";
      entry.state = source_requires_exit ? "drain-preemption" : "deferred-preemption";
    } else if (selected_candidate->score < MinimumSafeDirectRebalanceScore()) {
      entry.rebalance_class = "stable";
      entry.decision = "hold";
      entry.state = "below-threshold";
      entry.gate_reason =
          "score-below-threshold(" + std::to_string(selected_candidate->score) + "<" +
          std::to_string(MinimumSafeDirectRebalanceScore()) + ")";
    } else if (selected_candidate->same_node &&
               selected_candidate->action == "upgrade-to-exclusive") {
      entry.rebalance_class = "safe-direct";
      entry.decision = "propose";
      entry.state = "ready-in-place-upgrade";
    } else if (selected_candidate->same_node) {
      entry.rebalance_class = "stable";
      entry.decision = "hold";
      entry.state = "stay";
    } else {
      entry.rebalance_class = "safe-direct";
      entry.decision = "propose";
      entry.state = source_requires_exit ? "ready-drain-move" : "ready-move";
    }

    entries.push_back(std::move(entry));
  }

  std::sort(
      entries.begin(),
      entries.end(),
      [](const RebalancePlanEntry& left, const RebalancePlanEntry& right) {
        if (left.worker_name != right.worker_name) {
          return left.worker_name < right.worker_name;
        }
        return left.target_node_name < right.target_node_name;
      });
  return entries;
}

void PrintRebalancePlanEntries(const std::vector<RebalancePlanEntry>& entries) {
  std::cout << "rebalance-plan:\n";
  if (entries.empty()) {
    std::cout << "  (empty)\n";
    return;
  }
  for (const auto& entry : entries) {
    std::cout << "  - worker=" << entry.worker_name
              << " placement_mode=" << comet::ToString(entry.placement_mode)
              << " current=" << entry.current_node_name << ":" << entry.current_gpu_device
              << " class=" << (entry.rebalance_class.empty() ? "(empty)" : entry.rebalance_class)
              << " decision=" << entry.decision
              << " state=" << entry.state;
    if (!entry.target_node_name.empty() || !entry.target_gpu_device.empty()) {
      std::cout << " target=" << entry.target_node_name << ":" << entry.target_gpu_device;
    }
    if (!entry.action.empty()) {
      std::cout << " action=" << entry.action;
    }
    std::cout << " score=" << entry.score
              << " preemption_required=" << (entry.preemption_required ? "yes" : "no");
    if (!entry.victim_worker_names.empty()) {
      std::cout << " victims=";
      for (std::size_t index = 0; index < entry.victim_worker_names.size(); ++index) {
        if (index > 0) {
          std::cout << ",";
        }
        std::cout << entry.victim_worker_names[index];
      }
    }
    if (!entry.gate_reason.empty()) {
      std::cout << " gate_reason=" << entry.gate_reason;
    }
    std::cout << "\n";
  }
}

RebalancePolicySummary BuildRebalancePolicySummary(
    const std::vector<RebalancePlanEntry>& entries) {
  RebalancePolicySummary summary;
  for (const auto& entry : entries) {
    if (entry.state == "no-candidate") {
      ++summary.gated_count;
      ++summary.no_candidate_count;
      summary.gated_workers.push_back(entry.worker_name);
      summary.no_candidate_workers.push_back(entry.worker_name);
      continue;
    }
    if (entry.decision == "propose") {
      ++summary.actionable_count;
      ++summary.safe_direct_count;
      ++summary.propose_count;
      summary.actionable_workers.push_back(entry.worker_name);
      summary.safe_direct_workers.push_back(entry.worker_name);
      summary.proposed_workers.push_back(entry.worker_name);
      continue;
    }
    if (entry.decision == "defer") {
      ++summary.rollout_class_count;
      ++summary.defer_count;
      summary.rollout_class_workers.push_back(entry.worker_name);
      summary.deferred_workers.push_back(entry.worker_name);
      continue;
    }
    if (entry.state == "active-rollout") {
      ++summary.rollout_class_count;
      ++summary.blocked_active_rollout_count;
      summary.rollout_class_workers.push_back(entry.worker_name);
      summary.blocked_active_rollout_workers.push_back(entry.worker_name);
    } else if (entry.state == "assignment-in-flight" ||
               entry.state == "gated-target" ||
               entry.state == "draining-source" ||
               entry.state == "manual-intervention-required" ||
               entry.state == "active-scheduler-action" ||
               entry.state == "cooldown" ||
               entry.state == "min-residency") {
      ++summary.gated_count;
      summary.gated_workers.push_back(entry.worker_name);
      if (entry.state == "assignment-in-flight") {
        ++summary.assignment_busy_count;
        summary.assignment_busy_workers.push_back(entry.worker_name);
      } else if (entry.state == "gated-target") {
        ++summary.observation_gated_count;
        summary.observation_gated_workers.push_back(entry.worker_name);
      }
    } else {
      if (entry.state == "below-threshold") {
        ++summary.below_threshold_count;
        summary.below_threshold_workers.push_back(entry.worker_name);
      }
      ++summary.stable_hold_count;
      summary.stable_hold_workers.push_back(entry.worker_name);
    }
    ++summary.hold_count;
    summary.held_workers.push_back(entry.worker_name);
  }
  return summary;
}

void PrintWorkerListLine(
    const std::string& label,
    const std::vector<std::string>& workers) {
  if (workers.empty()) {
    return;
  }
  std::cout << "  " << label << "=";
  for (std::size_t index = 0; index < workers.size(); ++index) {
    if (index > 0) {
      std::cout << ",";
    }
    std::cout << workers[index];
  }
  std::cout << "\n";
}

void PrintRebalancePolicySummary(const RebalancePolicySummary& summary) {
  std::cout << "rebalance-policy:\n";
  std::cout << "  actionable=" << summary.actionable_count
            << " safe_direct=" << summary.safe_direct_count
            << " rollout_class=" << summary.rollout_class_count
            << " gated=" << summary.gated_count
            << " blocked_active_rollouts=" << summary.blocked_active_rollout_count
            << " assignment_busy=" << summary.assignment_busy_count
            << " observation_gated=" << summary.observation_gated_count
            << " stable_holds=" << summary.stable_hold_count
            << " below_threshold=" << summary.below_threshold_count
            << " deferred=" << summary.defer_count
            << " no_candidate=" << summary.no_candidate_count << "\n";
  std::cout << "  propose=" << summary.propose_count
            << " hold=" << summary.hold_count
            << " defer=" << summary.defer_count
            << " no_candidate=" << summary.no_candidate_count << "\n";
  PrintWorkerListLine("actionable_workers", summary.actionable_workers);
  PrintWorkerListLine("safe_direct_workers", summary.safe_direct_workers);
  PrintWorkerListLine("rollout_class_workers", summary.rollout_class_workers);
  PrintWorkerListLine("gated_workers", summary.gated_workers);
  PrintWorkerListLine(
      "blocked_active_rollout_workers", summary.blocked_active_rollout_workers);
  PrintWorkerListLine("assignment_busy_workers", summary.assignment_busy_workers);
  PrintWorkerListLine("observation_gated_workers", summary.observation_gated_workers);
  PrintWorkerListLine("stable_hold_workers", summary.stable_hold_workers);
  PrintWorkerListLine("below_threshold_workers", summary.below_threshold_workers);
  PrintWorkerListLine("proposed_workers", summary.proposed_workers);
  PrintWorkerListLine("held_workers", summary.held_workers);
  PrintWorkerListLine("deferred_workers", summary.deferred_workers);
  PrintWorkerListLine("no_candidate_workers", summary.no_candidate_workers);
}

void PrintRebalanceControllerGateSummary(
    const RebalanceControllerGateSummary& summary) {
  std::cout << "rebalance-controller-gate:\n";
  std::cout << "  cluster_ready=" << (summary.cluster_ready ? "yes" : "no")
            << " active_rollouts=" << summary.active_rollout_count
            << " blocking_assignment_nodes=" << summary.blocking_assignment_count
            << " unconverged_nodes=" << summary.unconverged_node_count << "\n";
  PrintWorkerListLine("active_rollout_workers", summary.active_rollout_workers);
  PrintWorkerListLine("blocking_assignment_nodes", summary.blocking_assignment_nodes);
  PrintWorkerListLine("unconverged_nodes", summary.unconverged_nodes);
}

RebalanceIterationBudgetSummary BuildRebalanceIterationBudgetSummary(int current_iteration) {
  RebalanceIterationBudgetSummary summary;
  summary.current_iteration = current_iteration;
  summary.max_iterations = MaximumRebalanceIterationsPerGeneration();
  summary.exhausted = summary.current_iteration >= summary.max_iterations;
  return summary;
}

void PrintRebalanceIterationBudgetSummary(
    const RebalanceIterationBudgetSummary& summary) {
  std::cout << "rebalance-iteration-budget:\n";
  std::cout << "  iteration=" << summary.current_iteration << "/" << summary.max_iterations
            << " exhausted=" << (summary.exhausted ? "yes" : "no") << "\n";
}

RebalanceLoopStatusSummary BuildRebalanceLoopStatusSummary(
    const RebalanceControllerGateSummary& controller_gate_summary,
    const RebalanceIterationBudgetSummary& iteration_budget_summary,
    const RebalancePolicySummary& policy_summary) {
  RebalanceLoopStatusSummary summary;
  if (!controller_gate_summary.cluster_ready) {
    summary.state = "waiting-for-convergence";
    if (controller_gate_summary.unconverged_node_count > 0) {
      summary.reason =
          "unconverged-nodes=" + std::to_string(controller_gate_summary.unconverged_node_count);
    } else if (controller_gate_summary.blocking_assignment_count > 0) {
      summary.reason =
          "blocking-assignments=" + std::to_string(controller_gate_summary.blocking_assignment_count);
    } else {
      summary.reason =
          "active-rollouts=" + std::to_string(controller_gate_summary.active_rollout_count);
    }
    return summary;
  }
  if (iteration_budget_summary.exhausted && policy_summary.actionable_count > 0) {
    summary.state = "complete";
    summary.reason =
        "rebalance-iteration-limit=" + std::to_string(iteration_budget_summary.current_iteration) +
        "/" + std::to_string(iteration_budget_summary.max_iterations);
    return summary;
  }
  if (policy_summary.rollout_class_count > 0) {
    summary.state = "waiting-for-rollout";
    summary.reason = "rollout-class-workers=" + std::to_string(policy_summary.rollout_class_count);
    return summary;
  }
  if (policy_summary.actionable_count > 0) {
    summary.state = "actionable";
    summary.reason = "safe-direct-workers=" + std::to_string(policy_summary.actionable_count);
    return summary;
  }
  summary.state = "complete";
  if (policy_summary.below_threshold_count > 0) {
    summary.reason =
        "remaining-moves-below-threshold=" + std::to_string(policy_summary.below_threshold_count);
  } else if (policy_summary.no_candidate_count > 0) {
    summary.reason =
        "no-candidate-workers=" + std::to_string(policy_summary.no_candidate_count);
  } else {
    summary.reason = "no-actionable-rebalance";
  }
  return summary;
}

void PrintRebalanceLoopStatusSummary(const RebalanceLoopStatusSummary& summary) {
  std::cout << "rebalance-loop-status:\n";
  std::cout << "  state=" << summary.state;
  if (!summary.reason.empty()) {
    std::cout << " reason=" << summary.reason;
  }
  std::cout << "\n";
}

void ShowDemoPlan() {
  PrintStateSummary(comet::BuildDemoState());
}

void PrintPreviewSummary(const comet::DesiredState& state) {
  std::cout << "preview:\n";
  std::cout << "  plane=" << state.plane_name << "\n";
  std::cout << "  nodes=" << state.nodes.size() << "\n";
  std::cout << "  disks=" << state.disks.size() << "\n";
  std::cout << "  instances=" << state.instances.size() << "\n";

  const auto node_plans = comet::BuildNodeComposePlans(state);
  for (const auto& plan : node_plans) {
    std::cout << "  node " << plan.node_name
              << ": services=" << plan.services.size()
              << " disks=" << plan.disks.size() << "\n";
  }
}

int RenderComposeForState(
    const comet::DesiredState& state,
    const std::optional<std::string>& node_name) {
  if (node_name.has_value()) {
    const auto plan = comet::FindNodeComposePlan(state, *node_name);
    if (!plan.has_value()) {
      std::cerr << "error: node '" << *node_name << "' not found in state\n";
      return 1;
    }
    std::cout << comet::RenderComposeYaml(*plan);
    return 0;
  }

  const auto plans = comet::BuildNodeComposePlans(state);
  for (std::size_t index = 0; index < plans.size(); ++index) {
    if (index > 0) {
      std::cout << "\n---\n";
    }
    std::cout << comet::RenderComposeYaml(plans[index]);
  }
  return 0;
}

int RenderDemoCompose(const std::optional<std::string>& node_name) {
  return RenderComposeForState(comet::BuildDemoState(), node_name);
}

int ValidateBundle(const std::string& bundle_dir) {
  const comet::DesiredState state = comet::ImportPlaneBundle(bundle_dir);
  comet::RequireSchedulingPolicy(state);
  const comet::SchedulingPolicyReport scheduling_report =
      comet::EvaluateSchedulingPolicy(state);
  std::cout << "bundle validation: OK\n";
  PrintPreviewSummary(state);
  std::cout << comet::RenderSchedulingPolicyReport(scheduling_report);
  PrintSchedulerDecisionSummary(state);
  PrintRolloutGateSummary(scheduling_report);
  return 0;
}

int PreviewBundle(const std::string& bundle_dir, const std::optional<std::string>& node_name) {
  const comet::DesiredState state = comet::ImportPlaneBundle(bundle_dir);
  const comet::SchedulingPolicyReport scheduling_report =
      comet::EvaluateSchedulingPolicy(state);
  PrintPreviewSummary(state);
  std::cout << comet::RenderSchedulingPolicyReport(scheduling_report);
  PrintSchedulerDecisionSummary(state);
  PrintRolloutGateSummary(scheduling_report);
  std::cout << "\ncompose-preview:\n";
  return RenderComposeForState(state, node_name);
}

int PlanBundle(const std::string& db_path, const std::string& bundle_dir) {
  comet::ControllerStore store(db_path);
  store.Initialize();
  const auto current_state = store.LoadDesiredState();
  const comet::DesiredState desired_state = comet::ImportPlaneBundle(bundle_dir);
  const auto availability_overrides = store.LoadNodeAvailabilityOverrides();
  const auto observations = store.LoadHostObservations();
  const comet::ReconcilePlan plan =
      comet::BuildReconcilePlan(current_state, desired_state);
  const comet::SchedulingPolicyReport scheduling_report =
      comet::EvaluateSchedulingPolicy(desired_state);

  std::cout << "bundle-plan:\n";
  std::cout << "  db=" << db_path << "\n";
  std::cout << "  bundle=" << bundle_dir << "\n";
  std::cout << comet::RenderReconcilePlan(plan);
  std::cout << comet::RenderSchedulingPolicyReport(scheduling_report);
  PrintSchedulerDecisionSummary(desired_state);
  PrintRolloutGateSummary(scheduling_report);
  PrintAssignmentDispatchSummary(
      desired_state,
      BuildAvailabilityOverrideMap(availability_overrides),
      observations,
      DefaultStaleAfterSeconds());
  return 0;
}

std::vector<comet::NodeExecutionPlan> FilterNodeExecutionPlans(
    const std::vector<comet::NodeExecutionPlan>& plans,
    const std::optional<std::string>& node_name) {
  if (!node_name.has_value()) {
    return plans;
  }

  std::vector<comet::NodeExecutionPlan> filtered;
  for (const auto& plan : plans) {
    if (plan.node_name == *node_name) {
      filtered.push_back(plan);
    }
  }

  if (filtered.empty()) {
    throw std::runtime_error("node '" + *node_name + "' not found in execution plan");
  }

  return filtered;
}

std::map<std::string, comet::NodeComposePlan> BuildComposePlanMap(const comet::DesiredState& state) {
  std::map<std::string, comet::NodeComposePlan> result;
  for (const auto& plan : comet::BuildNodeComposePlans(state)) {
    result.emplace(plan.node_name, plan);
  }
  return result;
}

std::map<std::string, comet::NodeAvailabilityOverride> BuildAvailabilityOverrideMap(
    const std::vector<comet::NodeAvailabilityOverride>& availability_overrides) {
  std::map<std::string, comet::NodeAvailabilityOverride> result;
  for (const auto& availability_override : availability_overrides) {
    result.emplace(availability_override.node_name, availability_override);
  }
  return result;
}

comet::NodeAvailability ResolveNodeAvailability(
    const std::map<std::string, comet::NodeAvailabilityOverride>& availability_overrides,
    const std::string& node_name) {
  const auto it = availability_overrides.find(node_name);
  if (it == availability_overrides.end()) {
    return comet::NodeAvailability::Active;
  }
  return it->second.availability;
}

bool IsNodeSchedulable(comet::NodeAvailability availability) {
  return availability == comet::NodeAvailability::Active;
}

void PrintNodeAvailabilityOverrides(
    const std::vector<comet::NodeAvailabilityOverride>& availability_overrides) {
  std::cout << "node-availability:\n";
  if (availability_overrides.empty()) {
    std::cout << "  (empty)\n";
    return;
  }

  for (const auto& availability_override : availability_overrides) {
    std::cout << "  - node=" << availability_override.node_name
              << " availability=" << comet::ToString(availability_override.availability)
              << " updated_at=" << availability_override.updated_at << "\n";
    if (!availability_override.status_message.empty()) {
      std::cout << "    message=" << availability_override.status_message << "\n";
    }
  }
}

std::optional<std::string> ObservedSchedulingGateReason(
    const std::vector<comet::HostObservation>& observations,
    const std::string& node_name,
    int stale_after_seconds) {
  const auto observation = FindHostObservationForNode(observations, node_name);
  if (!observation.has_value()) {
    return std::nullopt;
  }
  if (observation->status == comet::HostObservationStatus::Failed) {
    return std::string("failed");
  }
  const auto age_seconds = HeartbeatAgeSeconds(observation->heartbeat_at);
  if (HealthFromAge(age_seconds, stale_after_seconds) == "stale") {
    return std::string("stale");
  }
  const auto runtime_status = ParseRuntimeStatus(*observation);
  if (runtime_status.has_value() && runtime_status->runtime_phase == "failed") {
    return std::string("runtime-failed");
  }
  const auto gpu_telemetry = ParseGpuTelemetry(*observation);
  if (gpu_telemetry.has_value() && gpu_telemetry->degraded) {
    return std::string("telemetry-degraded");
  }
  return std::nullopt;
}

void PrintAssignmentDispatchSummary(
    const comet::DesiredState& desired_state,
    const std::map<std::string, comet::NodeAvailabilityOverride>& availability_overrides,
    const std::vector<comet::HostObservation>& observations,
    int stale_after_seconds) {
  std::size_t schedulable_nodes = 0;
  std::vector<std::string> skipped_nodes;
  for (const auto& node : desired_state.nodes) {
    const auto availability = ResolveNodeAvailability(availability_overrides, node.name);
    if (!IsNodeSchedulable(availability)) {
      skipped_nodes.push_back(
          node.name + "(" + comet::ToString(availability) + ")");
      continue;
    }
    const auto observed_gate_reason =
        ObservedSchedulingGateReason(observations, node.name, stale_after_seconds);
    if (observed_gate_reason.has_value()) {
      skipped_nodes.push_back(node.name + "(" + *observed_gate_reason + ")");
      continue;
    }
    ++schedulable_nodes;
  }

  std::cout << "assignment-dispatch:\n";
  std::cout << "  schedulable_nodes=" << schedulable_nodes << "/" << desired_state.nodes.size()
            << "\n";
  if (!skipped_nodes.empty()) {
    std::cout << "  skipped_nodes=";
    for (std::size_t index = 0; index < skipped_nodes.size(); ++index) {
      if (index > 0) {
        std::cout << ",";
      }
      std::cout << skipped_nodes[index];
    }
    std::cout << "\n";
  }
}

void WriteTextFile(const std::string& path, const std::string& contents) {
  const std::filesystem::path file_path(path);
  if (file_path.has_parent_path()) {
    std::filesystem::create_directories(file_path.parent_path());
  }

  std::ofstream out(path, std::ios::binary | std::ios::trunc);
  if (!out.is_open()) {
    throw std::runtime_error("failed to open file for write: " + path);
  }
  out << contents;
  if (!out.good()) {
    throw std::runtime_error("failed to write file: " + path);
  }
}

void RemoveFileIfExists(const std::string& path) {
  std::error_code error;
  std::filesystem::remove(path, error);
  if (error) {
    throw std::runtime_error("failed to remove file '" + path + "': " + error.message());
  }
}

void MaterializeComposeArtifacts(
    const comet::DesiredState& desired_state,
    const std::vector<comet::NodeExecutionPlan>& host_plans) {
  const auto desired_compose_plans = BuildComposePlanMap(desired_state);

  for (const auto& host_plan : host_plans) {
    for (const auto& operation : host_plan.operations) {
      if (operation.kind == comet::HostOperationKind::WriteComposeFile) {
        const auto compose_it = desired_compose_plans.find(host_plan.node_name);
        if (compose_it == desired_compose_plans.end()) {
          throw std::runtime_error(
              "missing compose plan for node '" + host_plan.node_name + "'");
        }
        WriteTextFile(operation.target, comet::RenderComposeYaml(compose_it->second));
      }

      if (operation.kind == comet::HostOperationKind::RemoveComposeFile) {
        RemoveFileIfExists(operation.target);
      }
    }
  }
}

std::string InferRuntimeArtifactPath(
    const std::string& artifacts_root,
    const std::string& plane_name) {
  return (
      std::filesystem::path(artifacts_root) / plane_name / "infer-runtime.json").string();
}

void MaterializeInferRuntimeArtifact(
    const comet::DesiredState& desired_state,
    const std::string& artifacts_root) {
  WriteTextFile(
      InferRuntimeArtifactPath(artifacts_root, desired_state.plane_name),
      comet::RenderInferRuntimeConfigJson(desired_state));
}

std::vector<comet::HostAssignment> BuildHostAssignments(
    const comet::DesiredState& desired_state,
    const std::string& artifacts_root,
    int desired_generation,
    const std::vector<comet::NodeAvailabilityOverride>& availability_overrides,
    const std::vector<comet::HostObservation>& observations,
    const std::optional<comet::SchedulingPolicyReport>& scheduling_report = std::nullopt) {
  std::vector<comet::HostAssignment> assignments;
  assignments.reserve(desired_state.nodes.size());
  const auto availability_override_map =
      BuildAvailabilityOverrideMap(availability_overrides);
  const auto rollout_actions_by_target_node =
      scheduling_report.has_value()
          ? BuildRolloutActionsByTargetNode(*scheduling_report)
          : std::map<std::string, std::vector<comet::SchedulerRolloutAction>>{};

  for (const auto& node : desired_state.nodes) {
    if (!IsNodeSchedulable(
            ResolveNodeAvailability(availability_override_map, node.name))) {
      continue;
    }
    if (ObservedSchedulingGateReason(
            observations, node.name, DefaultStaleAfterSeconds()).has_value()) {
      continue;
    }
    comet::HostAssignment assignment;
    assignment.node_name = node.name;
    assignment.plane_name = desired_state.plane_name;
    assignment.desired_generation = desired_generation;
    assignment.assignment_type = "apply-node-state";
    assignment.desired_state_json =
        comet::SerializeDesiredStateJson(
            comet::SliceDesiredStateForNode(desired_state, node.name));
    assignment.artifacts_root = artifacts_root;
    assignment.status = comet::HostAssignmentStatus::Pending;
    const auto rollout_it = rollout_actions_by_target_node.find(node.name);
    if (rollout_it != rollout_actions_by_target_node.end() &&
        !rollout_it->second.empty()) {
      std::set<std::string> gated_workers;
      for (const auto& action : rollout_it->second) {
        if (!action.worker_name.empty()) {
          gated_workers.insert(action.worker_name);
        }
      }
      std::ostringstream message;
      message << "scheduler rollout actions pending on target node " << node.name << " for workers ";
      bool first = true;
      for (const auto& worker_name : gated_workers) {
        if (!first) {
          message << ",";
        }
        first = false;
        message << worker_name;
      }
      assignment.status_message = message.str();
    }
    assignments.push_back(std::move(assignment));
  }

  return assignments;
}

std::optional<comet::HostAssignment> FindLatestHostAssignmentForNode(
    const std::vector<comet::HostAssignment>& assignments,
    const std::string& node_name) {
  std::optional<comet::HostAssignment> result;
  for (const auto& assignment : assignments) {
    if (assignment.node_name != node_name) {
      continue;
    }
    result = assignment;
  }
  return result;
}

std::optional<comet::HostAssignment> FindLatestHostAssignmentForPlane(
    const std::vector<comet::HostAssignment>& assignments,
    const std::string& plane_name) {
  std::optional<comet::HostAssignment> result;
  for (const auto& assignment : assignments) {
    if (assignment.plane_name != plane_name) {
      continue;
    }
    result = assignment;
  }
  return result;
}

bool AssignmentRepresentsDrainedNode(const comet::HostAssignment& assignment) {
  return assignment.assignment_type == "drain-node-state" &&
         (assignment.status == comet::HostAssignmentStatus::Pending ||
          assignment.status == comet::HostAssignmentStatus::Claimed ||
          assignment.status == comet::HostAssignmentStatus::Applied);
}

bool ObservedNodeStateNeedsResync(
    const comet::DesiredState& desired_state,
    const std::string& node_name,
    const comet::HostObservation& observation) {
  if (observation.observed_state_json.empty()) {
    return true;
  }

  const comet::DesiredState observed_node_state =
      comet::DeserializeDesiredStateJson(observation.observed_state_json);
  const comet::DesiredState desired_node_state =
      comet::SliceDesiredStateForNode(desired_state, node_name);

  if (desired_node_state.disks.empty() && desired_node_state.instances.empty()) {
    return false;
  }

  return observed_node_state.disks.empty() || observed_node_state.instances.empty();
}

std::optional<comet::HostAssignment> BuildResyncAssignmentForNode(
    const comet::DesiredState& desired_state,
    int desired_generation,
    const std::string& node_name,
    const std::vector<comet::HostAssignment>& existing_assignments,
    const std::optional<comet::HostObservation>& observation) {
  bool node_exists = false;
  for (const auto& node : desired_state.nodes) {
    if (node.name == node_name) {
      node_exists = true;
      break;
    }
  }
  if (!node_exists) {
    return std::nullopt;
  }

  const auto latest_assignment = FindLatestHostAssignmentForNode(existing_assignments, node_name);
  const bool latest_assignment_is_drain =
      latest_assignment.has_value() && latest_assignment->desired_generation == desired_generation &&
      AssignmentRepresentsDrainedNode(*latest_assignment);

  if (observation.has_value() &&
      observation->applied_generation.has_value() &&
      *observation->applied_generation == desired_generation &&
      observation->status != comet::HostObservationStatus::Failed &&
      !latest_assignment_is_drain &&
      !ObservedNodeStateNeedsResync(desired_state, node_name, *observation)) {
    return std::nullopt;
  }

  if (latest_assignment.has_value() &&
      latest_assignment->desired_generation == desired_generation &&
      latest_assignment->assignment_type == "apply-node-state" &&
      (latest_assignment->status == comet::HostAssignmentStatus::Pending ||
       latest_assignment->status == comet::HostAssignmentStatus::Claimed ||
       latest_assignment->status == comet::HostAssignmentStatus::Applied)) {
    return std::nullopt;
  }

  comet::HostAssignment assignment;
  assignment.node_name = node_name;
  assignment.plane_name = desired_state.plane_name;
  assignment.desired_generation = desired_generation;
  assignment.assignment_type = "apply-node-state";
  assignment.desired_state_json =
      comet::SerializeDesiredStateJson(
          comet::SliceDesiredStateForNode(desired_state, node_name));
  const auto plane_assignment =
      FindLatestHostAssignmentForPlane(existing_assignments, desired_state.plane_name);
  assignment.artifacts_root = latest_assignment.has_value()
                                  ? latest_assignment->artifacts_root
                                  : (plane_assignment.has_value()
                                         ? plane_assignment->artifacts_root
                                         : DefaultArtifactsRoot());
  assignment.status = comet::HostAssignmentStatus::Pending;
  assignment.status_message = "resync after node returned to active";
  return assignment;
}

std::optional<comet::NodeInventory> FindNodeInventory(
    const comet::DesiredState& desired_state,
    const std::string& node_name) {
  for (const auto& node : desired_state.nodes) {
    if (node.name == node_name) {
      return node;
    }
  }
  return std::nullopt;
}

std::optional<comet::HostAssignment> BuildDrainAssignmentForNode(
    const comet::DesiredState& desired_state,
    int desired_generation,
    const std::string& node_name,
    const std::vector<comet::HostAssignment>& existing_assignments) {
  const auto node = FindNodeInventory(desired_state, node_name);
  if (!node.has_value()) {
    return std::nullopt;
  }

  comet::DesiredState drain_state;
  drain_state.plane_name = desired_state.plane_name;
  drain_state.plane_shared_disk_name = desired_state.plane_shared_disk_name;
  drain_state.control_root = desired_state.control_root;
  drain_state.inference = desired_state.inference;
  drain_state.gateway = desired_state.gateway;
  drain_state.runtime_gpu_nodes = desired_state.runtime_gpu_nodes;
  drain_state.nodes.push_back(*node);

  const auto latest_assignment = FindLatestHostAssignmentForNode(existing_assignments, node_name);
  const auto plane_assignment =
      FindLatestHostAssignmentForPlane(existing_assignments, desired_state.plane_name);

  comet::HostAssignment assignment;
  assignment.node_name = node_name;
  assignment.plane_name = desired_state.plane_name;
  assignment.desired_generation = desired_generation;
  assignment.assignment_type = "drain-node-state";
  assignment.desired_state_json = comet::SerializeDesiredStateJson(drain_state);
  assignment.artifacts_root = latest_assignment.has_value()
                                  ? latest_assignment->artifacts_root
                                  : (plane_assignment.has_value()
                                         ? plane_assignment->artifacts_root
                                         : DefaultArtifactsRoot());
  assignment.status = comet::HostAssignmentStatus::Pending;
  assignment.status_message = "drain after node availability changed";
  return assignment;
}

void PrintHostAssignments(const std::vector<comet::HostAssignment>& assignments) {
  std::cout << "host-assignments:\n";
  if (assignments.empty()) {
    std::cout << "  (empty)\n";
    return;
  }

  for (const auto& assignment : assignments) {
    const comet::DesiredState desired_node_state =
        comet::DeserializeDesiredStateJson(assignment.desired_state_json);
    std::cout << "  - id=" << assignment.id
              << " node=" << assignment.node_name
              << " plane=" << assignment.plane_name
              << " generation=" << assignment.desired_generation
              << " attempts=" << assignment.attempt_count << "/" << assignment.max_attempts
              << " type=" << assignment.assignment_type
              << " status=" << comet::ToString(assignment.status)
              << " instances=" << desired_node_state.instances.size()
              << " artifacts_root=" << assignment.artifacts_root << "\n";
    if (!assignment.status_message.empty()) {
      std::cout << "    message=" << assignment.status_message << "\n";
    }
  }
}

std::time_t ToUtcTime(std::tm* timestamp) {
#if defined(_WIN32)
  return _mkgmtime(timestamp);
#else
  return timegm(timestamp);
#endif
}

std::optional<long long> HeartbeatAgeSeconds(const std::string& heartbeat_at) {
  if (heartbeat_at.empty()) {
    return std::nullopt;
  }

  std::tm heartbeat_tm{};
  std::istringstream input(heartbeat_at);
  input >> std::get_time(&heartbeat_tm, "%Y-%m-%d %H:%M:%S");
  if (input.fail()) {
    return std::nullopt;
  }

  const std::time_t heartbeat_time = ToUtcTime(&heartbeat_tm);
  if (heartbeat_time < 0) {
    return std::nullopt;
  }

  const std::time_t now = std::time(nullptr);
  return static_cast<long long>(now - heartbeat_time);
}

std::optional<long long> TimestampAgeSeconds(const std::string& timestamp_text) {
  return HeartbeatAgeSeconds(timestamp_text);
}

SchedulerRuntimeView LoadSchedulerRuntimeView(
    comet::ControllerStore& store,
    const std::optional<comet::DesiredState>& desired_state) {
  SchedulerRuntimeView view;
  if (!desired_state.has_value()) {
    return view;
  }
  view.plane_runtime = store.LoadSchedulerPlaneRuntime(desired_state->plane_name);
  for (const auto& runtime : store.LoadSchedulerWorkerRuntimes(desired_state->plane_name)) {
    view.worker_runtime_by_name.emplace(runtime.worker_name, runtime);
  }
  for (const auto& runtime : store.LoadSchedulerNodeRuntimes(desired_state->plane_name)) {
    view.node_runtime_by_name.emplace(runtime.node_name, runtime);
  }
  return view;
}

void PrintSchedulerRuntimeView(const SchedulerRuntimeView& view) {
  std::cout << "scheduler-runtime:\n";
  if (!view.plane_runtime.has_value()) {
    std::cout << "  plane_action=(none)\n";
  } else {
    const auto& runtime = *view.plane_runtime;
    std::cout << "  plane_action="
              << (runtime.active_action.empty() ? "(none)" : runtime.active_action)
              << " phase=" << (runtime.phase.empty() ? "(empty)" : runtime.phase)
              << " worker="
              << (runtime.active_worker_name.empty() ? "(empty)" : runtime.active_worker_name)
              << " generation=" << runtime.action_generation
              << " stable_samples=" << runtime.stable_samples << "/"
              << VerificationStableSamplesRequired()
              << " rollback_attempts=" << runtime.rollback_attempt_count << "\n";
    if (!runtime.status_message.empty()) {
      std::cout << "  status_message=" << runtime.status_message << "\n";
    }
  }
  if (!view.worker_runtime_by_name.empty()) {
    std::cout << "  worker_runtime:\n";
    for (const auto& [worker_name, runtime] : view.worker_runtime_by_name) {
      std::cout << "    - worker=" << worker_name
                << " last_move_at="
                << (runtime.last_move_at.empty() ? "(empty)" : runtime.last_move_at)
                << " last_eviction_at="
                << (runtime.last_eviction_at.empty() ? "(empty)" : runtime.last_eviction_at);
      if (runtime.last_verified_generation.has_value()) {
        std::cout << " last_verified_generation=" << *runtime.last_verified_generation;
      }
      if (!runtime.last_scheduler_phase.empty()) {
        std::cout << " last_phase=" << runtime.last_scheduler_phase;
      }
      std::cout << " manual_intervention_required="
                << (runtime.manual_intervention_required ? "yes" : "no") << "\n";
      if (!runtime.last_status_message.empty()) {
        std::cout << "      status_message=" << runtime.last_status_message << "\n";
      }
    }
  }
  if (!view.node_runtime_by_name.empty()) {
    std::cout << "  node_runtime:\n";
    for (const auto& [node_name, runtime] : view.node_runtime_by_name) {
      std::cout << "    - node=" << node_name
                << " last_move_at="
                << (runtime.last_move_at.empty() ? "(empty)" : runtime.last_move_at);
      if (runtime.last_verified_generation.has_value()) {
        std::cout << " last_verified_generation=" << *runtime.last_verified_generation;
      }
      std::cout << "\n";
    }
  }
}

const comet::RuntimeProcessStatus* FindInstanceRuntimeStatus(
    const std::vector<comet::RuntimeProcessStatus>& statuses,
    const std::string& instance_name,
    const std::string& gpu_device) {
  for (const auto& status : statuses) {
    if (status.instance_name == instance_name &&
        status.gpu_device == gpu_device) {
      return &status;
    }
  }
  return nullptr;
}

bool TelemetryShowsOwnedProcess(
    const std::optional<comet::GpuTelemetrySnapshot>& telemetry,
    const std::string& gpu_device,
    const std::string& instance_name) {
  if (!telemetry.has_value()) {
    return false;
  }
  for (const auto& device : telemetry->devices) {
    if (device.gpu_device != gpu_device) {
      continue;
    }
    for (const auto& process : device.processes) {
      if (process.instance_name == instance_name) {
        return true;
      }
    }
  }
  return false;
}

struct SchedulerVerificationResult {
  bool converged = false;
  bool stable = false;
  bool timed_out = false;
  int next_stable_samples = 0;
  std::string detail;
};

SchedulerVerificationResult EvaluateSchedulerActionVerification(
    const comet::SchedulerPlaneRuntime& plane_runtime,
    const std::vector<comet::HostObservation>& observations) {
  SchedulerVerificationResult result;
  const bool rollback_mode = plane_runtime.phase == "rollback-applied" ||
                             plane_runtime.phase == "rollback-planned";
  const std::string expected_node =
      rollback_mode ? plane_runtime.source_node_name : plane_runtime.target_node_name;
  const std::string expected_gpu =
      rollback_mode ? plane_runtime.source_gpu_device : plane_runtime.target_gpu_device;
  const std::string cleared_node =
      rollback_mode ? plane_runtime.target_node_name : plane_runtime.source_node_name;
  const std::string cleared_gpu =
      rollback_mode ? plane_runtime.target_gpu_device : plane_runtime.source_gpu_device;

  const auto target_observation = FindHostObservationForNode(observations, expected_node);
  const auto source_observation = FindHostObservationForNode(observations, cleared_node);
  if (!target_observation.has_value()) {
    result.detail = "missing-target-observation";
  } else {
    const auto target_runtimes = ParseInstanceRuntimeStatuses(*target_observation);
    const auto target_runtime =
        FindInstanceRuntimeStatus(target_runtimes, plane_runtime.active_worker_name, expected_gpu);
    const auto target_telemetry = ParseGpuTelemetry(*target_observation);
    const bool target_generation_applied =
        target_observation->applied_generation.has_value() &&
        *target_observation->applied_generation >= plane_runtime.action_generation;
    const bool target_runtime_ready =
        target_runtime != nullptr &&
        target_runtime->ready &&
        (target_runtime->runtime_phase == "running" ||
         target_runtime->runtime_phase == "ready" ||
         target_runtime->runtime_phase == "loaded");
    const bool target_gpu_owned =
        TelemetryShowsOwnedProcess(
            target_telemetry, expected_gpu, plane_runtime.active_worker_name);

    bool source_cleared = true;
    if (source_observation.has_value()) {
      const auto source_runtimes = ParseInstanceRuntimeStatuses(*source_observation);
      const auto source_runtime =
          FindInstanceRuntimeStatus(
              source_runtimes,
              plane_runtime.active_worker_name,
              cleared_gpu);
      const auto source_telemetry = ParseGpuTelemetry(*source_observation);
      source_cleared =
          source_runtime == nullptr &&
          !TelemetryShowsOwnedProcess(
              source_telemetry, cleared_gpu, plane_runtime.active_worker_name);
    }

    result.converged =
        target_generation_applied && target_runtime_ready && target_gpu_owned && source_cleared;
    if (result.converged) {
      result.next_stable_samples = plane_runtime.stable_samples + 1;
      result.stable = result.next_stable_samples >= VerificationStableSamplesRequired();
      result.detail = "verified-sample";
    } else {
      result.next_stable_samples = 0;
      std::ostringstream detail;
      detail << "target_generation_applied=" << (target_generation_applied ? "yes" : "no")
             << " target_runtime_ready=" << (target_runtime_ready ? "yes" : "no")
             << " target_gpu_owned=" << (target_gpu_owned ? "yes" : "no")
             << " source_cleared=" << (source_cleared ? "yes" : "no");
      result.detail = detail.str();
    }
  }

  const auto action_age = TimestampAgeSeconds(plane_runtime.started_at);
  result.timed_out =
      action_age.has_value() && *action_age >= VerificationTimeoutSeconds();
  return result;
}

std::string UtcNowSqlTimestamp() {
  const std::time_t now = std::time(nullptr);
  std::tm tm{};
  gmtime_r(&now, &tm);
  std::ostringstream out;
  out << std::put_time(&tm, "%Y-%m-%d %H:%M:%S");
  return out.str();
}

void MarkWorkerMoveVerified(
    comet::ControllerStore* store,
    const comet::SchedulerPlaneRuntime& plane_runtime) {
  if (store == nullptr) {
    return;
  }
  const std::string now = UtcNowSqlTimestamp();
  comet::SchedulerWorkerRuntime worker_runtime;
  if (const auto current = store->LoadSchedulerWorkerRuntime(plane_runtime.active_worker_name);
      current.has_value()) {
    worker_runtime = *current;
  }
  worker_runtime.plane_name = plane_runtime.plane_name;
  worker_runtime.worker_name = plane_runtime.active_worker_name;
  worker_runtime.last_move_at = now;
  worker_runtime.last_verified_generation = plane_runtime.action_generation;
  worker_runtime.last_scheduler_phase = "verified";
  worker_runtime.last_status_message =
      plane_runtime.phase == "rollback-applied"
          ? "rollback verification succeeded"
          : "move verification succeeded";
  worker_runtime.manual_intervention_required = false;
  store->UpsertSchedulerWorkerRuntime(worker_runtime);

  for (const auto& node_name : {plane_runtime.source_node_name, plane_runtime.target_node_name}) {
    if (node_name.empty()) {
      continue;
    }
    comet::SchedulerNodeRuntime node_runtime;
    if (const auto current = store->LoadSchedulerNodeRuntime(node_name); current.has_value()) {
      node_runtime = *current;
    }
    node_runtime.plane_name = plane_runtime.plane_name;
    node_runtime.node_name = node_name;
    node_runtime.last_move_at = now;
    node_runtime.last_verified_generation = plane_runtime.action_generation;
    store->UpsertSchedulerNodeRuntime(node_runtime);
  }
}

void MarkWorkersEvicted(
    comet::ControllerStore* store,
    const std::string& plane_name,
    const std::vector<std::string>& worker_names) {
  if (store == nullptr) {
    return;
  }
  const std::string now = UtcNowSqlTimestamp();
  for (const auto& worker_name : worker_names) {
    if (worker_name.empty()) {
      continue;
    }
    comet::SchedulerWorkerRuntime runtime;
    if (const auto current = store->LoadSchedulerWorkerRuntime(worker_name); current.has_value()) {
      runtime = *current;
    }
    runtime.plane_name = plane_name;
    runtime.worker_name = worker_name;
    runtime.last_eviction_at = now;
    runtime.last_scheduler_phase = "evicted";
    runtime.last_status_message = "eviction verified";
    store->UpsertSchedulerWorkerRuntime(runtime);
  }
}

std::string HealthFromAge(
    const std::optional<long long>& age_seconds,
    int stale_after_seconds) {
  if (!age_seconds.has_value()) {
    return "unknown";
  }
  return *age_seconds > stale_after_seconds ? "stale" : "online";
}

std::optional<comet::RuntimeStatus> ParseRuntimeStatus(
    const comet::HostObservation& observation) {
  if (observation.runtime_status_json.empty()) {
    return std::nullopt;
  }
  return comet::DeserializeRuntimeStatusJson(observation.runtime_status_json);
}

std::vector<comet::RuntimeProcessStatus> ParseInstanceRuntimeStatuses(
    const comet::HostObservation& observation) {
  if (observation.instance_runtime_json.empty()) {
    return {};
  }
  return comet::DeserializeRuntimeStatusListJson(observation.instance_runtime_json);
}

std::optional<comet::GpuTelemetrySnapshot> ParseGpuTelemetry(
    const comet::HostObservation& observation) {
  if (observation.gpu_telemetry_json.empty()) {
    return std::nullopt;
  }
  return comet::DeserializeGpuTelemetryJson(observation.gpu_telemetry_json);
}

void PrintHostObservations(
    const std::vector<comet::HostObservation>& observations,
    int stale_after_seconds) {
  std::cout << "host-observations:\n";
  if (observations.empty()) {
    std::cout << "  (empty)\n";
    return;
  }

  for (const auto& observation : observations) {
    std::size_t disk_count = 0;
    std::size_t instance_count = 0;
    if (!observation.observed_state_json.empty()) {
      const comet::DesiredState observed_state =
          comet::DeserializeDesiredStateJson(observation.observed_state_json);
      disk_count = observed_state.disks.size();
      instance_count = observed_state.instances.size();
    }
    const auto runtime_status = ParseRuntimeStatus(observation);
    const auto instance_statuses = ParseInstanceRuntimeStatuses(observation);
    const auto gpu_telemetry = ParseGpuTelemetry(observation);
    const auto age_seconds = HeartbeatAgeSeconds(observation.heartbeat_at);

    std::cout << "  - node=" << observation.node_name
              << " plane=" << (observation.plane_name.empty() ? "(none)" : observation.plane_name)
              << " status=" << comet::ToString(observation.status);
    if (observation.applied_generation.has_value()) {
      std::cout << " applied_generation=" << *observation.applied_generation;
    }
    if (observation.last_assignment_id.has_value()) {
      std::cout << " last_assignment_id=" << *observation.last_assignment_id;
    }
    std::cout << " disks=" << disk_count
              << " instances=" << instance_count
              << " heartbeat_at=" << observation.heartbeat_at;
    if (age_seconds.has_value()) {
      std::cout << " age_seconds=" << *age_seconds
                << " health=" << HealthFromAge(age_seconds, stale_after_seconds);
    }
    if (runtime_status.has_value()) {
      std::cout << " runtime_backend="
                << (runtime_status->runtime_backend.empty()
                        ? "(empty)"
                        : runtime_status->runtime_backend)
                << " runtime_phase="
                << (runtime_status->runtime_phase.empty()
                        ? "(empty)"
                        : runtime_status->runtime_phase)
                << " runtime_launch_ready="
                << (runtime_status->launch_ready ? "yes" : "no")
                << " runtime_model="
                << (runtime_status->active_model_id.empty()
                        ? "(empty)"
                        : runtime_status->active_model_id)
                << " gateway="
                << (runtime_status->gateway_listen.empty()
                        ? "(empty)"
                        : runtime_status->gateway_listen);
    }
    if (gpu_telemetry.has_value()) {
      std::cout << " telemetry_source="
                << (gpu_telemetry->source.empty() ? "(empty)" : gpu_telemetry->source)
                << " telemetry_degraded=" << (gpu_telemetry->degraded ? "yes" : "no")
                << " gpu_devices=" << gpu_telemetry->devices.size();
    }
    if (!instance_statuses.empty()) {
      std::cout << " instance_runtimes=" << instance_statuses.size();
    }
    std::cout << "\n";
    if (!observation.status_message.empty()) {
      std::cout << "    message=" << observation.status_message << "\n";
    }
    if (runtime_status.has_value()) {
      std::cout << "    runtime aliases=";
      if (runtime_status->aliases.empty()) {
        std::cout << "(empty)";
      } else {
        for (std::size_t index = 0; index < runtime_status->aliases.size(); ++index) {
          if (index > 0) {
            std::cout << ",";
          }
          std::cout << runtime_status->aliases[index];
        }
      }
      std::cout << " runtime_profile="
                << (runtime_status->active_runtime_profile.empty()
                        ? "(empty)"
                        : runtime_status->active_runtime_profile)
                << " inference_ready=" << (runtime_status->inference_ready ? "yes" : "no")
                << " gateway_ready=" << (runtime_status->gateway_ready ? "yes" : "no")
                << "\n";
    }
    if (gpu_telemetry.has_value()) {
      for (const auto& device : gpu_telemetry->devices) {
        std::cout << "    gpu device=" << device.gpu_device
                  << " used_vram_mb=" << device.used_vram_mb
                  << "/" << device.total_vram_mb
                  << " free_vram_mb=" << device.free_vram_mb
                  << " util_pct=" << device.gpu_utilization_pct;
        if (!device.processes.empty()) {
          std::cout << " processes=";
          for (std::size_t index = 0; index < device.processes.size(); ++index) {
            if (index > 0) {
              std::cout << ",";
            }
            std::cout << device.processes[index].instance_name
                      << ":" << device.processes[index].pid
                      << ":" << device.processes[index].used_vram_mb << "MB";
          }
        }
        std::cout << "\n";
      }
    }
    if (!instance_statuses.empty()) {
      for (const auto& instance_status : instance_statuses) {
        std::cout << "    instance name=" << instance_status.instance_name
                  << " role=" << instance_status.instance_role
                  << " phase=" << instance_status.runtime_phase
                  << " ready=" << (instance_status.ready ? "yes" : "no")
                  << " pid=" << instance_status.runtime_pid
                  << " gpu=" << (instance_status.gpu_device.empty() ? "(empty)" : instance_status.gpu_device);
        if (!instance_status.model_path.empty()) {
          std::cout << " model_path=" << instance_status.model_path;
        }
        std::cout << "\n";
      }
    }
  }
}

void PrintHostHealth(
    const std::optional<comet::DesiredState>& desired_state,
    const std::vector<comet::HostObservation>& observations,
    const std::vector<comet::NodeAvailabilityOverride>& availability_overrides,
    const std::optional<std::string>& node_name,
    int stale_after_seconds) {
  std::map<std::string, comet::HostObservation> observation_by_node;
  for (const auto& observation : observations) {
    observation_by_node.emplace(observation.node_name, observation);
  }
  const auto availability_override_map =
      BuildAvailabilityOverrideMap(availability_overrides);

  std::vector<std::string> nodes;
  std::set<std::string> seen_nodes;
  if (desired_state.has_value()) {
    for (const auto& node : desired_state->nodes) {
      if (!node_name.has_value() || node.name == *node_name) {
        nodes.push_back(node.name);
        seen_nodes.insert(node.name);
      }
    }
  }
  for (const auto& [observed_node_name, observation] : observation_by_node) {
    (void)observation;
    if ((!node_name.has_value() || observed_node_name == *node_name) &&
        seen_nodes.find(observed_node_name) == seen_nodes.end()) {
      nodes.push_back(observed_node_name);
      seen_nodes.insert(observed_node_name);
    }
  }

  std::cout << "host-health:\n";
  if (nodes.empty()) {
    std::cout << "  (empty)\n";
    return;
  }

  int online_count = 0;
  int stale_count = 0;
  int unknown_count = 0;

  for (const auto& current_node_name : nodes) {
    const auto observation_it = observation_by_node.find(current_node_name);
    if (observation_it == observation_by_node.end()) {
      std::cout << "  - node=" << current_node_name
                << " availability="
                << comet::ToString(
                       ResolveNodeAvailability(availability_override_map, current_node_name))
                << " health=unknown status=(none)\n";
      ++unknown_count;
      continue;
    }

    const auto age_seconds = HeartbeatAgeSeconds(observation_it->second.heartbeat_at);
    const std::string health = HealthFromAge(age_seconds, stale_after_seconds);
    const auto runtime_status = ParseRuntimeStatus(observation_it->second);
    const auto gpu_telemetry = ParseGpuTelemetry(observation_it->second);
    if (health == "online") {
      ++online_count;
    } else if (health == "stale") {
      ++stale_count;
    } else {
      ++unknown_count;
    }

    std::cout << "  - node=" << current_node_name
              << " availability="
              << comet::ToString(
                     ResolveNodeAvailability(availability_override_map, current_node_name))
              << " health=" << health
              << " status=" << comet::ToString(observation_it->second.status);
    if (observation_it->second.applied_generation.has_value()) {
      std::cout << " applied_generation=" << *observation_it->second.applied_generation;
    }
    if (age_seconds.has_value()) {
      std::cout << " age_seconds=" << *age_seconds;
    }
    if (observation_it->second.last_assignment_id.has_value()) {
      std::cout << " last_assignment_id=" << *observation_it->second.last_assignment_id;
    }
    if (runtime_status.has_value()) {
      std::cout << " runtime_backend="
                << (runtime_status->runtime_backend.empty()
                        ? "(empty)"
                        : runtime_status->runtime_backend)
                << " runtime_phase="
                << (runtime_status->runtime_phase.empty()
                        ? "(empty)"
                        : runtime_status->runtime_phase)
                << " runtime_launch_ready="
                << (runtime_status->launch_ready ? "yes" : "no")
                << " runtime_model="
                << (runtime_status->active_model_id.empty()
                        ? "(empty)"
                        : runtime_status->active_model_id);
    }
    if (gpu_telemetry.has_value()) {
      std::cout << " telemetry="
                << (gpu_telemetry->degraded ? "degraded" : "ok")
                << ":" << (gpu_telemetry->source.empty() ? "unknown" : gpu_telemetry->source);
    }
    std::cout << "\n";
    if (!observation_it->second.status_message.empty()) {
      std::cout << "    message=" << observation_it->second.status_message << "\n";
    }
  }

  std::cout << "summary: online=" << online_count
            << " stale=" << stale_count
            << " unknown=" << unknown_count << "\n";
}

int PlanHostOps(
    const std::string& db_path,
    const std::string& bundle_dir,
    const std::string& artifacts_root,
    const std::optional<std::string>& node_name) {
  comet::ControllerStore store(db_path);
  store.Initialize();
  const auto current_state = store.LoadDesiredState();
  const comet::DesiredState desired_state = comet::ImportPlaneBundle(bundle_dir);
  const auto host_plans =
      FilterNodeExecutionPlans(
          comet::BuildNodeExecutionPlans(current_state, desired_state, artifacts_root),
          node_name);

  std::cout << "host-op-plan:\n";
  std::cout << "  db=" << db_path << "\n";
  std::cout << "  bundle=" << bundle_dir << "\n";
  std::cout << "  artifacts_root=" << artifacts_root << "\n";
  std::cout << comet::RenderNodeExecutionPlans(host_plans);
  return 0;
}

int InitDb(const std::string& db_path) {
  comet::ControllerStore store(db_path);
  store.Initialize();
  std::cout << "initialized db: " << db_path << "\n";
  return 0;
}

int SeedDemo(const std::string& db_path) {
  comet::ControllerStore store(db_path);
  store.Initialize();
  const comet::DesiredState desired_state = comet::BuildDemoState();
  comet::RequireSchedulingPolicy(desired_state);
  const comet::SchedulingPolicyReport scheduling_report =
      comet::EvaluateSchedulingPolicy(desired_state);
  const auto availability_overrides = store.LoadNodeAvailabilityOverrides();
  const auto observations = store.LoadHostObservations();
  const int desired_generation = store.LoadDesiredGeneration().value_or(0) + 1;
  store.ReplaceDesiredState(desired_state, desired_generation, 0);
  store.ClearSchedulerPlaneRuntime(desired_state.plane_name);
  store.ReplaceRolloutActions(desired_generation, scheduling_report.rollout_actions);
  store.ReplaceHostAssignments(
      BuildHostAssignments(
          desired_state,
          DefaultArtifactsRoot(),
          desired_generation,
          availability_overrides,
          observations,
          scheduling_report));
  std::cout << "seeded demo state into: " << db_path << "\n";
  std::cout << "desired generation: " << desired_generation << "\n";
  PrintSchedulerDecisionSummary(desired_state);
  PrintRolloutGateSummary(scheduling_report);
  PrintAssignmentDispatchSummary(
      desired_state,
      BuildAvailabilityOverrideMap(availability_overrides),
      observations,
      DefaultStaleAfterSeconds());
  return 0;
}

int ImportBundle(const std::string& db_path, const std::string& bundle_dir) {
  comet::ControllerStore store(db_path);
  store.Initialize();
  const comet::DesiredState desired_state = comet::ImportPlaneBundle(bundle_dir);
  comet::RequireSchedulingPolicy(desired_state);
  const comet::SchedulingPolicyReport scheduling_report =
      comet::EvaluateSchedulingPolicy(desired_state);
  const auto availability_overrides = store.LoadNodeAvailabilityOverrides();
  const auto observations = store.LoadHostObservations();
  const int desired_generation = store.LoadDesiredGeneration().value_or(0) + 1;
  store.ReplaceDesiredState(desired_state, desired_generation, 0);
  store.ClearSchedulerPlaneRuntime(desired_state.plane_name);
  store.ReplaceRolloutActions(desired_generation, scheduling_report.rollout_actions);
  store.ReplaceHostAssignments(
      BuildHostAssignments(
          desired_state,
          DefaultArtifactsRoot(),
          desired_generation,
          availability_overrides,
          observations,
          scheduling_report));
  std::cout << "imported bundle '" << bundle_dir << "' into: " << db_path << "\n";
  std::cout << "desired generation: " << desired_generation << "\n";
  PrintSchedulerDecisionSummary(desired_state);
  PrintRolloutGateSummary(scheduling_report);
  PrintAssignmentDispatchSummary(
      desired_state,
      BuildAvailabilityOverrideMap(availability_overrides),
      observations,
      DefaultStaleAfterSeconds());
  return 0;
}

int ApplyBundle(
    const std::string& db_path,
    const std::string& bundle_dir,
    const std::string& artifacts_root) {
  comet::ControllerStore store(db_path);
  store.Initialize();
  const auto current_state = store.LoadDesiredState();
  const comet::DesiredState desired_state = comet::ImportPlaneBundle(bundle_dir);
  comet::RequireSchedulingPolicy(desired_state);
  const auto availability_overrides = store.LoadNodeAvailabilityOverrides();
  const auto observations = store.LoadHostObservations();
  const int desired_generation = store.LoadDesiredGeneration().value_or(0) + 1;
  const comet::ReconcilePlan plan =
      comet::BuildReconcilePlan(current_state, desired_state);
  const comet::SchedulingPolicyReport scheduling_report =
      comet::EvaluateSchedulingPolicy(desired_state);
  const auto host_plans =
      comet::BuildNodeExecutionPlans(current_state, desired_state, artifacts_root);

  std::cout << "apply-plan:\n";
  std::cout << comet::RenderReconcilePlan(plan);
  std::cout << comet::RenderSchedulingPolicyReport(scheduling_report);
  PrintSchedulerDecisionSummary(desired_state);
  PrintRolloutGateSummary(scheduling_report);
  std::cout << comet::RenderNodeExecutionPlans(host_plans);

  MaterializeComposeArtifacts(desired_state, host_plans);
  MaterializeInferRuntimeArtifact(desired_state, artifacts_root);
  store.ReplaceDesiredState(desired_state, desired_generation, 0);
  store.ClearSchedulerPlaneRuntime(desired_state.plane_name);
  store.ReplaceRolloutActions(desired_generation, scheduling_report.rollout_actions);
  store.ReplaceHostAssignments(
      BuildHostAssignments(
          desired_state,
          artifacts_root,
          desired_generation,
          availability_overrides,
          observations,
          scheduling_report));
  std::cout << "applied bundle '" << bundle_dir << "' into: " << db_path << "\n";
  std::cout << "desired generation: " << desired_generation << "\n";
  std::cout << "artifacts written under: " << artifacts_root << "\n";
  PrintAssignmentDispatchSummary(
      desired_state,
      BuildAvailabilityOverrideMap(availability_overrides),
      observations,
      DefaultStaleAfterSeconds());
  return 0;
}

int ShowHostAssignments(
    const std::string& db_path,
    const std::optional<std::string>& node_name) {
  comet::ControllerStore store(db_path);
  store.Initialize();

  std::cout << "db: " << db_path << "\n";
  PrintHostAssignments(store.LoadHostAssignments(node_name));
  return 0;
}

int ShowHostObservations(
    const std::string& db_path,
    const std::optional<std::string>& node_name,
    int stale_after_seconds) {
  comet::ControllerStore store(db_path);
  store.Initialize();

  std::cout << "db: " << db_path << "\n";
  std::cout << "stale_after_seconds: " << stale_after_seconds << "\n";
  PrintHostObservations(store.LoadHostObservations(node_name), stale_after_seconds);
  return 0;
}

int ShowNodeAvailability(
    const std::string& db_path,
    const std::optional<std::string>& node_name) {
  comet::ControllerStore store(db_path);
  store.Initialize();

  std::cout << "db: " << db_path << "\n";
  PrintNodeAvailabilityOverrides(store.LoadNodeAvailabilityOverrides(node_name));
  return 0;
}

int ShowHostHealth(
    const std::string& db_path,
    const std::optional<std::string>& node_name,
    int stale_after_seconds) {
  comet::ControllerStore store(db_path);
  store.Initialize();

  std::cout << "db: " << db_path << "\n";
  std::cout << "stale_after_seconds: " << stale_after_seconds << "\n";
  PrintHostHealth(
      store.LoadDesiredState(),
      store.LoadHostObservations(node_name),
      store.LoadNodeAvailabilityOverrides(node_name),
      node_name,
      stale_after_seconds);
  return 0;
}

int ShowRolloutActions(
    const std::string& db_path,
    const std::optional<std::string>& node_name) {
  comet::ControllerStore store(db_path);
  store.Initialize();

  std::cout << "db: " << db_path << "\n";
  if (const auto generation = store.LoadDesiredGeneration(); generation.has_value()) {
    std::cout << "desired generation: " << *generation << "\n";
  }
  const auto actions = store.LoadRolloutActions(node_name);
  std::set<std::string> worker_names;
  std::set<std::string> node_names;
  for (const auto& action : actions) {
    worker_names.insert(action.worker_name);
    node_names.insert(action.target_node_name);
  }
  if (!actions.empty()) {
    std::cout << "rollout-gates:\n";
    std::cout << "  gated_workers=" << worker_names.size()
              << " gated_nodes=" << node_names.size()
              << " deferred_actions=" << actions.size() << "\n";
  }
  PrintPersistedRolloutActions(actions);
  if (const auto state = store.LoadDesiredState(); state.has_value()) {
    PrintSchedulerRuntimeView(LoadSchedulerRuntimeView(store, state));
    if (const auto generation = store.LoadDesiredGeneration(); generation.has_value()) {
      PrintRolloutLifecycleEntries(
          BuildRolloutLifecycleEntries(
              *state,
              *generation,
              actions,
              store.LoadHostAssignments(),
              store.LoadHostObservations()));
    }
  }
  return 0;
}

int ShowRebalancePlan(
    const std::string& db_path,
    const std::optional<std::string>& node_name) {
  comet::ControllerStore store(db_path);
  store.Initialize();

  const auto state = store.LoadDesiredState();
  if (!state.has_value()) {
    std::cout << "rebalance-plan:\n  (empty)\n";
    return 0;
  }

  const auto observations = store.LoadHostObservations();
  const auto assignments = store.LoadHostAssignments();
  const auto scheduling_report = comet::EvaluateSchedulingPolicy(*state);
  const auto scheduler_runtime = LoadSchedulerRuntimeView(store, state);
  const auto rollout_lifecycle =
      BuildRolloutLifecycleEntries(
          *state,
          store.LoadDesiredGeneration().value_or(0),
          store.LoadRolloutActions(),
          assignments,
          observations);

  std::cout << "db: " << db_path << "\n";
  if (const auto generation = store.LoadDesiredGeneration(); generation.has_value()) {
    std::cout << "desired generation: " << *generation << "\n";
  }
  const auto rebalance_entries =
      BuildRebalancePlanEntries(
          *state,
          scheduling_report,
          store.LoadNodeAvailabilityOverrides(),
          rollout_lifecycle,
          assignments,
          scheduler_runtime,
          observations,
          DefaultStaleAfterSeconds(),
          node_name);
  const auto controller_gate_summary =
      BuildRebalanceControllerGateSummary(
          *state,
          store.LoadDesiredGeneration().value_or(0),
          store.LoadNodeAvailabilityOverrides(),
          rollout_lifecycle,
          assignments,
          scheduler_runtime,
          observations,
          DefaultStaleAfterSeconds());
  const auto iteration_budget_summary =
      BuildRebalanceIterationBudgetSummary(store.LoadRebalanceIteration().value_or(0));
  const auto rebalance_policy_summary =
      BuildRebalancePolicySummary(rebalance_entries);
  PrintRebalanceControllerGateSummary(controller_gate_summary);
  PrintRebalanceIterationBudgetSummary(iteration_budget_summary);
  PrintRebalanceLoopStatusSummary(
      BuildRebalanceLoopStatusSummary(
          controller_gate_summary,
          iteration_budget_summary,
          rebalance_policy_summary));
  PrintRebalancePlanEntries(rebalance_entries);
  PrintRebalancePolicySummary(rebalance_policy_summary);
  PrintSchedulerRuntimeView(scheduler_runtime);
  return 0;
}

int SetRolloutActionStatus(
    const std::string& db_path,
    int action_id,
    comet::RolloutActionStatus status,
    const std::optional<std::string>& status_message) {
  comet::ControllerStore store(db_path);
  store.Initialize();
  if (!store.UpdateRolloutActionStatus(action_id, status, status_message.value_or(""))) {
    throw std::runtime_error(
        "rollout action id=" + std::to_string(action_id) + " not found");
  }
  std::cout << "updated rollout action id=" << action_id
            << " status=" << comet::ToString(status) << "\n";
  PrintPersistedRolloutActions(store.LoadRolloutActions());
  return 0;
}

int EnqueueRolloutEviction(
    const std::string& db_path,
    int action_id) {
  comet::ControllerStore store(db_path);
  store.Initialize();

  const auto desired_state = store.LoadDesiredState();
  const auto desired_generation = store.LoadDesiredGeneration();
  if (!desired_state.has_value() || !desired_generation.has_value()) {
    throw std::runtime_error("no desired state found in controller db");
  }

  const auto rollout_actions = store.LoadRolloutActions();
  const auto action = FindRolloutActionById(rollout_actions, action_id);
  if (!action.has_value()) {
    throw std::runtime_error(
        "rollout action id=" + std::to_string(action_id) + " not found");
  }
  if (action->desired_generation != *desired_generation) {
    throw std::runtime_error(
        "rollout action id=" + std::to_string(action_id) +
        " does not belong to current desired generation " +
        std::to_string(*desired_generation));
  }
  if (action->action != "evict-best-effort") {
    throw std::runtime_error(
        "rollout action id=" + std::to_string(action_id) +
        " is not an evict-best-effort action");
  }
  if (action->status != comet::RolloutActionStatus::Pending &&
      action->status != comet::RolloutActionStatus::Acknowledged) {
    throw std::runtime_error(
        "rollout action id=" + std::to_string(action_id) +
        " cannot enqueue eviction from status=" +
        comet::ToString(action->status));
  }

  const auto existing_assignments = store.LoadHostAssignments();
  const auto eviction_assignments = BuildEvictionAssignmentsForAction(
      *desired_state,
      *desired_generation,
      *action,
      existing_assignments);
  store.EnqueueHostAssignments(
      eviction_assignments,
      "superseded by rollout eviction action id=" + std::to_string(action_id));

  std::set<std::string> node_names;
  for (const auto& assignment : eviction_assignments) {
    node_names.insert(assignment.node_name);
  }
  std::ostringstream message;
  message << "eviction assignments enqueued on nodes ";
  bool first = true;
  for (const auto& node_name : node_names) {
    if (!first) {
      message << ",";
    }
    first = false;
    message << node_name;
  }
  store.UpdateRolloutActionStatus(
      action_id,
      comet::RolloutActionStatus::Acknowledged,
      message.str());

  std::cout << "enqueued rollout eviction action id=" << action_id << "\n";
  PrintPersistedRolloutActions(store.LoadRolloutActions());
  for (const auto& node_name : node_names) {
    PrintHostAssignments(store.LoadHostAssignments(node_name));
  }
  return 0;
}

int ApplyReadyRolloutAction(
    const std::string& db_path,
    int action_id,
    const std::string& artifacts_root) {
  comet::ControllerStore store(db_path);
  store.Initialize();

  const auto desired_state = store.LoadDesiredState();
  const auto desired_generation = store.LoadDesiredGeneration();
  if (!desired_state.has_value() || !desired_generation.has_value()) {
    throw std::runtime_error("no desired state found in controller db");
  }

  const auto rollout_actions = store.LoadRolloutActions();
  const auto action = FindRolloutActionById(rollout_actions, action_id);
  if (!action.has_value()) {
    throw std::runtime_error(
        "rollout action id=" + std::to_string(action_id) + " not found");
  }
  if (action->status != comet::RolloutActionStatus::ReadyToRetry) {
    throw std::runtime_error(
        "rollout action id=" + std::to_string(action_id) +
        " is not ready-to-retry; current status=" +
        comet::ToString(action->status));
  }
  if (action->action != "retry-placement") {
    throw std::runtime_error(
        "rollout action id=" + std::to_string(action_id) +
        " is not a retry-placement action");
  }

  std::vector<std::string> victim_worker_names;
  for (const auto& candidate_action : rollout_actions) {
    if (candidate_action.desired_generation != action->desired_generation ||
        candidate_action.worker_name != action->worker_name ||
        candidate_action.step >= action->step) {
      continue;
    }
    if (candidate_action.status != comet::RolloutActionStatus::ReadyToRetry) {
      throw std::runtime_error(
          "prior rollout step id=" + std::to_string(candidate_action.id) +
          " is not ready-to-retry");
    }
    if (candidate_action.action == "evict-best-effort") {
      victim_worker_names = candidate_action.victim_worker_names;
    }
  }

  comet::DesiredState updated_state = *desired_state;
  MaterializeRetryPlacementAction(&updated_state, *action, victim_worker_names);
  comet::RequireSchedulingPolicy(updated_state);
  const comet::SchedulingPolicyReport scheduling_report =
      comet::EvaluateSchedulingPolicy(updated_state);
  const int next_generation = *desired_generation + 1;
  const auto availability_overrides = store.LoadNodeAvailabilityOverrides();
  const auto observations = store.LoadHostObservations();

  store.ReplaceDesiredState(updated_state, next_generation, 0);
  store.ClearSchedulerPlaneRuntime(updated_state.plane_name);
  store.ReplaceRolloutActions(next_generation, scheduling_report.rollout_actions);
  store.ReplaceHostAssignments(
      BuildHostAssignments(
          updated_state,
          artifacts_root,
          next_generation,
          availability_overrides,
          observations,
          scheduling_report));

  std::cout << "applied ready rollout action id=" << action_id << "\n";
  std::cout << "desired generation: " << next_generation << "\n";
  PrintStateSummary(updated_state);
  std::cout << comet::RenderSchedulingPolicyReport(scheduling_report);
  PrintSchedulerDecisionSummary(updated_state);
  PrintRolloutGateSummary(scheduling_report);
  return 0;
}

int ApplyRebalanceProposal(
    const std::string& db_path,
    const std::string& worker_name,
    const std::string& artifacts_root) {
  comet::ControllerStore store(db_path);
  store.Initialize();

  const auto desired_state = store.LoadDesiredState();
  const auto desired_generation = store.LoadDesiredGeneration();
  const auto rebalance_iteration = store.LoadRebalanceIteration();
  if (!desired_state.has_value() || !desired_generation.has_value()) {
    throw std::runtime_error("no desired state found in controller db");
  }

  const auto assignments = store.LoadHostAssignments();
  const auto observations = store.LoadHostObservations();
  const auto scheduling_report = comet::EvaluateSchedulingPolicy(*desired_state);
  const auto scheduler_runtime = LoadSchedulerRuntimeView(store, desired_state);
  const auto rollout_lifecycle =
      BuildRolloutLifecycleEntries(
          *desired_state,
          *desired_generation,
          store.LoadRolloutActions(),
          assignments,
          observations);
  const auto rebalance_entries =
      BuildRebalancePlanEntries(
          *desired_state,
          scheduling_report,
          store.LoadNodeAvailabilityOverrides(),
          rollout_lifecycle,
          assignments,
          scheduler_runtime,
          observations,
          DefaultStaleAfterSeconds());

  const auto rebalance_it = std::find_if(
      rebalance_entries.begin(),
      rebalance_entries.end(),
      [&](const RebalancePlanEntry& entry) { return entry.worker_name == worker_name; });
  if (rebalance_it == rebalance_entries.end()) {
    throw std::runtime_error(
        "no rebalance plan entry found for worker '" + worker_name + "'");
  }
  if (rebalance_it->decision != "propose") {
    throw std::runtime_error(
        "worker '" + worker_name + "' is not actionable for rebalance; current decision=" +
        rebalance_it->decision + " state=" + rebalance_it->state);
  }
  const auto iteration_budget_summary =
      BuildRebalanceIterationBudgetSummary(rebalance_iteration.value_or(0));
  if (iteration_budget_summary.exhausted) {
    throw std::runtime_error(
        "rebalance iteration budget exhausted (" +
        std::to_string(iteration_budget_summary.current_iteration) + "/" +
        std::to_string(iteration_budget_summary.max_iterations) +
        "); apply a fresh bundle or rollout generation before materializing another direct rebalance");
  }

  comet::DesiredState updated_state = *desired_state;
  MaterializeRebalancePlanEntry(&updated_state, *rebalance_it);
  comet::RequireSchedulingPolicy(updated_state);
  const comet::SchedulingPolicyReport updated_scheduling_report =
      comet::EvaluateSchedulingPolicy(updated_state);
  const int next_generation = *desired_generation + 1;
  const auto availability_overrides = store.LoadNodeAvailabilityOverrides();
  const auto host_plans =
      comet::BuildNodeExecutionPlans(desired_state, updated_state, artifacts_root);

  MaterializeComposeArtifacts(updated_state, host_plans);
  MaterializeInferRuntimeArtifact(updated_state, artifacts_root);
  store.ReplaceDesiredState(
      updated_state,
      next_generation,
      rebalance_iteration.value_or(0) + 1);
  store.ReplaceRolloutActions(next_generation, updated_scheduling_report.rollout_actions);
  store.ReplaceHostAssignments(
      BuildHostAssignments(
          updated_state,
          artifacts_root,
          next_generation,
          availability_overrides,
          observations,
          updated_scheduling_report));
  comet::SchedulerPlaneRuntime plane_runtime;
  plane_runtime.plane_name = updated_state.plane_name;
  plane_runtime.active_action = "rebalance";
  plane_runtime.active_worker_name = rebalance_it->worker_name;
  plane_runtime.phase = "verifying-move";
  plane_runtime.action_generation = next_generation;
  plane_runtime.stable_samples = 0;
  plane_runtime.rollback_attempt_count = 0;
  plane_runtime.source_node_name = rebalance_it->current_node_name;
  plane_runtime.source_gpu_device = rebalance_it->current_gpu_device;
  plane_runtime.target_node_name = rebalance_it->target_node_name;
  plane_runtime.target_gpu_device = rebalance_it->target_gpu_device;
  plane_runtime.previous_state_json = comet::SerializeDesiredStateJson(*desired_state);
  plane_runtime.status_message = "awaiting post-move verification";
  store.UpsertSchedulerPlaneRuntime(plane_runtime);

  std::cout << "applied rebalance proposal for worker '" << worker_name << "'\n";
  std::cout << "desired generation: " << next_generation << "\n";
  std::cout << "target=" << rebalance_it->target_node_name << ":"
            << rebalance_it->target_gpu_device << "\n";
  PrintStateSummary(updated_state);
  std::cout << comet::RenderSchedulingPolicyReport(updated_scheduling_report);
  PrintSchedulerDecisionSummary(updated_state);
  PrintRolloutGateSummary(updated_scheduling_report);
  PrintAssignmentDispatchSummary(
      updated_state,
      BuildAvailabilityOverrideMap(availability_overrides),
      observations,
      DefaultStaleAfterSeconds());
  return 0;
}

int ReconcileRebalanceProposals(
    const std::string& db_path,
    const std::string& artifacts_root) {
  comet::ControllerStore store(db_path);
  store.Initialize();

  const auto desired_state = store.LoadDesiredState();
  const auto desired_generation = store.LoadDesiredGeneration();
  const auto rebalance_iteration = store.LoadRebalanceIteration();
  if (!desired_state.has_value() || !desired_generation.has_value()) {
    throw std::runtime_error("no desired state found in controller db");
  }

  const auto assignments = store.LoadHostAssignments();
  const auto observations = store.LoadHostObservations();
  const auto scheduling_report = comet::EvaluateSchedulingPolicy(*desired_state);
  const auto scheduler_runtime = LoadSchedulerRuntimeView(store, desired_state);
  const auto rollout_lifecycle =
      BuildRolloutLifecycleEntries(
          *desired_state,
          *desired_generation,
          store.LoadRolloutActions(),
          assignments,
          observations);
  auto rebalance_entries =
      BuildRebalancePlanEntries(
          *desired_state,
          scheduling_report,
          store.LoadNodeAvailabilityOverrides(),
          rollout_lifecycle,
          assignments,
          scheduler_runtime,
          observations,
          DefaultStaleAfterSeconds());
  const auto controller_gate_summary =
      BuildRebalanceControllerGateSummary(
          *desired_state,
          *desired_generation,
          store.LoadNodeAvailabilityOverrides(),
          rollout_lifecycle,
          assignments,
          scheduler_runtime,
          observations,
          DefaultStaleAfterSeconds());
  const auto iteration_budget_summary =
      BuildRebalanceIterationBudgetSummary(rebalance_iteration.value_or(0));
  const auto rebalance_policy_summary =
      BuildRebalancePolicySummary(rebalance_entries);
  PrintRebalanceControllerGateSummary(controller_gate_summary);
  PrintRebalanceIterationBudgetSummary(iteration_budget_summary);
  PrintRebalanceLoopStatusSummary(
      BuildRebalanceLoopStatusSummary(
          controller_gate_summary,
          iteration_budget_summary,
          rebalance_policy_summary));

  if (!controller_gate_summary.cluster_ready) {
    std::cout << "rebalance proposals: blocked by controller gate\n";
    return 0;
  }

  rebalance_entries.erase(
      std::remove_if(
          rebalance_entries.begin(),
          rebalance_entries.end(),
          [](const RebalancePlanEntry& entry) {
            return entry.decision != "propose" || entry.rebalance_class != "safe-direct";
          }),
      rebalance_entries.end());

  if (rebalance_entries.empty()) {
    std::cout << "rebalance proposals: none actionable\n";
    return 0;
  }
  if (iteration_budget_summary.exhausted) {
    std::cout << "rebalance proposals: blocked by iteration budget\n";
    return 0;
  }

  std::sort(
      rebalance_entries.begin(),
      rebalance_entries.end(),
      [](const RebalancePlanEntry& left, const RebalancePlanEntry& right) {
        if (left.score != right.score) {
          return left.score > right.score;
        }
        return left.worker_name < right.worker_name;
      });

  std::cout << "selected rebalance proposal: worker=" << rebalance_entries.front().worker_name
            << " target=" << rebalance_entries.front().target_node_name << ":"
            << rebalance_entries.front().target_gpu_device
            << " score=" << rebalance_entries.front().score << "\n";
  return ApplyRebalanceProposal(
      db_path, rebalance_entries.front().worker_name, artifacts_root);
}

int AdvanceActiveSchedulerAction(
    const std::string& db_path,
    const std::string& artifacts_root) {
  comet::ControllerStore store(db_path);
  store.Initialize();

  const auto desired_state = store.LoadDesiredState();
  const auto desired_generation = store.LoadDesiredGeneration();
  if (!desired_state.has_value() || !desired_generation.has_value()) {
    std::cout << "scheduler active-action: no desired state\n";
    return 0;
  }

  const auto plane_runtime = store.LoadSchedulerPlaneRuntime(desired_state->plane_name);
  if (!plane_runtime.has_value() || plane_runtime->active_action.empty()) {
    std::cout << "scheduler active-action: none\n";
    return 0;
  }

  if (plane_runtime->phase == "rollback-planned") {
    if (plane_runtime->previous_state_json.empty()) {
      throw std::runtime_error(
          "rollback-planned action has no previous desired state payload");
    }
    const comet::DesiredState rollback_state =
        comet::DeserializeDesiredStateJson(plane_runtime->previous_state_json);
    comet::RequireSchedulingPolicy(rollback_state);
    const auto availability_overrides = store.LoadNodeAvailabilityOverrides();
    const auto observations = store.LoadHostObservations();
    const auto rollback_report = comet::EvaluateSchedulingPolicy(rollback_state);
    const int rollback_generation = *desired_generation + 1;
    store.ReplaceDesiredState(rollback_state, rollback_generation, 0);
    store.ReplaceRolloutActions(rollback_generation, rollback_report.rollout_actions);
    store.ReplaceHostAssignments(
        BuildHostAssignments(
            rollback_state,
            artifacts_root,
            rollback_generation,
            availability_overrides,
            observations,
            rollback_report));
    comet::SchedulerPlaneRuntime updated_runtime = *plane_runtime;
    updated_runtime.phase = "rollback-applied";
    updated_runtime.action_generation = rollback_generation;
    updated_runtime.stable_samples = 0;
    updated_runtime.rollback_attempt_count = 1;
    updated_runtime.started_at = UtcNowSqlTimestamp();
    updated_runtime.status_message = "rollback materialized after verification timeout";
    store.UpsertSchedulerPlaneRuntime(updated_runtime);
    std::cout << "scheduler active-action: rollback-applied worker="
              << updated_runtime.active_worker_name
              << " generation=" << rollback_generation << "\n";
    return 0;
  }

  const auto observations = store.LoadHostObservations();
  const auto verification = EvaluateSchedulerActionVerification(*plane_runtime, observations);
  comet::SchedulerPlaneRuntime updated_runtime = *plane_runtime;
  updated_runtime.stable_samples = verification.next_stable_samples;
  updated_runtime.status_message = verification.detail;

  if (verification.stable) {
    MarkWorkerMoveVerified(&store, updated_runtime);
    store.ClearSchedulerPlaneRuntime(updated_runtime.plane_name);
    std::cout << "scheduler active-action: verified worker="
              << updated_runtime.active_worker_name
              << " phase=" << updated_runtime.phase << "\n";
    return 0;
  }

  if (!verification.timed_out) {
    store.UpsertSchedulerPlaneRuntime(updated_runtime);
    std::cout << "scheduler active-action: waiting worker="
              << updated_runtime.active_worker_name
              << " phase=" << updated_runtime.phase
              << " stable_samples=" << updated_runtime.stable_samples << "/"
              << VerificationStableSamplesRequired()
              << " detail=" << verification.detail << "\n";
    return 0;
  }

  if (updated_runtime.rollback_attempt_count == 0 &&
      !updated_runtime.previous_state_json.empty()) {
    comet::SchedulerWorkerRuntime worker_runtime;
    if (const auto current = store.LoadSchedulerWorkerRuntime(updated_runtime.active_worker_name);
        current.has_value()) {
      worker_runtime = *current;
    }
    worker_runtime.plane_name = updated_runtime.plane_name;
    worker_runtime.worker_name = updated_runtime.active_worker_name;
    worker_runtime.last_scheduler_phase = "failed-verification";
    worker_runtime.last_status_message = verification.detail;
    worker_runtime.manual_intervention_required = false;
    store.UpsertSchedulerWorkerRuntime(worker_runtime);
    updated_runtime.phase = "rollback-planned";
    updated_runtime.stable_samples = 0;
    updated_runtime.started_at = UtcNowSqlTimestamp();
    updated_runtime.status_message = "verification timed out; rollback planned";
    store.UpsertSchedulerPlaneRuntime(updated_runtime);
    std::cout << "scheduler active-action: rollback-planned worker="
              << updated_runtime.active_worker_name
              << " generation=" << updated_runtime.action_generation << "\n";
    return 0;
  }

  comet::SchedulerWorkerRuntime worker_runtime;
  if (const auto current = store.LoadSchedulerWorkerRuntime(updated_runtime.active_worker_name);
      current.has_value()) {
    worker_runtime = *current;
  }
  worker_runtime.plane_name = updated_runtime.plane_name;
  worker_runtime.worker_name = updated_runtime.active_worker_name;
  worker_runtime.last_scheduler_phase = "manual-intervention-required";
  worker_runtime.last_status_message = verification.detail;
  worker_runtime.manual_intervention_required = true;
  store.UpsertSchedulerWorkerRuntime(worker_runtime);
  store.ClearSchedulerPlaneRuntime(updated_runtime.plane_name);
  std::cout << "scheduler active-action: manual-intervention-required worker="
            << updated_runtime.active_worker_name
            << " detail=" << verification.detail << "\n";
  return 0;
}

int SchedulerTick(
    const std::string& db_path,
    const std::string& artifacts_root) {
  comet::ControllerStore store(db_path);
  store.Initialize();

  const auto desired_state = store.LoadDesiredState();
  const auto desired_generation = store.LoadDesiredGeneration();
  if (!desired_state.has_value() || !desired_generation.has_value()) {
    std::cout << "scheduler-tick: no desired state\n";
    return 0;
  }

  if (const auto plane_runtime = store.LoadSchedulerPlaneRuntime(desired_state->plane_name);
      plane_runtime.has_value() && !plane_runtime->active_action.empty()) {
    std::cout << "scheduler-tick: step=active-scheduler-action\n";
    return AdvanceActiveSchedulerAction(db_path, artifacts_root);
  }

  const auto rollout_actions = store.LoadRolloutActions();
  bool has_active_rollout = false;
  for (const auto& action : rollout_actions) {
    if (action.desired_generation == *desired_generation &&
        action.status != comet::RolloutActionStatus::ReadyToRetry) {
      has_active_rollout = true;
      break;
    }
  }
  if (!rollout_actions.empty()) {
    std::cout << "scheduler-tick: step=rollout-reconcile\n";
    return ReconcileRolloutActions(db_path, artifacts_root);
  }

  std::cout << "scheduler-tick: step=rebalance-reconcile\n";
  if (has_active_rollout) {
    std::cout << "scheduler-tick: rollout still active\n";
    return 0;
  }
  return ReconcileRebalanceProposals(db_path, artifacts_root);
}

int ReconcileRolloutActions(
    const std::string& db_path,
    const std::string& artifacts_root) {
  comet::ControllerStore store(db_path);
  store.Initialize();

  const auto desired_state = store.LoadDesiredState();
  const auto desired_generation = store.LoadDesiredGeneration();
  if (!desired_state.has_value() || !desired_generation.has_value()) {
    throw std::runtime_error("no desired state found in controller db");
  }

  const auto all_rollout_actions = store.LoadRolloutActions();
  std::vector<comet::RolloutActionRecord> rollout_actions;
  for (const auto& action : all_rollout_actions) {
    if (action.desired_generation == *desired_generation) {
      rollout_actions.push_back(action);
    }
  }

  std::cout << "db: " << db_path << "\n";
  std::cout << "desired generation: " << *desired_generation << "\n";
  if (rollout_actions.empty()) {
    std::cout << "rollout reconcile: no rollout actions for current generation\n";
    return 0;
  }

  bool changed = false;
  for (const auto& action : rollout_actions) {
    if (action.action == "evict-best-effort") {
      if (action.status == comet::RolloutActionStatus::Pending) {
        const auto existing_assignments = store.LoadHostAssignments();
        const auto eviction_assignments = BuildEvictionAssignmentsForAction(
            *desired_state,
            *desired_generation,
            action,
            existing_assignments);
        store.EnqueueHostAssignments(
            eviction_assignments,
            "superseded by rollout eviction action id=" + std::to_string(action.id));
        store.UpdateRolloutActionStatus(
            action.id,
            comet::RolloutActionStatus::Acknowledged,
            "controller-managed eviction assignments enqueued");
        std::cout << "rollout reconcile: enqueued eviction action id=" << action.id << "\n";
        changed = true;
        continue;
      }

      if (action.status == comet::RolloutActionStatus::Acknowledged &&
          AreRolloutEvictionAssignmentsApplied(store.LoadHostAssignments(), action.id)) {
        store.UpdateRolloutActionStatus(
            action.id,
            comet::RolloutActionStatus::ReadyToRetry,
            "eviction assignments applied");
        MarkWorkersEvicted(&store, desired_state->plane_name, action.victim_worker_names);
        std::cout << "rollout reconcile: eviction action id=" << action.id
                  << " is ready-to-retry\n";
        changed = true;
      }
      continue;
    }

    if (action.action != "retry-placement") {
      continue;
    }

    auto current_action = FindRolloutActionById(store.LoadRolloutActions(), action.id);
    if (!current_action.has_value()) {
      continue;
    }

    const auto prior_evict_action = FindPriorRolloutActionForWorker(
        store.LoadRolloutActions(),
        *current_action,
        "evict-best-effort");
    if (current_action->status == comet::RolloutActionStatus::Pending &&
        prior_evict_action.has_value() &&
        prior_evict_action->status == comet::RolloutActionStatus::ReadyToRetry) {
      store.UpdateRolloutActionStatus(
          current_action->id,
          comet::RolloutActionStatus::ReadyToRetry,
          "preceding eviction completed");
      std::cout << "rollout reconcile: retry action id=" << current_action->id
                << " is ready-to-retry\n";
      changed = true;
      current_action = FindRolloutActionById(store.LoadRolloutActions(), action.id);
    }

    if (current_action.has_value() &&
        current_action->status == comet::RolloutActionStatus::ReadyToRetry) {
      std::cout << "rollout reconcile: materializing retry action id="
                << current_action->id << "\n";
      return ApplyReadyRolloutAction(db_path, current_action->id, artifacts_root);
    }
  }

  if (!changed) {
    std::cout << "rollout reconcile: no state changes\n";
  }
  PrintPersistedRolloutActions(store.LoadRolloutActions());
  if (const auto state = store.LoadDesiredState(); state.has_value()) {
    if (const auto generation = store.LoadDesiredGeneration(); generation.has_value()) {
      PrintRolloutLifecycleEntries(
          BuildRolloutLifecycleEntries(
              *state,
              *generation,
              store.LoadRolloutActions(),
              store.LoadHostAssignments(),
              store.LoadHostObservations()));
    }
  }
  return 0;
}

int SetNodeAvailability(
    const std::string& db_path,
    const std::string& node_name,
    comet::NodeAvailability availability,
    const std::optional<std::string>& status_message) {
  comet::ControllerStore store(db_path);
  store.Initialize();
  const auto previous_override = store.LoadNodeAvailabilityOverride(node_name);
  const auto previous_availability =
      previous_override.has_value() ? previous_override->availability
                                    : comet::NodeAvailability::Active;

  comet::NodeAvailabilityOverride availability_override;
  availability_override.node_name = node_name;
  availability_override.availability = availability;
  availability_override.status_message = status_message.value_or("");
  store.UpsertNodeAvailabilityOverride(availability_override);

  std::cout << "updated node availability for " << node_name << "\n";
  PrintNodeAvailabilityOverrides(store.LoadNodeAvailabilityOverrides(node_name));

  const auto desired_state = store.LoadDesiredState();
  const auto desired_generation = store.LoadDesiredGeneration();
  if (desired_state.has_value() && desired_generation.has_value()) {
    if (previous_availability == comet::NodeAvailability::Active &&
        availability != comet::NodeAvailability::Active) {
      const auto drain_assignment = BuildDrainAssignmentForNode(
          *desired_state,
          *desired_generation,
          node_name,
          store.LoadHostAssignments());
      if (drain_assignment.has_value()) {
        store.EnqueueHostAssignments(
            {*drain_assignment},
            "superseded by node drain for desired generation " +
                std::to_string(*desired_generation));
        std::cout << "queued drain assignment for " << node_name
                  << " at desired generation " << *desired_generation << "\n";
        PrintHostAssignments(store.LoadHostAssignments(node_name));
      }
    }

    if (previous_availability != comet::NodeAvailability::Active &&
        availability == comet::NodeAvailability::Active) {
      const auto resync_assignment = BuildResyncAssignmentForNode(
          *desired_state,
          *desired_generation,
          node_name,
          store.LoadHostAssignments(),
          store.LoadHostObservation(node_name));
      if (resync_assignment.has_value()) {
        store.EnqueueHostAssignments(
            {*resync_assignment},
            "superseded by node reactivation for desired generation " +
                std::to_string(*desired_generation));
        std::cout << "queued resync assignment for " << node_name
                  << " at desired generation " << *desired_generation << "\n";
        PrintHostAssignments(store.LoadHostAssignments(node_name));
      } else {
        std::cout << "no resync assignment needed for " << node_name << "\n";
      }
    }
  }
  return 0;
}

int RetryHostAssignment(const std::string& db_path, int assignment_id) {
  comet::ControllerStore store(db_path);
  store.Initialize();

  const auto assignment = store.LoadHostAssignment(assignment_id);
  if (!assignment.has_value()) {
    throw std::runtime_error(
        "host assignment id=" + std::to_string(assignment_id) + " not found");
  }
  if (assignment->status != comet::HostAssignmentStatus::Failed) {
    throw std::runtime_error(
        "host assignment id=" + std::to_string(assignment_id) +
        " is not failed; current status=" + comet::ToString(assignment->status));
  }

  const auto latest_generation = store.LoadDesiredGeneration();
  if (latest_generation.has_value() &&
      assignment->desired_generation != *latest_generation) {
    throw std::runtime_error(
        "host assignment id=" + std::to_string(assignment_id) +
        " belongs to stale desired generation " +
        std::to_string(assignment->desired_generation) +
        "; latest generation is " + std::to_string(*latest_generation));
  }

  if (!store.RetryFailedHostAssignment(
          assignment_id,
          "requeued by operator for desired generation " +
              std::to_string(assignment->desired_generation))) {
    throw std::runtime_error(
        "failed to requeue host assignment id=" + std::to_string(assignment_id));
  }

  const auto updated_assignment = store.LoadHostAssignment(assignment_id);
  std::cout << "requeued host assignment id=" << assignment_id << "\n";
  if (updated_assignment.has_value()) {
    PrintHostAssignments({*updated_assignment});
  }
  return 0;
}

int ShowState(const std::string& db_path) {
  comet::ControllerStore store(db_path);
  store.Initialize();
  const auto state = store.LoadDesiredState();
  if (!state.has_value()) {
    std::cout << "state: empty\n";
    return 0;
  }

  std::cout << "db: " << db_path << "\n";
  const auto generation = store.LoadDesiredGeneration();
  if (generation.has_value()) {
    std::cout << "desired generation: " << *generation << "\n";
  }
  PrintStateSummary(*state);
  const comet::SchedulingPolicyReport scheduling_report =
      comet::EvaluateSchedulingPolicy(*state);
  const auto observations = store.LoadHostObservations();
  const auto assignments = store.LoadHostAssignments();
  const auto scheduler_runtime = LoadSchedulerRuntimeView(store, state);
  const auto rollout_lifecycle =
      generation.has_value()
          ? BuildRolloutLifecycleEntries(
                *state,
                *generation,
                store.LoadRolloutActions(),
                assignments,
                observations)
          : std::vector<RolloutLifecycleEntry>{};
  std::cout << comet::RenderSchedulingPolicyReport(scheduling_report);
  PrintSchedulerDecisionSummary(*state);
  PrintRolloutGateSummary(scheduling_report);
  const auto rebalance_entries =
      BuildRebalancePlanEntries(
          *state,
          scheduling_report,
          store.LoadNodeAvailabilityOverrides(),
          rollout_lifecycle,
          assignments,
          scheduler_runtime,
          observations,
          DefaultStaleAfterSeconds());
  const auto controller_gate_summary =
      BuildRebalanceControllerGateSummary(
          *state,
          generation.value_or(0),
          store.LoadNodeAvailabilityOverrides(),
          rollout_lifecycle,
          assignments,
          scheduler_runtime,
          observations,
          DefaultStaleAfterSeconds());
  const auto iteration_budget_summary =
      BuildRebalanceIterationBudgetSummary(store.LoadRebalanceIteration().value_or(0));
  const auto rebalance_policy_summary =
      BuildRebalancePolicySummary(rebalance_entries);
  PrintRebalanceControllerGateSummary(controller_gate_summary);
  PrintRebalanceIterationBudgetSummary(iteration_budget_summary);
  PrintRebalanceLoopStatusSummary(
      BuildRebalanceLoopStatusSummary(
          controller_gate_summary,
          iteration_budget_summary,
          rebalance_policy_summary));
  PrintRebalancePlanEntries(rebalance_entries);
  PrintRebalancePolicySummary(rebalance_policy_summary);
  PrintSchedulerRuntimeView(scheduler_runtime);
  if (generation.has_value()) {
    PrintRolloutLifecycleEntries(rollout_lifecycle);
  }
  std::cout << "\n";
  PrintNodeAvailabilityOverrides(store.LoadNodeAvailabilityOverrides());
  std::cout << "\n";
  PrintHostObservations(observations, DefaultStaleAfterSeconds());
  std::cout << "\n";
  PrintHostHealth(
      state,
      observations,
      store.LoadNodeAvailabilityOverrides(),
      std::nullopt,
      DefaultStaleAfterSeconds());
  return 0;
}

int RenderCompose(const std::string& db_path, const std::optional<std::string>& node_name) {
  comet::ControllerStore store(db_path);
  store.Initialize();
  const auto state = store.LoadDesiredState();
  if (!state.has_value()) {
    std::cerr << "error: no desired state found in db '" << db_path << "'\n";
    return 1;
  }
  return RenderComposeForState(*state, node_name);
}

int RenderInferRuntime(const std::string& db_path) {
  comet::ControllerStore store(db_path);
  store.Initialize();
  const auto state = store.LoadDesiredState();
  if (!state.has_value()) {
    std::cerr << "error: no desired state found in db '" << db_path << "'\n";
    return 1;
  }
  std::cout << comet::RenderInferRuntimeConfigJson(*state) << "\n";
  return 0;
}

}  // namespace

int main(int argc, char** argv) {
  if (argc < 2) {
    PrintUsage();
    return 1;
  }

  const std::string command = argv[1];
  if (command == "show-demo-plan") {
    ShowDemoPlan();
    return 0;
  }

  if (command == "render-demo-compose") {
    return RenderDemoCompose(ParseNodeArg(argc, argv));
  }

  try {
    const std::string db_path = ResolveDbPath(ParseDbArg(argc, argv));

    if (command == "init-db") {
      return InitDb(db_path);
    }

    if (command == "seed-demo") {
      return SeedDemo(db_path);
    }

    if (command == "validate-bundle") {
      const auto bundle_dir = ParseBundleArg(argc, argv);
      if (!bundle_dir.has_value()) {
        std::cerr << "error: --bundle is required\n";
        return 1;
      }
      return ValidateBundle(*bundle_dir);
    }

    if (command == "preview-bundle") {
      const auto bundle_dir = ParseBundleArg(argc, argv);
      if (!bundle_dir.has_value()) {
        std::cerr << "error: --bundle is required\n";
        return 1;
      }
      return PreviewBundle(*bundle_dir, ParseNodeArg(argc, argv));
    }

    if (command == "plan-bundle") {
      const auto bundle_dir = ParseBundleArg(argc, argv);
      if (!bundle_dir.has_value()) {
        std::cerr << "error: --bundle is required\n";
        return 1;
      }
      return PlanBundle(db_path, *bundle_dir);
    }

    if (command == "apply-bundle") {
      const auto bundle_dir = ParseBundleArg(argc, argv);
      if (!bundle_dir.has_value()) {
        std::cerr << "error: --bundle is required\n";
        return 1;
      }
      return ApplyBundle(
          db_path,
          *bundle_dir,
          ResolveArtifactsRoot(ParseArtifactsRootArg(argc, argv)));
    }

    if (command == "plan-host-ops") {
      const auto bundle_dir = ParseBundleArg(argc, argv);
      if (!bundle_dir.has_value()) {
        std::cerr << "error: --bundle is required\n";
        return 1;
      }
      return PlanHostOps(
          db_path,
          *bundle_dir,
          ResolveArtifactsRoot(ParseArtifactsRootArg(argc, argv)),
          ParseNodeArg(argc, argv));
    }

    if (command == "show-state") {
      return ShowState(db_path);
    }

    if (command == "show-host-assignments") {
      return ShowHostAssignments(db_path, ParseNodeArg(argc, argv));
    }

    if (command == "show-host-observations") {
      return ShowHostObservations(
          db_path,
          ParseNodeArg(argc, argv),
          ParseStaleAfterArg(argc, argv).value_or(DefaultStaleAfterSeconds()));
    }

    if (command == "show-host-health") {
      return ShowHostHealth(
          db_path,
          ParseNodeArg(argc, argv),
          ParseStaleAfterArg(argc, argv).value_or(DefaultStaleAfterSeconds()));
    }

    if (command == "show-rollout-actions") {
      return ShowRolloutActions(
          db_path,
          ParseNodeArg(argc, argv));
    }

    if (command == "show-rebalance-plan") {
      return ShowRebalancePlan(
          db_path,
          ParseNodeArg(argc, argv));
    }

    if (command == "apply-rebalance-proposal") {
      const auto worker_name = ParseWorkerArg(argc, argv);
      if (!worker_name.has_value()) {
        std::cerr << "error: --worker is required\n";
        return 1;
      }
      return ApplyRebalanceProposal(
          db_path,
          *worker_name,
          ResolveArtifactsRoot(ParseArtifactsRootArg(argc, argv)));
    }

    if (command == "reconcile-rebalance-proposals") {
      return ReconcileRebalanceProposals(
          db_path,
          ResolveArtifactsRoot(ParseArtifactsRootArg(argc, argv)));
    }

    if (command == "scheduler-tick") {
      return SchedulerTick(
          db_path,
          ResolveArtifactsRoot(ParseArtifactsRootArg(argc, argv)));
    }

    if (command == "set-rollout-action-status") {
      const auto action_id = ParseIdArg(argc, argv);
      if (!action_id.has_value()) {
        std::cerr << "error: --id is required\n";
        return 1;
      }
      const auto requested_status = ParseStatusArg(argc, argv);
      if (!requested_status.has_value()) {
        std::cerr << "error: --status is required\n";
        return 1;
      }
      return SetRolloutActionStatus(
          db_path,
          *action_id,
          comet::ParseRolloutActionStatus(*requested_status),
          ParseMessageArg(argc, argv));
    }

    if (command == "enqueue-rollout-eviction") {
      const auto action_id = ParseIdArg(argc, argv);
      if (!action_id.has_value()) {
        std::cerr << "error: --id is required\n";
        return 1;
      }
      return EnqueueRolloutEviction(db_path, *action_id);
    }

    if (command == "reconcile-rollout-actions") {
      return ReconcileRolloutActions(
          db_path,
          ResolveArtifactsRoot(ParseArtifactsRootArg(argc, argv)));
    }

    if (command == "apply-ready-rollout-action") {
      const auto action_id = ParseIdArg(argc, argv);
      if (!action_id.has_value()) {
        std::cerr << "error: --id is required\n";
        return 1;
      }
      return ApplyReadyRolloutAction(
          db_path,
          *action_id,
          ResolveArtifactsRoot(ParseArtifactsRootArg(argc, argv)));
    }

    if (command == "show-node-availability") {
      return ShowNodeAvailability(db_path, ParseNodeArg(argc, argv));
    }

    if (command == "set-node-availability") {
      const auto requested_node_name = ParseNodeArg(argc, argv);
      if (!requested_node_name.has_value()) {
        std::cerr << "error: --node is required\n";
        return 1;
      }
      const auto requested_availability = ParseAvailabilityArg(argc, argv);
      if (!requested_availability.has_value()) {
        std::cerr << "error: --availability is required\n";
        return 1;
      }
      return SetNodeAvailability(
          db_path,
          *requested_node_name,
          comet::ParseNodeAvailability(*requested_availability),
          ParseMessageArg(argc, argv));
    }

    if (command == "retry-host-assignment") {
      const auto assignment_id = ParseIdArg(argc, argv);
      if (!assignment_id.has_value()) {
        std::cerr << "error: --id is required\n";
        return 1;
      }
      return RetryHostAssignment(db_path, *assignment_id);
    }

    if (command == "import-bundle") {
      const auto bundle_dir = ParseBundleArg(argc, argv);
      if (!bundle_dir.has_value()) {
        std::cerr << "error: --bundle is required\n";
        return 1;
      }
      return ImportBundle(db_path, *bundle_dir);
    }

    if (command == "render-compose") {
      return RenderCompose(db_path, ParseNodeArg(argc, argv));
    }

    if (command == "render-infer-runtime") {
      return RenderInferRuntime(db_path);
    }
  } catch (const std::exception& error) {
    std::cerr << "error: " << error.what() << "\n";
    return 1;
  }

  PrintUsage();
  return 1;
}
