#include "app/hostd_desired_state_display_support.h"

#include <filesystem>
#include <iomanip>
#include <iostream>
#include <optional>
#include <sstream>
#include <stdexcept>
#include <utility>

#include "naim/planning/execution_plan.h"
#include "naim/runtime/runtime_status.h"
#include "naim/state/demo_state.h"
#include "naim/state/sqlite_store.h"
#include "naim/state/state_json.h"

namespace naim::hostd {

HostdDesiredStateDisplaySupport::HostdDesiredStateDisplaySupport(
    const HostdDesiredStatePathSupport& path_support)
    : path_support_(path_support),
      runtime_telemetry_support_(),
      local_state_path_support_(),
      local_state_repository_(local_state_path_support_),
      local_runtime_state_support_(
          path_support_,
          local_state_repository_,
          runtime_telemetry_support_) {}

void HostdDesiredStateDisplaySupport::ShowDemoOps(
    const std::string& node_name,
    const std::string& storage_root,
    const std::optional<std::string>& runtime_root) const {
  const naim::DesiredState state =
      path_support_.RebaseStateForRuntimeRoot(
          naim::BuildDemoState(),
          storage_root,
          runtime_root);
  const auto plan = FindNodeExecutionPlan(
      naim::BuildNodeExecutionPlans(
          std::nullopt,
          state,
          DefaultArtifactsRoot()),
      node_name);

  std::cout << "hostd demo ops for node=" << plan.node_name << "\n";
  std::cout << naim::RenderNodeExecutionPlans({plan});
}

void HostdDesiredStateDisplaySupport::ShowStateOps(
    const std::string& db_path,
    const std::string& node_name,
    const std::string& artifacts_root,
    const std::string& storage_root,
    const std::optional<std::string>& runtime_root,
    const std::string& state_root) const {
  naim::ControllerStore store(db_path);
  store.Initialize();
  const auto state = store.LoadDesiredState();
  if (!state.has_value()) {
    throw std::runtime_error("no desired state found in db '" + db_path + "'");
  }

  const naim::DesiredState rebased_full_state =
      path_support_.RebaseStateForRuntimeRoot(*state, storage_root, runtime_root);
  const naim::DesiredState desired_node_state =
      naim::SliceDesiredStateForNode(rebased_full_state, node_name);
  const auto desired_generation = store.LoadDesiredGeneration();

  std::cout << "db=" << db_path << "\n";
  ShowDesiredNodeOps(
      desired_node_state,
      artifacts_root,
      runtime_root,
      state_root,
      "hostd desired ops",
      desired_generation);
}

void HostdDesiredStateDisplaySupport::ShowLocalState(
    const std::string& node_name,
    const std::string& state_root) const {
  const auto local_state =
      local_state_repository_.LoadLocalAppliedState(state_root, node_name);
  if (!local_state.has_value()) {
    std::cout << "hostd local state for node=" << node_name << "\n";
    std::cout << "state_path=" << local_state_path_support_.LocalStatePath(state_root, node_name)
              << "\n";
    const auto generation = local_state_repository_.LoadLocalAppliedGeneration(state_root, node_name);
    if (generation.has_value()) {
      std::cout << "applied_generation=" << *generation << "\n";
    }
    std::cout << "state: empty\n";
    return;
  }

  local_state_repository_.PrintLocalStateSummary(
      *local_state,
      local_state_path_support_.LocalStatePath(state_root, node_name),
      node_name,
      local_state_repository_.LoadLocalAppliedGeneration(state_root, node_name));
}

void HostdDesiredStateDisplaySupport::ShowRuntimeStatus(
    const std::string& node_name,
    const std::string& state_root) const {
  const auto local_state =
      local_state_repository_.LoadLocalAppliedState(state_root, node_name);
  std::cout << "hostd runtime status for node=" << node_name << "\n";
  std::cout << "state_path=" << local_state_path_support_.LocalStatePath(state_root, node_name)
            << "\n";
  if (!local_state.has_value()) {
    std::cout << "runtime_status: unavailable (no local applied state)\n";
    return;
  }

  const auto runtime_status =
      local_runtime_state_support_.LoadLocalRuntimeStatus(state_root, node_name);
  if (!runtime_status.has_value()) {
    std::cout << "runtime_status: empty\n";
    return;
  }

  std::cout << "plane=" << runtime_status->plane_name << "\n";
  std::cout << "control_root=" << runtime_status->control_root << "\n";
  std::cout << "primary_infer_node=" << runtime_status->primary_infer_node << "\n";
  std::cout << "runtime_backend="
            << (runtime_status->runtime_backend.empty()
                    ? "(empty)"
                    : runtime_status->runtime_backend)
            << "\n";
  std::cout << "runtime_phase="
            << (runtime_status->runtime_phase.empty() ? "(empty)" : runtime_status->runtime_phase)
            << "\n";
  std::cout << "supervisor_pid=" << runtime_status->supervisor_pid << "\n";
  std::cout << "started_at=" << FormatDisplayTimestamp(runtime_status->started_at) << "\n";
  std::cout << "enabled_gpu_nodes=" << runtime_status->enabled_gpu_nodes << "\n";
  std::cout << "registry_entries=" << runtime_status->registry_entries << "\n";
  std::cout << "active_model="
            << (runtime_status->active_model_id.empty()
                    ? "(empty)"
                    : runtime_status->active_model_id)
            << " served="
            << (runtime_status->active_served_model_name.empty()
                    ? "(empty)"
                    : runtime_status->active_served_model_name)
            << "\n";
  std::cout << "runtime_profile="
            << (runtime_status->active_runtime_profile.empty()
                    ? "(empty)"
                    : runtime_status->active_runtime_profile)
            << "\n";
  std::cout << "gateway_listen=" << runtime_status->gateway_listen << "\n";
  std::cout << "upstream_models_url=" << runtime_status->upstream_models_url << "\n";
  std::cout << "inference_health_url=" << runtime_status->inference_health_url << "\n";
  std::cout << "gateway_health_url=" << runtime_status->gateway_health_url << "\n";
  std::cout << "active_model_ready=" << (runtime_status->active_model_ready ? "yes" : "no")
            << "\n";
  std::cout << "gateway_plan_ready=" << (runtime_status->gateway_plan_ready ? "yes" : "no")
            << "\n";
  std::cout << "inference_ready=" << (runtime_status->inference_ready ? "yes" : "no") << "\n";
  std::cout << "gateway_ready=" << (runtime_status->gateway_ready ? "yes" : "no") << "\n";
  std::cout << "launch_ready=" << (runtime_status->launch_ready ? "yes" : "no") << "\n";
}

std::string HostdDesiredStateDisplaySupport::RuntimeConfigSummary(
    const naim::DesiredState& state) {
  std::ostringstream out;
  out << "gpu_nodes=" << state.runtime_gpu_nodes.size()
      << " primary_infer_node=" << state.inference.primary_infer_node
      << " gateway=" << state.gateway.listen_host << ":" << state.gateway.listen_port;
  return out.str();
}

std::string HostdDesiredStateDisplaySupport::DefaultArtifactsRoot() {
  return (std::filesystem::path("var") / "artifacts").string();
}

naim::NodeExecutionPlan HostdDesiredStateDisplaySupport::FindNodeExecutionPlan(
    const std::vector<naim::NodeExecutionPlan>& plans,
    const std::string& node_name) {
  for (const auto& plan : plans) {
    if (plan.node_name == node_name) {
      return plan;
    }
  }
  throw std::runtime_error("node '" + node_name + "' not found in execution plan");
}

bool HostdDesiredStateDisplaySupport::StateHasNode(
    const naim::DesiredState& state,
    const std::string& node_name) {
  for (const auto& node : state.nodes) {
    if (node.name == node_name) {
      return true;
    }
  }
  return false;
}

std::string HostdDesiredStateDisplaySupport::ComposePathForNode(
    const std::string& artifacts_root,
    const std::string& plane_name,
    const std::string& node_name) {
  return (std::filesystem::path(artifacts_root) / plane_name / node_name / "docker-compose.yml")
      .string();
}

naim::NodeExecutionPlan HostdDesiredStateDisplaySupport::ResolveNodeExecutionPlan(
    const std::vector<naim::NodeExecutionPlan>& plans,
    const std::optional<naim::DesiredState>& current_state,
    const naim::DesiredState& desired_state,
    const std::string& node_name,
    const std::string& artifacts_root) {
  for (const auto& plan : plans) {
    if (plan.node_name == node_name) {
      return plan;
    }
  }

  const bool in_current = current_state.has_value() && StateHasNode(*current_state, node_name);
  const bool in_desired = StateHasNode(desired_state, node_name);
  if (!in_current && !in_desired) {
    throw std::runtime_error("node '" + node_name + "' not found in execution plan");
  }

  const std::string plane_name = in_desired ? desired_state.plane_name : current_state->plane_name;
  naim::NodeExecutionPlan plan;
  plan.plane_name = plane_name;
  plan.node_name = node_name;
  plan.compose_file_path = ComposePathForNode(artifacts_root, plane_name, node_name);
  return plan;
}

std::optional<std::tm> HostdDesiredStateDisplaySupport::ParseDisplayTimestamp(
    const std::string& value) {
  if (value.empty()) {
    return std::nullopt;
  }
  for (const char* format : {"%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%dT%H:%M:%S"}) {
    std::tm tm{};
    std::istringstream input(value);
    input >> std::get_time(&tm, format);
    if (!input.fail()) {
      return tm;
    }
  }
  return std::nullopt;
}

std::string HostdDesiredStateDisplaySupport::FormatDisplayTimestamp(const std::string& value) {
  const auto parsed = ParseDisplayTimestamp(value);
  if (!parsed.has_value()) {
    return value.empty() ? "(empty)" : value;
  }
  std::ostringstream output;
  output << std::put_time(&*parsed, "%d/%m/%Y %H:%M:%S");
  return output.str();
}

void HostdDesiredStateDisplaySupport::ShowDesiredNodeOps(
    const naim::DesiredState& desired_node_state,
    const std::string& artifacts_root,
    const std::optional<std::string>& runtime_root,
    const std::string& state_root,
    const std::string& source_label,
    const std::optional<int>& desired_generation) const {
  const std::string node_name =
      local_state_repository_.RequireSingleNodeName(desired_node_state);
  const auto current_local_state =
      local_state_repository_.LoadLocalAppliedState(
          state_root,
          node_name,
          desired_node_state.plane_name);
  const auto applied_generation =
      local_state_repository_.LoadLocalAppliedGeneration(
          state_root,
          node_name,
          desired_node_state.plane_name);
  const auto plan = ResolveNodeExecutionPlan(
      naim::BuildNodeExecutionPlans(current_local_state, desired_node_state, artifacts_root),
      current_local_state,
      desired_node_state,
      node_name,
      artifacts_root);

  std::cout << source_label << " for node=" << plan.node_name << "\n";
  std::cout << "artifacts_root=" << artifacts_root << "\n";
  std::cout << "state_path=" << local_state_path_support_.LocalPlaneStatePath(
                                      state_root,
                                      node_name,
                                      desired_node_state.plane_name)
            << "\n";
  if (desired_generation.has_value()) {
    std::cout << "desired_generation=" << *desired_generation << "\n";
  }
  if (applied_generation.has_value()) {
    std::cout << "applied_generation=" << *applied_generation << "\n";
  }
  if (runtime_root.has_value()) {
    std::cout << "runtime_root=" << *runtime_root << "\n";
  }
  if (const auto runtime_config_path =
          path_support_.InferRuntimeConfigPathForNode(desired_node_state, node_name)) {
    std::cout << "infer_runtime_config=" << *runtime_config_path << "\n";
    std::cout << "infer_runtime_summary=" << RuntimeConfigSummary(desired_node_state) << "\n";
  }
  std::cout << naim::RenderNodeExecutionPlans({plan});
}

}  // namespace naim::hostd
