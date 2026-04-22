#include "bundle/bundle_cli_service.h"

#include <cmath>
#include <filesystem>
#include <iostream>
#include <set>
#include <sstream>
#include <stdexcept>
#include <utility>

#include "naim/planning/compose_renderer.h"
#include "naim/state/demo_state.h"
#include "naim/importing/import_bundle.h"
#include "naim/runtime/infer_runtime_config.h"
#include "naim/planning/planner.h"
#include "naim/planning/reconcile.h"
#include "naim/state/state_json.h"
#include "skills/knowledge_vault_common_skills.h"

namespace naim::controller {

namespace {

void PrintPreviewSummary(const naim::DesiredState& state) {
  std::cout << "preview:\n";
  std::cout << "  plane=" << state.plane_name << "\n";
  std::cout << "  nodes=" << state.nodes.size() << "\n";
  std::cout << "  disks=" << state.disks.size() << "\n";
  std::cout << "  instances=" << state.instances.size() << "\n";

  const auto node_plans = naim::BuildNodeComposePlans(state);
  for (const auto& plan : node_plans) {
    std::cout << "  node " << plan.node_name
              << ": services=" << plan.services.size()
              << " disks=" << plan.disks.size() << "\n";
  }
}

int RenderComposeForState(
    const naim::DesiredState& state,
    const std::optional<std::string>& node_name) {
  if (node_name.has_value()) {
    const auto plan = naim::FindNodeComposePlan(state, *node_name);
    if (!plan.has_value()) {
      std::cerr << "error: node '" << *node_name << "' not found in state\n";
      return 1;
    }
    std::cout << naim::RenderComposeYaml(*plan);
    return 0;
  }

  const auto plans = naim::BuildNodeComposePlans(state);
  for (std::size_t index = 0; index < plans.size(); ++index) {
    if (index > 0) {
      std::cout << "\n---\n";
    }
    std::cout << naim::RenderComposeYaml(plans[index]);
  }
  return 0;
}

std::vector<naim::NodeExecutionPlan> FilterNodeExecutionPlans(
    const std::vector<naim::NodeExecutionPlan>& plans,
    const std::optional<std::string>& node_name) {
  if (!node_name.has_value()) {
    return plans;
  }

  std::vector<naim::NodeExecutionPlan> filtered;
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

std::string RenderInferenceKnobs(const naim::InferenceRuntimeSettings& inference) {
  std::ostringstream output;
  output << "max_model_len=" << inference.max_model_len
         << " llama_ctx_size=" << inference.llama_ctx_size
         << " max_num_seqs=" << inference.max_num_seqs
         << " gpu_memory_utilization=" << inference.gpu_memory_utilization;
  return output.str();
}

void VerifyStoredDesiredStateInference(
    naim::ControllerStore& store,
    const naim::DesiredState& expected_state) {
  const auto stored_state = store.LoadDesiredState(expected_state.plane_name);
  if (!stored_state.has_value()) {
    throw std::runtime_error(
        "stored desired state missing after apply for plane '" +
        expected_state.plane_name + "'");
  }

  const auto& expected = expected_state.inference;
  const auto& actual = stored_state->inference;
  if (expected.max_model_len != actual.max_model_len ||
      expected.llama_ctx_size != actual.llama_ctx_size ||
      expected.max_num_seqs != actual.max_num_seqs ||
      std::abs(expected.gpu_memory_utilization - actual.gpu_memory_utilization) > 1e-9) {
    throw std::runtime_error(
        "stored desired state inference mismatch after apply for plane '" +
        expected_state.plane_name + "': expected {" +
        RenderInferenceKnobs(expected) + "} but stored {" +
        RenderInferenceKnobs(actual) + "}");
  }
}

std::set<std::string> SelectedFactorySkillIds(const std::optional<naim::DesiredState>& state) {
  std::set<std::string> result;
  if (!state.has_value() || !state->skills.has_value()) {
    return result;
  }
  for (const auto& skill_id : state->skills->factory_skill_ids) {
    result.insert(skill_id);
  }
  return result;
}

void DetachRemovedFactorySkillBindings(
    naim::ControllerStore& store,
    const std::optional<naim::DesiredState>& previous_state,
    const naim::DesiredState& next_state) {
  const auto previous_ids = SelectedFactorySkillIds(previous_state);
  const auto next_ids = SelectedFactorySkillIds(next_state);
  for (const auto& skill_id : previous_ids) {
    if (next_ids.count(skill_id) == 0) {
      store.DeletePlaneSkillBinding(next_state.plane_name, skill_id);
    }
  }
}

}  // namespace

BundleCliService::BundleCliService(
    const ControllerPrintService& controller_print_service,
    const DesiredStatePolicyService& desired_state_policy_service,
    const PlaneRealizationService& plane_realization_service,
    ControllerRuntimeSupportService runtime_support_service,
    std::string default_artifacts_root,
    int default_stale_after_seconds)
    : controller_print_service_(controller_print_service),
      desired_state_policy_service_(desired_state_policy_service),
      plane_realization_service_(plane_realization_service),
      runtime_support_service_(std::move(runtime_support_service)),
      default_artifacts_root_(std::move(default_artifacts_root)),
      default_stale_after_seconds_(default_stale_after_seconds) {}

void BundleCliService::AppendEvent(
    naim::ControllerStore& store,
    const std::string& category,
    const std::string& event_type,
    const std::string& message,
    const nlohmann::json& payload,
    const std::string& plane_name,
    const std::string& node_name,
    const std::string& worker_name,
    const std::optional<int>& assignment_id,
    const std::optional<int>& rollout_action_id,
    const std::string& severity) const {
  store.AppendEvent(naim::EventRecord{
      0,
      plane_name,
      node_name,
      worker_name,
      assignment_id,
      rollout_action_id,
      category,
      event_type,
      severity,
      message,
      payload.dump(),
      "",
  });
}

void BundleCliService::ShowDemoPlan() const {
  controller_print_service_.PrintStateSummary(naim::BuildDemoState());
}

int BundleCliService::RenderDemoCompose(
    const std::optional<std::string>& node_name) const {
  return RenderComposeForState(naim::BuildDemoState(), node_name);
}

int BundleCliService::InitDb(const std::string& db_path) const {
  naim::ControllerStore store(db_path);
  store.Initialize();
  std::cout << "initialized db: " << db_path << "\n";
  return 0;
}

int BundleCliService::ValidateBundle(const std::string& bundle_dir) const {
  const naim::DesiredState state = naim::ImportPlaneBundle(bundle_dir);
  naim::RequireSchedulingPolicy(state);
  const naim::SchedulingPolicyReport scheduling_report =
      naim::EvaluateSchedulingPolicy(state);
  std::cout << "bundle validation: OK\n";
  PrintPreviewSummary(state);
  std::cout << naim::RenderSchedulingPolicyReport(scheduling_report);
  controller_print_service_.PrintSchedulerDecisionSummary(state);
  controller_print_service_.PrintRolloutGateSummary(scheduling_report);
  return 0;
}

int BundleCliService::PreviewBundle(
    const std::string& bundle_dir,
    const std::optional<std::string>& node_name) const {
  const naim::DesiredState state = naim::ImportPlaneBundle(bundle_dir);
  const naim::SchedulingPolicyReport scheduling_report =
      naim::EvaluateSchedulingPolicy(state);
  PrintPreviewSummary(state);
  std::cout << naim::RenderSchedulingPolicyReport(scheduling_report);
  controller_print_service_.PrintSchedulerDecisionSummary(state);
  controller_print_service_.PrintRolloutGateSummary(scheduling_report);
  std::cout << "\ncompose-preview:\n";
  return RenderComposeForState(state, node_name);
}

int BundleCliService::PlanBundle(
    const std::string& db_path,
    const std::string& bundle_dir) const {
  naim::ControllerStore store(db_path);
  store.Initialize();
  const auto current_state = store.LoadDesiredState();
  const naim::DesiredState desired_state = naim::ImportPlaneBundle(bundle_dir);
  const auto availability_overrides = store.LoadNodeAvailabilityOverrides();
  const auto observations = store.LoadHostObservations();
  const naim::ReconcilePlan plan =
      naim::BuildReconcilePlan(current_state, desired_state);
  const naim::SchedulingPolicyReport scheduling_report =
      naim::EvaluateSchedulingPolicy(desired_state);

  std::cout << "bundle-plan:\n";
  std::cout << "  db=" << db_path << "\n";
  std::cout << "  bundle=" << bundle_dir << "\n";
  std::cout << naim::RenderReconcilePlan(plan);
  std::cout << naim::RenderSchedulingPolicyReport(scheduling_report);
  controller_print_service_.PrintSchedulerDecisionSummary(desired_state);
  controller_print_service_.PrintRolloutGateSummary(scheduling_report);
  controller_print_service_.PrintAssignmentDispatchSummary(
      desired_state,
      runtime_support_service_.BuildAvailabilityOverrideMap(availability_overrides),
      observations,
      default_stale_after_seconds_);
  return 0;
}

int BundleCliService::PlanHostOps(
    const std::string& db_path,
    const std::string& bundle_dir,
    const std::string& artifacts_root,
    const std::optional<std::string>& node_name) const {
  naim::ControllerStore store(db_path);
  store.Initialize();
  const auto current_state = store.LoadDesiredState();
  const naim::DesiredState desired_state = naim::ImportPlaneBundle(bundle_dir);
  const auto host_plans =
      FilterNodeExecutionPlans(
          naim::BuildNodeExecutionPlans(current_state, desired_state, artifacts_root),
          node_name);

  std::cout << "host-op-plan:\n";
  std::cout << "  db=" << db_path << "\n";
  std::cout << "  bundle=" << bundle_dir << "\n";
  std::cout << "  artifacts_root=" << artifacts_root << "\n";
  std::cout << naim::RenderNodeExecutionPlans(host_plans);
  return 0;
}

int BundleCliService::SeedDemo(const std::string& db_path) const {
  naim::ControllerStore store(db_path);
  store.Initialize();
  const naim::DesiredState desired_state = naim::BuildDemoState();
  naim::RequireSchedulingPolicy(desired_state);
  const naim::SchedulingPolicyReport scheduling_report =
      naim::EvaluateSchedulingPolicy(desired_state);
  const auto availability_overrides = store.LoadNodeAvailabilityOverrides();
  const auto observations = store.LoadHostObservations();
  const int desired_generation =
      store.LoadDesiredGeneration(desired_state.plane_name).value_or(0) + 1;
  store.ReplaceDesiredState(desired_state, desired_generation, 0);
  store.ClearSchedulerPlaneRuntime(desired_state.plane_name);
  store.ReplaceRolloutActions(
      desired_state.plane_name, desired_generation, scheduling_report.rollout_actions);
  store.ReplaceHostAssignments(
      plane_realization_service_.BuildHostAssignments(
          desired_state,
          default_artifacts_root_,
          desired_generation,
          availability_overrides,
          observations,
          scheduling_report));
  std::cout << "seeded demo state into: " << db_path << "\n";
  std::cout << "desired generation: " << desired_generation << "\n";
  controller_print_service_.PrintSchedulerDecisionSummary(desired_state);
  controller_print_service_.PrintRolloutGateSummary(scheduling_report);
  controller_print_service_.PrintAssignmentDispatchSummary(
      desired_state,
      runtime_support_service_.BuildAvailabilityOverrideMap(availability_overrides),
      observations,
      default_stale_after_seconds_);
  return 0;
}

int BundleCliService::ImportBundle(
    const std::string& db_path,
    const std::string& bundle_dir) const {
  naim::ControllerStore store(db_path);
  store.Initialize();
  const naim::DesiredState desired_state = naim::ImportPlaneBundle(bundle_dir);
  naim::RequireSchedulingPolicy(desired_state);
  const int desired_generation =
      store.LoadDesiredGeneration(desired_state.plane_name).value_or(0) + 1;
  store.ReplaceDesiredState(desired_state, desired_generation, 0);
  store.UpdatePlaneArtifactsRoot(desired_state.plane_name, default_artifacts_root_);
  store.ClearSchedulerPlaneRuntime(desired_state.plane_name);
  store.ReplaceRolloutActions(desired_state.plane_name, desired_generation, {});
  AppendEvent(
      store,
      "bundle",
      "imported",
      "imported bundle into desired state; rollout is staged until explicit start",
      nlohmann::json{
          {"bundle_dir", bundle_dir},
          {"desired_generation", desired_generation},
          {"worker_count", desired_state.instances.size()},
          {"disk_count", desired_state.disks.size()},
      },
      desired_state.plane_name,
      "",
      "",
      std::nullopt,
      std::nullopt,
      "info");
  std::cout << "imported bundle '" << bundle_dir << "' into: " << db_path << "\n";
  std::cout << "desired generation: " << desired_generation << "\n";
  std::cout << "runtime rollout is staged; use start-plane to enqueue host assignments\n";
  return 0;
}

int BundleCliService::ApplyBundle(
    const std::string& db_path,
    const std::string& bundle_dir,
    const std::string& artifacts_root) const {
  naim::ControllerStore store(db_path);
  store.Initialize();
  const naim::DesiredState desired_state = naim::ImportPlaneBundle(bundle_dir);
  const auto current_state = store.LoadDesiredState(desired_state.plane_name);
  naim::RequireSchedulingPolicy(desired_state);
  const int desired_generation =
      store.LoadDesiredGeneration(desired_state.plane_name).value_or(0) + 1;
  const naim::ReconcilePlan plan =
      naim::BuildReconcilePlan(current_state, desired_state);
  const naim::SchedulingPolicyReport scheduling_report =
      naim::EvaluateSchedulingPolicy(desired_state);
  const auto host_plans =
      naim::BuildNodeExecutionPlans(current_state, desired_state, artifacts_root);

  std::cout << "apply-plan:\n";
  std::cout << naim::RenderReconcilePlan(plan);
  std::cout << naim::RenderSchedulingPolicyReport(scheduling_report);
  controller_print_service_.PrintSchedulerDecisionSummary(desired_state);
  controller_print_service_.PrintRolloutGateSummary(scheduling_report);
  std::cout << naim::RenderNodeExecutionPlans(host_plans);

  plane_realization_service_.MaterializeComposeArtifacts(desired_state, host_plans);
  plane_realization_service_.MaterializeInferRuntimeArtifact(desired_state, artifacts_root);
  store.ReplaceDesiredState(desired_state, desired_generation, 0);
  DetachRemovedFactorySkillBindings(store, current_state, desired_state);
  store.UpdatePlaneArtifactsRoot(desired_state.plane_name, artifacts_root);
  store.ClearSchedulerPlaneRuntime(desired_state.plane_name);
  store.ReplaceRolloutActions(desired_state.plane_name, desired_generation, {});
  AppendEvent(
      store,
      "bundle",
      "applied",
      "applied bundle into desired state; rollout is staged until explicit start",
      nlohmann::json{
          {"bundle_dir", bundle_dir},
          {"artifacts_root", artifacts_root},
          {"desired_generation", desired_generation},
          {"worker_count", desired_state.instances.size()},
          {"disk_count", desired_state.disks.size()},
      },
      desired_state.plane_name,
      "",
      "",
      std::nullopt,
      std::nullopt,
      "info");
  std::cout << "applied bundle '" << bundle_dir << "' into: " << db_path << "\n";
  std::cout << "desired generation: " << desired_generation << "\n";
  std::cout << "artifacts written under: " << artifacts_root << "\n";
  std::cout << "runtime rollout is staged; use start-plane to enqueue host assignments\n";
  return 0;
}

int BundleCliService::ApplyDesiredState(
    const std::string& db_path,
    const naim::DesiredState& desired_state,
    const std::string& artifacts_root,
    const std::string& source_label) const {
  naim::ControllerStore store(db_path);
  store.Initialize();
  naim::DesiredState effective_desired_state = desired_state;
  desired_state_policy_service_.ApplyRegisteredHostExecutionModes(
      store, &effective_desired_state);
  desired_state_policy_service_.ResolveDesiredStateDynamicPlacements(
      store, &effective_desired_state);
  EnsureKnowledgeVaultCommonSkills(store, &effective_desired_state);
  desired_state_policy_service_.ValidateDesiredStateForControllerAdmission(
      store,
      effective_desired_state);
  desired_state_policy_service_.ValidateDesiredStateExecutionModes(
      effective_desired_state);
  const auto current_state = store.LoadDesiredState(effective_desired_state.plane_name);
  naim::RequireSchedulingPolicy(effective_desired_state);
  const auto availability_overrides = store.LoadNodeAvailabilityOverrides();
  const auto observations = store.LoadHostObservations();
  const int desired_generation =
      store.LoadDesiredGeneration(effective_desired_state.plane_name).value_or(0) + 1;
  const naim::ReconcilePlan plan =
      naim::BuildReconcilePlan(current_state, effective_desired_state);
  const naim::SchedulingPolicyReport scheduling_report =
      naim::EvaluateSchedulingPolicy(effective_desired_state);
  const auto host_plans =
      naim::BuildNodeExecutionPlans(current_state, effective_desired_state, artifacts_root);

  std::cout << "apply-plan:\n";
  std::cout << naim::RenderReconcilePlan(plan);
  std::cout << naim::RenderSchedulingPolicyReport(scheduling_report);
  controller_print_service_.PrintSchedulerDecisionSummary(effective_desired_state);
  controller_print_service_.PrintRolloutGateSummary(scheduling_report);
  std::cout << naim::RenderNodeExecutionPlans(host_plans);

  plane_realization_service_.MaterializeComposeArtifacts(
      effective_desired_state, host_plans);
  plane_realization_service_.MaterializeInferRuntimeArtifact(
      effective_desired_state, artifacts_root);
  store.ReplaceDesiredState(effective_desired_state, desired_generation, 0);
  DetachRemovedFactorySkillBindings(store, current_state, effective_desired_state);
  VerifyStoredDesiredStateInference(store, effective_desired_state);
  store.UpdatePlaneArtifactsRoot(effective_desired_state.plane_name, artifacts_root);
  store.ClearSchedulerPlaneRuntime(effective_desired_state.plane_name);
  store.ReplaceRolloutActions(effective_desired_state.plane_name, desired_generation, {});

  const bool existed = current_state.has_value();
  AppendEvent(
      store,
      "plane",
      existed ? "staged-update" : "created",
      existed ? "updated plane desired state; rollout is staged until explicit restart"
             : "created plane desired state in stopped lifecycle state",
      nlohmann::json{
          {"source", source_label},
          {"artifacts_root", artifacts_root},
          {"desired_generation", desired_generation},
          {"applied_generation",
           current_state.has_value()
               ? nlohmann::json(store.LoadPlane(effective_desired_state.plane_name)->applied_generation)
               : nlohmann::json(0)},
          {"worker_count", effective_desired_state.instances.size()},
          {"disk_count", effective_desired_state.disks.size()},
      },
      effective_desired_state.plane_name,
      "",
      "",
      std::nullopt,
      std::nullopt,
      "info");
  std::cout << (existed ? "staged update for" : "created") << " plane '"
            << effective_desired_state.plane_name
            << "' in: " << db_path << "\n";
  std::cout << "desired generation: " << desired_generation << "\n";
  const auto plane = store.LoadPlane(effective_desired_state.plane_name);
  if (plane.has_value()) {
    std::cout << "applied generation: " << plane->applied_generation << "\n";
  }
  std::cout << "applied runtime: "
            << RenderInferenceKnobs(effective_desired_state.inference) << "\n";
  std::cout << "runtime rollout is staged; use start-plane to enqueue host assignments\n";
  return 0;
}

int BundleCliService::ApplyStateFile(
    const std::string& db_path,
    const std::string& state_path,
    const std::string& artifacts_root) const {
  const auto desired_state = naim::LoadDesiredStateJson(state_path);
  if (!desired_state.has_value()) {
    throw std::runtime_error("failed to load desired state file '" + state_path + "'");
  }
  return ApplyDesiredState(
      db_path,
      *desired_state,
      artifacts_root,
      "state-file:" + state_path);
}

int BundleCliService::RenderCompose(
    const std::string& db_path,
    const std::optional<std::string>& node_name) const {
  naim::ControllerStore store(db_path);
  store.Initialize();
  const auto state = store.LoadDesiredState();
  if (!state.has_value()) {
    std::cerr << "error: no desired state found in db '" << db_path << "'\n";
    return 1;
  }
  return RenderComposeForState(*state, node_name);
}

int BundleCliService::RenderInferRuntime(const std::string& db_path) const {
  naim::ControllerStore store(db_path);
  store.Initialize();
  const auto state = store.LoadDesiredState();
  if (!state.has_value()) {
    std::cerr << "error: no desired state found in db '" << db_path << "'\n";
    return 1;
  }
  std::cout << naim::RenderInferRuntimeConfigJson(*state) << "\n";
  return 0;
}

ControllerActionResult BundleCliService::ExecuteValidateBundleAction(
    const std::string& bundle_dir) const {
  return RunControllerActionResult(
      "validate-bundle",
      [&]() { return ValidateBundle(bundle_dir); });
}

ControllerActionResult BundleCliService::ExecutePreviewBundleAction(
    const std::string& bundle_dir,
    const std::optional<std::string>& node_name) const {
  return RunControllerActionResult(
      "preview-bundle",
      [&]() { return PreviewBundle(bundle_dir, node_name); });
}

ControllerActionResult BundleCliService::ExecuteImportBundleAction(
    const std::string& db_path,
    const std::string& bundle_dir) const {
  return RunControllerActionResult(
      "import-bundle",
      [&]() { return ImportBundle(db_path, bundle_dir); });
}

ControllerActionResult BundleCliService::ExecuteApplyBundleAction(
    const std::string& db_path,
    const std::string& bundle_dir,
    const std::string& artifacts_root) const {
  return RunControllerActionResult(
      "apply-bundle",
      [&]() { return ApplyBundle(db_path, bundle_dir, artifacts_root); });
}

}  // namespace naim::controller
