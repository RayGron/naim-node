#include "app/controller_plane_support.h"

#include "app/controller_composition_support.h"

namespace comet::controller::plane_support {

ControllerPrintService CreateControllerPrintService(
    const ControllerRuntimeSupportService& runtime_support_service) {
  return ControllerPrintService(runtime_support_service);
}

BundleCliService CreateBundleCliService(
    const ControllerPrintService& controller_print_service,
    const DesiredStatePolicyService& desired_state_policy_service,
    const ControllerRuntimeSupportService& runtime_support_service,
    const PlaneRealizationService& plane_realization_service,
    const std::string& default_artifacts_root,
    int stale_after_seconds) {
  return BundleCliService(
      controller_print_service,
      desired_state_policy_service,
      plane_realization_service,
      runtime_support_service,
      default_artifacts_root,
      stale_after_seconds);
}

PlaneMutationService CreatePlaneMutationService(
    const BundleCliService& bundle_cli_service,
    PlaneMutationService::MakePlaneServiceFn make_plane_service) {
  return PlaneMutationService({
      [&](const std::string& db_path,
          const comet::DesiredState& desired_state,
          const std::string& artifacts_root,
          const std::string& source_label) {
        return bundle_cli_service.ApplyDesiredState(
            db_path, desired_state, artifacts_root, source_label);
      },
      std::move(make_plane_service),
  });
}

PlaneHttpService CreatePlaneHttpService(
    const ControllerRequestSupport& request_support,
    const PlaneMutationService& plane_mutation_service,
    const PlaneRegistryService& plane_registry_service,
    const ControllerStateService& controller_state_service,
    const DashboardService& dashboard_service,
    int stale_after_seconds) {
  return PlaneHttpService(PlaneHttpSupport(
      request_support,
      plane_mutation_service,
      plane_registry_service,
      controller_state_service,
      dashboard_service,
      stale_after_seconds));
}

PlaneService CreatePlaneService(
    const std::string& db_path,
    const ControllerPrintService& controller_print_service,
    const DesiredStatePolicyService& desired_state_policy_service,
    const PlaneRealizationService& plane_realization_service,
    const std::string& default_artifacts_root) {
  return PlaneService(
      db_path,
      [](const std::string& value) {
        return ControllerTimeSupport::FormatDisplayTimestamp(value);
      },
      [&](const comet::DesiredState& state) {
        controller_print_service.PrintStateSummary(state);
      },
      [&](comet::ControllerStore& store, comet::DesiredState* desired_state) {
        desired_state_policy_service.ApplyRegisteredHostExecutionModes(
            store, desired_state);
        desired_state_policy_service.ResolveDesiredStateDynamicPlacements(
            store, desired_state);
        desired_state_policy_service.ValidateDesiredStateForControllerAdmission(
            *desired_state);
        desired_state_policy_service.ValidateDesiredStateExecutionModes(
            *desired_state);
      },
      [](comet::ControllerStore& store,
         const std::string& category,
         const std::string& event_type,
         const std::string& message,
         const nlohmann::json& payload,
         const std::string& plane_name) {
        composition_support::AppendControllerEvent(
            store, category, event_type, message, payload, plane_name);
      },
      [](comet::ControllerStore& store, const std::string& plane_name) {
        return composition_support::CanFinalizeDeletedPlane(store, plane_name);
      },
      [&](const std::vector<comet::HostAssignment>& assignments, const std::string& plane_name) {
        return plane_realization_service.FindLatestHostAssignmentForPlane(assignments, plane_name);
      },
      [&](const comet::DesiredState& desired_state,
         const std::string& artifacts_root,
         int desired_generation,
         const std::vector<comet::NodeAvailabilityOverride>& availability_overrides,
         const std::vector<comet::HostObservation>& observations,
         const comet::SchedulingPolicyReport& scheduling_report) {
        return plane_realization_service.BuildHostAssignments(
            desired_state,
            artifacts_root,
            desired_generation,
            availability_overrides,
            observations,
            scheduling_report);
      },
      [&](const comet::DesiredState& desired_state,
         int desired_generation,
         const std::string& artifacts_root,
         const std::vector<comet::NodeAvailabilityOverride>& availability_overrides) {
        return plane_realization_service.BuildStopPlaneAssignments(
            desired_state,
            desired_generation,
            artifacts_root,
            availability_overrides);
      },
      [&](const comet::DesiredState& desired_state,
         int desired_generation,
         const std::string& artifacts_root) {
        return plane_realization_service.BuildDeletePlaneAssignments(
            desired_state,
            desired_generation,
            artifacts_root);
      },
      [default_artifacts_root]() { return default_artifacts_root; });
}

HostRegistryService CreateHostRegistryService(const std::string& db_path) {
  return HostRegistryService(
      db_path,
      [](comet::ControllerStore& store,
         const std::string& event_type,
         const std::string& message,
         const nlohmann::json& payload,
         const std::string& node_name,
         const std::string& severity) {
        composition_support::AppendControllerEvent(
            store,
            "host-registry",
            event_type,
            message,
            payload,
            "",
            node_name,
            "",
            std::nullopt,
            std::nullopt,
            severity);
      });
}

}  // namespace comet::controller::plane_support
