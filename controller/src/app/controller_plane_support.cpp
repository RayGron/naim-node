#include "app/controller_plane_support.h"

#include "app/controller_composition_support.h"
#include <memory>

namespace naim::controller::plane_support {

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
          const naim::DesiredState& desired_state,
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
    const PlaneSkillCatalogService& plane_skill_catalog_service,
    const KnowledgeVaultHttpService& knowledge_vault_http_service,
    const DashboardService& dashboard_service,
    int stale_after_seconds) {
  return PlaneHttpService(PlaneHttpSupport(
      request_support,
      plane_mutation_service,
      plane_registry_service,
      controller_state_service,
      dashboard_service,
      stale_after_seconds,
      [&knowledge_vault_http_service](
          const std::string& db_path,
          const HttpRequest& request,
          const std::string&) -> std::optional<HttpResponse> {
        HttpRequest rewritten = request;
        constexpr const char* kPlanePrefix = "/api/v1/planes/";
        const std::string remainder = request.path.substr(std::string(kPlanePrefix).size());
        const auto separator = remainder.find('/');
        if (separator == std::string::npos) {
          return std::nullopt;
        }
        const std::string suffix = remainder.substr(separator);
        rewritten.path = "/api/v1" + suffix;
        return knowledge_vault_http_service.HandleRequest(db_path, rewritten);
      }),
      plane_skill_catalog_service);
}

PlaneService CreatePlaneService(
    const std::string& db_path,
    const ControllerPrintService& controller_print_service,
    const DesiredStatePolicyService& desired_state_policy_service,
    const PlaneRealizationService& plane_realization_service,
    const std::string& default_artifacts_root) {
  auto state_presentation_support =
      std::make_shared<ControllerPlaneStatePresentationSupport>(controller_print_service);
  auto lifecycle_support = std::make_shared<ControllerPlaneLifecycleSupport>(
      desired_state_policy_service,
      plane_realization_service,
      default_artifacts_root);
  return PlaneService(
      db_path,
      std::move(state_presentation_support),
      std::move(lifecycle_support));
}

HostRegistryService CreateHostRegistryService(const std::string& db_path) {
  return HostRegistryService(
      db_path,
      [](naim::ControllerStore& store,
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

}  // namespace naim::controller::plane_support
