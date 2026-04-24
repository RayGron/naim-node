#pragma once

#include "app/controller_main_includes.h"

namespace naim::controller::plane_support {

ControllerPrintService CreateControllerPrintService(
    const ControllerRuntimeSupportService& runtime_support_service);

BundleCliService CreateBundleCliService(
    const ControllerPrintService& controller_print_service,
    const DesiredStatePolicyService& desired_state_policy_service,
    const ControllerRuntimeSupportService& runtime_support_service,
    const PlaneRealizationService& plane_realization_service,
    const std::string& default_artifacts_root,
    int stale_after_seconds);

PlaneMutationService CreatePlaneMutationService(
    const BundleCliService& bundle_cli_service,
    PlaneMutationService::MakePlaneServiceFn make_plane_service);

PlaneHttpService CreatePlaneHttpService(
    const ControllerRequestSupport& request_support,
    const PlaneMutationService& plane_mutation_service,
    const PlaneRegistryService& plane_registry_service,
    const ControllerStateService& controller_state_service,
    const PlaneSkillCatalogService& plane_skill_catalog_service,
    const KnowledgeVaultHttpService& knowledge_vault_http_service,
    const DashboardService& dashboard_service,
    int stale_after_seconds);

PlaneService CreatePlaneService(
    const std::string& db_path,
    const ControllerPrintService& controller_print_service,
    const DesiredStatePolicyService& desired_state_policy_service,
    const PlaneRealizationService& plane_realization_service,
    const std::string& default_artifacts_root);

HostRegistryService CreateHostRegistryService(const std::string& db_path);

}  // namespace naim::controller::plane_support
