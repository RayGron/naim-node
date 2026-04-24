#include "app/controller_plane_component_factory.h"

#include "app/controller_component_defaults.h"
#include "app/controller_plane_support.h"
#include "app/controller_read_model_support.h"
#include "app/controller_composition_support.h"
#include "scheduler/scheduler_domain_support.h"

namespace naim::controller {

using nlohmann::json;

BundleCliService ControllerPlaneComponentFactory::CreateBundleCliService() const {
  return plane_support::CreateBundleCliService(
      ControllerPrintServiceInstance(),
      DesiredStatePolicyServiceInstance(),
      RuntimeSupportService(),
      PlaneRealizationServiceInstance(),
      Defaults().DefaultArtifactsRoot(),
      Defaults().DefaultStaleAfterSeconds());
}

ControllerPrintService ControllerPlaneComponentFactory::CreateControllerPrintService() const {
  return plane_support::CreateControllerPrintService(RuntimeSupportService());
}

PlaneMutationService ControllerPlaneComponentFactory::CreatePlaneMutationService() const {
  const auto* controller_print_service = &ControllerPrintServiceInstance();
  const auto* desired_state_policy_service = &DesiredStatePolicyServiceInstance();
  const auto* plane_realization_service = &PlaneRealizationServiceInstance();
  const auto* defaults = &Defaults();
  return plane_support::CreatePlaneMutationService(
      BundleCliServiceInstance(),
      [controller_print_service,
       desired_state_policy_service,
       plane_realization_service,
       defaults](const std::string& db_path) {
        return plane_support::CreatePlaneService(
            db_path,
            *controller_print_service,
            *desired_state_policy_service,
            *plane_realization_service,
            defaults->DefaultArtifactsRoot());
      });
}

AssignmentOrchestrationService
ControllerPlaneComponentFactory::CreateAssignmentOrchestrationService() const {
  return AssignmentOrchestrationService(
      ControllerEventServiceInstance(),
      ControllerPrintServiceInstance(),
      Defaults().DefaultArtifactsRoot());
}

PlaneService ControllerPlaneComponentFactory::CreatePlaneService(
    const std::string& db_path) const {
  return plane_support::CreatePlaneService(
      db_path,
      ControllerPrintServiceInstance(),
      DesiredStatePolicyServiceInstance(),
      PlaneRealizationServiceInstance(),
      Defaults().DefaultArtifactsRoot());
}

PlaneHttpService ControllerPlaneComponentFactory::CreatePlaneHttpService() const {
  static const KnowledgeVaultHttpService knowledge_vault_http_service;
  return plane_support::CreatePlaneHttpService(
      RequestSupport(),
      PlaneMutationServiceInstance(),
      PlaneRegistryServiceInstance(),
      ControllerStateServiceInstance(),
      PlaneSkillCatalogServiceInstance(),
      knowledge_vault_http_service,
      DashboardServiceInstance(),
      Defaults().DefaultStaleAfterSeconds());
}

HostRegistryService ControllerPlaneComponentFactory::CreateHostRegistryService(
    const std::string& db_path) const {
  return plane_support::CreateHostRegistryService(db_path);
}

SkillsFactoryHttpService ControllerPlaneComponentFactory::CreateSkillsFactoryHttpService(
    const std::optional<std::string>& upstream_target) const {
  return SkillsFactoryHttpService(
      RequestSupport(),
      SkillsFactoryServiceInstance(),
      upstream_target);
}

const ControllerComponentDefaults& ControllerPlaneComponentFactory::Defaults() const {
  static const ControllerComponentDefaults defaults;
  return defaults;
}

const ControllerRequestSupport& ControllerPlaneComponentFactory::RequestSupport() const {
  static const ControllerRequestSupport request_support;
  return request_support;
}

const ControllerEventService&
ControllerPlaneComponentFactory::ControllerEventServiceInstance() const {
  static const ControllerEventService controller_event_service;
  return controller_event_service;
}

const ControllerRuntimeSupportService&
ControllerPlaneComponentFactory::RuntimeSupportService() const {
  static const ControllerRuntimeSupportService runtime_support_service;
  return runtime_support_service;
}

const DesiredStatePolicyService&
ControllerPlaneComponentFactory::DesiredStatePolicyServiceInstance() const {
  static const DesiredStatePolicyService desired_state_policy_service;
  return desired_state_policy_service;
}

const PlaneRealizationService&
ControllerPlaneComponentFactory::PlaneRealizationServiceInstance() const {
  static const PlaneRealizationService plane_realization_service(
      &RuntimeSupportService(),
      Defaults().DefaultStaleAfterSeconds());
  return plane_realization_service;
}

const ControllerPrintService&
ControllerPlaneComponentFactory::ControllerPrintServiceInstance() const {
  static const ControllerPrintService controller_print_service =
      plane_support::CreateControllerPrintService(RuntimeSupportService());
  return controller_print_service;
}

const BundleCliService& ControllerPlaneComponentFactory::BundleCliServiceInstance() const {
  static const BundleCliService bundle_cli_service =
      plane_support::CreateBundleCliService(
          ControllerPrintServiceInstance(),
          DesiredStatePolicyServiceInstance(),
          RuntimeSupportService(),
          PlaneRealizationServiceInstance(),
          Defaults().DefaultArtifactsRoot(),
          Defaults().DefaultStaleAfterSeconds());
  return bundle_cli_service;
}

const PlaneMutationService&
ControllerPlaneComponentFactory::PlaneMutationServiceInstance() const {
  const auto* bundle_cli_service = &BundleCliServiceInstance();
  const auto* controller_print_service = &ControllerPrintServiceInstance();
  const auto* desired_state_policy_service = &DesiredStatePolicyServiceInstance();
  const auto* plane_realization_service = &PlaneRealizationServiceInstance();
  const auto* defaults = &Defaults();
  static const PlaneMutationService plane_mutation_service =
      plane_support::CreatePlaneMutationService(
          *bundle_cli_service,
          [controller_print_service,
           desired_state_policy_service,
           plane_realization_service,
           defaults](const std::string& db_path) {
            return plane_support::CreatePlaneService(
                db_path,
                *controller_print_service,
                *desired_state_policy_service,
                *plane_realization_service,
                defaults->DefaultArtifactsRoot());
          });
  return plane_mutation_service;
}

const PlaneSkillRuntimeSyncService&
ControllerPlaneComponentFactory::PlaneSkillRuntimeSyncServiceInstance() const {
  static const PlaneSkillRuntimeSyncService runtime_sync_service;
  return runtime_sync_service;
}

const PlaneSkillCatalogService&
ControllerPlaneComponentFactory::PlaneSkillCatalogServiceInstance() const {
  const auto* plane_mutation_service = &PlaneMutationServiceInstance();
  const auto* runtime_sync_service = &PlaneSkillRuntimeSyncServiceInstance();
  const auto* defaults = &Defaults();
  static const PlaneSkillCatalogService plane_skill_catalog_service(
      *plane_mutation_service,
      *runtime_sync_service,
      [defaults](const std::string& db_path,
          const std::string& plane_name,
          const std::string& fallback_artifacts_root) {
        return defaults->ResolvePlaneArtifactsRoot(
            db_path, plane_name, fallback_artifacts_root);
      });
  return plane_skill_catalog_service;
}

const ControllerStateService&
ControllerPlaneComponentFactory::ControllerStateServiceInstance() const {
  static const ControllerStateService controller_state_service(
      ControllerStateService::Deps{
          [](naim::ControllerStore& store, const std::string& plane_name) {
            return composition_support::CanFinalizeDeletedPlane(store, plane_name);
          },
          [](naim::ControllerStore& store,
              const std::string& category,
              const std::string& event_type,
              const std::string& message,
              const json& payload,
              const std::string& plane_name) {
            composition_support::AppendControllerEvent(
                store,
                category,
                event_type,
                message,
                payload,
                plane_name);
          },
      });
  return controller_state_service;
}

const SchedulerViewService&
ControllerPlaneComponentFactory::SchedulerViewServiceInstance() const {
  static const SchedulerViewService scheduler_view_service;
  return scheduler_view_service;
}

const SchedulerDomainService&
ControllerPlaneComponentFactory::SchedulerDomainServiceInstance() const {
  const auto* plane_realization_service = &PlaneRealizationServiceInstance();
  const auto* runtime_support_service = &RuntimeSupportService();
  const auto* defaults = &Defaults();
  static const auto scheduler_domain_support =
      std::make_shared<ControllerSchedulerDomainSupport>(
          *runtime_support_service,
          *plane_realization_service);
  static const SchedulerDomainService scheduler_domain_service(
      scheduler_domain_support,
      SchedulerDomainPolicyConfig{
          defaults->DefaultStaleAfterSeconds(),
          defaults->MinimumSafeDirectRebalanceScore(),
          defaults->WorkerMinimumResidencySeconds(),
          defaults->NodeCooldownAfterMoveSeconds(),
          85,
          1024,
      });
  return scheduler_domain_service;
}

const StateAggregateLoader&
ControllerPlaneComponentFactory::StateAggregateLoaderInstance() const {
  static const StateAggregateLoader state_aggregate_loader(
      SchedulerDomainServiceInstance(),
      SchedulerViewServiceInstance(),
      RuntimeSupportService(),
      Defaults().MaximumRebalanceIterationsPerGeneration());
  return state_aggregate_loader;
}

const ReadModelService& ControllerPlaneComponentFactory::ReadModelServiceInstance() const {
  static const ReadModelService read_model_service =
      read_model_support::CreateReadModelService(RuntimeSupportService());
  return read_model_service;
}

const DashboardService& ControllerPlaneComponentFactory::DashboardServiceInstance() const {
  const auto* state_aggregate_loader = &StateAggregateLoaderInstance();
  const auto* read_model_service = &ReadModelServiceInstance();
  const auto* runtime_support_service = &RuntimeSupportService();
  static const DashboardService dashboard_service(
      DashboardService::Deps{
          state_aggregate_loader,
          [read_model_service](const naim::EventRecord& event) {
            return read_model_service->BuildEventPayloadItem(event);
          },
          [runtime_support_service](
              const std::vector<naim::NodeAvailabilityOverride>& overrides) {
            return runtime_support_service->BuildAvailabilityOverrideMap(overrides);
          },
          [runtime_support_service](
              const std::map<std::string, naim::NodeAvailabilityOverride>& overrides,
              const std::string& node_name) {
            return runtime_support_service->ResolveNodeAvailability(overrides, node_name);
          },
          [runtime_support_service](const std::string& heartbeat_at) {
            return runtime_support_service->HeartbeatAgeSeconds(heartbeat_at);
          },
          [runtime_support_service](
              const std::optional<long long>& age_seconds, int stale_after_seconds) {
            return runtime_support_service->HealthFromAge(age_seconds, stale_after_seconds);
          },
          [runtime_support_service](const naim::HostObservation& observation) {
            return runtime_support_service->ParseRuntimeStatus(observation);
          },
          [runtime_support_service](const naim::HostObservation& observation) {
            return runtime_support_service->ParseGpuTelemetry(observation);
          },
      });
  return dashboard_service;
}

const PlaneRegistryService&
ControllerPlaneComponentFactory::PlaneRegistryServiceInstance() const {
  const auto* runtime_support_service = &RuntimeSupportService();
  const auto* desired_state_policy_service = &DesiredStatePolicyServiceInstance();
  const auto* plane_realization_service = &PlaneRealizationServiceInstance();
  const auto* defaults = &Defaults();
  static const auto lifecycle_support = std::make_shared<ControllerPlaneLifecycleSupport>(
      *desired_state_policy_service,
      *plane_realization_service,
      defaults->DefaultArtifactsRoot());
  static const auto query_support = std::make_shared<ControllerPlaneRegistryQuerySupport>(
      *runtime_support_service);
  static const PlaneRegistryService plane_registry_service(
      lifecycle_support,
      query_support);
  return plane_registry_service;
}

const SkillsFactoryService& ControllerPlaneComponentFactory::SkillsFactoryServiceInstance() const {
  const auto* plane_mutation_service = &PlaneMutationServiceInstance();
  const auto* runtime_sync_service = &PlaneSkillRuntimeSyncServiceInstance();
  const auto* defaults = &Defaults();
  static const SkillsFactoryService skills_factory_service(
      *plane_mutation_service,
      *runtime_sync_service,
      [defaults](const std::string& db_path,
          const std::string& plane_name,
          const std::string& fallback_artifacts_root) {
        return defaults->ResolvePlaneArtifactsRoot(
            db_path, plane_name, fallback_artifacts_root);
      });
  return skills_factory_service;
}

}  // namespace naim::controller
