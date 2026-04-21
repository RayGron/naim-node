#pragma once

#include "app/controller_main_includes.h"

namespace naim::controller {

class ControllerComponentDefaults;

class ControllerPlaneComponentFactory final {
 public:
  BundleCliService CreateBundleCliService() const;
  ControllerPrintService CreateControllerPrintService() const;
  PlaneMutationService CreatePlaneMutationService() const;
  AssignmentOrchestrationService CreateAssignmentOrchestrationService() const;
  PlaneService CreatePlaneService(const std::string& db_path) const;
  PlaneHttpService CreatePlaneHttpService() const;
  HostRegistryService CreateHostRegistryService(const std::string& db_path) const;
  SkillsFactoryHttpService CreateSkillsFactoryHttpService(
      const std::optional<std::string>& upstream_target) const;

 private:
  const ControllerComponentDefaults& Defaults() const;
  const ControllerRequestSupport& RequestSupport() const;
  const ControllerEventService& ControllerEventServiceInstance() const;
  const ControllerRuntimeSupportService& RuntimeSupportService() const;
  const DesiredStatePolicyService& DesiredStatePolicyServiceInstance() const;
  const PlaneRealizationService& PlaneRealizationServiceInstance() const;
  const ControllerPrintService& ControllerPrintServiceInstance() const;
  const BundleCliService& BundleCliServiceInstance() const;
  const PlaneMutationService& PlaneMutationServiceInstance() const;
  const PlaneSkillRuntimeSyncService& PlaneSkillRuntimeSyncServiceInstance() const;
  const PlaneSkillCatalogService& PlaneSkillCatalogServiceInstance() const;
  const ControllerStateService& ControllerStateServiceInstance() const;
  const SchedulerViewService& SchedulerViewServiceInstance() const;
  const SchedulerDomainService& SchedulerDomainServiceInstance() const;
  const StateAggregateLoader& StateAggregateLoaderInstance() const;
  const ReadModelService& ReadModelServiceInstance() const;
  const DashboardService& DashboardServiceInstance() const;
  const PlaneRegistryService& PlaneRegistryServiceInstance() const;
  const SkillsFactoryService& SkillsFactoryServiceInstance() const;
};

}  // namespace naim::controller
