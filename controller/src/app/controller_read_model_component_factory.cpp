#include "app/controller_read_model_component_factory.h"

#include "app/controller_component_defaults.h"
#include "app/controller_plane_support.h"
#include "app/controller_read_model_support.h"
#include "scheduler/scheduler_domain_support.h"

namespace naim::controller {

BundleHttpService ControllerReadModelComponentFactory::CreateBundleHttpService(
    const IBundleCliService& bundle_cli_service) const {
  return read_model_support::CreateBundleHttpService(bundle_cli_service);
}

ReadModelService ControllerReadModelComponentFactory::CreateReadModelService() const {
  return read_model_support::CreateReadModelService(RuntimeSupportService());
}

ReadModelHttpService ControllerReadModelComponentFactory::CreateReadModelHttpService(
    const ReadModelService& read_model_service) const {
  return read_model_support::CreateReadModelHttpService(
      read_model_service,
      SchedulerViewServiceInstance(),
      StateAggregateLoaderInstance(),
      Defaults().DefaultStaleAfterSeconds());
}

SchedulerHttpService ControllerReadModelComponentFactory::CreateSchedulerHttpService(
    const ReadModelService& read_model_service) const {
  return read_model_support::CreateSchedulerHttpService(
      read_model_service,
      AssignmentOrchestrationServiceInstance(),
      SchedulerServiceFactoryInstance());
}

ReadModelCliService ControllerReadModelComponentFactory::CreateReadModelCliService() const {
  return read_model_support::CreateReadModelCliService(
      ControllerPrintServiceInstance(),
      StateAggregateLoaderInstance(),
      SchedulerViewServiceInstance(),
      Defaults().DefaultStaleAfterSeconds(),
      Defaults().VerificationStableSamplesRequired());
}

const ControllerComponentDefaults& ControllerReadModelComponentFactory::Defaults() const {
  static const ControllerComponentDefaults defaults;
  return defaults;
}

const ControllerRuntimeSupportService&
ControllerReadModelComponentFactory::RuntimeSupportService() const {
  static const ControllerRuntimeSupportService runtime_support_service;
  return runtime_support_service;
}

const ControllerPrintService&
ControllerReadModelComponentFactory::ControllerPrintServiceInstance() const {
  static const ControllerPrintService controller_print_service =
      plane_support::CreateControllerPrintService(RuntimeSupportService());
  return controller_print_service;
}

const SchedulerViewService&
ControllerReadModelComponentFactory::SchedulerViewServiceInstance() const {
  static const SchedulerViewService scheduler_view_service;
  return scheduler_view_service;
}

const SchedulerDomainService&
ControllerReadModelComponentFactory::SchedulerDomainServiceInstance() const {
  const auto* runtime_support_service = &RuntimeSupportService();
  const auto* defaults = &Defaults();
  static const PlaneRealizationService plane_realization_service(
      runtime_support_service,
      defaults->DefaultStaleAfterSeconds());
  static const auto scheduler_domain_support =
      std::make_shared<ControllerSchedulerDomainSupport>(
          *runtime_support_service,
          plane_realization_service);
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
ControllerReadModelComponentFactory::StateAggregateLoaderInstance() const {
  static const StateAggregateLoader state_aggregate_loader(
      SchedulerDomainServiceInstance(),
      SchedulerViewServiceInstance(),
      RuntimeSupportService(),
      Defaults().MaximumRebalanceIterationsPerGeneration());
  return state_aggregate_loader;
}

const AssignmentOrchestrationService&
ControllerReadModelComponentFactory::AssignmentOrchestrationServiceInstance() const {
  static const ControllerEventService controller_event_service;
  static const AssignmentOrchestrationService assignment_orchestration_service(
      controller_event_service,
      ControllerPrintServiceInstance(),
      Defaults().DefaultArtifactsRoot());
  return assignment_orchestration_service;
}

const ControllerSchedulerServiceFactory&
ControllerReadModelComponentFactory::SchedulerServiceFactoryInstance() const {
  static const ControllerSchedulerServiceFactory scheduler_service_factory;
  return scheduler_service_factory;
}

}  // namespace naim::controller
