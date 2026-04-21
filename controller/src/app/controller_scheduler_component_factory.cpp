#include "app/controller_scheduler_component_factory.h"

#include "app/controller_component_defaults.h"
#include "app/controller_plane_support.h"
#include "app/controller_read_model_support.h"
#include "scheduler/scheduler_domain_support.h"
#include "scheduler/scheduler_execution_dependencies.h"

namespace naim::controller {

SchedulerService ControllerSchedulerComponentFactory::CreateSchedulerService(
    const std::string& db_path,
    const std::string& artifacts_root) const {
  return SchedulerService(
      db_path,
      artifacts_root,
      Defaults().DefaultStaleAfterSeconds(),
      Defaults().VerificationStableSamplesRequired(),
      StateAggregateLoaderInstance(),
      SchedulerViewServiceInstance(),
      ReadModelCliServiceInstance(),
      ControllerPrintServiceInstance(),
      RuntimeSupportService(),
      SchedulerExecutionSupportInstance(),
      PlaneRealizationServiceInstance(),
      ControllerEventServiceInstance());
}

const ControllerComponentDefaults& ControllerSchedulerComponentFactory::Defaults() const {
  static const ControllerComponentDefaults defaults;
  return defaults;
}

const ControllerEventService&
ControllerSchedulerComponentFactory::ControllerEventServiceInstance() const {
  static const ControllerEventService controller_event_service;
  return controller_event_service;
}

const ControllerPrintService&
ControllerSchedulerComponentFactory::ControllerPrintServiceInstance() const {
  static const ControllerPrintService controller_print_service =
      plane_support::CreateControllerPrintService(RuntimeSupportService());
  return controller_print_service;
}

const ControllerRuntimeSupportService&
ControllerSchedulerComponentFactory::RuntimeSupportService() const {
  static const ControllerRuntimeSupportService runtime_support_service;
  return runtime_support_service;
}

const PlaneRealizationService&
ControllerSchedulerComponentFactory::PlaneRealizationServiceInstance() const {
  static const PlaneRealizationService plane_realization_service(
      &RuntimeSupportService(),
      Defaults().DefaultStaleAfterSeconds());
  return plane_realization_service;
}

const SchedulerViewService&
ControllerSchedulerComponentFactory::SchedulerViewServiceInstance() const {
  static const SchedulerViewService scheduler_view_service;
  return scheduler_view_service;
}

const SchedulerDomainService&
ControllerSchedulerComponentFactory::SchedulerDomainServiceInstance() const {
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
ControllerSchedulerComponentFactory::StateAggregateLoaderInstance() const {
  static const StateAggregateLoader state_aggregate_loader(
      SchedulerDomainServiceInstance(),
      SchedulerViewServiceInstance(),
      RuntimeSupportService(),
      Defaults().MaximumRebalanceIterationsPerGeneration());
  return state_aggregate_loader;
}

const ReadModelCliService&
ControllerSchedulerComponentFactory::ReadModelCliServiceInstance() const {
  static const ReadModelCliService read_model_cli_service =
      read_model_support::CreateReadModelCliService(
          ControllerPrintServiceInstance(),
          StateAggregateLoaderInstance(),
          SchedulerViewServiceInstance(),
          Defaults().DefaultStaleAfterSeconds(),
          Defaults().VerificationStableSamplesRequired());
  return read_model_cli_service;
}

const SchedulerExecutionSupport&
ControllerSchedulerComponentFactory::SchedulerExecutionSupportInstance() const {
  const auto* plane_realization_service = &PlaneRealizationServiceInstance();
  const auto* runtime_support_service = &RuntimeSupportService();
  const auto* defaults = &Defaults();
  static const auto assignment_query_support =
      std::make_shared<ControllerSchedulerAssignmentQuerySupport>(
          *plane_realization_service,
          defaults->DefaultArtifactsRoot());
  static const auto verification_support =
      std::make_shared<ControllerSchedulerVerificationSupport>(
          *runtime_support_service);
  static const SchedulerExecutionSupport scheduler_execution_support(
      assignment_query_support,
      verification_support,
      SchedulerExecutionVerificationConfig{
          defaults->VerificationTimeoutSeconds(),
          defaults->VerificationStableSamplesRequired(),
      });
  return scheduler_execution_support;
}

}  // namespace naim::controller
