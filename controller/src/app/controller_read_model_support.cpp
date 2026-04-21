#include "app/controller_read_model_support.h"

#include "app/controller_composition_support.h"

namespace naim::controller::read_model_support {

BundleHttpService CreateBundleHttpService(const IBundleCliService& bundle_cli_service) {
  return BundleHttpService(bundle_cli_service);
}

ReadModelService CreateReadModelService(
    const ControllerRuntimeSupportService& runtime_support_service) {
  return ReadModelService(runtime_support_service);
}

ReadModelHttpService CreateReadModelHttpService(
    const ReadModelService& read_model_service,
    const SchedulerViewService& scheduler_view_service,
    const StateAggregateLoader& state_aggregate_loader,
    int stale_after_seconds) {
  return ReadModelHttpService(ReadModelHttpSupport(
      read_model_service,
      scheduler_view_service,
      state_aggregate_loader,
      stale_after_seconds));
}

SchedulerHttpService CreateSchedulerHttpService(
    const ReadModelService& read_model_service,
    const AssignmentOrchestrationService& assignment_orchestration_service,
    const ControllerSchedulerServiceFactory& scheduler_service_factory) {
  static const ControllerRequestSupport request_support;
  return SchedulerHttpService(
      request_support,
      read_model_service,
      assignment_orchestration_service,
      scheduler_service_factory);
}

ReadModelCliService CreateReadModelCliService(
    const ControllerPrintService& controller_print_service,
    const StateAggregateLoader& state_aggregate_loader,
    const SchedulerViewService& scheduler_view_service,
    int stale_after_seconds,
    int stable_samples_required) {
  return ReadModelCliService(
      controller_print_service,
      state_aggregate_loader,
      scheduler_view_service,
      stale_after_seconds,
      stable_samples_required);
}

}  // namespace naim::controller::read_model_support
