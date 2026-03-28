#pragma once

#include "app/controller_main_includes.h"

namespace comet::controller::read_model_support {

BundleHttpService CreateBundleHttpService(const IBundleCliService& bundle_cli_service);

ReadModelService CreateReadModelService(
    const ControllerRuntimeSupportService& runtime_support_service);

ReadModelHttpService CreateReadModelHttpService(
    const ReadModelService& read_model_service,
    const SchedulerViewService& scheduler_view_service,
    const StateAggregateLoader& state_aggregate_loader,
    int stale_after_seconds);

SchedulerHttpService CreateSchedulerHttpService(
    const ReadModelService& read_model_service,
    const AssignmentOrchestrationService& assignment_orchestration_service,
    const ControllerSchedulerServiceFactory& scheduler_service_factory);

ReadModelCliService CreateReadModelCliService(
    const ControllerPrintService& controller_print_service,
    const StateAggregateLoader& state_aggregate_loader,
    const SchedulerViewService& scheduler_view_service,
    int stale_after_seconds,
    int stable_samples_required);

}  // namespace comet::controller::read_model_support
