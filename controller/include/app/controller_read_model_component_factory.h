#pragma once

#include "app/controller_main_includes.h"

namespace naim::controller {

class ControllerComponentDefaults;

class ControllerReadModelComponentFactory final {
 public:
  BundleHttpService CreateBundleHttpService(const IBundleCliService& bundle_cli_service) const;
  ReadModelService CreateReadModelService() const;
  ReadModelHttpService CreateReadModelHttpService(
      const ReadModelService& read_model_service) const;
  SchedulerHttpService CreateSchedulerHttpService(
      const ReadModelService& read_model_service) const;
  ReadModelCliService CreateReadModelCliService() const;

 private:
  const ControllerComponentDefaults& Defaults() const;
  const ControllerRuntimeSupportService& RuntimeSupportService() const;
  const ControllerPrintService& ControllerPrintServiceInstance() const;
  const SchedulerViewService& SchedulerViewServiceInstance() const;
  const SchedulerDomainService& SchedulerDomainServiceInstance() const;
  const StateAggregateLoader& StateAggregateLoaderInstance() const;
  const AssignmentOrchestrationService& AssignmentOrchestrationServiceInstance() const;
  const ControllerSchedulerServiceFactory& SchedulerServiceFactoryInstance() const;
};

}  // namespace naim::controller
