#pragma once

#include "app/controller_main_includes.h"

namespace naim::controller {

class ControllerComponentDefaults;

class ControllerSchedulerComponentFactory final {
 public:
  SchedulerService CreateSchedulerService(
      const std::string& db_path,
      const std::string& artifacts_root) const;

 private:
  const ControllerComponentDefaults& Defaults() const;
  const ControllerEventService& ControllerEventServiceInstance() const;
  const ControllerPrintService& ControllerPrintServiceInstance() const;
  const ControllerRuntimeSupportService& RuntimeSupportService() const;
  const PlaneRealizationService& PlaneRealizationServiceInstance() const;
  const SchedulerViewService& SchedulerViewServiceInstance() const;
  const SchedulerDomainService& SchedulerDomainServiceInstance() const;
  const StateAggregateLoader& StateAggregateLoaderInstance() const;
  const ReadModelCliService& ReadModelCliServiceInstance() const;
  const SchedulerExecutionSupport& SchedulerExecutionSupportInstance() const;
};

}  // namespace naim::controller
