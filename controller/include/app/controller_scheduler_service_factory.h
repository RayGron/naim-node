#pragma once

#include "scheduler/scheduler_http_service.h"

namespace comet::controller {

class ControllerSchedulerServiceFactory final : public ISchedulerServiceFactory {
 public:
  SchedulerService CreateSchedulerService(
      const std::string& db_path,
      const std::string& artifacts_root) const override;
};

}  // namespace comet::controller
