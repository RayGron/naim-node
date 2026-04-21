#pragma once

#include "scheduler/scheduler_http_service.h"

namespace naim::controller {

class ControllerSchedulerServiceFactory final : public ISchedulerServiceFactory {
 public:
  SchedulerService CreateSchedulerService(
      const std::string& db_path,
      const std::string& artifacts_root) const override;
};

}  // namespace naim::controller
