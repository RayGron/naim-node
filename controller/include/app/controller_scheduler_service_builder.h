#pragma once

#include <string>

#include "scheduler/scheduler_service.h"

namespace comet::controller {

SchedulerService BuildControllerSchedulerService(
    const std::string& db_path,
    const std::string& artifacts_root);

}  // namespace comet::controller
