#pragma once

#include <string>

#include "scheduler/scheduler_service.h"

namespace naim::controller {

SchedulerService BuildControllerSchedulerService(
    const std::string& db_path,
    const std::string& artifacts_root);

}  // namespace naim::controller
