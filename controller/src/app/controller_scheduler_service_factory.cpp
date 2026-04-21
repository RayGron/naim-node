#include "app/controller_scheduler_service_factory.h"

#include "app/controller_scheduler_service_builder.h"

namespace naim::controller {

SchedulerService ControllerSchedulerServiceFactory::CreateSchedulerService(
    const std::string& db_path,
    const std::string& artifacts_root) const {
  return BuildControllerSchedulerService(db_path, artifacts_root);
}

}  // namespace naim::controller
