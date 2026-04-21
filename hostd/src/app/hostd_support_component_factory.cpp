#include "app/hostd_support_component_factory.h"

#include "app/hostd_app_assignment_support.h"
#include "app/hostd_app_controller_support.h"
#include "app/hostd_app_observation_support.h"

namespace naim::hostd {

std::unique_ptr<HostdAppControllerSupport> HostdSupportComponentFactory::CreateBackendSupport() const {
  return std::make_unique<HostdAppControllerSupport>();
}

std::unique_ptr<HostdAppObservationSupport> HostdSupportComponentFactory::CreateObservationSupport() const {
  return std::make_unique<HostdAppObservationSupport>();
}

std::unique_ptr<HostdAppAssignmentSupport> HostdSupportComponentFactory::CreateAssignmentSupport() const {
  return std::make_unique<HostdAppAssignmentSupport>();
}

}  // namespace naim::hostd
