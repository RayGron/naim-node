#include "app/hostd_service_component_factory.h"

#include "backend/hostd_backend_factory.h"
#include "cli/hostd_cli.h"
#include "cli/hostd_cli_command_dispatcher.h"
#include "config/node_config_loader.h"
#include "observation/hostd_observation_service.h"
#include "state_apply/hostd_assignment_service.h"

namespace naim::hostd {

std::unique_ptr<HostdBackendFactory> HostdServiceComponentFactory::CreateBackendFactory(
    const IHttpHostdBackendSupport& support) const {
  return std::make_unique<HostdBackendFactory>(support);
}

std::unique_ptr<HostdObservationService> HostdServiceComponentFactory::CreateObservationService(
    const IHostdBackendFactory& backend_factory,
    const IHostdObservationSupport& support) const {
  return std::make_unique<HostdObservationService>(backend_factory, support);
}

std::unique_ptr<HostdAssignmentService> HostdServiceComponentFactory::CreateAssignmentService(
    const IHostdBackendFactory& backend_factory,
    const IHostdAssignmentSupport& support,
    const HostdObservationService& observation_service) const {
  return std::make_unique<HostdAssignmentService>(backend_factory, support, observation_service);
}

std::unique_ptr<HostdCli> HostdServiceComponentFactory::CreateCli(
    const HostdAssignmentService& assignment_service,
    const HostdObservationService& observation_service) const {
  return std::make_unique<HostdCli>(assignment_service, observation_service);
}

std::unique_ptr<NodeConfigLoader> HostdServiceComponentFactory::CreateConfigLoader() const {
  return std::make_unique<NodeConfigLoader>();
}

}  // namespace naim::hostd
