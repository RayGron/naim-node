#include "app/hostd_composition_root.h"

#include <memory>

#include "app/hostd_cli_actions.h"
#include "backend/default_http_hostd_backend_support.h"
#include "backend/hostd_backend_factory.h"
#include "cli/hostd_cli.h"
#include "cli/hostd_command_line.h"
#include "config/node_config_loader.h"
#include "observation/default_hostd_observation_support.h"
#include "observation/hostd_observation_service.h"
#include "state_apply/default_hostd_assignment_support.h"
#include "state_apply/hostd_assignment_service.h"

namespace comet::hostd {

class HostdCompositionRoot::Components final {
 public:
  Components()
      : backend_support(std::make_unique<DefaultHttpHostdBackendSupport>()),
        backend_factory(std::make_unique<HostdBackendFactory>(*backend_support)),
        observation_support(std::make_unique<DefaultHostdObservationSupport>()),
        observation_service(
            std::make_unique<HostdObservationService>(*backend_factory, *observation_support)),
        assignment_support(std::make_unique<DefaultHostdAssignmentSupport>()),
        assignment_service(std::make_unique<HostdAssignmentService>(
            *backend_factory,
            *assignment_support,
            *observation_service)),
        actions(std::make_unique<HostdCliActions>(*assignment_service, *observation_service)),
        cli(std::make_unique<HostdCli>(*actions)),
        config_loader(std::make_unique<NodeConfigLoader>()) {}

  std::unique_ptr<DefaultHttpHostdBackendSupport> backend_support;
  std::unique_ptr<HostdBackendFactory> backend_factory;
  std::unique_ptr<DefaultHostdObservationSupport> observation_support;
  std::unique_ptr<HostdObservationService> observation_service;
  std::unique_ptr<DefaultHostdAssignmentSupport> assignment_support;
  std::unique_ptr<HostdAssignmentService> assignment_service;
  std::unique_ptr<HostdCliActions> actions;
  std::unique_ptr<HostdCli> cli;
  std::unique_ptr<NodeConfigLoader> config_loader;
};

HostdCompositionRoot::HostdCompositionRoot() : components_(std::make_unique<Components>()) {}

HostdCompositionRoot::~HostdCompositionRoot() = default;

int HostdCompositionRoot::Run(int argc, char** argv) const {
  const HostdCommandLine command_line(argc, argv);
  return components_->cli->Run(command_line, *components_->config_loader, argv[0]);
}

}  // namespace comet::hostd
