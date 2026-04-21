#include "app/hostd_composition_root.h"

#include <memory>

#include "app/hostd_app_assignment_support.h"
#include "app/hostd_app_controller_support.h"
#include "app/hostd_app_observation_support.h"
#include "app/hostd_service_component_factory.h"
#include "app/hostd_support_component_factory.h"
#include "backend/hostd_backend_factory.h"
#include "cli/hostd_cli.h"
#include "cli/hostd_command_line.h"
#include "config/node_config_loader.h"
#include "observation/hostd_observation_service.h"
#include "state_apply/hostd_assignment_service.h"

namespace naim::hostd {

HostdCompositionRoot::HostdCompositionRoot()
    : support_factory_(std::make_unique<HostdSupportComponentFactory>()),
      service_factory_(std::make_unique<HostdServiceComponentFactory>()),
      backend_support_(support_factory_->CreateBackendSupport()),
      backend_factory_(service_factory_->CreateBackendFactory(*backend_support_)),
      observation_support_(support_factory_->CreateObservationSupport()),
      observation_service_(
          service_factory_->CreateObservationService(*backend_factory_, *observation_support_)),
      assignment_support_(support_factory_->CreateAssignmentSupport()),
      assignment_service_(
          service_factory_->CreateAssignmentService(
              *backend_factory_,
              *assignment_support_,
              *observation_service_)),
      cli_(service_factory_->CreateCli(*assignment_service_, *observation_service_)),
      config_loader_(service_factory_->CreateConfigLoader()) {}

HostdCompositionRoot::~HostdCompositionRoot() = default;

int HostdCompositionRoot::Run(int argc, char** argv) const {
  const HostdCommandLine command_line(argc, argv);
  return cli_->Run(command_line, *config_loader_, argv[0]);
}

}  // namespace naim::hostd
