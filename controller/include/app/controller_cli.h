#pragma once

#include <optional>
#include <string>

#include "app/controller_command_line.h"
#include "app/controller_service_interfaces.h"

namespace naim::controller {

class ControllerCli {
 public:
  ControllerCli(
      const ControllerCommandLine& command_line,
      IHostRegistryService& host_registry_service,
      IPlaneService& plane_service,
      ISchedulerService& scheduler_service,
      IWebUiService& web_ui_service,
      IBundleCliService& bundle_cli_service,
      IReadModelCliService& read_model_cli_service,
      IAssignmentOrchestrationService& assignment_orchestration_service,
      IControllerServeService& serve_service);

  std::optional<int> TryRun() const;

 private:
  int MissingRequired(const char* option_name) const;

  const ControllerCommandLine& command_line_;
  IHostRegistryService& host_registry_service_;
  IPlaneService& plane_service_;
  ISchedulerService& scheduler_service_;
  IWebUiService& web_ui_service_;
  IBundleCliService& bundle_cli_service_;
  IReadModelCliService& read_model_cli_service_;
  IAssignmentOrchestrationService& assignment_orchestration_service_;
  IControllerServeService& serve_service_;
};

}  // namespace naim::controller
