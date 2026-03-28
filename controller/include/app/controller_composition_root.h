#pragma once

#include <memory>
#include <optional>
#include <string>

#include "app/controller_cli.h"
#include "app/controller_command_line.h"
#include "app/controller_service_interfaces.h"
#include "auth/auth_support_service.h"

namespace comet::controller {

class ControllerComponentFactory;

class ControllerCompositionRoot final : public IControllerServeService {
 public:
  ControllerCompositionRoot(std::string db_path, std::string artifacts_root);
  ~ControllerCompositionRoot();

  ControllerCompositionRoot(const ControllerCompositionRoot&) = delete;
  ControllerCompositionRoot& operator=(const ControllerCompositionRoot&) = delete;

  int Serve(
      const std::string& listen_host,
      int listen_port,
      const std::optional<std::string>& requested_ui_root) override;

 ControllerCli BuildCli(const ControllerCommandLine& cli);

 private:
  std::unique_ptr<ControllerComponentFactory> factory_;
  std::string db_path_;
  std::string artifacts_root_;
  std::unique_ptr<AuthSupportService> auth_support_;
  std::unique_ptr<IBundleCliService> bundle_cli_service_;
  std::unique_ptr<IReadModelCliService> read_model_cli_service_;
  std::unique_ptr<IHostRegistryService> host_registry_service_;
  std::unique_ptr<IPlaneService> plane_service_;
  std::unique_ptr<IAssignmentOrchestrationService> assignment_orchestration_service_;
  std::unique_ptr<ISchedulerService> scheduler_service_;
  std::unique_ptr<IWebUiService> web_ui_service_;
};

}  // namespace comet::controller
