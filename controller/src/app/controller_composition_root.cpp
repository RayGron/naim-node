#include "app/controller_composition_root.h"

#include "app/controller_component_factory.h"
#include "app/controller_component_factory_support.h"
#include "app/controller_main_includes.h"
#include "app/controller_read_model_support.h"
#include "app/controller_serve_support.h"

namespace comet::controller {

ControllerCompositionRoot::ControllerCompositionRoot(
    std::string db_path,
    std::string artifacts_root)
    : factory_(std::make_unique<ControllerComponentFactory>()),
      db_path_(std::move(db_path)),
      artifacts_root_(std::move(artifacts_root)),
      auth_support_(std::make_unique<AuthSupportService>()),
      bundle_cli_service_(factory_->CreateBundleCliService()),
      read_model_cli_service_(factory_->CreateReadModelCliService()),
      host_registry_service_(factory_->CreateHostRegistryService(db_path_)),
      plane_service_(factory_->CreatePlaneService(db_path_)),
      assignment_orchestration_service_(factory_->CreateAssignmentOrchestrationService()),
      scheduler_service_(factory_->CreateSchedulerService(db_path_, artifacts_root_)),
      web_ui_service_(factory_->CreateWebUiService(db_path_)) {}

ControllerCompositionRoot::~ControllerCompositionRoot() = default;

int ControllerCompositionRoot::Serve(
    const std::string& listen_host,
    int listen_port,
    const std::optional<std::string>& requested_ui_root) {
  std::optional<std::filesystem::path> ui_root;
  if (requested_ui_root.has_value()) {
    ui_root = std::filesystem::path(*requested_ui_root);
  } else {
    ControllerUiService controller_ui_service;
    const std::filesystem::path default_ui_root = controller_ui_service.DefaultUiRoot();
    if (std::filesystem::exists(default_ui_root)) {
      ui_root = default_ui_root;
    }
  }

  auto interaction_http_service = factory_->CreateInteractionHttpService();
  auto auth_http_service = factory_->CreateAuthHttpService(*auth_support_);
  auto hostd_http_service = factory_->CreateHostdHttpService();
  auto bundle_http_service =
      read_model_support::CreateBundleHttpService(*bundle_cli_service_);
  auto model_library_service = factory_->CreateModelLibraryService();
  auto model_library_http_service =
      factory_->CreateModelLibraryHttpService(model_library_service);
  auto plane_http_service = factory_->CreatePlaneHttpService();
  auto read_model_service = factory_->CreateReadModelService();
  auto read_model_http_service =
      factory_->CreateReadModelHttpService(read_model_service);
  auto scheduler_http_service =
      factory_->CreateSchedulerHttpService(read_model_service);

  return serve_support::ServeControllerHttp(
      db_path_,
      artifacts_root_,
      listen_host,
      listen_port,
      ui_root,
      *auth_support_,
      auth_http_service,
      interaction_http_service,
      hostd_http_service,
      bundle_http_service,
      model_library_http_service,
      plane_http_service,
      read_model_service,
      read_model_http_service,
      scheduler_http_service,
      *assignment_orchestration_service_);
}

ControllerCli ControllerCompositionRoot::BuildCli(const ControllerCommandLine& cli) {
  return ControllerCli(
      cli,
      *host_registry_service_,
      *plane_service_,
      *scheduler_service_,
      *web_ui_service_,
      *bundle_cli_service_,
      *read_model_cli_service_,
      *assignment_orchestration_service_,
      *this);
}

}  // namespace comet::controller
