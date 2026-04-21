#include "app/controller_component_factory_support.h"

#include "app/controller_component_defaults.h"
#include "app/controller_http_component_factory.h"
#include "app/controller_plane_component_factory.h"
#include "app/controller_scheduler_component_factory.h"
#include "app/controller_read_model_component_factory.h"
#include "app/controller_composition_support.h"
#include "app/controller_scheduler_service_builder.h"
#include "app/controller_main_includes.h"

namespace naim::controller {

SchedulerService BuildControllerSchedulerService(
    const std::string& db_path,
    const std::string& artifacts_root) {
  return ControllerSchedulerComponentFactory().CreateSchedulerService(
      db_path, artifacts_root);
}

}  // namespace naim::controller

namespace naim::controller::component_factory_support {

using nlohmann::json;

std::unique_ptr<BundleCliService> CreateBundleCliService() {
  return std::make_unique<BundleCliService>(
      ControllerPlaneComponentFactory().CreateBundleCliService());
}

std::unique_ptr<IReadModelCliService> CreateReadModelCliService() {
  return std::make_unique<ReadModelCliService>(
      ControllerReadModelComponentFactory().CreateReadModelCliService());
}

std::unique_ptr<IHostRegistryService> CreateHostRegistryService(const std::string& db_path) {
  return std::make_unique<HostRegistryService>(
      ControllerPlaneComponentFactory().CreateHostRegistryService(db_path));
}

std::unique_ptr<IPlaneService> CreatePlaneService(const std::string& db_path) {
  return std::make_unique<PlaneService>(
      ControllerPlaneComponentFactory().CreatePlaneService(db_path));
}

std::unique_ptr<IAssignmentOrchestrationService> CreateAssignmentOrchestrationService() {
  return std::make_unique<AssignmentOrchestrationService>(
      ControllerPlaneComponentFactory().CreateAssignmentOrchestrationService());
}

std::unique_ptr<ISchedulerService> CreateSchedulerService(
    const std::string& db_path,
    const std::string& artifacts_root) {
  return std::make_unique<SchedulerService>(
      BuildControllerSchedulerService(db_path, artifacts_root));
}

std::unique_ptr<IWebUiService> CreateWebUiService(const std::string& db_path) {
  return std::make_unique<WebUiService>(
      db_path,
      [](naim::ControllerStore& store,
          const std::string& event_type,
          const std::string& message,
          const json& payload) {
        composition_support::AppendControllerEvent(
            store, "web-ui", event_type, message, payload);
      });
}

InteractionHttpService CreateInteractionHttpService() {
  return ControllerHttpComponentFactory().CreateInteractionHttpService();
}

AuthHttpService CreateAuthHttpService(AuthSupportService& auth_support) {
  return ControllerHttpComponentFactory().CreateAuthHttpService(auth_support);
}

HostdHttpService CreateHostdHttpService() {
  return ControllerHttpComponentFactory().CreateHostdHttpService();
}

ModelLibraryService CreateModelLibraryService() {
  return ControllerHttpComponentFactory().CreateModelLibraryService();
}

ModelLibraryHttpService CreateModelLibraryHttpService(
    const ModelLibraryService& model_library_service) {
  return ControllerHttpComponentFactory().CreateModelLibraryHttpService(
      model_library_service);
}

PlaneHttpService CreatePlaneHttpService() {
  return ControllerPlaneComponentFactory().CreatePlaneHttpService();
}

SkillsFactoryHttpService CreateSkillsFactoryHttpService(
    const std::optional<std::string>& upstream_target) {
  return ControllerPlaneComponentFactory().CreateSkillsFactoryHttpService(
      upstream_target);
}

ReadModelService CreateReadModelService() {
  return ControllerReadModelComponentFactory().CreateReadModelService();
}

ReadModelHttpService CreateReadModelHttpService(
    const ReadModelService& read_model_service) {
  return ControllerReadModelComponentFactory().CreateReadModelHttpService(
      read_model_service);
}

SchedulerHttpService CreateSchedulerHttpService(
    const ReadModelService& read_model_service) {
  return ControllerReadModelComponentFactory().CreateSchedulerHttpService(
      read_model_service);
}

}  // namespace naim::controller::component_factory_support
