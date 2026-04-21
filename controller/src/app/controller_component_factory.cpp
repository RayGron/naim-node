#include "app/controller_component_factory.h"

#include "app/controller_component_factory_support.h"

namespace naim::controller {

std::unique_ptr<BundleCliService> ControllerComponentFactory::CreateBundleCliService() const {
  return component_factory_support::CreateBundleCliService();
}

std::unique_ptr<IReadModelCliService> ControllerComponentFactory::CreateReadModelCliService()
    const {
  return component_factory_support::CreateReadModelCliService();
}

std::unique_ptr<IHostRegistryService> ControllerComponentFactory::CreateHostRegistryService(
    const std::string& db_path) const {
  return component_factory_support::CreateHostRegistryService(db_path);
}

std::unique_ptr<IPlaneService> ControllerComponentFactory::CreatePlaneService(
    const std::string& db_path) const {
  return component_factory_support::CreatePlaneService(db_path);
}

std::unique_ptr<IAssignmentOrchestrationService>
ControllerComponentFactory::CreateAssignmentOrchestrationService() const {
  return component_factory_support::CreateAssignmentOrchestrationService();
}

std::unique_ptr<ISchedulerService> ControllerComponentFactory::CreateSchedulerService(
    const std::string& db_path,
    const std::string& artifacts_root) const {
  return component_factory_support::CreateSchedulerService(db_path, artifacts_root);
}

std::unique_ptr<IWebUiService> ControllerComponentFactory::CreateWebUiService(
    const std::string& db_path) const {
  return component_factory_support::CreateWebUiService(db_path);
}

InteractionHttpService ControllerComponentFactory::CreateInteractionHttpService() const {
  return component_factory_support::CreateInteractionHttpService();
}

AuthHttpService ControllerComponentFactory::CreateAuthHttpService(
    AuthSupportService& auth_support) const {
  return component_factory_support::CreateAuthHttpService(auth_support);
}

HostdHttpService ControllerComponentFactory::CreateHostdHttpService() const {
  return component_factory_support::CreateHostdHttpService();
}

ModelLibraryService ControllerComponentFactory::CreateModelLibraryService() const {
  return component_factory_support::CreateModelLibraryService();
}

ModelLibraryHttpService ControllerComponentFactory::CreateModelLibraryHttpService(
    const ModelLibraryService& model_library_service) const {
  return component_factory_support::CreateModelLibraryHttpService(model_library_service);
}

PlaneHttpService ControllerComponentFactory::CreatePlaneHttpService() const {
  return component_factory_support::CreatePlaneHttpService();
}

SkillsFactoryHttpService ControllerComponentFactory::CreateSkillsFactoryHttpService(
    const std::optional<std::string>& upstream_target) const {
  return component_factory_support::CreateSkillsFactoryHttpService(upstream_target);
}

ReadModelService ControllerComponentFactory::CreateReadModelService() const {
  return component_factory_support::CreateReadModelService();
}

ReadModelHttpService ControllerComponentFactory::CreateReadModelHttpService(
    const ReadModelService& read_model_service) const {
  return component_factory_support::CreateReadModelHttpService(read_model_service);
}

SchedulerHttpService ControllerComponentFactory::CreateSchedulerHttpService(
    const ReadModelService& read_model_service) const {
  return component_factory_support::CreateSchedulerHttpService(read_model_service);
}

}  // namespace naim::controller
