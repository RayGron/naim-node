#pragma once

#include "app/controller_main_includes.h"

namespace naim::controller::component_factory_support {

std::unique_ptr<BundleCliService> CreateBundleCliService();
std::unique_ptr<IReadModelCliService> CreateReadModelCliService();
std::unique_ptr<IHostRegistryService> CreateHostRegistryService(const std::string& db_path);
std::unique_ptr<IPlaneService> CreatePlaneService(const std::string& db_path);
std::unique_ptr<IAssignmentOrchestrationService> CreateAssignmentOrchestrationService();
std::unique_ptr<ISchedulerService> CreateSchedulerService(
    const std::string& db_path,
    const std::string& artifacts_root);
std::unique_ptr<IWebUiService> CreateWebUiService(const std::string& db_path);

InteractionHttpService CreateInteractionHttpService();
AuthHttpService CreateAuthHttpService(AuthSupportService& auth_support);
HostdHttpService CreateHostdHttpService();
ModelLibraryService CreateModelLibraryService();
ModelLibraryHttpService CreateModelLibraryHttpService(
    const ModelLibraryService& model_library_service);
PlaneHttpService CreatePlaneHttpService();
SkillsFactoryHttpService CreateSkillsFactoryHttpService(
    const std::optional<std::string>& upstream_target = std::nullopt);
ReadModelService CreateReadModelService();
ReadModelHttpService CreateReadModelHttpService(
    const ReadModelService& read_model_service);
SchedulerHttpService CreateSchedulerHttpService(
    const ReadModelService& read_model_service);

}  // namespace naim::controller::component_factory_support
