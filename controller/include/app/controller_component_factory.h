#pragma once

#include "app/controller_main_includes.h"

namespace comet::controller {

class ControllerComponentFactory final {
 public:
  std::unique_ptr<BundleCliService> CreateBundleCliService() const;
  std::unique_ptr<IReadModelCliService> CreateReadModelCliService() const;
  std::unique_ptr<IHostRegistryService> CreateHostRegistryService(
      const std::string& db_path) const;
  std::unique_ptr<IPlaneService> CreatePlaneService(const std::string& db_path) const;
  std::unique_ptr<IAssignmentOrchestrationService> CreateAssignmentOrchestrationService() const;
  std::unique_ptr<ISchedulerService> CreateSchedulerService(
      const std::string& db_path,
      const std::string& artifacts_root) const;
  std::unique_ptr<IWebUiService> CreateWebUiService(const std::string& db_path) const;

  InteractionHttpService CreateInteractionHttpService() const;
  AuthHttpService CreateAuthHttpService(AuthSupportService& auth_support) const;
  HostdHttpService CreateHostdHttpService() const;
  ModelLibraryService CreateModelLibraryService() const;
  ModelLibraryHttpService CreateModelLibraryHttpService(
      const ModelLibraryService& model_library_service) const;
  PlaneHttpService CreatePlaneHttpService() const;
  ReadModelService CreateReadModelService() const;
  ReadModelHttpService CreateReadModelHttpService(
      const ReadModelService& read_model_service) const;
  SchedulerHttpService CreateSchedulerHttpService(
      const ReadModelService& read_model_service) const;
};

}  // namespace comet::controller
