#pragma once

#include "app/controller_main_includes.h"

namespace naim::controller {

class ControllerHttpComponentFactory final {
 public:
  InteractionHttpService CreateInteractionHttpService() const;
  AuthHttpService CreateAuthHttpService(AuthSupportService& auth_support) const;
  HostdHttpService CreateHostdHttpService() const;
  ModelLibraryService CreateModelLibraryService() const;
  ModelLibraryHttpService CreateModelLibraryHttpService(
      const ModelLibraryService& model_library_service) const;

 private:
  const ControllerRuntimeSupportService& RuntimeSupportService() const;
  const DesiredStatePolicyService& DesiredStatePolicyServiceInstance() const;
  const InteractionRuntimeSupportService& InteractionRuntimeSupportServiceInstance() const;
  const ControllerRequestSupport& RequestSupport() const;
};

}  // namespace naim::controller
