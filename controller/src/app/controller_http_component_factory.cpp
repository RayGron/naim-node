#include "app/controller_http_component_factory.h"

#include "app/controller_http_service_support.h"

namespace naim::controller {

InteractionHttpService ControllerHttpComponentFactory::CreateInteractionHttpService() const {
  return http_service_support::CreateInteractionHttpService(
      RuntimeSupportService(),
      DesiredStatePolicyServiceInstance(),
      InteractionRuntimeSupportServiceInstance());
}

AuthHttpService ControllerHttpComponentFactory::CreateAuthHttpService(
    AuthSupportService& auth_support) const {
  return http_service_support::CreateAuthHttpService(auth_support);
}

HostdHttpService ControllerHttpComponentFactory::CreateHostdHttpService() const {
  return http_service_support::CreateHostdHttpService();
}

ModelLibraryService ControllerHttpComponentFactory::CreateModelLibraryService() const {
  return http_service_support::CreateModelLibraryService(RequestSupport());
}

ModelLibraryHttpService ControllerHttpComponentFactory::CreateModelLibraryHttpService(
    const ModelLibraryService& model_library_service) const {
  return http_service_support::CreateModelLibraryHttpService(model_library_service);
}

const ControllerRuntimeSupportService&
ControllerHttpComponentFactory::RuntimeSupportService() const {
  static const ControllerRuntimeSupportService runtime_support_service;
  return runtime_support_service;
}

const DesiredStatePolicyService&
ControllerHttpComponentFactory::DesiredStatePolicyServiceInstance() const {
  static const DesiredStatePolicyService desired_state_policy_service;
  return desired_state_policy_service;
}

const InteractionRuntimeSupportService&
ControllerHttpComponentFactory::InteractionRuntimeSupportServiceInstance() const {
  static const InteractionRuntimeSupportService interaction_runtime_support_service;
  return interaction_runtime_support_service;
}

const ControllerRequestSupport& ControllerHttpComponentFactory::RequestSupport() const {
  static const ControllerRequestSupport request_support;
  return request_support;
}

}  // namespace naim::controller
