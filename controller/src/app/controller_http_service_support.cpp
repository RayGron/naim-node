#include "app/controller_http_service_support.h"

#include "app/controller_composition_support.h"
#include "auth/auth_http_support.h"
#include "interaction/interaction_http_support.h"
#include "interaction/interaction_payload_builder.h"
#include "skills/plane_skills_service.h"

namespace comet::controller::http_service_support {

namespace {

using SocketHandle = comet::platform::SocketHandle;

}  // namespace

std::string BuildInteractionUpstreamBody(
    const PlaneInteractionResolution& resolution,
    nlohmann::json payload,
    bool force_stream,
    const ResolvedInteractionPolicy& resolved_policy,
    bool structured_output_json) {
  return BuildInteractionUpstreamBodyPayload(
      resolution, std::move(payload), force_stream, resolved_policy, structured_output_json);
}

InteractionHttpService CreateInteractionHttpService(
    const ControllerRuntimeSupportService& runtime_support_service,
    const DesiredStatePolicyService& desired_state_policy_service,
    const InteractionRuntimeSupportService& interaction_runtime_support_service) {
  return InteractionHttpService(InteractionHttpSupport(
      runtime_support_service,
      desired_state_policy_service,
      interaction_runtime_support_service));
}

HostdHttpService CreateHostdHttpService() {
  return HostdHttpService(HostdHttpSupport(
      [](comet::ControllerStore& store,
         const std::string& event_type,
         const std::string& message,
         const nlohmann::json& payload,
         const std::string& node_name,
         const std::string& severity) {
        composition_support::AppendControllerEvent(
            store,
            "host-registry",
            event_type,
            message,
            payload,
            "",
            node_name,
            "",
            std::nullopt,
            std::nullopt,
            severity);
      }));
}

AuthHttpService CreateAuthHttpService(AuthSupportService& auth_support) {
  return AuthHttpService(AuthHttpSupport(auth_support));
}

ModelLibraryService CreateModelLibraryService(
    const ControllerRequestSupport& request_support) {
  return ModelLibraryService(ModelLibrarySupport(request_support));
}

ModelLibraryHttpService CreateModelLibraryHttpService(
    const ModelLibraryService& model_library_service) {
  return ModelLibraryHttpService(ModelLibraryHttpSupport(model_library_service));
}

}  // namespace comet::controller::http_service_support
