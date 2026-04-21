#pragma once

#include "app/controller_main_includes.h"

namespace naim::controller::http_service_support {

std::string BuildInteractionUpstreamBody(
    const PlaneInteractionResolution& resolution,
    nlohmann::json payload,
    bool force_stream,
    const ResolvedInteractionPolicy& resolved_policy,
    bool structured_output_json = false);

InteractionHttpService CreateInteractionHttpService(
    const ControllerRuntimeSupportService& runtime_support_service,
    const DesiredStatePolicyService& desired_state_policy_service,
    const InteractionRuntimeSupportService& interaction_runtime_support_service);

HostdHttpService CreateHostdHttpService();

AuthHttpService CreateAuthHttpService(AuthSupportService& auth_support);

ModelLibraryService CreateModelLibraryService(
    const ControllerRequestSupport& request_support);

ModelLibraryHttpService CreateModelLibraryHttpService(
    const ModelLibraryService& model_library_service);

}  // namespace naim::controller::http_service_support
