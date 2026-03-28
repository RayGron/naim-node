#pragma once

#include "app/controller_main_includes.h"

namespace comet::controller::serve_support {

int ServeControllerHttp(
    const std::string& db_path,
    const std::string& artifacts_root,
    const std::string& listen_host,
    int listen_port,
    const std::optional<std::filesystem::path>& ui_root,
    AuthSupportService& auth_support,
    AuthHttpService& auth_http_service,
    InteractionHttpService& interaction_http_service,
    HostdHttpService& hostd_http_service,
    BundleHttpService& bundle_http_service,
    ModelLibraryHttpService& model_library_http_service,
    PlaneHttpService& plane_http_service,
    ReadModelService& read_model_service,
    ReadModelHttpService& read_model_http_service,
    SchedulerHttpService& scheduler_http_service,
    IAssignmentOrchestrationService& assignment_orchestration_service);

}  // namespace comet::controller::serve_support
