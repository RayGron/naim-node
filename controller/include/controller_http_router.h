#pragma once

#include <filesystem>
#include <functional>
#include <optional>
#include <string>

#include <nlohmann/json.hpp>

#include "auth_http_service.h"
#include "auth_support_service.h"
#include "bundle_http_service.h"
#include "controller_http_transport.h"
#include "controller_http_types.h"
#include "hostd_http_service.h"
#include "interaction_http_service.h"
#include "model_library_http_service.h"
#include "plane_http_service.h"
#include "read_model_http_service.h"
#include "scheduler_http_service.h"

namespace comet::controller {

class ControllerHttpRouter {
 public:
  using BuildJsonResponseFn = std::function<HttpResponse(
      int,
      const nlohmann::json&,
      const std::map<std::string, std::string>&)>;
  using BuildControllerHealthPayloadFn =
      std::function<nlohmann::json(const std::string&)>;
  using ResolveUiRequestPathFn = std::function<
      std::optional<std::filesystem::path>(
          const std::filesystem::path&,
          const std::string&)>;
  using BuildStaticFileResponseFn =
      std::function<HttpResponse(const std::filesystem::path&)>;
  using ExecuteRetryHostAssignmentActionFn = std::function<
      ControllerActionResult(const std::string&, int)>;

  struct Deps {
    BuildJsonResponseFn build_json_response;
    BuildControllerHealthPayloadFn build_controller_health_payload;
    ResolveUiRequestPathFn resolve_ui_request_path;
    BuildStaticFileResponseFn build_static_file_response;
    ExecuteRetryHostAssignmentActionFn execute_retry_host_assignment_action;
  };

  struct Services {
    AuthHttpService* auth = nullptr;
    HostdHttpService* hostd = nullptr;
    BundleHttpService* bundle = nullptr;
    ModelLibraryHttpService* model_library = nullptr;
    PlaneHttpService* plane = nullptr;
    ReadModelHttpService* read_model = nullptr;
    SchedulerHttpService* scheduler = nullptr;
    InteractionHttpService* interaction = nullptr;
  };

  ControllerHttpRouter(
      std::string db_path,
      std::string default_artifacts_root,
      std::optional<std::filesystem::path> ui_root,
      AuthSupportService& auth_support,
      Services services,
      Deps deps);

  HttpResponse HandleRequest(const HttpRequest& request) const;

 private:
  HttpResponse HandlePlaneInteractionRequest(
      const HttpRequest& request) const;

  std::string db_path_;
  std::string default_artifacts_root_;
  std::optional<std::filesystem::path> ui_root_;
  AuthSupportService& auth_support_;
  Services services_;
  Deps deps_;
};

}  // namespace comet::controller
