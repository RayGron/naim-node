#pragma once

#include <filesystem>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include <nlohmann/json.hpp>

#include "infra/controller_health_service.h"
#include "infra/controller_action.h"
#include "http/controller_http_route_handler.h"
#include "http/controller_http_transport.h"
#include "http/controller_http_types.h"
#include "interaction/interaction_http_service.h"
#include "auth/auth_support_service.h"

namespace naim::controller {

class ControllerHttpRouter {
 public:
  using BuildJsonResponseFn = std::function<HttpResponse(
      int,
      const nlohmann::json&,
      const std::map<std::string, std::string>&)>;
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
    ResolveUiRequestPathFn resolve_ui_request_path;
    BuildStaticFileResponseFn build_static_file_response;
    ExecuteRetryHostAssignmentActionFn execute_retry_host_assignment_action;
  };

  ControllerHttpRouter(
      std::string db_path,
      std::string default_artifacts_root,
      std::optional<std::filesystem::path> ui_root,
      bool webgateway_routes_enabled,
      AuthSupportService& auth_support,
      InteractionHttpService& interaction_service,
      ControllerHealthService& health_service,
      std::vector<std::unique_ptr<IControllerHttpRouteHandler>> pre_auth_handlers,
      std::vector<std::unique_ptr<IControllerHttpRouteHandler>> post_auth_handlers,
      Deps deps);

  HttpResponse HandleRequest(const HttpRequest& request) const;

 private:
  static bool IsKnowledgeVaultRequest(const std::string& path);

  HttpResponse HandlePlaneInteractionRequest(
      const HttpRequest& request) const;

  std::string db_path_;
  std::string default_artifacts_root_;
  std::optional<std::filesystem::path> ui_root_;
  bool webgateway_routes_enabled_ = false;
  AuthSupportService& auth_support_;
  InteractionHttpService& interaction_service_;
  ControllerHealthService& health_service_;
  std::vector<std::unique_ptr<IControllerHttpRouteHandler>> pre_auth_handlers_;
  std::vector<std::unique_ptr<IControllerHttpRouteHandler>> post_auth_handlers_;
  Deps deps_;
};

}  // namespace naim::controller
