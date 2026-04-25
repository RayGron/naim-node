#include "app/controller_serve_support.h"

#include "app/controller_composition_support.h"
#include "app/controller_listener_policy.h"
#include "app/controller_request_context.h"
#include "app/controller_route_summary_builder.h"
#include "infra/controller_network_manager.h"
#include "interaction/interaction_request_contract_support.h"

namespace naim::controller::serve_support {

using nlohmann::json;
using SocketHandle = naim::platform::SocketHandle;

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
    KnowledgeVaultHttpService& knowledge_vault_http_service,
    PlaneHttpService& plane_http_service,
    SkillsFactoryHttpService& skills_factory_http_service,
    ReadModelService& read_model_service,
    ReadModelHttpService& read_model_http_service,
    SchedulerHttpService& scheduler_http_service,
    IAssignmentOrchestrationService& assignment_orchestration_service) {
  ControllerUiService controller_ui_service;
  ControllerHealthService controller_health_service;
  ProtocolRegistryService protocol_registry_service;

  std::vector<std::unique_ptr<IControllerHttpRouteHandler>> pre_auth_handlers;
  pre_auth_handlers.push_back(std::make_unique<AuthHttpRouteHandler>(auth_http_service));
  pre_auth_handlers.push_back(std::make_unique<HostdHttpRouteHandler>(hostd_http_service));

  std::vector<std::unique_ptr<IControllerHttpRouteHandler>> post_auth_handlers;
  post_auth_handlers.push_back(std::make_unique<BundleHttpRouteHandler>(bundle_http_service));
  post_auth_handlers.push_back(
      std::make_unique<ModelLibraryHttpRouteHandler>(model_library_http_service));
  post_auth_handlers.push_back(
      std::make_unique<KnowledgeVaultHttpRouteHandler>(knowledge_vault_http_service));
  post_auth_handlers.push_back(std::make_unique<PlaneHttpRouteHandler>(plane_http_service));
  post_auth_handlers.push_back(
      std::make_unique<SkillsFactoryHttpRouteHandler>(skills_factory_http_service));
  post_auth_handlers.push_back(std::make_unique<ReadModelHttpRouteHandler>(read_model_http_service));
  post_auth_handlers.push_back(std::make_unique<SchedulerHttpRouteHandler>(scheduler_http_service));
  post_auth_handlers.push_back(
      std::make_unique<ProtocolRegistryHttpRouteHandler>(protocol_registry_service));

  const ControllerListenerPolicy listener_policy;
  const bool webgateway_routes_enabled =
      listener_policy.WebGatewayRoutesEnabledForListener(listen_host);
  const ControllerRouteSummaryBuilder route_summary_builder;

  ControllerHttpRouter router(
      db_path,
      artifacts_root,
      ui_root,
      webgateway_routes_enabled,
      auth_support,
      interaction_http_service,
      controller_health_service,
      std::move(pre_auth_handlers),
      std::move(post_auth_handlers),
      {
          [&](int status_code,
              const json& payload,
              const std::map<std::string, std::string>& headers) {
            return composition_support::BuildJsonResponse(status_code, payload, headers);
          },
          [&](const std::filesystem::path& root, const std::string& request_path) {
            return controller_ui_service.ResolveRequestPath(root, request_path);
          },
          [&](const std::filesystem::path& file_path) {
            return controller_ui_service.BuildStaticFileResponse(file_path);
          },
          [&](const std::string& action_db_path, int assignment_id) {
            return assignment_orchestration_service.ExecuteRetryHostAssignmentAction(
                action_db_path,
                assignment_id);
          },
      });

  ControllerHttpServer server({
      [&](const HttpRequest& request) {
        const ControllerRequestContext::Scope scoped_request(request);
        return router.HandleRequest(request);
      },
      [&](SocketHandle client_fd,
         const std::string& interaction_db_path,
         const HttpRequest& request) {
        interaction_http_service.StreamPlaneInteractionSse(
            client_fd,
            interaction_db_path,
            request,
            auth_support);
      },
      [](const std::string& method, const std::string& path) {
        return InteractionRequestContractSupport{}.ParseInteractionStreamPlaneName(
            method, path);
      },
      [&](const naim::EventRecord& event) {
        return read_model_service.BuildEventPayloadItem(event);
      },
  });

  return server.Serve({
      db_path,
      artifacts_root,
      listen_host,
      listen_port,
      ui_root,
      route_summary_builder.BuildControllerRoutesSummary(webgateway_routes_enabled),
  });
}

int ServeSkillsFactoryHttp(
    const std::string& db_path,
    const std::string& artifacts_root,
    const std::string& listen_host,
    int listen_port,
    SkillsFactoryHttpService& skills_factory_http_service) {
  ControllerHttpServer server({
      [&](const HttpRequest& request) {
        const ControllerRequestContext::Scope scoped_request(request);
        if (request.path == "/health" || request.path == "/api/v1/health") {
          return composition_support::BuildJsonResponse(
              200,
              json{{"service", "naim-skills-factory"}, {"status", "ok"}},
              {});
        }
        if (const auto response =
                skills_factory_http_service.HandleRequest(db_path, artifacts_root, request);
            response.has_value()) {
          return *response;
        }
        return composition_support::BuildJsonResponse(404, json{{"status", "not_found"}}, {});
      },
      [](SocketHandle client_fd, const std::string&, const HttpRequest&) {
        ControllerNetworkManager::ShutdownAndCloseSocket(client_fd);
      },
      [](const std::string&, const std::string&) -> std::optional<std::string> {
        return std::nullopt;
      },
      [](const naim::EventRecord&) { return json::object(); },
  });

  return server.Serve({
      db_path,
      artifacts_root,
      listen_host,
      listen_port,
      std::nullopt,
      ControllerRouteSummaryBuilder{}.BuildSkillsFactoryRoutesSummary(),
  });
}

}  // namespace naim::controller::serve_support
