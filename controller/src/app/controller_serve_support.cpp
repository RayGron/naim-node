#include "app/controller_serve_support.h"

#include "app/controller_composition_support.h"
#include "app/controller_request_context.h"
#include "infra/controller_network_manager.h"
#include "interaction/interaction_request_contract_support.h"

#include <algorithm>
#include <cctype>
#include <cstdlib>
#include <cstdio>

namespace naim::controller::serve_support {

namespace {

using nlohmann::json;
using SocketHandle = naim::platform::SocketHandle;

std::string LowercaseCopy(std::string value) {
  std::transform(
      value.begin(),
      value.end(),
      value.begin(),
      [](unsigned char ch) { return static_cast<char>(std::tolower(ch)); });
  return value;
}

bool IsLoopbackHost(const std::string& host) {
  const std::string lowered = LowercaseCopy(host);
  return lowered == "127.0.0.1" || lowered == "localhost" || lowered == "::1";
}

bool IsPrivateIpv4Host(const std::string& host) {
  int a = 0;
  int b = 0;
  int c = 0;
  int d = 0;
  char tail = '\0';
  if (std::sscanf(host.c_str(), "%d.%d.%d.%d%c", &a, &b, &c, &d, &tail) != 4) {
    return false;
  }
  return a == 10 || a == 127 || (a == 169 && b == 254) ||
         (a == 172 && b >= 16 && b <= 31) || (a == 192 && b == 168);
}

bool WebGatewayRoutesEnabledForListener(const std::string& listen_host) {
  if (IsLoopbackHost(listen_host) || IsPrivateIpv4Host(listen_host)) {
    return true;
  }
  if (const char* internal_host = std::getenv("NAIM_CONTROLLER_INTERNAL_HOST");
      internal_host != nullptr && internal_host[0] != '\0') {
    return listen_host == internal_host;
  }
  return false;
}

}  // namespace

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
    SkillsFactoryHttpService& skills_factory_http_service,
    ReadModelService& read_model_service,
    ReadModelHttpService& read_model_http_service,
    SchedulerHttpService& scheduler_http_service,
    IAssignmentOrchestrationService& assignment_orchestration_service) {
  ControllerUiService controller_ui_service;
  ControllerHealthService controller_health_service;

  std::vector<std::unique_ptr<IControllerHttpRouteHandler>> pre_auth_handlers;
  pre_auth_handlers.push_back(std::make_unique<AuthHttpRouteHandler>(auth_http_service));
  pre_auth_handlers.push_back(std::make_unique<HostdHttpRouteHandler>(hostd_http_service));

  std::vector<std::unique_ptr<IControllerHttpRouteHandler>> post_auth_handlers;
  post_auth_handlers.push_back(std::make_unique<BundleHttpRouteHandler>(bundle_http_service));
  post_auth_handlers.push_back(
      std::make_unique<ModelLibraryHttpRouteHandler>(model_library_http_service));
  post_auth_handlers.push_back(std::make_unique<PlaneHttpRouteHandler>(plane_http_service));
  post_auth_handlers.push_back(
      std::make_unique<SkillsFactoryHttpRouteHandler>(skills_factory_http_service));
  post_auth_handlers.push_back(std::make_unique<ReadModelHttpRouteHandler>(read_model_http_service));
  post_auth_handlers.push_back(std::make_unique<SchedulerHttpRouteHandler>(scheduler_http_service));

  const bool webgateway_routes_enabled =
      WebGatewayRoutesEnabledForListener(listen_host);

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
      webgateway_routes_enabled
          ? "/health,/api/v1/health,/api/v1/bundles/validate,/api/v1/bundles/preview,/api/v1/bundles/import,/api/v1/bundles/apply,/api/v1/model-library,/api/v1/model-library/download,/api/v1/model-library/jobs/stop,/api/v1/model-library/jobs/resume,/api/v1/model-library/jobs/hide,/api/v1/model-library/jobs[DELETE],/api/v1/model-library/skills-factory-worker,/api/v1/planes,/api/v1/planes/<plane>,/api/v1/planes/<plane>/dashboard,/api/v1/planes/<plane>/start,/api/v1/planes/<plane>/stop,/api/v1/planes/<plane>[DELETE],/api/v1/planes/<plane>/interaction/status,/api/v1/planes/<plane>/interaction/models,/api/v1/planes/<plane>/interaction/sessions,/api/v1/planes/<plane>/interaction/sessions/<session_id>,/api/v1/planes/<plane>/interaction/chat/completions,/api/v1/planes/<plane>/interaction/chat/completions/stream,/api/v1/planes/<plane>/webgateway/status,/api/v1/planes/<plane>/webgateway/resolve,/api/v1/planes/<plane>/webgateway/review-response,/api/v1/planes/<plane>/webgateway/sessions,/api/v1/planes/<plane>/skills,/api/v1/skills-factory,/api/v1/skills-factory/<skill>,/api/v1/state,/api/v1/dashboard,/api/v1/host-assignments,/api/v1/host-observations,/api/v1/host-health,/api/v1/disk-state,/api/v1/rollout-actions,/api/v1/rebalance-plan,/api/v1/events,/api/v1/events/stream,/api/v1/scheduler-tick,/api/v1/reconcile-rebalance-proposals,/api/v1/reconcile-rollout-actions,/api/v1/apply-rebalance-proposal,/api/v1/set-rollout-action-status,/api/v1/enqueue-rollout-eviction,/api/v1/apply-ready-rollout-action,/api/v1/node-availability,/api/v1/retry-host-assignment,/api/v1/hostd/hosts,/api/v1/hostd/hosts/<node>/revoke,/api/v1/hostd/hosts/<node>/rotate-key,/api/v1/hostd/hosts/<node>/reset-onboarding,/api/v1/hostd/hosts/<node>/storage-role,/api/v1/hostd/model-artifacts/chunks/request,/api/v1/hostd/model-artifacts/chunks/poll,/api/v1/hostd/model-artifacts/manifest/request,/api/v1/hostd/model-artifacts/manifest/poll"
          : "/health,/api/v1/health,/api/v1/bundles/validate,/api/v1/bundles/preview,/api/v1/bundles/import,/api/v1/bundles/apply,/api/v1/model-library,/api/v1/model-library/download,/api/v1/model-library/jobs/stop,/api/v1/model-library/jobs/resume,/api/v1/model-library/jobs/hide,/api/v1/model-library/jobs[DELETE],/api/v1/model-library/skills-factory-worker,/api/v1/planes,/api/v1/planes/<plane>,/api/v1/planes/<plane>/dashboard,/api/v1/planes/<plane>/start,/api/v1/planes/<plane>/stop,/api/v1/planes/<plane>[DELETE],/api/v1/planes/<plane>/interaction/status,/api/v1/planes/<plane>/interaction/models,/api/v1/planes/<plane>/interaction/sessions,/api/v1/planes/<plane>/interaction/sessions/<session_id>,/api/v1/planes/<plane>/interaction/chat/completions,/api/v1/planes/<plane>/interaction/chat/completions/stream,/api/v1/planes/<plane>/skills,/api/v1/skills-factory,/api/v1/skills-factory/<skill>,/api/v1/state,/api/v1/dashboard,/api/v1/host-assignments,/api/v1/host-observations,/api/v1/host-health,/api/v1/disk-state,/api/v1/rollout-actions,/api/v1/rebalance-plan,/api/v1/events,/api/v1/events/stream,/api/v1/scheduler-tick,/api/v1/reconcile-rebalance-proposals,/api/v1/reconcile-rollout-actions,/api/v1/apply-rebalance-proposal,/api/v1/set-rollout-action-status,/api/v1/enqueue-rollout-eviction,/api/v1/apply-ready-rollout-action,/api/v1/node-availability,/api/v1/retry-host-assignment,/api/v1/hostd/hosts,/api/v1/hostd/hosts/<node>/revoke,/api/v1/hostd/hosts/<node>/rotate-key,/api/v1/hostd/hosts/<node>/reset-onboarding,/api/v1/hostd/hosts/<node>/storage-role,/api/v1/hostd/model-artifacts/chunks/request,/api/v1/hostd/model-artifacts/chunks/poll,/api/v1/hostd/model-artifacts/manifest/request,/api/v1/hostd/model-artifacts/manifest/poll",
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
      "/health,/api/v1/health,/api/v1/skills-factory,/api/v1/skills-factory/<skill>",
  });
}

}  // namespace naim::controller::serve_support
