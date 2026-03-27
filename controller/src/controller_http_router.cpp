#include "../include/controller_http_router.h"

#include <cctype>

#include "../include/controller_action.h"
#include "../include/controller_http_server_support.h"
#include "../include/interaction_service.h"
#include "comet/sqlite_store.h"

using nlohmann::json;

namespace comet::controller {
namespace {

std::string LowercaseCopy(const std::string& value) {
  std::string lowered;
  lowered.reserve(value.size());
  for (unsigned char ch : value) {
    lowered.push_back(static_cast<char>(std::tolower(ch)));
  }
  return lowered;
}

std::optional<int> FindQueryInt(
    const HttpRequest& request,
    const std::string& key) {
  const auto it = request.query_params.find(key);
  if (it == request.query_params.end() || it->second.empty()) {
    return std::nullopt;
  }
  return std::stoi(it->second);
}

}  // namespace

ControllerHttpRouter::ControllerHttpRouter(
    std::string db_path,
    std::string default_artifacts_root,
    std::optional<std::filesystem::path> ui_root,
    AuthSupportService& auth_support,
    Services services,
    Deps deps)
    : db_path_(std::move(db_path)),
      default_artifacts_root_(std::move(default_artifacts_root)),
      ui_root_(std::move(ui_root)),
      auth_support_(auth_support),
      services_(services),
      deps_(std::move(deps)) {}

HttpResponse ControllerHttpRouter::HandlePlaneInteractionRequest(
    const HttpRequest& request) const {
  const InteractionContractResponder interaction_responder;
  const std::string remainder =
      request.path.substr(std::string("/api/v1/planes/").size());
  if (remainder.empty()) {
    return deps_.build_json_response(404, json{{"status", "not_found"}}, {});
  }

  const auto interaction_status_pos = remainder.find("/interaction/status");
  if (interaction_status_pos != std::string::npos &&
      interaction_status_pos +
              std::string("/interaction/status").size() ==
          remainder.size()) {
    const std::string request_id = GenerateInteractionRequestId();
    const std::string plane_name = remainder.substr(0, interaction_status_pos);
    const auto build_standalone_error =
        [&](int status_code,
            const std::string& code,
            const std::string& message,
            bool retryable,
            const std::optional<std::string>& reason = std::nullopt,
            const std::optional<std::string>& served_model_name =
                std::nullopt,
            const std::optional<std::string>& active_model_id =
                std::nullopt,
            const json& details = json::object()) {
          return deps_.build_json_response(
              status_code,
              interaction_responder.BuildStandaloneErrorPayload(
                  request_id,
                  code,
                  message,
                  retryable,
                  plane_name,
                  reason,
                  served_model_name,
                  active_model_id,
                  details),
              BuildInteractionResponseHeaders(request_id));
        };
    if (request.method != "GET") {
      return build_standalone_error(
          405,
          "method_not_allowed",
          "interaction status endpoint accepts GET only",
          false);
    }
    try {
      PlaneInteractionResolution resolution =
          services_.interaction->ResolvePlane(db_path_, plane_name);
      if (resolution.desired_state.protected_plane) {
        comet::ControllerStore store(db_path_);
        store.Initialize();
        if (!auth_support_
                 .AuthenticateProtectedPlaneRequest(store, request, plane_name)
                 .has_value()) {
          return build_standalone_error(
              401,
              "unauthorized",
              "protected plane requires an authenticated WebAuthn session or SSH API session",
              false);
        }
      }
      json payload = resolution.status_payload;
      payload["request_id"] = request_id;
      payload["comet"] =
          BuildInteractionContractMetadata(resolution, request_id);
      return deps_.build_json_response(
          200,
          payload,
          BuildInteractionResponseHeaders(request_id));
    } catch (const std::exception& error) {
      return build_standalone_error(
          404,
          "plane_not_found",
          error.what(),
          false);
    }
  }

  const auto interaction_models_pos = remainder.find("/interaction/models");
  if (interaction_models_pos != std::string::npos &&
      interaction_models_pos +
              std::string("/interaction/models").size() ==
          remainder.size()) {
    const std::string request_id = GenerateInteractionRequestId();
    const std::string plane_name = remainder.substr(0, interaction_models_pos);
    const auto build_standalone_error =
        [&](int status_code,
            const std::string& code,
            const std::string& message,
            bool retryable,
            const std::optional<std::string>& reason = std::nullopt,
            const std::optional<std::string>& served_model_name =
                std::nullopt,
            const std::optional<std::string>& active_model_id =
                std::nullopt,
            const json& details = json::object()) {
          return deps_.build_json_response(
              status_code,
              interaction_responder.BuildStandaloneErrorPayload(
                  request_id,
                  code,
                  message,
                  retryable,
                  plane_name,
                  reason,
                  served_model_name,
                  active_model_id,
                  details),
              BuildInteractionResponseHeaders(request_id));
        };
    if (request.method != "GET") {
      return build_standalone_error(
          405,
          "method_not_allowed",
          "interaction models endpoint accepts GET only",
          false);
    }
    try {
      const PlaneInteractionResolution resolution =
          services_.interaction->ResolvePlane(db_path_, plane_name);
      if (resolution.desired_state.protected_plane) {
        comet::ControllerStore store(db_path_);
        store.Initialize();
        if (!auth_support_
                 .AuthenticateProtectedPlaneRequest(store, request, plane_name)
                 .has_value()) {
          return build_standalone_error(
              401,
              "unauthorized",
              "protected plane requires an authenticated WebAuthn session or SSH API session",
              false);
        }
      }
      return services_.interaction->ProxyJson(
          resolution,
          request_id,
          "GET",
          "/v1/models");
    } catch (const std::exception& error) {
      return build_standalone_error(
          404,
          "plane_not_found",
          error.what(),
          false);
    }
  }

  const auto interaction_chat_pos =
      remainder.find("/interaction/chat/completions");
  if (interaction_chat_pos != std::string::npos &&
      interaction_chat_pos +
              std::string("/interaction/chat/completions").size() ==
          remainder.size()) {
    const std::string request_id = GenerateInteractionRequestId();
    const std::string plane_name = remainder.substr(0, interaction_chat_pos);
    const auto build_standalone_error =
        [&](int status_code,
            const std::string& code,
            const std::string& message,
            bool retryable,
            const std::optional<std::string>& reason = std::nullopt,
            const std::optional<std::string>& served_model_name =
                std::nullopt,
            const std::optional<std::string>& active_model_id =
                std::nullopt,
            const json& details = json::object()) {
          return deps_.build_json_response(
              status_code,
              interaction_responder.BuildStandaloneErrorPayload(
                  request_id,
                  code,
                  message,
                  retryable,
                  plane_name,
                  reason,
                  served_model_name,
                  active_model_id,
                  details),
              BuildInteractionResponseHeaders(request_id));
        };
    if (request.method != "POST") {
      return build_standalone_error(
          405,
          "method_not_allowed",
          "interaction chat completions endpoint accepts POST only",
          false);
    }
    try {
      const PlaneInteractionResolution resolution =
          services_.interaction->ResolvePlane(db_path_, plane_name);
      const auto build_plane_error =
          [&](int status_code,
              const std::string& code,
              const std::string& message,
              bool retryable,
              const json& details = json::object()) {
            return deps_.build_json_response(
                status_code,
                interaction_responder.BuildPlaneErrorPayload(
                    resolution,
                    request_id,
                    code,
                    message,
                    retryable,
                    details),
                BuildInteractionResponseHeaders(request_id));
          };
      if (resolution.desired_state.protected_plane) {
        comet::ControllerStore store(db_path_);
        store.Initialize();
        if (!auth_support_
                 .AuthenticateProtectedPlaneRequest(store, request, plane_name)
                 .has_value()) {
          return build_standalone_error(
              401,
              "unauthorized",
              "protected plane requires an authenticated WebAuthn session or SSH API session",
              false);
        }
      }
      if (!resolution.status_payload.value("interaction_enabled", false)) {
        return build_plane_error(
            409,
            "interaction_disabled",
            "interaction is available only for plane_mode=llm",
            false);
      }
      if (!resolution.status_payload.value("ready", false) ||
          !resolution.target.has_value()) {
        return build_plane_error(
            409,
            "plane_not_ready",
            "plane interaction target is not ready",
            true);
      }
      const InteractionRequestValidator validator;
      InteractionRequestContext request_context;
      request_context.request_id = request_id;
      if (const auto validation_error = validator.ValidateAndNormalizeRequest(
              resolution,
              validator.ParsePayload(request.body),
              &request_context)) {
        return build_plane_error(
            validation_error->code == "model_mismatch" ? 409 : 400,
            validation_error->code,
            validation_error->message,
            validation_error->retryable,
            validation_error->details);
      }
      try {
        return services_.interaction->BuildSessionResponse(
            resolution,
            request_context,
            services_.interaction->ExecuteSession(resolution, request_context));
      } catch (const std::exception& error) {
        const std::string lowered = LowercaseCopy(error.what());
        const bool timeout_like =
            lowered.find("timed out") != std::string::npos ||
            lowered.find("timeout") != std::string::npos;
        return build_plane_error(
            timeout_like ? 504 : 502,
            timeout_like ? "upstream_timeout" : "upstream_invalid_response",
            error.what(),
            true);
      }
    } catch (const std::exception& error) {
      return build_standalone_error(
          404,
          "plane_not_found",
          error.what(),
          false);
    }
  }

  const auto interaction_stream_pos =
      remainder.find("/interaction/chat/completions/stream");
  if (interaction_stream_pos != std::string::npos &&
      interaction_stream_pos +
              std::string("/interaction/chat/completions/stream").size() ==
          remainder.size()) {
    return deps_.build_json_response(
        405,
        json{{"status", "method_not_allowed"}},
        {});
  }
  return deps_.build_json_response(404, json{{"status", "not_found"}}, {});
}

HttpResponse ControllerHttpRouter::HandleRequest(
    const HttpRequest& request) const {
  auth_support_.CleanupExpiredPendingAuthFlows();
  if (request.path == "/health" || request.path == "/api/v1/health") {
    if (request.method != "GET") {
      return deps_.build_json_response(
          405,
          json{{"status", "method_not_allowed"}},
          {});
    }
    return deps_.build_json_response(
        200,
        deps_.build_controller_health_payload(db_path_),
        {});
  }
  if (const auto auth_response =
          services_.auth->HandleRequest(db_path_, request);
      auth_response.has_value()) {
    return *auth_response;
  }
  if (const auto hostd_response =
          services_.hostd->HandleRequest(db_path_, request);
      hostd_response.has_value()) {
    return *hostd_response;
  }
  if (request.method == "GET" && ui_root_.has_value()) {
    if (const auto static_path =
            deps_.resolve_ui_request_path(*ui_root_, request.path);
        static_path.has_value()) {
      try {
        return deps_.build_static_file_response(*static_path);
      } catch (const std::exception& error) {
        return deps_.build_json_response(
            500,
            json{{"status", "internal_error"},
                 {"message", error.what()},
                 {"path", request.path}},
            {});
      }
    }
  }
  if (ControllerHttpServerSupport::StartsWithPath(request.path, "/api/v1/") &&
      request.path != "/api/v1/health" &&
      !ControllerHttpServerSupport::StartsWithPath(
          request.path,
          "/api/v1/auth/") &&
      !ControllerHttpServerSupport::StartsWithPath(
          request.path,
          "/api/v1/hostd/")) {
    const bool interaction_request =
        ControllerHttpServerSupport::StartsWithPath(
            request.path,
            "/api/v1/planes/") &&
        request.path.find("/interaction/") != std::string::npos;
    if (!interaction_request) {
      try {
        comet::ControllerStore store(db_path_);
        store.Initialize();
        if (!auth_support_
                 .AuthenticateControllerUserSession(store, request)
                 .has_value()) {
          return deps_.build_json_response(
              401,
              json{{"status", "unauthorized"},
                   {"message", "authentication required"}},
              {{"Set-Cookie",
                auth_support_.ClearSessionCookieHeader(request)}});
        }
      } catch (const std::exception& error) {
        return deps_.build_json_response(
            500,
            json{{"status", "internal_error"},
                 {"message", error.what()},
                 {"path", request.path}},
            {});
      }
    }
  }
  if (const auto bundle_response = services_.bundle->HandleRequest(
          db_path_,
          default_artifacts_root_,
          request);
      bundle_response.has_value()) {
    return *bundle_response;
  }
  if (const auto model_library_response =
          services_.model_library->HandleRequest(db_path_, request);
      model_library_response.has_value()) {
    return *model_library_response;
  }
  if (const auto plane_response = services_.plane->HandleRequest(
          db_path_,
          default_artifacts_root_,
          request);
      plane_response.has_value()) {
    return *plane_response;
  }
  if (ControllerHttpServerSupport::StartsWithPath(
          request.path,
          "/api/v1/planes/")) {
    return HandlePlaneInteractionRequest(request);
  }
  if (const auto read_model_response =
          services_.read_model->HandleRequest(db_path_, request);
      read_model_response.has_value()) {
    return *read_model_response;
  }
  if (request.path == "/api/v1/events/stream") {
    return deps_.build_json_response(
        405,
        json{{"status", "method_not_allowed"}},
        {});
  }
  if (const auto scheduler_response = services_.scheduler->HandleRequest(
          db_path_,
          default_artifacts_root_,
          request);
      scheduler_response.has_value()) {
    return *scheduler_response;
  }
  if (request.path == "/api/v1/retry-host-assignment") {
    if (request.method != "POST") {
      return deps_.build_json_response(
          405,
          json{{"status", "method_not_allowed"}},
          {});
    }
    const auto assignment_id = FindQueryInt(request, "id");
    if (!assignment_id.has_value()) {
      return deps_.build_json_response(
          400,
          json{{"status", "bad_request"},
               {"message", "missing required query parameter 'id'"}},
          {});
    }
    try {
      return deps_.build_json_response(
          200,
          BuildControllerActionPayload(
              deps_.execute_retry_host_assignment_action(
                  db_path_,
                  *assignment_id)),
          {});
    } catch (const std::exception& error) {
      return deps_.build_json_response(
          500,
          json{{"status", "internal_error"},
               {"message", error.what()},
               {"path", request.path}},
          {});
    }
  }
  return deps_.build_json_response(
      404,
      json{{"status", "not_found"},
           {"path", request.path},
           {"method", request.method}},
      {});
}

}  // namespace comet::controller
