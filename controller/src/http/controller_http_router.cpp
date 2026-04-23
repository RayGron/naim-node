#include "http/controller_http_router.h"

#include <cctype>

#include "infra/controller_action.h"
#include "http/controller_http_server_support.h"
#include "interaction/interaction_context_compression_service.h"
#include "interaction/interaction_conversation_service.h"
#include "interaction/interaction_request_contract_support.h"
#include "interaction/interaction_request_identity_support.h"
#include "interaction/interaction_service.h"
#include "naim/state/sqlite_store.h"

using nlohmann::json;

namespace naim::controller {
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

bool StartsWithPlanesApiPath(const std::string& path) {
  return ControllerHttpServerSupport::StartsWithPath(path, "/api/v1/planes/");
}

std::optional<std::string> ExtractPlaneFeatureRequestName(
    const std::string& path,
    const std::string& feature_suffix) {
  if (!StartsWithPlanesApiPath(path)) {
    return std::nullopt;
  }
  const std::string remainder = path.substr(std::string("/api/v1/planes/").size());
  const auto feature_pos = remainder.find(feature_suffix);
  if (feature_pos == std::string::npos) {
    return std::nullopt;
  }
  const std::size_t suffix_end = feature_pos + feature_suffix.size();
  if (suffix_end < remainder.size() && remainder.at(suffix_end) != '/') {
    return std::nullopt;
  }
  if (feature_pos == 0) {
    return std::nullopt;
  }
  return remainder.substr(0, feature_pos);
}

bool IsPlaneInteractionRequest(const std::string& path) {
  return StartsWithPlanesApiPath(path) &&
         path.find("/interaction/") != std::string::npos;
}

bool IsPlaneSkillsRequest(const std::string& path) {
  return ExtractPlaneFeatureRequestName(path, "/skills").has_value();
}

bool IsPlaneBrowsingRequest(const std::string& path) {
  return ExtractPlaneFeatureRequestName(path, "/webgateway").has_value();
}

bool IsHostdStorageRoleRequest(const std::string& path) {
  if (!ControllerHttpServerSupport::StartsWithPath(
          path,
          "/api/v1/hostd/hosts/")) {
    return false;
  }
  const auto suffix_pos = path.rfind("/storage-role");
  return suffix_pos != std::string::npos &&
         suffix_pos + std::string("/storage-role").size() == path.size();
}

int InteractionErrorStatusCode(const InteractionValidationError& error) {
  if (error.code == "model_mismatch" ||
      error.code == "skills_disabled" ||
      error.code == "skills_not_ready" ||
      error.code == "session_conflict" ||
      error.code == "session_plane_mismatch") {
    return 409;
  }
  if (error.code == "session_not_found") {
    return 404;
  }
  if (error.code == "session_delta_invalid") {
    return 422;
  }
  if (error.code == "session_restore_failed") {
    return 500;
  }
  return 400;
}

}  // namespace

bool ControllerHttpRouter::IsKnowledgeVaultRequest(const std::string& path) {
  return path == "/api/v1/knowledge-vault/status" ||
         ControllerHttpServerSupport::StartsWithPath(
             path,
             "/api/v1/knowledge-vault/");
}

ControllerHttpRouter::ControllerHttpRouter(
    std::string db_path,
    std::string default_artifacts_root,
    std::optional<std::filesystem::path> ui_root,
    bool webgateway_routes_enabled,
    AuthSupportService& auth_support,
    InteractionHttpService& interaction_service,
    ControllerHealthService& health_service,
    std::vector<std::unique_ptr<IControllerHttpRouteHandler>> pre_auth_handlers,
    std::vector<std::unique_ptr<IControllerHttpRouteHandler>> post_auth_handlers,
    Deps deps)
    : db_path_(std::move(db_path)),
      default_artifacts_root_(std::move(default_artifacts_root)),
      ui_root_(std::move(ui_root)),
      webgateway_routes_enabled_(webgateway_routes_enabled),
      auth_support_(auth_support),
      interaction_service_(interaction_service),
      health_service_(health_service),
      pre_auth_handlers_(std::move(pre_auth_handlers)),
      post_auth_handlers_(std::move(post_auth_handlers)),
      deps_(std::move(deps)) {}

HttpResponse ControllerHttpRouter::HandlePlaneInteractionRequest(
    const HttpRequest& request) const {
  const InteractionContractResponder interaction_responder;
  const InteractionRequestContractSupport request_contract_support;
  const InteractionRequestIdentitySupport request_identity_support;
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
    const std::string request_id = request_identity_support.GenerateRequestId();
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
              request_contract_support.BuildInteractionResponseHeaders(request_id));
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
          interaction_service_.ResolvePlane(db_path_, plane_name);
      if (resolution.desired_state.protected_plane) {
        naim::ControllerStore store(db_path_);
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
      payload["naim"] =
          request_contract_support.BuildInteractionContractMetadata(
              resolution, request_id);
      return deps_.build_json_response(
          200,
          payload,
          request_contract_support.BuildInteractionResponseHeaders(request_id));
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
    const std::string request_id = request_identity_support.GenerateRequestId();
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
              request_contract_support.BuildInteractionResponseHeaders(request_id));
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
          interaction_service_.ResolvePlane(db_path_, plane_name);
      if (resolution.desired_state.protected_plane) {
        naim::ControllerStore store(db_path_);
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
      return interaction_service_.ProxyJson(
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
  const auto interaction_sessions_pos = remainder.find("/interaction/sessions");
  if (interaction_sessions_pos != std::string::npos &&
      interaction_sessions_pos +
              std::string("/interaction/sessions").size() ==
          remainder.size()) {
    const std::string request_id = request_identity_support.GenerateRequestId();
    const std::string plane_name = remainder.substr(0, interaction_sessions_pos);
    const auto build_standalone_error =
        [&](int status_code,
            const std::string& code,
            const std::string& message,
            bool retryable) {
          return deps_.build_json_response(
              status_code,
              interaction_responder.BuildStandaloneErrorPayload(
                  request_id,
                  code,
                  message,
                  retryable,
                  plane_name),
              request_contract_support.BuildInteractionResponseHeaders(request_id));
        };
    if (request.method != "GET") {
      return build_standalone_error(
          405,
          "method_not_allowed",
          "interaction sessions endpoint accepts GET only",
          false);
    }
    try {
      const PlaneInteractionResolution resolution =
          interaction_service_.ResolvePlane(db_path_, plane_name);
      naim::ControllerStore store(db_path_);
      store.Initialize();
      const auto authenticated =
          resolution.desired_state.protected_plane
              ? auth_support_.AuthenticateProtectedPlaneRequest(
                    store, request, plane_name)
              : auth_support_.AuthenticateControllerUserSession(
                    store, request, std::nullopt);
      if (!authenticated.has_value()) {
        return build_standalone_error(
            401,
            "unauthorized",
            "authenticated user session is required",
            false);
      }
      const json payload = InteractionConversationService().BuildSessionsListPayload(
          db_path_, plane_name, authenticated->first.id);
      json response = payload;
      response["request_id"] = request_id;
      response["naim"] =
          request_contract_support.BuildInteractionContractMetadata(
              resolution, request_id);
      return deps_.build_json_response(
          200,
          response,
          request_contract_support.BuildInteractionResponseHeaders(request_id));
    } catch (const std::exception& error) {
      return build_standalone_error(
          404,
          "plane_not_found",
          error.what(),
          false);
    }
  }

  if (interaction_sessions_pos != std::string::npos &&
      interaction_sessions_pos +
              std::string("/interaction/sessions/").size() <
          remainder.size() &&
      remainder.rfind("/interaction/sessions/", interaction_sessions_pos) ==
          interaction_sessions_pos) {
    const std::string request_id = request_identity_support.GenerateRequestId();
    const std::string plane_name = remainder.substr(0, interaction_sessions_pos);
    const std::string session_id = remainder.substr(
        interaction_sessions_pos + std::string("/interaction/sessions/").size());
    const auto build_standalone_error =
        [&](int status_code,
            const std::string& code,
            const std::string& message,
            bool retryable) {
          return deps_.build_json_response(
              status_code,
              interaction_responder.BuildStandaloneErrorPayload(
                  request_id,
                  code,
                  message,
                  retryable,
                  plane_name),
              request_contract_support.BuildInteractionResponseHeaders(request_id));
        };
    if (request.method != "GET" && request.method != "DELETE") {
      return build_standalone_error(
          405,
          "method_not_allowed",
          "interaction session endpoint accepts GET and DELETE only",
          false);
    }
    try {
      const PlaneInteractionResolution resolution =
          interaction_service_.ResolvePlane(db_path_, plane_name);
      naim::ControllerStore store(db_path_);
      store.Initialize();
      const auto authenticated =
          resolution.desired_state.protected_plane
              ? auth_support_.AuthenticateProtectedPlaneRequest(
                    store, request, plane_name)
              : auth_support_.AuthenticateControllerUserSession(
                    store, request, std::nullopt);
      if (!authenticated.has_value()) {
        return build_standalone_error(
            401,
            "unauthorized",
            "authenticated user session is required",
            false);
      }
      InteractionConversationService conversation_service;
      if (request.method == "DELETE") {
        if (!conversation_service.DeleteSession(
                db_path_, plane_name, authenticated->first.id, session_id)) {
          return build_standalone_error(
              404,
              "session_not_found",
              "conversation session was not found",
              false);
        }
        return deps_.build_json_response(
            200,
            json{
                {"request_id", request_id},
                {"plane_name", plane_name},
                {"session_id", session_id},
                {"status", "deleted"},
                {"naim",
                 request_contract_support.BuildInteractionContractMetadata(
                     resolution, request_id)},
            },
            request_contract_support.BuildInteractionResponseHeaders(request_id));
      }
      const auto payload = conversation_service.BuildSessionDetailPayload(
          db_path_, plane_name, authenticated->first.id, session_id);
      if (!payload.has_value()) {
        return build_standalone_error(
            404,
            "session_not_found",
            "conversation session was not found",
            false);
      }
      json response = *payload;
      response["request_id"] = request_id;
      response["naim"] = request_contract_support.BuildInteractionContractMetadata(
          resolution, request_id, session_id);
      return deps_.build_json_response(
          200,
          response,
          request_contract_support.BuildInteractionResponseHeaders(request_id));
    } catch (const std::exception& error) {
      return build_standalone_error(
          404,
          "plane_not_found",
          error.what(),
          false);
    }
  }

  if (interaction_chat_pos != std::string::npos &&
      interaction_chat_pos +
              std::string("/interaction/chat/completions").size() ==
          remainder.size()) {
    const std::string request_id = request_identity_support.GenerateRequestId();
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
              request_contract_support.BuildInteractionResponseHeaders(request_id));
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
          interaction_service_.ResolvePlane(db_path_, plane_name);
      naim::ControllerStore store(db_path_);
      store.Initialize();
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
                request_contract_support.BuildInteractionResponseHeaders(request_id));
          };
      const auto authenticated =
          resolution.desired_state.protected_plane
              ? auth_support_.AuthenticateProtectedPlaneRequest(
                    store, request, plane_name)
              : auth_support_.AuthenticateControllerUserSession(
                    store, request, std::nullopt);
      if (resolution.desired_state.protected_plane && !authenticated.has_value()) {
          return build_standalone_error(
              401,
              "unauthorized",
              "protected plane requires an authenticated WebAuthn session or SSH API session",
              false);
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
      InteractionConversationPrincipal principal;
      if (authenticated.has_value()) {
        principal.owner_kind = "user";
        principal.owner_user_id = authenticated->first.id;
        principal.auth_session_kind = authenticated->second.session_kind;
        principal.authenticated = true;
      }
      if (const auto validation_error = InteractionConversationService().PrepareRequest(
              db_path_, resolution, principal, &request_context)) {
        return build_plane_error(
            InteractionErrorStatusCode(*validation_error),
            validation_error->code,
            validation_error->message,
            validation_error->retryable,
            validation_error->details);
      }
      if (const auto validation_error =
              interaction_service_.ResolveRequestContext(resolution, &request_context)) {
        return build_plane_error(
            InteractionErrorStatusCode(*validation_error),
            validation_error->code,
            validation_error->message,
            validation_error->retryable,
            validation_error->details);
      }
      InteractionContextCompressionService().Apply(resolution, &request_context);
      try {
        const auto result =
            interaction_service_.ExecuteSession(resolution, request_context);
        if (const auto persist_error = InteractionConversationService().PersistResponse(
                db_path_, resolution, &request_context, result)) {
          return build_plane_error(
              InteractionErrorStatusCode(*persist_error),
              persist_error->code,
              persist_error->message,
              persist_error->retryable,
              persist_error->details);
        }
        return interaction_service_.BuildSessionResponse(
            resolution,
            request_context,
            result);
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
        health_service_.BuildPayload(db_path_),
          {});
  }
  if (IsHostdStorageRoleRequest(request.path)) {
    try {
      naim::ControllerStore store(db_path_);
      store.Initialize();
      if (!auth_support_.RequireControllerAdminUser(store, request).has_value()) {
        return deps_.build_json_response(
            401,
            json{{"status", "unauthorized"},
                 {"message", "admin authentication required"}},
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
  for (const auto& handler : pre_auth_handlers_) {
    if (const auto response =
            handler->TryHandle(db_path_, default_artifacts_root_, request);
        response.has_value()) {
      return *response;
    }
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
    if (IsKnowledgeVaultRequest(request.path)) {
      for (const auto& handler : post_auth_handlers_) {
        if (const auto response =
                handler->TryHandle(db_path_, default_artifacts_root_, request);
            response.has_value()) {
          return *response;
        }
      }
    }
    const bool interaction_request = IsPlaneInteractionRequest(request.path);
    const bool skills_request = IsPlaneSkillsRequest(request.path);
    const bool browsing_request = IsPlaneBrowsingRequest(request.path);
    if (browsing_request && !webgateway_routes_enabled_) {
      return deps_.build_json_response(
          404,
          json{{"status", "not_found"},
               {"path", request.path},
               {"method", request.method}},
          {});
    }
    if (!interaction_request && !skills_request && !browsing_request) {
      try {
        naim::ControllerStore store(db_path_);
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
    } else if (skills_request || browsing_request) {
      try {
        naim::ControllerStore store(db_path_);
        store.Initialize();
        const auto plane_name = skills_request
                                    ? ExtractPlaneFeatureRequestName(request.path, "/skills")
                                    : ExtractPlaneFeatureRequestName(request.path, "/webgateway");
        if (plane_name.has_value()) {
          const auto desired_state = store.LoadDesiredState(*plane_name);
          if (desired_state.has_value() && desired_state->protected_plane &&
              !auth_support_
                   .AuthenticateProtectedPlaneRequest(
                       store,
                       request,
                       *plane_name)
                   .has_value()) {
            return deps_.build_json_response(
                401,
                json{
                    {"status", "unauthorized"},
                    {"message",
                     "protected plane requires an authenticated WebAuthn session or SSH API session"}},
                {});
          }
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
  for (const auto& handler : post_auth_handlers_) {
    if (const auto response =
            handler->TryHandle(db_path_, default_artifacts_root_, request);
        response.has_value()) {
      return *response;
    }
  }
  if (ControllerHttpServerSupport::StartsWithPath(
          request.path,
          "/api/v1/planes/")) {
    return HandlePlaneInteractionRequest(request);
  }
  if (request.path == "/api/v1/events/stream") {
    return deps_.build_json_response(
        405,
        json{{"status", "method_not_allowed"}},
        {});
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

}  // namespace naim::controller
