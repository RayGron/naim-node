#include "interaction/interaction_http_service.h"

#include <sstream>

#include "auth/auth_support_service.h"
#include "browsing/interaction_browsing_service.h"
#include "interaction/interaction_conversation_service.h"
#include "skills/plane_skills_service.h"

using nlohmann::json;
using comet::controller::InteractionContractResponder;
using comet::controller::InteractionConversationPrincipal;
using comet::controller::InteractionConversationService;
using comet::controller::InteractionRequestContext;
using comet::controller::InteractionRequestValidator;
using comet::controller::PlaneInteractionResolution;
using comet::controller::ResolvedInteractionPolicy;
using comet::controller::ResolveInteractionCompletionPolicy;

InteractionHttpService::InteractionHttpService(InteractionHttpSupport support)
    : support_(std::move(support)) {}

comet::controller::PlaneInteractionResolution InteractionHttpService::ResolvePlane(
    const std::string& db_path,
    const std::string& plane_name) const {
  return MakePlaneResolver().Resolve(db_path, plane_name);
}

comet::controller::InteractionSessionResult InteractionHttpService::ExecuteSession(
    const comet::controller::PlaneInteractionResolution& resolution,
    const comet::controller::InteractionRequestContext& request_context) const {
  return MakeSessionExecutor().Execute(resolution, request_context);
}

std::optional<comet::controller::InteractionValidationError>
InteractionHttpService::ResolveRequestSkills(
    const comet::controller::PlaneInteractionResolution& resolution,
    comet::controller::InteractionRequestContext* request_context) const {
  return comet::controller::PlaneSkillsService().ResolveInteractionSkills(
      resolution, request_context);
}

std::optional<comet::controller::InteractionValidationError>
InteractionHttpService::ResolveRequestBrowsing(
    const comet::controller::PlaneInteractionResolution& resolution,
    comet::controller::InteractionRequestContext* request_context) const {
  return comet::controller::InteractionBrowsingService().ResolveInteractionBrowsing(
      resolution, request_context);
}

std::optional<comet::controller::InteractionValidationError>
InteractionHttpService::ResolveRequestContext(
    const comet::controller::PlaneInteractionResolution& resolution,
    comet::controller::InteractionRequestContext* request_context) const {
  if (const auto error = ResolveRequestSkills(resolution, request_context)) {
    return error;
  }
  return ResolveRequestBrowsing(resolution, request_context);
}

json InteractionHttpService::BuildContinuationPayload(
    const json& original_payload,
    const std::string& accumulated_text,
    const comet::controller::InteractionCompletionPolicy& policy,
    bool natural_stop_without_marker,
    int total_completion_tokens) {
  json payload = original_payload;
  json messages = json::array();
  if (payload.contains("messages") && payload.at("messages").is_array()) {
    for (const auto& message : payload.at("messages")) {
      messages.push_back(message);
    }
  }
  const std::string recent_assistant_context =
      accumulated_text.empty()
          ? std::string{}
          : comet::controller::Utf8SafeSuffix(accumulated_text, 4096);
  const std::string trailing_excerpt =
      accumulated_text.empty()
          ? std::string{}
          : comet::controller::Utf8SafeSuffix(accumulated_text, 256);
  const int remaining_completion_tokens =
      std::max(0, policy.max_total_completion_tokens - total_completion_tokens);
  if (policy.thinking_enabled) {
    if (!payload.contains("chat_template_kwargs") ||
        !payload.at("chat_template_kwargs").is_object()) {
      payload["chat_template_kwargs"] = json::object();
    }
    payload["chat_template_kwargs"]["enable_thinking"] = false;
  }
  if (!recent_assistant_context.empty()) {
    messages.push_back(
        json{{"role", "assistant"}, {"content", recent_assistant_context}});
  }
  messages.push_back(json{
      {"role", "user"},
      {"content",
       comet::controller::BuildContinuationPrompt(
           policy,
           natural_stop_without_marker,
           trailing_excerpt,
           remaining_completion_tokens,
           policy.thinking_enabled,
           !accumulated_text.empty())},
  });
  payload["messages"] = messages;
  return payload;
}

bool InteractionHttpService::SendInteractionSseEvent(
    comet::platform::SocketHandle client_fd,
    const std::string& event_name,
    const json& payload) const {
  std::ostringstream frame;
  frame << "event: " << event_name << "\n";
  std::stringstream lines(payload.dump().append("\n"));
  std::string line;
  while (std::getline(lines, line)) {
    if (!line.empty() && line.back() == '\r') {
      line.pop_back();
    }
    frame << "data: " << line << "\n";
  }
  frame << "\n";
  return support_.SendAll(client_fd, frame.str());
}

bool InteractionHttpService::SendInteractionSseDone(
    comet::platform::SocketHandle client_fd) const {
  return support_.SendAll(client_fd, "data: [DONE]\n\n");
}

comet::controller::InteractionPlaneResolver
InteractionHttpService::MakePlaneResolver() const {
  return comet::controller::InteractionPlaneResolver(
      [&](const comet::DesiredState& desired_state) {
        return support_.FindInferInstanceName(desired_state);
      },
      [&](const comet::HostObservation& observation) {
        return support_.ParseInstanceRuntimeStatuses(observation);
      },
      [&](const comet::HostObservation& observation, const std::string& plane_name) {
        return support_.ObservationMatchesPlane(observation, plane_name);
      },
      [&](const comet::DesiredState& desired_state,
          const comet::HostObservation& observation) {
        return support_.BuildPlaneScopedRuntimeStatus(desired_state, observation);
      },
      [&](const std::string& gateway_listen, int fallback_port) {
        return support_.ParseInteractionTarget(gateway_listen, fallback_port);
      },
      [&](comet::ControllerStore& store, const comet::DesiredState& desired_state) {
        return support_.CountReadyWorkerMembers(store, desired_state);
      },
      [&](const std::optional<comet::controller::ControllerEndpointTarget>& target,
          const std::string& path) {
        return support_.ProbeControllerTargetOk(target, path);
      },
      [&](const comet::DesiredState& desired_state, const std::string& node_name) {
        return support_.DescribeUnsupportedControllerLocalRuntime(desired_state, node_name);
      });
}

comet::controller::InteractionSessionExecutor
InteractionHttpService::MakeSessionExecutor() const {
  return comet::controller::InteractionSessionExecutor(
      [&](const comet::controller::PlaneInteractionResolution& resolution,
          json payload,
          bool force_stream,
          const comet::controller::ResolvedInteractionPolicy& resolved_policy,
          bool structured_output_json) {
        return support_.BuildInteractionUpstreamBody(
            resolution,
            std::move(payload),
            force_stream,
            resolved_policy,
            structured_output_json);
      },
      [&](const comet::controller::ControllerEndpointTarget& target,
          const std::string& request_id,
          const std::string& body) {
        const HttpResponse response = support_.SendControllerHttpRequest(
            target,
            "POST",
            "/v1/chat/completions",
            body,
            {{"Accept", "application/json"},
             {"X-Comet-Request-Id", request_id}});
        return comet::controller::InteractionUpstreamResponse{
            response.status_code,
            response.body,
            response.headers,
        };
      },
      [](const json& payload) {
        return comet::controller::ExtractInteractionUsage(payload);
      },
      [](const json& payload) {
        return comet::controller::ExtractInteractionText(payload);
      },
      [](const json& payload) {
        return comet::controller::ExtractInteractionFinishReason(payload);
      },
      [](const std::string& input,
         const std::string& marker,
         bool* marker_seen) {
        return comet::controller::RemoveCompletionMarkers(
            input, marker, marker_seen);
      },
      [](const comet::controller::InteractionCompletionPolicy& policy,
         int total_completion_tokens) {
        return comet::controller::SessionReachedTargetLength(
            policy, total_completion_tokens);
      },
      [](const comet::controller::InteractionCompletionPolicy& policy,
         const comet::controller::InteractionSegmentSummary& summary) {
        return comet::controller::CanCompleteOnNaturalStop(policy, summary);
      },
      [](const json& original_payload,
         const std::string& accumulated_text,
         const comet::controller::InteractionCompletionPolicy& policy,
         bool natural_stop_without_marker,
         int total_completion_tokens) {
        return BuildContinuationPayload(
            original_payload,
            accumulated_text,
            policy,
            natural_stop_without_marker,
            total_completion_tokens);
      });
}

comet::controller::InteractionStreamSegmentExecutor
InteractionHttpService::MakeStreamSegmentExecutor() const {
  return comet::controller::InteractionStreamSegmentExecutor(
      [&](const comet::controller::PlaneInteractionResolution& resolution,
          json payload,
          bool force_stream,
          const comet::controller::ResolvedInteractionPolicy& resolved_policy,
          bool structured_output_json) {
        return support_.BuildInteractionUpstreamBody(
            resolution,
            std::move(payload),
            force_stream,
            resolved_policy,
            structured_output_json);
      },
      [](const comet::controller::ControllerEndpointTarget& target,
         const std::string& request_id,
         const std::string& body) {
        return OpenInteractionStreamRequest(target, request_id, body);
      },
      [&](const comet::controller::ControllerEndpointTarget& target,
          const std::string& request_id,
          const std::string& body) {
        const HttpResponse response = support_.SendControllerHttpRequest(
            target,
            "POST",
            "/v1/chat/completions",
            body,
            {{"Accept", "application/json"},
             {"X-Comet-Request-Id", request_id}});
        return comet::controller::InteractionUpstreamResponse{
            response.status_code,
            response.body,
            response.headers,
        };
      },
      [](const std::string& text) {
        return comet::controller::StartsWithReasoningPreamble(text);
      },
      [](std::string text) {
        return comet::controller::SanitizeInteractionText(std::move(text));
      },
      [](const json& payload) {
        return comet::controller::ExtractInteractionUsage(payload);
      },
      [](const json& payload) {
        return comet::controller::ExtractInteractionText(payload);
      },
      [](const json& payload) {
        return comet::controller::ExtractInteractionFinishReason(payload);
      });
}

comet::controller::InteractionProxyExecutor
InteractionHttpService::MakeProxyExecutor() const {
  return comet::controller::InteractionProxyExecutor(
      [&](const comet::controller::PlaneInteractionResolution& resolution,
          json payload,
          bool force_stream,
          const comet::controller::ResolvedInteractionPolicy& resolved_policy,
          bool structured_output_json) {
        return support_.BuildInteractionUpstreamBody(
            resolution,
            std::move(payload),
            force_stream,
            resolved_policy,
            structured_output_json);
      },
      [&](const comet::controller::ControllerEndpointTarget& target,
          const std::string& method,
          const std::string& path,
          const std::string& body,
          const std::string& request_id) {
        const HttpResponse response = support_.SendControllerHttpRequest(
            target,
            method,
            path,
            body,
            {{"Accept", "application/json"},
             {"X-Comet-Request-Id", request_id}});
        return comet::controller::InteractionUpstreamResponse{
            response.status_code,
            response.body,
            response.headers,
        };
      },
      [&](const comet::controller::PlaneInteractionResolution& resolution,
          comet::controller::InteractionRequestContext* request_context) {
        return ResolveRequestContext(resolution, request_context);
      });
}

comet::controller::InteractionStreamRequestResolver
InteractionHttpService::MakeStreamRequestResolver() const {
  return comet::controller::InteractionStreamRequestResolver(
      [&](const std::string& db_path, const std::string& plane_name) {
        return MakePlaneResolver().Resolve(db_path, plane_name);
      },
      [&](const comet::controller::PlaneInteractionResolution& resolution,
          comet::controller::InteractionRequestContext* request_context) {
        return ResolveRequestContext(resolution, request_context);
      });
}

comet::controller::InteractionStreamSessionExecutor
InteractionHttpService::MakeStreamSessionExecutor() const {
  return comet::controller::InteractionStreamSessionExecutor(
      [](const comet::controller::InteractionCompletionPolicy& policy,
         int total_completion_tokens) {
        return comet::controller::SessionReachedTargetLength(
            policy, total_completion_tokens);
      },
      [](const comet::controller::InteractionCompletionPolicy& policy,
         const comet::controller::InteractionSegmentSummary& summary) {
        return comet::controller::CanCompleteOnNaturalStop(policy, summary);
      },
      [](const json& original_payload,
         const std::string& accumulated_text,
         const comet::controller::InteractionCompletionPolicy& policy,
         bool natural_stop_without_marker,
         int total_completion_tokens) {
        return BuildContinuationPayload(
            original_payload,
            accumulated_text,
            policy,
            natural_stop_without_marker,
            total_completion_tokens);
      });
}

HttpResponse InteractionHttpService::BuildSessionResponse(
    const comet::controller::PlaneInteractionResolution& resolution,
    const comet::controller::InteractionRequestContext& request_context,
    const comet::controller::InteractionSessionResult& result) const {
  const comet::controller::InteractionSessionPresenter presenter;
  const auto response_spec =
      presenter.BuildResponseSpec(resolution, request_context, result);
  return support_.BuildJsonResponse(
      response_spec.status_code,
      response_spec.payload,
      comet::controller::BuildInteractionResponseHeaders(
          request_context.request_id));
}

HttpResponse InteractionHttpService::ProxyJson(
    const comet::controller::PlaneInteractionResolution& resolution,
    const std::string& request_id,
    const std::string& method,
    const std::string& path,
    const std::string& body) const {
  const auto proxy_executor = MakeProxyExecutor();
  const auto result =
      proxy_executor.Execute(resolution, request_id, method, path, body);
  if (result.json_response.has_value()) {
    return support_.BuildJsonResponse(
        result.json_response->status_code,
        result.json_response->payload,
        comet::controller::BuildInteractionResponseHeaders(request_id));
  }
  HttpResponse upstream;
  upstream.status_code = result.upstream.status_code;
  upstream.body = result.upstream.body;
  upstream.headers = result.upstream.headers;
  return upstream;
}

void InteractionHttpService::StreamPlaneInteractionSse(
    comet::platform::SocketHandle client_fd,
    const std::string& db_path,
    const HttpRequest& request,
    AuthSupportService& auth_support) const {
  const std::string request_id =
      comet::controller::GenerateInteractionRequestId();
  const InteractionContractResponder responder;
  const auto build_error_response =
      [&](int status_code,
          const std::string& code,
          const std::string& message,
          bool retryable,
          const std::optional<std::string>& plane_name = std::nullopt,
          const std::optional<comet::controller::PlaneInteractionResolution>& resolution =
              std::nullopt,
          const json& details = json::object()) {
        return support_.BuildJsonResponse(
            status_code,
            resolution.has_value()
                ? responder.BuildPlaneErrorPayload(
                      *resolution, request_id, code, message, retryable, details)
                : responder.BuildStandaloneErrorPayload(
                      request_id,
                      code,
                      message,
                      retryable,
                      plane_name),
            comet::controller::BuildInteractionResponseHeaders(request_id));
      };

  const auto plane_name =
      comet::controller::ParseInteractionStreamPlaneName(request.method, request.path);
  if (!plane_name.has_value()) {
    support_.SendHttpResponse(
        client_fd,
        build_error_response(
            404,
            "plane_not_found",
            "plane not found for interaction stream path",
            false));
    support_.ShutdownAndCloseSocket(client_fd);
    return;
  }

  PlaneInteractionResolution resolution;
  InteractionRequestContext request_context;
  ResolvedInteractionPolicy resolved_policy;
  InteractionConversationPrincipal principal;
  try {
    resolution = ResolvePlane(db_path, *plane_name);
    comet::ControllerStore store(db_path);
    store.Initialize();
    const auto authenticated =
        resolution.desired_state.protected_plane
            ? auth_support.AuthenticateProtectedPlaneRequest(
                  store, request, *plane_name)
            : auth_support.AuthenticateControllerUserSession(
                  store, request, std::nullopt);
    if (resolution.desired_state.protected_plane && !authenticated.has_value()) {
      support_.SendHttpResponse(
          client_fd,
          build_error_response(
              401,
              "unauthorized",
              "protected plane requires an authenticated WebAuthn session or SSH API session",
              false,
              *plane_name));
      support_.ShutdownAndCloseSocket(client_fd);
      return;
    }
    if (authenticated.has_value()) {
      principal.owner_kind = "user";
      principal.owner_user_id = authenticated->first.id;
      principal.auth_session_kind = authenticated->second.session_kind;
      principal.authenticated = true;
    }
    if (!resolution.status_payload.value("interaction_enabled", false)) {
      support_.SendHttpResponse(
          client_fd,
          build_error_response(
              409,
              "interaction_disabled",
              "interaction is available only for plane_mode=llm",
              false,
              *plane_name,
              resolution));
      support_.ShutdownAndCloseSocket(client_fd);
      return;
    }
    if (!resolution.status_payload.value("ready", false) ||
        !resolution.target.has_value()) {
      support_.SendHttpResponse(
          client_fd,
          build_error_response(
              409,
              "plane_not_ready",
              "plane interaction target is not ready",
              true,
              *plane_name,
              resolution));
      support_.ShutdownAndCloseSocket(client_fd);
      return;
    }
    const InteractionRequestValidator validator;
    request_context.request_id = request_id;
    if (const auto validation_error = validator.ValidateAndNormalizeRequest(
            resolution,
            validator.ParsePayload(request.body),
            &request_context)) {
      support_.SendHttpResponse(
          client_fd,
          build_error_response(
              validation_error->code == "model_mismatch" ? 409 : 400,
              validation_error->code,
              validation_error->message,
              validation_error->retryable,
              *plane_name,
              resolution,
              validation_error->details));
      support_.ShutdownAndCloseSocket(client_fd);
      return;
    }
    if (const auto validation_error = InteractionConversationService().PrepareRequest(
            db_path, resolution, principal, &request_context)) {
      const int status_code =
          validation_error->code == "session_not_found"
              ? 404
              : validation_error->code == "session_delta_invalid"
                    ? 422
                    : validation_error->code == "session_restore_failed"
                          ? 500
                          : 409;
      support_.SendHttpResponse(
          client_fd,
          build_error_response(
              status_code,
              validation_error->code,
              validation_error->message,
              validation_error->retryable,
              *plane_name,
              resolution,
              validation_error->details));
      support_.ShutdownAndCloseSocket(client_fd);
      return;
    }
    if (const auto validation_error =
            ResolveRequestContext(resolution, &request_context)) {
      const int status_code =
          validation_error->code == "model_mismatch" ||
                  validation_error->code == "skills_disabled" ||
                  validation_error->code == "skills_not_ready" ||
                  validation_error->code == "session_conflict" ||
                  validation_error->code == "session_plane_mismatch"
              ? 409
              : 400;
      support_.SendHttpResponse(
          client_fd,
          build_error_response(
              status_code,
              validation_error->code,
              validation_error->message,
              validation_error->retryable,
              *plane_name,
              resolution,
              validation_error->details));
      support_.ShutdownAndCloseSocket(client_fd);
      return;
    }
    resolved_policy = ResolveInteractionCompletionPolicy(
        resolution.desired_state, request_context.payload);
  } catch (const nlohmann::json::exception& error) {
    support_.SendHttpResponse(
        client_fd,
        build_error_response(
            400,
            "malformed_request",
            error.what(),
            false,
            *plane_name));
    support_.ShutdownAndCloseSocket(client_fd);
    return;
  } catch (const std::exception& error) {
    support_.SendHttpResponse(
        client_fd,
        build_error_response(
            404,
            "plane_not_found",
            error.what(),
            false,
            *plane_name));
    support_.ShutdownAndCloseSocket(client_fd);
    return;
  }

  const std::string stream_session_id =
      request_context.conversation_session_id.empty()
          ? comet::controller::GenerateInteractionSessionId()
          : request_context.conversation_session_id;

  if (!support_.SendSseHeaders(
          client_fd,
          comet::controller::BuildInteractionResponseHeaders(request_id))) {
    support_.ShutdownAndCloseSocket(client_fd);
    return;
  }

  const auto stream_session_executor = MakeStreamSessionExecutor();
  const auto stream_segment_executor = MakeStreamSegmentExecutor();
  const auto result = stream_session_executor.Execute(
      request_id,
      stream_session_id,
      *plane_name,
      resolution,
      request_context,
      resolved_policy,
      [&](const json& payload, int segment_index) {
        return stream_segment_executor.Execute(
            resolution,
            request_context,
            resolved_policy,
            request_id,
            payload,
            segment_index,
            [&](const std::string& model, const std::string& delta) {
              return SendInteractionSseEvent(
                  client_fd,
                  "delta",
                  json{
                      {"request_id", request_id},
                      {"session_id", stream_session_id},
                      {"segment_index", segment_index},
                      {"continuation_index", segment_index},
                      {"model", model},
                      {"delta", delta},
                    });
            });
      },
      [&](const std::string& event_name, const json& payload) {
        return SendInteractionSseEvent(client_fd, event_name, payload);
      },
      [&]() { return SendInteractionSseDone(client_fd); });
  (void)InteractionConversationService().PersistResponse(
      db_path, resolution, &request_context, result);

  support_.ShutdownAndCloseSocket(client_fd);
}
