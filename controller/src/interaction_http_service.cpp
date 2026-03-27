#include "../include/interaction_http_service.h"

#include <sstream>

using nlohmann::json;

InteractionHttpService::InteractionHttpService(Deps deps)
    : deps_(std::move(deps)) {}

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
           remaining_completion_tokens)},
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
  return deps_.send_all(client_fd, frame.str());
}

bool InteractionHttpService::SendInteractionSseDone(
    comet::platform::SocketHandle client_fd) const {
  return deps_.send_all(client_fd, "data: [DONE]\n\n");
}

comet::controller::InteractionPlaneResolver
InteractionHttpService::MakePlaneResolver() const {
  return comet::controller::InteractionPlaneResolver(
      deps_.find_infer_instance_name,
      deps_.parse_instance_runtime_statuses,
      deps_.observation_matches_plane,
      deps_.build_plane_scoped_runtime_status,
      deps_.parse_interaction_target,
      deps_.count_ready_worker_members,
      deps_.probe_controller_target_ok,
      deps_.describe_unsupported_controller_local_runtime);
}

comet::controller::InteractionSessionExecutor
InteractionHttpService::MakeSessionExecutor() const {
  return comet::controller::InteractionSessionExecutor(
      deps_.build_interaction_upstream_body,
      [&](const comet::controller::ControllerEndpointTarget& target,
          const std::string& request_id,
          const std::string& body) {
        const HttpResponse response = deps_.send_controller_http_request(
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
      deps_.build_interaction_upstream_body,
      [](const comet::controller::ControllerEndpointTarget& target,
         const std::string& request_id,
         const std::string& body) {
        return OpenInteractionStreamRequest(target, request_id, body);
      },
      [&](const comet::controller::ControllerEndpointTarget& target,
          const std::string& request_id,
          const std::string& body) {
        const HttpResponse response = deps_.send_controller_http_request(
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
      deps_.build_interaction_upstream_body,
      [&](const comet::controller::ControllerEndpointTarget& target,
          const std::string& method,
          const std::string& path,
          const std::string& body,
          const std::string& request_id) {
        const HttpResponse response = deps_.send_controller_http_request(
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
      });
}

comet::controller::InteractionStreamRequestResolver
InteractionHttpService::MakeStreamRequestResolver() const {
  return comet::controller::InteractionStreamRequestResolver(
      [&](const std::string& db_path, const std::string& plane_name) {
        return MakePlaneResolver().Resolve(db_path, plane_name);
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
  return deps_.build_json_response(
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
    return deps_.build_json_response(
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
    const HttpRequest& request) const {
  const std::string request_id =
      comet::controller::GenerateInteractionRequestId();
  const auto stream_resolution = MakeStreamRequestResolver().Resolve(
      db_path, request.method, request.path, request.body, request_id);
  if (stream_resolution.error_response.has_value()) {
    deps_.send_http_response(
        client_fd,
        deps_.build_json_response(
            stream_resolution.error_response->status_code,
            stream_resolution.error_response->payload,
            comet::controller::BuildInteractionResponseHeaders(request_id)));
    deps_.shutdown_and_close_socket(client_fd);
    return;
  }

  const auto& setup = *stream_resolution.setup;
  const std::string stream_session_id =
      comet::controller::GenerateInteractionSessionId();

  if (!deps_.send_sse_headers(
          client_fd,
          comet::controller::BuildInteractionResponseHeaders(request_id))) {
    deps_.shutdown_and_close_socket(client_fd);
    return;
  }

  const auto stream_session_executor = MakeStreamSessionExecutor();
  const auto stream_segment_executor = MakeStreamSegmentExecutor();
  stream_session_executor.Execute(
      request_id,
      stream_session_id,
      setup.plane_name,
      setup.resolution,
      setup.request_context,
      setup.resolved_policy,
      [&](const json& payload, int segment_index) {
        return stream_segment_executor.Execute(
            setup.resolution,
            setup.request_context,
            setup.resolved_policy,
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

  deps_.shutdown_and_close_socket(client_fd);
}
