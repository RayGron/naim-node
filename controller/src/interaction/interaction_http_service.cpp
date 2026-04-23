#include "interaction/interaction_http_service.h"

#include "auth/auth_support_service.h"
#include "browsing/interaction_browsing_service.h"
#include "interaction/interaction_conversation_service.h"
#include "interaction/interaction_http_executor_factory.h"
#include "interaction/interaction_request_contract_support.h"
#include "interaction/interaction_request_identity_support.h"
#include "interaction/interaction_sse_frame_builder.h"
#include "knowledge/knowledge_vault_service.h"
#include "skills/plane_skills_service.h"

using nlohmann::json;
using naim::controller::InteractionConversationService;
using naim::controller::InteractionRequestContext;
using naim::controller::PlaneInteractionResolution;
using naim::controller::ResolvedInteractionPolicy;

namespace {

constexpr const char* kKnowledgeSystemInstructionPayloadKey =
    "__naim_knowledge_system_instruction";
constexpr const char* kKnowledgeContextPayloadKey = "__naim_knowledge_context";

std::string LatestUserQuery(const nlohmann::json& payload) {
  if (!payload.contains("messages") || !payload.at("messages").is_array()) {
    return {};
  }
  for (auto it = payload.at("messages").rbegin(); it != payload.at("messages").rend(); ++it) {
    if (!it->is_object() || it->value("role", std::string{}) != "user") {
      continue;
    }
    if (!it->contains("content")) {
      continue;
    }
    if (it->at("content").is_string()) {
      return it->at("content").get<std::string>();
    }
    return it->at("content").dump();
  }
  return {};
}

std::string BuildKnowledgeInstruction(const nlohmann::json& context_payload) {
  const auto context = context_payload.value("context", nlohmann::json::array());
  if (!context.is_array() || context.empty()) {
    return {};
  }
  std::string instruction =
      "Knowledge Base context: use the following canonical knowledge snippets when "
      "they are relevant. Preserve provenance and do not invent facts outside this context.";
  int index = 1;
  for (const auto& item : context) {
    if (!item.is_object()) {
      continue;
    }
    instruction += "\n\n[" + std::to_string(index) + "] ";
    instruction += item.value("title", item.value("knowledge_id", std::string("knowledge")));
    instruction += "\nknowledge_id: " + item.value("knowledge_id", std::string{});
    instruction += "\nblock_id: " + item.value("block_id", std::string{});
    instruction += "\ntext: " + item.value("text", std::string{});
    ++index;
  }
  return instruction;
}

std::map<std::string, std::string> BuildCompressionHeaders(
    const naim::controller::InteractionSessionResult& result) {
  return {
      {"x-naim-context-compression-enabled",
       result.context_compression_enabled ? "true" : "false"},
      {"x-naim-context-compression-status", result.context_compression_status},
      {"x-naim-dialog-estimate-before",
       std::to_string(result.dialog_estimate_before)},
      {"x-naim-dialog-estimate-after",
       std::to_string(result.dialog_estimate_after)},
      {"x-naim-context-compression-ratio",
       std::to_string(result.context_compression_ratio)},
  };
}

}  // namespace

InteractionHttpService::InteractionHttpService(InteractionHttpSupport support)
    : support_(std::move(support)) {}

naim::controller::PlaneInteractionResolution InteractionHttpService::ResolvePlane(
    const std::string& db_path,
    const std::string& plane_name) const {
  return naim::controller::InteractionHttpExecutorFactory(support_)
      .MakePlaneResolver()
      .Resolve(db_path, plane_name);
}

naim::controller::InteractionSessionResult InteractionHttpService::ExecuteSession(
    const naim::controller::PlaneInteractionResolution& resolution,
    const naim::controller::InteractionRequestContext& request_context) const {
  return naim::controller::InteractionHttpExecutorFactory(support_)
      .MakeSessionExecutor()
      .Execute(resolution, request_context);
}

std::optional<naim::controller::InteractionValidationError>
InteractionHttpService::ResolveRequestSkills(
    const naim::controller::PlaneInteractionResolution& resolution,
    naim::controller::InteractionRequestContext* request_context) const {
  return naim::controller::PlaneSkillsService().ResolveInteractionSkills(
      resolution, request_context);
}

std::optional<naim::controller::InteractionValidationError>
InteractionHttpService::ResolveRequestBrowsing(
    const naim::controller::PlaneInteractionResolution& resolution,
    naim::controller::InteractionRequestContext* request_context) const {
  return naim::controller::InteractionBrowsingService().ResolveInteractionBrowsing(
      resolution, request_context);
}

std::optional<naim::controller::InteractionValidationError>
InteractionHttpService::ResolveRequestKnowledge(
    const naim::controller::PlaneInteractionResolution& resolution,
    naim::controller::InteractionRequestContext* request_context) const {
  if (!request_context || !resolution.desired_state.knowledge.has_value() ||
      !resolution.desired_state.knowledge->enabled) {
    return std::nullopt;
  }

  const auto& knowledge = *resolution.desired_state.knowledge;
  nlohmann::json body = {
      {"plane_id", resolution.desired_state.plane_name},
      {"query", LatestUserQuery(request_context->payload)},
      {"request_id", request_context->request_id},
      {"token_budget", knowledge.context_policy.token_budget},
      {"include_graph", knowledge.context_policy.include_graph},
      {"max_graph_depth", knowledge.context_policy.max_graph_depth},
      {"selected_knowledge_ids", knowledge.selected_knowledge_ids},
  };
  HttpRequest proxy_request;
  proxy_request.method = "POST";
  proxy_request.path = "/api/v1/knowledge-vault/context";
  proxy_request.headers["Content-Type"] = "application/json";
  proxy_request.body = body.dump();
  const HttpResponse response =
      naim::controller::KnowledgeVaultService().ProxyServiceRequest(
          resolution.db_path,
          proxy_request,
          "/v1/context");
  const auto parsed = nlohmann::json::parse(response.body, nullptr, false);
  if (response.status_code < 200 || response.status_code >= 300 || parsed.is_discarded()) {
    request_context->payload["__naim_knowledge_warning"] = nlohmann::json{
        {"status_code", response.status_code},
        {"message", parsed.is_discarded() ? response.body
                                          : parsed.value("message", std::string("knowledge context unavailable"))},
    };
    return std::nullopt;
  }
  request_context->payload[kKnowledgeContextPayloadKey] = parsed;
  const std::string instruction = BuildKnowledgeInstruction(parsed);
  if (!instruction.empty()) {
    request_context->payload[kKnowledgeSystemInstructionPayloadKey] = instruction;
  }
  return std::nullopt;
}

std::optional<naim::controller::InteractionValidationError>
InteractionHttpService::ResolveRequestContext(
    const naim::controller::PlaneInteractionResolution& resolution,
    naim::controller::InteractionRequestContext* request_context) const {
  if (const auto error = ResolveRequestSkills(resolution, request_context)) {
    return error;
  }
  if (const auto error = ResolveRequestBrowsing(resolution, request_context)) {
    return error;
  }
  return ResolveRequestKnowledge(resolution, request_context);
}

HttpResponse InteractionHttpService::BuildSessionResponse(
    const naim::controller::PlaneInteractionResolution& resolution,
    const naim::controller::InteractionRequestContext& request_context,
    const naim::controller::InteractionSessionResult& result) const {
  const naim::controller::InteractionSessionPresenter presenter;
  naim::controller::InteractionSessionResult reviewed_result = result;
  naim::controller::InteractionBrowsingService().ReviewInteractionResponse(
      resolution,
      request_context,
      &reviewed_result);
  const auto response_spec =
      presenter.BuildResponseSpec(resolution, request_context, reviewed_result);
  auto headers =
      naim::controller::InteractionRequestContractSupport{}
          .BuildInteractionResponseHeaders(request_context.request_id);
  for (const auto& [name, value] : BuildCompressionHeaders(reviewed_result)) {
    headers[name] = value;
  }
  return support_.BuildJsonResponse(
      response_spec.status_code,
      response_spec.payload,
      headers);
}

HttpResponse InteractionHttpService::ProxyJson(
    const naim::controller::PlaneInteractionResolution& resolution,
    const std::string& request_id,
    const std::string& method,
    const std::string& path,
    const std::string& body) const {
  const auto proxy_executor =
      naim::controller::InteractionHttpExecutorFactory(support_)
          .MakeProxyExecutor(
              [&](const naim::controller::PlaneInteractionResolution& candidate,
                  naim::controller::InteractionRequestContext* request_context) {
                return ResolveRequestContext(candidate, request_context);
              });
  const auto result =
      proxy_executor.Execute(resolution, request_id, method, path, body);
  if (result.json_response.has_value()) {
    return support_.BuildJsonResponse(
        result.json_response->status_code,
        result.json_response->payload,
        naim::controller::InteractionRequestContractSupport{}
            .BuildInteractionResponseHeaders(request_id));
  }
  HttpResponse upstream;
  upstream.status_code = result.upstream.status_code;
  upstream.body = result.upstream.body;
  upstream.headers = result.upstream.headers;
  return upstream;
}

void InteractionHttpService::StreamPlaneInteractionSse(
    naim::platform::SocketHandle client_fd,
    const std::string& db_path,
    const HttpRequest& request,
    AuthSupportService& auth_support) const {
  const std::string request_id =
      naim::controller::InteractionRequestIdentitySupport{}.GenerateRequestId();
  const auto executor_factory =
      naim::controller::InteractionHttpExecutorFactory(support_);
  const auto setup_result = executor_factory
      .MakeStreamRequestPreparationService(
          [&](const naim::controller::PlaneInteractionResolution& resolution,
              naim::controller::InteractionRequestContext* request_context) {
            return ResolveRequestContext(resolution, request_context);
          })
      .Prepare(
      db_path, request, request_id, auth_support);
  if (setup_result.error_response.has_value()) {
    support_.SendHttpResponse(client_fd, *setup_result.error_response);
    support_.ShutdownAndCloseSocket(client_fd);
    return;
  }

  const std::string plane_name = setup_result.setup->plane_name;
  PlaneInteractionResolution resolution = std::move(setup_result.setup->resolution);
  InteractionRequestContext request_context =
      std::move(setup_result.setup->request_context);
  ResolvedInteractionPolicy resolved_policy =
      std::move(setup_result.setup->resolved_policy);

  const std::string stream_session_id =
      request_context.conversation_session_id.empty()
          ? naim::controller::InteractionRequestIdentitySupport{}
                .GenerateSessionId()
          : request_context.conversation_session_id;

  if (!support_.SendSseHeaders(
          client_fd,
          naim::controller::InteractionRequestContractSupport{}
              .BuildInteractionResponseHeaders(request_id))) {
    support_.ShutdownAndCloseSocket(client_fd);
    return;
  }

  const auto stream_session_executor =
      executor_factory.MakeStreamSessionExecutor();
  const auto stream_segment_executor =
      executor_factory.MakeStreamSegmentExecutor();
  const naim::controller::InteractionSseFrameBuilder sse_frame_builder;
  const auto result = stream_session_executor.Execute(
      request_id,
      stream_session_id,
      plane_name,
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
              return support_.SendAll(
                  client_fd,
                  sse_frame_builder.BuildEventFrame(
                      "delta",
                      json{
                          {"request_id", request_id},
                          {"session_id", stream_session_id},
                          {"segment_index", segment_index},
                          {"continuation_index", segment_index},
                          {"model", model},
                          {"delta", delta},
                      }));
            });
      },
      [&](const std::string& event_name, const json& payload) {
        return support_.SendAll(
            client_fd,
            sse_frame_builder.BuildEventFrame(event_name, payload));
      },
      [&]() { return support_.SendAll(client_fd, sse_frame_builder.BuildDoneFrame()); });
  (void)InteractionConversationService().PersistResponse(
      db_path, resolution, &request_context, result);

  support_.ShutdownAndCloseSocket(client_fd);
}
