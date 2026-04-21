#include "browsing/interaction_browsing_service.h"

#include <algorithm>
#include <cctype>
#include <stdexcept>
#include <string_view>

#include "browsing/plane_browsing_service.h"
#include "interaction/interaction_service.h"

namespace naim::controller {

namespace {

std::string ReadJsonStringOrDefault(
    const nlohmann::json& payload,
    std::string_view key,
    std::string default_value = {}) {
  const auto found = payload.find(std::string(key));
  if (found == payload.end() || found->is_null() || !found->is_string()) {
    return default_value;
  }
  return found->get<std::string>();
}

nlohmann::json ParseJsonBodyOrObject(const std::string& body) {
  if (body.empty()) {
    return nlohmann::json::object();
  }
  const nlohmann::json parsed = nlohmann::json::parse(body, nullptr, false);
  return parsed.is_discarded() ? nlohmann::json::object() : parsed;
}

std::string LowercaseAscii(std::string value) {
  std::transform(
      value.begin(),
      value.end(),
      value.begin(),
      [](unsigned char ch) { return static_cast<char>(std::tolower(ch)); });
  return value;
}

bool ContainsAnyLiteral(
    const std::string& haystack,
    const std::initializer_list<std::string_view>& needles) {
  return std::any_of(
      needles.begin(),
      needles.end(),
      [&](const std::string_view needle) {
        return haystack.find(needle) != std::string::npos;
      });
}

bool ContainsAnyAscii(
    const std::string& haystack,
    const std::initializer_list<std::string_view>& needles) {
  const std::string lowered = LowercaseAscii(haystack);
  return std::any_of(
      needles.begin(),
      needles.end(),
      [&](const std::string_view needle) {
        return lowered.find(LowercaseAscii(std::string(needle))) != std::string::npos;
      });
}

nlohmann::json BuildDisabledWebGatewayContext() {
  return nlohmann::json{
      {"mode", "disabled"},
      {"mode_source", "default_off"},
      {"plane_enabled", false},
      {"ready", false},
      {"session_backend", "broker_fallback"},
      {"rendered_browser_enabled", true},
      {"rendered_browser_ready", false},
      {"login_enabled", false},
      {"toggle_only", false},
      {"decision", "disabled"},
      {"reason", "web_mode_disabled"},
      {"lookup_state", "disabled"},
      {"lookup_attempted", false},
      {"lookup_required", false},
      {"evidence_attached", false},
      {"searches", nlohmann::json::array()},
      {"sources", nlohmann::json::array()},
      {"errors", nlohmann::json::array()},
      {"refusal", nullptr},
      {"response_policy", nlohmann::json::object()},
  };
}

nlohmann::json BuildUnavailableWebGatewayContext(
    const std::string& reason,
    const std::string& error_message) {
  nlohmann::json context = BuildDisabledWebGatewayContext();
  context["mode"] = "enabled";
  context["mode_source"] = "webgateway_unreachable";
  context["plane_enabled"] = true;
  context["decision"] = "unavailable";
  context["reason"] = reason.empty() ? "webgateway_unavailable" : reason;
  context["lookup_state"] = "required_but_unavailable";
  context["lookup_attempted"] = false;
  context["lookup_required"] = true;
  context["response_policy"] = nlohmann::json{
      {"must_disclose_web_unavailable", true},
      {"must_not_suggest_local_access", false},
      {"must_refuse_upload", false},
      {"must_use_only_evidence", false},
      {"must_not_claim_unverified_web_lookup", true},
      {"blocked_reason", nullptr},
      {"unavailable_disclaimer",
       "Web browsing was unavailable for this request, so I could not verify fresh public sources online."},
  };
  if (!error_message.empty()) {
    context["errors"] = nlohmann::json::array(
        {nlohmann::json{{"code", reason.empty() ? "webgateway_unavailable" : reason},
                        {"message", error_message}}});
  }
  return context;
}

void ApplyWebGatewayPayload(
    InteractionRequestContext* context,
    const nlohmann::json& webgateway_context,
    const nlohmann::json& response_policy,
    const std::string& model_instruction,
    const std::optional<std::string>& refusal,
    const std::string& decision) {
  if (context == nullptr) {
    return;
  }
  if (!model_instruction.empty()) {
    context->payload[InteractionBrowsingService::kSystemInstructionPayloadKey] =
        model_instruction;
  }
  context->payload[InteractionBrowsingService::kSummaryPayloadKey] = webgateway_context;
  context->payload[InteractionBrowsingService::kWebGatewayContextPayloadKey] =
      webgateway_context;
  context->payload[InteractionBrowsingService::kWebGatewayPolicyPayloadKey] =
      response_policy;
  context->payload[InteractionBrowsingService::kWebGatewayReviewPayloadKey] =
      nlohmann::json{
          {"decision", decision},
          {"response_policy", response_policy},
          {"refusal", refusal.has_value() ? nlohmann::json(*refusal) : nlohmann::json(nullptr)},
      };
}

}  // namespace

std::optional<InteractionValidationError>
InteractionBrowsingService::ResolveInteractionBrowsing(
    const PlaneInteractionResolution& resolution,
    InteractionRequestContext* context) const {
  if (context == nullptr) {
    throw std::invalid_argument("interaction request context is required");
  }

  const PlaneBrowsingService service;
  if (!service.IsEnabled(resolution.desired_state)) {
    ApplyWebGatewayPayload(
        context,
        BuildDisabledWebGatewayContext(),
        nlohmann::json::object(),
        "",
        std::nullopt,
        "disabled");
    return std::nullopt;
  }

  if (const auto local_context = BuildLocalEnabledIdleContext(*context);
      local_context.has_value()) {
    ApplyWebGatewayPayload(
        context,
        *local_context,
        local_context->value("response_policy", nlohmann::json::object()),
        "WebGateway state: enabled_not_needed. Use only the WebGateway evidence and policy provided with this request. Do not claim extra online verification beyond that evidence.\n\nWebGateway determined that no web lookup was needed for this request. Answer directly without claiming fresh web verification.",
        std::nullopt,
        "not_needed");
    PersistBrowsingMode(*local_context, context);
    return std::nullopt;
  }

  nlohmann::json resolve_payload = {
      {"plane_name", resolution.desired_state.plane_name},
      {"conversation_slice",
       context->payload.contains("messages") && context->payload.at("messages").is_array()
           ? context->payload.at("messages")
           : nlohmann::json::array()},
      {"latest_user_message", LastUserMessageContent(*context)},
      {"web_mode", ReadPersistedBrowsingMode(*context)},
      {"plane_policy",
       nlohmann::json{
           {"enabled", true},
           {"browser_session_enabled",
            resolution.desired_state.browsing.has_value() &&
                    resolution.desired_state.browsing->policy.has_value()
                ? nlohmann::json(
                      resolution.desired_state.browsing->policy->browser_session_enabled)
                : nlohmann::json(false)},
           {"rendered_browser_enabled",
            resolution.desired_state.browsing.has_value() &&
                    resolution.desired_state.browsing->policy.has_value()
                ? nlohmann::json(
                      resolution.desired_state.browsing->policy->rendered_browser_enabled)
                : nlohmann::json(true)},
       }},
  };

  std::string error_code;
  std::string error_message;
  const auto response = service.ProxyPlaneBrowsingRequest(
      resolution.desired_state,
      "POST",
      "/resolve",
      resolve_payload.dump(),
      &error_code,
      &error_message);
  if (!response.has_value() || response->status_code != 200) {
    const nlohmann::json context_payload =
        BuildUnavailableWebGatewayContext(
            error_code.empty() ? "webgateway_unavailable" : error_code,
            error_message);
    ApplyWebGatewayPayload(
        context,
        context_payload,
        context_payload.value("response_policy", nlohmann::json::object()),
        "WebGateway could not provide usable evidence for this request. If online verification matters, state that web browsing was unavailable.",
        std::nullopt,
        "unavailable");
    return std::nullopt;
  }

  const nlohmann::json payload = ParseJsonBodyOrObject(response->body);
  const nlohmann::json webgateway_context =
      payload.contains("context") && payload.at("context").is_object()
          ? payload.at("context")
          : BuildDisabledWebGatewayContext();
  const nlohmann::json response_policy =
      payload.contains("response_policy") && payload.at("response_policy").is_object()
          ? payload.at("response_policy")
          : webgateway_context.value("response_policy", nlohmann::json::object());
  const auto refusal_it = payload.find("refusal");
  std::optional<std::string> refusal = std::nullopt;
  if (refusal_it != payload.end() && refusal_it->is_string()) {
    refusal = refusal_it->get<std::string>();
  }
  ApplyWebGatewayPayload(
      context,
      webgateway_context,
      response_policy,
      ReadJsonStringOrDefault(payload, "model_instruction"),
      refusal,
      ReadJsonStringOrDefault(payload, "decision", "disabled"));
  PersistBrowsingMode(webgateway_context, context);
  return std::nullopt;
}

void InteractionBrowsingService::ReviewInteractionResponse(
    const PlaneInteractionResolution& resolution,
    const InteractionRequestContext& request_context,
    InteractionSessionResult* result) const {
  if (result == nullptr) {
    return;
  }
  if (!request_context.payload.contains(kWebGatewayReviewPayloadKey) ||
      !request_context.payload.at(kWebGatewayReviewPayloadKey).is_object()) {
    return;
  }
  if (!PlaneBrowsingService().IsEnabled(resolution.desired_state)) {
    return;
  }

  nlohmann::json review_payload =
      request_context.payload.at(kWebGatewayReviewPayloadKey);
  review_payload["draft_model_answer"] = result->content;

  std::string error_code;
  std::string error_message;
  const auto review_response = PlaneBrowsingService().ProxyPlaneBrowsingRequest(
      resolution.desired_state,
      "POST",
      "/review-response",
      review_payload.dump(),
      &error_code,
      &error_message);
  if (!review_response.has_value() || review_response->status_code != 200) {
    return;
  }

  const nlohmann::json payload = ParseJsonBodyOrObject(review_response->body);
  const auto corrected = payload.find("corrected_answer");
  if (corrected != payload.end() && corrected->is_string()) {
    result->content = corrected->get<std::string>();
    if (!result->segments.empty()) {
      result->segments.back().text = result->content;
    }
  }
}

std::string InteractionBrowsingService::ReadPersistedBrowsingMode(
    const InteractionRequestContext& context) const {
  const nlohmann::json& state =
      context.payload.contains(kInteractionSessionContextStatePayloadKey) &&
              context.payload.at(kInteractionSessionContextStatePayloadKey).is_object()
          ? context.payload.at(kInteractionSessionContextStatePayloadKey)
          : context.session_context_state;
  if (!state.is_object()) {
    return "auto";
  }
  const std::string mode = state.value("browsing_mode", std::string{});
  if (mode == "enabled" || mode == "disabled") {
    return mode;
  }
  return "auto";
}

std::string InteractionBrowsingService::LastUserMessageContent(
    const InteractionRequestContext& context) const {
  if (!context.payload.contains("messages") ||
      !context.payload.at("messages").is_array()) {
    return "";
  }
  const auto& messages = context.payload.at("messages");
  for (auto it = messages.rbegin(); it != messages.rend(); ++it) {
    if ((*it).is_object() &&
        (*it).value("role", std::string{}) == "user" &&
        (*it).contains("content") &&
        (*it).at("content").is_string()) {
      return (*it).at("content").get<std::string>();
    }
  }
  return "";
}

bool InteractionBrowsingService::LatestMessageRequestsLookup(
    const InteractionRequestContext& context) const {
  const std::string latest = LastUserMessageContent(context);
  if (latest.empty()) {
    return false;
  }
  if (ContainsAnyLiteral(latest, {"http://", "https://"})) {
    return true;
  }
  if (ContainsAnyAscii(
          latest,
          {"enable web",
           "disable web",
           "web for this chat",
           "search online",
           "search the web",
           "use the web",
           "latest",
           "current",
           "recent",
           "today",
           "news",
           "source",
           "sources",
           "citation",
           "citations",
           "online",
           "fresh",
           "включи веб",
           "выключи веб",
           "используй веб",
           "используй интернет",
           "найди в интернете",
           "поиск в интернете",
           "последн",
           "сегодня",
           "источник",
           "источники",
           "ссылка",
           "ссылки"})) {
    return true;
  }
  return false;
}

std::optional<nlohmann::json> InteractionBrowsingService::BuildLocalEnabledIdleContext(
    const InteractionRequestContext& context) const {
  if (ReadPersistedBrowsingMode(context) != "enabled" ||
      LatestMessageRequestsLookup(context)) {
    return std::nullopt;
  }
  return nlohmann::json{
      {"mode", "enabled"},
      {"mode_source", "session_context"},
      {"plane_enabled", true},
      {"ready", false},
      {"session_backend", "broker_fallback"},
      {"rendered_browser_enabled", true},
      {"rendered_browser_ready", false},
      {"login_enabled", false},
      {"toggle_only", false},
      {"decision", "not_needed"},
      {"reason", "context_not_needed"},
      {"lookup_state", "enabled_not_needed"},
      {"lookup_attempted", false},
      {"lookup_required", false},
      {"evidence_attached", false},
      {"searches", nlohmann::json::array()},
      {"sources", nlohmann::json::array()},
      {"errors", nlohmann::json::array()},
      {"refusal", nullptr},
      {"response_policy", nlohmann::json::object()},
      {"indicator", nlohmann::json{{"compact", "web:on idle"}}},
      {"trace",
       nlohmann::json::array({nlohmann::json{{"stage", "mode"},
                                             {"status", "enabled_idle"},
                                             {"compact", "web:on idle"}}})},
  };
}

void InteractionBrowsingService::PersistBrowsingMode(
    const nlohmann::json& webgateway_context,
    InteractionRequestContext* context) const {
  if (context == nullptr || !webgateway_context.is_object()) {
    return;
  }
  std::string persisted_mode = "disabled";
  if (webgateway_context.value("mode", std::string{}) == "enabled") {
    persisted_mode = "enabled";
  }
  if (!context->session_context_state.is_object()) {
    context->session_context_state = nlohmann::json::object();
  }
  context->session_context_state["browsing_mode"] = persisted_mode;
  if (!context->payload.contains(kInteractionSessionContextStatePayloadKey) ||
      !context->payload.at(kInteractionSessionContextStatePayloadKey).is_object()) {
    context->payload[kInteractionSessionContextStatePayloadKey] = nlohmann::json::object();
  }
  context->payload[kInteractionSessionContextStatePayloadKey]["browsing_mode"] =
      persisted_mode;
}

}  // namespace naim::controller
