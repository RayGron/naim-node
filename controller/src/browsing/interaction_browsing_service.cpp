#include "browsing/interaction_browsing_service.h"

#include <stdexcept>
#include <string_view>

#include "browsing/plane_browsing_service.h"

namespace comet::controller {

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

std::string LastUserMessageContent(const nlohmann::json& payload) {
  if (!payload.contains("messages") || !payload.at("messages").is_array()) {
    return "";
  }
  const auto& messages = payload.at("messages");
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

  nlohmann::json resolve_payload = {
      {"plane_name", resolution.desired_state.plane_name},
      {"conversation_slice",
       context->payload.contains("messages") && context->payload.at("messages").is_array()
           ? context->payload.at("messages")
           : nlohmann::json::array()},
      {"latest_user_message", LastUserMessageContent(context->payload)},
      {"web_mode", "auto"},
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

}  // namespace comet::controller
