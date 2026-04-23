#include "interaction/interaction_runtime_request_codec.h"

#include <stdexcept>

#include "naim/state/state_json.h"

namespace naim::controller {

namespace {

using nlohmann::json;

json SerializePolicy(const InteractionCompletionPolicy& policy) {
  json value = {
      {"response_mode", policy.response_mode},
      {"max_tokens", policy.max_tokens},
      {"max_continuations", policy.max_continuations},
      {"max_total_completion_tokens", policy.max_total_completion_tokens},
      {"max_elapsed_time_ms", policy.max_elapsed_time_ms},
      {"thinking_enabled", policy.thinking_enabled},
      {"semantic_goal", policy.semantic_goal},
      {"completion_marker", policy.completion_marker},
      {"require_completion_marker", policy.require_completion_marker},
  };
  if (policy.target_completion_tokens.has_value()) {
    value["target_completion_tokens"] = *policy.target_completion_tokens;
  }
  return value;
}

InteractionCompletionPolicy DeserializePolicy(const json& value) {
  InteractionCompletionPolicy policy;
  if (!value.is_object()) {
    return policy;
  }
  policy.response_mode = value.value("response_mode", policy.response_mode);
  policy.max_tokens = value.value("max_tokens", policy.max_tokens);
  if (value.contains("target_completion_tokens") &&
      !value.at("target_completion_tokens").is_null()) {
    policy.target_completion_tokens = value.at("target_completion_tokens").get<int>();
  }
  policy.max_continuations =
      value.value("max_continuations", policy.max_continuations);
  policy.max_total_completion_tokens =
      value.value("max_total_completion_tokens", policy.max_total_completion_tokens);
  policy.max_elapsed_time_ms =
      value.value("max_elapsed_time_ms", policy.max_elapsed_time_ms);
  policy.thinking_enabled = value.value("thinking_enabled", policy.thinking_enabled);
  policy.semantic_goal = value.value("semantic_goal", policy.semantic_goal);
  policy.completion_marker =
      value.value("completion_marker", policy.completion_marker);
  policy.require_completion_marker =
      value.value("require_completion_marker", policy.require_completion_marker);
  return policy;
}

json SerializeResolvedPolicy(const ResolvedInteractionPolicy& policy) {
  return json{
      {"policy", SerializePolicy(policy.policy)},
      {"mode", policy.mode},
      {"repository_analysis", policy.repository_analysis},
      {"long_form", policy.long_form},
  };
}

ResolvedInteractionPolicy DeserializeResolvedPolicy(const json& value) {
  ResolvedInteractionPolicy policy;
  if (!value.is_object()) {
    return policy;
  }
  policy.policy = DeserializePolicy(value.value("policy", json::object()));
  policy.mode = value.value("mode", policy.mode);
  policy.repository_analysis =
      value.value("repository_analysis", policy.repository_analysis);
  policy.long_form = value.value("long_form", policy.long_form);
  return policy;
}

}  // namespace

std::string InteractionRuntimeRequestCodec::Serialize(
    const InteractionRuntimeExecutionRequest& request) const {
  return json{
      {"desired_state", json::parse(SerializeDesiredStateJson(request.desired_state))},
      {"status_payload", request.status_payload},
      {"payload", request.payload},
      {"resolved_policy", SerializeResolvedPolicy(request.resolved_policy)},
      {"structured_output_json", request.structured_output_json},
      {"force_stream", request.force_stream},
  }
      .dump();
}

InteractionRuntimeExecutionRequest InteractionRuntimeRequestCodec::Deserialize(
    const std::string& json_text) const {
  const json value = json_text.empty() ? json::object() : json::parse(json_text);
  if (!value.is_object()) {
    throw std::runtime_error("interaction runtime request must be a JSON object");
  }
  InteractionRuntimeExecutionRequest request;
  if (!value.contains("desired_state") || !value.at("desired_state").is_object()) {
    throw std::runtime_error("interaction runtime request is missing desired_state");
  }
  request.desired_state = DeserializeDesiredStateJson(value.at("desired_state").dump());
  request.status_payload = value.value("status_payload", json::object());
  request.payload = value.value("payload", json::object());
  request.resolved_policy =
      DeserializeResolvedPolicy(value.value("resolved_policy", json::object()));
  request.structured_output_json =
      value.value("structured_output_json", false);
  request.force_stream = value.value("force_stream", false);
  return request;
}

}  // namespace naim::controller
