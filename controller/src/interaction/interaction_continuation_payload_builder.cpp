#include "interaction/interaction_continuation_payload_builder.h"

#include "interaction/interaction_completion_policy_support.h"
#include "interaction/interaction_text_post_processor.h"

namespace naim::controller {

nlohmann::json InteractionContinuationPayloadBuilder::Build(
    const nlohmann::json& original_payload,
    const std::string& accumulated_text,
    const InteractionCompletionPolicy& policy,
    bool natural_stop_without_marker,
    int total_completion_tokens) const {
  nlohmann::json payload = original_payload;
  const InteractionCompletionPolicySupport completion_policy_support;
  const InteractionTextPostProcessor text_post_processor;
  nlohmann::json messages = nlohmann::json::array();
  if (payload.contains("messages") && payload.at("messages").is_array()) {
    for (const auto& message : payload.at("messages")) {
      messages.push_back(message);
    }
  }

  const std::string recent_assistant_context =
      accumulated_text.empty() ? std::string{}
                               : text_post_processor.Utf8SafeSuffix(
                                     accumulated_text, 4096);
  const std::string trailing_excerpt =
      accumulated_text.empty() ? std::string{}
                               : text_post_processor.Utf8SafeSuffix(
                                     accumulated_text, 256);
  const int remaining_completion_tokens =
      std::max(0, policy.max_total_completion_tokens - total_completion_tokens);

  if (policy.thinking_enabled) {
    if (!payload.contains("chat_template_kwargs") ||
        !payload.at("chat_template_kwargs").is_object()) {
      payload["chat_template_kwargs"] = nlohmann::json::object();
    }
    payload["chat_template_kwargs"]["enable_thinking"] = false;
  }

  if (!recent_assistant_context.empty()) {
    messages.push_back(nlohmann::json{
        {"role", "assistant"},
        {"content", recent_assistant_context},
    });
  }

  messages.push_back(nlohmann::json{
      {"role", "user"},
      {"content",
       completion_policy_support.BuildContinuationPrompt(
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

}  // namespace naim::controller
