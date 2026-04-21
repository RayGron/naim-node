#include <iostream>
#include <stdexcept>
#include <string>

#include "interaction/interaction_continuation_payload_builder.h"

namespace {

void Expect(bool condition, const std::string& message) {
  if (!condition) {
    throw std::runtime_error(message);
  }
}

void TestBuildAddsAssistantContextAndContinuationPrompt() {
  const naim::controller::InteractionContinuationPayloadBuilder builder;
  naim::controller::InteractionCompletionPolicy policy;
  policy.max_total_completion_tokens = 800;
  policy.thinking_enabled = false;

  const nlohmann::json payload = {
      {"messages", nlohmann::json::array({nlohmann::json{
          {"role", "user"},
          {"content", "Original question"},
      }})},
  };

  const auto result = builder.Build(payload, "Partial answer", policy, false, 120);
  Expect(result.at("messages").is_array(), "continuation payload should keep messages array");
  Expect(result.at("messages").size() == 3, "continuation payload should append assistant and user messages");
  Expect(result.at("messages").at(1).at("role").get<std::string>() == "assistant",
         "continuation payload should append assistant context");
  Expect(result.at("messages").at(1).at("content").get<std::string>() == "Partial answer",
         "continuation payload should include accumulated assistant text");
  Expect(result.at("messages").at(2).at("role").get<std::string>() == "user",
         "continuation payload should append continuation prompt");
  std::cout << "ok: interaction-continuation-payload-appends-context" << '\n';
}

void TestBuildDisablesThinkingForContinuationTurn() {
  const naim::controller::InteractionContinuationPayloadBuilder builder;
  naim::controller::InteractionCompletionPolicy policy;
  policy.max_total_completion_tokens = 1000;
  policy.thinking_enabled = true;

  const auto result = builder.Build(
      nlohmann::json::object(),
      "",
      policy,
      true,
      200);
  Expect(
      result.at("chat_template_kwargs").at("enable_thinking").get<bool>() == false,
      "continuation payload should disable thinking on follow-up turn");
  Expect(result.at("messages").size() == 1,
         "continuation payload without accumulated text should add only the continuation user prompt");
  std::cout << "ok: interaction-continuation-payload-disables-thinking" << '\n';
}

}  // namespace

int main() {
  try {
    TestBuildAddsAssistantContextAndContinuationPrompt();
    TestBuildDisablesThinkingForContinuationTurn();
    return 0;
  } catch (const std::exception& error) {
    std::cerr << "interaction_continuation_payload_builder_tests failed: "
              << error.what() << '\n';
    return 1;
  }
}
