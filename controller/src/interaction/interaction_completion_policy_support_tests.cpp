#include <iostream>
#include <stdexcept>
#include <string>

#include "interaction/interaction_completion_policy_support.h"

namespace {

void Expect(bool condition, const std::string& message) {
  if (!condition) {
    throw std::runtime_error(message);
  }
}

void TestNormalizesConfiguredPolicy() {
  const naim::controller::InteractionCompletionPolicySupport support;
  naim::InteractionSettings::CompletionPolicy configured;
  configured.response_mode = "Very-Long";
  configured.max_tokens = 99999;
  configured.max_continuations = 99;
  configured.max_total_completion_tokens = 99999;
  configured.max_elapsed_time_ms = 999999;
  configured.semantic_goal = "  finish task  ";

  const auto policy = support.NormalizeConfiguredPolicy(configured);
  Expect(policy.response_mode == "very_long", "policy mode should be canonicalized");
  Expect(policy.max_tokens == 2048, "policy max_tokens should be clamped");
  Expect(policy.max_continuations == 8, "policy continuations should be clamped");
  Expect(policy.max_total_completion_tokens == 16384,
         "policy total completion tokens should be clamped");
  Expect(policy.max_elapsed_time_ms == 600000,
         "policy elapsed time should be clamped");
  Expect(policy.semantic_goal == "finish task", "policy semantic goal should be trimmed");
  Expect(policy.require_completion_marker,
         "semantic goal should require completion marker");
  std::cout << "ok: interaction-completion-policy-normalizes-config" << '\n';
}

void TestResolvesAnalysisLongPolicy() {
  const naim::controller::InteractionCompletionPolicySupport support;
  naim::DesiredState desired_state;
  desired_state.interaction = naim::InteractionSettings{};
  desired_state.interaction->thinking_enabled = true;
  naim::InteractionSettings::CompletionPolicy analysis_long;
  analysis_long.response_mode = "long";
  analysis_long.max_tokens = 1500;
  desired_state.interaction->analysis_long_completion_policy = analysis_long;

  const nlohmann::json payload = {
      {"messages",
       nlohmann::json::array(
           {nlohmann::json{{"role", "user"},
                           {"content", "Please write a detailed analysis of the entire repository architecture."}}})},
  };
  const auto resolved = support.ResolvePolicy(desired_state, payload);
  Expect(resolved.mode == "analysis-long", "support should select analysis-long mode");
  Expect(resolved.repository_analysis, "support should mark repository analysis");
  Expect(resolved.long_form, "support should detect long-form request");
  Expect(resolved.policy.thinking_enabled, "support should propagate thinking_enabled");
  Expect(resolved.policy.max_continuations >= 6,
         "thinking mode should raise continuation budget");
  std::cout << "ok: interaction-completion-policy-resolves-analysis-long" << '\n';
}

void TestBuildsSemanticAndContinuationPrompts() {
  const naim::controller::InteractionCompletionPolicySupport support;
  naim::controller::InteractionCompletionPolicy policy;
  policy.completion_marker = "[[DONE]]";
  policy.semantic_goal = "finish requested artifact";
  policy.target_completion_tokens = 1200;
  policy.max_tokens = 800;

  const std::string semantic = support.BuildSemanticCompletionInstruction(policy);
  Expect(semantic.find("[[DONE]]") != std::string::npos,
         "semantic instruction should mention completion marker");
  Expect(semantic.find("finish requested artifact") != std::string::npos,
         "semantic instruction should mention semantic goal");

  const std::string continuation = support.BuildContinuationPrompt(
      policy,
      true,
      "last excerpt",
      400,
      true,
      false);
  Expect(continuation.find("last excerpt") != std::string::npos,
         "continuation prompt should include trailing excerpt");
  Expect(continuation.find("Do not output hidden reasoning") != std::string::npos,
         "continuation prompt should constrain hidden reasoning");
  std::cout << "ok: interaction-completion-policy-builds-prompts" << '\n';
}

void TestNaturalStopRules() {
  const naim::controller::InteractionCompletionPolicySupport support;
  naim::controller::InteractionCompletionPolicy policy;
  policy.target_completion_tokens = 100;
  naim::controller::InteractionSegmentSummary summary;
  summary.text = "answer";
  summary.finish_reason = "stop";

  Expect(
      support.SessionReachedTargetLength(policy, 120),
      "target length should be satisfied after reaching requested tokens");
  naim::controller::InteractionCompletionPolicy marker_policy;
  marker_policy.require_completion_marker = true;
  Expect(
      !support.CanCompleteOnNaturalStop(marker_policy, summary),
      "marker-required policies should reject natural stop");
  Expect(
      support.CanCompleteOnNaturalStop(policy, summary),
      "non-marker policies should allow natural stop on non-empty stop segment");
  naim::controller::InteractionCompletionPolicy fallback_policy;
  fallback_policy.response_mode = "normal";
  fallback_policy.max_tokens = 512;
  naim::controller::InteractionSegmentSummary truncated_like_summary;
  truncated_like_summary.text = "I am Jex AI.";
  truncated_like_summary.finish_reason = "length";
  truncated_like_summary.completion_tokens = 5;
  Expect(
      support.ShouldTreatLengthAsNaturalStop(
          fallback_policy, truncated_like_summary),
      "normal short responses should be treated as natural stop even when upstream reports length");
  naim::controller::InteractionCompletionPolicy long_policy;
  long_policy.response_mode = "very_long";
  Expect(
      !support.ShouldTreatLengthAsNaturalStop(long_policy, truncated_like_summary),
      "long-form policies should keep continuation behavior");
  std::cout << "ok: interaction-completion-policy-natural-stop-rules" << '\n';
}

}  // namespace

int main() {
  try {
    TestNormalizesConfiguredPolicy();
    TestResolvesAnalysisLongPolicy();
    TestBuildsSemanticAndContinuationPrompts();
    TestNaturalStopRules();
    return 0;
  } catch (const std::exception& error) {
    std::cerr << "interaction_completion_policy_support_tests failed: "
              << error.what() << '\n';
    return 1;
  }
}
