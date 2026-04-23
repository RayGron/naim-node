#include "interaction/interaction_completion_policy_support.h"

#include <algorithm>
#include <cctype>
#include <sstream>

#include "interaction/interaction_request_heuristics.h"

namespace naim::controller {

namespace {

int ClampInteractionPolicyValue(int value, int minimum, int maximum) {
  return std::max(minimum, std::min(value, maximum));
}

std::string TrimCopy(const std::string& value) {
  std::size_t start = 0;
  while (start < value.size() &&
         std::isspace(static_cast<unsigned char>(value[start])) != 0) {
    ++start;
  }
  std::size_t end = value.size();
  while (end > start &&
         std::isspace(static_cast<unsigned char>(value[end - 1])) != 0) {
    --end;
  }
  return value.substr(start, end - start);
}

}  // namespace

InteractionCompletionPolicy InteractionCompletionPolicySupport::NormalizeConfiguredPolicy(
    const naim::InteractionSettings::CompletionPolicy& configured_policy) const {
  InteractionCompletionPolicy policy;
  const InteractionRequestHeuristics heuristics;
  const std::string normalized_mode =
      heuristics.CanonicalResponseMode(configured_policy.response_mode);
  if (!normalized_mode.empty()) {
    policy.response_mode = normalized_mode;
  }
  policy.max_tokens =
      ClampInteractionPolicyValue(configured_policy.max_tokens, 1, 2048);
  if (configured_policy.target_completion_tokens.has_value()) {
    policy.target_completion_tokens = ClampInteractionPolicyValue(
        *configured_policy.target_completion_tokens, 1, 16384);
  }
  policy.max_continuations =
      ClampInteractionPolicyValue(configured_policy.max_continuations, 0, 8);
  policy.max_total_completion_tokens = ClampInteractionPolicyValue(
      configured_policy.max_total_completion_tokens,
      policy.max_tokens,
      16384);
  policy.max_elapsed_time_ms = ClampInteractionPolicyValue(
      configured_policy.max_elapsed_time_ms, 1000, 600000);
  if (configured_policy.semantic_goal.has_value()) {
    policy.semantic_goal = TrimCopy(*configured_policy.semantic_goal);
  }
  if (policy.target_completion_tokens.has_value()) {
    policy.max_total_completion_tokens = std::max(
        policy.max_total_completion_tokens,
        *policy.target_completion_tokens);
  }
  policy.require_completion_marker =
      !policy.semantic_goal.empty() ||
      policy.target_completion_tokens.has_value() ||
      policy.response_mode == "long" ||
      policy.response_mode == "very_long";
  return policy;
}

InteractionCompletionPolicy InteractionCompletionPolicySupport::DefaultChatPolicy() const {
  naim::InteractionSettings::CompletionPolicy configured_policy;
  configured_policy.response_mode = "normal";
  configured_policy.max_tokens = 512;
  configured_policy.max_continuations = 0;
  configured_policy.max_total_completion_tokens = 512;
  configured_policy.max_elapsed_time_ms = 30000;
  return NormalizeConfiguredPolicy(configured_policy);
}

ResolvedInteractionPolicy InteractionCompletionPolicySupport::ResolvePolicy(
    const naim::DesiredState& desired_state,
    const nlohmann::json& payload) const {
  ResolvedInteractionPolicy resolved;
  const InteractionRequestHeuristics heuristics;
  const std::string last_user_message = heuristics.LastUserMessageContent(payload);
  const bool long_form_task =
      heuristics.LooksLikeLongFormTaskRequest(last_user_message);
  const bool repository_analysis =
      heuristics.LooksLikeRepositoryAnalysisRequest(last_user_message);
  resolved.repository_analysis = repository_analysis;
  resolved.long_form = long_form_task;
  const auto finalize_resolved = [&](ResolvedInteractionPolicy current) {
    if (desired_state.interaction.has_value()) {
      current.policy.thinking_enabled =
          desired_state.interaction->thinking_enabled;
    }
    if (current.policy.thinking_enabled) {
      current.policy.max_continuations =
          std::max(current.policy.max_continuations, 6);
      current.policy.max_total_completion_tokens =
          std::max(current.policy.max_total_completion_tokens,
                   std::max(current.policy.max_tokens * 12, 8192));
      current.policy.max_elapsed_time_ms =
          std::max(current.policy.max_elapsed_time_ms, 300000);
    }
    return current;
  };
  if (desired_state.interaction.has_value()) {
    const auto& interaction = *desired_state.interaction;
    const std::string normalized_completion_mode =
        interaction.completion_policy.has_value()
            ? heuristics.CanonicalResponseMode(
                  interaction.completion_policy->response_mode)
            : std::string{};
    if (repository_analysis && long_form_task &&
        interaction.analysis_long_completion_policy.has_value()) {
      resolved.policy = NormalizeConfiguredPolicy(
          *interaction.analysis_long_completion_policy);
      resolved.mode = "analysis-long";
      return finalize_resolved(std::move(resolved));
    }
    if (repository_analysis && !long_form_task &&
        interaction.analysis_completion_policy.has_value()) {
      resolved.policy = NormalizeConfiguredPolicy(
          *interaction.analysis_completion_policy);
      resolved.mode = "analysis-default";
      return finalize_resolved(std::move(resolved));
    }
    if (long_form_task && interaction.long_completion_policy.has_value()) {
      resolved.policy = NormalizeConfiguredPolicy(
          *interaction.long_completion_policy);
      resolved.mode = "long";
      return finalize_resolved(std::move(resolved));
    }
    if (!long_form_task &&
        interaction.completion_policy.has_value() &&
        normalized_completion_mode != "long" &&
        normalized_completion_mode != "very_long") {
      resolved.policy = NormalizeConfiguredPolicy(
          *interaction.completion_policy);
      resolved.mode = "default";
      return finalize_resolved(std::move(resolved));
    }
    if (long_form_task &&
        interaction.completion_policy.has_value() &&
        (normalized_completion_mode == "long" ||
         normalized_completion_mode == "very_long")) {
      resolved.policy = NormalizeConfiguredPolicy(
          *interaction.completion_policy);
      resolved.mode = "long";
      return finalize_resolved(std::move(resolved));
    }
  }
  resolved.policy = DefaultChatPolicy();
  resolved.mode = repository_analysis
                      ? (long_form_task ? "analysis-long-fallback"
                                        : "analysis-default-fallback")
                      : (long_form_task ? "long-fallback"
                                        : "default-fallback");
  return finalize_resolved(std::move(resolved));
}

std::string InteractionCompletionPolicySupport::BuildRepositoryAnalysisInstruction() const {
  return "Repository analysis requirement: answer only from the repository or "
         "codebase evidence provided in the request. "
         "Cite concrete file paths for repo-specific claims. If evidence is "
         "insufficient, say exactly what is unknown. "
         "Do not claim lack of filesystem access when repository context is "
         "already present.";
}

std::string InteractionCompletionPolicySupport::BuildSemanticCompletionInstruction(
    const InteractionCompletionPolicy& policy) const {
  std::ostringstream instruction;
  instruction << "Semantic completion protocol:\n"
              << "- You may need multiple assistant segments to finish this task.\n"
              << "- End the final segment with the exact marker "
              << policy.completion_marker
              << " on its own line only when the task is fully complete.\n"
              << "- If the task is not complete in this segment, do not output the marker.\n"
              << "- For continuation segments, continue exactly where you stopped without repeating prior text unless recap is explicitly requested.\n"
              << "- Respect the user's requested length and scope. Once the requested artifact is complete, stop instead of adding optional extra sections.\n"
              << "- When you have satisfied the user's requested structure and length, emit the marker immediately on its own line and end the response.\n"
              << "- Do not emit tool calls, tool requests, or waiting states in this phase.\n";
  if (!policy.semantic_goal.empty()) {
    instruction << "- Task completion goal: " << policy.semantic_goal << "\n";
  }
  if (policy.target_completion_tokens.has_value()) {
    instruction << "- Aim for at least " << *policy.target_completion_tokens
                << " completion tokens before marking the task complete.\n";
  }
  return instruction.str();
}

std::string InteractionCompletionPolicySupport::BuildContinuationPrompt(
    const InteractionCompletionPolicy& policy,
    bool natural_stop_without_marker,
    const std::string& trailing_excerpt,
    int remaining_completion_tokens,
    bool hidden_thinking_mode,
    bool visible_output_started) const {
  std::ostringstream prompt;
  if (hidden_thinking_mode && !visible_output_started) {
    prompt << "Your hidden reasoning is not shown to the user. Stop reasoning now and "
           << "reply with only the final user-visible answer.";
  } else if (natural_stop_without_marker) {
    prompt << "Your previous segment stopped before you proved the task was complete. "
           << "If the task is already complete, reply with only "
           << policy.completion_marker
           << ". Otherwise continue exactly where you stopped.";
  } else {
    prompt << "Continue exactly where you stopped.";
  }
  if (!trailing_excerpt.empty()) {
    prompt << " The last visible excerpt from your previous segment was:\n"
           << trailing_excerpt
           << "\nContinue immediately after that excerpt.";
  }
  if (hidden_thinking_mode) {
    prompt << " Do not output hidden reasoning, <think> blocks, or reasoning preambles."
           << " The next segment must contain only the final answer that the user should read.";
  }
  if (remaining_completion_tokens > 0) {
    prompt << " You have approximately " << remaining_completion_tokens
           << " completion tokens remaining across all future segments.";
    if (remaining_completion_tokens <= policy.max_tokens) {
      prompt << " Finish the remaining required content and conclude in this segment if possible.";
    } else {
      prompt << " Prioritize the remaining required sections and avoid optional expansion.";
    }
  }
  prompt << " Do not repeat prior text. Emit " << policy.completion_marker
         << " on its own line only in the final segment when the task is fully complete."
         << " Do not restart the outline, recap earlier sections, or add extra filler once the requested artifact is complete.";
  return prompt.str();
}

bool InteractionCompletionPolicySupport::SessionReachedTargetLength(
    const InteractionCompletionPolicy& policy,
    int total_completion_tokens) const {
  return !policy.target_completion_tokens.has_value() ||
         total_completion_tokens >= *policy.target_completion_tokens;
}

bool InteractionCompletionPolicySupport::CanCompleteOnNaturalStop(
    const InteractionCompletionPolicy& policy,
    const InteractionSegmentSummary& summary) const {
  if (policy.require_completion_marker) {
    return false;
  }
  return summary.finish_reason != "length" && !TrimCopy(summary.text).empty();
}

bool InteractionCompletionPolicySupport::ShouldTreatLengthAsNaturalStop(
    const InteractionCompletionPolicy& policy,
    const InteractionSegmentSummary& summary) const {
  if (policy.require_completion_marker || policy.response_mode != "normal") {
    return false;
  }
  if (TrimCopy(summary.text).empty()) {
    return false;
  }
  if (summary.finish_reason != "length") {
    return false;
  }
  if (summary.completion_tokens <= 0) {
    return false;
  }
  return summary.completion_tokens < policy.max_tokens;
}

}  // namespace naim::controller
