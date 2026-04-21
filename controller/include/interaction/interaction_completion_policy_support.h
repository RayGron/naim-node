#pragma once

#include <string>

#include <nlohmann/json.hpp>

#include "interaction/interaction_types.h"

namespace naim::controller {

class InteractionCompletionPolicySupport final {
 public:
  InteractionCompletionPolicy NormalizeConfiguredPolicy(
      const naim::InteractionSettings::CompletionPolicy& configured_policy) const;

  InteractionCompletionPolicy DefaultChatPolicy() const;

  ResolvedInteractionPolicy ResolvePolicy(
      const naim::DesiredState& desired_state,
      const nlohmann::json& payload) const;

  std::string BuildRepositoryAnalysisInstruction() const;

  std::string BuildSemanticCompletionInstruction(
      const InteractionCompletionPolicy& policy) const;

  std::string BuildContinuationPrompt(
      const InteractionCompletionPolicy& policy,
      bool natural_stop_without_marker,
      const std::string& trailing_excerpt = "",
      int remaining_completion_tokens = 0,
      bool hidden_thinking_mode = false,
      bool visible_output_started = false) const;

  bool SessionReachedTargetLength(
      const InteractionCompletionPolicy& policy,
      int total_completion_tokens) const;

  bool CanCompleteOnNaturalStop(
      const InteractionCompletionPolicy& policy,
      const InteractionSegmentSummary& summary) const;
};

}  // namespace naim::controller
