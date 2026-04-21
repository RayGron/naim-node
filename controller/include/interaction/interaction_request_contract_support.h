#pragma once

#include <map>
#include <optional>
#include <string>
#include <string_view>

#include <nlohmann/json.hpp>

#include "interaction/interaction_types.h"

namespace naim::controller {

class InteractionRequestContractSupport final {
 public:
  bool PayloadContainsUnsupportedInteractionField(
      const nlohmann::json& payload,
      std::string* field_name) const;

  std::optional<std::string> ParsePublicSessionId(
      const nlohmann::json& payload) const;

  nlohmann::json RequestAppliedSkills(
      const InteractionRequestContext& request_context) const;

  nlohmann::json RequestAutoAppliedSkills(
      const InteractionRequestContext& request_context) const;

  std::optional<std::string> RequestSkillsSessionId(
      const InteractionRequestContext& request_context) const;

  std::string RequestSkillResolutionMode(
      const InteractionRequestContext& request_context) const;

  nlohmann::json RequestBrowsingSummary(
      const InteractionRequestContext& request_context) const;

  std::optional<std::string> ParseInteractionStreamPlaneName(
      const std::string& request_method,
      const std::string& request_path) const;

  std::map<std::string, std::string> BuildInteractionResponseHeaders(
      const std::string& request_id) const;

  std::string ResolveInteractionServedModelName(
      const PlaneInteractionResolution& resolution) const;

  std::string ResolveInteractionActiveModelId(
      const PlaneInteractionResolution& resolution) const;

  nlohmann::json BuildInteractionContractMetadata(
      const PlaneInteractionResolution& resolution,
      const std::string& request_id,
      const std::optional<std::string>& session_id = std::nullopt,
      const std::optional<int>& segment_count = std::nullopt,
      const std::optional<int>& continuation_count = std::nullopt) const;

 private:
  std::string ReadJsonStringOrEmpty(
      const nlohmann::json& payload,
      std::string_view key) const;

  std::string TrimCopy(const std::string& value) const;

  nlohmann::json DefaultBrowsingSummary() const;
};

}  // namespace naim::controller
