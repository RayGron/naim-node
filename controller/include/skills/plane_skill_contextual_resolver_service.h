#pragma once

#include <string>
#include <vector>

#include <nlohmann/json.hpp>

#include "interaction/interaction_service.h"

namespace naim::controller {

struct ContextualSkillSelection {
  std::string mode = "none";
  int candidate_count = 0;
  std::vector<std::string> selected_skill_ids;
  nlohmann::json selected_skills = nlohmann::json::array();
};

class PlaneSkillContextualResolverService final {
 public:
  std::string ExtractPromptText(const nlohmann::json& payload) const;

  ContextualSkillSelection Resolve(
      const std::string& db_path,
      const PlaneInteractionResolution& resolution,
      const nlohmann::json& payload) const;

  nlohmann::json BuildDebugPayload(
      const std::string& db_path,
      const PlaneInteractionResolution& resolution,
      const nlohmann::json& payload) const;
};

}  // namespace naim::controller
