#pragma once

#include <string>
#include <vector>

#include <nlohmann/json.hpp>

namespace naim::controller {

class InteractionRequestHeuristics {
 public:
  std::string LastUserMessageContent(const nlohmann::json& payload) const;

  bool LooksLikeLongFormTaskRequest(const std::string& text) const;

  bool LooksLikeRepositoryAnalysisRequest(const std::string& text) const;

  std::string CanonicalResponseMode(const std::string& value) const;

 private:
  std::string NormalizeInteractionText(const std::string& value) const;
  bool ContainsAnySubstring(
      const std::string& haystack,
      const std::vector<std::string>& needles) const;
};

}  // namespace naim::controller
