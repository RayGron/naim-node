#pragma once

#include <optional>
#include <string>

#include <nlohmann/json.hpp>

#include "comet/state/models.h"

namespace comet::controller {

class ControllerLanguageSupport final {
 public:
  static std::string NormalizeLanguageCode(const std::string& value);
  static std::string LanguageLabel(const std::string& code);
  static std::optional<std::string> ResolveInteractionPreferredLanguage(
      const comet::DesiredState& desired_state,
      const nlohmann::json& payload);
  static std::string BuildLanguageInstruction(
      const comet::DesiredState& desired_state,
      const std::optional<std::string>& preferred_language);
};

}  // namespace comet::controller
