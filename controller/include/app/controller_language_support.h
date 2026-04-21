#pragma once

#include <optional>
#include <string>

#include <nlohmann/json.hpp>

#include "naim/state/models.h"

namespace naim::controller {

class ControllerLanguageSupport final {
 public:
  static std::string NormalizeLanguageCode(const std::string& value);
  static std::string LanguageLabel(const std::string& code);
  static std::optional<std::string> ResolveInteractionPreferredLanguage(
      const naim::DesiredState& desired_state,
      const nlohmann::json& payload);
  static std::string BuildLanguageInstruction(
      const naim::DesiredState& desired_state,
      const std::optional<std::string>& preferred_language);
};

}  // namespace naim::controller
