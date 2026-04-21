#include "app/controller_language_support.h"

#include <cctype>

namespace naim::controller {

std::string ControllerLanguageSupport::NormalizeLanguageCode(const std::string& value) {
  std::string normalized;
  normalized.reserve(value.size());
  for (unsigned char ch : value) {
    if (ch == '-') {
      normalized.push_back('_');
    } else {
      normalized.push_back(static_cast<char>(std::tolower(ch)));
    }
  }
  return normalized;
}

std::string ControllerLanguageSupport::LanguageLabel(const std::string& code) {
  const std::string normalized = NormalizeLanguageCode(code);
  if (normalized == "ru") {
    return "Russian";
  }
  if (normalized == "en") {
    return "English";
  }
  if (normalized == "uk" || normalized == "uk_ua") {
    return "Ukrainian";
  }
  if (normalized == "de" || normalized == "de_de") {
    return "German";
  }
  return code.empty() ? std::string("English") : code;
}

std::optional<std::string> ControllerLanguageSupport::ResolveInteractionPreferredLanguage(
    const naim::DesiredState& desired_state,
    const nlohmann::json& payload) {
  if (payload.contains("preferred_language") &&
      payload.at("preferred_language").is_string()) {
    const std::string preferred = payload.at("preferred_language").get<std::string>();
    if (!preferred.empty()) {
      return NormalizeLanguageCode(preferred);
    }
  }
  if (desired_state.interaction.has_value() &&
      !desired_state.interaction->default_response_language.empty()) {
    return NormalizeLanguageCode(desired_state.interaction->default_response_language);
  }
  return std::nullopt;
}

std::string ControllerLanguageSupport::BuildLanguageInstruction(
    const naim::DesiredState& desired_state,
    const std::optional<std::string>& preferred_language) {
  const std::string no_reasoning_instruction =
      " Do not output chain-of-thought, hidden reasoning, analysis traces, or <think> blocks. Output only the final user-facing answer.";
  if (preferred_language.has_value() && !preferred_language->empty()) {
    return "Response language requirement: Reply in " + LanguageLabel(*preferred_language) +
           ". Ignore the model's default language preferences. Never default to Chinese unless the user explicitly requests Chinese." +
           no_reasoning_instruction;
  }
  if (desired_state.interaction.has_value()) {
    if (desired_state.interaction->follow_user_language) {
      return "Response language requirement: Reply in the same language as the user's last message. Never default to Chinese unless the user explicitly requests Chinese." +
             no_reasoning_instruction;
    }
    if (!desired_state.interaction->default_response_language.empty()) {
      return "Response language requirement: Reply in " +
             LanguageLabel(desired_state.interaction->default_response_language) +
             ". Ignore the model's default language preferences. Never default to Chinese unless the user explicitly requests Chinese." +
             no_reasoning_instruction;
    }
  }
  return "Response language requirement: Reply in the same language as the user's last message. Never default to Chinese unless the user explicitly requests Chinese." +
         no_reasoning_instruction;
}

}  // namespace naim::controller
