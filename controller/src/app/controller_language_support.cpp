#include "app/controller_language_support.h"

#include <cctype>

namespace naim::controller {
namespace {

bool IsSupportedResponseLanguage(const std::string& normalized) {
  return normalized == "en" ||
         normalized == "es" ||
         normalized == "pt" ||
         normalized == "zh" ||
         normalized == "zh_cn" ||
         normalized == "zh_tw";
}

}  // namespace

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
  if (normalized == "en") {
    return "English";
  }
  if (normalized == "es") {
    return "Spanish";
  }
  if (normalized == "pt" || normalized == "pt_br" || normalized == "pt_pt") {
    return "Portuguese";
  }
  if (normalized == "zh" || normalized == "zh_cn" || normalized == "zh_tw") {
    return "Chinese";
  }
  return "English";
}

std::optional<std::string> ControllerLanguageSupport::ResolveInteractionPreferredLanguage(
    const naim::DesiredState& desired_state,
    const nlohmann::json& payload) {
  if (payload.contains("preferred_language") &&
      payload.at("preferred_language").is_string()) {
    const std::string preferred = NormalizeLanguageCode(
        payload.at("preferred_language").get<std::string>());
    if (!preferred.empty()) {
      return IsSupportedResponseLanguage(preferred) ? std::optional<std::string>(preferred)
                                                    : std::nullopt;
    }
  }
  if (desired_state.interaction.has_value() &&
      !desired_state.interaction->default_response_language.empty()) {
    const std::string configured = NormalizeLanguageCode(
        desired_state.interaction->default_response_language);
    if (IsSupportedResponseLanguage(configured)) {
      return configured;
    }
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
  const std::string fallback_instruction =
      "Response language requirement: Reply in the same language as the user's last message only if it is English, Spanish, Portuguese, or Chinese; otherwise reply in English. Never default to Chinese unless the user explicitly requests Chinese.";
  if (desired_state.interaction.has_value()) {
    if (desired_state.interaction->follow_user_language) {
      return fallback_instruction + no_reasoning_instruction;
    }
    if (!desired_state.interaction->default_response_language.empty()) {
      const std::string configured = NormalizeLanguageCode(
          desired_state.interaction->default_response_language);
      if (!IsSupportedResponseLanguage(configured)) {
        return "Response language requirement: Reply in English. The configured default response language is unsupported." +
               no_reasoning_instruction;
      }
      return "Response language requirement: Reply in " +
             LanguageLabel(configured) +
             ". Ignore the model's default language preferences. Never default to Chinese unless the user explicitly requests Chinese." +
             no_reasoning_instruction;
    }
  }
  return fallback_instruction + no_reasoning_instruction;
}

}  // namespace naim::controller
