#include "interaction/interaction_request_heuristics.h"

#include <algorithm>
#include <cctype>
#include <vector>

namespace naim::controller {

std::string InteractionRequestHeuristics::LastUserMessageContent(
    const nlohmann::json& payload) const {
  if (!payload.contains("messages") || !payload.at("messages").is_array()) {
    return "";
  }
  const auto& messages = payload.at("messages");
  for (auto it = messages.rbegin(); it != messages.rend(); ++it) {
    if ((*it).is_object() &&
        (*it).value("role", std::string{}) == "user" &&
        (*it).contains("content") &&
        (*it).at("content").is_string()) {
      return (*it).at("content").get<std::string>();
    }
  }
  return "";
}

bool InteractionRequestHeuristics::LooksLikeLongFormTaskRequest(
    const std::string& text) const {
  const std::string normalized = NormalizeInteractionText(text);
  if (normalized.size() >= 280) {
    return true;
  }
  static const std::vector<std::string> explicit_long_markers = {
      "несколько сообщений", "разбивай на несколько сообщений",
      "разбей на несколько сообщений", "продолжай в нескольких сообщениях",
      "in several messages", "multiple messages",
      "split across multiple messages", "continue in multiple messages",
      "2048 слов", "1024 слов", "1536 слов", "2048 words", "1024 words",
      "1536 words", "2048 токен", "1024 токен", "2048 token", "1024 token",
  };
  if (ContainsAnySubstring(normalized, explicit_long_markers)) {
    return true;
  }
  static const std::vector<std::string> long_form_keywords = {
      "напиши историю", "напиши рассказ", "напиши эссе", "напиши статью",
      "подробный план", "подробно опиши", "развернуто опиши", "детально опиши",
      "историю", "рассказ", "эссе", "статью", "гайд", "руководство",
      "write a story", "write an essay", "write an article", "write a guide",
      "detailed plan", "detailed analysis", "long-form", "long form",
      "структуру проекта", "архитектуру проекта", "изучи весь проект",
      "объясни проект", "проследи путь", "по всему репозиторию",
      "repository-wide", "repo-wide", "entire repository", "whole repository",
      "cross-file", "cross file", "project structure", "project architecture",
      "trace the path", "explain the repository", "analyze the whole project",
  };
  return ContainsAnySubstring(normalized, long_form_keywords);
}

bool InteractionRequestHeuristics::LooksLikeRepositoryAnalysisRequest(
    const std::string& text) const {
  const std::string normalized = NormalizeInteractionText(text);
  static const std::vector<std::string> analysis_markers = {
      "репозитор", "структур", "архитектур", "по проекту", "по репозиторию",
      "изучи проект", "изучи репозиторий", "кодовая база", "проследи путь",
      "cross-file", "cross file", "repo-wide", "repository-wide", "repository",
      "repo ", "project structure", "project architecture", "trace the path",
      "analyze the project", "analyze the repository", "codebase", "code base",
      "grounded in files", "with file references",
  };
  return ContainsAnySubstring(normalized, analysis_markers);
}

std::string InteractionRequestHeuristics::CanonicalResponseMode(
    const std::string& value) const {
  return NormalizeInteractionText(value);
}

std::string InteractionRequestHeuristics::NormalizeInteractionText(
    const std::string& value) const {
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

bool InteractionRequestHeuristics::ContainsAnySubstring(
    const std::string& haystack,
    const std::vector<std::string>& needles) const {
  return std::any_of(
      needles.begin(),
      needles.end(),
      [&](const std::string& needle) {
        return !needle.empty() && haystack.find(needle) != std::string::npos;
      });
}

}  // namespace naim::controller
