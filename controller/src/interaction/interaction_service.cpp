#include "interaction/interaction_service.h"

#include "browsing/interaction_browsing_service.h"
#include "browsing/plane_browsing_service.h"
#include <algorithm>
#include <atomic>
#include <array>
#include <cctype>
#include <chrono>
#include <set>
#include <sstream>
#include <thread>

#include "skills/plane_skills_service.h"
#include "comet/runtime/model_adapter.h"
#include "comet/state/worker_group_topology.h"

namespace comet::controller {

namespace {

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

std::string NormalizeInteractionText(const std::string& value) {
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

std::string LowercaseCopy(const std::string& value) {
  std::string lowered;
  lowered.reserve(value.size());
  for (unsigned char ch : value) {
    lowered.push_back(static_cast<char>(std::tolower(ch)));
  }
  return lowered;
}

bool DecodeAvailableChunkedHttpBody(
    std::string& encoded,
    std::string* decoded,
    bool* stream_finished) {
  bool progressed = false;
  while (true) {
    const std::size_t line_end = encoded.find("\r\n");
    if (line_end == std::string::npos) {
      return progressed;
    }
    std::string chunk_size_text = encoded.substr(0, line_end);
    const std::size_t extensions = chunk_size_text.find(';');
    if (extensions != std::string::npos) {
      chunk_size_text = chunk_size_text.substr(0, extensions);
    }
    chunk_size_text = TrimCopy(chunk_size_text);
    std::size_t chunk_size = 0;
    try {
      chunk_size =
          static_cast<std::size_t>(std::stoull(chunk_size_text, nullptr, 16));
    } catch (const std::exception&) {
      throw std::runtime_error(
          "invalid HTTP chunk size '" + chunk_size_text + "'");
    }

    const std::size_t chunk_data_begin = line_end + 2;
    if (chunk_size == 0) {
      if (encoded.size() < chunk_data_begin + 2) {
        return progressed;
      }
      encoded.erase(0, chunk_data_begin + 2);
      *stream_finished = true;
      return true;
    }

    if (encoded.size() < chunk_data_begin + chunk_size + 2) {
      return progressed;
    }
    decoded->append(encoded, chunk_data_begin, chunk_size);
    encoded.erase(0, chunk_data_begin + chunk_size + 2);
    progressed = true;
  }
}

comet::runtime::ModelIdentity BuildResolutionModelIdentity(
    const PlaneInteractionResolution& resolution) {
  comet::runtime::ModelIdentity identity;
  identity.model_id = resolution.status_payload.value("active_model_id", std::string{});
  identity.served_model_name =
      resolution.status_payload.value("served_model_name", std::string{});
  if (resolution.runtime_status.has_value()) {
    if (identity.model_id.empty()) {
      identity.model_id = resolution.runtime_status->active_model_id;
    }
    if (identity.served_model_name.empty()) {
      identity.served_model_name = resolution.runtime_status->active_served_model_name;
    }
    identity.cached_local_model_path = resolution.runtime_status->cached_local_model_path;
    identity.cached_runtime_model_path = resolution.runtime_status->model_path;
    identity.runtime_profile = resolution.runtime_status->active_runtime_profile;
  }
  return identity;
}

int ClampInteractionPolicyValue(int value, int minimum, int maximum) {
  return std::max(minimum, std::min(value, maximum));
}

std::string LastUserMessageContent(const nlohmann::json& payload) {
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

bool ContainsAnySubstring(
    const std::string& haystack,
    const std::vector<std::string>& needles) {
  for (const auto& needle : needles) {
    if (!needle.empty() && haystack.find(needle) != std::string::npos) {
      return true;
    }
  }
  return false;
}

bool LooksLikeLongFormTaskRequest(const std::string& text) {
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

bool LooksLikeRepositoryAnalysisRequest(const std::string& text) {
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

bool PayloadContainsUnsupportedInteractionField(
    const nlohmann::json& payload,
    std::string* field_name) {
  static const std::vector<std::string> unsupported_fields = {
      "tools",
      "tool_choice",
      "functions",
      "function_call",
  };
  for (const auto& field : unsupported_fields) {
    if (payload.contains(field)) {
      if (field_name != nullptr) {
        *field_name = field;
      }
      return true;
    }
  }
  return false;
}

std::optional<std::string> ParsePublicSessionId(const nlohmann::json& payload) {
  if (!payload.contains("session_id") || payload.at("session_id").is_null()) {
    return std::nullopt;
  }
  if (!payload.at("session_id").is_string()) {
    throw std::runtime_error("session_id must be a string");
  }
  const std::string session_id = TrimCopy(payload.at("session_id").get<std::string>());
  if (session_id.empty()) {
    throw std::runtime_error("session_id must not be empty");
  }
  return session_id;
}

nlohmann::json RequestAppliedSkills(const InteractionRequestContext& request_context) {
  if (request_context.payload.contains(PlaneSkillsService::kAppliedSkillsPayloadKey) &&
      request_context.payload.at(PlaneSkillsService::kAppliedSkillsPayloadKey).is_array()) {
    return request_context.payload.at(PlaneSkillsService::kAppliedSkillsPayloadKey);
  }
  return nlohmann::json::array();
}

nlohmann::json RequestAutoAppliedSkills(
    const InteractionRequestContext& request_context) {
  if (request_context.payload.contains(
          PlaneSkillsService::kAutoAppliedSkillsPayloadKey) &&
      request_context.payload.at(
          PlaneSkillsService::kAutoAppliedSkillsPayloadKey).is_array()) {
    return request_context.payload.at(
        PlaneSkillsService::kAutoAppliedSkillsPayloadKey);
  }
  return nlohmann::json::array();
}

std::optional<std::string> RequestSkillsSessionId(
    const InteractionRequestContext& request_context) {
  if (request_context.payload.contains(PlaneSkillsService::kSkillsSessionIdPayloadKey) &&
      request_context.payload.at(PlaneSkillsService::kSkillsSessionIdPayloadKey).is_string()) {
    return request_context.payload.at(PlaneSkillsService::kSkillsSessionIdPayloadKey)
        .get<std::string>();
  }
  return std::nullopt;
}

std::string RequestSkillResolutionMode(
    const InteractionRequestContext& request_context) {
  if (request_context.payload.contains(
          PlaneSkillsService::kSkillResolutionModePayloadKey) &&
      request_context.payload.at(
          PlaneSkillsService::kSkillResolutionModePayloadKey).is_string()) {
    return request_context.payload.at(
        PlaneSkillsService::kSkillResolutionModePayloadKey)
        .get<std::string>();
  }
  return "none";
}

nlohmann::json RequestBrowsingSummary(
    const InteractionRequestContext& request_context) {
  if (request_context.payload.contains(
          InteractionBrowsingService::kSummaryPayloadKey) &&
      request_context.payload.at(
          InteractionBrowsingService::kSummaryPayloadKey).is_object()) {
    return request_context.payload.at(
        InteractionBrowsingService::kSummaryPayloadKey);
  }
  return nlohmann::json{
      {"mode", "disabled"},
      {"mode_source", "default_off"},
      {"plane_enabled", false},
      {"ready", false},
      {"toggle_only", false},
      {"decision", "disabled"},
      {"reason", "web_mode_disabled"},
      {"lookup_state", "disabled"},
      {"lookup_attempted", false},
      {"lookup_required", false},
      {"evidence_attached", false},
      {"searches", nlohmann::json::array()},
      {"sources", nlohmann::json::array()},
      {"errors", nlohmann::json::array()},
      {"indicator",
       nlohmann::json{
           {"compact", "web:off"},
           {"label", "Web disabled"},
           {"active", false},
           {"ready", false},
           {"lookup_state", "disabled"},
           {"lookup_attempted", false},
           {"search_count", 0},
           {"source_count", 0},
           {"error_count", 0},
       }},
      {"trace",
       nlohmann::json::array(
           {nlohmann::json{{"stage", "mode"}, {"status", "off"}, {"compact", "web:off"}},
            nlohmann::json{{"stage", "decision"},
                           {"status", "disabled"},
                           {"compact", "decide:disabled"}}})},
  };
}

bool IsUtf8ContinuationByte(unsigned char value) {
  return (value & 0xC0) == 0x80;
}

std::size_t Utf8SequenceLength(unsigned char lead) {
  if ((lead & 0x80) == 0) {
    return 1;
  }
  if ((lead & 0xE0) == 0xC0) {
    return 2;
  }
  if ((lead & 0xF0) == 0xE0) {
    return 3;
  }
  if ((lead & 0xF8) == 0xF0) {
    return 4;
  }
  return 0;
}

std::size_t ValidUtf8PrefixLength(const std::string& value) {
  std::size_t index = 0;
  while (index < value.size()) {
    const unsigned char lead = static_cast<unsigned char>(value[index]);
    const std::size_t sequence_length = Utf8SequenceLength(lead);
    if (sequence_length == 0) {
      break;
    }
    if (index + sequence_length > value.size()) {
      break;
    }
    bool valid = true;
    for (std::size_t offset = 1; offset < sequence_length; ++offset) {
      if (!IsUtf8ContinuationByte(
              static_cast<unsigned char>(value[index + offset]))) {
        valid = false;
        break;
      }
    }
    if (!valid) {
      break;
    }
    index += sequence_length;
  }
  return index;
}

std::string HybridReplicaGroupKey(const comet::WorkerGroupMemberSpec& member) {
  const std::string node_name = member.node_name.empty() ? std::string("unknown-node")
                                                         : member.node_name;
  return "hybrid-node-" + node_name + "-start-" +
         std::to_string(std::max(0, member.data_parallel_start_rank));
}

int ExpectedHybridApiEndpoints(const comet::DesiredState& desired_state) {
  std::set<std::string> keys;
  for (const auto& member : desired_state.worker_group.members) {
    if (!member.enabled || !member.data_parallel_api_endpoint) {
      continue;
    }
    keys.insert(HybridReplicaGroupKey(member));
  }
  return static_cast<int>(keys.size());
}

}  // namespace

InteractionCompletionPolicy NormalizeConfiguredInteractionCompletionPolicy(
    const comet::InteractionSettings::CompletionPolicy& configured_policy) {
  InteractionCompletionPolicy policy;
  const std::string normalized_mode =
      NormalizeInteractionText(configured_policy.response_mode);
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

InteractionCompletionPolicy DefaultChatInteractionCompletionPolicy() {
  comet::InteractionSettings::CompletionPolicy configured_policy;
  configured_policy.response_mode = "normal";
  configured_policy.max_tokens = 512;
  configured_policy.max_continuations = 0;
  configured_policy.max_total_completion_tokens = 512;
  configured_policy.max_elapsed_time_ms = 30000;
  return NormalizeConfiguredInteractionCompletionPolicy(configured_policy);
}

ResolvedInteractionPolicy ResolveInteractionCompletionPolicy(
    const comet::DesiredState& desired_state,
    const nlohmann::json& payload) {
  ResolvedInteractionPolicy resolved;
  const std::string last_user_message = LastUserMessageContent(payload);
  const bool long_form_task = LooksLikeLongFormTaskRequest(last_user_message);
  const bool repository_analysis =
      LooksLikeRepositoryAnalysisRequest(last_user_message);
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
    if (repository_analysis && long_form_task &&
        interaction.analysis_long_completion_policy.has_value()) {
      resolved.policy = NormalizeConfiguredInteractionCompletionPolicy(
          *interaction.analysis_long_completion_policy);
      resolved.mode = "analysis-long";
      return finalize_resolved(std::move(resolved));
    }
    if (repository_analysis && !long_form_task &&
        interaction.analysis_completion_policy.has_value()) {
      resolved.policy = NormalizeConfiguredInteractionCompletionPolicy(
          *interaction.analysis_completion_policy);
      resolved.mode = "analysis-default";
      return finalize_resolved(std::move(resolved));
    }
    if (long_form_task && interaction.long_completion_policy.has_value()) {
      resolved.policy = NormalizeConfiguredInteractionCompletionPolicy(
          *interaction.long_completion_policy);
      resolved.mode = "long";
      return finalize_resolved(std::move(resolved));
    }
    if (!long_form_task &&
        interaction.completion_policy.has_value() &&
        NormalizeInteractionText(interaction.completion_policy->response_mode) !=
            "long" &&
        NormalizeInteractionText(interaction.completion_policy->response_mode) !=
            "very_long") {
      resolved.policy = NormalizeConfiguredInteractionCompletionPolicy(
          *interaction.completion_policy);
      resolved.mode = "default";
      return finalize_resolved(std::move(resolved));
    }
    if (long_form_task &&
        interaction.completion_policy.has_value() &&
        (NormalizeInteractionText(interaction.completion_policy->response_mode) ==
             "long" ||
         NormalizeInteractionText(interaction.completion_policy->response_mode) ==
             "very_long")) {
      resolved.policy = NormalizeConfiguredInteractionCompletionPolicy(
          *interaction.completion_policy);
      resolved.mode = "long";
      return finalize_resolved(std::move(resolved));
    }
  }
  resolved.policy = DefaultChatInteractionCompletionPolicy();
  resolved.mode = repository_analysis
                      ? (long_form_task ? "analysis-long-fallback"
                                        : "analysis-default-fallback")
                      : (long_form_task ? "long-fallback"
                                        : "default-fallback");
  return finalize_resolved(std::move(resolved));
}

std::string GenerateInteractionRequestId() {
  static std::atomic<unsigned long long> counter{0};
  const auto now = std::chrono::duration_cast<std::chrono::milliseconds>(
                       std::chrono::system_clock::now().time_since_epoch())
                       .count();
  return "req-" + std::to_string(now) + "-" + std::to_string(++counter);
}

std::string GenerateInteractionSessionId() {
  static std::atomic<unsigned long long> counter{0};
  const auto now = std::chrono::duration_cast<std::chrono::milliseconds>(
                       std::chrono::system_clock::now().time_since_epoch())
                       .count();
  return "sess-" + std::to_string(now) + "-" + std::to_string(++counter);
}

std::string BuildRepositoryAnalysisInstruction() {
  return "Repository analysis requirement: answer only from the repository or "
         "codebase evidence provided in the request. "
         "Cite concrete file paths for repo-specific claims. If evidence is "
         "insufficient, say exactly what is unknown. "
         "Do not claim lack of filesystem access when repository context is "
         "already present.";
}

std::string BuildSemanticCompletionInstruction(
    const InteractionCompletionPolicy& policy) {
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

std::string BuildContinuationPrompt(
    const InteractionCompletionPolicy& policy,
    bool natural_stop_without_marker,
    const std::string& trailing_excerpt,
    int remaining_completion_tokens,
    bool hidden_thinking_mode,
    bool visible_output_started) {
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

namespace {

std::string RemoveThinkBlocks(std::string value) {
  while (true) {
    const std::size_t begin = value.find("<think>");
    if (begin == std::string::npos) {
      return value;
    }
    const std::size_t end = value.find("</think>", begin);
    if (end == std::string::npos) {
      return value.substr(0, begin);
    }
    value.erase(begin, end + std::string("</think>").size() - begin);
  }
}

std::vector<std::string> SplitParagraphs(const std::string& value) {
  std::vector<std::string> paragraphs;
  std::string current;
  bool last_blank = false;
  std::istringstream input(value);
  std::string line;
  while (std::getline(input, line)) {
    const bool blank = TrimCopy(line).empty();
    if (blank) {
      if (!current.empty()) {
        paragraphs.push_back(TrimCopy(current));
        current.clear();
      }
      last_blank = true;
      continue;
    }
    if (!current.empty()) {
      current += last_blank ? "\n" : "\n";
    }
    current += line;
    last_blank = false;
  }
  if (!current.empty()) {
    paragraphs.push_back(TrimCopy(current));
  }
  return paragraphs;
}

}  // namespace

bool StartsWithReasoningPreamble(const std::string& text) {
  const std::string lowered = LowercaseCopy(TrimCopy(text));
  return lowered.rfind("thinking process:", 0) == 0 ||
         lowered.rfind("reasoning:", 0) == 0 ||
         lowered.rfind("analysis:", 0) == 0 ||
         lowered.rfind("chain of thought:", 0) == 0;
}

std::string SanitizeInteractionText(std::string text) {
  text = RemoveThinkBlocks(std::move(text));
  text = TrimCopy(text);
  if (StartsWithReasoningPreamble(text)) {
    const auto paragraphs = SplitParagraphs(text);
    for (auto it = paragraphs.rbegin(); it != paragraphs.rend(); ++it) {
      const std::string candidate = TrimCopy(*it);
      if (candidate.empty()) {
        continue;
      }
      const std::string lowered = LowercaseCopy(candidate);
      if (StartsWithReasoningPreamble(candidate) ||
          lowered.rfind("1.", 0) == 0 ||
          lowered.rfind("2.", 0) == 0 ||
          lowered.rfind("3.", 0) == 0 ||
          lowered.rfind("* ", 0) == 0) {
        continue;
      }
      return candidate;
    }
  }
  return text;
}

std::string Utf8SafeSuffix(const std::string& value, std::size_t max_bytes) {
  if (value.size() <= max_bytes) {
    return value;
  }
  std::size_t start = value.size() - max_bytes;
  while (start < value.size() &&
         IsUtf8ContinuationByte(static_cast<unsigned char>(value[start]))) {
    ++start;
  }
  return value.substr(start);
}

bool SessionReachedTargetLength(
    const InteractionCompletionPolicy& policy,
    int total_completion_tokens) {
  return !policy.target_completion_tokens.has_value() ||
         total_completion_tokens >= *policy.target_completion_tokens;
}

bool CanCompleteOnNaturalStop(
    const InteractionCompletionPolicy& policy,
    const InteractionSegmentSummary& summary) {
  if (policy.require_completion_marker) {
    return false;
  }
  return summary.finish_reason != "length" && !TrimCopy(summary.text).empty();
}

std::string RemoveCompletionMarkers(
    const std::string& input,
    const std::string& marker,
    bool* marker_seen) {
  std::string output = input;
  std::size_t position = std::string::npos;
  while ((position = output.find(marker)) != std::string::npos) {
    if (marker_seen != nullptr) {
      *marker_seen = true;
    }
    output.erase(position, marker.size());
  }
  return output;
}

std::string ConsumeCompletionMarkerFilteredChunk(
    CompletionMarkerFilterState& state,
    const std::string& chunk,
    const std::string& marker,
    bool final_flush) {
  state.pending += chunk;
  std::string emitted;
  while (true) {
    const std::size_t marker_pos = state.pending.find(marker);
    if (marker_pos != std::string::npos) {
      emitted += state.pending.substr(0, marker_pos);
      state.pending.erase(0, marker_pos + marker.size());
      state.marker_seen = true;
      continue;
    }
    if (final_flush) {
      emitted += state.pending;
      state.pending.clear();
      break;
    }
    if (state.pending.size() > marker.size()) {
      const std::size_t safe_prefix = state.pending.size() - marker.size() + 1;
      const std::string candidate = state.pending.substr(0, safe_prefix);
      const std::size_t valid_prefix = ValidUtf8PrefixLength(candidate);
      if (valid_prefix == 0) {
        break;
      }
      emitted += state.pending.substr(0, valid_prefix);
      state.pending.erase(0, valid_prefix);
    }
    break;
  }
  return emitted;
}

std::string ExtractInteractionText(const nlohmann::json& payload) {
  if (!payload.contains("choices") || !payload.at("choices").is_array() ||
      payload.at("choices").empty()) {
    throw std::runtime_error(
        "upstream interaction response did not include choices");
  }
  const auto& choice = payload.at("choices").at(0);
  if (choice.contains("message") && choice.at("message").is_object()) {
    return SanitizeInteractionText(
        choice.at("message").value("content", std::string{}));
  }
  return SanitizeInteractionText(choice.value("text", std::string{}));
}

std::string ExtractInteractionFinishReason(const nlohmann::json& payload) {
  if (!payload.contains("choices") || !payload.at("choices").is_array() ||
      payload.at("choices").empty()) {
    return "stop";
  }
  const auto& choice = payload.at("choices").at(0);
  return choice.value("finish_reason", std::string{"stop"});
}

nlohmann::json ExtractInteractionUsage(const nlohmann::json& payload) {
  if (!payload.contains("usage") || !payload.at("usage").is_object()) {
    return nlohmann::json{
        {"prompt_tokens", 0},
        {"completion_tokens", 0},
        {"total_tokens", 0},
    };
  }
  const auto& usage = payload.at("usage");
  return nlohmann::json{
      {"prompt_tokens", usage.value("prompt_tokens", 0)},
      {"completion_tokens", usage.value("completion_tokens", 0)},
      {"total_tokens", usage.value("total_tokens", 0)},
  };
}

bool TryConsumeSseFrame(std::string& buffer, InteractionSseFrame* frame) {
  const std::size_t separator = buffer.find("\n\n");
  if (separator == std::string::npos) {
    return false;
  }
  const std::string raw_frame = buffer.substr(0, separator);
  buffer.erase(0, separator + 2);
  frame->event_name = "message";
  frame->data.clear();
  std::stringstream stream(raw_frame);
  std::string line;
  std::vector<std::string> data_lines;
  while (std::getline(stream, line)) {
    if (!line.empty() && line.back() == '\r') {
      line.pop_back();
    }
    const std::string trimmed_line = TrimCopy(line);
    if (line.empty() || line[0] == ':') {
      continue;
    }
    if (!trimmed_line.empty()) {
      const std::size_t extensions = trimmed_line.find(';');
      const std::string chunk_size_candidate =
          TrimCopy(trimmed_line.substr(
              0, extensions == std::string::npos ? trimmed_line.size()
                                                 : extensions));
      const bool looks_like_chunk_size =
          !chunk_size_candidate.empty() &&
          std::all_of(
              chunk_size_candidate.begin(),
              chunk_size_candidate.end(),
              [](unsigned char ch) { return std::isxdigit(ch) != 0; });
      if (looks_like_chunk_size) {
        continue;
      }
    }
    if (line.rfind("event:", 0) == 0) {
      frame->event_name = TrimCopy(line.substr(6));
      continue;
    }
    if (line.rfind("data:", 0) == 0) {
      const std::size_t offset = line.size() > 5 && line[5] == ' ' ? 6 : 5;
      data_lines.push_back(line.substr(offset));
    }
  }
  for (std::size_t index = 0; index < data_lines.size(); ++index) {
    if (index > 0) {
      frame->data.push_back('\n');
    }
    frame->data += data_lines[index];
  }
  return true;
}

std::optional<std::string> ParseInteractionStreamPlaneName(
    const std::string& request_method,
    const std::string& request_path) {
  if (request_method != "POST") {
    return std::nullopt;
  }
  constexpr std::string_view kPrefix = "/api/v1/planes/";
  constexpr std::string_view kSuffix = "/interaction/chat/completions/stream";
  if (request_path.rfind(std::string(kPrefix), 0) != 0) {
    return std::nullopt;
  }
  if (request_path.size() <= kPrefix.size() + kSuffix.size()) {
    return std::nullopt;
  }
  if (!request_path.ends_with(kSuffix)) {
    return std::nullopt;
  }
  return request_path.substr(
      kPrefix.size(),
      request_path.size() - kPrefix.size() - kSuffix.size());
}

std::map<std::string, std::string> BuildInteractionResponseHeaders(
    const std::string& request_id) {
  return {
      {"X-Comet-Request-Id", request_id},
  };
}

std::string ResolveInteractionServedModelName(
    const PlaneInteractionResolution& resolution) {
  if (resolution.runtime_status.has_value() &&
      !resolution.runtime_status->active_served_model_name.empty()) {
    return resolution.runtime_status->active_served_model_name;
  }
  return resolution.status_payload.value("served_model_name", std::string{});
}

std::string ResolveInteractionActiveModelId(
    const PlaneInteractionResolution& resolution) {
  if (resolution.runtime_status.has_value() &&
      !resolution.runtime_status->active_model_id.empty()) {
    return resolution.runtime_status->active_model_id;
  }
  return resolution.status_payload.value("active_model_id", std::string{});
}

nlohmann::json BuildInteractionContractMetadata(
    const PlaneInteractionResolution& resolution,
    const std::string& request_id,
    const std::optional<std::string>& session_id,
    const std::optional<int>& segment_count,
    const std::optional<int>& continuation_count) {
  nlohmann::json metadata{
      {"request_id", request_id},
      {"plane_name", resolution.status_payload.value("plane_name", std::string{})},
      {"served_model_name", ResolveInteractionServedModelName(resolution)},
      {"active_model_id", ResolveInteractionActiveModelId(resolution)},
      {"reason", resolution.status_payload.value("reason", std::string{})},
  };
  if (session_id.has_value()) {
    metadata["session_id"] = *session_id;
  }
  if (segment_count.has_value()) {
    metadata["segment_count"] = *segment_count;
  }
  if (continuation_count.has_value()) {
    metadata["continuation_count"] = *continuation_count;
  }
  if (resolution.desired_state.interaction.has_value()) {
    if (resolution.desired_state.interaction->completion_policy.has_value()) {
      metadata["completion_policy"] = nlohmann::json{
          {"response_mode",
           resolution.desired_state.interaction->completion_policy->response_mode},
          {"max_tokens",
           resolution.desired_state.interaction->completion_policy->max_tokens},
          {"thinking_enabled",
           resolution.desired_state.interaction->thinking_enabled},
      };
    }
    if (resolution.desired_state.interaction->long_completion_policy.has_value()) {
      metadata["long_completion_policy"] = nlohmann::json{
          {"response_mode",
           resolution.desired_state.interaction->long_completion_policy->response_mode},
          {"max_tokens",
           resolution.desired_state.interaction->long_completion_policy->max_tokens},
      };
    }
    if (resolution.desired_state.interaction->analysis_completion_policy.has_value()) {
      metadata["analysis_completion_policy"] = nlohmann::json{
          {"response_mode",
           resolution.desired_state.interaction->analysis_completion_policy->response_mode},
          {"max_tokens",
           resolution.desired_state.interaction->analysis_completion_policy->max_tokens},
      };
    }
    if (resolution.desired_state.interaction->analysis_long_completion_policy
            .has_value()) {
      metadata["analysis_long_completion_policy"] = nlohmann::json{
          {"response_mode",
           resolution.desired_state.interaction->analysis_long_completion_policy
               ->response_mode},
          {"max_tokens",
           resolution.desired_state.interaction->analysis_long_completion_policy
               ->max_tokens},
      };
    }
  }
  return metadata;
}

nlohmann::json InteractionRequestValidator::ParsePayload(
    const std::string& body) const {
  return body.empty() ? nlohmann::json::object() : nlohmann::json::parse(body);
}

std::optional<InteractionValidationError>
InteractionRequestValidator::ValidateAndNormalizeRequest(
    const PlaneInteractionResolution& resolution,
    const nlohmann::json& original_payload,
    InteractionRequestContext* context) const {
  if (context == nullptr) {
    throw std::invalid_argument("interaction request context is required");
  }
  if (!original_payload.is_object()) {
    return InteractionValidationError{
        "malformed_request",
        "interaction request body must be a JSON object",
        false,
        nlohmann::json::object(),
    };
  }

  nlohmann::json payload = original_payload;
  context->original_payload = original_payload;
  std::string unsupported_field;
  if (PayloadContainsUnsupportedInteractionField(payload, &unsupported_field)) {
    return InteractionValidationError{
        "unsupported_field",
        "interaction request field '" + unsupported_field +
            "' is not supported by comet-node",
        false,
        nlohmann::json{{"field", unsupported_field}},
    };
  }

  if (payload.contains("response_format")) {
    if (payload.at("response_format").is_object()) {
      const std::string response_format_type =
          payload.at("response_format").value("type", std::string{});
      if (response_format_type == "json_object") {
        context->structured_output_json = true;
      } else {
        return InteractionValidationError{
            "unsupported_response_format",
            "only response_format.type=json_object is supported",
            false,
            nlohmann::json{{"field", "response_format"},
                           {"supported_type", "json_object"},
                           {"received_type", response_format_type}},
        };
      }
    } else {
      return InteractionValidationError{
          "malformed_request",
          "response_format must be an object",
          false,
          nlohmann::json{{"field", "response_format"}},
      };
    }
  }

  if (!payload.contains("messages") || !payload.at("messages").is_array()) {
    return InteractionValidationError{
        "malformed_request",
        "chat completion request is missing messages array",
        false,
        nlohmann::json{{"field", "messages"}},
    };
  }

  try {
    context->requested_session_id = ParsePublicSessionId(payload);
  } catch (const std::exception& error) {
    return InteractionValidationError{
        "malformed_request",
        error.what(),
        false,
        nlohmann::json{{"field", "session_id"}},
    };
  }
  context->client_messages = payload.at("messages");

  const std::string served_model_name = ResolveInteractionServedModelName(resolution);
  const std::string active_model_id = ResolveInteractionActiveModelId(resolution);
  if (payload.contains("model") && !payload.at("model").is_null()) {
    if (!payload.at("model").is_string()) {
      return InteractionValidationError{
          "malformed_request",
          "model must be a string when provided",
          false,
          nlohmann::json{{"field", "model"}},
      };
    }
    const std::string requested_model = payload.at("model").get<std::string>();
    const bool matches_served =
        !served_model_name.empty() && requested_model == served_model_name;
    const bool matches_root =
        !active_model_id.empty() && requested_model == active_model_id;
    if (!requested_model.empty() && !matches_served && !matches_root) {
      return InteractionValidationError{
          "model_mismatch",
          "requested model does not match the active model for this plane",
          false,
          nlohmann::json{{"requested_model", requested_model},
                         {"served_model_name", served_model_name},
                         {"active_model_id", active_model_id}},
      };
    }
  }
  if (!served_model_name.empty()) {
    payload["model"] = served_model_name;
    context->normalized_model = served_model_name;
  } else if (!active_model_id.empty()) {
    payload["model"] = active_model_id;
    context->normalized_model = active_model_id;
  }

  if (payload.contains("auto_skills") && !payload.at("auto_skills").is_boolean()) {
    return InteractionValidationError{
        "malformed_request",
        "auto_skills must be a boolean",
        false,
        nlohmann::json{{"field", "auto_skills"}},
    };
  }
  if (!payload.contains("auto_skills")) {
    payload["auto_skills"] = true;
  }

  context->payload = std::move(payload);
  return std::nullopt;
}

nlohmann::json InteractionContractResponder::BuildStandaloneErrorPayload(
    const std::string& request_id,
    const std::string& code,
    const std::string& message,
    bool retryable,
    const std::optional<std::string>& plane_name,
    const std::optional<std::string>& reason,
    const std::optional<std::string>& served_model_name,
    const std::optional<std::string>& active_model_id,
    const nlohmann::json& details) const {
  nlohmann::json payload{
      {"request_id", request_id},
      {"plane_name", plane_name.has_value() ? nlohmann::json(*plane_name)
                                            : nlohmann::json(nullptr)},
      {"reason", reason.has_value() ? nlohmann::json(*reason)
                                    : nlohmann::json(nullptr)},
      {"served_model_name",
       served_model_name.has_value() ? nlohmann::json(*served_model_name)
                                     : nlohmann::json(nullptr)},
      {"active_model_id",
       active_model_id.has_value() ? nlohmann::json(*active_model_id)
                                   : nlohmann::json(nullptr)},
      {"error",
       nlohmann::json{
           {"code", code},
           {"message", message},
           {"retryable", retryable},
       }},
      {"comet",
       nlohmann::json{
           {"request_id", request_id},
           {"plane_name", plane_name.has_value() ? nlohmann::json(*plane_name)
                                                 : nlohmann::json(nullptr)},
           {"served_model_name",
            served_model_name.has_value() ? nlohmann::json(*served_model_name)
                                          : nlohmann::json(nullptr)},
           {"active_model_id",
            active_model_id.has_value() ? nlohmann::json(*active_model_id)
                                        : nlohmann::json(nullptr)},
       }},
  };
  if (!details.empty()) {
    payload["error"]["details"] = details;
  }
  return payload;
}

nlohmann::json InteractionContractResponder::BuildPlaneErrorPayload(
    const PlaneInteractionResolution& resolution,
    const std::string& request_id,
    const std::string& code,
    const std::string& message,
    bool retryable,
    const nlohmann::json& details) const {
  nlohmann::json payload = resolution.status_payload;
  payload["request_id"] = request_id;
  payload["error"] = nlohmann::json{
      {"code", code},
      {"message", message},
      {"retryable", retryable},
  };
  payload["comet"] = BuildInteractionContractMetadata(resolution, request_id);
  if (!details.empty()) {
    payload["error"]["details"] = details;
  }
  return payload;
}

nlohmann::json InteractionSessionPresenter::BuildSessionPayload(
    const InteractionSessionResult& result) const {
  nlohmann::json segments = nlohmann::json::array();
  for (const auto& segment : result.segments) {
    segments.push_back(nlohmann::json{
        {"index", segment.index},
        {"continuation_index", segment.continuation_index},
        {"finish_reason", segment.finish_reason},
        {"usage",
         nlohmann::json{
             {"prompt_tokens", segment.prompt_tokens},
             {"completion_tokens", segment.completion_tokens},
             {"total_tokens", segment.total_tokens},
         }},
        {"latency_ms", segment.latency_ms},
        {"marker_seen", segment.marker_seen},
    });
  }
  return nlohmann::json{
      {"id", result.session_id},
      {"status", result.completion_status},
      {"stop_reason", result.stop_reason},
      {"segment_count", static_cast<int>(result.segments.size())},
      {"continuation_count", result.continuation_count},
      {"finish_reason", result.final_finish_reason},
      {"usage",
       nlohmann::json{
           {"prompt_tokens", result.total_prompt_tokens},
           {"completion_tokens", result.total_completion_tokens},
           {"total_tokens", result.total_tokens},
       }},
      {"latency_ms", result.total_latency_ms},
      {"marker_seen", result.marker_seen},
      {"segments", std::move(segments)},
  };
}

std::optional<nlohmann::json>
InteractionSessionPresenter::ParseStructuredOutputObject(
    const std::string& text) const {
  nlohmann::json parsed = nlohmann::json::parse(text, nullptr, false);
  if (parsed.is_discarded() || !parsed.is_object()) {
    return std::nullopt;
  }
  return parsed;
}

InteractionJsonResponseSpec InteractionSessionPresenter::BuildResponseSpec(
    const PlaneInteractionResolution& resolution,
    const InteractionRequestContext& request_context,
    const InteractionSessionResult& result) const {
  const InteractionContractResponder responder;
  nlohmann::json session_payload = BuildSessionPayload(result);
  session_payload["applied_skills"] = RequestAppliedSkills(request_context);
  session_payload["auto_applied_skills"] = RequestAutoAppliedSkills(request_context);
  session_payload["skill_resolution_mode"] =
      RequestSkillResolutionMode(request_context);
  session_payload["browsing"] = RequestBrowsingSummary(request_context);
  session_payload["skills_session_id"] =
      RequestSkillsSessionId(request_context).has_value()
          ? nlohmann::json(*RequestSkillsSessionId(request_context))
          : nlohmann::json(nullptr);
  if (request_context.structured_output_json) {
    if (result.completion_status != "completed") {
      return {
          422,
          responder.BuildPlaneErrorPayload(
              resolution,
              request_context.request_id,
              "structured_output_truncated",
              "structured output session ended before a valid JSON object was completed",
              true,
              nlohmann::json{
                  {"completion_status", result.completion_status},
                  {"stop_reason", result.stop_reason},
                  {"session", session_payload},
              }),
      };
    }
    const auto parsed_json = ParseStructuredOutputObject(result.content);
    if (!parsed_json.has_value()) {
      return {
          422,
          responder.BuildPlaneErrorPayload(
              resolution,
              request_context.request_id,
              "structured_output_malformed",
              "structured output session did not produce a valid JSON object",
              true,
              nlohmann::json{
                  {"completion_status", result.completion_status},
                  {"stop_reason", result.stop_reason},
                  {"content_excerpt",
                   result.content.size() > 512
                       ? result.content.substr(0, 512) + "...[truncated]"
                       : result.content},
                  {"session", session_payload},
              }),
      };
    }
    return {
        200,
        nlohmann::json{
            {"id", "chatcmpl-comet-session"},
            {"object", "chat.completion"},
            {"request_id", request_context.request_id},
            {"model", result.model},
            {"choices",
             nlohmann::json::array({nlohmann::json{
                 {"index", 0},
                 {"message",
                  nlohmann::json{
                      {"role", "assistant"},
                      {"content", result.content},
                  }},
                 {"finish_reason", "stop"},
             }})},
            {"usage", session_payload.at("usage")},
            {"session", session_payload},
            {"applied_skills", RequestAppliedSkills(request_context)},
            {"auto_applied_skills", RequestAutoAppliedSkills(request_context)},
            {"skill_resolution_mode",
             RequestSkillResolutionMode(request_context)},
            {"browsing", RequestBrowsingSummary(request_context)},
            {"skills_session_id",
             RequestSkillsSessionId(request_context).has_value()
                 ? nlohmann::json(*RequestSkillsSessionId(request_context))
                 : nlohmann::json(nullptr)},
            {"comet",
             BuildInteractionContractMetadata(
                 resolution,
                 request_context.request_id,
                 result.session_id,
                 static_cast<int>(result.segments.size()),
                 result.continuation_count)},
            {"structured_output",
             nlohmann::json{
                 {"mode", "json_object"},
                 {"valid", true},
                 {"json", *parsed_json},
             }},
        },
    };
  }
  return {
      200,
      nlohmann::json{
          {"id", "chatcmpl-comet-session"},
          {"object", "chat.completion"},
          {"request_id", request_context.request_id},
          {"model", result.model},
          {"choices",
           nlohmann::json::array({nlohmann::json{
               {"index", 0},
               {"message",
                nlohmann::json{
                    {"role", "assistant"},
                    {"content", result.content},
                }},
               {"finish_reason",
                result.completion_status == "completed" ? "stop" : "length"},
           }})},
          {"usage", session_payload.at("usage")},
          {"session", session_payload},
          {"applied_skills", RequestAppliedSkills(request_context)},
          {"auto_applied_skills", RequestAutoAppliedSkills(request_context)},
          {"skill_resolution_mode",
           RequestSkillResolutionMode(request_context)},
          {"browsing", RequestBrowsingSummary(request_context)},
          {"skills_session_id",
           RequestSkillsSessionId(request_context).has_value()
               ? nlohmann::json(*RequestSkillsSessionId(request_context))
               : nlohmann::json(nullptr)},
          {"comet",
           BuildInteractionContractMetadata(
               resolution,
               request_context.request_id,
               result.session_id,
               static_cast<int>(result.segments.size()),
               result.continuation_count)},
      },
  };
}

nlohmann::json InteractionStreamPresenter::BuildSessionStartedEvent(
    const std::string& request_id,
    const std::string& session_id,
    const std::string& plane_name,
    const PlaneInteractionResolution& resolution,
    const InteractionRequestContext& request_context) const {
  return nlohmann::json{
      {"request_id", request_id},
      {"session_id", session_id},
      {"plane_name", plane_name},
      {"served_model_name", ResolveInteractionServedModelName(resolution)},
      {"active_model_id", ResolveInteractionActiveModelId(resolution)},
      {"applied_skills", RequestAppliedSkills(request_context)},
      {"auto_applied_skills", RequestAutoAppliedSkills(request_context)},
      {"skill_resolution_mode", RequestSkillResolutionMode(request_context)},
      {"browsing", RequestBrowsingSummary(request_context)},
      {"skills_session_id",
       RequestSkillsSessionId(request_context).has_value()
           ? nlohmann::json(*RequestSkillsSessionId(request_context))
           : nlohmann::json(nullptr)},
  };
}

nlohmann::json InteractionStreamPresenter::BuildSegmentStartedEvent(
    const std::string& request_id,
    const std::string& session_id,
    int segment_index) const {
  return nlohmann::json{
      {"request_id", request_id},
      {"session_id", session_id},
      {"segment_index", segment_index},
      {"continuation_index", segment_index},
  };
}

nlohmann::json InteractionStreamPresenter::BuildSegmentCompleteEvent(
    const std::string& request_id,
    const std::string& session_id,
    int segment_index,
    const InteractionSegmentSummary& summary) const {
  return nlohmann::json{
      {"request_id", request_id},
      {"session_id", session_id},
      {"segment_index", segment_index},
      {"continuation_index", segment_index},
      {"finish_reason", summary.finish_reason},
      {"usage",
       nlohmann::json{
           {"prompt_tokens", summary.prompt_tokens},
           {"completion_tokens", summary.completion_tokens},
           {"total_tokens", summary.total_tokens},
       }},
      {"latency_ms", summary.latency_ms},
      {"marker_seen", summary.marker_seen},
  };
}

nlohmann::json InteractionStreamPresenter::BuildContinuationStartedEvent(
    const std::string& request_id,
    const std::string& session_id,
    int continuation_index,
    const InteractionSegmentSummary& summary) const {
  return nlohmann::json{
      {"request_id", request_id},
      {"session_id", session_id},
      {"continuation_index", continuation_index},
      {"reason",
       summary.finish_reason == "length" ? "segment_hit_token_limit"
                                         : "semantic_completion_not_confirmed"},
  };
}

nlohmann::json InteractionStreamPresenter::BuildSessionFailedEvent(
    const std::string& request_id,
    const std::string& session_id,
    const std::string& code,
    const std::string& message,
    bool retryable,
    const InteractionSessionResult& session,
    const std::optional<nlohmann::json>& session_payload) const {
  nlohmann::json payload{
      {"request_id", request_id},
      {"session_id", session_id},
      {"status", "failed"},
      {"error",
       nlohmann::json{
           {"code", code},
           {"message", message},
           {"retryable", retryable},
       }},
      {"message", message},
      {"segment_count", static_cast<int>(session.segments.size())},
      {"continuation_count", session.continuation_count},
  };
  if (session_payload.has_value()) {
    payload["session"] = *session_payload;
  }
  return payload;
}

nlohmann::json InteractionStreamPresenter::BuildErrorEvent(
    const std::string& request_id,
    const std::string& code,
    const std::string& message,
    bool retryable,
    const std::string& plane_name) const {
  return nlohmann::json{
      {"request_id", request_id},
      {"error",
       nlohmann::json{
           {"code", code},
           {"message", message},
           {"retryable", retryable},
       }},
      {"message", message},
      {"plane_name", plane_name},
  };
}

nlohmann::json InteractionStreamPresenter::BuildSessionCompleteEvent(
    const PlaneInteractionResolution& resolution,
    const std::string& request_id,
    const InteractionSessionResult& session,
    const nlohmann::json& session_payload,
    const InteractionRequestContext& request_context) const {
  return nlohmann::json{
      {"request_id", request_id},
      {"session", session_payload},
      {"applied_skills", RequestAppliedSkills(request_context)},
      {"auto_applied_skills", RequestAutoAppliedSkills(request_context)},
      {"skill_resolution_mode", RequestSkillResolutionMode(request_context)},
      {"browsing", RequestBrowsingSummary(request_context)},
      {"skills_session_id",
       RequestSkillsSessionId(request_context).has_value()
           ? nlohmann::json(*RequestSkillsSessionId(request_context))
           : nlohmann::json(nullptr)},
      {"comet",
       BuildInteractionContractMetadata(
           resolution,
           request_id,
           session.session_id,
           static_cast<int>(session.segments.size()),
           session.continuation_count)},
  };
}

nlohmann::json InteractionStreamPresenter::BuildCompleteEvent(
    const PlaneInteractionResolution& resolution,
    const std::string& request_id,
    const InteractionRequestContext& request_context,
    const InteractionSessionResult& session,
    const nlohmann::json& session_payload,
    const std::optional<nlohmann::json>& parsed_structured_output) const {
  return nlohmann::json{
      {"request_id", request_id},
      {"model", session.model},
      {"finish_reason", session.completion_status == "completed" ? "stop" : "length"},
      {"latency_ms", session.total_latency_ms},
      {"usage", session_payload.at("usage")},
      {"session", session_payload},
      {"applied_skills", RequestAppliedSkills(request_context)},
      {"auto_applied_skills", RequestAutoAppliedSkills(request_context)},
      {"skill_resolution_mode", RequestSkillResolutionMode(request_context)},
      {"browsing", RequestBrowsingSummary(request_context)},
      {"skills_session_id",
       RequestSkillsSessionId(request_context).has_value()
           ? nlohmann::json(*RequestSkillsSessionId(request_context))
           : nlohmann::json(nullptr)},
      {"comet",
       BuildInteractionContractMetadata(
           resolution,
           request_id,
           session.session_id,
           static_cast<int>(session.segments.size()),
           session.continuation_count)},
      {"structured_output",
       request_context.structured_output_json
           ? nlohmann::json{
                 {"mode", "json_object"},
                 {"valid", true},
                 {"json", parsed_structured_output.value_or(nlohmann::json::object())},
             }
           : nlohmann::json(nullptr)},
      {"completion_status", session.completion_status},
      {"stop_reason", session.stop_reason},
      {"continuation_count", session.continuation_count},
      {"segment_count", static_cast<int>(session.segments.size())},
  };
}

InteractionSessionExecutor::InteractionSessionExecutor(
    BuildInteractionUpstreamBodyFn build_interaction_upstream_body,
    SendInteractionRequestFn send_interaction_request,
    ExtractInteractionUsageFn extract_interaction_usage,
    ExtractInteractionTextFn extract_interaction_text,
    ExtractInteractionFinishReasonFn extract_interaction_finish_reason,
    RemoveCompletionMarkersFn remove_completion_markers,
    SessionReachedTargetLengthFn session_reached_target_length,
    CanCompleteOnNaturalStopFn can_complete_on_natural_stop,
    BuildContinuationPayloadFn build_continuation_payload)
    : build_interaction_upstream_body_(std::move(build_interaction_upstream_body)),
      send_interaction_request_(std::move(send_interaction_request)),
      extract_interaction_usage_(std::move(extract_interaction_usage)),
      extract_interaction_text_(std::move(extract_interaction_text)),
      extract_interaction_finish_reason_(
          std::move(extract_interaction_finish_reason)),
      remove_completion_markers_(std::move(remove_completion_markers)),
      session_reached_target_length_(std::move(session_reached_target_length)),
      can_complete_on_natural_stop_(std::move(can_complete_on_natural_stop)),
      build_continuation_payload_(std::move(build_continuation_payload)) {}

InteractionSessionResult InteractionSessionExecutor::Execute(
    const PlaneInteractionResolution& resolution,
    const InteractionRequestContext& request_context) const {
  const nlohmann::json& original_payload = request_context.payload;
  const ResolvedInteractionPolicy resolved_policy =
      ResolveInteractionCompletionPolicy(resolution.desired_state, original_payload);
  const InteractionCompletionPolicy& policy = resolved_policy.policy;
  const comet::runtime::ModelIdentity model_identity =
      BuildResolutionModelIdentity(resolution);
  InteractionSessionResult result;
  result.session_id = request_context.conversation_session_id.empty()
                          ? GenerateInteractionSessionId()
                          : request_context.conversation_session_id;
  const auto session_started_at = std::chrono::steady_clock::now();

  nlohmann::json current_payload = original_payload;
  for (int segment_index = 0;; ++segment_index) {
    const auto segment_started_at = std::chrono::steady_clock::now();
    InteractionUpstreamResponse upstream;
    std::string upstream_body;
    constexpr int kMaxAttempts = 3;
    for (int attempt = 0; attempt < kMaxAttempts; ++attempt) {
      upstream_body = build_interaction_upstream_body_(
          resolution,
          current_payload,
          false,
          resolved_policy,
          request_context.structured_output_json);
      upstream = send_interaction_request_(
          *resolution.target,
          request_context.request_id,
          upstream_body);
      if (upstream.status_code == 200 || upstream.status_code < 500 ||
          attempt + 1 == kMaxAttempts) {
        break;
      }
      std::this_thread::sleep_for(
          std::chrono::milliseconds(250 * (attempt + 1)));
    }
    if (upstream.status_code != 200) {
      const std::string upstream_detail =
          upstream.body.empty() ? std::string{} : (": " + upstream.body);
      const std::string request_excerpt =
          upstream_body.size() > 1024
              ? upstream_body.substr(0, 1024) + "...[truncated]"
              : upstream_body;
      throw std::runtime_error(
          "upstream interaction request failed with status " +
          std::to_string(upstream.status_code) + " target=" +
          resolution.target->raw + " request=" + request_excerpt +
          upstream_detail);
    }
    const nlohmann::json upstream_payload = upstream.body.empty()
                                                ? nlohmann::json::object()
                                                : nlohmann::json::parse(upstream.body);
    const auto segment_finished_at = std::chrono::steady_clock::now();
    const nlohmann::json usage = extract_interaction_usage_(upstream_payload);
    bool marker_seen_in_segment = false;
    const std::string clean_text = remove_completion_markers_(
        comet::runtime::ModelAdapter::SanitizeVisibleText(
            extract_interaction_text_(upstream_payload),
            model_identity),
        policy.completion_marker,
        &marker_seen_in_segment);
    InteractionSegmentSummary summary;
    summary.index = segment_index;
    summary.continuation_index = segment_index;
    summary.text = clean_text;
    summary.finish_reason = extract_interaction_finish_reason_(upstream_payload);
    summary.prompt_tokens = usage.value("prompt_tokens", 0);
    summary.completion_tokens = usage.value("completion_tokens", 0);
    summary.total_tokens = usage.value("total_tokens", 0);
    summary.latency_ms = static_cast<int>(
        std::chrono::duration_cast<std::chrono::milliseconds>(
            segment_finished_at - segment_started_at)
            .count());
    summary.marker_seen = marker_seen_in_segment;
    result.model = upstream_payload.value("model", result.model);
    result.content += clean_text;
    result.segments.push_back(summary);
    result.total_prompt_tokens += summary.prompt_tokens;
    result.total_completion_tokens += summary.completion_tokens;
    result.total_tokens += summary.total_tokens;
    result.total_latency_ms = static_cast<int>(
        std::chrono::duration_cast<std::chrono::milliseconds>(
            segment_finished_at - session_started_at)
            .count());
    result.final_finish_reason = summary.finish_reason;
    result.marker_seen = result.marker_seen || marker_seen_in_segment;

    if (result.marker_seen &&
        session_reached_target_length_(policy, result.total_completion_tokens)) {
      result.completion_status = "completed";
      result.stop_reason = "semantic_completion_marker";
      break;
    }
    if (can_complete_on_natural_stop_(policy, summary) &&
        session_reached_target_length_(policy, result.total_completion_tokens)) {
      result.completion_status = "completed";
      result.stop_reason = "natural_stop";
      break;
    }

    if (result.total_completion_tokens >= policy.max_total_completion_tokens) {
      result.completion_status = "incomplete_due_to_limits";
      result.stop_reason = "max_total_completion_tokens_reached";
      break;
    }
    if (result.total_latency_ms >= policy.max_elapsed_time_ms) {
      result.completion_status = "incomplete_due_to_limits";
      result.stop_reason = "max_elapsed_time_ms_reached";
      break;
    }
    if (segment_index >= policy.max_continuations) {
      result.completion_status = "incomplete_due_to_limits";
      result.stop_reason = "max_continuations_reached";
      break;
    }

    result.continuation_count = segment_index + 1;
    current_payload = build_continuation_payload_(
        original_payload,
        result.content,
        policy,
        summary.finish_reason != "length",
        result.total_completion_tokens);
  }

  if (result.completion_status == "in_progress") {
    result.completion_status = "failed";
    result.stop_reason = "session_state_unresolved";
  }
  return result;
}

InteractionProxyExecutor::InteractionProxyExecutor(
    BuildInteractionUpstreamBodyFn build_interaction_upstream_body,
    SendProxyRequestFn send_proxy_request,
    ResolveRequestSkillsFn resolve_request_skills)
    : build_interaction_upstream_body_(std::move(build_interaction_upstream_body)),
      send_proxy_request_(std::move(send_proxy_request)),
      resolve_request_skills_(std::move(resolve_request_skills)) {}

InteractionProxyResult InteractionProxyExecutor::Execute(
    const PlaneInteractionResolution& resolution,
    const std::string& request_id,
    const std::string& method,
    const std::string& path,
    const std::string& body) const {
  const InteractionContractResponder responder;
  const auto build_plane_error =
      [&](int status_code,
          const std::string& code,
          const std::string& message,
          bool retryable,
          const nlohmann::json& details = nlohmann::json::object()) {
        return InteractionProxyResult{
            false,
            InteractionJsonResponseSpec{
                status_code,
                responder.BuildPlaneErrorPayload(
                    resolution, request_id, code, message, retryable, details),
            },
            InteractionUpstreamResponse{},
        };
      };

  if (!resolution.status_payload.value("interaction_enabled", false)) {
    return build_plane_error(
        409,
        "interaction_disabled",
        "interaction is available only for plane_mode=llm",
        false);
  }
  if (!resolution.status_payload.value("ready", false) ||
      !resolution.target.has_value()) {
    return build_plane_error(
        409,
        "plane_not_ready",
        "plane interaction target is not ready",
        true);
  }

  try {
    std::string upstream_body = body;
    bool structured_output_json = false;
    if (method == "POST") {
      const InteractionRequestValidator validator;
      InteractionRequestContext request_context;
      request_context.request_id = request_id;
      if (const auto validation_error = validator.ValidateAndNormalizeRequest(
              resolution,
              validator.ParsePayload(body),
              &request_context)) {
        return build_plane_error(
            validation_error->code == "model_mismatch" ? 409 : 400,
            validation_error->code,
            validation_error->message,
            validation_error->retryable,
            validation_error->details);
      }
      if (const auto validation_error =
              resolve_request_skills_(resolution, &request_context)) {
        return build_plane_error(
            validation_error->code == "model_mismatch" ||
                    validation_error->code == "skills_disabled" ||
                    validation_error->code == "skills_not_ready"
                ? 409
                : 400,
            validation_error->code,
            validation_error->message,
            validation_error->retryable,
            validation_error->details);
      }
      structured_output_json = request_context.structured_output_json;
      const ResolvedInteractionPolicy resolved_policy =
          ResolveInteractionCompletionPolicy(
              resolution.desired_state,
              request_context.payload);
      upstream_body = build_interaction_upstream_body_(
          resolution,
          request_context.payload,
          false,
          resolved_policy,
          structured_output_json);
    }

    InteractionUpstreamResponse upstream;
    constexpr int kMaxAttempts = 3;
    for (int attempt = 0; attempt < kMaxAttempts; ++attempt) {
      try {
        upstream = send_proxy_request_(
            *resolution.target, method, path, upstream_body, request_id);
        if (upstream.status_code < 500 || attempt + 1 == kMaxAttempts) {
          break;
        }
      } catch (const std::exception&) {
        if (attempt + 1 == kMaxAttempts) {
          throw;
        }
      }
      std::this_thread::sleep_for(
          std::chrono::milliseconds(250 * (attempt + 1)));
    }

    if (upstream.status_code >= 400) {
      return build_plane_error(
          upstream.status_code >= 500 ? 503 : upstream.status_code,
          upstream.status_code >= 500 ? "upstream_unavailable"
                                      : "upstream_request_failed",
          upstream.body.empty()
              ? ("upstream interaction request failed with status " +
                 std::to_string(upstream.status_code))
              : upstream.body,
          upstream.status_code >= 500);
    }

    upstream.headers["x-comet-request-id"] = request_id;
    if (path == "/v1/models" && !upstream.body.empty()) {
      nlohmann::json payload = nlohmann::json::parse(upstream.body);
      payload["request_id"] = request_id;
      payload["comet"] = BuildInteractionContractMetadata(resolution, request_id);
      return InteractionProxyResult{
          false,
          InteractionJsonResponseSpec{200, std::move(payload)},
          InteractionUpstreamResponse{},
      };
    }

    return InteractionProxyResult{
        true,
        std::nullopt,
        std::move(upstream),
    };
  } catch (const std::exception& error) {
    const std::string lowered = LowercaseCopy(error.what());
    const bool timeout_like =
        lowered.find("timed out") != std::string::npos ||
        lowered.find("timeout") != std::string::npos;
    return build_plane_error(
        timeout_like ? 504 : 502,
        timeout_like ? "upstream_timeout" : "upstream_invalid_response",
        error.what(),
        true);
  }
}

InteractionStreamRequestResolver::InteractionStreamRequestResolver(
    ResolvePlaneInteractionFn resolve_plane_interaction,
    ResolveRequestSkillsFn resolve_request_skills)
    : resolve_plane_interaction_(std::move(resolve_plane_interaction)),
      resolve_request_skills_(std::move(resolve_request_skills)) {}

InteractionStreamResolutionResult InteractionStreamRequestResolver::Resolve(
    const std::string& db_path,
    const std::string& request_method,
    const std::string& request_path,
    const std::string& request_body,
    const std::string& request_id) const {
  const InteractionContractResponder responder;
  const auto build_standalone_error =
      [&](int status_code,
          const std::string& code,
          const std::string& message,
          bool retryable,
          const std::optional<std::string>& plane_name = std::nullopt,
          const std::optional<std::string>& reason = std::nullopt,
          const std::optional<std::string>& served_model_name = std::nullopt,
          const std::optional<std::string>& active_model_id = std::nullopt,
          const nlohmann::json& details = nlohmann::json::object()) {
        return InteractionStreamResolutionResult{
            InteractionJsonResponseSpec{
                status_code,
                responder.BuildStandaloneErrorPayload(
                    request_id,
                    code,
                    message,
                    retryable,
                    plane_name,
                    reason,
                    served_model_name,
                    active_model_id,
                    details),
            },
            std::nullopt,
        };
      };
  if (request_method != "POST") {
    return build_standalone_error(
        405,
        "method_not_allowed",
        "interaction stream endpoint accepts POST only",
        false);
  }
  const auto plane_name =
      ParseInteractionStreamPlaneName(request_method, request_path);
  if (!plane_name.has_value()) {
    return build_standalone_error(
        404,
        "plane_not_found",
        "plane not found for interaction stream path",
        false);
  }

  PlaneInteractionResolution resolution;
  InteractionRequestContext request_context;
  try {
    const InteractionRequestValidator validator;
    const auto build_plane_error =
        [&](int status_code,
            const std::string& code,
            const std::string& message,
            bool retryable,
            const nlohmann::json& details = nlohmann::json::object()) {
          return InteractionStreamResolutionResult{
              InteractionJsonResponseSpec{
                  status_code,
                  responder.BuildPlaneErrorPayload(
                      resolution, request_id, code, message, retryable, details),
              },
              std::nullopt,
          };
        };
    resolution = resolve_plane_interaction_(db_path, *plane_name);
    if (!resolution.status_payload.value("interaction_enabled", false)) {
      return build_plane_error(
          409,
          "interaction_disabled",
          "interaction is available only for plane_mode=llm",
          false);
    }
    if (!resolution.status_payload.value("ready", false) ||
        !resolution.target.has_value()) {
      return build_plane_error(
          409,
          "plane_not_ready",
          "plane interaction target is not ready",
          true);
    }
    request_context.request_id = request_id;
    if (const auto validation_error = validator.ValidateAndNormalizeRequest(
            resolution,
            validator.ParsePayload(request_body),
            &request_context)) {
      return build_plane_error(
          validation_error->code == "model_mismatch" ? 409 : 400,
          validation_error->code,
          validation_error->message,
          validation_error->retryable,
          validation_error->details);
    }
    if (const auto validation_error =
            resolve_request_skills_(resolution, &request_context)) {
      return build_plane_error(
          validation_error->code == "model_mismatch" ||
                  validation_error->code == "skills_disabled" ||
                  validation_error->code == "skills_not_ready"
              ? 409
              : 400,
          validation_error->code,
          validation_error->message,
          validation_error->retryable,
          validation_error->details);
    }
  } catch (const nlohmann::json::exception& error) {
    return build_standalone_error(
        400, "malformed_request", error.what(), false, plane_name);
  } catch (const std::exception& error) {
    return build_standalone_error(
        404, "plane_not_found", error.what(), false, plane_name);
  }

  return InteractionStreamResolutionResult{
      std::nullopt,
      InteractionStreamSetup{
          *plane_name,
          std::move(resolution),
          std::move(request_context),
          ResolveInteractionCompletionPolicy(
              resolution.desired_state,
              request_context.payload),
      },
  };
}

InteractionStreamSessionExecutor::InteractionStreamSessionExecutor(
    SessionReachedTargetLengthFn session_reached_target_length,
    CanCompleteOnNaturalStopFn can_complete_on_natural_stop,
    BuildContinuationPayloadFn build_continuation_payload)
    : session_reached_target_length_(std::move(session_reached_target_length)),
      can_complete_on_natural_stop_(std::move(can_complete_on_natural_stop)),
      build_continuation_payload_(std::move(build_continuation_payload)) {}

InteractionSessionResult InteractionStreamSessionExecutor::Execute(
    const std::string& request_id,
    const std::string& session_id,
    const std::string& plane_name,
    const PlaneInteractionResolution& resolution,
    const InteractionRequestContext& request_context,
    const ResolvedInteractionPolicy& resolved_policy,
    ExecuteStreamedSegmentFn execute_streamed_segment,
    SendEventFn send_event,
    SendDoneFn send_done) const {
  const InteractionCompletionPolicy& policy = resolved_policy.policy;
  const nlohmann::json& original_payload = request_context.payload;
  InteractionSessionResult session;
  session.session_id = session_id;
  const auto session_started_at = std::chrono::steady_clock::now();
  const InteractionSessionPresenter session_presenter;
  const InteractionStreamPresenter stream_presenter;

  try {
    if (!send_event(
            "session_started",
            stream_presenter.BuildSessionStartedEvent(
                request_id,
                session.session_id,
                plane_name,
                resolution,
                request_context))) {
      throw std::runtime_error("failed to write session_started");
    }

    nlohmann::json current_payload = original_payload;
    for (int segment_index = 0;; ++segment_index) {
      if (!send_event(
              "segment_started",
              stream_presenter.BuildSegmentStartedEvent(
                  request_id, session.session_id, segment_index))) {
        throw std::runtime_error("failed to write segment_started");
      }

      const StreamedInteractionSegmentResult segment =
          execute_streamed_segment(current_payload, segment_index);
      if (session.model.empty() && !segment.model.empty()) {
        session.model = segment.model;
      }
      session.content += segment.cleaned_text;
      session.segments.push_back(segment.summary);
      session.total_prompt_tokens += segment.summary.prompt_tokens;
      session.total_completion_tokens += segment.summary.completion_tokens;
      session.total_tokens += segment.summary.total_tokens;
      session.total_latency_ms = static_cast<int>(
          std::chrono::duration_cast<std::chrono::milliseconds>(
              std::chrono::steady_clock::now() - session_started_at)
              .count());
      session.final_finish_reason = segment.summary.finish_reason;
      session.marker_seen = session.marker_seen || segment.summary.marker_seen;

      if (!send_event(
              "segment_complete",
              stream_presenter.BuildSegmentCompleteEvent(
                  request_id, session.session_id, segment_index, segment.summary))) {
        throw std::runtime_error("failed to write segment_complete");
      }

      if (session.marker_seen &&
          session_reached_target_length_(policy, session.total_completion_tokens)) {
        session.completion_status = "completed";
        session.stop_reason = "semantic_completion_marker";
        break;
      }
      if (can_complete_on_natural_stop_(policy, segment.summary) &&
          session_reached_target_length_(policy, session.total_completion_tokens)) {
        session.completion_status = "completed";
        session.stop_reason = "natural_stop";
        break;
      }
      if (session.total_completion_tokens >= policy.max_total_completion_tokens) {
        session.completion_status = "incomplete_due_to_limits";
        session.stop_reason = "max_total_completion_tokens_reached";
        break;
      }
      if (session.total_latency_ms >= policy.max_elapsed_time_ms) {
        session.completion_status = "incomplete_due_to_limits";
        session.stop_reason = "max_elapsed_time_ms_reached";
        break;
      }
      if (segment_index >= policy.max_continuations) {
        session.completion_status = "incomplete_due_to_limits";
        session.stop_reason = "max_continuations_reached";
        break;
      }

      session.continuation_count = segment_index + 1;
      if (!send_event(
              "continuation_started",
              stream_presenter.BuildContinuationStartedEvent(
                  request_id,
                  session.session_id,
                  session.continuation_count,
                  segment.summary))) {
        throw std::runtime_error("failed to write continuation_started");
      }

      current_payload = build_continuation_payload_(
          original_payload,
          session.content,
          policy,
          segment.summary.finish_reason != "length",
          session.total_completion_tokens);
    }

    if (session.completion_status == "in_progress") {
      session.completion_status = "failed";
      session.stop_reason = "session_state_unresolved";
    }

    nlohmann::json session_payload = session_presenter.BuildSessionPayload(session);
    session_payload["applied_skills"] = RequestAppliedSkills(request_context);
    session_payload["auto_applied_skills"] = RequestAutoAppliedSkills(request_context);
    session_payload["skill_resolution_mode"] =
        RequestSkillResolutionMode(request_context);
    session_payload["browsing"] = RequestBrowsingSummary(request_context);
    session_payload["skills_session_id"] =
        RequestSkillsSessionId(request_context).has_value()
            ? nlohmann::json(*RequestSkillsSessionId(request_context))
            : nlohmann::json(nullptr);
    std::optional<nlohmann::json> parsed_structured_output = std::nullopt;
    if (request_context.structured_output_json) {
      if (session.completion_status != "completed") {
        send_event(
            "session_failed",
            stream_presenter.BuildSessionFailedEvent(
                request_id,
                session.session_id,
                "structured_output_truncated",
                "structured output session ended before a valid JSON object was completed",
                true,
                session,
                std::optional<nlohmann::json>(session_payload)));
        send_event(
            "error",
            stream_presenter.BuildErrorEvent(
                request_id,
                "structured_output_truncated",
                "structured output session ended before a valid JSON object was completed",
                true,
                plane_name));
        send_done();
        return session;
      }
      parsed_structured_output =
          session_presenter.ParseStructuredOutputObject(session.content);
      if (!parsed_structured_output.has_value()) {
        send_event(
            "session_failed",
            stream_presenter.BuildSessionFailedEvent(
                request_id,
                session.session_id,
                "structured_output_malformed",
                "structured output session did not produce a valid JSON object",
                true,
                session,
                std::optional<nlohmann::json>(session_payload)));
        send_event(
            "error",
            stream_presenter.BuildErrorEvent(
                request_id,
                "structured_output_malformed",
                "structured output session did not produce a valid JSON object",
                true,
                plane_name));
        send_done();
        return session;
      }
    }

    send_event(
        "session_complete",
        stream_presenter.BuildSessionCompleteEvent(
            resolution, request_id, session, session_payload, request_context));
    send_event(
        "complete",
        stream_presenter.BuildCompleteEvent(
            resolution,
            request_id,
            request_context,
            session,
            session_payload,
            parsed_structured_output));
    send_done();
    return session;
  } catch (const std::exception& error) {
    const std::string lowered = LowercaseCopy(error.what());
    const bool timeout_like =
        lowered.find("timed out") != std::string::npos ||
        lowered.find("timeout") != std::string::npos;
    send_event(
        "session_failed",
        stream_presenter.BuildSessionFailedEvent(
            request_id,
            session.session_id,
            timeout_like ? "upstream_timeout" : "stream_session_failed",
            error.what(),
            true,
            session));
    send_event(
        "error",
        stream_presenter.BuildErrorEvent(
            request_id,
            timeout_like ? "upstream_timeout" : "stream_session_failed",
            error.what(),
            true,
            plane_name));
    send_done();
    return session;
  }
}

InteractionStreamSegmentExecutor::InteractionStreamSegmentExecutor(
    BuildInteractionUpstreamBodyFn build_interaction_upstream_body,
    OpenStreamingRequestFn open_streaming_request,
    SendFallbackRequestFn send_fallback_request,
    StartsWithReasoningPreambleFn starts_with_reasoning_preamble,
    SanitizeInteractionTextFn sanitize_interaction_text,
    ExtractInteractionUsageFn extract_interaction_usage,
    ExtractInteractionTextFn extract_interaction_text,
    ExtractInteractionFinishReasonFn extract_interaction_finish_reason)
    : build_interaction_upstream_body_(
          std::move(build_interaction_upstream_body)),
      open_streaming_request_(std::move(open_streaming_request)),
      send_fallback_request_(std::move(send_fallback_request)),
      starts_with_reasoning_preamble_(
          std::move(starts_with_reasoning_preamble)),
      sanitize_interaction_text_(std::move(sanitize_interaction_text)),
      extract_interaction_usage_(std::move(extract_interaction_usage)),
      extract_interaction_text_(std::move(extract_interaction_text)),
      extract_interaction_finish_reason_(
          std::move(extract_interaction_finish_reason)) {}

StreamedInteractionSegmentResult InteractionStreamSegmentExecutor::Execute(
    const PlaneInteractionResolution& resolution,
    const InteractionRequestContext& request_context,
    const ResolvedInteractionPolicy& resolved_policy,
    const std::string& request_id,
    const nlohmann::json& payload,
    int segment_index,
    SendDeltaFn send_delta) const {
  const InteractionCompletionPolicy& policy = resolved_policy.policy;
  const comet::runtime::ModelIdentity model_identity =
      BuildResolutionModelIdentity(resolution);
  StreamedInteractionSegmentResult result;
  result.summary.index = segment_index;
  result.summary.continuation_index = segment_index;
  const auto segment_started_at = std::chrono::steady_clock::now();

  const auto flush_fallback_response =
      [&](const nlohmann::json& fallback_payload,
          bool assign_only = false) -> std::optional<StreamedInteractionSegmentResult> {
        bool marker_seen_in_fallback = false;
        const std::string fallback_text = RemoveCompletionMarkers(
            comet::runtime::ModelAdapter::SanitizeVisibleText(
                extract_interaction_text_(fallback_payload),
                model_identity),
            policy.completion_marker,
            &marker_seen_in_fallback);
        if (!fallback_text.empty()) {
          if (!assign_only) {
            if (!send_delta(
                    fallback_payload.value("model", std::string{}),
                    fallback_text)) {
              throw std::runtime_error(
                  "failed to write downstream fallback delta");
            }
          }
          result.cleaned_text = fallback_text;
        }
        const nlohmann::json usage =
            extract_interaction_usage_(fallback_payload);
        const auto segment_finished_at = std::chrono::steady_clock::now();
        result.summary.text = result.cleaned_text;
        result.summary.finish_reason =
            extract_interaction_finish_reason_(fallback_payload);
        result.summary.prompt_tokens = usage.value("prompt_tokens", 0);
        result.summary.completion_tokens = usage.value("completion_tokens", 0);
        result.summary.total_tokens = usage.value("total_tokens", 0);
        result.summary.latency_ms = static_cast<int>(
            std::chrono::duration_cast<std::chrono::milliseconds>(
                segment_finished_at - segment_started_at)
                .count());
        result.summary.marker_seen = marker_seen_in_fallback;
        if (result.model.empty()) {
          result.model = fallback_payload.value("model", std::string{});
        }
        return result;
      };

  const auto run_non_stream_fallback = [&](bool assign_only = false)
      -> std::optional<StreamedInteractionSegmentResult> {
    const InteractionUpstreamResponse fallback = send_fallback_request_(
        *resolution.target,
        request_id,
        build_interaction_upstream_body_(
            resolution,
            payload,
            false,
            resolved_policy,
            request_context.structured_output_json));
    if (fallback.status_code != 200 || fallback.body.empty()) {
      return std::nullopt;
    }
    return flush_fallback_response(
        nlohmann::json::parse(fallback.body), assign_only);
  };

  try {
    InteractionStreamingUpstreamConnection stream =
        open_streaming_request_(
            *resolution.target,
            request_id,
            build_interaction_upstream_body_(
                resolution,
                payload,
                true,
                resolved_policy,
                request_context.structured_output_json));
    const auto close_stream = [&]() {
      if (stream.close) {
        stream.close();
        stream.close = nullptr;
      }
    };

    try {
      CompletionMarkerFilterState filter_state;
      nlohmann::json complete_payload = nlohmann::json::object();
      bool saw_complete = false;
      bool upstream_stream_finished = false;
      std::string openai_stream_raw_text;
      std::string openai_stream_emitted_text;
      std::string transport_buffer = std::move(stream.initial_body);
      std::string sse_buffer;
      if (stream.chunked_transfer) {
        DecodeAvailableChunkedHttpBody(
            transport_buffer, &sse_buffer, &upstream_stream_finished);
      } else {
        sse_buffer = std::move(transport_buffer);
      }

      const auto emit_openai_stream_progress =
          [&](const std::string& model_name, bool final_flush) {
            std::string visible_text;
            const std::size_t think_close =
                openai_stream_raw_text.rfind("</think>");
            if (think_close != std::string::npos) {
              visible_text = openai_stream_raw_text.substr(
                  think_close + std::string("</think>").size());
            } else {
              const std::string trimmed = TrimCopy(openai_stream_raw_text);
              if (!final_flush &&
                  (openai_stream_raw_text.find("<think>") !=
                       std::string::npos ||
                   starts_with_reasoning_preamble_(trimmed))) {
                return;
              }
              visible_text = openai_stream_raw_text;
            }
            bool marker_seen = false;
            visible_text = RemoveCompletionMarkers(
                visible_text,
                policy.completion_marker,
                &marker_seen);
            filter_state.marker_seen = filter_state.marker_seen || marker_seen;
            visible_text = sanitize_interaction_text_(std::move(visible_text));
            visible_text = comet::runtime::ModelAdapter::SanitizeVisibleText(
                std::move(visible_text), model_identity);
            if (visible_text.empty()) {
              return;
            }
            if (visible_text.size() < openai_stream_emitted_text.size() ||
                visible_text.compare(
                    0,
                    openai_stream_emitted_text.size(),
                    openai_stream_emitted_text) != 0) {
              if (!final_flush || !openai_stream_emitted_text.empty()) {
                return;
              }
            }
            const std::string delta =
                visible_text.substr(openai_stream_emitted_text.size());
            if (delta.empty()) {
              return;
            }
            openai_stream_emitted_text = visible_text;
            result.cleaned_text += delta;
            if (!send_delta(model_name, delta)) {
              throw std::runtime_error("failed to write downstream delta");
            }
          };

      const auto handle_frame = [&](const InteractionSseFrame& frame) {
        if (frame.data == "[DONE]") {
          emit_openai_stream_progress(result.model, true);
          if (!saw_complete) {
            complete_payload = nlohmann::json{
                {"model", result.model},
                {"finish_reason", "stop"},
                {"usage",
                 nlohmann::json{
                     {"prompt_tokens", 0},
                     {"completion_tokens", 0},
                     {"total_tokens", 0},
                 }},
            };
            saw_complete = true;
          }
          return false;
        }
        if (frame.event_name == "message") {
          if (frame.data.empty()) {
            return true;
          }
          const nlohmann::json chunk_payload =
              nlohmann::json::parse(frame.data);
          if (result.model.empty()) {
            result.model = chunk_payload.value("model", std::string{});
          }
          if (chunk_payload.contains("choices") &&
              chunk_payload.at("choices").is_array() &&
              !chunk_payload.at("choices").empty()) {
            const nlohmann::json& choice = chunk_payload.at("choices").at(0);
            std::string delta_text;
            if (choice.contains("delta") && choice.at("delta").is_object()) {
              const nlohmann::json& delta = choice.at("delta");
              if (delta.contains("content")) {
                const nlohmann::json& content = delta.at("content");
                if (content.is_string()) {
                  delta_text = content.get<std::string>();
                } else if (content.is_array()) {
                  for (const auto& part : content) {
                    if (part.is_object() && part.contains("text") &&
                        part.at("text").is_string()) {
                      delta_text += part.at("text").get<std::string>();
                    }
                  }
                }
              }
            }
            openai_stream_raw_text += delta_text;
            emit_openai_stream_progress(
                chunk_payload.value("model", std::string{}), false);
            if (choice.contains("finish_reason") &&
                !choice.at("finish_reason").is_null()) {
              emit_openai_stream_progress(
                  chunk_payload.value("model", std::string{}), true);
              complete_payload = nlohmann::json{
                  {"model", chunk_payload.value("model", std::string{})},
                  {"finish_reason",
                   choice.value("finish_reason", std::string{"stop"})},
                  {"usage", extract_interaction_usage_(chunk_payload)},
              };
              saw_complete = true;
            }
          }
          return true;
        }
        if (frame.event_name == "delta") {
          if (frame.data.empty()) {
            return true;
          }
          const nlohmann::json delta_payload =
              nlohmann::json::parse(frame.data);
          const std::string filtered = ConsumeCompletionMarkerFilteredChunk(
              filter_state,
              delta_payload.value("delta", std::string{}),
              policy.completion_marker,
              false);
          if (!filtered.empty()) {
            result.cleaned_text += filtered;
            if (!send_delta(
                    delta_payload.value("model", std::string{}), filtered)) {
              throw std::runtime_error("failed to write downstream delta");
            }
          }
          return true;
        }
        if (frame.event_name == "complete") {
          complete_payload = nlohmann::json::parse(frame.data);
          saw_complete = true;
          return true;
        }
        if (frame.event_name == "error") {
          const nlohmann::json error_payload =
              nlohmann::json::parse(frame.data);
          throw std::runtime_error(error_payload.value(
              "message", std::string{"upstream stream error"}));
        }
        return true;
      };

      const auto drain_frames = [&](bool final_chunk) {
        if (stream.chunked_transfer) {
          DecodeAvailableChunkedHttpBody(
              transport_buffer, &sse_buffer, &upstream_stream_finished);
        }
        if (final_chunk && !sse_buffer.empty() &&
            sse_buffer.find("\n\n") == std::string::npos) {
          sse_buffer.append("\n\n");
        }
        InteractionSseFrame frame;
        while (TryConsumeSseFrame(sse_buffer, &frame)) {
          if (!handle_frame(frame)) {
            break;
          }
        }
      };

      while (true) {
        drain_frames(false);
        if (saw_complete && sse_buffer.find("[DONE]") != std::string::npos) {
          break;
        }
        if (stream.chunked_transfer && upstream_stream_finished) {
          drain_frames(true);
          break;
        }
        const std::string chunk =
            stream.read_next_chunk ? stream.read_next_chunk() : std::string{};
        if (chunk.empty()) {
          drain_frames(true);
          break;
        }
        if (stream.chunked_transfer) {
          transport_buffer += chunk;
        } else {
          sse_buffer += chunk;
        }
      }

      const std::string final_filtered = ConsumeCompletionMarkerFilteredChunk(
          filter_state,
          std::string{},
          policy.completion_marker,
          true);
      if (!final_filtered.empty()) {
        result.cleaned_text += final_filtered;
        if (!send_delta(std::string{}, final_filtered)) {
          throw std::runtime_error("failed to flush downstream delta");
        }
      }

      if (!saw_complete) {
        if (TrimCopy(result.cleaned_text).empty()) {
          if (const auto fallback = run_non_stream_fallback();
              fallback.has_value()) {
            close_stream();
            return *fallback;
          }
        }
        if (!saw_complete && !TrimCopy(result.cleaned_text).empty()) {
          complete_payload = nlohmann::json{
              {"model", result.model},
              {"finish_reason", "length"},
              {"usage",
               nlohmann::json{
                   {"prompt_tokens", 0},
                   {"completion_tokens", 0},
                   {"total_tokens", 0},
               }},
          };
          saw_complete = true;
        }
        if (!saw_complete) {
          throw std::runtime_error(
              "upstream interaction stream ended without completion event");
        }
      }

      const auto segment_finished_at = std::chrono::steady_clock::now();
      const nlohmann::json usage =
          complete_payload.value("usage", nlohmann::json::object());
      result.summary.text = result.cleaned_text;
      result.summary.finish_reason =
          complete_payload.value("finish_reason", std::string{"stop"});
      result.summary.prompt_tokens = usage.value("prompt_tokens", 0);
      result.summary.completion_tokens = usage.value("completion_tokens", 0);
      result.summary.total_tokens = usage.value("total_tokens", 0);
      result.summary.latency_ms = complete_payload.value(
          "latency_ms",
          static_cast<int>(
              std::chrono::duration_cast<std::chrono::milliseconds>(
                  segment_finished_at - segment_started_at)
                  .count()));
      result.summary.marker_seen = filter_state.marker_seen;
      if (result.model.empty()) {
        result.model = complete_payload.value("model", std::string{});
      }
      close_stream();
      return result;
    } catch (...) {
      close_stream();
      throw;
    }
  } catch (...) {
    if (TrimCopy(result.cleaned_text).empty()) {
      try {
        if (const auto fallback = run_non_stream_fallback();
            fallback.has_value()) {
          return *fallback;
        }
      } catch (...) {
      }
    }
    throw;
  }
}

InteractionPlaneResolver::InteractionPlaneResolver(
    FindInferInstanceNameFn find_infer_instance_name,
    ParseInstanceRuntimeStatusesFn parse_instance_runtime_statuses,
    ObservationMatchesPlaneFn observation_matches_plane,
    BuildPlaneScopedRuntimeStatusFn build_plane_scoped_runtime_status,
    ParseInteractionTargetFn parse_interaction_target,
    CountReadyWorkerMembersFn count_ready_worker_members,
    ProbeControllerTargetOkFn probe_controller_target_ok,
    DescribeUnsupportedControllerLocalRuntimeFn
        describe_unsupported_controller_local_runtime)
    : find_infer_instance_name_(std::move(find_infer_instance_name)),
      parse_instance_runtime_statuses_(std::move(parse_instance_runtime_statuses)),
      observation_matches_plane_(std::move(observation_matches_plane)),
      build_plane_scoped_runtime_status_(std::move(build_plane_scoped_runtime_status)),
      parse_interaction_target_(std::move(parse_interaction_target)),
      count_ready_worker_members_(std::move(count_ready_worker_members)),
      probe_controller_target_ok_(std::move(probe_controller_target_ok)),
      describe_unsupported_controller_local_runtime_(
          std::move(describe_unsupported_controller_local_runtime)) {}

PlaneInteractionResolution InteractionPlaneResolver::Resolve(
    const std::string& db_path,
    const std::string& plane_name) const {
  comet::ControllerStore store(db_path);
  store.Initialize();

  const auto desired_state = store.LoadDesiredState(plane_name);
  if (!desired_state.has_value()) {
    throw std::runtime_error("plane '" + plane_name + "' not found");
  }
  PlaneInteractionResolution resolution;
  resolution.db_path = db_path;
  resolution.desired_state = *desired_state;
  resolution.plane_record = store.LoadPlane(plane_name);
  const std::string primary_node = desired_state->inference.primary_infer_node;
  bool observation_matches_plane = false;
  if (!primary_node.empty()) {
    resolution.observation = store.LoadHostObservation(primary_node);
    bool infer_runtime_present = false;
    if (resolution.observation.has_value()) {
      if (const auto infer_instance_name = find_infer_instance_name_(*desired_state);
          infer_instance_name.has_value()) {
        const auto instance_statuses =
            parse_instance_runtime_statuses_(*resolution.observation);
        infer_runtime_present = std::any_of(
            instance_statuses.begin(),
            instance_statuses.end(),
            [&](const comet::RuntimeProcessStatus& status) {
              return status.instance_name == *infer_instance_name;
            });
      }
    }
    if (resolution.observation.has_value()) {
      try {
        observation_matches_plane =
            observation_matches_plane_(*resolution.observation, plane_name) ||
            infer_runtime_present;
      } catch (const std::exception&) {
        observation_matches_plane = infer_runtime_present;
      }
    }
    if (observation_matches_plane) {
      resolution.runtime_status =
          build_plane_scoped_runtime_status_(*desired_state, *resolution.observation);
    }
    if (!resolution.runtime_status.has_value() && observation_matches_plane &&
        !resolution.observation->runtime_status_json.empty()) {
      resolution.runtime_status = comet::DeserializeRuntimeStatusJson(
          resolution.observation->runtime_status_json);
    }
    if (resolution.runtime_status.has_value()) {
      resolution.target = parse_interaction_target_(
          resolution.runtime_status->gateway_listen,
          desired_state->gateway.listen_port);
    }
  }

  const bool llm_plane = desired_state->plane_mode == comet::PlaneMode::Llm;
  const bool running_plane =
      resolution.plane_record.has_value() && resolution.plane_record->state == "running";
  const PlaneSkillsService skills_service;
  const bool skills_enabled = skills_service.IsEnabled(*desired_state);
  const auto skills_target = skills_service.ResolveTarget(*desired_state);
  const bool skills_ready =
      skills_enabled && running_plane && skills_target.has_value() &&
      probe_controller_target_ok_(skills_target, "/health");
  const auto skills_instance = std::find_if(
      desired_state->instances.begin(),
      desired_state->instances.end(),
      [](const comet::InstanceSpec& instance) {
        return instance.role == comet::InstanceRole::Skills;
      });
  const PlaneBrowsingService browsing_service;
  const bool browsing_enabled = browsing_service.IsEnabled(*desired_state);
  const auto browsing_target = browsing_service.ResolveTarget(*desired_state);
  const bool browsing_ready =
      browsing_enabled && running_plane && browsing_target.has_value() &&
      probe_controller_target_ok_(browsing_target, "/health");
  const auto browsing_instance = std::find_if(
      desired_state->instances.begin(),
      desired_state->instances.end(),
      [](const comet::InstanceSpec& instance) {
        return instance.role == comet::InstanceRole::Browsing;
      });
  const bool data_parallel =
      comet::DataParallelEnabled(desired_state->inference);
  const bool hybrid_data_parallel =
      data_parallel &&
      desired_state->inference.data_parallel_lb_mode == comet::kDataParallelLbModeHybrid;
  int expected_worker_members =
      std::max(0, desired_state->worker_group.expected_workers);
  int ready_worker_members = count_ready_worker_members_(store, *desired_state);
  int expected_replica_groups =
      resolution.runtime_status.has_value() && resolution.runtime_status->replica_groups_expected > 0
          ? resolution.runtime_status->replica_groups_expected
          : comet::ExpectedReplicaGroupCount(
                desired_state->inference,
                desired_state->worker_group);
  if (ready_worker_members == 0 && resolution.runtime_status.has_value() &&
      resolution.runtime_status->inference_ready) {
    ready_worker_members = expected_worker_members;
  }
  if (hybrid_data_parallel) {
    expected_replica_groups =
        resolution.runtime_status.has_value() && resolution.runtime_status->api_endpoints_expected > 0
            ? resolution.runtime_status->api_endpoints_expected
            : ExpectedHybridApiEndpoints(*desired_state);
  }
  int ready_replica_groups =
      resolution.runtime_status.has_value() ? resolution.runtime_status->replica_groups_ready : 0;
  const bool need_fallback_replica_counts =
      !resolution.runtime_status.has_value() ||
      (expected_replica_groups > 0 && ready_replica_groups == 0);
  if (need_fallback_replica_counts) {
    if (hybrid_data_parallel) {
      ready_replica_groups = std::min(expected_replica_groups, ready_worker_members);
    } else if (data_parallel) {
      ready_replica_groups =
          expected_worker_members > 0 ? ready_worker_members / expected_worker_members : 0;
      ready_replica_groups = std::min(expected_replica_groups, ready_replica_groups);
    } else if (expected_worker_members <= 0 ||
               ready_worker_members >= expected_worker_members) {
      ready_replica_groups = expected_replica_groups > 0 ? 1 : 0;
    }
  }
  int degraded_replica_groups =
      resolution.runtime_status.has_value() ? resolution.runtime_status->replica_groups_degraded
                                            : std::max(0, expected_replica_groups - ready_replica_groups);
  if (hybrid_data_parallel && resolution.runtime_status.has_value()) {
    if (resolution.runtime_status->api_endpoints_expected > 0) {
      expected_worker_members = resolution.runtime_status->api_endpoints_expected;
    }
    ready_worker_members =
        std::max(ready_worker_members, resolution.runtime_status->api_endpoints_ready);
  }

  if (!resolution.target.has_value()) {
    resolution.target = parse_interaction_target_(
        desired_state->gateway.listen_host + ":" +
            std::to_string(desired_state->gateway.listen_port),
        desired_state->gateway.listen_port);
  }

  if (!resolution.runtime_status.has_value() && resolution.target.has_value()) {
    comet::RuntimeStatus runtime;
    runtime.plane_name = desired_state->plane_name;
    runtime.control_root = desired_state->control_root;
    runtime.primary_infer_node = desired_state->inference.primary_infer_node;
    runtime.runtime_backend = desired_state->inference.runtime_engine;
    if (const auto infer_instance_name = find_infer_instance_name_(*desired_state);
        infer_instance_name.has_value()) {
      runtime.instance_name = *infer_instance_name;
      runtime.instance_role = "infer";
      runtime.node_name = desired_state->inference.primary_infer_node;
    }
    if (desired_state->bootstrap_model.has_value()) {
      runtime.active_model_id = desired_state->bootstrap_model->model_id;
      runtime.active_served_model_name =
          desired_state->bootstrap_model->served_model_name.value_or(std::string{});
      runtime.cached_local_model_path =
          desired_state->bootstrap_model->local_path.value_or(std::string{});
      runtime.model_path = runtime.cached_local_model_path;
      runtime.active_model_ready = !runtime.active_model_id.empty();
    }
    runtime.gateway_listen =
        desired_state->gateway.listen_host + ":" +
        std::to_string(desired_state->gateway.listen_port);
    runtime.gateway_health_url =
        "http://127.0.0.1:" + std::to_string(desired_state->gateway.listen_port) +
        "/health";
    runtime.upstream_models_url =
        "http://127.0.0.1:" + std::to_string(desired_state->gateway.listen_port) +
        "/v1/models";
    runtime.inference_health_url = runtime.upstream_models_url;
    runtime.gateway_plan_ready = true;
    runtime.gateway_ready = probe_controller_target_ok_(resolution.target, "/health");
    runtime.inference_ready =
        probe_controller_target_ok_(resolution.target, "/v1/models");
    if (hybrid_data_parallel) {
      const int expected_api_endpoints = ExpectedHybridApiEndpoints(*desired_state);
      runtime.api_endpoints_expected = expected_api_endpoints;
      runtime.api_endpoints_ready =
          runtime.inference_ready ? expected_api_endpoints : 0;
      runtime.replica_groups_expected = expected_api_endpoints;
      runtime.replica_groups_ready =
          runtime.inference_ready ? expected_api_endpoints : 0;
      runtime.replica_groups_degraded =
          std::max(0, expected_api_endpoints - runtime.replica_groups_ready);
    }
    resolution.runtime_status = std::move(runtime);
  }

  if (resolution.runtime_status.has_value()) {
    if (ready_worker_members == 0 && resolution.runtime_status->inference_ready) {
      ready_worker_members = std::max(expected_worker_members, 1);
    }
    if (hybrid_data_parallel) {
      if (resolution.runtime_status->api_endpoints_expected > 0) {
        expected_worker_members = resolution.runtime_status->api_endpoints_expected;
        expected_replica_groups = resolution.runtime_status->api_endpoints_expected;
      } else {
        expected_worker_members = ExpectedHybridApiEndpoints(*desired_state);
        expected_replica_groups = expected_worker_members;
      }
      if (resolution.runtime_status->api_endpoints_ready > 0) {
        ready_worker_members =
            std::max(ready_worker_members, resolution.runtime_status->api_endpoints_ready);
        ready_replica_groups =
            std::max(ready_replica_groups, resolution.runtime_status->api_endpoints_ready);
      } else if (resolution.runtime_status->inference_ready) {
        ready_worker_members = std::max(ready_worker_members, expected_worker_members);
        ready_replica_groups = std::max(ready_replica_groups, expected_replica_groups);
      }
      degraded_replica_groups =
          std::max(0, expected_replica_groups - ready_replica_groups);
      resolution.runtime_status->api_endpoints_expected = expected_replica_groups;
      resolution.runtime_status->api_endpoints_ready = ready_replica_groups;
    }
    resolution.runtime_status->registry_entries =
        std::max(resolution.runtime_status->registry_entries, ready_worker_members);
    resolution.runtime_status->data_parallel_mode = desired_state->inference.data_parallel_mode;
    resolution.runtime_status->data_parallel_lb_mode =
        desired_state->inference.data_parallel_lb_mode;
    resolution.runtime_status->replica_groups_expected = expected_replica_groups;
    resolution.runtime_status->replica_groups_ready = ready_replica_groups;
    resolution.runtime_status->replica_groups_degraded = degraded_replica_groups;
    const bool replica_topology_ready =
        expected_replica_groups == 0 ||
        ready_replica_groups >= expected_replica_groups;
    resolution.runtime_status->launch_ready =
        resolution.runtime_status->active_model_ready &&
        resolution.runtime_status->inference_ready &&
        resolution.runtime_status->gateway_ready &&
        replica_topology_ready;
    resolution.runtime_status->ready = resolution.runtime_status->launch_ready;
  }
  const bool observation_ready =
      observation_matches_plane ||
      (resolution.runtime_status.has_value() &&
       (resolution.runtime_status->gateway_ready ||
        resolution.runtime_status->inference_ready));
  const bool worker_group_degraded =
      expected_worker_members > 0 && ready_worker_members > 0 &&
      ready_worker_members < expected_worker_members;
  const bool replica_group_degraded =
      expected_replica_groups > 0 && ready_replica_groups < expected_replica_groups;
  const auto local_runtime_blocker =
      describe_unsupported_controller_local_runtime_(*desired_state, primary_node);
  const bool runtime_ready =
      resolution.runtime_status.has_value() &&
      resolution.runtime_status->active_model_ready &&
      resolution.runtime_status->inference_ready &&
      resolution.runtime_status->gateway_ready &&
      resolution.runtime_status->launch_ready;
  std::string reason = "ready";
  if (!llm_plane) {
    reason = "plane_mode_compute";
  } else if (!running_plane) {
    reason = "plane_not_running";
  } else if (local_runtime_blocker.has_value()) {
    reason = "unsupported_local_runtime";
  } else if (!observation_ready) {
    reason = "no_observation";
  } else if (resolution.observation->status == comet::HostObservationStatus::Failed) {
    reason = "runtime_start_failed";
  } else if (!resolution.runtime_status.has_value()) {
    reason = "runtime_status_missing";
  } else if (!resolution.runtime_status->active_model_ready) {
    reason = "active_model_missing";
  } else if (data_parallel && expected_replica_groups > 0 && ready_replica_groups == 0) {
    reason = "no_ready_replicas";
  } else if (expected_replica_groups > 0 && ready_replica_groups < expected_replica_groups) {
    reason = data_parallel
                 ? (ready_worker_members > 0 ? "replica_group_partial"
                                             : "replica_group_missing")
                 : "worker_group_partial";
  } else if (!resolution.runtime_status->gateway_ready) {
    reason = "gateway_not_ready";
  } else if (!resolution.runtime_status->inference_ready) {
    reason = "inference_not_ready";
  } else if (!resolution.target.has_value()) {
    reason = "gateway_target_missing";
  }

  nlohmann::json degraded_reasons = nlohmann::json::array();
  if (data_parallel && expected_replica_groups > 0 && ready_replica_groups == 0) {
    degraded_reasons.push_back("no_ready_replicas");
  } else if (replica_group_degraded) {
    degraded_reasons.push_back(
        data_parallel
            ? (ready_worker_members > 0 ? "replica_group_partial"
                                        : "replica_group_missing")
            : "worker_group_partial");
  }

  resolution.status_payload = nlohmann::json{
      {"plane_name", plane_name},
      {"plane_mode", comet::ToString(desired_state->plane_mode)},
      {"interaction_enabled", llm_plane},
      {"skills_enabled", skills_enabled},
      {"skills_ready", skills_ready},
      {"skills_container_name",
       skills_instance != desired_state->instances.end()
           ? nlohmann::json(skills_instance->name)
           : nlohmann::json(nullptr)},
      {"browsing_enabled", browsing_enabled},
      {"browsing_ready", browsing_ready},
      {"browsing_container_name",
       browsing_instance != desired_state->instances.end()
           ? nlohmann::json(browsing_instance->name)
           : nlohmann::json(nullptr)},
      {"browser_session_enabled",
       desired_state->browsing.has_value() && desired_state->browsing->policy.has_value()
           ? nlohmann::json(desired_state->browsing->policy->browser_session_enabled)
           : nlohmann::json(false)},
      {"ready", llm_plane && running_plane && observation_ready && runtime_ready &&
                    resolution.target.has_value()},
      {"reason", reason},
      {"plane_state",
       resolution.plane_record.has_value()
           ? nlohmann::json(resolution.plane_record->state)
           : nlohmann::json(nullptr)},
      {"primary_infer_node",
       primary_node.empty() ? nlohmann::json(nullptr) : nlohmann::json(primary_node)},
      {"worker_group_id",
       desired_state->worker_group.group_id.empty()
           ? nlohmann::json(nullptr)
           : nlohmann::json(desired_state->worker_group.group_id)},
      {"data_parallel_mode", desired_state->inference.data_parallel_mode},
      {"data_parallel_lb_mode", desired_state->inference.data_parallel_lb_mode},
      {"worker_group_expected", expected_worker_members},
      {"worker_group_ready", ready_worker_members},
      {"replica_groups_expected", expected_replica_groups},
      {"replica_groups_ready", ready_replica_groups},
      {"replica_groups_degraded", degraded_replica_groups},
      {"degraded", worker_group_degraded || replica_group_degraded},
      {"degraded_reasons", degraded_reasons},
      {"active_model_id",
       resolution.runtime_status.has_value() &&
               !resolution.runtime_status->active_model_id.empty()
           ? nlohmann::json(resolution.runtime_status->active_model_id)
           : nlohmann::json(nullptr)},
      {"served_model_name",
       resolution.runtime_status.has_value() &&
               !resolution.runtime_status->active_served_model_name.empty()
           ? nlohmann::json(resolution.runtime_status->active_served_model_name)
           : nlohmann::json(nullptr)},
      {"default_response_language",
       resolution.desired_state.interaction.has_value()
           ? nlohmann::json(
                 resolution.desired_state.interaction->default_response_language)
           : nlohmann::json(nullptr)},
      {"supported_response_languages",
       resolution.desired_state.interaction.has_value()
           ? nlohmann::json(
                 resolution.desired_state.interaction->supported_response_languages)
           : nlohmann::json(nlohmann::json::array())},
      {"follow_user_language",
       resolution.desired_state.interaction.has_value()
           ? nlohmann::json(
                 resolution.desired_state.interaction->follow_user_language)
           : nlohmann::json(true)},
      {"analysis_system_prompt_configured",
       resolution.desired_state.interaction.has_value() &&
               resolution.desired_state.interaction->analysis_system_prompt.has_value() &&
               !resolution.desired_state.interaction->analysis_system_prompt->empty()
           ? nlohmann::json(true)
           : nlohmann::json(false)},
      {"completion_policy",
       resolution.desired_state.interaction.has_value() &&
               resolution.desired_state.interaction->completion_policy.has_value()
           ? nlohmann::json{
                 {"response_mode",
                  resolution.desired_state.interaction->completion_policy
                      ->response_mode},
                 {"max_tokens",
                  resolution.desired_state.interaction->completion_policy
                      ->max_tokens},
                 {"target_completion_tokens",
                  resolution.desired_state.interaction->completion_policy
                          ->target_completion_tokens.has_value()
                      ? nlohmann::json(*resolution.desired_state.interaction
                                            ->completion_policy
                                            ->target_completion_tokens)
                      : nlohmann::json(nullptr)},
                 {"max_continuations",
                  resolution.desired_state.interaction->completion_policy
                      ->max_continuations},
                 {"max_total_completion_tokens",
                  resolution.desired_state.interaction->completion_policy
                      ->max_total_completion_tokens},
                 {"max_elapsed_time_ms",
                  resolution.desired_state.interaction->completion_policy
                      ->max_elapsed_time_ms},
                 {"semantic_goal",
                  resolution.desired_state.interaction->completion_policy
                              ->semantic_goal.has_value()
                      ? nlohmann::json(*resolution.desired_state.interaction
                                            ->completion_policy
                                            ->semantic_goal)
                      : nlohmann::json(nullptr)},
             }
           : nlohmann::json(nullptr)},
      {"long_completion_policy",
       resolution.desired_state.interaction.has_value() &&
               resolution.desired_state.interaction->long_completion_policy.has_value()
           ? nlohmann::json{
                 {"response_mode",
                  resolution.desired_state.interaction->long_completion_policy
                      ->response_mode},
                 {"max_tokens",
                  resolution.desired_state.interaction->long_completion_policy
                      ->max_tokens},
                 {"target_completion_tokens",
                  resolution.desired_state.interaction->long_completion_policy
                          ->target_completion_tokens.has_value()
                      ? nlohmann::json(*resolution.desired_state.interaction
                                            ->long_completion_policy
                                            ->target_completion_tokens)
                      : nlohmann::json(nullptr)},
                 {"max_continuations",
                  resolution.desired_state.interaction->long_completion_policy
                      ->max_continuations},
                 {"max_total_completion_tokens",
                  resolution.desired_state.interaction->long_completion_policy
                      ->max_total_completion_tokens},
                 {"max_elapsed_time_ms",
                  resolution.desired_state.interaction->long_completion_policy
                      ->max_elapsed_time_ms},
                 {"semantic_goal",
                  resolution.desired_state.interaction->long_completion_policy
                              ->semantic_goal.has_value()
                      ? nlohmann::json(*resolution.desired_state.interaction
                                            ->long_completion_policy
                                            ->semantic_goal)
                      : nlohmann::json(nullptr)},
             }
           : nlohmann::json(nullptr)},
      {"analysis_completion_policy",
       resolution.desired_state.interaction.has_value() &&
               resolution.desired_state.interaction->analysis_completion_policy
                   .has_value()
           ? nlohmann::json{
                 {"response_mode",
                  resolution.desired_state.interaction->analysis_completion_policy
                      ->response_mode},
                 {"max_tokens",
                  resolution.desired_state.interaction->analysis_completion_policy
                      ->max_tokens},
                 {"target_completion_tokens",
                  resolution.desired_state.interaction->analysis_completion_policy
                          ->target_completion_tokens.has_value()
                      ? nlohmann::json(*resolution.desired_state.interaction
                                            ->analysis_completion_policy
                                            ->target_completion_tokens)
                      : nlohmann::json(nullptr)},
                 {"max_continuations",
                  resolution.desired_state.interaction->analysis_completion_policy
                      ->max_continuations},
                 {"max_total_completion_tokens",
                  resolution.desired_state.interaction->analysis_completion_policy
                      ->max_total_completion_tokens},
                 {"max_elapsed_time_ms",
                  resolution.desired_state.interaction->analysis_completion_policy
                      ->max_elapsed_time_ms},
                 {"semantic_goal",
                  resolution.desired_state.interaction->analysis_completion_policy
                              ->semantic_goal.has_value()
                      ? nlohmann::json(*resolution.desired_state.interaction
                                            ->analysis_completion_policy
                                            ->semantic_goal)
                      : nlohmann::json(nullptr)},
             }
           : nlohmann::json(nullptr)},
      {"analysis_long_completion_policy",
       resolution.desired_state.interaction.has_value() &&
               resolution.desired_state.interaction->analysis_long_completion_policy
                   .has_value()
           ? nlohmann::json{
                 {"response_mode",
                  resolution.desired_state.interaction
                      ->analysis_long_completion_policy->response_mode},
                 {"max_tokens",
                  resolution.desired_state.interaction
                      ->analysis_long_completion_policy->max_tokens},
                 {"target_completion_tokens",
                  resolution.desired_state.interaction
                              ->analysis_long_completion_policy
                              ->target_completion_tokens.has_value()
                      ? nlohmann::json(*resolution.desired_state.interaction
                                            ->analysis_long_completion_policy
                                            ->target_completion_tokens)
                      : nlohmann::json(nullptr)},
                 {"max_continuations",
                  resolution.desired_state.interaction
                      ->analysis_long_completion_policy->max_continuations},
                 {"max_total_completion_tokens",
                  resolution.desired_state.interaction
                      ->analysis_long_completion_policy
                      ->max_total_completion_tokens},
                 {"max_elapsed_time_ms",
                  resolution.desired_state.interaction
                      ->analysis_long_completion_policy->max_elapsed_time_ms},
                 {"semantic_goal",
                  resolution.desired_state.interaction
                              ->analysis_long_completion_policy
                              ->semantic_goal.has_value()
                      ? nlohmann::json(*resolution.desired_state.interaction
                                            ->analysis_long_completion_policy
                                            ->semantic_goal)
                      : nlohmann::json(nullptr)},
             }
           : nlohmann::json(nullptr)},
      {"gateway_listen",
       resolution.runtime_status.has_value() &&
               !resolution.runtime_status->gateway_listen.empty()
           ? nlohmann::json(resolution.runtime_status->gateway_listen)
           : nlohmann::json(nullptr)},
      {"gateway_target",
       resolution.target.has_value()
           ? nlohmann::json(
                 resolution.target->host + ":" +
                 std::to_string(resolution.target->port))
           : nlohmann::json(nullptr)},
      {"runtime_status",
       resolution.runtime_status.has_value()
           ? nlohmann::json::parse(
                 comet::SerializeRuntimeStatusJson(*resolution.runtime_status))
           : nlohmann::json(nullptr)},
      {"failure_detail",
       reason == "unsupported_local_runtime"
           ? nlohmann::json(*local_runtime_blocker)
           : (reason == "runtime_start_failed" && resolution.observation.has_value() &&
                      !resolution.observation->status_message.empty()
                  ? nlohmann::json(resolution.observation->status_message)
                  : nlohmann::json(nullptr))},
  };
  return resolution;
}

}  // namespace comet::controller
