#include "interaction/interaction_conversation_payload_builder.h"

#include <algorithm>
#include <sstream>

namespace naim::controller {

namespace {

using nlohmann::json;

constexpr int kRecentMessageTailCount = 8;

}  // namespace

json InteractionConversationPayloadBuilder::ParseJsonOr(
    const std::string& text,
    const json& fallback) const {
  if (text.empty()) {
    return fallback;
  }
  const json parsed = json::parse(text, nullptr, false);
  if (parsed.is_discarded()) {
    return fallback;
  }
  return parsed;
}

json InteractionConversationPayloadBuilder::ParseJsonObject(
    const std::string& text) const {
  const json parsed = ParseJsonOr(text, json::object());
  return parsed.is_object() ? parsed : json::object();
}

std::string InteractionConversationPayloadBuilder::JsonString(
    const json& value) const {
  return value.dump(-1, ' ', false, json::error_handler_t::replace);
}

int InteractionConversationPayloadBuilder::EstimateTokensForJson(
    const json& value) const {
  const std::size_t bytes = JsonString(value).size();
  return std::max(1, static_cast<int>((bytes + 3) / 4));
}

std::vector<json> InteractionConversationPayloadBuilder::MessageRecordsToJson(
    const std::vector<naim::InteractionMessageRecord>& records) const {
  std::vector<json> messages;
  messages.reserve(records.size());
  for (const auto& record : records) {
    messages.push_back(ParseJsonOr(record.content_json, json{
                                                       {"role", record.role},
                                                       {"content", json(nullptr)},
                                                   }));
  }
  return messages;
}

std::size_t InteractionConversationPayloadBuilder::CommonPrefixLength(
    const std::vector<json>& stored_messages,
    const json& incoming_messages) const {
  if (!incoming_messages.is_array()) {
    return 0;
  }
  const std::size_t limit =
      std::min(stored_messages.size(), incoming_messages.size());
  std::size_t prefix = 0;
  while (prefix < limit &&
         JsonEqual(stored_messages[prefix], incoming_messages[prefix])) {
    ++prefix;
  }
  return prefix;
}

json InteractionConversationPayloadBuilder::BuildPromptMessages(
    const std::vector<naim::InteractionSummaryRecord>& summaries,
    const std::vector<json>& stored_messages,
    const json& delta_messages) const {
  json prompt_messages = json::array();
  const std::string summary_instruction = BuildSummarySystemInstruction(summaries);
  if (!summary_instruction.empty()) {
    prompt_messages.push_back(
        json{{"role", "system"}, {"content", summary_instruction}});
  }
  const json tail_with_delta = TailMessagesWithDelta(stored_messages, delta_messages);
  for (const auto& message : tail_with_delta) {
    prompt_messages.push_back(message);
  }
  return prompt_messages;
}

std::vector<naim::InteractionSummaryRecord>
InteractionConversationPayloadBuilder::BuildSummaryRecords(
    const std::string& session_id,
    const std::vector<json>& all_messages,
    const json& context_state,
    const std::string& created_at,
    int max_model_len,
    const json& prompt_messages) const {
  const int estimated_prompt_tokens = EstimateTokensForJson(prompt_messages);
  const int soft_limit = std::max(1, (max_model_len * 60) / 100);
  if (all_messages.size() <= static_cast<std::size_t>(kRecentMessageTailCount) &&
      estimated_prompt_tokens <= soft_limit) {
    return {};
  }

  const int summary_end =
      static_cast<int>(all_messages.size()) - kRecentMessageTailCount - 1;
  if (summary_end < 0) {
    return {};
  }

  std::vector<json> summary_messages;
  summary_messages.reserve(static_cast<std::size_t>(summary_end) + 1);
  for (int index = 0; index <= summary_end; ++index) {
    summary_messages.push_back(all_messages[static_cast<std::size_t>(index)]);
  }

  return {
      naim::InteractionSummaryRecord{
          0,
          session_id,
          0,
          summary_end,
          JsonString(BuildSummaryJson(summary_messages, context_state, 0, summary_end)),
          created_at,
      },
  };
}

json InteractionConversationPayloadBuilder::BuildSessionSummaryPayload(
    const naim::InteractionSessionRecord& session,
    std::size_t message_count,
    std::size_t summary_count) const {
  const json context_state = ParseJsonObject(session.context_state_json);
  return json{
      {"id", session.session_id},
      {"plane_name", session.plane_name},
      {"state", session.state},
      {"owner_kind", session.owner_kind},
      {"owner_user_id",
       session.owner_user_id.has_value() ? json(*session.owner_user_id) : json(nullptr)},
      {"auth_session_kind",
       session.auth_session_kind.empty() ? json(nullptr) : json(session.auth_session_kind)},
      {"created_at", session.created_at},
      {"updated_at", session.updated_at},
      {"last_used_at", session.last_used_at},
      {"archived_at",
       session.archived_at.empty() ? json(nullptr) : json(session.archived_at)},
      {"archive_path",
       session.archive_path.empty() ? json(nullptr) : json(session.archive_path)},
      {"compression_state", session.compression_state},
      {"latest_prompt_tokens", session.latest_prompt_tokens},
      {"estimated_context_tokens", session.estimated_context_tokens},
      {"message_count", static_cast<int>(message_count)},
      {"summary_count", static_cast<int>(summary_count)},
      {"browsing_mode",
       context_state.value("browsing_mode", std::string{"disabled"})},
      {"applied_skill_ids",
       context_state.value("applied_skill_ids", json::array())},
  };
}

json InteractionConversationPayloadBuilder::BuildSessionMessagesPayload(
    const std::vector<naim::InteractionMessageRecord>& messages) const {
  json payload = json::array();
  for (const auto& message : messages) {
    json item = ParseJsonOr(message.content_json, json::object());
    item["seq"] = message.seq;
    item["created_at"] = message.created_at;
    if (const json usage = ParseJsonObject(message.usage_json); !usage.empty()) {
      item["usage"] = usage;
    }
    payload.push_back(std::move(item));
  }
  return payload;
}

json InteractionConversationPayloadBuilder::BuildSessionSummariesPayload(
    const std::vector<naim::InteractionSummaryRecord>& summaries) const {
  json payload = json::array();
  for (const auto& summary : summaries) {
    payload.push_back(json{
        {"turn_range_start", summary.turn_range_start},
        {"turn_range_end", summary.turn_range_end},
        {"summary", ParseJsonObject(summary.summary_json)},
        {"created_at", summary.created_at},
    });
  }
  return payload;
}

std::string InteractionConversationPayloadBuilder::TrimCopy(
    const std::string& value) const {
  const auto begin = value.find_first_not_of(" \t\r\n");
  if (begin == std::string::npos) {
    return "";
  }
  const auto end = value.find_last_not_of(" \t\r\n");
  return value.substr(begin, end - begin + 1);
}

bool InteractionConversationPayloadBuilder::JsonEqual(
    const json& left,
    const json& right) const {
  return JsonString(left) == JsonString(right);
}

json InteractionConversationPayloadBuilder::TailMessagesWithDelta(
    const std::vector<json>& stored_messages,
    const json& delta_messages) const {
  json merged = json::array();
  const std::size_t tail_count =
      std::min(stored_messages.size(), static_cast<std::size_t>(kRecentMessageTailCount));
  for (std::size_t index = stored_messages.size() - tail_count;
       index < stored_messages.size();
       ++index) {
    merged.push_back(stored_messages[index]);
  }
  if (delta_messages.is_array()) {
    for (const auto& message : delta_messages) {
      merged.push_back(message);
    }
  }
  return merged;
}

std::string InteractionConversationPayloadBuilder::ExcerptText(
    const json& message,
    std::size_t max_bytes) const {
  std::string text;
  if (message.is_object()) {
    if (message.contains("content") && message.at("content").is_string()) {
      text = message.at("content").get<std::string>();
    } else if (message.contains("content")) {
      text = JsonString(message.at("content"));
    }
  } else if (message.is_string()) {
    text = message.get<std::string>();
  } else {
    text = JsonString(message);
  }
  text = TrimCopy(text);
  if (text.size() > max_bytes) {
    text = text.substr(0, max_bytes) + "...[truncated]";
  }
  return text;
}

json InteractionConversationPayloadBuilder::BuildSummaryJson(
    const std::vector<json>& messages,
    const json& context_state,
    int turn_range_start,
    int turn_range_end) const {
  json stable_facts = json::array();
  json decisions = json::array();
  json open_threads = json::array();
  json user_preferences = json::array();
  std::string session_goal;

  for (const auto& message : messages) {
    if (!message.is_object()) {
      continue;
    }
    const std::string role = message.value("role", std::string{});
    const std::string excerpt = ExcerptText(message);
    if (excerpt.empty()) {
      continue;
    }
    if (role == "user") {
      if (session_goal.empty()) {
        session_goal = excerpt;
      }
      if (stable_facts.size() < 4) {
        stable_facts.push_back(excerpt);
      } else if (open_threads.size() < 3) {
        open_threads.push_back(excerpt);
      }
    } else if (role == "assistant") {
      if (decisions.size() < 4) {
        decisions.push_back(excerpt);
      }
    } else if (role == "system" && user_preferences.size() < 2) {
      user_preferences.push_back(excerpt);
    }
  }

  json applied_skill_ids = json::array();
  if (context_state.contains("applied_skill_ids") &&
      context_state.at("applied_skill_ids").is_array()) {
    applied_skill_ids = context_state.at("applied_skill_ids");
  }

  return json{
      {"session_goal", session_goal},
      {"stable_facts", stable_facts},
      {"user_preferences", user_preferences},
      {"decisions", decisions},
      {"artifacts_and_ids", json::array()},
      {"open_threads", open_threads},
      {"browsing_mode", context_state.value("browsing_mode", std::string{"disabled"})},
      {"applied_skill_ids", applied_skill_ids},
      {"turn_range",
       json{{"start", turn_range_start}, {"end", turn_range_end}}},
  };
}

std::string InteractionConversationPayloadBuilder::BuildSummarySystemInstruction(
    const std::vector<naim::InteractionSummaryRecord>& summaries) const {
  if (summaries.empty()) {
    return "";
  }
  std::ostringstream instruction;
  instruction << "Conversation memory summary maintained by naim-node.";
  for (const auto& record : summaries) {
    const json summary = ParseJsonObject(record.summary_json);
    instruction << "\n\nSummary block turns "
                << summary.value("turn_range", json::object()).value("start", 0)
                << "-" << summary.value("turn_range", json::object()).value("end", 0)
                << ":";
    const std::string goal = summary.value("session_goal", std::string{});
    if (!goal.empty()) {
      instruction << "\n- Goal: " << goal;
    }
    const auto append_lines = [&](const char* label, const char* key) {
      if (!summary.contains(key) || !summary.at(key).is_array() ||
          summary.at(key).empty()) {
        return;
      }
      instruction << "\n- " << label << ":";
      for (const auto& item : summary.at(key)) {
        instruction << "\n  * " << item.get<std::string>();
      }
    };
    append_lines("Stable facts", "stable_facts");
    append_lines("User preferences", "user_preferences");
    append_lines("Decisions", "decisions");
    append_lines("Open threads", "open_threads");
    const std::string browsing_mode =
        summary.value("browsing_mode", std::string{});
    if (!browsing_mode.empty()) {
      instruction << "\n- Browsing mode: " << browsing_mode;
    }
  }
  return instruction.str();
}

}  // namespace naim::controller
