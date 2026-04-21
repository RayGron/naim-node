#include "interaction/interaction_conversation_record_builder.h"

#include "interaction/interaction_conversation_payload_builder.h"

namespace naim::controller {

namespace {

using nlohmann::json;

}  // namespace

std::vector<naim::InteractionMessageRecord>
InteractionConversationRecordBuilder::AppendNewMessageRecords(
    std::vector<naim::InteractionMessageRecord> existing,
    const json& delta_messages,
    const std::string& assistant_text,
    const json& usage,
    const std::string& created_at) const {
  const InteractionConversationPayloadBuilder payload_builder;
  int next_seq = existing.empty() ? 0 : existing.back().seq + 1;
  if (delta_messages.is_array()) {
    for (const auto& message : delta_messages) {
      naim::InteractionMessageRecord record;
      record.session_id = existing.empty() ? "" : existing.front().session_id;
      record.seq = next_seq++;
      record.role = message.value("role", std::string{});
      record.kind = record.role;
      record.content_json = payload_builder.JsonString(message);
      record.usage_json = "{}";
      record.created_at = created_at;
      existing.push_back(std::move(record));
    }
  }
  naim::InteractionMessageRecord assistant_record;
  assistant_record.session_id = existing.empty() ? "" : existing.front().session_id;
  assistant_record.seq = next_seq;
  assistant_record.role = "assistant";
  assistant_record.kind = "assistant";
  assistant_record.content_json = payload_builder.JsonString(json{
      {"role", "assistant"},
      {"content", assistant_text},
  });
  assistant_record.usage_json = payload_builder.JsonString(usage);
  assistant_record.created_at = created_at;
  existing.push_back(std::move(assistant_record));
  return existing;
}

std::vector<naim::InteractionMessageRecord>
InteractionConversationRecordBuilder::AssignSessionId(
    std::vector<naim::InteractionMessageRecord> records,
    const std::string& session_id) const {
  for (auto& record : records) {
    record.session_id = session_id;
  }
  return records;
}

std::vector<json> InteractionConversationRecordBuilder::MessagesForArchive(
    const std::vector<naim::InteractionMessageRecord>& records) const {
  const InteractionConversationPayloadBuilder payload_builder;
  std::vector<json> messages;
  messages.reserve(records.size());
  for (const auto& record : records) {
    messages.push_back(payload_builder.ParseJsonOr(record.content_json, json::object()));
  }
  return messages;
}

std::vector<naim::InteractionMessageRecord>
InteractionConversationRecordBuilder::BuildRestoredMessageRecords(
    const std::string& session_id,
    const json& messages_json,
    const std::string& created_at) const {
  const InteractionConversationPayloadBuilder payload_builder;
  std::vector<naim::InteractionMessageRecord> messages;
  int next_seq = 0;
  for (const auto& message : messages_json) {
    naim::InteractionMessageRecord record;
    record.session_id = session_id;
    record.seq = next_seq++;
    record.role = message.value("role", std::string{});
    record.kind = record.role;
    record.content_json = payload_builder.JsonString(message);
    record.usage_json = "{}";
    record.created_at = created_at;
    messages.push_back(std::move(record));
  }
  return messages;
}

std::vector<naim::InteractionSummaryRecord>
InteractionConversationRecordBuilder::BuildRestoredSummaryRecords(
    const std::string& session_id,
    const json& summaries_json,
    const std::string& created_at) const {
  const InteractionConversationPayloadBuilder payload_builder;
  std::vector<naim::InteractionSummaryRecord> summaries;
  for (const auto& summary_json : summaries_json) {
    naim::InteractionSummaryRecord summary;
    summary.session_id = session_id;
    summary.turn_range_start = summary_json.value("turn_range_start", 0);
    summary.turn_range_end = summary_json.value("turn_range_end", 0);
    summary.summary_json =
        payload_builder.JsonString(summary_json.value("summary", json::object()));
    summary.created_at = summary_json.value("created_at", created_at);
    summaries.push_back(std::move(summary));
  }
  return summaries;
}

}  // namespace naim::controller
