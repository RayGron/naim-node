#include <iostream>
#include <stdexcept>
#include <string>

#include "interaction/interaction_conversation_record_builder.h"

namespace {

using nlohmann::json;

void Expect(bool condition, const std::string& message) {
  if (!condition) {
    throw std::runtime_error(message);
  }
}

void TestAppendsDeltaAndAssistantRecords() {
  const naim::controller::InteractionConversationRecordBuilder builder;
  std::vector<naim::InteractionMessageRecord> existing{
      naim::InteractionMessageRecord{
          "session-1",
          0,
          "user",
          "user",
          R"({"role":"user","content":"hello"})",
          "{}",
          "2026-04-09 10:00:00",
      },
  };

  auto records = builder.AppendNewMessageRecords(
      std::move(existing),
      json::array({json{{"role", "user"}, {"content", "new"}}}),
      "reply",
      json{{"prompt_tokens", 3}},
      "2026-04-09 10:01:00");
  records = builder.AssignSessionId(std::move(records), "session-1");

  Expect(records.size() == 3, "builder should append delta and assistant records");
  Expect(records.at(1).seq == 1 && records.at(2).seq == 2,
         "builder should assign sequential seq values");
  Expect(records.at(2).role == "assistant", "builder should append assistant record");
  Expect(records.at(2).session_id == "session-1",
         "builder should assign session id across records");
  std::cout << "ok: interaction-conversation-record-builder-appends-messages" << '\n';
}

void TestBuildsArchiveMessagesAndRestoresRecords() {
  const naim::controller::InteractionConversationRecordBuilder builder;
  const std::vector<naim::InteractionMessageRecord> records{
      naim::InteractionMessageRecord{
          "session-1",
          0,
          "assistant",
          "assistant",
          R"({"role":"assistant","content":"reply"})",
          "{}",
          "2026-04-09 10:01:00",
      },
  };
  const auto archive_messages = builder.MessagesForArchive(records);
  Expect(archive_messages.size() == 1, "archive conversion should preserve record count");
  Expect(archive_messages.at(0).value("content", std::string{}) == "reply",
         "archive conversion should parse stored message payload");

  const auto restored = builder.BuildRestoredMessageRecords(
      "session-2",
      json::array({json{{"role", "user"}, {"content", "restored"}}}),
      "2026-04-09 10:02:00");
  Expect(restored.size() == 1, "restore should build one message record");
  Expect(restored.at(0).session_id == "session-2",
         "restore should bind target session id");
  std::cout << "ok: interaction-conversation-record-builder-archives-messages" << '\n';
}

void TestBuildsRestoredSummaryRecords() {
  const naim::controller::InteractionConversationRecordBuilder builder;
  const auto summaries = builder.BuildRestoredSummaryRecords(
      "session-3",
      json::array({json{
          {"turn_range_start", 0},
          {"turn_range_end", 2},
          {"summary", json{{"session_goal", "goal"}}},
      }}),
      "2026-04-09 10:03:00");
  Expect(summaries.size() == 1, "restore should build summary records");
  Expect(summaries.at(0).session_id == "session-3",
         "summary restore should bind target session id");
  Expect(summaries.at(0).created_at == "2026-04-09 10:03:00",
         "summary restore should default created_at when absent");
  std::cout << "ok: interaction-conversation-record-builder-restores-summaries" << '\n';
}

}  // namespace

int main() {
  try {
    TestAppendsDeltaAndAssistantRecords();
    TestBuildsArchiveMessagesAndRestoresRecords();
    TestBuildsRestoredSummaryRecords();
    return 0;
  } catch (const std::exception& error) {
    std::cerr << "interaction_conversation_record_builder_tests failed: "
              << error.what() << '\n';
    return 1;
  }
}
