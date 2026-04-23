#include <iostream>
#include <stdexcept>
#include <string>
#include <vector>

#include "interaction/interaction_conversation_payload_builder.h"

namespace {

using nlohmann::json;

void Expect(bool condition, const std::string& message) {
  if (!condition) {
    throw std::runtime_error(message);
  }
}

void TestBuildsPromptMessagesWithSummaryAndTail() {
  const naim::controller::InteractionConversationPayloadBuilder builder;
  const std::vector<naim::InteractionSummaryRecord> summaries{
      naim::InteractionSummaryRecord{
          1,
          "session-1",
          0,
          3,
          R"({"session_goal":"ship feature","stable_facts":["fact"],"user_preferences":[],"decisions":["done"],"open_threads":[],"browsing_mode":"enabled","turn_range":{"start":0,"end":3}})",
          "2026-04-09 10:00:00",
      },
  };
  const std::vector<json> stored_messages{
      json{{"role", "user"}, {"content", "m1"}},
      json{{"role", "assistant"}, {"content", "m2"}},
      json{{"role", "user"}, {"content", "m3"}},
  };
  const json delta_messages = json::array(
      {json{{"role", "user"}, {"content", "new"}}});

  const json prompt = builder.BuildPromptMessages(
      summaries,
      stored_messages,
      delta_messages);
  Expect(prompt.is_array(), "prompt payload should be an array");
  Expect(prompt.size() == 5, "prompt should include summary, tail, and delta");
  Expect(prompt.at(0).value("role", std::string{}) == "system",
         "prompt should prepend summary instruction");
  Expect(prompt.at(0).value("content", std::string{}).find("ship feature") != std::string::npos,
         "summary instruction should include session goal");
  Expect(prompt.at(4).value("content", std::string{}) == "new",
         "prompt should append delta message");
  std::cout << "ok: interaction-conversation-payload-builds-prompt" << '\n';
}

void TestBuildsSummaryRecordsForLargeHistory() {
  const naim::controller::InteractionConversationPayloadBuilder builder;
  std::vector<json> messages;
  for (int index = 0; index < 10; ++index) {
    messages.push_back(json{
        {"role", (index % 2 == 0) ? "user" : "assistant"},
        {"content", "message-" + std::to_string(index)},
    });
  }
  const json context_state = {
      {"browsing_mode", "enabled"},
      {"applied_skill_ids", json::array({"skill-a"})},
  };
  const json prompt_messages = messages;

  const auto summaries = builder.BuildSummaryRecords(
      "session-1",
      messages,
      context_state,
      "2026-04-09 10:00:00",
      8,
      prompt_messages);
  Expect(summaries.size() == 1, "large histories should produce one summary block");
  const json summary = builder.ParseJsonObject(summaries.front().summary_json);
  Expect(summary.value("browsing_mode", std::string{}) == "enabled",
         "summary should preserve browsing mode");
  Expect(summary.at("applied_skill_ids").size() == 1,
         "summary should preserve applied skill ids");
  std::cout << "ok: interaction-conversation-payload-builds-summary-records" << '\n';
}

void TestBuildsSessionPayloads() {
  const naim::controller::InteractionConversationPayloadBuilder builder;
  naim::InteractionSessionRecord session;
  session.session_id = "session-1";
  session.plane_name = "plane-a";
  session.state = "active";
  session.owner_kind = "user";
  session.owner_user_id = 7;
  session.context_state_json =
      R"({"browsing_mode":"enabled","applied_skill_ids":["skill-a"],"context_compression":{"enabled":true,"mode":"auto","target":"dialog_and_knowledge","warnings":["trimmed"],"compression_ratio":0.5}})";
  session.created_at = "2026-04-09 10:00:00";
  session.updated_at = "2026-04-09 10:01:00";
  session.last_used_at = "2026-04-09 10:01:00";

  const auto summary_payload = builder.BuildSessionSummaryPayload(session, 2, 1);
  Expect(summary_payload.at("browsing_mode").get<std::string>() == "enabled",
         "session summary should expose browsing mode");
  Expect(summary_payload.at("applied_skill_ids").size() == 1,
         "session summary should expose applied skill ids");
  Expect(summary_payload.at("context_compression_enabled").get<bool>(),
         "session summary should expose context compression flag");
  Expect(summary_payload.at("compression_mode").get<std::string>() == "auto",
         "session summary should expose compression mode");
  Expect(summary_payload.at("compression_target").get<std::string>() == "dialog_and_knowledge",
         "session summary should expose compression target");
  Expect(summary_payload.at("compression_warning_count").get<int>() == 1,
         "session summary should expose warning count");
  Expect(summary_payload.at("last_compression_ratio").get<double>() == 0.5,
         "session summary should expose compression ratio");

  const std::vector<naim::InteractionMessageRecord> messages{
      naim::InteractionMessageRecord{
          "session-1",
          0,
          "assistant",
          "assistant",
          R"({"role":"assistant","content":"reply"})",
          R"({"prompt_tokens":3})",
          "2026-04-09 10:01:00",
      },
  };
  const auto message_payload = builder.BuildSessionMessagesPayload(messages);
  Expect(message_payload.at(0).at("usage").at("prompt_tokens").get<int>() == 3,
         "message payload should expose parsed usage");
  std::cout << "ok: interaction-conversation-payload-builds-session-payloads" << '\n';
}

}  // namespace

int main() {
  try {
    TestBuildsPromptMessagesWithSummaryAndTail();
    TestBuildsSummaryRecordsForLargeHistory();
    TestBuildsSessionPayloads();
    return 0;
  } catch (const std::exception& error) {
    std::cerr << "interaction_conversation_payload_builder_tests failed: "
              << error.what() << '\n';
    return 1;
  }
}
