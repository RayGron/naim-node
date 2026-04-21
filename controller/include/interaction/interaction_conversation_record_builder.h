#pragma once

#include <string>
#include <vector>

#include <nlohmann/json.hpp>

#include "naim/state/sqlite_store.h"

namespace naim::controller {

class InteractionConversationRecordBuilder final {
 public:
  std::vector<naim::InteractionMessageRecord> AppendNewMessageRecords(
      std::vector<naim::InteractionMessageRecord> existing,
      const nlohmann::json& delta_messages,
      const std::string& assistant_text,
      const nlohmann::json& usage,
      const std::string& created_at) const;

  std::vector<naim::InteractionMessageRecord> AssignSessionId(
      std::vector<naim::InteractionMessageRecord> records,
      const std::string& session_id) const;

  std::vector<nlohmann::json> MessagesForArchive(
      const std::vector<naim::InteractionMessageRecord>& records) const;

  std::vector<naim::InteractionMessageRecord> BuildRestoredMessageRecords(
      const std::string& session_id,
      const nlohmann::json& messages_json,
      const std::string& created_at) const;

  std::vector<naim::InteractionSummaryRecord> BuildRestoredSummaryRecords(
      const std::string& session_id,
      const nlohmann::json& summaries_json,
      const std::string& created_at) const;
};

}  // namespace naim::controller
