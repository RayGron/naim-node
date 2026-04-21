#pragma once

#include <optional>
#include <string>
#include <vector>

#include <sqlite3.h>

#include "naim/state/sqlite_store.h"

namespace naim {

class InteractionRepository final {
 public:
  explicit InteractionRepository(sqlite3* db);

  void UpsertInteractionSession(const InteractionSessionRecord& session);
  bool UpdateInteractionSessionVersioned(
      const InteractionSessionRecord& session,
      int expected_version);
  std::optional<InteractionSessionRecord> LoadInteractionSessionForOwner(
      const std::string& plane_name,
      const std::string& session_id,
      const std::string& owner_kind,
      const std::optional<int>& owner_user_id) const;
  std::optional<InteractionSessionRecord> LoadInteractionSessionForOwnerAnyPlane(
      const std::string& session_id,
      const std::string& owner_kind,
      const std::optional<int>& owner_user_id) const;
  std::vector<InteractionSessionRecord> LoadInteractionSessionsForUser(
      const std::string& plane_name,
      int user_id) const;
  std::vector<InteractionSessionRecord> LoadArchiveEligibleInteractionSessions(
      const std::string& cutoff_timestamp,
      int limit) const;

  void ReplaceInteractionMessages(
      const std::string& session_id,
      const std::vector<InteractionMessageRecord>& messages);
  std::vector<InteractionMessageRecord> LoadInteractionMessages(
      const std::string& session_id) const;

  void ReplaceInteractionSummaries(
      const std::string& session_id,
      const std::vector<InteractionSummaryRecord>& summaries);
  std::vector<InteractionSummaryRecord> LoadInteractionSummaries(
      const std::string& session_id) const;

  void UpsertInteractionArchive(const InteractionArchiveRecord& archive);
  std::optional<InteractionArchiveRecord> LoadInteractionArchiveForOwner(
      const std::string& plane_name,
      const std::string& session_id,
      const std::string& owner_kind,
      const std::optional<int>& owner_user_id) const;
  bool DeleteInteractionSessionForOwner(
      const std::string& plane_name,
      const std::string& session_id,
      const std::string& owner_kind,
      const std::optional<int>& owner_user_id);

 private:
  static InteractionSessionRecord ReadInteractionSession(sqlite3_stmt* statement);
  static InteractionMessageRecord ReadInteractionMessage(sqlite3_stmt* statement);
  static InteractionSummaryRecord ReadInteractionSummary(sqlite3_stmt* statement);
  static InteractionArchiveRecord ReadInteractionArchive(sqlite3_stmt* statement);

  sqlite3* db_;
};

}  // namespace naim
