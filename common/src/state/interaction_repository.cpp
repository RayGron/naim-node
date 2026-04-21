#include "naim/state/interaction_repository.h"

#include <stdexcept>

#include "naim/state/sqlite_statement.h"

namespace naim {

namespace {

using Statement = SqliteStatement;

std::string ToColumnText(sqlite3_stmt* statement, int column_index) {
  const unsigned char* text = sqlite3_column_text(statement, column_index);
  if (text == nullptr) {
    return "";
  }
  return reinterpret_cast<const char*>(text);
}

std::optional<int> ToOptionalColumnInt(sqlite3_stmt* statement, int column_index) {
  if (sqlite3_column_type(statement, column_index) == SQLITE_NULL) {
    return std::nullopt;
  }
  return sqlite3_column_int(statement, column_index);
}

void Exec(sqlite3* db, const std::string& sql) {
  char* error_message = nullptr;
  const int rc = sqlite3_exec(db, sql.c_str(), nullptr, nullptr, &error_message);
  if (rc != SQLITE_OK) {
    const std::string message = error_message == nullptr ? "unknown sqlite error" : error_message;
    sqlite3_free(error_message);
    throw std::runtime_error("sqlite exec failed: " + message);
  }
}

void BindOwnerFilters(
    Statement& statement,
    int* next_index,
    const std::string& owner_kind,
    const std::optional<int>& owner_user_id) {
  statement.BindText((*next_index)++, owner_kind);
  statement.BindOptionalInt((*next_index)++, owner_user_id);
  statement.BindOptionalInt((*next_index)++, owner_user_id);
}

std::string OwnerWhereClause() {
  return " owner_kind = ? AND ((? IS NULL AND owner_user_id IS NULL) OR owner_user_id = ?)";
}

}  // namespace

InteractionRepository::InteractionRepository(sqlite3* db) : db_(db) {}

void InteractionRepository::UpsertInteractionSession(
    const InteractionSessionRecord& session) {
  Statement statement(
      db_,
      "INSERT INTO interaction_sessions("
      "session_id, plane_name, owner_kind, owner_user_id, auth_session_kind, state, "
      "last_used_at, archived_at, archive_path, archive_codec, archive_sha256, "
      "context_state_json, latest_prompt_tokens, estimated_context_tokens, compression_state, "
      "version, created_at, updated_at) "
      "VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18) "
      "ON CONFLICT(session_id) DO UPDATE SET "
      "plane_name = excluded.plane_name, "
      "owner_kind = excluded.owner_kind, "
      "owner_user_id = excluded.owner_user_id, "
      "auth_session_kind = excluded.auth_session_kind, "
      "state = excluded.state, "
      "last_used_at = excluded.last_used_at, "
      "archived_at = excluded.archived_at, "
      "archive_path = excluded.archive_path, "
      "archive_codec = excluded.archive_codec, "
      "archive_sha256 = excluded.archive_sha256, "
      "context_state_json = excluded.context_state_json, "
      "latest_prompt_tokens = excluded.latest_prompt_tokens, "
      "estimated_context_tokens = excluded.estimated_context_tokens, "
      "compression_state = excluded.compression_state, "
      "version = excluded.version, "
      "updated_at = excluded.updated_at;");
  statement.BindText(1, session.session_id);
  statement.BindText(2, session.plane_name);
  statement.BindText(3, session.owner_kind);
  statement.BindOptionalInt(4, session.owner_user_id);
  statement.BindText(5, session.auth_session_kind);
  statement.BindText(6, session.state);
  statement.BindText(7, session.last_used_at);
  statement.BindText(8, session.archived_at);
  statement.BindText(9, session.archive_path);
  statement.BindText(10, session.archive_codec);
  statement.BindText(11, session.archive_sha256);
  statement.BindText(12, session.context_state_json);
  statement.BindInt(13, session.latest_prompt_tokens);
  statement.BindInt(14, session.estimated_context_tokens);
  statement.BindText(15, session.compression_state);
  statement.BindInt(16, session.version);
  statement.BindText(17, session.created_at);
  statement.BindText(18, session.updated_at);
  statement.StepDone();
}

bool InteractionRepository::UpdateInteractionSessionVersioned(
    const InteractionSessionRecord& session,
    int expected_version) {
  Statement statement(
      db_,
      "UPDATE interaction_sessions SET "
      "plane_name = ?2, owner_kind = ?3, owner_user_id = ?4, auth_session_kind = ?5, "
      "state = ?6, last_used_at = ?7, archived_at = ?8, archive_path = ?9, archive_codec = ?10, "
      "archive_sha256 = ?11, context_state_json = ?12, latest_prompt_tokens = ?13, "
      "estimated_context_tokens = ?14, compression_state = ?15, version = ?16, updated_at = ?17 "
      "WHERE session_id = ?1 AND version = ?18;");
  statement.BindText(1, session.session_id);
  statement.BindText(2, session.plane_name);
  statement.BindText(3, session.owner_kind);
  statement.BindOptionalInt(4, session.owner_user_id);
  statement.BindText(5, session.auth_session_kind);
  statement.BindText(6, session.state);
  statement.BindText(7, session.last_used_at);
  statement.BindText(8, session.archived_at);
  statement.BindText(9, session.archive_path);
  statement.BindText(10, session.archive_codec);
  statement.BindText(11, session.archive_sha256);
  statement.BindText(12, session.context_state_json);
  statement.BindInt(13, session.latest_prompt_tokens);
  statement.BindInt(14, session.estimated_context_tokens);
  statement.BindText(15, session.compression_state);
  statement.BindInt(16, session.version);
  statement.BindText(17, session.updated_at);
  statement.BindInt(18, expected_version);
  statement.StepDone();
  return sqlite3_changes(db_) > 0;
}

std::optional<InteractionSessionRecord>
InteractionRepository::LoadInteractionSessionForOwner(
    const std::string& plane_name,
    const std::string& session_id,
    const std::string& owner_kind,
    const std::optional<int>& owner_user_id) const {
  Statement statement(
      db_,
      "SELECT session_id, plane_name, owner_kind, owner_user_id, auth_session_kind, state, "
      "last_used_at, archived_at, archive_path, archive_codec, archive_sha256, "
      "context_state_json, latest_prompt_tokens, estimated_context_tokens, compression_state, "
      "version, created_at, updated_at "
      "FROM interaction_sessions WHERE plane_name = ?1 AND session_id = ?2 AND" +
          OwnerWhereClause() + ";");
  int index = 1;
  statement.BindText(index++, plane_name);
  statement.BindText(index++, session_id);
  BindOwnerFilters(statement, &index, owner_kind, owner_user_id);
  if (!statement.StepRow()) {
    return std::nullopt;
  }
  return ReadInteractionSession(statement.raw());
}

std::optional<InteractionSessionRecord>
InteractionRepository::LoadInteractionSessionForOwnerAnyPlane(
    const std::string& session_id,
    const std::string& owner_kind,
    const std::optional<int>& owner_user_id) const {
  Statement statement(
      db_,
      "SELECT session_id, plane_name, owner_kind, owner_user_id, auth_session_kind, state, "
      "last_used_at, archived_at, archive_path, archive_codec, archive_sha256, "
      "context_state_json, latest_prompt_tokens, estimated_context_tokens, compression_state, "
      "version, created_at, updated_at "
      "FROM interaction_sessions WHERE session_id = ?1 AND" + OwnerWhereClause() +
          " ORDER BY updated_at DESC LIMIT 1;");
  int index = 1;
  statement.BindText(index++, session_id);
  BindOwnerFilters(statement, &index, owner_kind, owner_user_id);
  if (!statement.StepRow()) {
    return std::nullopt;
  }
  return ReadInteractionSession(statement.raw());
}

std::vector<InteractionSessionRecord>
InteractionRepository::LoadInteractionSessionsForUser(
    const std::string& plane_name,
    int user_id) const {
  std::vector<InteractionSessionRecord> sessions;
  Statement statement(
      db_,
      "SELECT session_id, plane_name, owner_kind, owner_user_id, auth_session_kind, state, "
      "last_used_at, archived_at, archive_path, archive_codec, archive_sha256, "
      "context_state_json, latest_prompt_tokens, estimated_context_tokens, compression_state, "
      "version, created_at, updated_at "
      "FROM interaction_sessions "
      "WHERE plane_name = ?1 AND owner_kind = 'user' AND owner_user_id = ?2 "
      "ORDER BY updated_at DESC, session_id ASC;");
  statement.BindText(1, plane_name);
  statement.BindInt(2, user_id);
  while (statement.StepRow()) {
    sessions.push_back(ReadInteractionSession(statement.raw()));
  }
  return sessions;
}

std::vector<InteractionSessionRecord>
InteractionRepository::LoadArchiveEligibleInteractionSessions(
    const std::string& cutoff_timestamp,
    int limit) const {
  std::vector<InteractionSessionRecord> sessions;
  Statement statement(
      db_,
      "SELECT session_id, plane_name, owner_kind, owner_user_id, auth_session_kind, state, "
      "last_used_at, archived_at, archive_path, archive_codec, archive_sha256, "
      "context_state_json, latest_prompt_tokens, estimated_context_tokens, compression_state, "
      "version, created_at, updated_at "
      "FROM interaction_sessions "
      "WHERE state = 'active' AND last_used_at != '' AND last_used_at < ?1 "
      "ORDER BY last_used_at ASC LIMIT ?2;");
  statement.BindText(1, cutoff_timestamp);
  statement.BindInt(2, limit);
  while (statement.StepRow()) {
    sessions.push_back(ReadInteractionSession(statement.raw()));
  }
  return sessions;
}

void InteractionRepository::ReplaceInteractionMessages(
    const std::string& session_id,
    const std::vector<InteractionMessageRecord>& messages) {
  Exec(db_, "BEGIN IMMEDIATE TRANSACTION;");
  try {
    {
      Statement clear_statement(
          db_,
          "DELETE FROM interaction_messages WHERE session_id = ?1;");
      clear_statement.BindText(1, session_id);
      clear_statement.StepDone();
    }
    for (const auto& message : messages) {
      Statement insert_statement(
          db_,
          "INSERT INTO interaction_messages("
          "session_id, seq, role, kind, content_json, usage_json, created_at) "
          "VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7);");
      insert_statement.BindText(1, session_id);
      insert_statement.BindInt(2, message.seq);
      insert_statement.BindText(3, message.role);
      insert_statement.BindText(4, message.kind);
      insert_statement.BindText(5, message.content_json);
      insert_statement.BindText(6, message.usage_json);
      insert_statement.BindText(7, message.created_at);
      insert_statement.StepDone();
    }
    Exec(db_, "COMMIT;");
  } catch (...) {
    Exec(db_, "ROLLBACK;");
    throw;
  }
}

std::vector<InteractionMessageRecord> InteractionRepository::LoadInteractionMessages(
    const std::string& session_id) const {
  std::vector<InteractionMessageRecord> messages;
  Statement statement(
      db_,
      "SELECT session_id, seq, role, kind, content_json, usage_json, created_at "
      "FROM interaction_messages WHERE session_id = ?1 ORDER BY seq ASC;");
  statement.BindText(1, session_id);
  while (statement.StepRow()) {
    messages.push_back(ReadInteractionMessage(statement.raw()));
  }
  return messages;
}

void InteractionRepository::ReplaceInteractionSummaries(
    const std::string& session_id,
    const std::vector<InteractionSummaryRecord>& summaries) {
  Exec(db_, "BEGIN IMMEDIATE TRANSACTION;");
  try {
    {
      Statement clear_statement(
          db_,
          "DELETE FROM interaction_summaries WHERE session_id = ?1;");
      clear_statement.BindText(1, session_id);
      clear_statement.StepDone();
    }
    for (const auto& summary : summaries) {
      Statement insert_statement(
          db_,
          "INSERT INTO interaction_summaries("
          "session_id, turn_range_start, turn_range_end, summary_json, created_at) "
          "VALUES(?1, ?2, ?3, ?4, ?5);");
      insert_statement.BindText(1, session_id);
      insert_statement.BindInt(2, summary.turn_range_start);
      insert_statement.BindInt(3, summary.turn_range_end);
      insert_statement.BindText(4, summary.summary_json);
      insert_statement.BindText(5, summary.created_at);
      insert_statement.StepDone();
    }
    Exec(db_, "COMMIT;");
  } catch (...) {
    Exec(db_, "ROLLBACK;");
    throw;
  }
}

std::vector<InteractionSummaryRecord> InteractionRepository::LoadInteractionSummaries(
    const std::string& session_id) const {
  std::vector<InteractionSummaryRecord> summaries;
  Statement statement(
      db_,
      "SELECT id, session_id, turn_range_start, turn_range_end, summary_json, created_at "
      "FROM interaction_summaries WHERE session_id = ?1 ORDER BY turn_range_start ASC, id ASC;");
  statement.BindText(1, session_id);
  while (statement.StepRow()) {
    summaries.push_back(ReadInteractionSummary(statement.raw()));
  }
  return summaries;
}

void InteractionRepository::UpsertInteractionArchive(
    const InteractionArchiveRecord& archive) {
  Statement statement(
      db_,
      "INSERT INTO interaction_archives("
      "session_id, plane_name, owner_kind, owner_user_id, archive_path, archive_codec, "
      "archive_sha256, archived_at, restore_state, created_at, updated_at) "
      "VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11) "
      "ON CONFLICT(session_id) DO UPDATE SET "
      "plane_name = excluded.plane_name, "
      "owner_kind = excluded.owner_kind, "
      "owner_user_id = excluded.owner_user_id, "
      "archive_path = excluded.archive_path, "
      "archive_codec = excluded.archive_codec, "
      "archive_sha256 = excluded.archive_sha256, "
      "archived_at = excluded.archived_at, "
      "restore_state = excluded.restore_state, "
      "updated_at = excluded.updated_at;");
  statement.BindText(1, archive.session_id);
  statement.BindText(2, archive.plane_name);
  statement.BindText(3, archive.owner_kind);
  statement.BindOptionalInt(4, archive.owner_user_id);
  statement.BindText(5, archive.archive_path);
  statement.BindText(6, archive.archive_codec);
  statement.BindText(7, archive.archive_sha256);
  statement.BindText(8, archive.archived_at);
  statement.BindText(9, archive.restore_state);
  statement.BindText(10, archive.created_at);
  statement.BindText(11, archive.updated_at);
  statement.StepDone();
}

std::optional<InteractionArchiveRecord>
InteractionRepository::LoadInteractionArchiveForOwner(
    const std::string& plane_name,
    const std::string& session_id,
    const std::string& owner_kind,
    const std::optional<int>& owner_user_id) const {
  Statement statement(
      db_,
      "SELECT session_id, plane_name, owner_kind, owner_user_id, archive_path, archive_codec, "
      "archive_sha256, archived_at, restore_state, created_at, updated_at "
      "FROM interaction_archives WHERE plane_name = ?1 AND session_id = ?2 AND" +
          OwnerWhereClause() + ";");
  int index = 1;
  statement.BindText(index++, plane_name);
  statement.BindText(index++, session_id);
  BindOwnerFilters(statement, &index, owner_kind, owner_user_id);
  if (!statement.StepRow()) {
    return std::nullopt;
  }
  return ReadInteractionArchive(statement.raw());
}

bool InteractionRepository::DeleteInteractionSessionForOwner(
    const std::string& plane_name,
    const std::string& session_id,
    const std::string& owner_kind,
    const std::optional<int>& owner_user_id) {
  Exec(db_, "BEGIN IMMEDIATE TRANSACTION;");
  try {
    {
      Statement archive_statement(
          db_,
          "DELETE FROM interaction_archives WHERE plane_name = ?1 AND session_id = ?2 AND" +
              OwnerWhereClause() + ";");
      int index = 1;
      archive_statement.BindText(index++, plane_name);
      archive_statement.BindText(index++, session_id);
      BindOwnerFilters(archive_statement, &index, owner_kind, owner_user_id);
      archive_statement.StepDone();
    }
    Statement statement(
        db_,
        "DELETE FROM interaction_sessions WHERE plane_name = ?1 AND session_id = ?2 AND" +
            OwnerWhereClause() + ";");
    int index = 1;
    statement.BindText(index++, plane_name);
    statement.BindText(index++, session_id);
    BindOwnerFilters(statement, &index, owner_kind, owner_user_id);
    statement.StepDone();
    const bool deleted = sqlite3_changes(db_) > 0;
    Exec(db_, "COMMIT;");
    return deleted;
  } catch (...) {
    Exec(db_, "ROLLBACK;");
    throw;
  }
}

InteractionSessionRecord InteractionRepository::ReadInteractionSession(sqlite3_stmt* statement) {
  InteractionSessionRecord session;
  session.session_id = ToColumnText(statement, 0);
  session.plane_name = ToColumnText(statement, 1);
  session.owner_kind = ToColumnText(statement, 2);
  session.owner_user_id = ToOptionalColumnInt(statement, 3);
  session.auth_session_kind = ToColumnText(statement, 4);
  session.state = ToColumnText(statement, 5);
  session.last_used_at = ToColumnText(statement, 6);
  session.archived_at = ToColumnText(statement, 7);
  session.archive_path = ToColumnText(statement, 8);
  session.archive_codec = ToColumnText(statement, 9);
  session.archive_sha256 = ToColumnText(statement, 10);
  session.context_state_json = ToColumnText(statement, 11);
  session.latest_prompt_tokens = sqlite3_column_int(statement, 12);
  session.estimated_context_tokens = sqlite3_column_int(statement, 13);
  session.compression_state = ToColumnText(statement, 14);
  session.version = sqlite3_column_int(statement, 15);
  session.created_at = ToColumnText(statement, 16);
  session.updated_at = ToColumnText(statement, 17);
  return session;
}

InteractionMessageRecord InteractionRepository::ReadInteractionMessage(sqlite3_stmt* statement) {
  InteractionMessageRecord message;
  message.session_id = ToColumnText(statement, 0);
  message.seq = sqlite3_column_int(statement, 1);
  message.role = ToColumnText(statement, 2);
  message.kind = ToColumnText(statement, 3);
  message.content_json = ToColumnText(statement, 4);
  message.usage_json = ToColumnText(statement, 5);
  message.created_at = ToColumnText(statement, 6);
  return message;
}

InteractionSummaryRecord InteractionRepository::ReadInteractionSummary(sqlite3_stmt* statement) {
  InteractionSummaryRecord summary;
  summary.id = sqlite3_column_int(statement, 0);
  summary.session_id = ToColumnText(statement, 1);
  summary.turn_range_start = sqlite3_column_int(statement, 2);
  summary.turn_range_end = sqlite3_column_int(statement, 3);
  summary.summary_json = ToColumnText(statement, 4);
  summary.created_at = ToColumnText(statement, 5);
  return summary;
}

InteractionArchiveRecord InteractionRepository::ReadInteractionArchive(sqlite3_stmt* statement) {
  InteractionArchiveRecord archive;
  archive.session_id = ToColumnText(statement, 0);
  archive.plane_name = ToColumnText(statement, 1);
  archive.owner_kind = ToColumnText(statement, 2);
  archive.owner_user_id = ToOptionalColumnInt(statement, 3);
  archive.archive_path = ToColumnText(statement, 4);
  archive.archive_codec = ToColumnText(statement, 5);
  archive.archive_sha256 = ToColumnText(statement, 6);
  archive.archived_at = ToColumnText(statement, 7);
  archive.restore_state = ToColumnText(statement, 8);
  archive.created_at = ToColumnText(statement, 9);
  archive.updated_at = ToColumnText(statement, 10);
  return archive;
}

}  // namespace naim
