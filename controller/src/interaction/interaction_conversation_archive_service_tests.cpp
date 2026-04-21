#include <chrono>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <stdexcept>
#include <string>

#include <sqlite3.h>
#include <zstd.h>

#include "naim/security/crypto_utils.h"
#include "naim/state/sqlite_store.h"
#include "interaction/interaction_conversation_archive_service.h"
#include "interaction/interaction_conversation_payload_builder.h"

namespace {

using nlohmann::json;

void Expect(bool condition, const std::string& message) {
  if (!condition) {
    throw std::runtime_error(message);
  }
}

std::filesystem::path MakeTempRoot(const std::string& name) {
  const auto stamp = std::chrono::duration_cast<std::chrono::microseconds>(
                         std::chrono::system_clock::now().time_since_epoch())
                         .count();
  return std::filesystem::temp_directory_path() /
         ("naim-" + name + "-" + std::to_string(stamp));
}

std::string CompressStringZstd(const std::string& input) {
  std::string output(ZSTD_compressBound(input.size()), '\0');
  const std::size_t compressed_size = ZSTD_compress(
      output.data(),
      output.size(),
      input.data(),
      input.size(),
      3);
  if (ZSTD_isError(compressed_size) != 0) {
    throw std::runtime_error("failed to compress archive payload");
  }
  output.resize(compressed_size);
  return output;
}

void WriteBinaryFile(const std::filesystem::path& path, const std::string& contents) {
  std::filesystem::create_directories(path.parent_path());
  std::ofstream output(path, std::ios::binary);
  output.write(contents.data(), static_cast<std::streamsize>(contents.size()));
  if (!output.good()) {
    throw std::runtime_error("failed to write archive test file");
  }
}

void InsertPlane(const std::string& db_path, const std::string& plane_name) {
  sqlite3* db = nullptr;
  if (sqlite3_open(db_path.c_str(), &db) != SQLITE_OK) {
    throw std::runtime_error("failed to open sqlite db for plane insert");
  }
  const std::string sql =
      "INSERT INTO planes(name, shared_disk_name, state) VALUES('" + plane_name +
      "', '" + plane_name + "-shared', 'running');";
  char* error_message = nullptr;
  const int rc = sqlite3_exec(db, sql.c_str(), nullptr, nullptr, &error_message);
  const std::string error =
      error_message != nullptr ? std::string(error_message) : std::string{};
  sqlite3_free(error_message);
  sqlite3_close(db);
  if (rc != SQLITE_OK) {
    throw std::runtime_error("failed to insert plane: " + error);
  }
}

void TestArchivesInactiveSessions() {
  const auto root = MakeTempRoot("interaction-archive");
  std::filesystem::create_directories(root);
  const auto cleanup = [&]() { std::filesystem::remove_all(root); };
  try {
    const std::string db_path = (root / "controller.sqlite").string();
    naim::ControllerStore store(db_path);
    store.Initialize();
    InsertPlane(db_path, "plane-a");
    const auto user = store.CreateBootstrapAdmin("archive-user", "hash");

    naim::InteractionSessionRecord session;
    session.session_id = "session-1";
    session.plane_name = "plane-a";
    session.owner_kind = "user";
    session.owner_user_id = user.id;
    session.state = "active";
    session.last_used_at = "2020-01-01 00:00:00";
    session.created_at = "2020-01-01 00:00:00";
    session.updated_at = "2020-01-01 00:00:00";
    store.UpsertInteractionSession(session);
    store.ReplaceInteractionMessages(
        session.session_id,
        {naim::InteractionMessageRecord{
            session.session_id,
            0,
            "user",
            "user",
            R"({"role":"user","content":"hello"})",
            "{}",
            "2020-01-01 00:00:00",
        }});
    store.ReplaceInteractionSummaries(
        session.session_id,
        {naim::InteractionSummaryRecord{
            0,
            session.session_id,
            0,
            0,
            R"({"session_goal":"goal"})",
            "2020-01-01 00:00:00",
        }});

    const naim::controller::InteractionConversationArchiveService archive_service;
    archive_service.MaybeArchiveInactiveSessions(db_path);

    const auto archived = store.LoadInteractionSessionForOwner(
        "plane-a", "session-1", "user", user.id);
    Expect(archived.has_value(), "archived session should remain queryable");
    Expect(archived->state == "archived", "inactive session should become archived");
    Expect(!archived->archive_path.empty(), "archived session should store archive path");
    Expect(std::filesystem::exists(archived->archive_path),
           "archive file should be written");
    Expect(store.LoadInteractionMessages(session.session_id).empty(),
           "archived session should clear live messages");
    Expect(store.LoadInteractionSummaries(session.session_id).empty(),
           "archived session should clear live summaries");
    std::cout << "ok: interaction-conversation-archive-archives-inactive-session" << '\n';
    cleanup();
  } catch (...) {
    cleanup();
    throw;
  }
}

void TestRestoresArchivedSession() {
  const auto root = MakeTempRoot("interaction-restore");
  std::filesystem::create_directories(root);
  const auto cleanup = [&]() { std::filesystem::remove_all(root); };
  try {
    const std::string db_path = (root / "controller.sqlite").string();
    naim::ControllerStore store(db_path);
    store.Initialize();
    InsertPlane(db_path, "plane-b");
    const auto user = store.CreateBootstrapAdmin("restore-user", "hash");

    const naim::controller::InteractionConversationPayloadBuilder payload_builder;
    const std::filesystem::path archive_path =
        root / "interaction-archives" / "plane-b" / "session-2.json.zst";
    const json archive_payload{
        {"session",
         json{
             {"session_id", "session-2"},
             {"plane_name", "plane-b"},
             {"owner_kind", "user"},
             {"owner_user_id", user.id},
             {"auth_session_kind", "user"},
             {"created_at", "2020-01-01 00:00:00"},
             {"updated_at", "2020-01-01 00:00:00"},
             {"context_state", json{{"browsing_mode", "enabled"}}},
             {"latest_prompt_tokens", 3},
             {"estimated_context_tokens", 7},
             {"compression_state", "compressed"},
             {"version", 1},
         }},
        {"messages",
         json::array({json{{"role", "user"}, {"content", "restored-message"}}})},
        {"summaries",
         json::array({json{
             {"turn_range_start", 0},
             {"turn_range_end", 0},
             {"summary", json{{"session_goal", "goal"}}},
             {"created_at", "2020-01-01 00:00:00"},
         }})},
    };
    const std::string compressed =
        CompressStringZstd(payload_builder.JsonString(archive_payload));
    WriteBinaryFile(archive_path, compressed);

    naim::InteractionSessionRecord archived_session;
    archived_session.session_id = "session-2";
    archived_session.plane_name = "plane-b";
    archived_session.owner_kind = "user";
    archived_session.owner_user_id = user.id;
    archived_session.auth_session_kind = "user";
    archived_session.state = "archived";
    archived_session.last_used_at = "2020-01-02 00:00:00";
    archived_session.archived_at = "2020-01-02 00:00:00";
    archived_session.archive_path = archive_path.string();
    archived_session.archive_codec = "zstd";
    archived_session.archive_sha256 = naim::ComputeSha256Hex(compressed);
    archived_session.context_state_json =
        payload_builder.JsonString(json{{"browsing_mode", "enabled"}});
    archived_session.latest_prompt_tokens = 3;
    archived_session.estimated_context_tokens = 7;
    archived_session.compression_state = "compressed";
    archived_session.version = 1;
    archived_session.created_at = "2020-01-01 00:00:00";
    archived_session.updated_at = "2020-01-02 00:00:00";
    store.UpsertInteractionSession(archived_session);

    store.UpsertInteractionArchive(naim::InteractionArchiveRecord{
        "session-2",
        "plane-b",
        "user",
        user.id,
        archive_path.string(),
        "zstd",
        archived_session.archive_sha256,
        "2020-01-02 00:00:00",
        "archived",
        "2020-01-02 00:00:00",
        "2020-01-02 00:00:00",
    });

    const naim::controller::InteractionConversationArchiveService archive_service;
    const auto error =
        archive_service.RestoreArchivedSession(
            store, "plane-b", "session-2", "user", user.id);
    Expect(!error.has_value(), "restore should succeed for valid archive");

    const auto restored = store.LoadInteractionSessionForOwner(
        "plane-b", "session-2", "user", user.id);
    Expect(restored.has_value(), "restore should recreate active session");
    Expect(restored->state == "active", "restored session should be active");
    Expect(store.LoadInteractionMessages("session-2").size() == 1,
           "restore should repopulate messages");
    Expect(store.LoadInteractionSummaries("session-2").size() == 1,
           "restore should repopulate summaries");
    std::cout << "ok: interaction-conversation-archive-restores-session" << '\n';
    cleanup();
  } catch (...) {
    cleanup();
    throw;
  }
}

}  // namespace

int main() {
  try {
    TestArchivesInactiveSessions();
    TestRestoresArchivedSession();
    return 0;
  } catch (const std::exception& error) {
    std::cerr << "interaction_conversation_archive_service_tests failed: "
              << error.what() << '\n';
    return 1;
  }
}
