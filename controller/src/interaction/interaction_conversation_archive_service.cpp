#include "interaction/interaction_conversation_archive_service.h"

#include <chrono>
#include <fstream>
#include <sstream>
#include <stdexcept>

#include <zstd.h>

#include "app/controller_time_support.h"
#include "naim/security/crypto_utils.h"
#include "naim/state/sqlite_store.h"
#include "interaction/interaction_conversation_payload_builder.h"
#include "interaction/interaction_conversation_record_builder.h"

namespace naim::controller {

namespace {

using nlohmann::json;

constexpr int kArchiveAgeSeconds = 7 * 24 * 60 * 60;
constexpr int kArchiveSweepIntervalSeconds = 60;
constexpr int kZstdCompressionLevel = 3;

}  // namespace

std::atomic<long long>
    InteractionConversationArchiveService::last_archive_sweep_epoch_ms_{0};

std::optional<InteractionValidationError>
InteractionConversationArchiveService::RestoreArchivedSession(
    naim::ControllerStore& store,
    const std::string& plane_name,
    const std::string& session_id,
    const std::string& owner_kind,
    const std::optional<int>& owner_user_id) const {
  const InteractionConversationPayloadBuilder payload_builder;
  const InteractionConversationRecordBuilder record_builder;
  const auto archive =
      store.LoadInteractionArchiveForOwner(plane_name, session_id, owner_kind, owner_user_id);
  if (!archive.has_value()) {
    return InteractionValidationError{
        "session_not_found",
        "conversation session was not found",
        false,
        json::object(),
    };
  }

  try {
    const std::filesystem::path archive_path(archive->archive_path);
    const std::string compressed = ReadBinaryFile(archive_path);
    if (!archive->archive_sha256.empty() &&
        naim::ComputeSha256Hex(compressed) != archive->archive_sha256) {
      return InteractionValidationError{
          "session_restore_failed",
          "conversation archive checksum mismatch",
          false,
          json::object(),
      };
    }

    const json payload = payload_builder.ParseJsonOr(
        DecompressStringZstd(compressed),
        json::object());
    const json session_json = payload.value("session", json::object());
    const json messages_json = payload.value("messages", json::array());
    const json summaries_json = payload.value("summaries", json::array());
    if (!session_json.is_object() || !messages_json.is_array() ||
        !summaries_json.is_array()) {
      return InteractionValidationError{
          "session_restore_failed",
          "conversation archive payload is malformed",
          false,
          json::object(),
      };
    }

    naim::InteractionSessionRecord session;
    session.session_id = session_json.value("session_id", session_id);
    session.plane_name = session_json.value("plane_name", plane_name);
    session.owner_kind = session_json.value("owner_kind", owner_kind);
    if (session_json.contains("owner_user_id") &&
        !session_json.at("owner_user_id").is_null()) {
      session.owner_user_id = session_json.at("owner_user_id").get<int>();
    }
    session.auth_session_kind =
        session_json.value("auth_session_kind", std::string{});
    session.state = "active";
    session.last_used_at = UtcNowSqlTimestamp();
    session.archived_at.clear();
    session.archive_path = archive->archive_path;
    session.archive_codec = archive->archive_codec;
    session.archive_sha256 = archive->archive_sha256;
    session.context_state_json =
        payload_builder.JsonString(session_json.value("context_state", json::object()));
    session.latest_prompt_tokens =
        session_json.value("latest_prompt_tokens", 0);
    session.estimated_context_tokens =
        session_json.value("estimated_context_tokens", 0);
    session.compression_state =
        session_json.value("compression_state", std::string{"none"});
    session.version = session_json.value("version", 1) + 1;
    session.created_at = session_json.value("created_at", session.last_used_at);
    session.updated_at = session.last_used_at;
    store.UpsertInteractionSession(session);

    store.ReplaceInteractionMessages(
        session.session_id,
        record_builder.BuildRestoredMessageRecords(
            session.session_id, messages_json, session.updated_at));
    store.ReplaceInteractionSummaries(
        session.session_id,
        record_builder.BuildRestoredSummaryRecords(
            session.session_id, summaries_json, session.updated_at));

    naim::InteractionArchiveRecord restored_archive = *archive;
    restored_archive.restore_state = "restored";
    restored_archive.updated_at = session.updated_at;
    store.UpsertInteractionArchive(restored_archive);
    return std::nullopt;
  } catch (const std::exception& error) {
    return InteractionValidationError{
        "session_restore_failed",
        error.what(),
        false,
        json::object(),
    };
  }
}

void InteractionConversationArchiveService::MaybeArchiveInactiveSessions(
    const std::string& db_path) const {
  const InteractionConversationPayloadBuilder payload_builder;
  const InteractionConversationRecordBuilder record_builder;
  const auto now = std::chrono::duration_cast<std::chrono::milliseconds>(
                       std::chrono::system_clock::now().time_since_epoch())
                       .count();
  long long previous = last_archive_sweep_epoch_ms_.load();
  if (previous != 0 &&
      now - previous < static_cast<long long>(kArchiveSweepIntervalSeconds) * 1000) {
    return;
  }
  if (!last_archive_sweep_epoch_ms_.compare_exchange_strong(previous, now)) {
    return;
  }

  naim::ControllerStore store(db_path);
  store.Initialize();
  const std::string cutoff = SqlTimestampBeforeSeconds(kArchiveAgeSeconds);
  const auto sessions = store.LoadArchiveEligibleInteractionSessions(cutoff, 8);
  for (const auto& session : sessions) {
    const auto messages = store.LoadInteractionMessages(session.session_id);
    const auto summaries = store.LoadInteractionSummaries(session.session_id);

    json archive_payload{
        {"session",
         json{
             {"session_id", session.session_id},
             {"plane_name", session.plane_name},
             {"owner_kind", session.owner_kind},
             {"owner_user_id",
              session.owner_user_id.has_value() ? json(*session.owner_user_id)
                                                : json(nullptr)},
             {"auth_session_kind", session.auth_session_kind},
             {"created_at", session.created_at},
             {"updated_at", session.updated_at},
             {"context_state", payload_builder.ParseJsonObject(session.context_state_json)},
             {"latest_prompt_tokens", session.latest_prompt_tokens},
             {"estimated_context_tokens", session.estimated_context_tokens},
             {"compression_state", session.compression_state},
             {"version", session.version},
         }},
        {"messages", record_builder.MessagesForArchive(messages)},
        {"summaries", json::array()},
    };
    for (const auto& summary : summaries) {
      archive_payload["summaries"].push_back(json{
          {"turn_range_start", summary.turn_range_start},
          {"turn_range_end", summary.turn_range_end},
          {"summary", payload_builder.ParseJsonObject(summary.summary_json)},
          {"created_at", summary.created_at},
      });
    }

    const std::filesystem::path archive_path =
        ArchiveRootPath(db_path) / session.plane_name /
        (session.session_id + ".json.zst");
    const std::string compressed =
        CompressStringZstd(payload_builder.JsonString(archive_payload));
    const std::string checksum = naim::ComputeSha256Hex(compressed);
    WriteBinaryFile(archive_path, compressed);

    naim::InteractionSessionRecord archived_session = session;
    archived_session.state = "archived";
    archived_session.archived_at = UtcNowSqlTimestamp();
    archived_session.archive_path = archive_path.string();
    archived_session.archive_codec = "zstd";
    archived_session.archive_sha256 = checksum;
    archived_session.last_used_at = archived_session.archived_at;
    archived_session.updated_at = archived_session.archived_at;
    archived_session.version = session.version + 1;
    if (!store.UpdateInteractionSessionVersioned(archived_session, session.version)) {
      continue;
    }
    store.ReplaceInteractionMessages(session.session_id, {});
    store.ReplaceInteractionSummaries(session.session_id, {});
    store.UpsertInteractionArchive(naim::InteractionArchiveRecord{
        session.session_id,
        session.plane_name,
        session.owner_kind,
        session.owner_user_id,
        archive_path.string(),
        "zstd",
        checksum,
        archived_session.archived_at,
        "archived",
        archived_session.archived_at,
        archived_session.updated_at,
    });
  }
}

std::string InteractionConversationArchiveService::UtcNowSqlTimestamp() const {
  return ControllerTimeSupport::UtcNowSqlTimestamp();
}

std::string InteractionConversationArchiveService::SqlTimestampBeforeSeconds(
    int seconds) const {
  return ControllerTimeSupport::SqlTimestampAfterSeconds(-seconds);
}

std::filesystem::path InteractionConversationArchiveService::ArchiveRootPath(
    const std::string& db_path) const {
  std::filesystem::path db_file(db_path);
  const std::filesystem::path parent =
      db_file.has_parent_path() ? db_file.parent_path() : std::filesystem::current_path();
  return parent / "interaction-archives";
}

std::string InteractionConversationArchiveService::CompressStringZstd(
    const std::string& input) const {
  std::string output(ZSTD_compressBound(input.size()), '\0');
  const std::size_t compressed_size = ZSTD_compress(
      output.data(),
      output.size(),
      input.data(),
      input.size(),
      kZstdCompressionLevel);
  if (ZSTD_isError(compressed_size) != 0) {
    throw std::runtime_error(
        "failed to compress interaction archive: " +
        std::string(ZSTD_getErrorName(compressed_size)));
  }
  output.resize(compressed_size);
  return output;
}

std::string InteractionConversationArchiveService::DecompressStringZstd(
    const std::string& input) const {
  const unsigned long long expected_size =
      ZSTD_getFrameContentSize(input.data(), input.size());
  if (expected_size == ZSTD_CONTENTSIZE_ERROR ||
      expected_size == ZSTD_CONTENTSIZE_UNKNOWN) {
    throw std::runtime_error("failed to determine interaction archive size");
  }
  std::string output(static_cast<std::size_t>(expected_size), '\0');
  const std::size_t actual_size = ZSTD_decompress(
      output.data(), output.size(), input.data(), input.size());
  if (ZSTD_isError(actual_size) != 0) {
    throw std::runtime_error(
        "failed to decompress interaction archive: " +
        std::string(ZSTD_getErrorName(actual_size)));
  }
  output.resize(actual_size);
  return output;
}

std::string InteractionConversationArchiveService::ReadBinaryFile(
    const std::filesystem::path& path) const {
  std::ifstream input(path, std::ios::binary);
  if (!input.is_open()) {
    throw std::runtime_error("failed to open archive file");
  }
  std::ostringstream buffer;
  buffer << input.rdbuf();
  return buffer.str();
}

void InteractionConversationArchiveService::WriteBinaryFile(
    const std::filesystem::path& path,
    const std::string& contents) const {
  std::filesystem::create_directories(path.parent_path());
  std::ofstream output(path, std::ios::binary);
  if (!output.is_open()) {
    throw std::runtime_error("failed to create archive file");
  }
  output.write(contents.data(), static_cast<std::streamsize>(contents.size()));
  if (!output.good()) {
    throw std::runtime_error("failed to write archive file");
  }
}

}  // namespace naim::controller
