#include "interaction/interaction_conversation_service.h"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <sstream>
#include <stdexcept>
#include <string_view>
#include <vector>

#include <zstd.h>

#include "app/controller_time_support.h"
#include "comet/security/crypto_utils.h"
#include "skills/plane_skills_service.h"

namespace comet::controller {

namespace {

using nlohmann::json;

constexpr int kArchiveAgeSeconds = 7 * 24 * 60 * 60;
constexpr int kArchiveSweepIntervalSeconds = 60;
constexpr int kRecentMessageTailCount = 8;
constexpr int kZstdCompressionLevel = 3;

std::atomic<long long> g_last_archive_sweep_epoch_ms{0};

std::string TrimCopy(const std::string& value) {
  const auto begin = value.find_first_not_of(" \t\r\n");
  if (begin == std::string::npos) {
    return "";
  }
  const auto end = value.find_last_not_of(" \t\r\n");
  return value.substr(begin, end - begin + 1);
}

json ParseJsonOr(const std::string& text, const json& fallback) {
  if (text.empty()) {
    return fallback;
  }
  const json parsed = json::parse(text, nullptr, false);
  if (parsed.is_discarded()) {
    return fallback;
  }
  return parsed;
}

json ParseJsonObject(const std::string& text) {
  const json parsed = ParseJsonOr(text, json::object());
  return parsed.is_object() ? parsed : json::object();
}

std::string JsonString(const json& value) {
  return value.dump(-1, ' ', false, json::error_handler_t::replace);
}

std::string UtcNowSqlTimestamp() {
  return ControllerTimeSupport::UtcNowSqlTimestamp();
}

std::string SqlTimestampBeforeSeconds(int seconds) {
  return ControllerTimeSupport::SqlTimestampAfterSeconds(-seconds);
}

int EstimateTokensForJson(const json& value) {
  const std::size_t bytes = JsonString(value).size();
  return std::max(1, static_cast<int>((bytes + 3) / 4));
}

std::vector<json> MessageRecordsToJson(
    const std::vector<comet::InteractionMessageRecord>& records) {
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

bool JsonEqual(const json& left, const json& right) {
  return JsonString(left) == JsonString(right);
}

std::size_t CommonPrefixLength(
    const std::vector<json>& stored_messages,
    const json& incoming_messages) {
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

json TailMessagesWithDelta(
    const std::vector<json>& stored_messages,
    const json& delta_messages) {
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

std::string ExcerptText(const json& message, std::size_t max_bytes = 240) {
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

json BuildSummaryJson(
    const std::vector<json>& messages,
    const json& context_state,
    int turn_range_start,
    int turn_range_end) {
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

std::string BuildSummarySystemInstruction(
    const std::vector<comet::InteractionSummaryRecord>& summaries) {
  if (summaries.empty()) {
    return "";
  }
  std::ostringstream instruction;
  instruction << "Conversation memory summary maintained by comet-node.";
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

std::vector<comet::InteractionSummaryRecord> BuildSummaryRecords(
    const std::string& session_id,
    const std::vector<json>& all_messages,
    const json& context_state,
    const std::string& created_at,
    int max_model_len,
    const json& prompt_messages) {
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
      comet::InteractionSummaryRecord{
          0,
          session_id,
          0,
          summary_end,
          JsonString(BuildSummaryJson(summary_messages, context_state, 0, summary_end)),
          created_at,
      },
  };
}

json BuildPromptMessages(
    const std::vector<comet::InteractionSummaryRecord>& summaries,
    const std::vector<json>& stored_messages,
    const json& delta_messages) {
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

std::vector<comet::InteractionMessageRecord> AppendNewMessageRecords(
    std::vector<comet::InteractionMessageRecord> existing,
    const json& delta_messages,
    const std::string& assistant_text,
    const json& usage,
    const std::string& created_at) {
  int next_seq = existing.empty() ? 0 : existing.back().seq + 1;
  if (delta_messages.is_array()) {
    for (const auto& message : delta_messages) {
      comet::InteractionMessageRecord record;
      record.session_id = existing.empty() ? "" : existing.front().session_id;
      record.seq = next_seq++;
      record.role = message.value("role", std::string{});
      record.kind = record.role;
      record.content_json = JsonString(message);
      record.usage_json = "{}";
      record.created_at = created_at;
      existing.push_back(std::move(record));
    }
  }
  comet::InteractionMessageRecord assistant_record;
  assistant_record.session_id = existing.empty() ? "" : existing.front().session_id;
  assistant_record.seq = next_seq;
  assistant_record.role = "assistant";
  assistant_record.kind = "assistant";
  assistant_record.content_json = JsonString(json{
      {"role", "assistant"},
      {"content", assistant_text},
  });
  assistant_record.usage_json = JsonString(usage);
  assistant_record.created_at = created_at;
  existing.push_back(std::move(assistant_record));
  return existing;
}

std::vector<comet::InteractionMessageRecord> AssignSessionId(
    std::vector<comet::InteractionMessageRecord> records,
    const std::string& session_id) {
  for (auto& record : records) {
    record.session_id = session_id;
  }
  return records;
}

std::vector<json> MessagesForArchive(
    const std::vector<comet::InteractionMessageRecord>& records) {
  std::vector<json> messages;
  messages.reserve(records.size());
  for (const auto& record : records) {
    messages.push_back(ParseJsonOr(record.content_json, json::object()));
  }
  return messages;
}

std::filesystem::path ArchiveRootPath(const std::string& db_path) {
  std::filesystem::path db_file(db_path);
  const std::filesystem::path parent =
      db_file.has_parent_path() ? db_file.parent_path() : std::filesystem::current_path();
  return parent / "interaction-archives";
}

std::string CompressStringZstd(const std::string& input) {
  std::string output(
      ZSTD_compressBound(input.size()),
      '\0');
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

std::string DecompressStringZstd(const std::string& input) {
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

std::string ReadBinaryFile(const std::filesystem::path& path) {
  std::ifstream input(path, std::ios::binary);
  if (!input.is_open()) {
    throw std::runtime_error("failed to open archive file");
  }
  std::ostringstream buffer;
  buffer << input.rdbuf();
  return buffer.str();
}

void WriteBinaryFile(const std::filesystem::path& path, const std::string& contents) {
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

json SessionSummaryPayload(
    const comet::InteractionSessionRecord& session,
    std::size_t message_count,
    std::size_t summary_count) {
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

std::optional<InteractionValidationError> RestoreArchivedSession(
    comet::ControllerStore& store,
    const std::string& plane_name,
    const std::string& session_id,
    const std::string& owner_kind,
    const std::optional<int>& owner_user_id) {
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
        comet::ComputeSha256Hex(compressed) != archive->archive_sha256) {
      return InteractionValidationError{
          "session_restore_failed",
          "conversation archive checksum mismatch",
          false,
          json::object(),
      };
    }

    const json payload =
        ParseJsonOr(DecompressStringZstd(compressed), json::object());
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

    comet::InteractionSessionRecord session;
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
        JsonString(session_json.value("context_state", json::object()));
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

    std::vector<comet::InteractionMessageRecord> messages;
    int next_seq = 0;
    for (const auto& message : messages_json) {
      comet::InteractionMessageRecord record;
      record.session_id = session.session_id;
      record.seq = next_seq++;
      record.role = message.value("role", std::string{});
      record.kind = record.role;
      record.content_json = JsonString(message);
      record.usage_json = "{}";
      record.created_at = session.updated_at;
      messages.push_back(std::move(record));
    }
    store.ReplaceInteractionMessages(session.session_id, messages);

    std::vector<comet::InteractionSummaryRecord> summaries;
    for (const auto& summary_json : summaries_json) {
      comet::InteractionSummaryRecord summary;
      summary.session_id = session.session_id;
      summary.turn_range_start =
          summary_json.value("turn_range_start", 0);
      summary.turn_range_end =
          summary_json.value("turn_range_end", 0);
      summary.summary_json =
          JsonString(summary_json.value("summary", json::object()));
      summary.created_at = summary_json.value("created_at", session.updated_at);
      summaries.push_back(std::move(summary));
    }
    store.ReplaceInteractionSummaries(session.session_id, summaries);

    comet::InteractionArchiveRecord restored_archive = *archive;
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

void MaybeArchiveInactiveSessions(const std::string& db_path) {
  const auto now = std::chrono::duration_cast<std::chrono::milliseconds>(
                       std::chrono::system_clock::now().time_since_epoch())
                       .count();
  long long previous = g_last_archive_sweep_epoch_ms.load();
  if (previous != 0 &&
      now - previous < static_cast<long long>(kArchiveSweepIntervalSeconds) * 1000) {
    return;
  }
  if (!g_last_archive_sweep_epoch_ms.compare_exchange_strong(previous, now)) {
    return;
  }

  comet::ControllerStore store(db_path);
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
             {"context_state", ParseJsonObject(session.context_state_json)},
             {"latest_prompt_tokens", session.latest_prompt_tokens},
             {"estimated_context_tokens", session.estimated_context_tokens},
             {"compression_state", session.compression_state},
             {"version", session.version},
         }},
        {"messages", MessagesForArchive(messages)},
        {"summaries", json::array()},
    };
    for (const auto& summary : summaries) {
      archive_payload["summaries"].push_back(json{
          {"turn_range_start", summary.turn_range_start},
          {"turn_range_end", summary.turn_range_end},
          {"summary", ParseJsonObject(summary.summary_json)},
          {"created_at", summary.created_at},
      });
    }

    const std::filesystem::path archive_path =
        ArchiveRootPath(db_path) / session.plane_name /
        (session.session_id + ".json.zst");
    const std::string compressed =
        CompressStringZstd(JsonString(archive_payload));
    const std::string checksum = comet::ComputeSha256Hex(compressed);
    WriteBinaryFile(archive_path, compressed);

    comet::InteractionSessionRecord archived_session = session;
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
    store.UpsertInteractionArchive(comet::InteractionArchiveRecord{
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

}  // namespace

std::optional<InteractionValidationError> InteractionConversationService::PrepareRequest(
    const std::string& db_path,
    const PlaneInteractionResolution& resolution,
    const InteractionConversationPrincipal& principal,
    InteractionRequestContext* context) const {
  if (context == nullptr) {
    throw std::invalid_argument("interaction request context is required");
  }

  MaybeArchiveInactiveSessions(db_path);

  context->owner_kind = principal.owner_kind;
  context->owner_user_id = principal.owner_user_id;
  context->auth_session_kind = principal.auth_session_kind;
  context->delta_messages = context->client_messages;

  const std::string plane_name =
      resolution.status_payload.value("plane_name", std::string{});
  comet::ControllerStore store(db_path);
  store.Initialize();

  std::optional<comet::InteractionSessionRecord> session;
  std::vector<comet::InteractionMessageRecord> stored_records;
  std::vector<comet::InteractionSummaryRecord> summary_records;
  std::vector<json> stored_messages;

  if (context->requested_session_id.has_value()) {
    session = store.LoadInteractionSessionForOwner(
        plane_name,
        *context->requested_session_id,
        principal.owner_kind,
        principal.owner_user_id);
    if (!session.has_value()) {
      const auto owner_any_plane = store.LoadInteractionSessionForOwnerAnyPlane(
          *context->requested_session_id,
          principal.owner_kind,
          principal.owner_user_id);
      if (owner_any_plane.has_value() &&
          owner_any_plane->plane_name != plane_name) {
        return InteractionValidationError{
            "session_plane_mismatch",
            "conversation session belongs to a different plane",
            false,
            json::object(),
        };
      }
      if (const auto restore_error = RestoreArchivedSession(
              store,
              plane_name,
              *context->requested_session_id,
              principal.owner_kind,
              principal.owner_user_id);
          !restore_error.has_value() ||
          restore_error->code != "session_not_found") {
        if (restore_error.has_value()) {
          return restore_error;
        }
        context->session_restored_from_archive = true;
        session = store.LoadInteractionSessionForOwner(
            plane_name,
            *context->requested_session_id,
            principal.owner_kind,
            principal.owner_user_id);
      }
    } else if (session->state == "archived") {
      if (const auto restore_error = RestoreArchivedSession(
              store,
              plane_name,
              session->session_id,
              principal.owner_kind,
              principal.owner_user_id);
          restore_error.has_value()) {
        return restore_error;
      }
      context->session_restored_from_archive = true;
      session = store.LoadInteractionSessionForOwner(
          plane_name,
          session->session_id,
          principal.owner_kind,
          principal.owner_user_id);
    }

    if (!session.has_value()) {
      return InteractionValidationError{
          "session_not_found",
          "conversation session was not found",
          false,
          json::object(),
      };
    }

    stored_records = store.LoadInteractionMessages(session->session_id);
    summary_records = store.LoadInteractionSummaries(session->session_id);
    stored_messages = MessageRecordsToJson(stored_records);
    context->expected_session_version = session->version;
    context->conversation_session_id = session->session_id;
    context->session_context_state = ParseJsonObject(session->context_state_json);

    const std::size_t prefix = CommonPrefixLength(stored_messages, context->client_messages);
    if (prefix > 0 && context->client_messages.is_array()) {
      json delta = json::array();
      for (std::size_t index = prefix; index < context->client_messages.size(); ++index) {
        delta.push_back(context->client_messages[index]);
      }
      context->delta_messages = std::move(delta);
    }
    if (!context->delta_messages.is_array() || context->delta_messages.empty()) {
      return InteractionValidationError{
          "session_delta_invalid",
          "continuation request must include at least one new message",
          false,
          json::object(),
      };
    }
  } else {
    context->conversation_session_id = GenerateInteractionSessionId();
    context->expected_session_version = 0;
    context->session_context_state = json::object();
  }

  context->payload["session_id"] = context->conversation_session_id;
  context->payload[kInteractionSessionContextStatePayloadKey] =
      context->session_context_state;
  context->payload["messages"] =
      BuildPromptMessages(summary_records, stored_messages, context->delta_messages);
  return std::nullopt;
}

std::optional<InteractionValidationError> InteractionConversationService::PersistResponse(
    const std::string& db_path,
    const PlaneInteractionResolution& resolution,
    InteractionRequestContext* context,
    const InteractionSessionResult& result) const {
  if (context == nullptr || context->conversation_session_id.empty() ||
      result.segments.empty()) {
    return std::nullopt;
  }

  const std::string plane_name =
      resolution.status_payload.value("plane_name", std::string{});
  const std::string now = UtcNowSqlTimestamp();

  comet::ControllerStore store(db_path);
  store.Initialize();

  auto session = store.LoadInteractionSessionForOwner(
      plane_name,
      context->conversation_session_id,
      context->owner_kind,
      context->owner_user_id);

  std::vector<comet::InteractionMessageRecord> message_records =
      session.has_value() ? store.LoadInteractionMessages(session->session_id)
                          : std::vector<comet::InteractionMessageRecord>{};

  const json usage{
      {"prompt_tokens", result.total_prompt_tokens},
      {"completion_tokens", result.total_completion_tokens},
      {"total_tokens", result.total_tokens},
  };
  message_records = AssignSessionId(
      AppendNewMessageRecords(
          std::move(message_records),
          context->delta_messages,
          result.content,
          usage,
          now),
      context->conversation_session_id);

  std::vector<json> all_messages = MessageRecordsToJson(message_records);
  json context_state = context->payload.contains(kInteractionSessionContextStatePayloadKey) &&
                               context->payload.at(kInteractionSessionContextStatePayloadKey)
                                   .is_object()
                           ? context->payload.at(kInteractionSessionContextStatePayloadKey)
                           : context->session_context_state;
  json applied_skill_ids = json::array();
  if (context->payload.contains(PlaneSkillsService::kAppliedSkillsPayloadKey) &&
      context->payload.at(PlaneSkillsService::kAppliedSkillsPayloadKey).is_array()) {
    for (const auto& item : context->payload.at(PlaneSkillsService::kAppliedSkillsPayloadKey)) {
      if (item.is_object() && item.contains("id") && item.at("id").is_string()) {
        applied_skill_ids.push_back(item.at("id").get<std::string>());
      }
    }
  }
  context_state["applied_skill_ids"] = applied_skill_ids;

  const auto summary_records = BuildSummaryRecords(
      context->conversation_session_id,
      all_messages,
      context_state,
      now,
      std::max(1, resolution.desired_state.inference.max_model_len),
      context->payload.value("messages", json::array()));

  comet::InteractionSessionRecord updated;
  updated.session_id = context->conversation_session_id;
  updated.plane_name = plane_name;
  updated.owner_kind = context->owner_kind;
  updated.owner_user_id = context->owner_user_id;
  updated.auth_session_kind = context->auth_session_kind;
  updated.state = "active";
  updated.last_used_at = now;
  updated.archive_path = session.has_value() ? session->archive_path : "";
  updated.archive_codec = session.has_value() ? session->archive_codec : "";
  updated.archive_sha256 = session.has_value() ? session->archive_sha256 : "";
  updated.context_state_json = JsonString(context_state);
  updated.latest_prompt_tokens = result.total_prompt_tokens;
  updated.estimated_context_tokens =
      EstimateTokensForJson(context->payload.value("messages", json::array()));
  updated.compression_state = summary_records.empty() ? "none" : "compressed";
  updated.version = session.has_value() ? session->version + 1 : 1;
  updated.created_at = session.has_value() ? session->created_at : now;
  updated.updated_at = now;

  if (session.has_value()) {
    updated.archived_at.clear();
    if (!store.UpdateInteractionSessionVersioned(updated, session->version)) {
      return InteractionValidationError{
          "session_conflict",
          "conversation session was modified concurrently",
          true,
          json::object(),
      };
    }
  } else {
    store.UpsertInteractionSession(updated);
  }

  store.ReplaceInteractionMessages(updated.session_id, message_records);
  store.ReplaceInteractionSummaries(updated.session_id, summary_records);
  MaybeArchiveInactiveSessions(db_path);
  return std::nullopt;
}

nlohmann::json InteractionConversationService::BuildSessionsListPayload(
    const std::string& db_path,
    const std::string& plane_name,
    int user_id) const {
  comet::ControllerStore store(db_path);
  store.Initialize();
  json sessions = json::array();
  for (const auto& session : store.LoadInteractionSessionsForUser(plane_name, user_id)) {
    const auto messages = store.LoadInteractionMessages(session.session_id);
    const auto summaries = store.LoadInteractionSummaries(session.session_id);
    sessions.push_back(
        SessionSummaryPayload(session, messages.size(), summaries.size()));
  }
  return json{
      {"plane_name", plane_name},
      {"sessions", sessions},
  };
}

std::optional<nlohmann::json> InteractionConversationService::BuildSessionDetailPayload(
    const std::string& db_path,
    const std::string& plane_name,
    int user_id,
    const std::string& session_id) const {
  comet::ControllerStore store(db_path);
  store.Initialize();
  const auto session =
      store.LoadInteractionSessionForOwner(plane_name, session_id, "user", user_id);
  if (!session.has_value()) {
    return std::nullopt;
  }

  const auto messages = store.LoadInteractionMessages(session_id);
  const auto summaries = store.LoadInteractionSummaries(session_id);
  json message_payload = json::array();
  for (const auto& message : messages) {
    json payload = ParseJsonOr(message.content_json, json::object());
    payload["seq"] = message.seq;
    payload["created_at"] = message.created_at;
    if (const json usage = ParseJsonObject(message.usage_json); !usage.empty()) {
      payload["usage"] = usage;
    }
    message_payload.push_back(std::move(payload));
  }
  json summary_payload = json::array();
  for (const auto& summary : summaries) {
    summary_payload.push_back(json{
        {"turn_range_start", summary.turn_range_start},
        {"turn_range_end", summary.turn_range_end},
        {"summary", ParseJsonObject(summary.summary_json)},
        {"created_at", summary.created_at},
    });
  }

  return json{
      {"session", SessionSummaryPayload(*session, messages.size(), summaries.size())},
      {"messages", message_payload},
      {"summaries", summary_payload},
  };
}

bool InteractionConversationService::DeleteSession(
    const std::string& db_path,
    const std::string& plane_name,
    int user_id,
    const std::string& session_id) const {
  comet::ControllerStore store(db_path);
  store.Initialize();
  const auto session =
      store.LoadInteractionSessionForOwner(plane_name, session_id, "user", user_id);
  const auto archive =
      store.LoadInteractionArchiveForOwner(plane_name, session_id, "user", user_id);
  if (!store.DeleteInteractionSessionForOwner(plane_name, session_id, "user", user_id)) {
    return false;
  }
  const std::string archive_path =
      archive.has_value() ? archive->archive_path
                          : (session.has_value() ? session->archive_path : "");
  if (!archive_path.empty()) {
    std::error_code error;
    std::filesystem::remove(std::filesystem::path(archive_path), error);
  }
  return true;
}

}  // namespace comet::controller
