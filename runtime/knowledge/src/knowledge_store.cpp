#include "knowledge/knowledge_store.h"

#include <algorithm>
#include <chrono>
#include <cctype>
#include <filesystem>
#include <iomanip>
#include <random>
#include <sstream>
#include <stdexcept>
#include <utility>

#include "naim/security/crypto_utils.h"
#include "naim/state/sqlite_statement.h"

namespace naim::knowledge_runtime {

namespace {

void Exec(sqlite3* db, const std::string& sql) {
  char* error_message = nullptr;
  const int rc = sqlite3_exec(db, sql.c_str(), nullptr, nullptr, &error_message);
  if (rc != SQLITE_OK) {
    const std::string message =
        error_message == nullptr ? "unknown sqlite error" : error_message;
    sqlite3_free(error_message);
    throw std::runtime_error(message);
  }
}

std::string ColumnText(sqlite3_stmt* statement, int column) {
  const auto* value = sqlite3_column_text(statement, column);
  return value == nullptr ? std::string{} : reinterpret_cast<const char*>(value);
}

nlohmann::json ParseJsonOr(const std::string& text, nlohmann::json fallback) {
  if (text.empty()) {
    return fallback;
  }
  const auto parsed = nlohmann::json::parse(text, nullptr, false);
  return parsed.is_discarded() ? fallback : parsed;
}

}  // namespace

KnowledgeStore::KnowledgeStore(std::filesystem::path db_path) : db_path_(std::move(db_path)) {}

KnowledgeStore::~KnowledgeStore() {
  if (db_ != nullptr) {
    sqlite3_close(db_);
  }
}

void KnowledgeStore::Open() {
  std::filesystem::create_directories(db_path_.parent_path());
  if (sqlite3_open(db_path_.string().c_str(), &db_) != SQLITE_OK) {
    const std::string message = db_ == nullptr ? "unknown sqlite open error" : sqlite3_errmsg(db_);
    if (db_ != nullptr) {
      sqlite3_close(db_);
      db_ = nullptr;
    }
    throw std::runtime_error("failed to open knowledge store: " + message);
  }
  sqlite3_busy_timeout(db_, 10000);
  Exec(db_, "PRAGMA journal_mode=WAL;");
  Exec(db_, "PRAGMA foreign_keys=ON;");
  Exec(
      db_,
      "CREATE TABLE IF NOT EXISTS blocks("
      "block_id TEXT PRIMARY KEY,"
      "knowledge_id TEXT NOT NULL,"
      "version_id TEXT NOT NULL,"
      "type TEXT NOT NULL,"
      "title TEXT NOT NULL,"
      "body TEXT NOT NULL,"
      "payload_json TEXT NOT NULL,"
      "content_hash TEXT NOT NULL,"
      "created_at TEXT NOT NULL,"
      "created_by TEXT NOT NULL,"
      "source_ids_json TEXT NOT NULL,"
      "scope_ids_json TEXT NOT NULL,"
      "confidence REAL NOT NULL);");
  Exec(db_, "CREATE INDEX IF NOT EXISTS idx_blocks_knowledge_id ON blocks(knowledge_id);");
  Exec(
      db_,
      "CREATE TABLE IF NOT EXISTS block_search("
      "block_id TEXT PRIMARY KEY,"
      "title TEXT NOT NULL,"
      "body TEXT NOT NULL);");
  Exec(
      db_,
      "CREATE TABLE IF NOT EXISTS heads("
      "knowledge_id TEXT PRIMARY KEY,"
      "head_block_id TEXT NOT NULL,"
      "version_id TEXT NOT NULL,"
      "updated_at TEXT NOT NULL,"
      "event_id TEXT NOT NULL,"
      "content_hash TEXT NOT NULL);");
  Exec(
      db_,
      "CREATE TABLE IF NOT EXISTS relations("
      "relation_id TEXT PRIMARY KEY,"
      "from_block_id TEXT NOT NULL,"
      "to_block_id TEXT NOT NULL,"
      "type TEXT NOT NULL,"
      "confidence REAL NOT NULL,"
      "scope_ids_json TEXT NOT NULL,"
      "created_at TEXT NOT NULL,"
      "created_by TEXT NOT NULL);");
  Exec(db_, "CREATE INDEX IF NOT EXISTS idx_relations_from ON relations(from_block_id);");
  Exec(db_, "CREATE INDEX IF NOT EXISTS idx_relations_to ON relations(to_block_id);");
  Exec(
      db_,
      "CREATE TABLE IF NOT EXISTS events("
      "sequence INTEGER PRIMARY KEY AUTOINCREMENT,"
      "event_id TEXT NOT NULL UNIQUE,"
      "type TEXT NOT NULL,"
      "scope_ids_json TEXT NOT NULL,"
      "knowledge_ids_json TEXT NOT NULL,"
      "block_ids_json TEXT NOT NULL,"
      "payload_json TEXT NOT NULL,"
      "created_at TEXT NOT NULL,"
      "created_by TEXT NOT NULL);");
  Exec(
      db_,
      "CREATE TABLE IF NOT EXISTS capsules("
      "capsule_id TEXT PRIMARY KEY,"
      "plane_id TEXT NOT NULL,"
      "manifest_json TEXT NOT NULL,"
      "created_at TEXT NOT NULL);");
  Exec(
      db_,
      "CREATE TABLE IF NOT EXISTS overlays("
      "overlay_change_id TEXT PRIMARY KEY,"
      "plane_id TEXT NOT NULL,"
      "capsule_id TEXT NOT NULL,"
      "overlay_json TEXT NOT NULL,"
      "created_at TEXT NOT NULL);");
  Exec(
      db_,
      "CREATE TABLE IF NOT EXISTS replica_merge_checkpoints("
      "replica_merge_id TEXT PRIMARY KEY,"
      "plane_id TEXT NOT NULL,"
      "capsule_id TEXT NOT NULL,"
      "checkpoint_json TEXT NOT NULL,"
      "status TEXT NOT NULL,"
      "started_at TEXT NOT NULL,"
      "completed_at TEXT NOT NULL);");
}

nlohmann::json KnowledgeStore::Status(const std::string& service_id) const {
  return nlohmann::json{
      {"service_id", service_id},
      {"status", "ready"},
      {"store_profile", "canonical-shard"},
      {"storage_engine", "sqlite"},
      {"schema_version", "knowledge.v1"},
      {"index_epoch", "idx_" + std::to_string(LatestEventSequence())},
      {"latest_event_sequence", LatestEventSequence()},
  };
}

nlohmann::json KnowledgeStore::WriteBlock(const nlohmann::json& payload) {
  auto block = naim::knowledge::BlockFromJson(payload);
  if (block.block_id.empty()) {
    block.block_id = NewId("blk");
  }
  if (block.knowledge_id.empty()) {
    block.knowledge_id = "knowledge." + block.block_id;
  }
  if (block.version_id.empty()) {
    block.version_id = "v1";
  }
  if (block.created_at.empty()) {
    block.created_at = UtcNow();
  }
  if (block.created_by.empty()) {
    block.created_by = "naim-knowledged";
  }
  if (block.content_hash.empty()) {
    block.content_hash = naim::ComputeSha256Hex(block.title + "\n" + block.body);
  }

  naim::SqliteStatement statement(
      db_,
      "INSERT INTO blocks(block_id, knowledge_id, version_id, type, title, body, payload_json, "
      "content_hash, created_at, created_by, source_ids_json, scope_ids_json, confidence) "
      "VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13);");
  statement.BindText(1, block.block_id);
  statement.BindText(2, block.knowledge_id);
  statement.BindText(3, block.version_id);
  statement.BindText(4, block.type);
  statement.BindText(5, block.title);
  statement.BindText(6, block.body);
  statement.BindText(7, JsonText(block.payload));
  statement.BindText(8, block.content_hash);
  statement.BindText(9, block.created_at);
  statement.BindText(10, block.created_by);
  statement.BindText(11, JsonText(block.source_ids));
  statement.BindText(12, JsonText(block.scope_ids));
  statement.BindDouble(13, block.confidence);
  statement.StepDone();

  naim::SqliteStatement search(
      db_,
      "INSERT INTO block_search(block_id, title, body) VALUES(?1, ?2, ?3);");
  search.BindText(1, block.block_id);
  search.BindText(2, block.title);
  search.BindText(3, block.body);
  search.StepDone();

  const int sequence = AppendEvent(
      "block.created",
      {block.knowledge_id},
      {block.block_id},
      nlohmann::json{{"block_id", block.block_id}, {"knowledge_id", block.knowledge_id}});
  return nlohmann::json{{"block", naim::knowledge::ToJson(block)}, {"event_sequence", sequence}};
}

nlohmann::json KnowledgeStore::ReadBlock(const std::string& block_id) const {
  naim::SqliteStatement statement(
      db_,
      "SELECT block_id, knowledge_id, version_id, type, title, body, payload_json, content_hash, "
      "created_at, created_by, source_ids_json, scope_ids_json, confidence "
      "FROM blocks WHERE block_id = ?1;");
  statement.BindText(1, block_id);
  if (!statement.StepRow()) {
    return nlohmann::json{{"error", "not_found"}, {"message", "block not found"}};
  }
  naim::knowledge::KnowledgeBlock block;
  block.block_id = ColumnText(statement.raw(), 0);
  block.knowledge_id = ColumnText(statement.raw(), 1);
  block.version_id = ColumnText(statement.raw(), 2);
  block.type = ColumnText(statement.raw(), 3);
  block.title = ColumnText(statement.raw(), 4);
  block.body = ColumnText(statement.raw(), 5);
  block.payload = ParseJsonOr(ColumnText(statement.raw(), 6), nlohmann::json::object());
  block.content_hash = ColumnText(statement.raw(), 7);
  block.created_at = ColumnText(statement.raw(), 8);
  block.created_by = ColumnText(statement.raw(), 9);
  block.source_ids = ParseJsonOr(ColumnText(statement.raw(), 10), nlohmann::json::array())
                         .get<std::vector<std::string>>();
  block.scope_ids = ParseJsonOr(ColumnText(statement.raw(), 11), nlohmann::json::array())
                        .get<std::vector<std::string>>();
  block.confidence = sqlite3_column_double(statement.raw(), 12);
  return nlohmann::json{{"block", naim::knowledge::ToJson(block)}};
}

nlohmann::json KnowledgeStore::ResolveHead(const std::string& knowledge_id) const {
  naim::SqliteStatement statement(
      db_,
      "SELECT knowledge_id, head_block_id, version_id, updated_at, event_id, content_hash "
      "FROM heads WHERE knowledge_id = ?1;");
  statement.BindText(1, knowledge_id);
  if (!statement.StepRow()) {
    return nlohmann::json{{"head", nullptr}};
  }
  naim::knowledge::KnowledgeHead head;
  head.knowledge_id = ColumnText(statement.raw(), 0);
  head.head_block_id = ColumnText(statement.raw(), 1);
  head.version_id = ColumnText(statement.raw(), 2);
  head.updated_at = ColumnText(statement.raw(), 3);
  head.event_id = ColumnText(statement.raw(), 4);
  head.content_hash = ColumnText(statement.raw(), 5);
  return nlohmann::json{{"head", naim::knowledge::ToJson(head)}};
}

nlohmann::json KnowledgeStore::UpdateHead(
    const std::string& knowledge_id,
    const nlohmann::json& payload) {
  const std::string head_block_id = payload.value("head_block_id", std::string{});
  if (head_block_id.empty()) {
    throw std::runtime_error("head_block_id is required");
  }
  const auto block_payload = ReadBlock(head_block_id);
  if (block_payload.contains("error")) {
    throw std::runtime_error("head block does not exist");
  }
  const auto block = block_payload.at("block");
  naim::knowledge::KnowledgeHead head;
  head.knowledge_id = knowledge_id;
  head.head_block_id = head_block_id;
  head.version_id = payload.value("version_id", block.value("version_id", std::string("v1")));
  head.updated_at = UtcNow();
  head.event_id = NewId("evt");
  head.content_hash = block.value("content_hash", std::string{});

  naim::SqliteStatement statement(
      db_,
      "INSERT INTO heads(knowledge_id, head_block_id, version_id, updated_at, event_id, "
      "content_hash) VALUES(?1, ?2, ?3, ?4, ?5, ?6) "
      "ON CONFLICT(knowledge_id) DO UPDATE SET head_block_id=excluded.head_block_id, "
      "version_id=excluded.version_id, updated_at=excluded.updated_at, event_id=excluded.event_id, "
      "content_hash=excluded.content_hash;");
  statement.BindText(1, head.knowledge_id);
  statement.BindText(2, head.head_block_id);
  statement.BindText(3, head.version_id);
  statement.BindText(4, head.updated_at);
  statement.BindText(5, head.event_id);
  statement.BindText(6, head.content_hash);
  statement.StepDone();
  const int sequence = AppendEvent(
      "head.updated",
      {knowledge_id},
      {head_block_id},
      nlohmann::json{{"knowledge_id", knowledge_id}, {"head_block_id", head_block_id}});
  return nlohmann::json{{"head", naim::knowledge::ToJson(head)}, {"event_sequence", sequence}};
}

nlohmann::json KnowledgeStore::WriteRelation(const nlohmann::json& payload) {
  auto relation = naim::knowledge::RelationFromJson(payload);
  if (relation.relation_id.empty()) {
    relation.relation_id = NewId("rel");
  }
  if (relation.from_block_id.empty() || relation.to_block_id.empty()) {
    throw std::runtime_error("from_block_id and to_block_id are required");
  }
  if (relation.created_at.empty()) {
    relation.created_at = UtcNow();
  }
  if (relation.created_by.empty()) {
    relation.created_by = "naim-knowledged";
  }
  naim::SqliteStatement statement(
      db_,
      "INSERT INTO relations(relation_id, from_block_id, to_block_id, type, confidence, "
      "scope_ids_json, created_at, created_by) VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8);");
  statement.BindText(1, relation.relation_id);
  statement.BindText(2, relation.from_block_id);
  statement.BindText(3, relation.to_block_id);
  statement.BindText(4, relation.type);
  statement.BindDouble(5, relation.confidence);
  statement.BindText(6, JsonText(relation.scope_ids));
  statement.BindText(7, relation.created_at);
  statement.BindText(8, relation.created_by);
  statement.StepDone();
  const int sequence = AppendEvent(
      "relation.created",
      {},
      {relation.from_block_id, relation.to_block_id},
      nlohmann::json{{"relation_id", relation.relation_id}});
  return nlohmann::json{
      {"relation", naim::knowledge::ToJson(relation)},
      {"event_sequence", sequence},
  };
}

nlohmann::json KnowledgeStore::Neighbors(const std::string& block_id) const {
  naim::SqliteStatement statement(
      db_,
      "SELECT relation_id, from_block_id, to_block_id, type, confidence, scope_ids_json, "
      "created_at, created_by FROM relations WHERE from_block_id = ?1 OR to_block_id = ?1 "
      "ORDER BY created_at DESC;");
  statement.BindText(1, block_id);
  nlohmann::json items = nlohmann::json::array();
  while (statement.StepRow()) {
    naim::knowledge::KnowledgeRelation relation;
    relation.relation_id = ColumnText(statement.raw(), 0);
    relation.from_block_id = ColumnText(statement.raw(), 1);
    relation.to_block_id = ColumnText(statement.raw(), 2);
    relation.type = ColumnText(statement.raw(), 3);
    relation.confidence = sqlite3_column_double(statement.raw(), 4);
    relation.scope_ids = ParseJsonOr(ColumnText(statement.raw(), 5), nlohmann::json::array())
                             .get<std::vector<std::string>>();
    relation.created_at = ColumnText(statement.raw(), 6);
    relation.created_by = ColumnText(statement.raw(), 7);
    items.push_back(naim::knowledge::ToJson(relation));
  }
  return nlohmann::json{{"neighbors", items}};
}

nlohmann::json KnowledgeStore::Search(const nlohmann::json& payload) const {
  const std::string query = payload.value("query", std::string{});
  if (query.empty()) {
    return nlohmann::json{{"results", nlohmann::json::array()}};
  }
  naim::SqliteStatement statement(
      db_,
      "SELECT b.block_id, b.knowledge_id, b.version_id, b.title, substr(b.body, 1, 240) "
      "FROM block_search s JOIN blocks b ON b.block_id = s.block_id "
      "WHERE lower(s.title) LIKE ?1 OR lower(s.body) LIKE ?1 "
      "ORDER BY b.created_at DESC LIMIT 20;");
  std::string lowered_query = query;
  std::transform(
      lowered_query.begin(),
      lowered_query.end(),
      lowered_query.begin(),
      [](unsigned char ch) { return static_cast<char>(std::tolower(ch)); });
  statement.BindText(1, "%" + lowered_query + "%");
  nlohmann::json results = nlohmann::json::array();
  while (statement.StepRow()) {
    results.push_back(nlohmann::json{
        {"block_id", ColumnText(statement.raw(), 0)},
        {"knowledge_id", ColumnText(statement.raw(), 1)},
        {"version_id", ColumnText(statement.raw(), 2)},
        {"title", ColumnText(statement.raw(), 3)},
        {"summary", ColumnText(statement.raw(), 4)},
        {"score", 1.0},
        {"shard_id", "kv_default"},
        {"redaction", nullptr},
    });
  }
  return nlohmann::json{{"results", results}};
}

nlohmann::json KnowledgeStore::BuildCapsule(const nlohmann::json& payload) {
  naim::knowledge::CapsuleManifest manifest;
  manifest.capsule_id = payload.value("capsule_id", NewId("cap"));
  manifest.plane_id = payload.value("plane_id", std::string{});
  if (manifest.plane_id.empty()) {
    throw std::runtime_error("plane_id is required");
  }
  manifest.base_event_seq = LatestEventSequence();
  manifest.created_at = UtcNow();
  manifest.policy = payload.value("policy", nlohmann::json::object());
  manifest.indexes = nlohmann::json{{"text", "idx_" + std::to_string(manifest.base_event_seq)}};
  manifest.included = payload.value("included", nlohmann::json::array());
  naim::SqliteStatement statement(
      db_,
      "INSERT INTO capsules(capsule_id, plane_id, manifest_json, created_at) "
      "VALUES(?1, ?2, ?3, ?4);");
  statement.BindText(1, manifest.capsule_id);
  statement.BindText(2, manifest.plane_id);
  statement.BindText(3, JsonText(naim::knowledge::ToJson(manifest)));
  statement.BindText(4, manifest.created_at);
  statement.StepDone();
  AppendEvent("capsule.published", {}, {}, naim::knowledge::ToJson(manifest));
  return nlohmann::json{{"capsule_id", manifest.capsule_id}, {"manifest", naim::knowledge::ToJson(manifest)}};
}

nlohmann::json KnowledgeStore::WriteOverlay(const nlohmann::json& payload) {
  auto proposal = naim::knowledge::OverlayFromJson(payload);
  if (proposal.overlay_change_id.empty()) {
    proposal.overlay_change_id = NewId("ov");
  }
  if (proposal.plane_id.empty() || proposal.capsule_id.empty()) {
    throw std::runtime_error("plane_id and capsule_id are required");
  }
  if (proposal.created_at.empty()) {
    proposal.created_at = UtcNow();
  }
  if (proposal.created_by.empty()) {
    proposal.created_by = "plane-runtime";
  }
  naim::SqliteStatement statement(
      db_,
      "INSERT INTO overlays(overlay_change_id, plane_id, capsule_id, overlay_json, created_at) "
      "VALUES(?1, ?2, ?3, ?4, ?5);");
  statement.BindText(1, proposal.overlay_change_id);
  statement.BindText(2, proposal.plane_id);
  statement.BindText(3, proposal.capsule_id);
  statement.BindText(4, JsonText(naim::knowledge::ToJson(proposal)));
  statement.BindText(5, proposal.created_at);
  statement.StepDone();
  const int sequence = AppendEvent(
      "overlay.proposed",
      {},
      {},
      nlohmann::json{{"overlay_change_id", proposal.overlay_change_id}});
  return nlohmann::json{
      {"overlay_change_id", proposal.overlay_change_id},
      {"status", "stored"},
      {"event_sequence", sequence},
  };
}

nlohmann::json KnowledgeStore::TriggerReplicaMerge(const nlohmann::json& payload) {
  naim::knowledge::ReplicaMergeCheckpoint checkpoint;
  checkpoint.replica_merge_id = NewId("rm");
  checkpoint.plane_id = payload.value("plane_id", std::string{});
  checkpoint.capsule_id = payload.value("capsule_id", std::string{});
  if (checkpoint.plane_id.empty() || checkpoint.capsule_id.empty()) {
    throw std::runtime_error("plane_id and capsule_id are required");
  }
  checkpoint.base_event_seq = payload.value("base_event_seq", 0);
  checkpoint.canonical_event_seq_before = LatestEventSequence();
  checkpoint.started_at = UtcNow();
  checkpoint.completed_at = checkpoint.started_at;
  checkpoint.status = "completed";
  checkpoint.canonical_event_seq_after = AppendEvent(
      "replica.merge.completed",
      {},
      {},
      nlohmann::json{
          {"replica_merge_id", checkpoint.replica_merge_id},
          {"plane_id", checkpoint.plane_id},
          {"capsule_id", checkpoint.capsule_id},
          {"mode", "v0-checkpoint-only"},
      });
  naim::SqliteStatement statement(
      db_,
      "INSERT INTO replica_merge_checkpoints(replica_merge_id, plane_id, capsule_id, "
      "checkpoint_json, status, started_at, completed_at) VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7);");
  statement.BindText(1, checkpoint.replica_merge_id);
  statement.BindText(2, checkpoint.plane_id);
  statement.BindText(3, checkpoint.capsule_id);
  statement.BindText(4, JsonText(naim::knowledge::ToJson(checkpoint)));
  statement.BindText(5, checkpoint.status);
  statement.BindText(6, checkpoint.started_at);
  statement.BindText(7, checkpoint.completed_at);
  statement.StepDone();
  return nlohmann::json{
      {"replica_merge_id", checkpoint.replica_merge_id},
      {"status", checkpoint.status},
      {"checkpoint", naim::knowledge::ToJson(checkpoint)},
  };
}

nlohmann::json KnowledgeStore::ReplicaMergeStatus(const std::string& plane_id) const {
  naim::SqliteStatement statement(
      db_,
      "SELECT checkpoint_json FROM replica_merge_checkpoints WHERE plane_id = ?1 "
      "ORDER BY started_at DESC LIMIT 1;");
  statement.BindText(1, plane_id);
  if (!statement.StepRow()) {
    return nlohmann::json{{"last_successful_checkpoint", nullptr}, {"status", "skipped"}};
  }
  return nlohmann::json{
      {"last_successful_checkpoint", ParseJsonOr(ColumnText(statement.raw(), 0), nullptr)},
      {"status", "completed"},
  };
}

std::string KnowledgeStore::UtcNow() const {
  const auto now = std::chrono::system_clock::now();
  const std::time_t time = std::chrono::system_clock::to_time_t(now);
  std::tm tm{};
#if defined(_WIN32)
  gmtime_s(&tm, &time);
#else
  gmtime_r(&time, &tm);
#endif
  std::ostringstream stream;
  stream << std::put_time(&tm, "%Y-%m-%dT%H:%M:%SZ");
  return stream.str();
}

std::string KnowledgeStore::NewId(const std::string& prefix) const {
  static std::mt19937_64 rng(std::random_device{}());
  const auto now = std::chrono::system_clock::now().time_since_epoch().count();
  std::ostringstream stream;
  stream << prefix << "_" << std::hex << now << rng();
  return stream.str();
}

int KnowledgeStore::LatestEventSequence() const {
  naim::SqliteStatement statement(db_, "SELECT COALESCE(MAX(sequence), 0) FROM events;");
  if (!statement.StepRow()) {
    return 0;
  }
  return sqlite3_column_int(statement.raw(), 0);
}

int KnowledgeStore::AppendEvent(
    const std::string& type,
    const std::vector<std::string>& knowledge_ids,
    const std::vector<std::string>& block_ids,
    const nlohmann::json& payload) {
  const std::string event_id = NewId("evt");
  naim::SqliteStatement statement(
      db_,
      "INSERT INTO events(event_id, type, scope_ids_json, knowledge_ids_json, block_ids_json, "
      "payload_json, created_at, created_by) VALUES(?1, ?2, '[]', ?3, ?4, ?5, ?6, ?7);");
  statement.BindText(1, event_id);
  statement.BindText(2, type);
  statement.BindText(3, JsonText(knowledge_ids));
  statement.BindText(4, JsonText(block_ids));
  statement.BindText(5, JsonText(payload));
  statement.BindText(6, UtcNow());
  statement.BindText(7, "naim-knowledged");
  statement.StepDone();
  return static_cast<int>(sqlite3_last_insert_rowid(db_));
}

std::string KnowledgeStore::JsonText(const nlohmann::json& value) {
  return value.dump();
}

}  // namespace naim::knowledge_runtime
