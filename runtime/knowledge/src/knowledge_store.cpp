#include "knowledge/knowledge_store.h"

#include <algorithm>
#include <chrono>
#include <cctype>
#include <filesystem>
#include <iomanip>
#include <map>
#include <random>
#include <set>
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

void TryExec(sqlite3* db, const std::string& sql) {
  char* error_message = nullptr;
  const int rc = sqlite3_exec(db, sql.c_str(), nullptr, nullptr, &error_message);
  if (rc != SQLITE_OK) {
    sqlite3_free(error_message);
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

bool JsonArrayContainsString(const nlohmann::json& values, const std::string& expected) {
  if (!values.is_array()) {
    return false;
  }
  for (const auto& value : values) {
    if (value.is_string() && value.get<std::string>() == expected) {
      return true;
    }
  }
  return false;
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
  Exec(db_, "CREATE INDEX IF NOT EXISTS idx_events_type ON events(type);");
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
      "overlay_event_seq INTEGER NOT NULL DEFAULT 0,"
      "overlay_json TEXT NOT NULL,"
      "created_at TEXT NOT NULL);");
  TryExec(db_, "ALTER TABLE overlays ADD COLUMN overlay_event_seq INTEGER NOT NULL DEFAULT 0;");
  Exec(db_, "CREATE INDEX IF NOT EXISTS idx_overlays_plane_capsule_seq ON overlays(plane_id, capsule_id, overlay_event_seq);");
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
  Exec(
      db_,
      "CREATE TABLE IF NOT EXISTS replica_merge_schedules("
      "plane_id TEXT NOT NULL,"
      "capsule_id TEXT NOT NULL,"
      "cadence TEXT NOT NULL DEFAULT 'daily',"
      "next_run_at TEXT NOT NULL,"
      "last_checkpoint_json TEXT NOT NULL DEFAULT '{}',"
      "status TEXT NOT NULL DEFAULT 'scheduled',"
      "updated_at TEXT NOT NULL,"
      "PRIMARY KEY(plane_id, capsule_id));");
  Exec(
      db_,
      "CREATE TABLE IF NOT EXISTS sources("
      "source_key TEXT PRIMARY KEY,"
      "source_kind TEXT NOT NULL,"
      "source_ref TEXT NOT NULL,"
      "content_hash TEXT NOT NULL,"
      "block_id TEXT NOT NULL,"
      "event_id TEXT NOT NULL,"
      "status TEXT NOT NULL,"
      "metadata_json TEXT NOT NULL,"
      "created_at TEXT NOT NULL);");
  Exec(db_, "CREATE INDEX IF NOT EXISTS idx_sources_hash ON sources(content_hash);");
  Exec(
      db_,
      "CREATE TABLE IF NOT EXISTS review_queue("
      "review_id TEXT PRIMARY KEY,"
      "overlay_change_id TEXT NOT NULL,"
      "knowledge_id TEXT NOT NULL,"
      "type TEXT NOT NULL,"
      "status TEXT NOT NULL,"
      "item_json TEXT NOT NULL,"
      "created_at TEXT NOT NULL,"
      "updated_at TEXT NOT NULL);");
  Exec(db_, "CREATE INDEX IF NOT EXISTS idx_review_status ON review_queue(status);");
  Exec(
      db_,
      "CREATE TABLE IF NOT EXISTS repair_reports("
      "report_id TEXT PRIMARY KEY,"
      "status TEXT NOT NULL,"
      "findings_json TEXT NOT NULL,"
      "created_at TEXT NOT NULL);");
  Exec(
      db_,
      "CREATE TABLE IF NOT EXISTS catalog_objects("
      "object_id TEXT PRIMARY KEY,"
      "shard_id TEXT NOT NULL,"
      "scope_ids_json TEXT NOT NULL,"
      "hints_json TEXT NOT NULL,"
      "updated_at TEXT NOT NULL);");
  Exec(
      db_,
      "CREATE TABLE IF NOT EXISTS catalog_terms("
      "term TEXT NOT NULL,"
      "object_id TEXT NOT NULL,"
      "shard_id TEXT NOT NULL,"
      "scope_ids_json TEXT NOT NULL,"
      "updated_at TEXT NOT NULL,"
      "PRIMARY KEY(term, object_id));");
  Exec(
      db_,
      "CREATE TABLE IF NOT EXISTS consumer_checkpoints("
      "consumer_id TEXT PRIMARY KEY,"
      "event_sequence INTEGER NOT NULL,"
      "updated_at TEXT NOT NULL);");
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
  std::vector<std::string> requested_scopes;
  if (payload.contains("scope_ids") && payload.at("scope_ids").is_array()) {
    requested_scopes = JsonStringArray(payload.at("scope_ids"));
  } else if (const std::string scope_id = payload.value("scope_id", std::string{});
             !scope_id.empty()) {
    requested_scopes.push_back(scope_id);
  }
  const int limit = std::max(1, std::min(100, payload.value("limit", 20)));
  naim::SqliteStatement statement(
      db_,
      "SELECT b.block_id, b.knowledge_id, b.version_id, b.title, substr(b.body, 1, 240), "
      "b.scope_ids_json, b.content_hash, b.confidence "
      "FROM block_search s JOIN blocks b ON b.block_id = s.block_id "
      "WHERE lower(s.title) LIKE ?1 OR lower(s.body) LIKE ?1 "
      "ORDER BY b.created_at DESC LIMIT ?2;");
  std::string lowered_query = query;
  std::transform(
      lowered_query.begin(),
      lowered_query.end(),
      lowered_query.begin(),
      [](unsigned char ch) { return static_cast<char>(std::tolower(ch)); });
  statement.BindText(1, "%" + lowered_query + "%");
  statement.BindInt(2, limit);
  nlohmann::json results = nlohmann::json::array();
  while (statement.StepRow()) {
    const auto scope_ids = ParseJsonOr(ColumnText(statement.raw(), 5), nlohmann::json::array());
    if (!ScopeAllowed(scope_ids, requested_scopes)) {
      results.push_back(nlohmann::json{
          {"block_id", ColumnText(statement.raw(), 0)},
          {"knowledge_id", ColumnText(statement.raw(), 1)},
          {"mode", "redacted"},
          {"redaction", nlohmann::json{{"reason", "out_of_scope"}}},
      });
      continue;
    }
    results.push_back(nlohmann::json{
        {"block_id", ColumnText(statement.raw(), 0)},
        {"knowledge_id", ColumnText(statement.raw(), 1)},
        {"version_id", ColumnText(statement.raw(), 2)},
        {"title", ColumnText(statement.raw(), 3)},
        {"summary", ColumnText(statement.raw(), 4)},
        {"score", 1.0},
        {"confidence", sqlite3_column_double(statement.raw(), 7)},
        {"freshness", "current"},
        {"shard_id", "kv_default"},
        {"content_hash", ColumnText(statement.raw(), 6)},
        {"redaction", nullptr},
    });
  }
  return nlohmann::json{{"results", results}};
}

nlohmann::json KnowledgeStore::Context(const nlohmann::json& payload) const {
  auto request = naim::knowledge::ContextRequestFromJson(payload);
  const auto search = Search(nlohmann::json{
      {"query", request.query},
      {"scope_id", request.scope_id},
      {"limit", std::max(1, std::min(20, request.token_budget / 600))},
  });
  nlohmann::json context = nlohmann::json::array();
  nlohmann::json redacted = nlohmann::json::array();
  for (const auto& result : search.value("results", nlohmann::json::array())) {
    if (!result.value("mode", std::string{}).empty() || !result.at("redaction").is_null()) {
      redacted.push_back(result);
      continue;
    }
    nlohmann::json entry{
        {"block_id", result.value("block_id", std::string{})},
        {"knowledge_id", result.value("knowledge_id", std::string{})},
        {"version_id", result.value("version_id", std::string{})},
        {"shard_id", result.value("shard_id", std::string("kv_default"))},
        {"title", result.value("title", std::string{})},
        {"text", result.value("summary", std::string{})},
        {"relations", nlohmann::json::array()},
        {"provenance", nlohmann::json::array()},
        {"confidence", result.value("confidence", 1.0)},
        {"freshness", result.value("freshness", std::string("current"))},
        {"content_hash", result.value("content_hash", std::string{})},
      };
    if (request.include_graph && request.max_graph_depth > 0) {
      entry["relations"] = Neighbors(result.value("block_id", std::string{})).value(
          "neighbors",
          nlohmann::json::array());
    }
    context.push_back(entry);
  }
  return nlohmann::json{
      {"request_id", request.request_id},
      {"context", context},
      {"redacted", redacted},
      {"warnings", nlohmann::json::array()},
  };
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

nlohmann::json KnowledgeStore::ReadCapsule(const std::string& capsule_id) const {
  naim::SqliteStatement statement(
      db_,
      "SELECT manifest_json FROM capsules WHERE capsule_id = ?1;");
  statement.BindText(1, capsule_id);
  if (!statement.StepRow()) {
    return nlohmann::json{{"error", "not_found"}, {"message", "capsule not found"}};
  }
  const auto manifest = ParseJsonOr(ColumnText(statement.raw(), 0), nlohmann::json::object());
  if (!manifest.is_object() || manifest.value("capsule_id", std::string{}) != capsule_id) {
    return nlohmann::json{{"error", "invalid_capsule"}, {"message", "capsule manifest is invalid"}};
  }
  return nlohmann::json{{"manifest", manifest}, {"status", "valid"}};
}

nlohmann::json KnowledgeStore::IngestSource(const nlohmann::json& payload) {
  auto request = naim::knowledge::SourceIngestRequestFromJson(payload);
  static const std::set<std::string> kAllowedKinds{
      "document",
      "api",
      "operator",
      "agent",
      "runtime",
      "web",
  };
  if (kAllowedKinds.find(request.source_kind) == kAllowedKinds.end()) {
    const int sequence = AppendEvent(
        "source.rejected",
        {},
        {},
        nlohmann::json{{"source_kind", request.source_kind}, {"reason", "unsupported_source_kind"}});
    return nlohmann::json{
        {"status", "rejected"},
        {"reason", "unsupported_source_kind"},
        {"source_event_sequence", sequence},
    };
  }
  if (request.content.empty() && request.content_hash.empty()) {
    const int sequence = AppendEvent(
        "source.rejected",
        {},
        {},
        nlohmann::json{{"source_kind", request.source_kind}, {"reason", "missing_content"}});
    return nlohmann::json{
        {"status", "rejected"},
        {"reason", "missing_content"},
        {"source_event_sequence", sequence},
    };
  }
  if (request.content_hash.empty()) {
    request.content_hash = naim::ComputeSha256Hex(request.content);
  }
  const std::string source_key =
      naim::ComputeSha256Hex(request.source_kind + "\n" + request.source_ref + "\n" + request.content_hash);
  {
    naim::SqliteStatement duplicate(
        db_,
        "SELECT block_id, event_id FROM sources WHERE source_key = ?1 OR content_hash = ?2 LIMIT 1;");
    duplicate.BindText(1, source_key);
    duplicate.BindText(2, request.content_hash);
    if (duplicate.StepRow()) {
      const int sequence = AppendEvent(
          "source.duplicate",
          {},
          {ColumnText(duplicate.raw(), 0)},
          nlohmann::json{{"source_key", source_key}, {"content_hash", request.content_hash}});
      return nlohmann::json{
          {"status", "duplicate"},
          {"source_block_id", ColumnText(duplicate.raw(), 0)},
          {"source_event_id", ColumnText(duplicate.raw(), 1)},
          {"event_sequence", sequence},
      };
    }
  }

  naim::knowledge::KnowledgeBlock block;
  block.block_id = NewId("src");
  block.knowledge_id = "source." + source_key.substr(0, 16);
  block.version_id = "v1";
  block.type = "source";
  block.title = request.metadata.value("title", request.source_ref.empty() ? request.source_kind : request.source_ref);
  block.body = request.content;
  block.content_hash = request.content_hash;
  block.created_at = UtcNow();
  block.created_by = "source-ingest";
  block.source_ids = {source_key};
  block.scope_ids = request.scope_ids;
  block.payload = nlohmann::json{
      {"source_kind", request.source_kind},
      {"source_ref", request.source_ref},
      {"metadata", request.metadata},
      {"provenance", nlohmann::json{{"content_hash", request.content_hash}}},
  };

  const auto written = WriteBlock(naim::knowledge::ToJson(block));
  const std::string event_id = NewId("evt_source");
  const int sequence = AppendEvent(
      "source.ingested",
      {block.knowledge_id},
      {block.block_id},
      nlohmann::json{
          {"source_kind", request.source_kind},
          {"source_ref", request.source_ref},
          {"content_hash", request.content_hash},
          {"block_id", block.block_id},
      });
  naim::SqliteStatement source_insert(
      db_,
      "INSERT INTO sources(source_key, source_kind, source_ref, content_hash, block_id, event_id, "
      "status, metadata_json, created_at) VALUES(?1, ?2, ?3, ?4, ?5, ?6, 'accepted', ?7, ?8);");
  source_insert.BindText(1, source_key);
  source_insert.BindText(2, request.source_kind);
  source_insert.BindText(3, request.source_ref);
  source_insert.BindText(4, request.content_hash);
  source_insert.BindText(5, block.block_id);
  source_insert.BindText(6, event_id);
  source_insert.BindText(7, JsonText(request.metadata));
  source_insert.BindText(8, block.created_at);
  source_insert.StepDone();

  return nlohmann::json{
      {"status", "accepted"},
      {"source_event_id", event_id},
      {"source_block_id", block.block_id},
      {"event_sequence", sequence},
      {"block", written.value("block", nlohmann::json::object())},
      {"index_triggers", nlohmann::json::array({"text", "catalog", "capsule-delta"})},
  };
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
      "INSERT INTO overlays(overlay_change_id, plane_id, capsule_id, overlay_event_seq, overlay_json, created_at) "
      "VALUES(?1, ?2, ?3, ?4, ?5, ?6);");
  const int sequence = AppendEvent(
      "overlay.proposed",
      {},
      {},
      nlohmann::json{{"overlay_change_id", proposal.overlay_change_id}});
  statement.BindText(1, proposal.overlay_change_id);
  statement.BindText(2, proposal.plane_id);
  statement.BindText(3, proposal.capsule_id);
  statement.BindInt(4, sequence);
  statement.BindText(5, JsonText(naim::knowledge::ToJson(proposal)));
  statement.BindText(6, proposal.created_at);
  statement.StepDone();
  return nlohmann::json{
      {"overlay_change_id", proposal.overlay_change_id},
      {"status", "stored"},
      {"event_sequence", sequence},
      {"overlay_event_seq", sequence},
  };
}

nlohmann::json KnowledgeStore::ScheduleReplicaMerge(const nlohmann::json& payload) {
  const std::string plane_id = payload.value("plane_id", std::string{});
  const std::string capsule_id = payload.value("capsule_id", std::string{});
  if (plane_id.empty() || capsule_id.empty()) {
    throw std::runtime_error("plane_id and capsule_id are required");
  }
  const std::string cadence = payload.value("cadence", std::string("daily"));
  if (cadence != "daily") {
    throw std::runtime_error("only daily replica merge cadence is supported");
  }
  const std::string next_run_at = payload.value("next_run_at", UtcNow());
  naim::SqliteStatement statement(
      db_,
      "INSERT INTO replica_merge_schedules(plane_id, capsule_id, cadence, next_run_at, "
      "last_checkpoint_json, status, updated_at) VALUES(?1, ?2, ?3, ?4, '{}', 'scheduled', ?5) "
      "ON CONFLICT(plane_id, capsule_id) DO UPDATE SET cadence=excluded.cadence, "
      "next_run_at=excluded.next_run_at, status='scheduled', updated_at=excluded.updated_at;");
  statement.BindText(1, plane_id);
  statement.BindText(2, capsule_id);
  statement.BindText(3, cadence);
  statement.BindText(4, next_run_at);
  statement.BindText(5, UtcNow());
  statement.StepDone();
  AppendEvent(
      "replica.merge.scheduled",
      {},
      {},
      nlohmann::json{{"plane_id", plane_id}, {"capsule_id", capsule_id}, {"cadence", cadence}});
  return nlohmann::json{
      {"plane_id", plane_id},
      {"capsule_id", capsule_id},
      {"cadence", cadence},
      {"next_run_at", next_run_at},
      {"status", "scheduled"},
  };
}

nlohmann::json KnowledgeStore::RunScheduledReplicaMerges(const nlohmann::json& payload) {
  const bool force = payload.value("force", false);
  const std::string plane_filter = payload.value("plane_id", std::string{});
  naim::SqliteStatement statement(
      db_,
      plane_filter.empty()
          ? "SELECT plane_id, capsule_id FROM replica_merge_schedules WHERE status = 'scheduled' ORDER BY next_run_at ASC;"
          : "SELECT plane_id, capsule_id FROM replica_merge_schedules WHERE status = 'scheduled' AND plane_id = ?1 ORDER BY next_run_at ASC;");
  if (!plane_filter.empty()) {
    statement.BindText(1, plane_filter);
  }
  std::vector<std::pair<std::string, std::string>> scheduled;
  while (statement.StepRow()) {
    scheduled.emplace_back(ColumnText(statement.raw(), 0), ColumnText(statement.raw(), 1));
  }
  nlohmann::json jobs = nlohmann::json::array();
  for (const auto& [plane_id, capsule_id] : scheduled) {
    if (!force) {
      // The first version intentionally lets controller ticks call this once per daily window.
      // Time-window enforcement belongs to the controller scheduler that owns the cadence.
    }
    auto result = TriggerReplicaMerge(nlohmann::json{
        {"plane_id", plane_id},
        {"capsule_id", capsule_id},
        {"reason", "scheduled"},
    });
    jobs.push_back(result);
    naim::SqliteStatement update(
        db_,
        "UPDATE replica_merge_schedules SET last_checkpoint_json = ?3, next_run_at = ?4, "
        "updated_at = ?4 WHERE plane_id = ?1 AND capsule_id = ?2;");
    update.BindText(1, plane_id);
    update.BindText(2, capsule_id);
    update.BindText(3, JsonText(result.value("checkpoint", nlohmann::json::object())));
    update.BindText(4, UtcNow());
    update.StepDone();
  }
  return nlohmann::json{{"jobs", jobs}, {"status", "completed"}};
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
  checkpoint.overlay_event_seq_from =
      LastCheckpointSequence(checkpoint.plane_id, checkpoint.capsule_id) + 1;
  checkpoint.canonical_event_seq_before = LatestEventSequence();
  checkpoint.started_at = UtcNow();
  AppendEvent(
      "replica.merge.started",
      {},
      {},
      nlohmann::json{
          {"replica_merge_id", checkpoint.replica_merge_id},
          {"plane_id", checkpoint.plane_id},
          {"capsule_id", checkpoint.capsule_id},
      });

  int accepted = 0;
  int review = 0;
  int rejected = 0;
  int max_overlay_seq = checkpoint.overlay_event_seq_from - 1;
  try {
    naim::SqliteStatement overlays(
        db_,
        "SELECT overlay_change_id, overlay_event_seq, overlay_json FROM overlays "
        "WHERE plane_id = ?1 AND capsule_id = ?2 AND overlay_event_seq >= ?3 "
        "ORDER BY overlay_event_seq ASC;");
    overlays.BindText(1, checkpoint.plane_id);
    overlays.BindText(2, checkpoint.capsule_id);
    overlays.BindInt(3, checkpoint.overlay_event_seq_from);
    while (overlays.StepRow()) {
      const std::string overlay_change_id = ColumnText(overlays.raw(), 0);
      const int overlay_seq = sqlite3_column_int(overlays.raw(), 1);
      max_overlay_seq = std::max(max_overlay_seq, overlay_seq);
      const auto overlay_json = ParseJsonOr(ColumnText(overlays.raw(), 2), nlohmann::json::object());
      if (!overlay_json.is_object()) {
        ++rejected;
        continue;
      }
      const auto proposal = naim::knowledge::OverlayFromJson(overlay_json);
      bool conflict = false;
      std::string conflict_knowledge_id;
      if (proposal.base_versions.is_object()) {
        for (const auto& item : proposal.base_versions.items()) {
          const std::string knowledge_id = item.key();
          const std::string expected_version =
              item.value().is_string() ? item.value().get<std::string>() : std::string{};
          const nlohmann::json current_head =
              ResolveHead(knowledge_id).value("head", nlohmann::json(nullptr));
          if (!current_head.is_null() &&
              current_head.value("version_id", std::string{}) != expected_version) {
            conflict = true;
            conflict_knowledge_id = knowledge_id;
            break;
          }
        }
      }
      const double confidence = proposal.confidence;
      if (conflict || confidence < 0.5) {
        naim::knowledge::ReviewItem item;
        item.review_id = NewId("rev");
        item.overlay_change_id = overlay_change_id;
        item.knowledge_id = conflict_knowledge_id.empty() ? proposal.plane_id : conflict_knowledge_id;
        item.type = conflict ? "version_conflict" : "low_confidence";
        item.created_at = UtcNow();
        item.safe_summary = "Knowledge overlay requires review";
        item.affected_scopes = {};
        item.conflicts = nlohmann::json::array({overlay_json});
        PersistReviewItem(item);
        ++review;
        continue;
      }
      for (const auto& block : proposal.proposed_blocks) {
        if (!block.is_object()) {
          continue;
        }
        const auto written = WriteBlock(block);
        const auto block_json = written.value("block", nlohmann::json::object());
        const std::string knowledge_id = block_json.value("knowledge_id", std::string{});
        const std::string block_id = block_json.value("block_id", std::string{});
        if (!knowledge_id.empty() && !block_id.empty()) {
          UpdateHead(knowledge_id, nlohmann::json{{"head_block_id", block_id}});
        }
        ++accepted;
      }
      for (const auto& relation : proposal.proposed_relations) {
        if (relation.is_object()) {
          WriteRelation(relation);
          ++accepted;
        }
      }
    }
    checkpoint.overlay_event_seq_to = max_overlay_seq;
    checkpoint.status = "completed";
    checkpoint.completed_at = UtcNow();
    checkpoint.canonical_event_seq_after = AppendEvent(
        "replica.merge.completed",
        {},
        {},
        nlohmann::json{
            {"replica_merge_id", checkpoint.replica_merge_id},
            {"plane_id", checkpoint.plane_id},
            {"capsule_id", checkpoint.capsule_id},
            {"accepted", accepted},
            {"review", review},
            {"rejected", rejected},
            {"capsule_publication", accepted > 0 ? "delta_requested" : "skipped"},
        });
  } catch (const std::exception& error) {
    checkpoint.status = "failed";
    checkpoint.completed_at = UtcNow();
    checkpoint.overlay_event_seq_to = max_overlay_seq;
    checkpoint.canonical_event_seq_after = LatestEventSequence();
    AppendEvent(
        "replica.merge.failed",
        {},
        {},
        nlohmann::json{
            {"replica_merge_id", checkpoint.replica_merge_id},
            {"plane_id", checkpoint.plane_id},
            {"capsule_id", checkpoint.capsule_id},
            {"message", error.what()},
        });
  }
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
      {"accepted", accepted},
      {"review", review},
      {"rejected", rejected},
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

nlohmann::json KnowledgeStore::ListReviewItems(const nlohmann::json& payload) const {
  const std::string status = payload.value("status", std::string("pending"));
  naim::SqliteStatement statement(
      db_,
      "SELECT item_json FROM review_queue WHERE status = ?1 ORDER BY created_at ASC LIMIT ?2;");
  statement.BindText(1, status);
  statement.BindInt(2, std::max(1, std::min(200, payload.value("limit", 50))));
  nlohmann::json items = nlohmann::json::array();
  while (statement.StepRow()) {
    items.push_back(ParseJsonOr(ColumnText(statement.raw(), 0), nlohmann::json::object()));
  }
  return nlohmann::json{{"items", items}, {"status", status}};
}

nlohmann::json KnowledgeStore::DecideReviewItem(
    const std::string& review_id,
    const nlohmann::json& payload) {
  const std::string action = payload.value("action", std::string{});
  static const std::set<std::string> kAllowedActions{
      "accept",
      "reject",
      "request-more-evidence",
      "needs_evidence",
  };
  if (kAllowedActions.find(action) == kAllowedActions.end()) {
    throw std::runtime_error("unsupported review action");
  }
  naim::SqliteStatement load(
      db_,
      "SELECT item_json FROM review_queue WHERE review_id = ?1;");
  load.BindText(1, review_id);
  if (!load.StepRow()) {
    return nlohmann::json{{"error", "not_found"}, {"message", "review item not found"}};
  }
  auto item_json = ParseJsonOr(ColumnText(load.raw(), 0), nlohmann::json::object());
  std::string new_status = action == "accept" ? "accepted" : action;
  if (action == "request-more-evidence") {
    new_status = "needs_evidence";
  }
  item_json["status"] = new_status;
  item_json["decision"] = nlohmann::json{
      {"action", action},
      {"reason", payload.value("reason", std::string{})},
      {"decided_at", UtcNow()},
  };
  naim::SqliteStatement update(
      db_,
      "UPDATE review_queue SET status = ?2, item_json = ?3, updated_at = ?4 WHERE review_id = ?1;");
  update.BindText(1, review_id);
  update.BindText(2, new_status);
  update.BindText(3, JsonText(item_json));
  update.BindText(4, UtcNow());
  update.StepDone();
  AppendEvent(
      "review." + new_status,
      {},
      {},
      nlohmann::json{{"review_id", review_id}, {"action", action}});
  return nlohmann::json{{"review_id", review_id}, {"status", new_status}, {"item", item_json}};
}

nlohmann::json KnowledgeStore::RunRepair(const nlohmann::json& payload) {
  const bool apply = payload.value("apply", false);
  nlohmann::json findings = nlohmann::json::array();
  const auto add_finding = [&](const std::string& severity,
                               const std::string& type,
                               const std::string& block_id,
                               const std::string& relation_id,
                               const std::string& action) {
    naim::knowledge::RepairFinding finding;
    finding.finding_id = NewId("repair");
    finding.severity = severity;
    finding.type = type;
    finding.block_id = block_id;
    finding.relation_id = relation_id;
    finding.event_seq = LatestEventSequence();
    finding.repair_action = action;
    finding.created_at = UtcNow();
    findings.push_back(naim::knowledge::ToJson(finding));
  };

  {
    naim::SqliteStatement missing_search(
        db_,
        "SELECT b.block_id, b.title, b.body FROM blocks b "
        "LEFT JOIN block_search s ON s.block_id = b.block_id WHERE s.block_id IS NULL;");
    while (missing_search.StepRow()) {
      const std::string block_id = ColumnText(missing_search.raw(), 0);
      add_finding("warning", "stale_text_index", block_id, "", apply ? "applied" : "queued");
      if (apply) {
        naim::SqliteStatement insert(
            db_,
            "INSERT OR REPLACE INTO block_search(block_id, title, body) VALUES(?1, ?2, ?3);");
        insert.BindText(1, block_id);
        insert.BindText(2, ColumnText(missing_search.raw(), 1));
        insert.BindText(3, ColumnText(missing_search.raw(), 2));
        insert.StepDone();
      }
    }
  }
  {
    naim::SqliteStatement broken_edges(
        db_,
        "SELECT r.relation_id, r.from_block_id, r.to_block_id FROM relations r "
        "LEFT JOIN blocks bf ON bf.block_id = r.from_block_id "
        "LEFT JOIN blocks bt ON bt.block_id = r.to_block_id "
        "WHERE bf.block_id IS NULL OR bt.block_id IS NULL;");
    while (broken_edges.StepRow()) {
      add_finding(
          "error",
          "edge_missing_block",
          ColumnText(broken_edges.raw(), 1),
          ColumnText(broken_edges.raw(), 0),
          "manual_review");
    }
  }
  {
    naim::SqliteStatement capsule_members(
        db_,
        "SELECT capsule_id, manifest_json FROM capsules;");
    while (capsule_members.StepRow()) {
      const auto manifest =
          ParseJsonOr(ColumnText(capsule_members.raw(), 1), nlohmann::json::object());
      for (const auto& item : manifest.value("included", nlohmann::json::array())) {
        const std::string block_id =
            item.is_string() ? item.get<std::string>() : item.value("block_id", std::string{});
        if (block_id.empty()) {
          continue;
        }
        const auto block = ReadBlock(block_id);
        if (block.contains("error")) {
          add_finding("error", "missing_capsule_member", block_id, "", "manual_review");
        }
      }
    }
  }

  const std::string report_id = NewId("rr");
  naim::SqliteStatement insert_report(
      db_,
      "INSERT INTO repair_reports(report_id, status, findings_json, created_at) "
      "VALUES(?1, ?2, ?3, ?4);");
  insert_report.BindText(1, report_id);
  insert_report.BindText(2, findings.empty() ? "clean" : "findings");
  insert_report.BindText(3, JsonText(findings));
  insert_report.BindText(4, UtcNow());
  insert_report.StepDone();
  const int sequence = AppendEvent(
      "repair.completed",
      {},
      {},
      nlohmann::json{{"report_id", report_id}, {"finding_count", findings.size()}});
  return nlohmann::json{
      {"report_id", report_id},
      {"status", findings.empty() ? "clean" : "findings"},
      {"findings", findings},
      {"event_sequence", sequence},
      {"index_epoch", "idx_" + std::to_string(LatestEventSequence())},
  };
}

nlohmann::json KnowledgeStore::MarkdownExport(const nlohmann::json& payload) const {
  std::vector<std::string> requested_scopes;
  if (payload.contains("scope_ids")) {
    requested_scopes = JsonStringArray(payload.at("scope_ids"));
  }
  nlohmann::json files = nlohmann::json::array();
  nlohmann::json warnings = nlohmann::json::array();
  naim::SqliteStatement statement(
      db_,
      "SELECT block_id, knowledge_id, version_id, type, title, body, source_ids_json, "
      "scope_ids_json, confidence, content_hash FROM blocks ORDER BY knowledge_id, created_at;");
  while (statement.StepRow()) {
    const auto scope_ids = ParseJsonOr(ColumnText(statement.raw(), 7), nlohmann::json::array());
    if (!ScopeAllowed(scope_ids, requested_scopes)) {
      warnings.push_back(nlohmann::json{
          {"block_id", ColumnText(statement.raw(), 0)},
          {"warning", "restricted_export_skipped"},
      });
      continue;
    }
    const std::string title = ColumnText(statement.raw(), 4).empty()
                                  ? ColumnText(statement.raw(), 1)
                                  : ColumnText(statement.raw(), 4);
    std::string slug = NormalizeTerm(title);
    std::replace(slug.begin(), slug.end(), ' ', '-');
    if (slug.empty()) {
      slug = ColumnText(statement.raw(), 0);
    }
    std::ostringstream markdown;
    markdown << "---\n"
             << "type: " << ColumnText(statement.raw(), 3) << "\n"
             << "status: generated\n"
             << "knowledge_id: " << ColumnText(statement.raw(), 1) << "\n"
             << "block_id: " << ColumnText(statement.raw(), 0) << "\n"
             << "version_id: " << ColumnText(statement.raw(), 2) << "\n"
             << "content_hash: " << ColumnText(statement.raw(), 9) << "\n"
             << "scope_ids: " << scope_ids.dump() << "\n"
             << "source_ids: " << ParseJsonOr(ColumnText(statement.raw(), 6), nlohmann::json::array()).dump() << "\n"
             << "related: []\n"
             << "---\n\n"
             << "# " << MarkdownEscape(title) << "\n\n"
             << MarkdownEscape(ColumnText(statement.raw(), 5)) << "\n";
    files.push_back(nlohmann::json{
        {"path", slug + ".md"},
        {"block_id", ColumnText(statement.raw(), 0)},
        {"content", markdown.str()},
    });
  }
  return nlohmann::json{{"files", files}, {"warnings", warnings}, {"status", "generated"}};
}

nlohmann::json KnowledgeStore::GraphNeighborhood(const nlohmann::json& payload) const {
  const std::string center = payload.value("center_id", std::string{});
  const int depth = std::max(1, std::min(2, payload.value("depth", 1)));
  std::set<std::string> visited;
  std::vector<std::string> frontier{center};
  nlohmann::json nodes = nlohmann::json::array();
  nlohmann::json edges = nlohmann::json::array();
  for (int current_depth = 0; current_depth < depth && !frontier.empty(); ++current_depth) {
    std::vector<std::string> next;
    for (const auto& block_id : frontier) {
      if (!visited.insert(block_id).second) {
        continue;
      }
      const auto block = ReadBlock(block_id);
      if (!block.contains("error")) {
        nodes.push_back(block.value("block", nlohmann::json::object()));
      }
      const auto neighbors = Neighbors(block_id).value("neighbors", nlohmann::json::array());
      for (const auto& relation : neighbors) {
        edges.push_back(relation);
        const std::string from = relation.value("from_block_id", std::string{});
        const std::string to = relation.value("to_block_id", std::string{});
        next.push_back(from == block_id ? to : from);
      }
    }
    frontier = std::move(next);
  }
  return nlohmann::json{{"nodes", nodes}, {"edges", edges}, {"warnings", nlohmann::json::array()}};
}

nlohmann::json KnowledgeStore::CatalogUpsert(const nlohmann::json& payload) {
  const std::string object_id = payload.value("object_id", std::string{});
  if (object_id.empty()) {
    throw std::runtime_error("object_id is required");
  }
  const std::string shard_id = payload.value("shard_id", std::string("kv_default"));
  const nlohmann::json scope_ids = payload.value("scope_ids", nlohmann::json::array());
  const nlohmann::json hints = payload.value("hints", nlohmann::json::array());
  naim::SqliteStatement upsert(
      db_,
      "INSERT INTO catalog_objects(object_id, shard_id, scope_ids_json, hints_json, updated_at) "
      "VALUES(?1, ?2, ?3, ?4, ?5) "
      "ON CONFLICT(object_id) DO UPDATE SET shard_id=excluded.shard_id, "
      "scope_ids_json=excluded.scope_ids_json, hints_json=excluded.hints_json, updated_at=excluded.updated_at;");
  upsert.BindText(1, object_id);
  upsert.BindText(2, shard_id);
  upsert.BindText(3, JsonText(scope_ids));
  upsert.BindText(4, JsonText(hints));
  upsert.BindText(5, UtcNow());
  upsert.StepDone();
  for (const auto& hint : hints) {
    const std::string term = NormalizeTerm(hint.is_string() ? hint.get<std::string>() : hint.dump());
    if (term.empty()) {
      continue;
    }
    naim::SqliteStatement term_upsert(
        db_,
        "INSERT OR REPLACE INTO catalog_terms(term, object_id, shard_id, scope_ids_json, updated_at) "
        "VALUES(?1, ?2, ?3, ?4, ?5);");
    term_upsert.BindText(1, term);
    term_upsert.BindText(2, object_id);
    term_upsert.BindText(3, shard_id);
    term_upsert.BindText(4, JsonText(scope_ids));
    term_upsert.BindText(5, UtcNow());
    term_upsert.StepDone();
  }
  return nlohmann::json{{"object_id", object_id}, {"shard_id", shard_id}, {"status", "stored"}};
}

nlohmann::json KnowledgeStore::CatalogQuery(const nlohmann::json& payload) const {
  const std::string term = NormalizeTerm(payload.value("term", std::string{}));
  std::vector<std::string> requested_scopes;
  if (payload.contains("scope_ids")) {
    requested_scopes = JsonStringArray(payload.at("scope_ids"));
  }
  naim::SqliteStatement query(
      db_,
      "SELECT object_id, shard_id, scope_ids_json FROM catalog_terms WHERE term LIKE ?1 LIMIT ?2;");
  query.BindText(1, "%" + term + "%");
  query.BindInt(2, std::max(1, std::min(200, payload.value("limit", 50))));
  nlohmann::json candidates = nlohmann::json::array();
  while (query.StepRow()) {
    const auto scope_ids = ParseJsonOr(ColumnText(query.raw(), 2), nlohmann::json::array());
    if (!ScopeAllowed(scope_ids, requested_scopes)) {
      candidates.push_back(nlohmann::json{
          {"object_id", ColumnText(query.raw(), 0)},
          {"mode", "redacted"},
          {"redaction", nlohmann::json{{"reason", "out_of_scope"}}},
      });
      continue;
    }
    candidates.push_back(nlohmann::json{
        {"object_id", ColumnText(query.raw(), 0)},
        {"shard_id", ColumnText(query.raw(), 1)},
        {"scope_ids", scope_ids},
        {"stale_hint", false},
    });
  }
  return nlohmann::json{{"candidates", candidates}, {"status", "ok"}};
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

int KnowledgeStore::LastCheckpointSequence(
    const std::string& plane_id,
    const std::string& capsule_id) const {
  naim::SqliteStatement statement(
      db_,
      "SELECT checkpoint_json FROM replica_merge_checkpoints "
      "WHERE plane_id = ?1 AND capsule_id = ?2 AND status = 'completed' "
      "ORDER BY completed_at DESC LIMIT 1;");
  statement.BindText(1, plane_id);
  statement.BindText(2, capsule_id);
  if (!statement.StepRow()) {
    return 0;
  }
  return ParseJsonOr(ColumnText(statement.raw(), 0), nlohmann::json::object())
      .value("overlay_event_seq_to", 0);
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

void KnowledgeStore::PersistReviewItem(const naim::knowledge::ReviewItem& item) {
  naim::SqliteStatement statement(
      db_,
      "INSERT INTO review_queue(review_id, overlay_change_id, knowledge_id, type, status, "
      "item_json, created_at, updated_at) VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7, ?7) "
      "ON CONFLICT(review_id) DO UPDATE SET status=excluded.status, "
      "item_json=excluded.item_json, updated_at=excluded.updated_at;");
  statement.BindText(1, item.review_id);
  statement.BindText(2, item.overlay_change_id);
  statement.BindText(3, item.knowledge_id);
  statement.BindText(4, item.type);
  statement.BindText(5, item.status);
  statement.BindText(6, JsonText(naim::knowledge::ToJson(item)));
  statement.BindText(7, item.created_at.empty() ? UtcNow() : item.created_at);
  statement.StepDone();
  AppendEvent(
      "review.queued",
      {item.knowledge_id},
      {},
      nlohmann::json{{"review_id", item.review_id}, {"overlay_change_id", item.overlay_change_id}});
}

bool KnowledgeStore::ScopeAllowed(
    const nlohmann::json& record_scope_ids,
    const std::vector<std::string>& requested_scope_ids) const {
  if (requested_scope_ids.empty()) {
    return true;
  }
  if (!record_scope_ids.is_array() || record_scope_ids.empty()) {
    return true;
  }
  for (const auto& scope : requested_scope_ids) {
    if (JsonArrayContainsString(record_scope_ids, scope)) {
      return true;
    }
  }
  return false;
}

std::vector<std::string> KnowledgeStore::JsonStringArray(const nlohmann::json& value) {
  std::vector<std::string> result;
  if (!value.is_array()) {
    return result;
  }
  for (const auto& item : value) {
    if (item.is_string()) {
      result.push_back(item.get<std::string>());
    }
  }
  return result;
}

std::string KnowledgeStore::JsonText(const nlohmann::json& value) {
  return value.dump();
}

std::string KnowledgeStore::NormalizeTerm(const std::string& value) {
  std::string result;
  bool previous_space = false;
  for (unsigned char ch : value) {
    if (std::isalnum(ch)) {
      result.push_back(static_cast<char>(std::tolower(ch)));
      previous_space = false;
    } else if (!previous_space && !result.empty()) {
      result.push_back(' ');
      previous_space = true;
    }
  }
  while (!result.empty() && result.back() == ' ') {
    result.pop_back();
  }
  return result;
}

std::string KnowledgeStore::MarkdownEscape(const std::string& value) {
  std::string result;
  result.reserve(value.size());
  for (char ch : value) {
    if (ch == '\r') {
      continue;
    }
    result.push_back(ch);
  }
  return result;
}

}  // namespace naim::knowledge_runtime
