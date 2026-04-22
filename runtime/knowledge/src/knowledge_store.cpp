#include "knowledge/knowledge_store.h"

#include <algorithm>
#include <chrono>
#include <cctype>
#include <map>
#include <random>
#include <set>
#include <sstream>
#include <stdexcept>
#include <utility>

#include <rocksdb/write_batch.h>

#include "knowledge/knowledge_store_keys.h"
#include "knowledge/knowledge_text_processor.h"
#include "knowledge/rocksdb_json_repository.h"
#include "naim/security/crypto_utils.h"

namespace naim::knowledge_runtime {

namespace {

bool StringSetContains(const std::set<std::string>& values, const std::string& value) {
  return values.empty() || values.find(value) != values.end();
}

std::string BlockSearchText(const nlohmann::json& block) {
  return KnowledgeTextProcessor::Lowercase(
      block.value("title", std::string{}) + "\n" +
      block.value("body", std::string{}) + "\n" +
      block.value("knowledge_id", std::string{}) + "\n" +
      block.value("block_id", std::string{}));
}

std::string RelationSearchText(const nlohmann::json& relation) {
  return KnowledgeTextProcessor::Lowercase(
      relation.value("relation_id", std::string{}) + "\n" +
      relation.value("type", std::string{}) + "\n" +
      relation.value("from_block_id", std::string{}) + "\n" +
      relation.value("to_block_id", std::string{}));
}

}  // namespace

KnowledgeStore::KnowledgeStore(std::filesystem::path store_path)
    : repository_(std::make_unique<RocksDbJsonRepository>(std::move(store_path))) {}

KnowledgeStore::~KnowledgeStore() = default;

void KnowledgeStore::Open() {
  repository_->Open();
}

nlohmann::json KnowledgeStore::Status(const std::string& service_id) const {
  return nlohmann::json{
      {"service_id", service_id},
      {"status", "ready"},
      {"store_profile", "canonical-shard"},
      {"storage_engine", "rocksdb"},
      {"store_path", repository_->path().string()},
      {"schema_version", "knowledge.v1"},
      {"index_epoch", "idx_" + std::to_string(LatestEventSequence())},
      {"latest_event_sequence", LatestEventSequence()},
  };
}

nlohmann::json KnowledgeStore::WriteBlock(const nlohmann::json& payload) {
  auto block = naim::knowledge::KnowledgeJsonCodec::BlockFromJson(payload);
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

  const auto block_json = naim::knowledge::KnowledgeJsonCodec::ToJson(block);
  rocksdb::WriteBatch batch;
  batch.Put("blocks:" + block.block_id, block_json.dump());
  KnowledgeTextProcessor::IndexTerms(batch, block_json);
  for (const auto& scope_id : block.scope_ids) {
    batch.Put("acl:" + scope_id + ":" + block.block_id, block_json.dump());
  }
  repository_->Write(batch, "write knowledge block");

  const int sequence = AppendEvent(
      "block.created",
      {block.knowledge_id},
      {block.block_id},
      nlohmann::json{{"block_id", block.block_id}, {"knowledge_id", block.knowledge_id}});
  return nlohmann::json{{"block", block_json}, {"event_sequence", sequence}};
}

nlohmann::json KnowledgeStore::ReadBlock(const std::string& block_id) const {
  const auto block = repository_->GetJson("blocks:" + block_id);
  if (!block.has_value()) {
    return nlohmann::json{{"error", "not_found"}, {"message", "block not found"}};
  }
  return nlohmann::json{{"block", *block}};
}

nlohmann::json KnowledgeStore::ResolveHead(const std::string& knowledge_id) const {
  const auto head = repository_->GetJson("heads:" + knowledge_id);
  return nlohmann::json{{"head", head.has_value() ? *head : nlohmann::json(nullptr)}};
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
  repository_->PutJson("heads:" + knowledge_id, naim::knowledge::KnowledgeJsonCodec::ToJson(head));
  const int sequence = AppendEvent(
      "head.updated",
      {knowledge_id},
      {head_block_id},
      nlohmann::json{{"knowledge_id", knowledge_id}, {"head_block_id", head_block_id}});
  return nlohmann::json{{"head", naim::knowledge::KnowledgeJsonCodec::ToJson(head)}, {"event_sequence", sequence}};
}

nlohmann::json KnowledgeStore::WriteRelation(const nlohmann::json& payload) {
  auto relation = naim::knowledge::KnowledgeJsonCodec::RelationFromJson(payload);
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
  const auto relation_json = naim::knowledge::KnowledgeJsonCodec::ToJson(relation);
  rocksdb::WriteBatch batch;
  batch.Put(
      "edges_out:" + relation.from_block_id + ":" + relation.type + ":" + relation.to_block_id +
          ":" + relation.relation_id,
      relation_json.dump());
  batch.Put(
      "edges_in:" + relation.to_block_id + ":" + relation.type + ":" + relation.from_block_id +
          ":" + relation.relation_id,
      relation_json.dump());
  repository_->Write(batch, "write relation");
  const int sequence = AppendEvent(
      "relation.created",
      {},
      {relation.from_block_id, relation.to_block_id},
      nlohmann::json{{"relation_id", relation.relation_id}});
  return nlohmann::json{{"relation", relation_json}, {"event_sequence", sequence}};
}

nlohmann::json KnowledgeStore::Neighbors(const std::string& block_id) const {
  nlohmann::json items = nlohmann::json::array();
  std::set<std::string> seen;
  for (const auto& [key, value] : repository_->ScanJson("edges_out:" + block_id + ":")) {
    const std::string relation_id = value.value("relation_id", key);
    if (seen.insert(relation_id).second) {
      items.push_back(value);
    }
  }
  for (const auto& [key, value] : repository_->ScanJson("edges_in:" + block_id + ":")) {
    const std::string relation_id = value.value("relation_id", key);
    if (seen.insert(relation_id).second) {
      items.push_back(value);
    }
  }
  return nlohmann::json{{"neighbors", items}};
}

nlohmann::json KnowledgeStore::Search(const nlohmann::json& payload) const {
  const std::string query = payload.value("query", std::string{});
  const bool list_all = payload.value("list_all", false);
  const auto selected_knowledge_ids =
      payload.contains("selected_knowledge_ids")
          ? JsonStringArray(payload.at("selected_knowledge_ids"))
          : std::vector<std::string>{};
  const std::set<std::string> selected_filter(
      selected_knowledge_ids.begin(),
      selected_knowledge_ids.end());
  if (query.empty() && !list_all && selected_filter.empty()) {
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
  const std::string lowered_query = KnowledgeTextProcessor::Lowercase(query);
  nlohmann::json results = nlohmann::json::array();
  for (const auto& [key, block] : repository_->ScanJson("blocks:")) {
    const std::string knowledge_id = block.value("knowledge_id", std::string{});
    if (!StringSetContains(selected_filter, knowledge_id)) {
      continue;
    }
    const auto neighbors = Neighbors(block.value("block_id", std::string{}))
                               .value("neighbors", nlohmann::json::array());
    bool matched = query.empty() || BlockSearchText(block).find(lowered_query) != std::string::npos;
    for (const auto& relation : neighbors) {
      if (matched) {
        break;
      }
      if (RelationSearchText(relation).find(lowered_query) != std::string::npos) {
        matched = true;
        break;
      }
      const std::string from = relation.value("from_block_id", std::string{});
      const std::string to = relation.value("to_block_id", std::string{});
      const std::string neighbor_id =
          from == block.value("block_id", std::string{}) ? to : from;
      const auto neighbor = repository_->GetJson("blocks:" + neighbor_id);
      if (neighbor.has_value() &&
          BlockSearchText(*neighbor).find(lowered_query) != std::string::npos) {
        matched = true;
        break;
      }
    }
    if (!matched) {
      continue;
    }
    const auto scope_ids = block.value("scope_ids", nlohmann::json::array());
    if (!ScopeAllowed(scope_ids, requested_scopes)) {
      results.push_back(nlohmann::json{
          {"block_id", block.value("block_id", std::string{})},
          {"knowledge_id", knowledge_id},
          {"mode", "redacted"},
          {"redaction", nlohmann::json{{"reason", "out_of_scope"}}},
      });
    } else {
      const std::string body = block.value("body", std::string{});
      results.push_back(nlohmann::json{
          {"block_id", block.value("block_id", std::string{})},
          {"knowledge_id", knowledge_id},
          {"version_id", block.value("version_id", std::string{})},
          {"title", block.value("title", std::string{})},
          {"type", block.value("type", std::string("note"))},
          {"scope_ids", scope_ids},
          {"summary", body.substr(0, 240)},
          {"score", 1.0},
          {"confidence", block.value("confidence", 1.0)},
          {"freshness", "current"},
          {"relation_count", neighbors.size()},
          {"shard_id", std::string(KnowledgeStoreKeys::kDefaultShardId)},
          {"content_hash", block.value("content_hash", std::string{})},
          {"redaction", nullptr},
      });
    }
    if (static_cast<int>(results.size()) >= limit) {
      break;
    }
  }
  return nlohmann::json{{"results", results}};
}

nlohmann::json KnowledgeStore::Context(const nlohmann::json& payload) const {
  auto request = naim::knowledge::KnowledgeJsonCodec::ContextRequestFromJson(payload);
  const auto search = Search(nlohmann::json{
      {"query", request.query},
      {"scope_id", request.scope_id},
      {"selected_knowledge_ids", payload.value("selected_knowledge_ids", nlohmann::json::array())},
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
        {"shard_id", result.value("shard_id", std::string(KnowledgeStoreKeys::kDefaultShardId))},
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
  if (!request.plane_id.empty() && payload.contains("capsule_id")) {
    const std::string prefix =
        "overlays:" + request.plane_id + ":" + payload.value("capsule_id", std::string{}) + ":";
    auto overlays = repository_->ScanJson(prefix);
    std::reverse(overlays.begin(), overlays.end());
    for (const auto& [key, overlay] : overlays) {
      for (const auto& block : overlay.value("proposed_blocks", nlohmann::json::array())) {
        if (!block.is_object()) {
          continue;
        }
        context.push_back(nlohmann::json{
            {"block_id", block.value("block_id", std::string{})},
            {"knowledge_id", block.value("knowledge_id", std::string{})},
            {"version_id", block.value("version_id", std::string("overlay"))},
            {"shard_id", "overlay"},
            {"title", block.value("title", std::string{})},
            {"text", block.value("body", std::string{})},
            {"relations", nlohmann::json::array()},
            {"provenance", nlohmann::json::array({nlohmann::json{{"source", "overlay"}}})},
            {"confidence", overlay.value("confidence", 1.0)},
            {"freshness", "overlay"},
            {"content_hash", block.value("content_hash", std::string{})},
        });
      }
    }
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
  manifest.storage_engine = "rocksdb";
  manifest.storage_profile = payload.value("storage_profile", std::string("capsule-base"));
  manifest.base_event_seq = LatestEventSequence();
  manifest.created_at = UtcNow();
  manifest.policy = payload.value("policy", nlohmann::json::object());
  manifest.indexes = nlohmann::json{{"text", "idx_" + std::to_string(manifest.base_event_seq)}};
  manifest.included = payload.value("included", nlohmann::json::array());
  const auto manifest_json = naim::knowledge::KnowledgeJsonCodec::ToJson(manifest);
  rocksdb::WriteBatch batch;
  batch.Put("capsules:" + manifest.capsule_id, manifest_json.dump());
  for (const auto& item : manifest.included) {
    const std::string block_id =
        item.is_string() ? item.get<std::string>() : item.value("block_id", std::string{});
    if (!block_id.empty()) {
      batch.Put("capsule_members:" + manifest.capsule_id + ":" + block_id, manifest_json.dump());
    }
  }
  repository_->Write(batch, "write capsule");
  AppendEvent("capsule.published", {}, {}, manifest_json);
  return nlohmann::json{{"capsule_id", manifest.capsule_id}, {"manifest", manifest_json}};
}

nlohmann::json KnowledgeStore::ReadCapsule(const std::string& capsule_id) const {
  const auto manifest = repository_->GetJson("capsules:" + capsule_id);
  if (!manifest.has_value()) {
    return nlohmann::json{{"error", "not_found"}, {"message", "capsule not found"}};
  }
  if (!manifest->is_object() || manifest->value("capsule_id", std::string{}) != capsule_id) {
    return nlohmann::json{{"error", "invalid_capsule"}, {"message", "capsule manifest is invalid"}};
  }
  return nlohmann::json{{"manifest", *manifest}, {"status", "valid"}};
}

nlohmann::json KnowledgeStore::IngestSource(const nlohmann::json& payload) {
  auto request = naim::knowledge::KnowledgeJsonCodec::SourceIngestRequestFromJson(payload);
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
  const std::string source_key = naim::ComputeSha256Hex(
      request.source_kind + "\n" + request.source_ref + "\n" + request.content_hash);
  if (const auto existing = repository_->GetJson("sources:" + source_key); existing.has_value()) {
    const int sequence = AppendEvent(
        "source.duplicate",
        {},
        {existing->value("block_id", std::string{})},
        nlohmann::json{{"source_key", source_key}, {"content_hash", request.content_hash}});
    return nlohmann::json{
        {"status", "duplicate"},
        {"source_block_id", existing->value("block_id", std::string{})},
        {"source_event_id", existing->value("event_id", std::string{})},
        {"event_sequence", sequence},
    };
  }
  const auto hash_hits = repository_->ScanJson("source_hash:" + request.content_hash + ":");
  if (!hash_hits.empty()) {
    const auto existing = hash_hits.front().second;
    const int sequence = AppendEvent(
        "source.duplicate",
        {},
        {existing.value("block_id", std::string{})},
        nlohmann::json{{"source_key", source_key}, {"content_hash", request.content_hash}});
    return nlohmann::json{
        {"status", "duplicate"},
        {"source_block_id", existing.value("block_id", std::string{})},
        {"source_event_id", existing.value("event_id", std::string{})},
        {"event_sequence", sequence},
    };
  }

  naim::knowledge::KnowledgeBlock block;
  block.block_id = NewId("src");
  block.knowledge_id = "source." + source_key.substr(0, 16);
  block.version_id = "v1";
  block.type = "source";
  block.title =
      request.metadata.value("title", request.source_ref.empty() ? request.source_kind : request.source_ref);
  block.body = request.content;
  block.content_hash = request.content_hash;
  block.created_at = UtcNow();
  block.created_by = "source-ingest";
  block.source_ids = {source_key};
  block.scope_ids = request.scope_ids;
  const auto chunks = KnowledgeTextProcessor::ChunkSource(source_key, block.block_id, request.content);
  const auto claims = KnowledgeTextProcessor::ExtractClaims(source_key, block.block_id, chunks);
  nlohmann::json relation_candidates = nlohmann::json::array();
  for (const auto& claim : claims) {
    relation_candidates.push_back(nlohmann::json{
        {"from_block_id", block.block_id},
        {"to_block_id", ""},
        {"type", "supports"},
        {"claim_id", claim.value("claim_id", std::string{})},
        {"confidence", claim.value("confidence", 0.5)},
    });
  }
  block.payload = nlohmann::json{
      {"source_kind", request.source_kind},
      {"source_ref", request.source_ref},
      {"metadata", request.metadata},
      {"provenance", nlohmann::json{{"content_hash", request.content_hash}}},
      {"chunks", chunks},
      {"claims", claims},
      {"relation_candidates", relation_candidates},
  };

  const auto written = WriteBlock(naim::knowledge::KnowledgeJsonCodec::ToJson(block));
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
  nlohmann::json source_record{
      {"source_key", source_key},
      {"source_kind", request.source_kind},
      {"source_ref", request.source_ref},
      {"content_hash", request.content_hash},
      {"block_id", block.block_id},
      {"event_id", event_id},
      {"status", "accepted"},
      {"metadata", request.metadata},
      {"created_at", block.created_at},
  };
  rocksdb::WriteBatch batch;
  batch.Put("sources:" + source_key, source_record.dump());
  batch.Put("source_hash:" + request.content_hash + ":" + source_key, source_record.dump());
  for (const auto& chunk : chunks) {
    batch.Put("source_chunks:" + chunk.value("chunk_id", std::string{}), chunk.dump());
  }
  for (const auto& claim : claims) {
    batch.Put("source_claims:" + claim.value("claim_id", std::string{}), claim.dump());
  }
  repository_->Write(batch, "write source ingest records");
  return nlohmann::json{
      {"status", "accepted"},
      {"source_event_id", event_id},
      {"source_block_id", block.block_id},
      {"event_sequence", sequence},
      {"block", written.value("block", nlohmann::json::object())},
      {"chunks", chunks},
      {"claims", claims},
      {"relation_candidates", relation_candidates},
      {"provenance", nlohmann::json{{"source_key", source_key}, {"content_hash", request.content_hash}}},
      {"index_triggers", nlohmann::json::array({"text", "catalog", "capsule-delta"})},
  };
}

nlohmann::json KnowledgeStore::WriteOverlay(const nlohmann::json& payload) {
  auto proposal = naim::knowledge::KnowledgeJsonCodec::OverlayFromJson(payload);
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
  const int sequence = AppendEvent(
      "overlay.proposed",
      {},
      {},
      nlohmann::json{{"overlay_change_id", proposal.overlay_change_id}});
  auto proposal_json = naim::knowledge::KnowledgeJsonCodec::ToJson(proposal);
  proposal_json["overlay_event_seq"] = sequence;
  const std::string sequence_key = KnowledgeStoreKeys::EventKey(sequence).substr(std::string("events:").size());
  rocksdb::WriteBatch batch;
  batch.Put(
      "overlays:" + proposal.plane_id + ":" + proposal.capsule_id + ":" + sequence_key + ":" +
          proposal.overlay_change_id,
      proposal_json.dump());
  batch.Put("overlay_by_id:" + proposal.overlay_change_id, proposal_json.dump());
  repository_->Write(batch, "write overlay");
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
  nlohmann::json schedule{
      {"plane_id", plane_id},
      {"capsule_id", capsule_id},
      {"cadence", cadence},
      {"next_run_at", next_run_at},
      {"last_checkpoint", nlohmann::json::object()},
      {"status", "scheduled"},
      {"updated_at", UtcNow()},
  };
  repository_->PutJson("replica_schedules:" + plane_id + ":" + capsule_id, schedule);
  AppendEvent(
      "replica.merge.scheduled",
      {},
      {},
      nlohmann::json{{"plane_id", plane_id}, {"capsule_id", capsule_id}, {"cadence", cadence}});
  return schedule;
}

nlohmann::json KnowledgeStore::RunScheduledReplicaMerges(const nlohmann::json& payload) {
  const bool force = payload.value("force", false);
  const std::string plane_filter = payload.value("plane_id", std::string{});
  nlohmann::json jobs = nlohmann::json::array();
  for (const auto& [key, schedule] : repository_->ScanJson("replica_schedules:")) {
    if (schedule.value("status", std::string{}) != "scheduled") {
      continue;
    }
    const std::string plane_id = schedule.value("plane_id", std::string{});
    if (!plane_filter.empty() && plane_filter != plane_id) {
      continue;
    }
    if (!force) {
      // Controller owns wall-clock cadence; the runtime reconciles schedules selected by controller.
    }
    auto result = TriggerReplicaMerge(nlohmann::json{
        {"plane_id", plane_id},
        {"capsule_id", schedule.value("capsule_id", std::string{})},
        {"reason", "scheduled"},
    });
    jobs.push_back(result);
    auto updated = schedule;
    updated["last_checkpoint"] = result.value("checkpoint", nlohmann::json::object());
    updated["next_run_at"] = UtcNow();
    updated["updated_at"] = UtcNow();
    repository_->PutJson(key, updated);
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
    const std::string prefix = "overlays:" + checkpoint.plane_id + ":" + checkpoint.capsule_id + ":";
    for (const auto& [key, overlay_json] : repository_->ScanJson(prefix)) {
      const int overlay_seq = overlay_json.value("overlay_event_seq", 0);
      if (overlay_seq < checkpoint.overlay_event_seq_from) {
        continue;
      }
      max_overlay_seq = std::max(max_overlay_seq, overlay_seq);
      const auto proposal = naim::knowledge::KnowledgeJsonCodec::OverlayFromJson(overlay_json);
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
      if (conflict || proposal.confidence < 0.5) {
        naim::knowledge::ReviewItem item;
        item.review_id = NewId("rev");
        item.overlay_change_id = overlay_json.value("overlay_change_id", std::string{});
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
  const auto checkpoint_json = naim::knowledge::KnowledgeJsonCodec::ToJson(checkpoint);
  repository_->PutJson(
      "replica_checkpoints:" + checkpoint.plane_id + ":" + checkpoint.capsule_id + ":" +
          checkpoint.replica_merge_id,
      checkpoint_json);
  return nlohmann::json{
      {"replica_merge_id", checkpoint.replica_merge_id},
      {"status", checkpoint.status},
      {"accepted", accepted},
      {"review", review},
      {"rejected", rejected},
      {"checkpoint", checkpoint_json},
  };
}

nlohmann::json KnowledgeStore::ReconcileDailyReplicaSchedules(const nlohmann::json& payload) {
  nlohmann::json reconciled = nlohmann::json::array();
  nlohmann::json skipped = nlohmann::json::array();
  const std::string plane_filter = payload.value("plane_id", std::string{});
  for (const auto& [key, schedule] : repository_->ScanJson("replica_schedules:")) {
    if (schedule.value("cadence", std::string("daily")) != "daily") {
      skipped.push_back(nlohmann::json{{"key", key}, {"reason", "non_daily"}});
      continue;
    }
    const std::string plane_id = schedule.value("plane_id", std::string{});
    if (!plane_filter.empty() && plane_filter != plane_id) {
      continue;
    }
    reconciled.push_back(TriggerReplicaMerge(nlohmann::json{
        {"plane_id", plane_id},
        {"capsule_id", schedule.value("capsule_id", std::string{})},
        {"reason", "daily_reconcile"},
    }));
  }
  return nlohmann::json{{"status", "completed"}, {"reconciled", reconciled}, {"skipped", skipped}};
}

nlohmann::json KnowledgeStore::ReplicaMergeStatus(const std::string& plane_id) const {
  nlohmann::json latest = nullptr;
  std::string latest_started;
  const std::string prefix =
      plane_id.empty() ? std::string("replica_checkpoints:") : "replica_checkpoints:" + plane_id + ":";
  for (const auto& [key, checkpoint] : repository_->ScanJson(prefix)) {
    if (checkpoint.value("status", std::string{}) != "completed") {
      continue;
    }
    const std::string started = checkpoint.value("started_at", std::string{});
    if (latest.is_null() || started > latest_started) {
      latest = checkpoint;
      latest_started = started;
    }
  }
  return nlohmann::json{
      {"last_successful_checkpoint", latest},
      {"status", latest.is_null() ? "skipped" : "completed"},
  };
}

nlohmann::json KnowledgeStore::ListReviewItems(const nlohmann::json& payload) const {
  const std::string status = payload.value("status", std::string("pending"));
  const int limit = std::max(1, std::min(200, payload.value("limit", 50)));
  nlohmann::json items = nlohmann::json::array();
  for (const auto& [key, item] : repository_->ScanJson("review_items:")) {
    if (item.value("status", std::string("pending")) != status) {
      continue;
    }
    items.push_back(item);
    if (static_cast<int>(items.size()) >= limit) {
      break;
    }
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
  const auto existing = repository_->GetJson("review_items:" + review_id);
  if (!existing.has_value()) {
    return nlohmann::json{{"error", "not_found"}, {"message", "review item not found"}};
  }
  auto item_json = *existing;
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
  repository_->PutJson("review_items:" + review_id, item_json);
  if (action == "accept" && item_json.contains("conflicts") && item_json.at("conflicts").is_array() &&
      !item_json.at("conflicts").empty()) {
    const auto overlay_json = item_json.at("conflicts").front();
    if (overlay_json.is_object()) {
      for (const auto& block : overlay_json.value("proposed_blocks", nlohmann::json::array())) {
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
      }
      for (const auto& relation : overlay_json.value("proposed_relations", nlohmann::json::array())) {
        if (relation.is_object()) {
          WriteRelation(relation);
        }
      }
    }
  }
  AppendEvent(
      "review." + new_status,
      {},
      {},
      nlohmann::json{{"review_id", review_id}, {"action", action}});
  return nlohmann::json{{"review_id", review_id}, {"status", new_status}, {"item", item_json}};
}

nlohmann::json KnowledgeStore::RunRepair(const nlohmann::json& payload) {
  const bool apply = payload.value("apply", false);
  const bool full_rebuild = payload.value("full_rebuild", payload.value("rebuild", false));
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
    findings.push_back(naim::knowledge::KnowledgeJsonCodec::ToJson(finding));
  };

  rocksdb::WriteBatch batch;
  if (full_rebuild) {
    for (const auto& [key, value] : repository_->ScanRaw("terms:")) {
      batch.Delete(key);
    }
  }
  for (const auto& [key, block] : repository_->ScanJson("blocks:")) {
    const std::string block_id = block.value("block_id", std::string{});
    if (block_id.empty()) {
      continue;
    }
    const auto term_hits = repository_->ScanRaw("terms:");
    const bool has_any_term = std::any_of(term_hits.begin(), term_hits.end(), [&](const auto& row) {
      return row.first.size() >= block_id.size() &&
             row.first.compare(row.first.size() - block_id.size(), block_id.size(), block_id) == 0;
    });
    if (full_rebuild || !has_any_term) {
      add_finding("warning", "stale_text_index", block_id, "", apply ? "applied" : "queued");
      if (apply || full_rebuild) {
        KnowledgeTextProcessor::IndexTerms(batch, block);
      }
    }
    for (const auto& scope_id : block.value("scope_ids", nlohmann::json::array())) {
      if (!scope_id.is_string()) {
        continue;
      }
      const std::string acl_key = "acl:" + scope_id.get<std::string>() + ":" + block_id;
      if (!repository_->GetJson(acl_key).has_value()) {
        add_finding("error", "acl_index_missing", block_id, "", apply ? "applied" : "queued");
        if (apply) {
          batch.Put(acl_key, block.dump());
        }
      }
    }
  }
  for (const auto& [key, relation] : repository_->ScanJson("edges_out:")) {
    const std::string from = relation.value("from_block_id", std::string{});
    const std::string to = relation.value("to_block_id", std::string{});
    const std::string type = relation.value("type", std::string{});
    const std::string relation_id = relation.value("relation_id", std::string{});
    if (ReadBlock(from).contains("error") || ReadBlock(to).contains("error")) {
      add_finding("error", "edge_missing_block", from, relation_id, "manual_review");
    }
    const std::string inverse_key = "edges_in:" + to + ":" + type + ":" + from + ":" + relation_id;
    if (!repository_->GetJson(inverse_key).has_value()) {
      add_finding("error", "edge_symmetry_missing", from, relation_id, apply ? "applied" : "queued");
      if (apply) {
        batch.Put(inverse_key, relation.dump());
      }
    }
  }
  for (const auto& [key, manifest] : repository_->ScanJson("capsules:")) {
    const std::string capsule_id = manifest.value("capsule_id", std::string{});
    for (const auto& item : manifest.value("included", nlohmann::json::array())) {
      const std::string block_id =
          item.is_string() ? item.get<std::string>() : item.value("block_id", std::string{});
      if (block_id.empty()) {
        continue;
      }
      if (ReadBlock(block_id).contains("error")) {
        add_finding("error", "missing_capsule_member", block_id, "", "manual_review");
      }
      const std::string member_key = "capsule_members:" + capsule_id + ":" + block_id;
      if (!repository_->GetJson(member_key).has_value()) {
        add_finding("warning", "capsule_membership_index_missing", block_id, "", apply ? "applied" : "queued");
        if (apply) {
          batch.Put(member_key, manifest.dump());
        }
      }
    }
  }
  if (apply || full_rebuild) {
    repository_->Write(batch, "apply repair batch");
  }

  const std::string report_id = NewId("rr");
  const nlohmann::json report{
      {"report_id", report_id},
      {"status", findings.empty() ? "clean" : "findings"},
      {"findings", findings},
      {"created_at", UtcNow()},
  };
  repository_->PutJson("repair_reports:" + report_id, report);
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
  const bool strict = payload.value("strict", false);
  nlohmann::json files = nlohmann::json::array();
  nlohmann::json warnings = nlohmann::json::array();
  std::map<std::string, std::string> titles;
  for (const auto& [key, block] : repository_->ScanJson("blocks:")) {
    titles[block.value("block_id", std::string{})] =
        block.value("title", block.value("knowledge_id", std::string{}));
  }
  for (const auto& [key, block] : repository_->ScanJson("blocks:")) {
    const auto scope_ids = block.value("scope_ids", nlohmann::json::array());
    if (!ScopeAllowed(scope_ids, requested_scopes)) {
      warnings.push_back(nlohmann::json{
          {"warning", "restricted_export_skipped"},
      });
      continue;
    }
    const std::string block_id = block.value("block_id", std::string{});
    const std::string title =
        block.value("title", block.value("knowledge_id", std::string("Knowledge Note")));
    std::string slug = NormalizeTerm(title);
    std::replace(slug.begin(), slug.end(), ' ', '-');
    if (slug.empty()) {
      slug = block_id;
    }
    nlohmann::json related = nlohmann::json::array();
    std::ostringstream link_section;
    for (const auto& relation : Neighbors(block_id).value("neighbors", nlohmann::json::array())) {
      const std::string other =
          relation.value("from_block_id", std::string{}) == block_id
              ? relation.value("to_block_id", std::string{})
              : relation.value("from_block_id", std::string{});
      const auto title_it = titles.find(other);
      if (title_it == titles.end()) {
        continue;
      }
      related.push_back("[[" + title_it->second + "]]");
      link_section << "- [[" << MarkdownEscape(title_it->second) << "]]";
      const std::string type = relation.value("type", std::string{});
      if (!type.empty()) {
        link_section << " (" << MarkdownEscape(type) << ")";
      }
      link_section << "\n";
    }
    std::ostringstream markdown;
    markdown << "---\n"
             << "type: " << block.value("type", std::string("knowledge")) << "\n"
             << "status: generated\n"
             << "knowledge_id: " << block.value("knowledge_id", std::string{}) << "\n"
             << "block_id: " << block_id << "\n"
             << "version_id: " << block.value("version_id", std::string{}) << "\n"
             << "content_hash: " << block.value("content_hash", std::string{}) << "\n"
             << "scope_ids: " << scope_ids.dump() << "\n"
             << "source_ids: " << block.value("source_ids", nlohmann::json::array()).dump() << "\n"
             << "aliases: " << block.value("payload", nlohmann::json::object())
                                    .value("aliases", nlohmann::json::array())
                                    .dump()
             << "\n"
             << "related: " << related.dump() << "\n"
             << "---\n\n"
             << "# " << MarkdownEscape(title) << "\n\n"
             << MarkdownEscape(block.value("body", std::string{})) << "\n";
    if (!related.empty()) {
      markdown << "\n## Related\n\n" << link_section.str();
    }
    files.push_back(nlohmann::json{{"path", slug + ".md"}, {"block_id", block_id}, {"content", markdown.str()}});
  }
  if (strict && !warnings.empty()) {
    return nlohmann::json{{"files", files}, {"warnings", warnings}, {"status", "rejected"}};
  }
  return nlohmann::json{{"files", files}, {"warnings", warnings}, {"status", "generated"}};
}

nlohmann::json KnowledgeStore::MarkdownImport(const nlohmann::json& payload) {
  const std::string plane_id = payload.value("plane_id", std::string("markdown-import"));
  const std::string capsule_id = payload.value("capsule_id", std::string("markdown-import"));
  nlohmann::json accepted_for_review = nlohmann::json::array();
  nlohmann::json rejected = nlohmann::json::array();
  for (const auto& file : payload.value("files", nlohmann::json::array())) {
    if (!file.is_object()) {
      rejected.push_back(nlohmann::json{{"reason", "malformed_file"}});
      continue;
    }
    const std::string content = file.value("content", std::string{});
    if (content.empty()) {
      rejected.push_back(nlohmann::json{{"path", file.value("path", std::string{})}, {"reason", "empty_content"}});
      continue;
    }
    nlohmann::json frontmatter = nlohmann::json::object();
    std::string body = content;
    if (content.rfind("---\n", 0) == 0) {
      const std::size_t end = content.find("\n---", 4);
      if (end != std::string::npos) {
        std::istringstream fm(content.substr(4, end - 4));
        std::string line;
        while (std::getline(fm, line)) {
          const std::size_t colon = line.find(':');
          if (colon == std::string::npos) {
            continue;
          }
          std::string key = line.substr(0, colon);
          std::string value = line.substr(colon + 1);
          value.erase(value.begin(), std::find_if(value.begin(), value.end(), [](unsigned char ch) {
            return std::isspace(ch) == 0;
          }));
          auto parsed = nlohmann::json::parse(value, nullptr, false);
          frontmatter[key] = parsed.is_discarded() ? nlohmann::json(value) : parsed;
        }
        body = content.substr(end + 4);
      }
    }
    std::string title = file.value("title", std::string{});
    if (title.empty()) {
      std::istringstream input(body);
      std::string line;
      while (std::getline(input, line)) {
        if (line.rfind("# ", 0) == 0) {
          title = line.substr(2);
          break;
        }
      }
    }
    if (title.empty()) {
      title = file.value("path", std::string("Imported Markdown"));
    }
    const std::string knowledge_id = frontmatter.value(
        "knowledge_id",
        "markdown." + naim::ComputeSha256Hex(title).substr(0, 16));
    const auto overlay = WriteOverlay(nlohmann::json{
        {"plane_id", plane_id},
        {"capsule_id", capsule_id},
        {"change_type", "markdown_import"},
        {"confidence", 0.4},
        {"proposed_blocks",
         nlohmann::json::array({nlohmann::json{
             {"knowledge_id", knowledge_id},
             {"title", title},
             {"body", body},
             {"scope_ids", file.value("scope_ids", frontmatter.value("scope_ids", nlohmann::json::array()))},
             {"source_ids", frontmatter.value(
                                "source_ids",
                                nlohmann::json::array({file.value("path", std::string{})}))},
             {"payload",
              nlohmann::json{
                  {"aliases", frontmatter.value("aliases", nlohmann::json::array())},
                  {"related", frontmatter.value("related", nlohmann::json::array())},
                  {"import_frontmatter", frontmatter},
              }},
         }})},
        {"rationale", "Imported Markdown is treated as a proposal, not canonical truth."},
    });
    accepted_for_review.push_back(overlay);
  }
  AppendEvent(
      "markdown.import.proposed",
      {},
      {},
      nlohmann::json{{"accepted", accepted_for_review.size()}, {"rejected", rejected.size()}});
  return nlohmann::json{
      {"accepted_for_review", accepted_for_review},
      {"rejected", rejected},
      {"status", "proposed"},
  };
}

nlohmann::json KnowledgeStore::GraphNeighborhood(const nlohmann::json& payload) const {
  std::vector<std::string> centers;
  if (const std::string center = payload.value("center_id", std::string{});
      !center.empty()) {
    if (repository_->GetJson("blocks:" + center).has_value()) {
      centers.push_back(center);
    } else {
      const auto head = ResolveHead(center).value("head", nlohmann::json(nullptr));
      if (head.is_object() && !head.value("head_block_id", std::string{}).empty()) {
        centers.push_back(head.value("head_block_id", std::string{}));
      }
    }
  }
  if (payload.contains("center_ids") && payload.at("center_ids").is_array()) {
    for (const auto& item : payload.at("center_ids")) {
      if (item.is_string() && !item.get<std::string>().empty()) {
        centers.push_back(item.get<std::string>());
      }
    }
  }
  const auto knowledge_ids =
      payload.contains("knowledge_ids")
          ? JsonStringArray(payload.at("knowledge_ids"))
          : std::vector<std::string>{};
  for (const auto& knowledge_id : knowledge_ids) {
    const auto head = ResolveHead(knowledge_id).value("head", nlohmann::json(nullptr));
    if (head.is_object()) {
      const std::string head_block_id = head.value("head_block_id", std::string{});
      if (!head_block_id.empty()) {
        centers.push_back(head_block_id);
        continue;
      }
    }
    for (const auto& [key, block] : repository_->ScanJson("blocks:")) {
      if (block.value("knowledge_id", std::string{}) == knowledge_id) {
        const std::string block_id = block.value("block_id", std::string{});
        if (!block_id.empty()) {
          centers.push_back(block_id);
        }
      }
    }
  }
  const int depth = std::max(1, std::min(2, payload.value("depth", 1)));
  std::set<std::string> visited;
  std::vector<std::string> frontier = std::move(centers);
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
  const std::string shard_id = payload.value("shard_id", std::string(KnowledgeStoreKeys::kDefaultShardId));
  auto object = payload;
  object["object_id"] = object_id;
  object["shard_id"] = shard_id;
  object["updated_at"] = UtcNow();
  rocksdb::WriteBatch batch;
  batch.Put("catalog_objects:" + object_id, object.dump());
  for (const auto& hint : payload.value("hints", nlohmann::json::array())) {
    const std::string term = NormalizeTerm(hint.is_string() ? hint.get<std::string>() : hint.dump());
    if (!term.empty()) {
      batch.Put("catalog_terms:" + term + ":" + object_id, object.dump());
    }
  }
  for (const auto& edge : payload.value("boundary_edges", nlohmann::json::array())) {
    if (!edge.is_object()) {
      continue;
    }
    const std::string edge_id = edge.value("edge_id", NewId("bedge"));
    auto edge_record = edge;
    edge_record["edge_id"] = edge_id;
    edge_record["updated_at"] = UtcNow();
    batch.Put("catalog_boundary_edges:" + edge_id, edge_record.dump());
  }
  if (payload.contains("shard_health") && payload.at("shard_health").is_object()) {
    auto health = payload.at("shard_health");
    health["shard_id"] = shard_id;
    health["updated_at"] = UtcNow();
    batch.Put("shard_health:" + shard_id, health.dump());
  }
  repository_->Write(batch, "write catalog");
  return nlohmann::json{{"object_id", object_id}, {"shard_id", shard_id}, {"status", "stored"}};
}

nlohmann::json KnowledgeStore::CatalogQuery(const nlohmann::json& payload) const {
  const std::string term = NormalizeTerm(payload.value("term", std::string{}));
  std::vector<std::string> requested_scopes;
  if (payload.contains("scope_ids")) {
    requested_scopes = JsonStringArray(payload.at("scope_ids"));
  }
  const int limit = std::max(1, std::min(200, payload.value("limit", 50)));
  nlohmann::json candidates = nlohmann::json::array();
  for (const auto& [key, object] : repository_->ScanJson("catalog_terms:")) {
    if (!term.empty() && key.find(term) == std::string::npos) {
      continue;
    }
    const auto scope_ids = object.value("scope_ids", nlohmann::json::array());
    if (!ScopeAllowed(scope_ids, requested_scopes)) {
      candidates.push_back(nlohmann::json{
          {"object_id", object.value("object_id", std::string{})},
          {"mode", "redacted"},
          {"redaction", nlohmann::json{{"reason", "out_of_scope"}}},
      });
    } else {
      const std::string shard_id = object.value("shard_id", std::string(KnowledgeStoreKeys::kDefaultShardId));
      const auto health = repository_->GetJson("shard_health:" + shard_id);
      candidates.push_back(nlohmann::json{
          {"object_id", object.value("object_id", std::string{})},
          {"shard_id", shard_id},
          {"scope_ids", scope_ids},
          {"stale_hint", !repository_->GetJson("catalog_objects:" + object.value("object_id", std::string{})).has_value()},
          {"shard_status", health.value_or(nlohmann::json{{"status", "unknown"}}).value("status", "unknown")},
      });
    }
    if (static_cast<int>(candidates.size()) >= limit) {
      break;
    }
  }
  return nlohmann::json{{"candidates", candidates}, {"status", "ok"}};
}

nlohmann::json KnowledgeStore::QueryRoute(const nlohmann::json& payload) const {
  const auto catalog = CatalogQuery(nlohmann::json{
      {"term", payload.value("query", payload.value("term", std::string{}))},
      {"scope_ids", payload.value("scope_ids", nlohmann::json::array())},
      {"limit", payload.value("limit", 50)},
  });
  std::map<std::string, nlohmann::json> shard_requests;
  nlohmann::json redacted = nlohmann::json::array();
  nlohmann::json warnings = nlohmann::json::array();
  for (const auto& candidate : catalog.value("candidates", nlohmann::json::array())) {
    if (candidate.value("mode", std::string{}) == "redacted") {
      redacted.push_back(candidate);
      continue;
    }
    const std::string shard_id = candidate.value("shard_id", std::string(KnowledgeStoreKeys::kDefaultShardId));
    const auto health = repository_->GetJson("shard_health:" + shard_id);
    const std::string shard_status =
        health.value_or(nlohmann::json{{"status", "unknown"}}).value("status", "unknown");
    if (shard_status == "failed" || shard_status == "offline") {
      warnings.push_back(nlohmann::json{{"shard_id", shard_id}, {"warning", "partial_failure"}});
      continue;
    }
    auto& request = shard_requests[shard_id];
    request["shard_id"] = shard_id;
    request["object_ids"].push_back(candidate.value("object_id", std::string{}));
    request["scope_ids"] = payload.value("scope_ids", nlohmann::json::array());
  }
  nlohmann::json requests = nlohmann::json::array();
  for (auto& [shard_id, request] : shard_requests) {
    requests.push_back(request);
  }
  return nlohmann::json{
      {"status", "planned"},
      {"shard_requests", requests},
      {"redacted", redacted},
      {"warnings", warnings},
      {"partial", !warnings.empty()},
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
  const auto value = repository_->GetRaw("meta:latest_event_sequence");
  if (!value.has_value()) {
    return 0;
  }
  return value->empty() ? 0 : std::stoi(*value);
}

int KnowledgeStore::LastCheckpointSequence(
    const std::string& plane_id,
    const std::string& capsule_id) const {
  int latest = 0;
  for (const auto& [key, checkpoint] :
       repository_->ScanJson("replica_checkpoints:" + plane_id + ":" + capsule_id + ":")) {
    if (checkpoint.value("status", std::string{}) != "completed") {
      continue;
    }
    latest = std::max(latest, checkpoint.value("overlay_event_seq_to", 0));
  }
  return latest;
}

int KnowledgeStore::AppendEvent(
    const std::string& type,
    const std::vector<std::string>& knowledge_ids,
    const std::vector<std::string>& block_ids,
    const nlohmann::json& payload) {
  const int sequence = LatestEventSequence() + 1;
  nlohmann::json event{
      {"sequence", sequence},
      {"event_id", NewId("evt")},
      {"type", type},
      {"scope_ids", nlohmann::json::array()},
      {"knowledge_ids", knowledge_ids},
      {"block_ids", block_ids},
      {"payload", payload},
      {"created_at", UtcNow()},
      {"created_by", "naim-knowledged"},
  };
  rocksdb::WriteBatch batch;
  batch.Put(KnowledgeStoreKeys::EventKey(sequence), event.dump());
  batch.Put("meta:latest_event_sequence", std::to_string(sequence));
  repository_->Write(batch, "append knowledge event");
  return sequence;
}

void KnowledgeStore::PersistReviewItem(const naim::knowledge::ReviewItem& item) {
  auto item_json = naim::knowledge::KnowledgeJsonCodec::ToJson(item);
  if (!item_json.contains("status")) {
    item_json["status"] = item.status.empty() ? "pending" : item.status;
  }
  repository_->PutJson("review_items:" + item.review_id, item_json);
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
    if (KnowledgeTextProcessor::JsonArrayContainsString(record_scope_ids, scope)) {
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
