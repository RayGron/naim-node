#include "naim/knowledge/knowledge_types.h"

namespace naim::knowledge {

std::vector<std::string> KnowledgeJsonCodec::StringArray(const nlohmann::json& value, const char* key) {
  if (!value.contains(key) || !value.at(key).is_array()) {
    return {};
  }
  std::vector<std::string> result;
  for (const auto& item : value.at(key)) {
    if (item.is_string()) {
      result.push_back(item.get<std::string>());
    }
  }
  return result;
}

nlohmann::json KnowledgeJsonCodec::ToJson(const KnowledgeBlock& value) {
  return nlohmann::json{
      {"block_id", value.block_id},
      {"knowledge_id", value.knowledge_id},
      {"version_id", value.version_id},
      {"type", value.type},
      {"title", value.title},
      {"body", value.body},
      {"payload", value.payload},
      {"content_hash", value.content_hash},
      {"created_at", value.created_at},
      {"created_by", value.created_by},
      {"source_ids", value.source_ids},
      {"scope_ids", value.scope_ids},
      {"confidence", value.confidence},
  };
}

nlohmann::json KnowledgeJsonCodec::ToJson(const KnowledgeHead& value) {
  return nlohmann::json{
      {"knowledge_id", value.knowledge_id},
      {"head_block_id", value.head_block_id},
      {"version_id", value.version_id},
      {"updated_at", value.updated_at},
      {"event_id", value.event_id},
      {"content_hash", value.content_hash},
  };
}

nlohmann::json KnowledgeJsonCodec::ToJson(const KnowledgeRelation& value) {
  return nlohmann::json{
      {"relation_id", value.relation_id},
      {"from_block_id", value.from_block_id},
      {"to_block_id", value.to_block_id},
      {"type", value.type},
      {"confidence", value.confidence},
      {"scope_ids", value.scope_ids},
      {"created_at", value.created_at},
      {"created_by", value.created_by},
  };
}

nlohmann::json KnowledgeJsonCodec::ToJson(const KnowledgeEvent& value) {
  return nlohmann::json{
      {"event_id", value.event_id},
      {"sequence", value.sequence},
      {"type", value.type},
      {"scope_ids", value.scope_ids},
      {"knowledge_ids", value.knowledge_ids},
      {"block_ids", value.block_ids},
      {"payload", value.payload},
      {"created_at", value.created_at},
      {"created_by", value.created_by},
  };
}

nlohmann::json KnowledgeJsonCodec::ToJson(const CapsuleManifest& value) {
  return nlohmann::json{
      {"capsule_id", value.capsule_id},
      {"plane_id", value.plane_id},
      {"schema_version", value.schema_version},
      {"storage_engine", value.storage_engine},
      {"storage_profile", value.storage_profile},
      {"base_event_seq", value.base_event_seq},
      {"created_at", value.created_at},
      {"included", value.included},
      {"policy", value.policy},
      {"indexes", value.indexes},
  };
}

nlohmann::json KnowledgeJsonCodec::ToJson(const OverlayProposal& value) {
  return nlohmann::json{
      {"overlay_change_id", value.overlay_change_id},
      {"plane_id", value.plane_id},
      {"capsule_id", value.capsule_id},
      {"base_event_seq", value.base_event_seq},
      {"base_versions", value.base_versions},
      {"change_type", value.change_type},
      {"proposed_blocks", value.proposed_blocks},
      {"proposed_relations", value.proposed_relations},
      {"confidence", value.confidence},
      {"rationale", value.rationale},
      {"created_at", value.created_at},
      {"created_by", value.created_by},
  };
}

nlohmann::json KnowledgeJsonCodec::ToJson(const ReplicaMergeCheckpoint& value) {
  return nlohmann::json{
      {"replica_merge_id", value.replica_merge_id},
      {"plane_id", value.plane_id},
      {"capsule_id", value.capsule_id},
      {"base_event_seq", value.base_event_seq},
      {"overlay_event_seq_from", value.overlay_event_seq_from},
      {"overlay_event_seq_to", value.overlay_event_seq_to},
      {"canonical_event_seq_before", value.canonical_event_seq_before},
      {"canonical_event_seq_after", value.canonical_event_seq_after},
      {"status", value.status},
      {"merge_policy_version", value.merge_policy_version},
      {"started_at", value.started_at},
      {"completed_at", value.completed_at.empty() ? nlohmann::json(nullptr) : nlohmann::json(value.completed_at)},
  };
}

nlohmann::json KnowledgeJsonCodec::ToJson(const KnowledgeVaultPlacement& value) {
  return nlohmann::json{
      {"service_id", value.service_id},
      {"node_name", value.node_name},
      {"storage_root", value.storage_root},
      {"image", value.image},
      {"endpoint", value.endpoint},
  };
}

nlohmann::json KnowledgeJsonCodec::ToJson(const KnowledgeVaultStatus& value) {
  return nlohmann::json{
      {"service_id", value.service_id},
      {"status", value.status},
      {"storage_node", value.storage_node},
      {"endpoint", value.endpoint},
      {"store_profile", value.store_profile},
      {"schema_version", value.schema_version},
      {"index_epoch", value.index_epoch},
      {"latest_event_sequence", value.latest_event_sequence},
      {"checkpoints", value.checkpoints},
  };
}

nlohmann::json KnowledgeJsonCodec::ToJson(const ContextRequest& value) {
  return nlohmann::json{
      {"plane_id", value.plane_id},
      {"scope_id", value.scope_id},
      {"request_id", value.request_id},
      {"mode", value.mode},
      {"token_budget", value.token_budget},
      {"freshness", value.freshness},
      {"query", value.query},
      {"include_graph", value.include_graph},
      {"max_graph_depth", value.max_graph_depth},
  };
}

nlohmann::json KnowledgeJsonCodec::ToJson(const ContextBundle& value) {
  return nlohmann::json{
      {"request_id", value.request_id},
      {"context", value.context},
      {"redacted", value.redacted},
      {"warnings", value.warnings},
  };
}

nlohmann::json KnowledgeJsonCodec::ToJson(const SourceIngestRequest& value) {
  return nlohmann::json{
      {"source_kind", value.source_kind},
      {"source_ref", value.source_ref},
      {"content", value.content},
      {"content_hash", value.content_hash},
      {"scope_ids", value.scope_ids},
      {"metadata", value.metadata},
  };
}

nlohmann::json KnowledgeJsonCodec::ToJson(const SourceIngestResult& value) {
  return nlohmann::json{
      {"source_event_id", value.source_event_id},
      {"source_block_id", value.source_block_id},
      {"status", value.status},
      {"reason", value.reason.empty() ? nlohmann::json(nullptr) : nlohmann::json(value.reason)},
  };
}

nlohmann::json KnowledgeJsonCodec::ToJson(const ReviewItem& value) {
  return nlohmann::json{
      {"review_id", value.review_id},
      {"overlay_change_id", value.overlay_change_id},
      {"knowledge_id", value.knowledge_id},
      {"type", value.type},
      {"status", value.status},
      {"created_at", value.created_at},
      {"safe_summary", value.safe_summary},
      {"affected_scopes", value.affected_scopes},
      {"evidence", value.evidence},
      {"conflicts", value.conflicts},
  };
}

nlohmann::json KnowledgeJsonCodec::ToJson(const RepairFinding& value) {
  return nlohmann::json{
      {"finding_id", value.finding_id},
      {"severity", value.severity},
      {"type", value.type},
      {"shard_id", value.shard_id},
      {"block_id", value.block_id.empty() ? nlohmann::json(nullptr) : nlohmann::json(value.block_id)},
      {"relation_id", value.relation_id.empty() ? nlohmann::json(nullptr) : nlohmann::json(value.relation_id)},
      {"event_seq", value.event_seq},
      {"repair_action", value.repair_action},
      {"created_at", value.created_at},
  };
}

KnowledgeBlock KnowledgeJsonCodec::BlockFromJson(const nlohmann::json& value) {
  KnowledgeBlock block;
  block.block_id = value.value("block_id", std::string{});
  block.knowledge_id = value.value("knowledge_id", std::string{});
  block.version_id = value.value("version_id", std::string{});
  block.type = value.value("type", std::string("note"));
  block.title = value.value("title", std::string{});
  block.body = value.value("body", std::string{});
  block.payload = value.value("payload", nlohmann::json::object());
  block.content_hash = value.value("content_hash", std::string{});
  block.created_at = value.value("created_at", std::string{});
  block.created_by = value.value("created_by", std::string{});
  block.source_ids = StringArray(value, "source_ids");
  block.scope_ids = StringArray(value, "scope_ids");
  block.confidence = value.value("confidence", 1.0);
  return block;
}

KnowledgeRelation KnowledgeJsonCodec::RelationFromJson(const nlohmann::json& value) {
  KnowledgeRelation relation;
  relation.relation_id = value.value("relation_id", std::string{});
  relation.from_block_id = value.value("from_block_id", std::string{});
  relation.to_block_id = value.value("to_block_id", std::string{});
  relation.type = value.value("type", std::string("related"));
  relation.confidence = value.value("confidence", 1.0);
  relation.scope_ids = StringArray(value, "scope_ids");
  relation.created_at = value.value("created_at", std::string{});
  relation.created_by = value.value("created_by", std::string{});
  return relation;
}

OverlayProposal KnowledgeJsonCodec::OverlayFromJson(const nlohmann::json& value) {
  OverlayProposal proposal;
  proposal.overlay_change_id = value.value("overlay_change_id", std::string{});
  proposal.plane_id = value.value("plane_id", std::string{});
  proposal.capsule_id = value.value("capsule_id", std::string{});
  proposal.base_event_seq = value.value("base_event_seq", 0);
  proposal.base_versions = value.value("base_versions", nlohmann::json::object());
  proposal.change_type = value.value("change_type", std::string{});
  proposal.proposed_blocks = value.value("proposed_blocks", nlohmann::json::array());
  proposal.proposed_relations = value.value("proposed_relations", nlohmann::json::array());
  proposal.confidence = value.value("confidence", 1.0);
  proposal.rationale = value.value("rationale", std::string{});
  proposal.created_at = value.value("created_at", std::string{});
  proposal.created_by = value.value("created_by", std::string{});
  return proposal;
}

ContextRequest KnowledgeJsonCodec::ContextRequestFromJson(const nlohmann::json& value) {
  ContextRequest request;
  request.plane_id = value.value("plane_id", std::string{});
  request.scope_id = value.value("scope_id", std::string{});
  request.request_id = value.value("request_id", std::string{});
  request.mode = value.value("mode", std::string("normal"));
  request.token_budget = value.value("token_budget", 12000);
  request.freshness = value.value("freshness", std::string("current"));
  request.query = value.value("query", std::string{});
  request.include_graph = value.value("include_graph", true);
  request.max_graph_depth = value.value("max_graph_depth", 1);
  return request;
}

SourceIngestRequest KnowledgeJsonCodec::SourceIngestRequestFromJson(const nlohmann::json& value) {
  SourceIngestRequest request;
  request.source_kind = value.value("source_kind", std::string{});
  request.source_ref = value.value("source_ref", std::string{});
  request.content = value.value("content", std::string{});
  request.content_hash = value.value("content_hash", std::string{});
  request.scope_ids = StringArray(value, "scope_ids");
  request.metadata = value.value("metadata", nlohmann::json::object());
  return request;
}

ReviewItem KnowledgeJsonCodec::ReviewItemFromJson(const nlohmann::json& value) {
  ReviewItem item;
  item.review_id = value.value("review_id", std::string{});
  item.overlay_change_id = value.value("overlay_change_id", std::string{});
  item.knowledge_id = value.value("knowledge_id", std::string{});
  item.type = value.value("type", std::string{});
  item.status = value.value("status", std::string("pending"));
  item.created_at = value.value("created_at", std::string{});
  item.safe_summary = value.value("safe_summary", std::string{});
  item.affected_scopes = StringArray(value, "affected_scopes");
  item.evidence = value.value("evidence", nlohmann::json::array());
  item.conflicts = value.value("conflicts", nlohmann::json::array());
  return item;
}

RepairFinding KnowledgeJsonCodec::RepairFindingFromJson(const nlohmann::json& value) {
  RepairFinding finding;
  finding.finding_id = value.value("finding_id", std::string{});
  finding.severity = value.value("severity", std::string("info"));
  finding.type = value.value("type", std::string{});
  finding.shard_id = value.value("shard_id", std::string("kv_default"));
  if (value.contains("block_id") && value.at("block_id").is_string()) {
    finding.block_id = value.at("block_id").get<std::string>();
  }
  if (value.contains("relation_id") && value.at("relation_id").is_string()) {
    finding.relation_id = value.at("relation_id").get<std::string>();
  }
  finding.event_seq = value.value("event_seq", 0);
  finding.repair_action = value.value("repair_action", std::string("none"));
  finding.created_at = value.value("created_at", std::string{});
  return finding;
}

}  // namespace naim::knowledge
