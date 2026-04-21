#include "naim/knowledge/knowledge_types.h"

namespace naim::knowledge {

namespace {

std::vector<std::string> StringArray(const nlohmann::json& value, const char* key) {
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

}  // namespace

nlohmann::json ToJson(const KnowledgeBlock& value) {
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

nlohmann::json ToJson(const KnowledgeHead& value) {
  return nlohmann::json{
      {"knowledge_id", value.knowledge_id},
      {"head_block_id", value.head_block_id},
      {"version_id", value.version_id},
      {"updated_at", value.updated_at},
      {"event_id", value.event_id},
      {"content_hash", value.content_hash},
  };
}

nlohmann::json ToJson(const KnowledgeRelation& value) {
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

nlohmann::json ToJson(const KnowledgeEvent& value) {
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

nlohmann::json ToJson(const CapsuleManifest& value) {
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

nlohmann::json ToJson(const OverlayProposal& value) {
  return nlohmann::json{
      {"overlay_change_id", value.overlay_change_id},
      {"plane_id", value.plane_id},
      {"capsule_id", value.capsule_id},
      {"base_event_seq", value.base_event_seq},
      {"base_versions", value.base_versions},
      {"change_type", value.change_type},
      {"proposed_blocks", value.proposed_blocks},
      {"proposed_relations", value.proposed_relations},
      {"rationale", value.rationale},
      {"created_at", value.created_at},
      {"created_by", value.created_by},
  };
}

nlohmann::json ToJson(const ReplicaMergeCheckpoint& value) {
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

KnowledgeBlock BlockFromJson(const nlohmann::json& value) {
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

KnowledgeRelation RelationFromJson(const nlohmann::json& value) {
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

OverlayProposal OverlayFromJson(const nlohmann::json& value) {
  OverlayProposal proposal;
  proposal.overlay_change_id = value.value("overlay_change_id", std::string{});
  proposal.plane_id = value.value("plane_id", std::string{});
  proposal.capsule_id = value.value("capsule_id", std::string{});
  proposal.base_event_seq = value.value("base_event_seq", 0);
  proposal.base_versions = value.value("base_versions", nlohmann::json::object());
  proposal.change_type = value.value("change_type", std::string{});
  proposal.proposed_blocks = value.value("proposed_blocks", nlohmann::json::array());
  proposal.proposed_relations = value.value("proposed_relations", nlohmann::json::array());
  proposal.rationale = value.value("rationale", std::string{});
  proposal.created_at = value.value("created_at", std::string{});
  proposal.created_by = value.value("created_by", std::string{});
  return proposal;
}

}  // namespace naim::knowledge
