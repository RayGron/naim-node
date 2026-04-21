#pragma once

#include <optional>
#include <string>
#include <vector>

#include <nlohmann/json.hpp>

namespace naim::knowledge {

struct KnowledgeBlock {
  std::string block_id;
  std::string knowledge_id;
  std::string version_id;
  std::string type = "note";
  std::string title;
  std::string body;
  nlohmann::json payload = nlohmann::json::object();
  std::string content_hash;
  std::string created_at;
  std::string created_by;
  std::vector<std::string> source_ids;
  std::vector<std::string> scope_ids;
  double confidence = 1.0;
};

struct KnowledgeHead {
  std::string knowledge_id;
  std::string head_block_id;
  std::string version_id;
  std::string updated_at;
  std::string event_id;
  std::string content_hash;
};

struct KnowledgeRelation {
  std::string relation_id;
  std::string from_block_id;
  std::string to_block_id;
  std::string type = "related";
  double confidence = 1.0;
  std::vector<std::string> scope_ids;
  std::string created_at;
  std::string created_by;
};

struct KnowledgeEvent {
  std::string event_id;
  int sequence = 0;
  std::string type;
  std::vector<std::string> scope_ids;
  std::vector<std::string> knowledge_ids;
  std::vector<std::string> block_ids;
  nlohmann::json payload = nlohmann::json::object();
  std::string created_at;
  std::string created_by;
};

struct CapsuleManifest {
  std::string capsule_id;
  std::string plane_id;
  int schema_version = 1;
  std::string storage_engine = "sqlite";
  std::string storage_profile = "naim-knowledge-capsule-v1";
  int base_event_seq = 0;
  std::string created_at;
  nlohmann::json included = nlohmann::json::array();
  nlohmann::json policy = nlohmann::json::object();
  nlohmann::json indexes = nlohmann::json::object();
};

struct OverlayProposal {
  std::string overlay_change_id;
  std::string plane_id;
  std::string capsule_id;
  int base_event_seq = 0;
  nlohmann::json base_versions = nlohmann::json::object();
  std::string change_type;
  nlohmann::json proposed_blocks = nlohmann::json::array();
  nlohmann::json proposed_relations = nlohmann::json::array();
  std::string rationale;
  std::string created_at;
  std::string created_by;
};

struct ReplicaMergeCheckpoint {
  std::string replica_merge_id;
  std::string plane_id;
  std::string capsule_id;
  int base_event_seq = 0;
  int overlay_event_seq_from = 0;
  int overlay_event_seq_to = 0;
  int canonical_event_seq_before = 0;
  int canonical_event_seq_after = 0;
  std::string status = "started";
  std::string merge_policy_version = "v0";
  std::string started_at;
  std::string completed_at;
};

nlohmann::json ToJson(const KnowledgeBlock& value);
nlohmann::json ToJson(const KnowledgeHead& value);
nlohmann::json ToJson(const KnowledgeRelation& value);
nlohmann::json ToJson(const KnowledgeEvent& value);
nlohmann::json ToJson(const CapsuleManifest& value);
nlohmann::json ToJson(const OverlayProposal& value);
nlohmann::json ToJson(const ReplicaMergeCheckpoint& value);

KnowledgeBlock BlockFromJson(const nlohmann::json& value);
KnowledgeRelation RelationFromJson(const nlohmann::json& value);
OverlayProposal OverlayFromJson(const nlohmann::json& value);

}  // namespace naim::knowledge
