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
  double confidence = 1.0;
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

struct KnowledgeVaultPlacement {
  std::string service_id = "kv_default";
  std::string node_name;
  std::string storage_root;
  std::string image;
  std::string endpoint;
};

struct KnowledgeVaultStatus {
  std::string service_id = "kv_default";
  std::string status = "stopped";
  std::string storage_node;
  std::string endpoint;
  std::string store_profile;
  std::string schema_version;
  std::string index_epoch;
  int latest_event_sequence = 0;
  nlohmann::json checkpoints = nlohmann::json::object();
};

struct ContextRequest {
  std::string plane_id;
  std::string scope_id;
  std::string request_id;
  std::string mode = "normal";
  int token_budget = 12000;
  std::string freshness = "current";
  std::string query;
  bool include_graph = true;
  int max_graph_depth = 1;
};

struct ContextBundle {
  std::string request_id;
  std::vector<nlohmann::json> context;
  std::vector<nlohmann::json> redacted;
  std::vector<std::string> warnings;
};

struct SourceIngestRequest {
  std::string source_kind;
  std::string source_ref;
  std::string content;
  std::string content_hash;
  std::vector<std::string> scope_ids;
  nlohmann::json metadata = nlohmann::json::object();
};

struct SourceIngestResult {
  std::string source_event_id;
  std::string source_block_id;
  std::string status;
  std::string reason;
};

struct ReviewItem {
  std::string review_id;
  std::string overlay_change_id;
  std::string knowledge_id;
  std::string type;
  std::string status = "pending";
  std::string created_at;
  std::string safe_summary;
  std::vector<std::string> affected_scopes;
  nlohmann::json evidence = nlohmann::json::array();
  nlohmann::json conflicts = nlohmann::json::array();
};

struct RepairFinding {
  std::string finding_id;
  std::string severity = "info";
  std::string type;
  std::string shard_id = "kv_default";
  std::string block_id;
  std::string relation_id;
  int event_seq = 0;
  std::string repair_action = "none";
  std::string created_at;
};

nlohmann::json ToJson(const KnowledgeBlock& value);
nlohmann::json ToJson(const KnowledgeHead& value);
nlohmann::json ToJson(const KnowledgeRelation& value);
nlohmann::json ToJson(const KnowledgeEvent& value);
nlohmann::json ToJson(const CapsuleManifest& value);
nlohmann::json ToJson(const OverlayProposal& value);
nlohmann::json ToJson(const ReplicaMergeCheckpoint& value);
nlohmann::json ToJson(const KnowledgeVaultPlacement& value);
nlohmann::json ToJson(const KnowledgeVaultStatus& value);
nlohmann::json ToJson(const ContextRequest& value);
nlohmann::json ToJson(const ContextBundle& value);
nlohmann::json ToJson(const SourceIngestRequest& value);
nlohmann::json ToJson(const SourceIngestResult& value);
nlohmann::json ToJson(const ReviewItem& value);
nlohmann::json ToJson(const RepairFinding& value);

KnowledgeBlock BlockFromJson(const nlohmann::json& value);
KnowledgeRelation RelationFromJson(const nlohmann::json& value);
OverlayProposal OverlayFromJson(const nlohmann::json& value);
ContextRequest ContextRequestFromJson(const nlohmann::json& value);
SourceIngestRequest SourceIngestRequestFromJson(const nlohmann::json& value);
ReviewItem ReviewItemFromJson(const nlohmann::json& value);
RepairFinding RepairFindingFromJson(const nlohmann::json& value);

}  // namespace naim::knowledge
