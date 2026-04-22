#include "naim/knowledge/knowledge_interfaces.h"
#include "naim/knowledge/knowledge_types.h"

#include <iostream>
#include <stdexcept>

namespace {

void Expect(bool condition, const std::string& message) {
  if (!condition) {
    throw std::runtime_error(message);
  }
}

}  // namespace

int main() {
  naim::knowledge::KnowledgeBlock block;
  block.block_id = "blk_test";
  block.knowledge_id = "knowledge.test";
  block.version_id = "v1";
  block.title = "Test";
  block.body = "Body";
  block.scope_ids = {"scope.default"};

  const auto round_trip = naim::knowledge::KnowledgeJsonCodec::BlockFromJson(naim::knowledge::KnowledgeJsonCodec::ToJson(block));
  Expect(round_trip.block_id == block.block_id, "block_id should round-trip");
  Expect(round_trip.knowledge_id == block.knowledge_id, "knowledge_id should round-trip");
  Expect(round_trip.scope_ids.size() == 1, "scope_ids should round-trip");

  naim::knowledge::KnowledgeRelation relation;
  relation.relation_id = "rel_test";
  relation.from_block_id = "blk_a";
  relation.to_block_id = "blk_b";
  relation.type = "depends_on";
  const auto relation_round_trip =
      naim::knowledge::KnowledgeJsonCodec::RelationFromJson(naim::knowledge::KnowledgeJsonCodec::ToJson(relation));
  Expect(relation_round_trip.type == "depends_on", "relation type should round-trip");

  naim::knowledge::OverlayProposal proposal;
  proposal.overlay_change_id = "ov_test";
  proposal.plane_id = "plane";
  proposal.change_type = "claim_add";
  const auto proposal_round_trip =
      naim::knowledge::KnowledgeJsonCodec::OverlayFromJson(naim::knowledge::KnowledgeJsonCodec::ToJson(proposal));
  Expect(proposal_round_trip.change_type == "claim_add", "overlay should round-trip");

  naim::knowledge::ContextRequest context_request;
  context_request.plane_id = "plane";
  context_request.scope_id = "scope.default";
  context_request.request_id = "req_test";
  context_request.query = "knowledge";
  const auto context_round_trip =
      naim::knowledge::KnowledgeJsonCodec::ContextRequestFromJson(naim::knowledge::KnowledgeJsonCodec::ToJson(context_request));
  Expect(context_round_trip.scope_id == "scope.default", "context request should round-trip");

  naim::knowledge::SourceIngestRequest ingest_request;
  ingest_request.source_kind = "document";
  ingest_request.source_ref = "doc://test";
  ingest_request.content_hash = "hash";
  ingest_request.scope_ids = {"scope.default"};
  const auto ingest_round_trip =
      naim::knowledge::KnowledgeJsonCodec::SourceIngestRequestFromJson(naim::knowledge::KnowledgeJsonCodec::ToJson(ingest_request));
  Expect(ingest_round_trip.source_kind == "document", "source ingest should round-trip");

  naim::knowledge::KnowledgeVaultPlacement placement;
  placement.service_id = "kv_default";
  placement.node_name = "storage1";
  const auto placement_json = naim::knowledge::KnowledgeJsonCodec::ToJson(placement);
  Expect(placement_json.value("node_name", std::string{}) == "storage1", "placement should serialize");

  naim::knowledge::KnowledgeVaultStatus status;
  status.status = "ready";
  status.schema_version = "knowledge.v1";
  status.latest_event_sequence = 7;
  const auto status_json = naim::knowledge::KnowledgeJsonCodec::ToJson(status);
  Expect(status_json.value("schema_version", std::string{}) == "knowledge.v1", "status should serialize");

  naim::knowledge::RepairFinding finding;
  finding.finding_id = "repair_test";
  finding.type = "stale_text_index";
  const auto finding_round_trip =
      naim::knowledge::KnowledgeJsonCodec::RepairFindingFromJson(naim::knowledge::KnowledgeJsonCodec::ToJson(finding));
  Expect(finding_round_trip.type == "stale_text_index", "repair finding should round-trip");

  naim::knowledge::InMemoryKnowledgeStoreFake fake_store;
  fake_store.WriteBlockBatch({block});
  const auto fake_block = fake_store.ReadBlock("blk_test");
  Expect(fake_block.knowledge_id == "knowledge.test", "fake store should expose interface behavior");

  std::cout << "ok: knowledge-contract-dtos-round-trip\n";
  return 0;
}
