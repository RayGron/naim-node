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

  const auto round_trip = naim::knowledge::BlockFromJson(naim::knowledge::ToJson(block));
  Expect(round_trip.block_id == block.block_id, "block_id should round-trip");
  Expect(round_trip.knowledge_id == block.knowledge_id, "knowledge_id should round-trip");
  Expect(round_trip.scope_ids.size() == 1, "scope_ids should round-trip");

  naim::knowledge::KnowledgeRelation relation;
  relation.relation_id = "rel_test";
  relation.from_block_id = "blk_a";
  relation.to_block_id = "blk_b";
  relation.type = "depends_on";
  const auto relation_round_trip =
      naim::knowledge::RelationFromJson(naim::knowledge::ToJson(relation));
  Expect(relation_round_trip.type == "depends_on", "relation type should round-trip");

  naim::knowledge::OverlayProposal proposal;
  proposal.overlay_change_id = "ov_test";
  proposal.plane_id = "plane";
  proposal.change_type = "claim_add";
  const auto proposal_round_trip =
      naim::knowledge::OverlayFromJson(naim::knowledge::ToJson(proposal));
  Expect(proposal_round_trip.change_type == "claim_add", "overlay should round-trip");

  std::cout << "ok: knowledge-contract-dtos-round-trip\n";
  return 0;
}
