#include "naim/knowledge/knowledge_interfaces.h"

#include <algorithm>
#include <stdexcept>

namespace naim::knowledge {

KnowledgeBlock InMemoryKnowledgeStoreFake::ReadBlock(const std::string& block_id) {
  const auto it = std::find_if(
      blocks_.begin(),
      blocks_.end(),
      [&](const auto& block) { return block.block_id == block_id; });
  if (it == blocks_.end()) {
    throw std::runtime_error("block not found");
  }
  return *it;
}

std::optional<KnowledgeHead> InMemoryKnowledgeStoreFake::ResolveHead(
    const std::string& knowledge_id) {
  const auto it = std::find_if(
      heads_.begin(),
      heads_.end(),
      [&](const auto& head) { return head.knowledge_id == knowledge_id; });
  if (it == heads_.end()) {
    return std::nullopt;
  }
  return *it;
}

void InMemoryKnowledgeStoreFake::WriteBlockBatch(const std::vector<KnowledgeBlock>& blocks) {
  for (const auto& block : blocks) {
    blocks_.push_back(block);
    KnowledgeHead head;
    head.knowledge_id = block.knowledge_id;
    head.head_block_id = block.block_id;
    head.version_id = block.version_id;
    auto existing = std::find_if(
        heads_.begin(),
        heads_.end(),
        [&](const auto& value) { return value.knowledge_id == head.knowledge_id; });
    if (existing == heads_.end()) {
      heads_.push_back(head);
    } else {
      *existing = head;
    }
  }
}

std::vector<KnowledgeRelation> InMemoryKnowledgeStoreFake::Neighbors(
    const std::string& block_id,
    const std::vector<std::string>& relation_types,
    const std::vector<std::string>& scope_ids) {
  std::vector<KnowledgeRelation> result;
  for (const auto& relation : relations_) {
    const bool touches = relation.from_block_id == block_id || relation.to_block_id == block_id;
    const bool relation_allowed =
        relation_types.empty() ||
        std::find(relation_types.begin(), relation_types.end(), relation.type) != relation_types.end();
    const bool scope_allowed =
        scope_ids.empty() ||
        std::any_of(
            relation.scope_ids.begin(),
            relation.scope_ids.end(),
            [&](const auto& scope) {
              return std::find(scope_ids.begin(), scope_ids.end(), scope) != scope_ids.end();
            });
    if (touches && relation_allowed && scope_allowed) {
      result.push_back(relation);
    }
  }
  return result;
}

}  // namespace naim::knowledge
