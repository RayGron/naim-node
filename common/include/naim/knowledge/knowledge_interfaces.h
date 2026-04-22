#pragma once

#include <optional>
#include <string>
#include <vector>

#include "naim/knowledge/knowledge_types.h"

namespace naim::knowledge {

class IKnowledgeVaultRuntime {
 public:
  virtual ~IKnowledgeVaultRuntime() = default;
  virtual KnowledgeVaultStatus Status() = 0;
  virtual void Start(const KnowledgeVaultPlacement& placement) = 0;
  virtual void Stop(const std::string& service_id) = 0;
  virtual ReplicaMergeCheckpoint CreateCheckpoint(const ReplicaMergeCheckpoint& request) = 0;
};

class IKnowledgeStore {
 public:
  virtual ~IKnowledgeStore() = default;
  virtual KnowledgeBlock ReadBlock(const std::string& block_id) = 0;
  virtual std::optional<KnowledgeHead> ResolveHead(const std::string& knowledge_id) = 0;
  virtual void WriteBlockBatch(const std::vector<KnowledgeBlock>& blocks) = 0;
  virtual std::vector<KnowledgeRelation> Neighbors(
      const std::string& block_id,
      const std::vector<std::string>& relation_types,
      const std::vector<std::string>& scope_ids) = 0;
};

class IKnowledgeEventLog {
 public:
  virtual ~IKnowledgeEventLog() = default;
  virtual std::string Append(const KnowledgeEvent& event) = 0;
  virtual std::vector<KnowledgeEvent> ReadAfter(int sequence, std::size_t limit) = 0;
  virtual void CommitConsumer(const std::string& consumer_id, int sequence) = 0;
};

class IKnowledgeTextIndex {
 public:
  virtual ~IKnowledgeTextIndex() = default;
  virtual std::vector<nlohmann::json> SearchText(const nlohmann::json& request) = 0;
  virtual void ApplyIndexDelta(const nlohmann::json& delta) = 0;
};

class IKnowledgeVectorIndex {
 public:
  virtual ~IKnowledgeVectorIndex() = default;
  virtual std::vector<nlohmann::json> SearchVector(const nlohmann::json& request) = 0;
  virtual void Rebuild(const nlohmann::json& input) = 0;
};

class IKnowledgeCapsuleStore {
 public:
  virtual ~IKnowledgeCapsuleStore() = default;
  virtual CapsuleManifest ReadManifest(const std::string& capsule_id) = 0;
  virtual CapsuleManifest BuildCapsule(const nlohmann::json& request) = 0;
};

class IKnowledgeQueryRouter {
 public:
  virtual ~IKnowledgeQueryRouter() = default;
  virtual ContextBundle BuildContext(const ContextRequest& request) = 0;
  virtual nlohmann::json Search(const nlohmann::json& request) = 0;
  virtual nlohmann::json WriteOverlay(const OverlayProposal& request) = 0;
};

class IKnowledgeOverlayStore {
 public:
  virtual ~IKnowledgeOverlayStore() = default;
  virtual nlohmann::json WriteOverlay(const OverlayProposal& proposal) = 0;
  virtual std::vector<OverlayProposal> ReadPending(
      const std::string& plane_id,
      const std::string& capsule_id,
      int after_sequence) = 0;
};

class IKnowledgeReplicaMergeScheduler {
 public:
  virtual ~IKnowledgeReplicaMergeScheduler() = default;
  virtual std::string ScheduleDaily(const std::string& plane_id, const std::string& capsule_id) = 0;
  virtual ReplicaMergeCheckpoint TriggerNow(const ReplicaMergeCheckpoint& request) = 0;
  virtual std::optional<ReplicaMergeCheckpoint> LastSuccessfulCheckpoint(
      const std::string& plane_id) = 0;
};

class InMemoryKnowledgeStoreFake final : public IKnowledgeStore {
 public:
  KnowledgeBlock ReadBlock(const std::string& block_id) override;
  std::optional<KnowledgeHead> ResolveHead(const std::string& knowledge_id) override;
  void WriteBlockBatch(const std::vector<KnowledgeBlock>& blocks) override;
  std::vector<KnowledgeRelation> Neighbors(
      const std::string& block_id,
      const std::vector<std::string>& relation_types,
      const std::vector<std::string>& scope_ids) override;

 private:
  std::vector<KnowledgeBlock> blocks_;
  std::vector<KnowledgeRelation> relations_;
  std::vector<KnowledgeHead> heads_;
};

}  // namespace naim::knowledge
