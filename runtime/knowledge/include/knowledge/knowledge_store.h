#pragma once

#include <filesystem>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "naim/knowledge/knowledge_types.h"

namespace naim::knowledge_runtime {

class RocksDbJsonRepository;

class KnowledgeStore final {
 public:
  explicit KnowledgeStore(std::filesystem::path store_path);
  ~KnowledgeStore();

  KnowledgeStore(const KnowledgeStore&) = delete;
  KnowledgeStore& operator=(const KnowledgeStore&) = delete;

  void Open();
  nlohmann::json Status(const std::string& service_id) const;
  nlohmann::json WriteBlock(const nlohmann::json& payload);
  nlohmann::json ReadBlock(const std::string& block_id) const;
  nlohmann::json ResolveHead(const std::string& knowledge_id) const;
  nlohmann::json UpdateHead(const std::string& knowledge_id, const nlohmann::json& payload);
  nlohmann::json WriteRelation(const nlohmann::json& payload);
  nlohmann::json Neighbors(const std::string& block_id) const;
  nlohmann::json Search(const nlohmann::json& payload) const;
  nlohmann::json Context(const nlohmann::json& payload) const;
  nlohmann::json BuildCapsule(const nlohmann::json& payload);
  nlohmann::json ReadCapsule(const std::string& capsule_id) const;
  nlohmann::json IngestSource(const nlohmann::json& payload);
  nlohmann::json WriteOverlay(const nlohmann::json& payload);
  nlohmann::json ScheduleReplicaMerge(const nlohmann::json& payload);
  nlohmann::json RunScheduledReplicaMerges(const nlohmann::json& payload);
  nlohmann::json TriggerReplicaMerge(const nlohmann::json& payload);
  nlohmann::json ReplicaMergeStatus(const std::string& plane_id) const;
  nlohmann::json ListReviewItems(const nlohmann::json& payload) const;
  nlohmann::json DecideReviewItem(const std::string& review_id, const nlohmann::json& payload);
  nlohmann::json RunRepair(const nlohmann::json& payload);
  nlohmann::json MarkdownExport(const nlohmann::json& payload) const;
  nlohmann::json MarkdownImport(const nlohmann::json& payload);
  nlohmann::json GraphNeighborhood(const nlohmann::json& payload) const;
  nlohmann::json CatalogUpsert(const nlohmann::json& payload);
  nlohmann::json CatalogQuery(const nlohmann::json& payload) const;
  nlohmann::json QueryRoute(const nlohmann::json& payload) const;
  nlohmann::json ReconcileDailyReplicaSchedules(const nlohmann::json& payload);

 private:
  std::string UtcNow() const;
  std::string NewId(const std::string& prefix) const;
  int LatestEventSequence() const;
  int LastCheckpointSequence(const std::string& plane_id, const std::string& capsule_id) const;
  int AppendEvent(
      const std::string& type,
      const std::vector<std::string>& knowledge_ids,
      const std::vector<std::string>& block_ids,
      const nlohmann::json& payload);
  void PersistReviewItem(const naim::knowledge::ReviewItem& item);
  bool ScopeAllowed(
      const nlohmann::json& record_scope_ids,
      const std::vector<std::string>& requested_scope_ids) const;
  static std::vector<std::string> JsonStringArray(const nlohmann::json& value);
  static std::string JsonText(const nlohmann::json& value);
  static std::string NormalizeTerm(const std::string& value);
  static std::string MarkdownEscape(const std::string& value);

  std::unique_ptr<RocksDbJsonRepository> repository_;
};

}  // namespace naim::knowledge_runtime
