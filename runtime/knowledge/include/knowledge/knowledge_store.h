#pragma once

#include <filesystem>
#include <optional>
#include <string>
#include <vector>

#include <sqlite3.h>

#include "naim/knowledge/knowledge_types.h"

namespace naim::knowledge_runtime {

class KnowledgeStore final {
 public:
  explicit KnowledgeStore(std::filesystem::path db_path);
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
  nlohmann::json BuildCapsule(const nlohmann::json& payload);
  nlohmann::json WriteOverlay(const nlohmann::json& payload);
  nlohmann::json TriggerReplicaMerge(const nlohmann::json& payload);
  nlohmann::json ReplicaMergeStatus(const std::string& plane_id) const;

 private:
  std::string UtcNow() const;
  std::string NewId(const std::string& prefix) const;
  int LatestEventSequence() const;
  int AppendEvent(
      const std::string& type,
      const std::vector<std::string>& knowledge_ids,
      const std::vector<std::string>& block_ids,
      const nlohmann::json& payload);
  static std::string JsonText(const nlohmann::json& value);

  std::filesystem::path db_path_;
  sqlite3* db_ = nullptr;
};

}  // namespace naim::knowledge_runtime
