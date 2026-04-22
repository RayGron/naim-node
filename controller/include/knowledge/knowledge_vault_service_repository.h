#pragma once

#include <optional>
#include <string>

#include <nlohmann/json.hpp>
#include <sqlite3.h>

#include "knowledge/knowledge_vault_service.h"

namespace naim::controller {

class KnowledgeVaultServiceRepository final {
 public:
  std::optional<KnowledgeVaultServiceRecord> LoadService(
      const std::string& db_path,
      const std::string& service_id) const;
  void UpsertService(
      const std::string& db_path,
      const KnowledgeVaultServiceRecord& record) const;
  void UpdateServiceStatus(
      const std::string& db_path,
      const std::string& service_id,
      const nlohmann::json& status) const;

 private:
  static std::string ToText(sqlite3_stmt* statement, int column);
  static void EnsureControllerSchema(const std::string& db_path);
};

}  // namespace naim::controller
