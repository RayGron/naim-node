#pragma once

#include <string>
#include <vector>

#include <nlohmann/json.hpp>

namespace rocksdb {
class WriteBatch;
}

namespace naim::knowledge_runtime {

class KnowledgeTextProcessor final {
 public:
  static bool JsonArrayContainsString(
      const nlohmann::json& values,
      const std::string& expected);
  static std::string Lowercase(std::string value);
  static void IndexTerms(rocksdb::WriteBatch& batch, const nlohmann::json& block);
  static std::vector<nlohmann::json> ChunkSource(
      const std::string& source_key,
      const std::string& block_id,
      const std::string& content);
  static std::vector<nlohmann::json> ExtractClaims(
      const std::string& source_key,
      const std::string& block_id,
      const std::vector<nlohmann::json>& chunks);
};

}  // namespace naim::knowledge_runtime
