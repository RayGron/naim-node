#pragma once

#include <filesystem>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include <nlohmann/json.hpp>

namespace rocksdb {
class DB;
class Status;
class WriteBatch;
}

namespace naim::knowledge_runtime {

class RocksDbJsonRepository final {
 public:
  explicit RocksDbJsonRepository(std::filesystem::path store_path);
  ~RocksDbJsonRepository();

  RocksDbJsonRepository(const RocksDbJsonRepository&) = delete;
  RocksDbJsonRepository& operator=(const RocksDbJsonRepository&) = delete;

  void Open();
  const std::filesystem::path& path() const { return store_path_; }
  std::optional<std::string> GetRaw(const std::string& key) const;
  std::optional<nlohmann::json> GetJson(const std::string& key) const;
  void PutJson(const std::string& key, const nlohmann::json& value);
  void Write(rocksdb::WriteBatch& batch, const std::string& action);
  std::vector<std::pair<std::string, std::string>> ScanRaw(const std::string& prefix) const;
  std::vector<std::pair<std::string, nlohmann::json>> ScanJson(const std::string& prefix) const;

 private:
  static void CheckStatus(const rocksdb::Status& status, const std::string& action);
  static nlohmann::json ParseJsonOr(const std::string& text, nlohmann::json fallback);
  static bool HasPrefix(const std::string& value, const std::string& prefix);

  std::filesystem::path store_path_;
  std::unique_ptr<rocksdb::DB> db_;
};

}  // namespace naim::knowledge_runtime
