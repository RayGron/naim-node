#include "knowledge/rocksdb_json_repository.h"

#include <stdexcept>

#include <rocksdb/db.h>
#include <rocksdb/iterator.h>
#include <rocksdb/options.h>
#include <rocksdb/write_batch.h>

namespace naim::knowledge_runtime {

RocksDbJsonRepository::RocksDbJsonRepository(std::filesystem::path store_path)
    : store_path_(std::move(store_path)) {}

RocksDbJsonRepository::~RocksDbJsonRepository() = default;

void RocksDbJsonRepository::Open() {
  std::filesystem::create_directories(store_path_);
  rocksdb::Options options;
  options.create_if_missing = true;
  options.IncreaseParallelism();
  options.OptimizeLevelStyleCompaction();
  rocksdb::DB* raw = nullptr;
  CheckStatus(
      rocksdb::DB::Open(options, store_path_.string(), &raw),
      "open rocksdb knowledge store");
  db_.reset(raw);
}

std::optional<std::string> RocksDbJsonRepository::GetRaw(const std::string& key) const {
  std::string value;
  const auto status = db_->Get(rocksdb::ReadOptions{}, key, &value);
  if (status.IsNotFound()) {
    return std::nullopt;
  }
  CheckStatus(status, "rocksdb get " + key);
  return value;
}

std::optional<nlohmann::json> RocksDbJsonRepository::GetJson(const std::string& key) const {
  const auto value = GetRaw(key);
  if (!value.has_value()) {
    return std::nullopt;
  }
  return ParseJsonOr(*value, nlohmann::json::object());
}

void RocksDbJsonRepository::PutJson(
    const std::string& key,
    const nlohmann::json& value) {
  CheckStatus(db_->Put(rocksdb::WriteOptions{}, key, value.dump()), "rocksdb put " + key);
}

void RocksDbJsonRepository::Write(
    rocksdb::WriteBatch& batch,
    const std::string& action) {
  CheckStatus(db_->Write(rocksdb::WriteOptions{}, &batch), action);
}

std::vector<std::pair<std::string, std::string>> RocksDbJsonRepository::ScanRaw(
    const std::string& prefix) const {
  std::vector<std::pair<std::string, std::string>> rows;
  rocksdb::ReadOptions options;
  std::unique_ptr<rocksdb::Iterator> iterator(db_->NewIterator(options));
  for (iterator->Seek(prefix); iterator->Valid(); iterator->Next()) {
    const std::string key = iterator->key().ToString();
    if (!HasPrefix(key, prefix)) {
      break;
    }
    rows.emplace_back(key, iterator->value().ToString());
  }
  CheckStatus(iterator->status(), "rocksdb scan " + prefix);
  return rows;
}

std::vector<std::pair<std::string, nlohmann::json>> RocksDbJsonRepository::ScanJson(
    const std::string& prefix) const {
  std::vector<std::pair<std::string, nlohmann::json>> rows;
  for (const auto& [key, value] : ScanRaw(prefix)) {
    rows.emplace_back(key, ParseJsonOr(value, nlohmann::json::object()));
  }
  return rows;
}

void RocksDbJsonRepository::CheckStatus(
    const rocksdb::Status& status,
    const std::string& action) {
  if (!status.ok()) {
    throw std::runtime_error(action + ": " + status.ToString());
  }
}

nlohmann::json RocksDbJsonRepository::ParseJsonOr(
    const std::string& text,
    nlohmann::json fallback) {
  if (text.empty()) {
    return fallback;
  }
  const auto parsed = nlohmann::json::parse(text, nullptr, false);
  return parsed.is_discarded() ? fallback : parsed;
}

bool RocksDbJsonRepository::HasPrefix(
    const std::string& value,
    const std::string& prefix) {
  return value.rfind(prefix, 0) == 0;
}

}  // namespace naim::knowledge_runtime
