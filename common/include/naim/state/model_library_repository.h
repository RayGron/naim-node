#pragma once

#include <optional>
#include <string>
#include <vector>

#include <sqlite3.h>

#include "naim/state/sqlite_store.h"

namespace naim {

class ModelLibraryRepository final {
 public:
  explicit ModelLibraryRepository(sqlite3* db);

  void UpsertModelLibraryDownloadJob(
      const ModelLibraryDownloadJobRecord& job);

  std::optional<ModelLibraryDownloadJobRecord> LoadModelLibraryDownloadJob(
      const std::string& job_id) const;

  std::vector<ModelLibraryDownloadJobRecord> LoadModelLibraryDownloadJobs(
      const std::optional<std::string>& status) const;

  bool DeleteModelLibraryDownloadJob(const std::string& job_id);

 private:
  static std::string SerializeStringArray(
      const std::vector<std::string>& values);
  static std::vector<std::string> ParseStringArray(const std::string& payload);
  static ModelLibraryDownloadJobRecord ReadModelLibraryDownloadJob(
      sqlite3_stmt* statement);

  sqlite3* db_ = nullptr;
};

}  // namespace naim
