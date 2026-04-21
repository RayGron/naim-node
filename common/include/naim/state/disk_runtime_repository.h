#pragma once

#include <optional>
#include <string>
#include <vector>

#include <sqlite3.h>

#include "naim/state/sqlite_store.h"

namespace naim {

class DiskRuntimeRepository final {
 public:
  explicit DiskRuntimeRepository(sqlite3* db);

  void UpsertDiskRuntimeState(const DiskRuntimeState& runtime_state);
  std::optional<DiskRuntimeState> LoadDiskRuntimeState(
      const std::string& disk_name,
      const std::string& node_name) const;
  std::vector<DiskRuntimeState> LoadDiskRuntimeStates(
      const std::optional<std::string>& plane_name,
      const std::optional<std::string>& node_name) const;

 private:
  static DiskRuntimeState ReadDiskRuntimeState(sqlite3_stmt* statement);

  sqlite3* db_;
};

}  // namespace naim
