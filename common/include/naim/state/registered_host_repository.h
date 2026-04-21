#pragma once

#include <optional>
#include <string>
#include <vector>

#include <sqlite3.h>

#include "naim/state/sqlite_store.h"

namespace naim {

class RegisteredHostRepository final {
 public:
  explicit RegisteredHostRepository(sqlite3* db);

  void UpsertRegisteredHost(const RegisteredHostRecord& host);
  std::optional<RegisteredHostRecord> LoadRegisteredHost(const std::string& node_name) const;
  std::vector<RegisteredHostRecord> LoadRegisteredHosts(
      const std::optional<std::string>& node_name = std::nullopt) const;

 private:
  sqlite3* db_ = nullptr;
};

}  // namespace naim
