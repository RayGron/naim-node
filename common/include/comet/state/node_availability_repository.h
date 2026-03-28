#pragma once

#include <optional>
#include <string>
#include <vector>

#include <sqlite3.h>

#include "comet/state/sqlite_store.h"

namespace comet {

class NodeAvailabilityRepository final {
 public:
  explicit NodeAvailabilityRepository(sqlite3* db);

  void UpsertNodeAvailabilityOverride(const NodeAvailabilityOverride& availability_override);
  std::optional<NodeAvailabilityOverride> LoadNodeAvailabilityOverride(
      const std::string& node_name) const;
  std::vector<NodeAvailabilityOverride> LoadNodeAvailabilityOverrides(
      const std::optional<std::string>& node_name = std::nullopt) const;

 private:
  sqlite3* db_ = nullptr;
};

}  // namespace comet
