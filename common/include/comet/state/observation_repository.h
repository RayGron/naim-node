#pragma once

#include <optional>
#include <string>
#include <vector>

#include <sqlite3.h>

#include "comet/state/sqlite_store.h"

namespace comet {

class ObservationRepository final {
 public:
  explicit ObservationRepository(sqlite3* db);

  void UpsertHostObservation(const HostObservation& observation);
  std::optional<HostObservation> LoadHostObservation(const std::string& node_name) const;
  std::vector<HostObservation> LoadHostObservations(
      const std::optional<std::string>& node_name,
      const std::optional<std::string>& plane_name) const;

 private:
  static HostObservation ReadHostObservation(sqlite3_stmt* statement);

  sqlite3* db_;
};

}  // namespace comet
