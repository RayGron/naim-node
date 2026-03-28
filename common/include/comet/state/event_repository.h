#pragma once

#include <optional>
#include <string>
#include <vector>

#include <sqlite3.h>

#include "comet/state/sqlite_store.h"

namespace comet {

class EventRepository final {
 public:
  explicit EventRepository(sqlite3* db);

  void AppendEvent(const EventRecord& event);
  std::vector<EventRecord> LoadEvents(
      const std::optional<std::string>& plane_name,
      const std::optional<std::string>& node_name,
      const std::optional<std::string>& worker_name,
      const std::optional<std::string>& category,
      int limit,
      const std::optional<int>& since_id,
      bool ascending) const;

 private:
  static EventRecord ReadEvent(sqlite3_stmt* statement);

  sqlite3* db_;
};

}  // namespace comet
