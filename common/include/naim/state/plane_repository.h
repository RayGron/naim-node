#pragma once

#include <optional>
#include <string>
#include <vector>

#include <sqlite3.h>

#include "naim/state/sqlite_store.h"

namespace naim {

class PlaneRepository final {
 public:
  explicit PlaneRepository(sqlite3* db);

  std::optional<int> LoadPlaneRebalanceIteration(const std::string& plane_name) const;
  std::vector<PlaneRecord> LoadPlanes() const;
  std::optional<PlaneRecord> LoadPlane(const std::string& plane_name) const;
  bool UpdatePlaneState(const std::string& plane_name, const std::string& state);
  bool UpdatePlaneAppliedGeneration(
      const std::string& plane_name,
      int applied_generation);
  bool UpdatePlaneArtifactsRoot(
      const std::string& plane_name,
      const std::string& artifacts_root);
  void DeletePlane(const std::string& plane_name);

 private:
  static PlaneRecord ReadPlane(sqlite3_stmt* statement);

  sqlite3* db_;
};

}  // namespace naim
