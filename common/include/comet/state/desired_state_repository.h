#pragma once

#include <optional>
#include <string>
#include <vector>

#include <sqlite3.h>

#include "comet/state/models.h"

namespace comet {

class DesiredStateRepository final {
 public:
  explicit DesiredStateRepository(sqlite3* db);

  void ReplaceDesiredState(const DesiredState& state, int generation, int rebalance_iteration);
  std::optional<DesiredState> LoadDesiredState() const;
  std::optional<DesiredState> LoadDesiredState(const std::string& plane_name) const;
  std::vector<DesiredState> LoadDesiredStates() const;
  std::optional<int> LoadDesiredGeneration() const;
  std::optional<int> LoadDesiredGeneration(const std::string& plane_name) const;
  std::optional<int> LoadRebalanceIteration() const;
  std::optional<int> LoadRebalanceIteration(const std::string& plane_name) const;

 private:
  sqlite3* db_ = nullptr;
};

}  // namespace comet
