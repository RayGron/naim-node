#pragma once

#include <vector>

#include <nlohmann/json.hpp>

#include "naim/state/sqlite_store.h"

namespace naim::controller {

class PlaneDashboardSkillsSummaryService final {
 public:
  static nlohmann::json BuildPayload(
      const naim::DesiredState& desired_state,
      const std::vector<naim::PlaneSkillBindingRecord>& bindings);
};

}  // namespace naim::controller
