#pragma once

#include <vector>

#include <nlohmann/json.hpp>

#include "comet/state/sqlite_store.h"

namespace comet::controller {

class PlaneDashboardSkillsSummaryService final {
 public:
  static nlohmann::json BuildPayload(
      const comet::DesiredState& desired_state,
      const std::vector<comet::PlaneSkillBindingRecord>& bindings);
};

}  // namespace comet::controller
