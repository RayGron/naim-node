#include "plane/plane_dashboard_skills_summary_service.h"

#include <map>

namespace comet::controller {

nlohmann::json PlaneDashboardSkillsSummaryService::BuildPayload(
    const comet::DesiredState& desired_state,
    const std::vector<comet::PlaneSkillBindingRecord>& bindings) {
  const bool enabled =
      desired_state.skills.has_value() && desired_state.skills->enabled;
  const int total_count = desired_state.skills.has_value()
                              ? static_cast<int>(
                                    desired_state.skills->factory_skill_ids.size())
                              : 0;

  std::map<std::string, bool> enabled_by_skill_id;
  for (const auto& binding : bindings) {
    enabled_by_skill_id[binding.skill_id] = binding.enabled;
  }

  int enabled_count = 0;
  if (desired_state.skills.has_value()) {
    for (const auto& skill_id : desired_state.skills->factory_skill_ids) {
      const auto it = enabled_by_skill_id.find(skill_id);
      if (it == enabled_by_skill_id.end() || it->second) {
        ++enabled_count;
      }
    }
  }

  return nlohmann::json{
      {"enabled", enabled},
      {"enabled_count", enabled ? enabled_count : 0},
      {"total_count", total_count},
  };
}

}  // namespace comet::controller
