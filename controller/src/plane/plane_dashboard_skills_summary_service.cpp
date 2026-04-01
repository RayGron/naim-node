#include "plane/plane_dashboard_skills_summary_service.h"

#include <algorithm>
#include <map>

namespace comet::controller {

namespace {

bool IsSkillsEnabled(const comet::DesiredState& desired_state) {
  return desired_state.skills.has_value() && desired_state.skills->enabled;
}

int CountAttachedSkills(const comet::DesiredState& desired_state) {
  if (!desired_state.skills.has_value()) {
    return 0;
  }
  return static_cast<int>(desired_state.skills->factory_skill_ids.size());
}

std::map<std::string, bool> BuildEnabledBySkillId(
    const std::vector<comet::PlaneSkillBindingRecord>& bindings) {
  std::map<std::string, bool> enabled_by_skill_id;
  for (const auto& binding : bindings) {
    enabled_by_skill_id[binding.skill_id] = binding.enabled;
  }
  return enabled_by_skill_id;
}

int CountEnabledAttachedSkills(
    const comet::DesiredState& desired_state,
    const std::map<std::string, bool>& enabled_by_skill_id) {
  if (!desired_state.skills.has_value()) {
    return 0;
  }

  return static_cast<int>(std::count_if(
      desired_state.skills->factory_skill_ids.begin(),
      desired_state.skills->factory_skill_ids.end(),
      [&enabled_by_skill_id](const std::string& skill_id) {
        const auto it = enabled_by_skill_id.find(skill_id);
        return it == enabled_by_skill_id.end() || it->second;
      }));
}

}  // namespace

nlohmann::json PlaneDashboardSkillsSummaryService::BuildPayload(
    const comet::DesiredState& desired_state,
    const std::vector<comet::PlaneSkillBindingRecord>& bindings) {
  const bool enabled = IsSkillsEnabled(desired_state);
  const int total_count = CountAttachedSkills(desired_state);
  const auto enabled_by_skill_id = BuildEnabledBySkillId(bindings);
  const int enabled_count =
      CountEnabledAttachedSkills(desired_state, enabled_by_skill_id);

  return nlohmann::json{
      {"enabled", enabled},
      {"enabled_count", enabled ? enabled_count : 0},
      {"total_count", total_count},
  };
}

}  // namespace comet::controller
