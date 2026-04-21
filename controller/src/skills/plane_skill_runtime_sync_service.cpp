#include "skills/plane_skill_runtime_sync_service.h"

#include <set>
#include <stdexcept>

#include <nlohmann/json.hpp>

#include "naim/state/sqlite_store.h"
#include "http/controller_http_transport.h"
#include "skills/plane_skills_service.h"

namespace naim::controller {

namespace {

using nlohmann::json;

std::vector<std::pair<std::string, std::string>> DefaultJsonHeaders() {
  return {{"Content-Type", "application/json"}};
}

std::vector<std::string> SelectedSkillIds(const naim::DesiredState& desired_state) {
  if (!desired_state.skills.has_value()) {
    return {};
  }
  return desired_state.skills->factory_skill_ids;
}

json BuildSyncedSkillPayload(
    const naim::SkillsFactorySkillRecord& canonical,
    const std::optional<naim::PlaneSkillBindingRecord>& binding) {
  json payload = json::object();
  payload["id"] = canonical.id;
  payload["name"] = canonical.name;
  payload["description"] = canonical.description;
  payload["content"] = canonical.content;
  payload["match_terms"] = canonical.match_terms;
  payload["internal"] = canonical.internal;
  payload["enabled"] = !binding.has_value() || binding->enabled;
  payload["session_ids"] = binding.has_value() ? json(binding->session_ids) : json::array();
  payload["naim_links"] = binding.has_value() ? json(binding->naim_links) : json::array();
  return payload;
}

}  // namespace

bool PlaneSkillRuntimeSyncService::IsReadyForSync(
    const std::string& db_path,
    const naim::DesiredState& desired_state) {
  if (desired_state.plane_mode != naim::PlaneMode::Llm) {
    return false;
  }
  if (!desired_state.skills.has_value() || !desired_state.skills->enabled) {
    return false;
  }
  naim::ControllerStore store(db_path);
  store.Initialize();
  const auto plane = store.LoadPlane(desired_state.plane_name);
  if (!plane.has_value() || plane->state != "running") {
    return false;
  }
  const PlaneSkillsService plane_skills_service;
  const auto target = plane_skills_service.ResolveTarget(desired_state);
  if (!target.has_value()) {
    return false;
  }
  try {
    const auto response = SendControllerHttpRequest(
        *target, "GET", "/health", "", DefaultJsonHeaders());
    return response.status_code == 200;
  } catch (const std::exception&) {
    return false;
  }
}

bool PlaneSkillRuntimeSyncService::SyncPlane(
    const std::string& db_path,
    const naim::DesiredState& desired_state) const {
  if (!IsReadyForSync(db_path, desired_state)) {
    return false;
  }

  const PlaneSkillsService plane_skills_service;
  const auto target = plane_skills_service.ResolveTarget(desired_state);
  if (!target.has_value()) {
    return false;
  }

  naim::ControllerStore store(db_path);
  store.Initialize();
  const auto selected_ids = SelectedSkillIds(desired_state);
  std::set<std::string> selected(selected_ids.begin(), selected_ids.end());

  std::set<std::string> remote_ids;
  try {
    const auto response = SendControllerHttpRequest(
        *target, "GET", "/v1/skills", "", DefaultJsonHeaders());
    if (response.status_code == 200) {
      const auto payload = response.body.empty() ? json::object() : json::parse(response.body);
      if (payload.contains("skills") && payload.at("skills").is_array()) {
        for (const auto& item : payload.at("skills")) {
          if (item.is_object() && item.contains("id") && item.at("id").is_string()) {
            remote_ids.insert(item.at("id").get<std::string>());
          }
        }
      }
    }
  } catch (const std::exception&) {
    return false;
  }

  for (const auto& skill_id : selected_ids) {
    const auto canonical = store.LoadSkillsFactorySkill(skill_id);
    if (!canonical.has_value()) {
      continue;
    }
    const auto binding = store.LoadPlaneSkillBinding(desired_state.plane_name, skill_id);
    try {
      const auto response = SendControllerHttpRequest(
          *target,
          "PUT",
          "/v1/skills/" + skill_id,
          BuildSyncedSkillPayload(*canonical, binding).dump(),
          DefaultJsonHeaders());
      if (response.status_code >= 400) {
        return false;
      }
    } catch (const std::exception&) {
      return false;
    }
    remote_ids.erase(skill_id);
  }

  for (const auto& stale_skill_id : remote_ids) {
    try {
      const auto response = SendControllerHttpRequest(
          *target,
          "DELETE",
          "/v1/skills/" + stale_skill_id,
          "",
          DefaultJsonHeaders());
      if (response.status_code != 200 && response.status_code != 404) {
        return false;
      }
    } catch (const std::exception&) {
      return false;
    }
  }

  return true;
}

}  // namespace naim::controller
