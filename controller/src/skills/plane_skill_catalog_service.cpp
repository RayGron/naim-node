#include "skills/plane_skill_catalog_service.h"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <set>
#include <stdexcept>
#include <utility>

#include "naim/state/sqlite_store.h"
#include "naim/state/state_json.h"

namespace naim::controller {

namespace {

using nlohmann::json;

std::vector<std::string> UniqueNonEmptyStringArray(
    const json& payload,
    const std::string& key) {
  if (!payload.contains(key) || payload.at(key).is_null()) {
    return {};
  }
  if (!payload.at(key).is_array()) {
    throw std::invalid_argument(key + " must be an array");
  }
  std::vector<std::string> result;
  std::set<std::string> seen;
  for (const auto& item : payload.at(key)) {
    if (!item.is_string()) {
      throw std::invalid_argument(key + " items must be strings");
    }
    const std::string value = item.get<std::string>();
    if (value.empty()) {
      throw std::invalid_argument(key + " items must not be empty");
    }
    if (seen.insert(value).second) {
      result.push_back(value);
    }
  }
  return result;
}

std::string MaxTimestamp(
    const std::string& left,
    const std::string& right) {
  return left >= right ? left : right;
}

bool ContainsSkillId(
    const std::vector<std::string>& items,
    const std::string& skill_id) {
  return std::find(items.begin(), items.end(), skill_id) != items.end();
}

std::vector<std::string> RemoveSkillId(
    const std::vector<std::string>& items,
    const std::string& skill_id) {
  std::vector<std::string> result;
  result.reserve(items.size());
  for (const auto& item : items) {
    if (item != skill_id) {
      result.push_back(item);
    }
  }
  return result;
}

}  // namespace

PlaneSkillCatalogService::PlaneSkillCatalogService(
    PlaneMutationService plane_mutation_service,
    PlaneSkillRuntimeSyncService runtime_sync_service,
    ResolveArtifactsRootFn resolve_artifacts_root)
    : plane_mutation_service_(std::move(plane_mutation_service)),
      runtime_sync_service_(std::move(runtime_sync_service)),
      resolve_artifacts_root_(std::move(resolve_artifacts_root)) {}

std::string PlaneSkillCatalogService::GenerateSkillId() {
  static std::atomic<unsigned long long> counter{0};
  const auto now = std::chrono::duration_cast<std::chrono::milliseconds>(
                       std::chrono::system_clock::now().time_since_epoch())
                       .count();
  return "skill-" + std::to_string(now) + "-" + std::to_string(++counter);
}

PlaneSkillCatalogService::PlaneSkillInput PlaneSkillCatalogService::ParsePlaneSkillInput(
    const json& payload,
    bool partial) {
  if (!payload.is_object()) {
    throw std::invalid_argument("request body must be a JSON object");
  }
  PlaneSkillInput input;
  if (!partial) {
    for (const char* key : {"name", "description", "content"}) {
      if (!payload.contains(key) || !payload.at(key).is_string() ||
          payload.at(key).get<std::string>().empty()) {
        throw std::invalid_argument(std::string(key) + " is required");
      }
    }
  }
  if (payload.contains("name")) {
    if (!payload.at("name").is_string() || payload.at("name").get<std::string>().empty()) {
      throw std::invalid_argument("name must be a non-empty string");
    }
    input.name = payload.at("name").get<std::string>();
  }
  if (payload.contains("description")) {
    if (!payload.at("description").is_string() ||
        payload.at("description").get<std::string>().empty()) {
      throw std::invalid_argument("description must be a non-empty string");
    }
    input.description = payload.at("description").get<std::string>();
  }
  if (payload.contains("content")) {
    if (!payload.at("content").is_string() || payload.at("content").get<std::string>().empty()) {
      throw std::invalid_argument("content must be a non-empty string");
    }
    input.content = payload.at("content").get<std::string>();
  }
  input.match_terms = UniqueNonEmptyStringArray(payload, "match_terms");
  if (payload.contains("enabled")) {
    if (!payload.at("enabled").is_boolean()) {
      throw std::invalid_argument("enabled must be a boolean");
    }
    input.enabled = payload.at("enabled").get<bool>();
  }
  if (payload.contains("internal")) {
    if (!payload.at("internal").is_boolean()) {
      throw std::invalid_argument("internal must be a boolean");
    }
    input.internal = payload.at("internal").get<bool>();
  }
  input.session_ids = UniqueNonEmptyStringArray(payload, "session_ids");
  input.naim_links = UniqueNonEmptyStringArray(payload, "naim_links");
  return input;
}

nlohmann::json PlaneSkillCatalogService::BuildSkillPayload(
    naim::ControllerStore& store,
    const naim::DesiredState& desired_state,
    const std::string& plane_name,
    const std::string& skill_id) const {
  if (!desired_state.skills.has_value() || !ContainsSkillId(desired_state.skills->factory_skill_ids, skill_id)) {
    throw std::runtime_error("skill '" + skill_id + "' is not attached to plane '" + plane_name + "'");
  }
  const auto canonical = store.LoadSkillsFactorySkill(skill_id);
  if (!canonical.has_value()) {
    throw std::runtime_error("skill '" + skill_id + "' not found");
  }
  const auto binding = store.LoadPlaneSkillBinding(plane_name, skill_id);
  return json{
      {"id", canonical->id},
      {"name", canonical->name},
      {"description", canonical->description},
      {"content", canonical->content},
      {"match_terms", canonical->match_terms},
      {"internal", canonical->internal},
      {"enabled", !binding.has_value() || binding->enabled},
      {"session_ids", binding.has_value() ? json(binding->session_ids) : json::array()},
      {"naim_links", binding.has_value() ? json(binding->naim_links) : json::array()},
      {"created_at", canonical->created_at},
      {"updated_at",
       binding.has_value() ? MaxTimestamp(canonical->updated_at, binding->updated_at)
                           : canonical->updated_at},
  };
}

nlohmann::json PlaneSkillCatalogService::BuildListPayload(
    const std::string& db_path,
    const std::string& plane_name) const {
  naim::ControllerStore store(db_path);
  store.Initialize();
  const auto desired_state = store.LoadDesiredState(plane_name);
  if (!desired_state.has_value()) {
    throw std::runtime_error("plane '" + plane_name + "' not found");
  }
  json skills = json::array();
  if (desired_state->skills.has_value()) {
    for (const auto& skill_id : desired_state->skills->factory_skill_ids) {
      skills.push_back(BuildSkillPayload(store, *desired_state, plane_name, skill_id));
    }
  }
  return json{{"skills", skills}};
}

nlohmann::json PlaneSkillCatalogService::BuildSkillPayload(
    const std::string& db_path,
    const std::string& plane_name,
    const std::string& skill_id) const {
  naim::ControllerStore store(db_path);
  store.Initialize();
  const auto desired_state = store.LoadDesiredState(plane_name);
  if (!desired_state.has_value()) {
    throw std::runtime_error("plane '" + plane_name + "' not found");
  }
  return BuildSkillPayload(store, *desired_state, plane_name, skill_id);
}

nlohmann::json PlaneSkillCatalogService::CreateSkill(
    const std::string& db_path,
    const std::string& plane_name,
    const nlohmann::json& payload,
    const std::string& fallback_artifacts_root) const {
  naim::ControllerStore store(db_path);
  store.Initialize();
  auto desired_state = store.LoadDesiredState(plane_name);
  const auto plane = store.LoadPlane(plane_name);
  if (!desired_state.has_value() || !plane.has_value()) {
    throw std::runtime_error("plane '" + plane_name + "' not found");
  }
  if (desired_state->plane_mode != naim::PlaneMode::Llm) {
    throw std::runtime_error("skills are available only for llm planes");
  }
  if (!desired_state->skills.has_value() || !desired_state->skills->enabled) {
    throw std::runtime_error("skills are not enabled for this plane");
  }

  auto input = ParsePlaneSkillInput(payload, false);
  input.id = GenerateSkillId();

  const auto existing_canonical = store.LoadSkillsFactorySkill(input.id);
  naim::SkillsFactorySkillRecord canonical;
  canonical.id = input.id;
  canonical.name = input.name;
  canonical.description = input.description;
  canonical.content = input.content;
  canonical.match_terms = input.match_terms;
  canonical.internal = input.internal;
  canonical.created_at = existing_canonical.has_value() ? existing_canonical->created_at : "";
  canonical.updated_at = "";
  store.UpsertSkillsFactorySkill(canonical);

  naim::PlaneSkillBindingRecord binding;
  binding.plane_name = plane_name;
  binding.skill_id = input.id;
  binding.enabled = input.enabled;
  binding.session_ids = input.session_ids;
  binding.naim_links = input.naim_links;
  store.UpsertPlaneSkillBinding(binding);

  auto next_state = *desired_state;
  if (!next_state.skills.has_value()) {
    next_state.skills = naim::SkillsSettings{};
    next_state.skills->enabled = true;
  }
  if (!ContainsSkillId(next_state.skills->factory_skill_ids, input.id)) {
    next_state.skills->factory_skill_ids.push_back(input.id);
  }

  const std::string artifacts_root = resolve_artifacts_root_(
      db_path, plane_name, fallback_artifacts_root);
  const auto result = plane_mutation_service_.ExecuteUpsertPlaneStateAction(
      db_path,
      naim::SerializeDesiredStateJson(next_state),
      artifacts_root,
      plane_name,
      "plane-skills:create");
  if (result.exit_code != 0) {
    throw std::runtime_error(
        result.output.empty() ? "failed to persist plane desired state" : result.output);
  }

  (void)runtime_sync_service_.SyncPlane(db_path, next_state);
  return BuildSkillPayload(db_path, plane_name, input.id);
}

nlohmann::json PlaneSkillCatalogService::UpdateSkill(
    const std::string& db_path,
    const std::string& plane_name,
    const std::string& skill_id,
    const nlohmann::json& payload,
    bool partial,
    const std::string& fallback_artifacts_root) const {
  naim::ControllerStore store(db_path);
  store.Initialize();
  auto desired_state = store.LoadDesiredState(plane_name);
  if (!desired_state.has_value()) {
    throw std::runtime_error("plane '" + plane_name + "' not found");
  }
  if (!desired_state->skills.has_value() || !desired_state->skills->enabled) {
    throw std::runtime_error("skills are not enabled for this plane");
  }

  const auto canonical_current = store.LoadSkillsFactorySkill(skill_id);
  if (!canonical_current.has_value()) {
    throw std::runtime_error("skill '" + skill_id + "' not found");
  }
  const auto binding_current = store.LoadPlaneSkillBinding(plane_name, skill_id);
  if (!ContainsSkillId(desired_state->skills->factory_skill_ids, skill_id)) {
    throw std::runtime_error("skill '" + skill_id + "' is not attached to plane '" + plane_name + "'");
  }

  const auto input = ParsePlaneSkillInput(payload, partial);
  naim::SkillsFactorySkillRecord canonical = *canonical_current;
  if (!input.name.empty()) {
    canonical.name = input.name;
  }
  if (!input.description.empty()) {
    canonical.description = input.description;
  }
  if (!input.content.empty()) {
    canonical.content = input.content;
  }
  if (payload.contains("match_terms")) {
    canonical.match_terms = input.match_terms;
  }
  if (payload.contains("internal")) {
    canonical.internal = input.internal;
  }
  store.UpsertSkillsFactorySkill(canonical);

  naim::PlaneSkillBindingRecord binding;
  if (binding_current.has_value()) {
    binding = *binding_current;
  } else {
    binding.plane_name = plane_name;
    binding.skill_id = skill_id;
  }
  if (!partial || payload.contains("enabled")) {
    binding.enabled = input.enabled;
  }
  if (!partial || payload.contains("session_ids")) {
    binding.session_ids = input.session_ids;
  }
  if (!partial || payload.contains("naim_links")) {
    binding.naim_links = input.naim_links;
  }
  store.UpsertPlaneSkillBinding(binding);

  auto next_state = *desired_state;
  if (!ContainsSkillId(next_state.skills->factory_skill_ids, skill_id)) {
    next_state.skills->factory_skill_ids.push_back(skill_id);
  }
  const std::string artifacts_root = resolve_artifacts_root_(
      db_path, plane_name, fallback_artifacts_root);
  const auto result = plane_mutation_service_.ExecuteUpsertPlaneStateAction(
      db_path,
      naim::SerializeDesiredStateJson(next_state),
      artifacts_root,
      plane_name,
      partial ? "plane-skills:patch" : "plane-skills:update");
  if (result.exit_code != 0) {
    throw std::runtime_error(
        result.output.empty() ? "failed to persist plane desired state" : result.output);
  }

  (void)runtime_sync_service_.SyncPlane(db_path, next_state);
  return BuildSkillPayload(db_path, plane_name, skill_id);
}

nlohmann::json PlaneSkillCatalogService::DeleteSkill(
    const std::string& db_path,
    const std::string& plane_name,
    const std::string& skill_id,
    const std::string& fallback_artifacts_root) const {
  naim::ControllerStore store(db_path);
  store.Initialize();
  auto desired_state = store.LoadDesiredState(plane_name);
  if (!desired_state.has_value()) {
    throw std::runtime_error("plane '" + plane_name + "' not found");
  }
  if (!desired_state->skills.has_value() ||
      !ContainsSkillId(desired_state->skills->factory_skill_ids, skill_id)) {
    throw std::runtime_error("skill '" + skill_id + "' is not attached to plane '" + plane_name + "'");
  }

  auto next_state = *desired_state;
  next_state.skills->factory_skill_ids = RemoveSkillId(next_state.skills->factory_skill_ids, skill_id);
  if (next_state.skills->factory_skill_ids.empty()) {
    store.DeletePlaneSkillBinding(plane_name, skill_id);
  } else {
    store.DeletePlaneSkillBinding(plane_name, skill_id);
  }

  const std::string artifacts_root = resolve_artifacts_root_(
      db_path, plane_name, fallback_artifacts_root);
  const auto result = plane_mutation_service_.ExecuteUpsertPlaneStateAction(
      db_path,
      naim::SerializeDesiredStateJson(next_state),
      artifacts_root,
      plane_name,
      "plane-skills:detach");
  if (result.exit_code != 0) {
    throw std::runtime_error(
        result.output.empty() ? "failed to persist plane desired state" : result.output);
  }

  (void)runtime_sync_service_.SyncPlane(db_path, next_state);
  return json{
      {"status", "deleted"},
      {"plane_name", plane_name},
      {"skill_id", skill_id},
  };
}

}  // namespace naim::controller
