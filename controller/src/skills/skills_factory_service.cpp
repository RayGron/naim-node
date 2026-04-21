#include "skills/skills_factory_service.h"

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

std::vector<std::string> LoadPlaneNamesUsingSkill(
    const std::vector<naim::DesiredState>& states,
    const std::string& skill_id) {
  std::vector<std::string> result;
  for (const auto& state : states) {
    if (!state.skills.has_value()) {
      continue;
    }
    const auto& ids = state.skills->factory_skill_ids;
    if (std::find(ids.begin(), ids.end(), skill_id) != ids.end()) {
      result.push_back(state.plane_name);
    }
  }
  std::sort(result.begin(), result.end());
  return result;
}

bool ContainsSkillId(
    const std::vector<std::string>& items,
    const std::string& skill_id) {
  return std::find(items.begin(), items.end(), skill_id) != items.end();
}

bool PathMatchesOrIsDescendant(
    const std::string& path,
    const std::string& prefix) {
  return path == prefix || path.starts_with(prefix + "/");
}

std::string NormalizeGroupPath(const std::string& raw_value) {
  std::vector<std::string> segments;
  std::string current;
  for (const char ch : raw_value) {
    if (ch == '/' || ch == '\\') {
      if (!current.empty()) {
        segments.push_back(current);
        current.clear();
      }
      continue;
    }
    current.push_back(ch);
  }
  if (!current.empty()) {
    segments.push_back(current);
  }

  std::string normalized;
  for (const auto& segment : segments) {
    const auto start = segment.find_first_not_of(" \t\r\n");
    const auto end = segment.find_last_not_of(" \t\r\n");
    if (start == std::string::npos || end == std::string::npos) {
      continue;
    }
    const std::string trimmed = segment.substr(start, end - start + 1);
    if (trimmed.empty()) {
      continue;
    }
    if (!normalized.empty()) {
      normalized += "/";
    }
    normalized += trimmed;
  }
  return normalized;
}

std::string RewriteGroupPathPrefix(
    const std::string& path,
    const std::string& from_prefix,
    const std::string& to_prefix) {
  if (path == from_prefix) {
    return to_prefix;
  }
  if (!path.starts_with(from_prefix + "/")) {
    return path;
  }
  const std::string suffix = path.substr(from_prefix.size() + 1);
  return to_prefix.empty() ? suffix : to_prefix + "/" + suffix;
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

}  // namespace

SkillsFactoryService::SkillsFactoryService(
    PlaneMutationService plane_mutation_service,
    PlaneSkillRuntimeSyncService runtime_sync_service,
    ResolveArtifactsRootFn resolve_artifacts_root)
    : plane_mutation_service_(std::move(plane_mutation_service)),
      runtime_sync_service_(std::move(runtime_sync_service)),
      resolve_artifacts_root_(std::move(resolve_artifacts_root)) {}

std::string SkillsFactoryService::GenerateSkillId() {
  static std::atomic<unsigned long long> counter{0};
  const auto now = std::chrono::duration_cast<std::chrono::milliseconds>(
                       std::chrono::system_clock::now().time_since_epoch())
                       .count();
  return "skill-" + std::to_string(now) + "-" + std::to_string(++counter);
}

SkillsFactoryService::CanonicalSkillInput SkillsFactoryService::ParseCanonicalSkillInput(
    const json& payload,
    bool partial) {
  if (!payload.is_object()) {
    throw std::invalid_argument("request body must be a JSON object");
  }
  CanonicalSkillInput input;
  if (!partial) {
    for (const char* key : {"name", "description", "content"}) {
      if (!payload.contains(key) || !payload.at(key).is_string() ||
          payload.at(key).get<std::string>().empty()) {
        throw std::invalid_argument(std::string(key) + " is required");
      }
    }
  }
  if (payload.contains("id")) {
    if (!payload.at("id").is_string() || payload.at("id").get<std::string>().empty()) {
      throw std::invalid_argument("id must be a non-empty string");
    }
    input.id = payload.at("id").get<std::string>();
  }
  if (payload.contains("name")) {
    if (!payload.at("name").is_string() || payload.at("name").get<std::string>().empty()) {
      throw std::invalid_argument("name must be a non-empty string");
    }
    input.name = payload.at("name").get<std::string>();
  }
  if (payload.contains("group_path")) {
    if (!payload.at("group_path").is_string()) {
      throw std::invalid_argument("group_path must be a string");
    }
    input.group_path = NormalizeGroupPath(payload.at("group_path").get<std::string>());
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
  if (payload.contains("internal")) {
    if (!payload.at("internal").is_boolean()) {
      throw std::invalid_argument("internal must be a boolean");
    }
    input.internal = payload.at("internal").get<bool>();
  }
  return input;
}

SkillsFactoryService::GroupMutationInput SkillsFactoryService::ParseGroupMutationInput(
    const json& payload,
    const char* key) {
  if (!payload.is_object()) {
    throw std::invalid_argument("request body must be a JSON object");
  }
  if (!payload.contains(key) || !payload.at(key).is_string()) {
    throw std::invalid_argument(std::string(key) + " must be a string");
  }
  const std::string normalized = NormalizeGroupPath(payload.at(key).get<std::string>());
  if (normalized.empty()) {
    throw std::invalid_argument(std::string(key) + " must not be empty");
  }
  return GroupMutationInput{normalized};
}

std::vector<std::string> SkillsFactoryService::LoadPlanesUsingSkill(
    naim::ControllerStore& store,
    const std::string& skill_id) const {
  return LoadPlaneNamesUsingSkill(store.LoadDesiredStates(), skill_id);
}

nlohmann::json SkillsFactoryService::BuildSkillPayload(
    const std::string& db_path,
    const naim::SkillsFactorySkillRecord& skill) const {
  naim::ControllerStore store(db_path);
  store.Initialize();
  const auto plane_names = LoadPlanesUsingSkill(store, skill.id);
  return json{
      {"id", skill.id},
      {"name", skill.name},
      {"group_path", skill.group_path},
      {"description", skill.description},
      {"content", skill.content},
      {"match_terms", skill.match_terms},
      {"internal", skill.internal},
      {"created_at", skill.created_at},
      {"updated_at", skill.updated_at},
      {"plane_names", plane_names},
      {"plane_count", static_cast<int>(plane_names.size())},
  };
}

nlohmann::json SkillsFactoryService::BuildListPayload(const std::string& db_path) const {
  naim::ControllerStore store(db_path);
  store.Initialize();
  json items = json::array();
  for (const auto& skill : store.LoadSkillsFactorySkills()) {
    items.push_back(BuildSkillPayload(db_path, skill));
  }
  json groups = json::array();
  for (const auto& group : store.LoadSkillsFactoryGroups()) {
    groups.push_back(json{{"path", group.path}});
  }
  return json{{"skills", items}, {"groups", groups}};
}

nlohmann::json SkillsFactoryService::BuildSkillPayload(
    const std::string& db_path,
    const std::string& skill_id) const {
  naim::ControllerStore store(db_path);
  store.Initialize();
  const auto skill = store.LoadSkillsFactorySkill(skill_id);
  if (!skill.has_value()) {
    throw std::runtime_error("skill '" + skill_id + "' not found");
  }
  return BuildSkillPayload(db_path, *skill);
}

nlohmann::json SkillsFactoryService::CreateSkill(
    const std::string& db_path,
    const nlohmann::json& payload) const {
  naim::ControllerStore store(db_path);
  store.Initialize();
  auto input = ParseCanonicalSkillInput(payload, false);
  if (input.id.empty()) {
    input.id = GenerateSkillId();
  }
  naim::SkillsFactorySkillRecord skill;
  skill.id = input.id;
  skill.name = input.name;
  skill.group_path = input.group_path;
  skill.description = input.description;
  skill.content = input.content;
  skill.match_terms = input.match_terms;
  skill.internal = input.internal;
  store.UpsertSkillsFactorySkill(skill);
  if (!skill.group_path.empty()) {
    store.UpsertSkillsFactoryGroup(naim::SkillsFactoryGroupRecord{
        skill.group_path,
        "",
        "",
    });
  }
  return BuildSkillPayload(db_path, input.id);
}

nlohmann::json SkillsFactoryService::CreateGroup(
    const std::string& db_path,
    const nlohmann::json& payload) const {
  naim::ControllerStore store(db_path);
  store.Initialize();
  const auto input = ParseGroupMutationInput(payload, "path");
  store.UpsertSkillsFactoryGroup(naim::SkillsFactoryGroupRecord{
      input.path,
      "",
      "",
  });
  return json{{"status", "created"}, {"path", input.path}};
}

nlohmann::json SkillsFactoryService::RenameGroup(
    const std::string& db_path,
    const nlohmann::json& payload) const {
  const auto from_input = ParseGroupMutationInput(payload, "from_path");
  const auto to_input = ParseGroupMutationInput(payload, "to_path");
  if (from_input.path == to_input.path) {
    return json{
        {"status", "renamed"},
        {"from_path", from_input.path},
        {"to_path", to_input.path},
    };
  }
  if (to_input.path.starts_with(from_input.path + "/")) {
    throw std::invalid_argument("to_path must not be nested inside from_path");
  }

  naim::ControllerStore store(db_path);
  store.Initialize();
  const auto groups = store.LoadSkillsFactoryGroups();
  bool matched_group = false;
  for (const auto& group : groups) {
    if (PathMatchesOrIsDescendant(group.path, from_input.path)) {
      matched_group = true;
      break;
    }
  }
  auto skills = store.LoadSkillsFactorySkills();
  for (auto& skill : skills) {
    if (!PathMatchesOrIsDescendant(skill.group_path, from_input.path)) {
      continue;
    }
    matched_group = true;
    skill.group_path = RewriteGroupPathPrefix(skill.group_path, from_input.path, to_input.path);
    store.UpsertSkillsFactorySkill(skill);
  }
  if (!matched_group) {
    throw std::runtime_error("group '" + from_input.path + "' not found");
  }
  for (const auto& group : groups) {
    if (!PathMatchesOrIsDescendant(group.path, from_input.path)) {
      continue;
    }
    store.UpsertSkillsFactoryGroup(naim::SkillsFactoryGroupRecord{
        RewriteGroupPathPrefix(group.path, from_input.path, to_input.path),
        group.created_at,
        group.updated_at,
    });
  }
  for (const auto& group : groups) {
    if (PathMatchesOrIsDescendant(group.path, from_input.path)) {
      store.DeleteSkillsFactoryGroup(group.path);
    }
  }
  store.UpsertSkillsFactoryGroup(naim::SkillsFactoryGroupRecord{
      to_input.path,
      "",
      "",
  });
  return json{
      {"status", "renamed"},
      {"from_path", from_input.path},
      {"to_path", to_input.path},
  };
}

nlohmann::json SkillsFactoryService::DeleteGroup(
    const std::string& db_path,
    const nlohmann::json& payload) const {
  const auto input = ParseGroupMutationInput(payload, "path");
  naim::ControllerStore store(db_path);
  store.Initialize();

  bool matched_group = false;
  for (auto skill : store.LoadSkillsFactorySkills()) {
    if (!PathMatchesOrIsDescendant(skill.group_path, input.path)) {
      continue;
    }
    matched_group = true;
    skill.group_path.clear();
    store.UpsertSkillsFactorySkill(skill);
  }

  const auto groups = store.LoadSkillsFactoryGroups();
  for (const auto& group : groups) {
    if (!PathMatchesOrIsDescendant(group.path, input.path)) {
      continue;
    }
    matched_group = true;
    store.DeleteSkillsFactoryGroup(group.path);
  }
  if (!matched_group) {
    throw std::runtime_error("group '" + input.path + "' not found");
  }
  return json{{"status", "deleted"}, {"path", input.path}};
}

nlohmann::json SkillsFactoryService::UpdateSkill(
    const std::string& db_path,
    const std::string& skill_id,
    const nlohmann::json& payload,
    bool partial,
    const std::string&) const {
  naim::ControllerStore store(db_path);
  store.Initialize();
  auto current = store.LoadSkillsFactorySkill(skill_id);
  if (!current.has_value()) {
    throw std::runtime_error("skill '" + skill_id + "' not found");
  }
  const auto input = ParseCanonicalSkillInput(payload, partial);
  if (!input.name.empty()) {
    current->name = input.name;
  }
  if (payload.contains("group_path")) {
    current->group_path = input.group_path;
  }
  if (!input.description.empty()) {
    current->description = input.description;
  }
  if (!input.content.empty()) {
    current->content = input.content;
  }
  if (payload.contains("match_terms")) {
    current->match_terms = input.match_terms;
  }
  if (payload.contains("internal")) {
    current->internal = input.internal;
  }
  store.UpsertSkillsFactorySkill(*current);
  if (!current->group_path.empty()) {
    store.UpsertSkillsFactoryGroup(naim::SkillsFactoryGroupRecord{
        current->group_path,
        "",
        "",
    });
  }
  SyncAffectedPlanes(db_path, LoadPlanesUsingSkill(store, skill_id));
  return BuildSkillPayload(db_path, skill_id);
}

void SkillsFactoryService::SyncAffectedPlanes(
    const std::string& db_path,
    const std::vector<std::string>& plane_names) const {
  naim::ControllerStore store(db_path);
  store.Initialize();
  for (const auto& plane_name : plane_names) {
    const auto desired_state = store.LoadDesiredState(plane_name);
    if (desired_state.has_value()) {
      (void)runtime_sync_service_.SyncPlane(db_path, *desired_state);
    }
  }
}

nlohmann::json SkillsFactoryService::DeleteSkill(
    const std::string& db_path,
    const std::string& skill_id,
    const std::string& fallback_artifacts_root) const {
  naim::ControllerStore store(db_path);
  store.Initialize();
  if (!store.LoadSkillsFactorySkill(skill_id).has_value()) {
    throw std::runtime_error("skill '" + skill_id + "' not found");
  }

  std::vector<std::string> affected_planes;
  for (const auto& desired_state : store.LoadDesiredStates()) {
    if (!desired_state.skills.has_value() ||
        !ContainsSkillId(desired_state.skills->factory_skill_ids, skill_id)) {
      continue;
    }
    auto next_state = desired_state;
    next_state.skills->factory_skill_ids = RemoveSkillId(next_state.skills->factory_skill_ids, skill_id);
    const std::string artifacts_root = resolve_artifacts_root_(
        db_path, desired_state.plane_name, fallback_artifacts_root);
    const auto result = plane_mutation_service_.ExecuteUpsertPlaneStateAction(
        db_path,
        naim::SerializeDesiredStateJson(next_state),
        artifacts_root,
        desired_state.plane_name,
        "skills-factory:detach");
    if (result.exit_code != 0) {
      throw std::runtime_error(
          result.output.empty() ? "failed to persist plane desired state" : result.output);
    }
    affected_planes.push_back(desired_state.plane_name);
  }

  store.DeletePlaneSkillBindingsForSkill(skill_id);
  store.DeleteSkillsFactorySkill(skill_id);
  SyncAffectedPlanes(db_path, affected_planes);
  return json{{"status", "deleted"}, {"skill_id", skill_id}};
}

}  // namespace naim::controller
