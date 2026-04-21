#include "naim/state/skills_factory_repository.h"

#include <string>

#include <nlohmann/json.hpp>

#include "naim/state/sqlite_statement.h"

namespace naim {

namespace {

using Statement = SqliteStatement;
using nlohmann::json;

std::string ToColumnText(sqlite3_stmt* statement, int column_index) {
  const unsigned char* text = sqlite3_column_text(statement, column_index);
  if (text == nullptr) {
    return "";
  }
  return reinterpret_cast<const char*>(text);
}

std::vector<std::string> DeserializeStringArray(const std::string& json_text) {
  if (json_text.empty()) {
    return {};
  }
  const json parsed = json::parse(json_text, nullptr, false);
  if (!parsed.is_array()) {
    return {};
  }
  std::vector<std::string> values;
  values.reserve(parsed.size());
  for (const auto& item : parsed) {
    if (item.is_string()) {
      values.push_back(item.get<std::string>());
    }
  }
  return values;
}

std::string SerializeStringArray(const std::vector<std::string>& values) {
  return json(values).dump();
}

}  // namespace

SkillsFactoryRepository::SkillsFactoryRepository(sqlite3* db) : db_(db) {}

void SkillsFactoryRepository::UpsertSkillsFactorySkill(const SkillsFactorySkillRecord& skill) {
  Statement statement(
      db_,
      "INSERT INTO skills_factory_skills("
      "id, name, group_path, description, content, match_terms_json, internal, created_at, updated_at"
      ") VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9) "
      "ON CONFLICT(id) DO UPDATE SET "
      "name = excluded.name, "
      "group_path = excluded.group_path, "
      "description = excluded.description, "
      "content = excluded.content, "
      "match_terms_json = excluded.match_terms_json, "
      "internal = excluded.internal, "
      "created_at = excluded.created_at, "
      "updated_at = excluded.updated_at;");
  statement.BindText(1, skill.id);
  statement.BindText(2, skill.name);
  statement.BindText(3, skill.group_path);
  statement.BindText(4, skill.description);
  statement.BindText(5, skill.content);
  statement.BindText(6, SerializeStringArray(skill.match_terms));
  statement.BindInt(7, skill.internal ? 1 : 0);
  statement.BindText(8, skill.created_at);
  statement.BindText(9, skill.updated_at);
  statement.StepDone();
}

std::optional<SkillsFactorySkillRecord> SkillsFactoryRepository::LoadSkillsFactorySkill(
    const std::string& skill_id) const {
  Statement statement(
      db_,
      "SELECT id, name, group_path, description, content, match_terms_json, internal, created_at, updated_at "
      "FROM skills_factory_skills WHERE id = ?1;");
  statement.BindText(1, skill_id);
  if (!statement.StepRow()) {
    return std::nullopt;
  }
  return ReadSkillsFactorySkill(statement.raw());
}

std::vector<SkillsFactorySkillRecord> SkillsFactoryRepository::LoadSkillsFactorySkills() const {
  Statement statement(
      db_,
      "SELECT id, name, group_path, description, content, match_terms_json, internal, created_at, updated_at "
      "FROM skills_factory_skills ORDER BY group_path ASC, updated_at DESC, name ASC, id ASC;");
  std::vector<SkillsFactorySkillRecord> skills;
  while (statement.StepRow()) {
    skills.push_back(ReadSkillsFactorySkill(statement.raw()));
  }
  return skills;
}

bool SkillsFactoryRepository::DeleteSkillsFactorySkill(const std::string& skill_id) {
  Statement statement(
      db_,
      "DELETE FROM skills_factory_skills WHERE id = ?1;");
  statement.BindText(1, skill_id);
  statement.StepDone();
  return sqlite3_changes(db_) == 1;
}

void SkillsFactoryRepository::UpsertSkillsFactoryGroup(const SkillsFactoryGroupRecord& group) {
  Statement statement(
      db_,
      "INSERT INTO skills_factory_groups(path, created_at, updated_at) VALUES(?1, ?2, ?3) "
      "ON CONFLICT(path) DO UPDATE SET "
      "created_at = excluded.created_at, "
      "updated_at = excluded.updated_at;");
  statement.BindText(1, group.path);
  statement.BindText(2, group.created_at);
  statement.BindText(3, group.updated_at);
  statement.StepDone();
}

std::vector<SkillsFactoryGroupRecord> SkillsFactoryRepository::LoadSkillsFactoryGroups() const {
  Statement statement(
      db_,
      "SELECT path, created_at, updated_at "
      "FROM skills_factory_groups ORDER BY path ASC;");
  std::vector<SkillsFactoryGroupRecord> groups;
  while (statement.StepRow()) {
    groups.push_back(ReadSkillsFactoryGroup(statement.raw()));
  }
  return groups;
}

bool SkillsFactoryRepository::DeleteSkillsFactoryGroup(const std::string& path) {
  Statement statement(
      db_,
      "DELETE FROM skills_factory_groups WHERE path = ?1;");
  statement.BindText(1, path);
  statement.StepDone();
  return sqlite3_changes(db_) == 1;
}

void SkillsFactoryRepository::UpsertPlaneSkillBinding(const PlaneSkillBindingRecord& binding) {
  Statement statement(
      db_,
      "INSERT INTO plane_skill_bindings("
      "plane_name, skill_id, enabled, session_ids_json, naim_links_json, created_at, updated_at"
      ") VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7) "
      "ON CONFLICT(plane_name, skill_id) DO UPDATE SET "
      "enabled = excluded.enabled, "
      "session_ids_json = excluded.session_ids_json, "
      "naim_links_json = excluded.naim_links_json, "
      "created_at = excluded.created_at, "
      "updated_at = excluded.updated_at;");
  statement.BindText(1, binding.plane_name);
  statement.BindText(2, binding.skill_id);
  statement.BindInt(3, binding.enabled ? 1 : 0);
  statement.BindText(4, SerializeStringArray(binding.session_ids));
  statement.BindText(5, SerializeStringArray(binding.naim_links));
  statement.BindText(6, binding.created_at);
  statement.BindText(7, binding.updated_at);
  statement.StepDone();
}

std::optional<PlaneSkillBindingRecord> SkillsFactoryRepository::LoadPlaneSkillBinding(
    const std::string& plane_name,
    const std::string& skill_id) const {
  Statement statement(
      db_,
      "SELECT plane_name, skill_id, enabled, session_ids_json, naim_links_json, created_at, "
      "updated_at "
      "FROM plane_skill_bindings WHERE plane_name = ?1 AND skill_id = ?2;");
  statement.BindText(1, plane_name);
  statement.BindText(2, skill_id);
  if (!statement.StepRow()) {
    return std::nullopt;
  }
  return ReadPlaneSkillBinding(statement.raw());
}

std::vector<PlaneSkillBindingRecord> SkillsFactoryRepository::LoadPlaneSkillBindings(
    const std::optional<std::string>& plane_name,
    const std::optional<std::string>& skill_id) const {
  std::string sql =
      "SELECT plane_name, skill_id, enabled, session_ids_json, naim_links_json, created_at, "
      "updated_at FROM plane_skill_bindings";
  int bind_index = 1;
  bool has_where = false;
  if (plane_name.has_value()) {
    sql += " WHERE plane_name = ?" + std::to_string(bind_index++);
    has_where = true;
  }
  if (skill_id.has_value()) {
    sql += has_where ? " AND " : " WHERE ";
    sql += "skill_id = ?" + std::to_string(bind_index++);
  }
  sql += " ORDER BY plane_name ASC, updated_at DESC, skill_id ASC;";

  Statement statement(db_, sql);
  bind_index = 1;
  if (plane_name.has_value()) {
    statement.BindText(bind_index++, *plane_name);
  }
  if (skill_id.has_value()) {
    statement.BindText(bind_index++, *skill_id);
  }

  std::vector<PlaneSkillBindingRecord> bindings;
  while (statement.StepRow()) {
    bindings.push_back(ReadPlaneSkillBinding(statement.raw()));
  }
  return bindings;
}

bool SkillsFactoryRepository::DeletePlaneSkillBinding(
    const std::string& plane_name,
    const std::string& skill_id) {
  Statement statement(
      db_,
      "DELETE FROM plane_skill_bindings WHERE plane_name = ?1 AND skill_id = ?2;");
  statement.BindText(1, plane_name);
  statement.BindText(2, skill_id);
  statement.StepDone();
  return sqlite3_changes(db_) == 1;
}

int SkillsFactoryRepository::DeletePlaneSkillBindingsForSkill(const std::string& skill_id) {
  Statement statement(
      db_,
      "DELETE FROM plane_skill_bindings WHERE skill_id = ?1;");
  statement.BindText(1, skill_id);
  statement.StepDone();
  return sqlite3_changes(db_);
}

SkillsFactorySkillRecord SkillsFactoryRepository::ReadSkillsFactorySkill(sqlite3_stmt* statement) {
  SkillsFactorySkillRecord skill;
  skill.id = ToColumnText(statement, 0);
  skill.name = ToColumnText(statement, 1);
  skill.group_path = ToColumnText(statement, 2);
  skill.description = ToColumnText(statement, 3);
  skill.content = ToColumnText(statement, 4);
  skill.match_terms = DeserializeStringArray(ToColumnText(statement, 5));
  skill.internal = sqlite3_column_int(statement, 6) != 0;
  skill.created_at = ToColumnText(statement, 7);
  skill.updated_at = ToColumnText(statement, 8);
  return skill;
}

SkillsFactoryGroupRecord SkillsFactoryRepository::ReadSkillsFactoryGroup(sqlite3_stmt* statement) {
  SkillsFactoryGroupRecord group;
  group.path = ToColumnText(statement, 0);
  group.created_at = ToColumnText(statement, 1);
  group.updated_at = ToColumnText(statement, 2);
  return group;
}

PlaneSkillBindingRecord SkillsFactoryRepository::ReadPlaneSkillBinding(sqlite3_stmt* statement) {
  PlaneSkillBindingRecord binding;
  binding.plane_name = ToColumnText(statement, 0);
  binding.skill_id = ToColumnText(statement, 1);
  binding.enabled = sqlite3_column_int(statement, 2) != 0;
  binding.session_ids = DeserializeStringArray(ToColumnText(statement, 3));
  binding.naim_links = DeserializeStringArray(ToColumnText(statement, 4));
  binding.created_at = ToColumnText(statement, 5);
  binding.updated_at = ToColumnText(statement, 6);
  return binding;
}

}  // namespace naim
