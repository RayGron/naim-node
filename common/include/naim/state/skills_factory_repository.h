#pragma once

#include <optional>
#include <string>
#include <vector>

#include <sqlite3.h>

#include "naim/state/sqlite_store.h"

namespace naim {

class SkillsFactoryRepository final {
 public:
  explicit SkillsFactoryRepository(sqlite3* db);

  void UpsertSkillsFactorySkill(const SkillsFactorySkillRecord& skill);
  std::optional<SkillsFactorySkillRecord> LoadSkillsFactorySkill(
      const std::string& skill_id) const;
  std::vector<SkillsFactorySkillRecord> LoadSkillsFactorySkills() const;
  bool DeleteSkillsFactorySkill(const std::string& skill_id);
  void UpsertSkillsFactoryGroup(const SkillsFactoryGroupRecord& group);
  std::vector<SkillsFactoryGroupRecord> LoadSkillsFactoryGroups() const;
  bool DeleteSkillsFactoryGroup(const std::string& path);

  void UpsertPlaneSkillBinding(const PlaneSkillBindingRecord& binding);
  std::optional<PlaneSkillBindingRecord> LoadPlaneSkillBinding(
      const std::string& plane_name,
      const std::string& skill_id) const;
  std::vector<PlaneSkillBindingRecord> LoadPlaneSkillBindings(
      const std::optional<std::string>& plane_name = std::nullopt,
      const std::optional<std::string>& skill_id = std::nullopt) const;
  bool DeletePlaneSkillBinding(
      const std::string& plane_name,
      const std::string& skill_id);
  int DeletePlaneSkillBindingsForSkill(const std::string& skill_id);

 private:
  static SkillsFactorySkillRecord ReadSkillsFactorySkill(sqlite3_stmt* statement);
  static SkillsFactoryGroupRecord ReadSkillsFactoryGroup(sqlite3_stmt* statement);
  static PlaneSkillBindingRecord ReadPlaneSkillBinding(sqlite3_stmt* statement);

  sqlite3* db_ = nullptr;
};

}  // namespace naim
