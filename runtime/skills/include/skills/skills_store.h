#pragma once

#include <filesystem>
#include <mutex>
#include <optional>
#include <string>
#include <vector>

#include <nlohmann/json.hpp>
#include <sqlite3.h>

namespace naim::skills {

class ApiError final : public std::exception {
 public:
  ApiError(int status, std::string code, std::string message);

  const char* what() const noexcept override;

  int status() const;
  const std::string& code() const;
  const std::string& message() const;

 private:
  int status_ = 500;
  std::string code_;
  std::string message_;
};

class SkillsStore final {
 public:
  explicit SkillsStore(const std::filesystem::path& db_path);
  ~SkillsStore();

  SkillsStore(const SkillsStore&) = delete;
  SkillsStore& operator=(const SkillsStore&) = delete;

  static std::string UtcNow();

  nlohmann::json ListSkills();
  nlohmann::json GetSkill(const std::string& skill_id);
  nlohmann::json CreateSkill(const nlohmann::json& payload);
  nlohmann::json ReplaceSkill(
      const std::string& skill_id,
      const nlohmann::json& payload,
      bool partial);
  nlohmann::json ResolveSkills(const nlohmann::json& payload);
  void DeleteSkill(const std::string& skill_id);

 private:
  struct SkillArrays {
    std::vector<std::string> session_ids;
    std::vector<std::string> naim_links;
  };

  void InitializeSchema();
  static std::string GenerateUuid7Like();
  static std::vector<std::string> NormalizeStringList(
      const nlohmann::json& value,
      const std::string& field_name,
      bool allow_empty = false);
  static nlohmann::json NormalizeSkillPayload(
      const nlohmann::json& payload,
      bool partial);
  SkillArrays LoadSkillArraysLocked(const std::string& skill_id);
  nlohmann::json SkillFromStatementLocked(sqlite3_stmt* statement);
  void ReplaceArraysLocked(
      const std::string& skill_id,
      const std::vector<std::string>& session_ids,
      const std::vector<std::string>& naim_links);
  std::optional<nlohmann::json> FindSkillLocked(
      const std::string& skill_id,
      bool enabled_only);

  std::filesystem::path db_path_;
  sqlite3* db_ = nullptr;
  std::mutex mutex_;
};

}  // namespace naim::skills
