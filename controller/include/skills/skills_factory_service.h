#pragma once

#include <functional>
#include <optional>
#include <string>
#include <vector>

#include <nlohmann/json.hpp>

#include "naim/state/sqlite_store.h"
#include "plane/plane_mutation_service.h"
#include "skills/plane_skill_runtime_sync_service.h"

namespace naim::controller {

class SkillsFactoryService final {
 public:
  using ResolveArtifactsRootFn = std::function<std::string(
      const std::string& db_path,
      const std::string& plane_name,
      const std::string& fallback_artifacts_root)>;

  SkillsFactoryService(
      PlaneMutationService plane_mutation_service,
      PlaneSkillRuntimeSyncService runtime_sync_service,
      ResolveArtifactsRootFn resolve_artifacts_root);

  nlohmann::json BuildListPayload(const std::string& db_path) const;
  nlohmann::json BuildSkillPayload(
      const std::string& db_path,
      const std::string& skill_id) const;
  nlohmann::json CreateSkill(
      const std::string& db_path,
      const nlohmann::json& payload) const;
  nlohmann::json CreateGroup(
      const std::string& db_path,
      const nlohmann::json& payload) const;
  nlohmann::json RenameGroup(
      const std::string& db_path,
      const nlohmann::json& payload) const;
  nlohmann::json DeleteGroup(
      const std::string& db_path,
      const nlohmann::json& payload) const;
  nlohmann::json UpdateSkill(
      const std::string& db_path,
      const std::string& skill_id,
      const nlohmann::json& payload,
      bool partial,
      const std::string& fallback_artifacts_root) const;
  nlohmann::json DeleteSkill(
      const std::string& db_path,
      const std::string& skill_id,
      const std::string& fallback_artifacts_root) const;

 private:
  struct CanonicalSkillInput {
    std::string id;
    std::string name;
    std::string group_path;
    std::string description;
    std::string content;
    std::vector<std::string> match_terms;
    bool internal = false;
  };

  struct GroupMutationInput {
    std::string path;
  };

  static CanonicalSkillInput ParseCanonicalSkillInput(
      const nlohmann::json& payload,
      bool partial);
  static GroupMutationInput ParseGroupMutationInput(
      const nlohmann::json& payload,
      const char* key);
  static std::string GenerateSkillId();

  nlohmann::json BuildSkillPayload(
      const std::string& db_path,
      const naim::SkillsFactorySkillRecord& skill) const;
  std::vector<std::string> LoadPlanesUsingSkill(
      naim::ControllerStore& store,
      const std::string& skill_id) const;
  void SyncAffectedPlanes(
      const std::string& db_path,
      const std::vector<std::string>& plane_names) const;

  PlaneMutationService plane_mutation_service_;
  PlaneSkillRuntimeSyncService runtime_sync_service_;
  ResolveArtifactsRootFn resolve_artifacts_root_;
};

}  // namespace naim::controller
