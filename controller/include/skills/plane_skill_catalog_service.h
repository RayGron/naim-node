#pragma once

#include <functional>
#include <string>

#include <nlohmann/json.hpp>

#include "naim/state/sqlite_store.h"
#include "plane/plane_mutation_service.h"
#include "skills/plane_skill_runtime_sync_service.h"

namespace naim::controller {

class PlaneSkillCatalogService final {
 public:
  using ResolveArtifactsRootFn = std::function<std::string(
      const std::string& db_path,
      const std::string& plane_name,
      const std::string& fallback_artifacts_root)>;

  PlaneSkillCatalogService(
      PlaneMutationService plane_mutation_service,
      PlaneSkillRuntimeSyncService runtime_sync_service,
      ResolveArtifactsRootFn resolve_artifacts_root);

  nlohmann::json BuildListPayload(
      const std::string& db_path,
      const std::string& plane_name) const;
  nlohmann::json BuildSkillPayload(
      const std::string& db_path,
      const std::string& plane_name,
      const std::string& skill_id) const;
  nlohmann::json CreateSkill(
      const std::string& db_path,
      const std::string& plane_name,
      const nlohmann::json& payload,
      const std::string& fallback_artifacts_root) const;
  nlohmann::json UpdateSkill(
      const std::string& db_path,
      const std::string& plane_name,
      const std::string& skill_id,
      const nlohmann::json& payload,
      bool partial,
      const std::string& fallback_artifacts_root) const;
  nlohmann::json DeleteSkill(
      const std::string& db_path,
      const std::string& plane_name,
      const std::string& skill_id,
      const std::string& fallback_artifacts_root) const;

 private:
  struct PlaneSkillInput {
    std::string id;
    std::string name;
    std::string description;
    std::string content;
    std::vector<std::string> match_terms;
    bool internal = false;
    bool enabled = true;
    std::vector<std::string> session_ids;
    std::vector<std::string> naim_links;
  };

  static PlaneSkillInput ParsePlaneSkillInput(
      const nlohmann::json& payload,
      bool partial);
  static std::string GenerateSkillId();

  nlohmann::json BuildSkillPayload(
      naim::ControllerStore& store,
      const naim::DesiredState& desired_state,
      const std::string& plane_name,
      const std::string& skill_id) const;

  PlaneMutationService plane_mutation_service_;
  PlaneSkillRuntimeSyncService runtime_sync_service_;
  ResolveArtifactsRootFn resolve_artifacts_root_;
};

}  // namespace naim::controller
