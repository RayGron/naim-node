#include <filesystem>
#include <iostream>
#include <stdexcept>
#include <string>
#include <vector>

#include <nlohmann/json.hpp>

#include "naim/state/desired_state_v2_renderer.h"
#include "naim/state/desired_state_v2_validator.h"
#include "naim/state/sqlite_store.h"
#include "plane/plane_mutation_service.h"
#include "skills/knowledge_vault_common_skills.h"
#include "skills/plane_skill_catalog_service.h"
#include "skills/skills_factory_service.h"

namespace fs = std::filesystem;

namespace {

using nlohmann::json;

void Expect(bool condition, const std::string& message) {
  if (!condition) {
    throw std::runtime_error(message);
  }
}

naim::DesiredState BuildDesiredState(
    const std::string& plane_name,
    const std::vector<std::string>& factory_skill_ids = {}) {
  json value{
      {"version", 2},
      {"plane_name", plane_name},
      {"plane_mode", "llm"},
      {"model",
       {
           {"source", {{"type", "local"}, {"path", "/models/qwen"}}},
           {"materialization", {{"mode", "reference"}, {"local_path", "/models/qwen"}}},
           {"served_model_name", plane_name + "-model"},
       }},
      {"runtime",
       {{"engine", "llama.cpp"}, {"distributed_backend", "llama_rpc"}, {"workers", 1}}},
      {"infer", {{"replicas", 1}}},
      {"skills",
       {
           {"enabled", true},
           {"factory_skill_ids", factory_skill_ids},
       }},
      {"app", {{"enabled", false}}},
  };
  naim::DesiredStateV2Validator::ValidateOrThrow(value);
  return naim::DesiredStateV2Renderer::Render(value);
}

naim::controller::PlaneMutationService MakeMutationService() {
  naim::controller::PlaneMutationService::Deps deps;
  deps.apply_desired_state =
      [](const std::string& db_path,
         const naim::DesiredState& desired_state,
         const std::string&,
         const std::string&) {
        naim::ControllerStore store(db_path);
        store.Initialize();
        store.ReplaceDesiredState(desired_state);
        return 0;
      };
  deps.make_plane_service =
      [](const std::string&) -> naim::controller::PlaneService {
        throw std::runtime_error("plane service should not be used in skills tests");
      };
  return naim::controller::PlaneMutationService(std::move(deps));
}

void SeedDesiredState(
    naim::ControllerStore& store,
    const naim::DesiredState& desired_state,
    int generation = 1) {
  store.ReplaceDesiredState(desired_state, generation);
  const auto plane = store.LoadPlane(desired_state.plane_name);
  Expect(plane.has_value(), "plane should exist after replacing desired state");
}

json FindSkillById(const json& skills, const std::string& skill_id) {
  for (const auto& skill : skills) {
    if (skill.value("id", std::string{}) == skill_id) {
      return skill;
    }
  }
  throw std::runtime_error("skill '" + skill_id + "' not found in payload");
}

}  // namespace

int main() {
  try {
    const fs::path temp_root =
        fs::temp_directory_path() / "naim-skills-factory-service-tests";
    std::error_code error;
    fs::remove_all(temp_root, error);
    fs::create_directories(temp_root);
    const fs::path db_path = temp_root / "controller.sqlite";

    {
      naim::ControllerStore store(db_path.string());
      store.Initialize();
      SeedDesiredState(store, BuildDesiredState("factory-plane", {"skill-alpha"}), 3);
      store.UpsertSkillsFactorySkill(naim::SkillsFactorySkillRecord{
          "skill-alpha",
          "Alpha skill",
          "alpha/core",
          "Canonical alpha description",
          "canonical alpha content",
          {"alpha", "core"},
          true,
          "",
          "",
      });
      store.UpsertPlaneSkillBinding(naim::PlaneSkillBindingRecord{
          "factory-plane",
          "skill-alpha",
          false,
          {"session-a"},
          {"naim://alpha"},
          "",
          "",
      });

      naim::controller::SkillsFactoryService factory_service(
          MakeMutationService(),
          naim::controller::PlaneSkillRuntimeSyncService(),
          [](const std::string&, const std::string&, const std::string& fallback) {
            return fallback;
          });
      const auto payload = factory_service.BuildListPayload(db_path.string());
      Expect(payload.at("skills").size() == 4, "factory list should contain seeded common skills");
      Expect(payload.at("groups").size() == 0, "factory list should start without explicit groups");
      const auto& item = FindSkillById(payload.at("skills"), "skill-alpha");
      Expect(item.at("id").get<std::string>() == "skill-alpha", "factory skill id mismatch");
      Expect(item.at("plane_count").get<int>() == 1, "factory skill plane_count mismatch");
      Expect(
          item.at("plane_names").get<std::vector<std::string>>() ==
              std::vector<std::string>({"factory-plane"}),
          "factory skill plane_names mismatch");
      Expect(
          item.at("group_path").get<std::string>() == "alpha/core",
          "factory skill group_path mismatch");
      Expect(
          item.at("match_terms").get<std::vector<std::string>>() ==
              std::vector<std::string>({"alpha", "core"}),
          "factory skill match_terms mismatch");
      Expect(item.at("internal").get<bool>(), "factory skill internal flag mismatch");

      const auto& common_item =
          FindSkillById(payload.at("skills"), "knowledge-vault-replica-search");
      Expect(
          common_item.at("group_path").get<std::string>().empty(),
          "common Knowledge Vault skill should not belong to a group");
      Expect(
          !common_item.at("internal").get<bool>(),
          "common Knowledge Vault skill should be user-visible");

      const auto deleted =
          factory_service.DeleteSkill(db_path.string(), "skill-alpha", temp_root.string());
      Expect(
          deleted.at("status").get<std::string>() == "deleted",
          "factory delete should return deleted status");
      Expect(
          !store.LoadSkillsFactorySkill("skill-alpha").has_value(),
          "factory delete should remove canonical record");
      Expect(
          !store.LoadPlaneSkillBinding("factory-plane", "skill-alpha").has_value(),
          "factory delete should remove plane binding");
      const auto next_state = store.LoadDesiredState("factory-plane");
      Expect(next_state.has_value(), "factory delete should keep plane desired state");
      Expect(
          next_state->skills.has_value() && next_state->skills->factory_skill_ids.empty(),
          "factory delete should detach skill id from plane desired state");
      std::cout << "ok: factory-delete-detaches-all-planes" << '\n';
    }

    {
      naim::ControllerStore store(db_path.string());
      store.Initialize();
      auto desired_state = BuildDesiredState("knowledge-plane", {"existing-skill"});
      naim::KnowledgeSettings knowledge;
      knowledge.enabled = true;
      desired_state.knowledge = knowledge;

      const bool changed =
          naim::controller::EnsureKnowledgeVaultCommonSkills(store, &desired_state);
      Expect(changed, "Knowledge Vault common skills should be attached to eligible planes");
      Expect(
          desired_state.skills->factory_skill_ids ==
              std::vector<std::string>({
                  "existing-skill",
                  "knowledge-vault-replica-search",
                  "knowledge-vault-replica-answer-with-citations",
                  "knowledge-vault-replica-gap-check",
              }),
          "Knowledge Vault common skills should append after existing ids");
      Expect(
          store.LoadSkillsFactorySkill("knowledge-vault-replica-search").has_value(),
          "Knowledge Vault common skill records should be seeded");

      auto non_knowledge_state = BuildDesiredState("no-knowledge-plane", {"existing-skill"});
      const bool non_knowledge_changed =
          naim::controller::AttachKnowledgeVaultCommonSkills(&non_knowledge_state);
      Expect(
          !non_knowledge_changed &&
              non_knowledge_state.skills->factory_skill_ids ==
                  std::vector<std::string>({"existing-skill"}),
          "Knowledge Vault common skills should not attach without enabled knowledge");
      std::cout << "ok: knowledge-vault-common-skills-auto-attach" << '\n';
    }

    {
      naim::ControllerStore store(db_path.string());
      store.Initialize();
      store.UpsertSkillsFactorySkill(naim::SkillsFactorySkillRecord{
          "skill-grouped",
          "Grouped skill",
          "lt-cypher/market/forecast",
          "Grouped skill description",
          "Grouped skill content",
          {"grouped"},
          false,
          "",
          "",
      });
      naim::controller::SkillsFactoryService factory_service(
          MakeMutationService(),
          naim::controller::PlaneSkillRuntimeSyncService(),
          [](const std::string&, const std::string&, const std::string& fallback) {
            return fallback;
          });

      const auto created_group = factory_service.CreateGroup(
          db_path.string(),
          json{{"path", "lt-cypher/localtrade/auth"}});
      Expect(
          created_group.at("path").get<std::string>() == "lt-cypher/localtrade/auth",
          "factory group create should normalize and return path");

      const auto renamed_group = factory_service.RenameGroup(
          db_path.string(),
          json{{"from_path", "lt-cypher"}, {"to_path", "lt-jex"}});
      Expect(
          renamed_group.at("to_path").get<std::string>() == "lt-jex",
          "factory group rename should return new path");

      const auto grouped_skill = store.LoadSkillsFactorySkill("skill-grouped");
      Expect(grouped_skill.has_value(), "renamed grouped skill should still exist");
      Expect(
          grouped_skill->group_path == "lt-jex/market/forecast",
          "group rename should rewrite skill group path");

      const auto list_after_rename = factory_service.BuildListPayload(db_path.string());
      Expect(
          !list_after_rename.at("groups").empty(),
          "group list should include explicit groups after rename");

      const auto deleted_group = factory_service.DeleteGroup(
          db_path.string(),
          json{{"path", "lt-jex/localtrade"}});
      Expect(
          deleted_group.at("status").get<std::string>() == "deleted",
          "factory group delete should return deleted status");

      const auto list_after_delete = factory_service.BuildListPayload(db_path.string());
      bool found_localtrade_group = false;
      for (const auto& entry : list_after_delete.at("groups")) {
        if (entry.at("path").get<std::string>() == "lt-jex/localtrade/auth") {
          found_localtrade_group = true;
        }
      }
      Expect(!found_localtrade_group, "deleted subtree group should be removed from explicit groups");
      std::cout << "ok: factory-group-create-rename-delete" << '\n';
    }

    {
      naim::ControllerStore store(db_path.string());
      store.Initialize();
      SeedDesiredState(store, BuildDesiredState("catalog-plane"), 5);

      naim::controller::PlaneSkillCatalogService catalog_service(
          MakeMutationService(),
          naim::controller::PlaneSkillRuntimeSyncService(),
          [](const std::string&, const std::string&, const std::string& fallback) {
            return fallback;
          });
      const auto created = catalog_service.CreateSkill(
          db_path.string(),
          "catalog-plane",
          json{
              {"name", "Catalog skill"},
              {"description", "Plane-owned binding using canonical content"},
              {"content", "always answer CATALOG-SKILL"},
              {"match_terms", json::array({"catalog", "canonical"})},
              {"internal", true},
              {"enabled", false},
              {"session_ids", json::array({"session-1", "session-2"})},
              {"naim_links", json::array({"naim://one"})},
          },
          temp_root.string());
      const auto skill_id = created.at("id").get<std::string>();
      Expect(!skill_id.empty(), "catalog create should return a skill id");
      Expect(
          created.at("enabled").get<bool>() == false,
          "catalog create should preserve enabled flag");
      Expect(
          created.at("session_ids").get<std::vector<std::string>>() ==
              std::vector<std::string>({"session-1", "session-2"}),
          "catalog create should persist session_ids");

      const auto canonical = store.LoadSkillsFactorySkill(skill_id);
      Expect(canonical.has_value(), "catalog create should upsert canonical record");
      Expect(
          canonical->content == "always answer CATALOG-SKILL",
          "catalog create should persist canonical content");
      Expect(
          canonical->match_terms == std::vector<std::string>({"catalog", "canonical"}),
          "catalog create should persist canonical match_terms");
      Expect(canonical->internal, "catalog create should persist canonical internal flag");
      const auto binding = store.LoadPlaneSkillBinding("catalog-plane", skill_id);
      Expect(binding.has_value(), "catalog create should persist plane binding");
      Expect(!binding->enabled, "catalog create should persist plane-local enabled=false");
      const auto state_after_create = store.LoadDesiredState("catalog-plane");
      Expect(state_after_create.has_value(), "catalog create should keep plane desired state");
      Expect(
          state_after_create->skills.has_value() &&
              state_after_create->skills->factory_skill_ids ==
                  std::vector<std::string>({skill_id}),
          "catalog create should attach skill id to plane desired state");

      const auto listed = catalog_service.BuildListPayload(db_path.string(), "catalog-plane");
      Expect(listed.at("skills").size() == 1, "catalog list should contain one attached skill");
      Expect(
          listed.at("skills").front().at("content").get<std::string>() ==
              "always answer CATALOG-SKILL",
          "catalog list should merge canonical content");

      const auto deleted = catalog_service.DeleteSkill(
          db_path.string(), "catalog-plane", skill_id, temp_root.string());
      Expect(
          deleted.at("status").get<std::string>() == "deleted",
          "catalog delete should return deleted status");
      Expect(
          store.LoadSkillsFactorySkill(skill_id).has_value(),
          "catalog delete should keep canonical record");
      Expect(
          !store.LoadPlaneSkillBinding("catalog-plane", skill_id).has_value(),
          "catalog delete should remove plane binding");
      const auto state_after_delete = store.LoadDesiredState("catalog-plane");
      Expect(state_after_delete.has_value(), "catalog delete should keep plane desired state");
      Expect(
          state_after_delete->skills.has_value() &&
              state_after_delete->skills->factory_skill_ids.empty(),
          "catalog delete should detach skill id from plane desired state");
      std::cout << "ok: plane-catalog-create-and-detach" << '\n';
    }

    return 0;
  } catch (const std::exception& error) {
    std::cerr << "skills_factory_service_tests failed: " << error.what() << '\n';
    return 1;
  }
}
