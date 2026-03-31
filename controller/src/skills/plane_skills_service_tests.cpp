#include <filesystem>
#include <iostream>
#include <stdexcept>
#include <string>

#include <nlohmann/json.hpp>

#include "comet/state/sqlite_store.h"
#include "interaction/interaction_service.h"
#include "plane/plane_dashboard_skills_summary_service.h"
#include "skills/plane_skill_contextual_resolver_service.h"
#include "skills/plane_skills_service.h"

namespace {

namespace fs = std::filesystem;
using nlohmann::json;

void Expect(bool condition, const std::string& message) {
  if (!condition) {
    throw std::runtime_error(message);
  }
}

comet::DesiredState BuildDesiredStateWithSkillsPort(const std::string& host_ip) {
  comet::DesiredState desired_state;
  desired_state.plane_name = "maglev";
  desired_state.plane_mode = comet::PlaneMode::Llm;
  comet::SkillsSettings skills_settings;
  skills_settings.enabled = true;
  desired_state.skills = skills_settings;

  comet::InstanceSpec skills;
  skills.name = "skills-maglev";
  skills.plane_name = "maglev";
  skills.node_name = "local-hostd";
  skills.role = comet::InstanceRole::Skills;
  comet::PublishedPort published_port;
  published_port.host_ip = host_ip;
  published_port.host_port = 27978;
  published_port.container_port = 18120;
  skills.published_ports.push_back(published_port);
  desired_state.instances.push_back(skills);
  return desired_state;
}

comet::DesiredState BuildDesiredState(
    const std::string& plane_name,
    const std::vector<std::string>& factory_skill_ids,
    bool skills_enabled = true) {
  comet::DesiredState desired_state;
  desired_state.plane_name = plane_name;
  desired_state.plane_mode = comet::PlaneMode::Llm;
  if (skills_enabled || !factory_skill_ids.empty()) {
    comet::SkillsSettings settings;
    settings.enabled = skills_enabled;
    settings.factory_skill_ids = factory_skill_ids;
    desired_state.skills = settings;
  }
  return desired_state;
}

std::string MakeTempDbPath() {
  const auto root =
      fs::temp_directory_path() / "comet-plane-skills-service-tests";
  fs::create_directories(root);
  return (root / "controller.sqlite").string();
}

void SeedCanonicalSkill(
    comet::ControllerStore* store,
    const std::string& id,
    const std::string& name,
    const std::string& description,
    const std::string& content) {
  comet::SkillsFactorySkillRecord record;
  record.id = id;
  record.name = name;
  record.description = description;
  record.content = content;
  store->UpsertSkillsFactorySkill(record);
}

}  // namespace

int main() {
  try {
    comet::controller::PlaneSkillsService service;

    {
      const auto target =
          service.ResolveTarget(BuildDesiredStateWithSkillsPort("127.0.0.1"));
      Expect(target.has_value(), "skills target should resolve when a published port exists");
      Expect(target->host == "127.0.0.1", "skills target host should use published host_ip");
      Expect(target->port == 27978, "skills target port should use published host_port");
      Expect(
          target->raw == "http://127.0.0.1:27978",
          "skills target raw URL should use normalized published endpoint");
      std::cout << "ok: published-host-ip-target" << '\n';
    }

    {
      const auto target =
          service.ResolveTarget(BuildDesiredStateWithSkillsPort("0.0.0.0"));
      Expect(target.has_value(), "skills target should resolve for wildcard published host_ip");
      Expect(
          target->host == "127.0.0.1",
          "skills target host should normalize wildcard host_ip for controller probes");
      Expect(
          target->raw == "http://127.0.0.1:27978",
          "skills target raw URL should normalize wildcard host_ip");
      std::cout << "ok: wildcard-host-ip-normalization" << '\n';
    }

    {
      const std::string db_path = MakeTempDbPath();
      fs::remove(db_path);
      comet::ControllerStore store(db_path);
      store.Initialize();
      const auto desired_state = BuildDesiredState(
          "catalog-plane", {"code-agent-root-cause-debug"});
      store.ReplaceDesiredState(desired_state, 1);
      SeedCanonicalSkill(
          &store,
          "code-agent-root-cause-debug",
          "root-cause-debug",
          "Debug bugs by reproducing them, isolating the invariant, and validating the root cause.",
          "When handling a bug or regression, reproduce the issue, validate the root cause, and confirm the path.");

      comet::controller::PlaneInteractionResolution resolution;
      resolution.db_path = db_path;
      resolution.desired_state = desired_state;

      const auto selection =
          comet::controller::PlaneSkillContextualResolverService().Resolve(
              db_path,
              resolution,
              json{{"messages",
                    json::array(
                        {json{{"role", "user"},
                              {"content",
                               "Please debug this regression and find the root cause."}}})}});
      Expect(
          selection.mode == "contextual",
          "resolver should select a contextual skill for a matching prompt");
      Expect(
          selection.candidate_count == 1,
          "resolver should count enabled plane-local candidates");
      Expect(
          selection.selected_skill_ids.size() == 1 &&
              selection.selected_skill_ids.front() ==
                  "code-agent-root-cause-debug",
          "resolver should return the matching plane-local skill id");
      std::cout << "ok: contextual-resolver-selects-plane-local-skill" << '\n';
    }

    {
      const std::string db_path = MakeTempDbPath();
      fs::remove(db_path);
      comet::ControllerStore store(db_path);
      store.Initialize();
      const auto desired_state = BuildDesiredState(
          "catalog-plane", {"code-agent-safe-change"});
      store.ReplaceDesiredState(desired_state, 1);
      SeedCanonicalSkill(
          &store,
          "code-agent-safe-change",
          "safe-change",
          "Limit changes to the smallest safe patch.",
          "Keep the patch minimal and avoid unrelated edits.");

      comet::controller::PlaneInteractionResolution resolution;
      resolution.db_path = db_path;
      resolution.desired_state = desired_state;

      const auto selection =
          comet::controller::PlaneSkillContextualResolverService().Resolve(
              db_path,
              resolution,
              json{{"messages",
                    json::array(
                        {json{{"role", "user"},
                              {"content",
                               "Summarize the recent rollout status for this plane."}}})}});
      Expect(
          selection.mode == "none" && selection.candidate_count == 1 &&
              selection.selected_skill_ids.empty(),
          "resolver should return none when no candidate clears the score threshold");
      std::cout << "ok: contextual-resolver-no-match" << '\n';
    }

    {
      const std::string db_path = MakeTempDbPath();
      fs::remove(db_path);
      comet::ControllerStore store(db_path);
      store.Initialize();
      const auto desired_state = BuildDesiredState("catalog-plane", {});
      store.ReplaceDesiredState(desired_state, 1);
      SeedCanonicalSkill(
          &store,
          "factory-only-skill",
          "repo-map",
          "Build a repository map before changes.",
          "Map the repository before changing code.");

      comet::controller::PlaneInteractionResolution resolution;
      resolution.db_path = db_path;
      resolution.desired_state = desired_state;

      const auto selection =
          comet::controller::PlaneSkillContextualResolverService().Resolve(
              db_path,
              resolution,
              json{{"messages",
                    json::array(
                        {json{{"role", "user"},
                              {"content",
                               "Please map the repository before making changes."}}})}});
      Expect(
          selection.mode == "none" && selection.candidate_count == 0,
          "resolver should ignore skills present only in SkillsFactory");
      std::cout << "ok: contextual-resolver-ignores-factory-only-skills" << '\n';
    }

    {
      const std::string db_path = MakeTempDbPath();
      fs::remove(db_path);
      comet::ControllerStore store(db_path);
      store.Initialize();
      const auto desired_state = BuildDesiredState(
          "catalog-plane", {"code-agent-root-cause-debug"});
      store.ReplaceDesiredState(desired_state, 1);
      SeedCanonicalSkill(
          &store,
          "code-agent-root-cause-debug",
          "root-cause-debug",
          "Debug bugs by reproducing them, isolating the invariant, and validating the root cause.",
          "When handling a bug or regression, reproduce the issue, validate the root cause, and confirm the path.");
      comet::PlaneSkillBindingRecord binding;
      binding.plane_name = "catalog-plane";
      binding.skill_id = "code-agent-root-cause-debug";
      binding.enabled = false;
      store.UpsertPlaneSkillBinding(binding);

      comet::controller::PlaneInteractionResolution resolution;
      resolution.db_path = db_path;
      resolution.desired_state = desired_state;

      const auto selection =
          comet::controller::PlaneSkillContextualResolverService().Resolve(
              db_path,
              resolution,
              json{{"messages",
                    json::array(
                        {json{{"role", "user"},
                              {"content",
                               "Please debug this regression and find the root cause."}}})}});
      Expect(
          selection.mode == "none" && selection.candidate_count == 0,
          "resolver should exclude disabled plane-local skills");
      std::cout << "ok: contextual-resolver-excludes-disabled-skills" << '\n';
    }

    {
      const std::string db_path = MakeTempDbPath();
      fs::remove(db_path);
      comet::ControllerStore store(db_path);
      store.Initialize();
      const auto desired_state = BuildDesiredState(
          "catalog-plane", {"code-agent-root-cause-debug"});
      store.ReplaceDesiredState(desired_state, 1);
      SeedCanonicalSkill(
          &store,
          "code-agent-root-cause-debug",
          "root-cause-debug",
          "Debug bugs by reproducing them, isolating the invariant, and validating the root cause.",
          "When handling a bug or regression, reproduce the issue, validate the root cause, and confirm the path.");

      comet::controller::PlaneInteractionResolution resolution;
      resolution.db_path = db_path;
      resolution.desired_state = desired_state;

      const auto payload =
          comet::controller::PlaneSkillContextualResolverService().BuildDebugPayload(
              db_path,
              resolution,
              json{{"prompt", "Please debug this regression and find the root cause."}});
      Expect(
          payload.at("skill_resolution_mode").get<std::string>() == "contextual",
          "debug payload should report contextual mode for a match");
      Expect(
          payload.at("candidate_count").get<int>() == 1,
          "debug payload should include the plane-local candidate count");
      Expect(
          payload.at("selected_skill_ids").size() == 1 &&
              payload.at("selected_skill_ids").front().get<std::string>() ==
                  "code-agent-root-cause-debug",
          "debug payload should report the selected skill id");
      std::cout << "ok: contextual-resolver-debug-payload" << '\n';
    }

    {
      const auto payload =
          comet::controller::PlaneDashboardSkillsSummaryService::BuildPayload(
          BuildDesiredState("dashboard-plane", {"skill-alpha", "skill-beta"}, true),
          std::vector<comet::PlaneSkillBindingRecord>{
              comet::PlaneSkillBindingRecord{
                  "dashboard-plane", "skill-alpha", false, {}, {}, "", ""},
              comet::PlaneSkillBindingRecord{
                  "dashboard-plane", "skill-beta", true, {}, {}, "", ""},
          });
      Expect(
          payload.at("enabled").get<bool>(),
          "dashboard skills payload should report enabled state");
      Expect(
          payload.at("enabled_count").get<int>() == 1,
          "dashboard skills payload should count enabled plane-local skills");
      Expect(
          payload.at("total_count").get<int>() == 2,
          "dashboard skills payload should count attached plane-local skills");
      std::cout << "ok: dashboard-skills-counts" << '\n';
    }

    {
      const auto payload =
          comet::controller::PlaneDashboardSkillsSummaryService::BuildPayload(
          BuildDesiredState("dashboard-plane", {"skill-alpha"}, false),
          {});
      Expect(
          !payload.at("enabled").get<bool>(),
          "dashboard skills payload should report disabled state");
      Expect(
          payload.at("enabled_count").get<int>() == 0,
          "dashboard skills payload should show zero enabled count when disabled");
      std::cout << "ok: dashboard-skills-disabled" << '\n';
    }

    {
      comet::controller::InteractionRequestValidator validator;
      comet::controller::InteractionRequestContext request_context;
      comet::controller::PlaneInteractionResolution resolution;
      resolution.desired_state = BuildDesiredState("interaction-plane", {});
      resolution.status_payload = json::object();
      const auto error = validator.ValidateAndNormalizeRequest(
          resolution,
          json{{"messages", json::array()}},
          &request_context);
      Expect(!error.has_value(), "validator should accept a basic interaction request");
      Expect(
          request_context.payload.at("auto_skills").get<bool>(),
          "validator should default auto_skills to true");
      std::cout << "ok: interaction-validator-default-auto-skills" << '\n';
    }

    {
      comet::controller::InteractionRequestValidator validator;
      comet::controller::InteractionRequestContext request_context;
      comet::controller::PlaneInteractionResolution resolution;
      resolution.desired_state = BuildDesiredState("interaction-plane", {});
      resolution.status_payload = json::object();
      const auto error = validator.ValidateAndNormalizeRequest(
          resolution,
          json{{"messages", json::array()},
               {"auto_skills", "yes"}},
          &request_context);
      Expect(
          error.has_value() && error->code == "malformed_request",
          "validator should reject non-boolean auto_skills");
      std::cout << "ok: interaction-validator-rejects-malformed-auto-skills" << '\n';
    }

    {
      comet::controller::InteractionRequestValidator validator;
      comet::controller::InteractionRequestContext request_context;
      comet::controller::PlaneInteractionResolution resolution;
      resolution.desired_state = BuildDesiredState("interaction-plane", {});
      resolution.status_payload = json::object();
      const auto error = validator.ValidateAndNormalizeRequest(
          resolution,
          json{{"messages", json::array()},
               {"auto_skills", false}},
          &request_context);
      Expect(!error.has_value(), "validator should accept auto_skills=false");
      Expect(
          !request_context.payload.at("auto_skills").get<bool>(),
          "validator should preserve explicit auto_skills=false");
      std::cout << "ok: interaction-validator-preserves-auto-skills-false" << '\n';
    }

    {
      comet::controller::InteractionSessionPresenter presenter;
      comet::controller::InteractionRequestContext request_context;
      request_context.request_id = "req-1";
      request_context.payload = json{
          {comet::controller::PlaneSkillsService::kAppliedSkillsPayloadKey,
           json::array({json{{"id", "skill-alpha"}, {"name", "safe-change"}}})},
          {comet::controller::PlaneSkillsService::kAutoAppliedSkillsPayloadKey,
           json::array({json{{"id", "skill-alpha"}, {"name", "safe-change"}}})},
          {comet::controller::PlaneSkillsService::kSkillResolutionModePayloadKey,
           "contextual"},
      };
      comet::controller::InteractionSessionResult result;
      result.session_id = "sess-1";
      result.model = "demo-model";
      result.content = "ok";
      result.completion_status = "completed";
      result.final_finish_reason = "stop";
      result.stop_reason = "natural_stop";
      comet::controller::PlaneInteractionResolution resolution;
      resolution.status_payload = json::object();
      const auto response = presenter.BuildResponseSpec(
          resolution,
          request_context,
          result);
      Expect(
          response.payload.at("auto_applied_skills").is_array(),
          "interaction response should expose auto_applied_skills");
      Expect(
          response.payload.at("skill_resolution_mode").get<std::string>() ==
              "contextual",
          "interaction response should expose skill_resolution_mode");
      std::cout << "ok: interaction-response-includes-skill-resolution-fields" << '\n';
    }

    return 0;
  } catch (const std::exception& error) {
    std::cerr << "plane_skills_service_tests failed: " << error.what() << '\n';
    return 1;
  }
}
