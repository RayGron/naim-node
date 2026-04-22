#include <cmath>
#include <filesystem>
#include <iostream>
#include <stdexcept>
#include <string>
#include <vector>

#include <nlohmann/json.hpp>

#include "bundle/bundle_cli_service.h"
#include "infra/controller_print_service.h"
#include "infra/controller_runtime_support_service.h"
#include "plane/desired_state_policy_service.h"
#include "plane/plane_realization_service.h"

#include "naim/state/state_json.h"
#include "naim/state/desired_state_v2_renderer.h"
#include "naim/state/desired_state_v2_validator.h"
#include "naim/state/sqlite_store.h"

namespace fs = std::filesystem;

namespace {

using nlohmann::json;

void Expect(bool condition, const std::string& message) {
  if (!condition) {
    throw std::runtime_error(message);
  }
}

naim::DesiredState BuildRenderedState(int max_num_seqs, bool enable_knowledge_skills = false) {
  json desired_state_v2{
      {"version", 2},
      {"plane_name", "apply-runtime-overrides"},
      {"plane_mode", "llm"},
      {"model",
       {
           {"source", {{"type", "local"}, {"path", "/models/qwen"}}},
           {"materialization", {{"mode", "reference"}, {"local_path", "/models/qwen"}}},
           {"served_model_name", "qwen-apply-runtime-overrides"},
       }},
      {"runtime",
       {
           {"engine", "llama.cpp"},
           {"distributed_backend", "llama_rpc"},
           {"workers", 1},
           {"max_model_len", 4096},
           {"llama_ctx_size", 4096},
           {"max_num_seqs", max_num_seqs},
           {"gpu_memory_utilization", 0.9},
       }},
      {"topology",
       {{"nodes",
         json::array(
              {{{"name", "gpu-hostd"},
                {"execution_mode", "mixed"},
                {"gpu_memory_mb", {{"0", 24576}}}}})}}},
      {"infer", {{"node", "gpu-hostd"}, {"replicas", 1}}},
      {"worker",
       {{"assignments",
         json::array(
             {{{"node", "gpu-hostd"}, {"gpu_device", "0"}}})}}},
      {"app", {{"enabled", false}}},
  };
  if (enable_knowledge_skills) {
    desired_state_v2["skills"] = {
        {"enabled", true},
        {"factory_skill_ids", json::array({"existing-skill"})},
    };
    desired_state_v2["knowledge"] = {
        {"enabled", true},
    };
  }
  naim::DesiredStateV2Validator::ValidateOrThrow(desired_state_v2);
  return naim::DesiredStateV2Renderer::Render(desired_state_v2);
}

}  // namespace

int main() {
  try {
    const fs::path temp_root =
        fs::temp_directory_path() / "naim-bundle-cli-service-tests";
    const fs::path db_path = temp_root / "controller.sqlite";
    const fs::path artifacts_root = temp_root / "artifacts";
    std::error_code error;
    fs::remove_all(temp_root, error);
    fs::create_directories(artifacts_root);

    naim::controller::ControllerRuntimeSupportService runtime_support;
    naim::controller::ControllerPrintService print_service(runtime_support);
    naim::controller::DesiredStatePolicyService desired_state_policy_service;
    naim::controller::PlaneRealizationService plane_realization_service(
        &runtime_support,
        300);
    naim::controller::BundleCliService bundle_cli_service(
        print_service,
        desired_state_policy_service,
        plane_realization_service,
        runtime_support,
        artifacts_root.string(),
        300);

    Expect(
        bundle_cli_service.ApplyDesiredState(
            db_path.string(),
            BuildRenderedState(16),
            artifacts_root.string(),
            "test-state-16") == 0,
        "first apply should succeed");
    Expect(
        bundle_cli_service.ApplyDesiredState(
            db_path.string(),
            BuildRenderedState(24),
            artifacts_root.string(),
            "test-state-24") == 0,
        "second apply should succeed");

    naim::ControllerStore store(db_path.string());
    store.Initialize();
    const auto stored_state = store.LoadDesiredState("apply-runtime-overrides");
    Expect(stored_state.has_value(), "stored desired state should exist");
    Expect(
        stored_state->inference.max_num_seqs == 24,
        "stored desired state should preserve updated max_num_seqs");
    Expect(
        stored_state->inference.llama_ctx_size == 4096,
        "stored desired state should preserve updated llama_ctx_size");
    Expect(
        stored_state->inference.max_model_len == 4096,
        "stored desired state should preserve updated max_model_len");
    Expect(
        std::abs(stored_state->inference.gpu_memory_utilization - 0.9) < 1e-9,
        "stored desired state should preserve updated gpu_memory_utilization");

    const auto serialized_v2 =
        nlohmann::json::parse(naim::SerializeDesiredStateV2Json(*stored_state));
    Expect(
        serialized_v2.at("runtime").at("max_num_seqs").get<int>() == 24,
        "projected desired-state.v2 should preserve updated max_num_seqs");

    Expect(
        bundle_cli_service.ApplyDesiredState(
            db_path.string(),
            BuildRenderedState(32, true),
            artifacts_root.string(),
            "test-knowledge-skills") == 0,
        "knowledge skills apply should succeed");
    const auto knowledge_state = store.LoadDesiredState("apply-runtime-overrides");
    Expect(knowledge_state.has_value(), "knowledge desired state should exist");
    Expect(
        knowledge_state->skills.has_value() &&
            knowledge_state->skills->factory_skill_ids ==
                std::vector<std::string>({
                    "existing-skill",
                    "knowledge-vault-replica-search",
                    "knowledge-vault-replica-answer-with-citations",
                    "knowledge-vault-replica-gap-check",
                }),
        "Knowledge Vault common skills should auto-attach during apply");
    Expect(
        store.LoadSkillsFactorySkill("knowledge-vault-replica-search").has_value(),
        "Knowledge Vault common skill should be seeded during apply");

    std::cout << "bundle_cli_service_tests passed\n";
    return 0;
  } catch (const std::exception& ex) {
    std::cerr << "bundle_cli_service_tests failed: " << ex.what() << '\n';
    return 1;
  }
}
