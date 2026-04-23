#include <algorithm>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <set>
#include <stdexcept>
#include <string>
#include <vector>

#include <nlohmann/json.hpp>

#include "naim/planning/planner.h"
#include "naim/planning/execution_plan.h"
#include "naim/runtime/infer_runtime_config.h"
#include "naim/state/state_json.h"
#include "naim/state/desired_state_v2_projector.h"
#include "naim/state/desired_state_v2_renderer.h"
#include "naim/state/desired_state_v2_validator.h"
#include "naim/state/worker_group_topology.h"

namespace {

using nlohmann::json;

naim::DesiredState RenderValid(const json& value, const std::string& name) {
  try {
    naim::DesiredStateV2Validator::ValidateOrThrow(value);
    auto state = naim::DesiredStateV2Renderer::Render(value);
    if (state.plane_name.empty()) {
      throw std::runtime_error("renderer returned empty plane_name");
    }
    return state;
  } catch (const std::exception& ex) {
    throw std::runtime_error("expected valid scenario '" + name + "': " + ex.what());
  }
}

void ExpectInvalid(const json& value, const std::string& name) {
  try {
    naim::DesiredStateV2Validator::ValidateOrThrow(value);
  } catch (const std::exception&) {
    std::cout << "ok-invalid: " << name << '\n';
    return;
  }
  throw std::runtime_error("expected invalid scenario '" + name + "' to fail validation");
}

void Expect(bool condition, const std::string& message) {
  if (!condition) {
    throw std::runtime_error(message);
  }
}

const naim::InstanceSpec& FindInstance(
    const naim::DesiredState& state,
    const std::string& name) {
  for (const auto& instance : state.instances) {
    if (instance.name == name) {
      return instance;
    }
  }
  throw std::runtime_error("instance not found: " + name);
}

naim::InstanceSpec& FindMutableInstance(
    naim::DesiredState* state,
    const std::string& name) {
  if (state == nullptr) {
    throw std::runtime_error("state is null");
  }
  for (auto& instance : state->instances) {
    if (instance.name == name) {
      return instance;
    }
  }
  throw std::runtime_error("instance not found: " + name);
}

const naim::DiskSpec& FindDisk(
    const naim::DesiredState& state,
    const std::string& name) {
  for (const auto& disk : state.disks) {
    if (disk.name == name) {
      return disk;
    }
  }
  throw std::runtime_error("disk not found: " + name);
}

const naim::DiskSpec& FindDiskOnNode(
    const naim::DesiredState& state,
    const std::string& name,
    const std::string& node_name) {
  for (const auto& disk : state.disks) {
    if (disk.name == name && disk.node_name == node_name) {
      return disk;
    }
  }
  throw std::runtime_error("disk not found: " + name + " on " + node_name);
}

}  // namespace

int main() {
  try {
    {
      const json knowledge_enabled{
          {"version", 2},
          {"plane_name", "knowledge-enabled"},
          {"plane_mode", "llm"},
          {"model",
           {
               {"source", {{"type", "local"}, {"path", "/models/qwen"}}},
               {"materialization", {{"mode", "reference"}, {"local_path", "/models/qwen"}}},
               {"served_model_name", "qwen-knowledge"},
           }},
          {"knowledge",
           {
               {"enabled", true},
               {"service_id", "kv_default"},
               {"selection_mode", "latest"},
               {"selected_knowledge_ids", json::array({"knowledge.alpha"})},
               {"context_policy",
                {{"include_graph", true}, {"max_graph_depth", 1}, {"token_budget", 12000}}},
           }},
          {"runtime",
           {{"engine", "llama.cpp"}, {"distributed_backend", "llama_rpc"}, {"workers", 1}}},
          {"infer", {{"replicas", 1}}},
          {"app", {{"enabled", false}}},
      };
      const auto state = RenderValid(knowledge_enabled, "knowledge-enabled");
      Expect(state.knowledge.has_value(), "knowledge-enabled: knowledge missing");
      Expect(
          state.knowledge->selected_knowledge_ids == std::vector<std::string>{"knowledge.alpha"},
          "knowledge-enabled: selected ids mismatch");
      std::cout << "ok: knowledge-enabled" << '\n';
    }

    {
      ExpectInvalid(
          json{
              {"version", 2},
              {"plane_name", "knowledge-compute"},
              {"plane_mode", "compute"},
              {"knowledge", {{"enabled", true}, {"selected_knowledge_ids", json::array()}}},
              {"runtime", {{"engine", "custom"}, {"workers", 1}}},
              {"app", {{"enabled", false}}},
          },
          "knowledge-compute");
    }

    {
      const json turboquant_defaults{
          {"version", 2},
          {"plane_name", "turboquant-defaults"},
          {"plane_mode", "llm"},
          {"model",
           {
               {"source", {{"type", "local"}, {"path", "/models/qwen"}}},
               {"materialization", {{"mode", "reference"}, {"local_path", "/models/qwen"}}},
               {"served_model_name", "qwen-turboquant"},
           }},
          {"features", {{"turboquant", {{"enabled", true}}}}},
          {"runtime",
           {{"engine", "llama.cpp"}, {"distributed_backend", "llama_rpc"}, {"workers", 1}}},
          {"infer", {{"replicas", 1}}},
          {"app", {{"enabled", false}}},
      };
      const auto state = RenderValid(turboquant_defaults, "turboquant-defaults");
      Expect(state.turboquant.has_value(), "turboquant-defaults: turboquant missing");
      Expect(state.turboquant->enabled, "turboquant-defaults: turboquant should be enabled");
      Expect(
          state.turboquant->cache_type_k == std::optional<std::string>("turbo4"),
          "turboquant-defaults: cache_type_k should default to turbo4");
      Expect(
          state.turboquant->cache_type_v == std::optional<std::string>("turbo4"),
          "turboquant-defaults: cache_type_v should default to turbo4");
      Expect(FindInstance(state, "infer-turboquant-defaults")
                     .environment.at("NAIM_LLAMA_RUNTIME_FLAVOR") == "turboquant",
             "turboquant-defaults: infer should use turboquant runtime flavor");
      Expect(FindInstance(state, "worker-turboquant-defaults")
                     .environment.at("NAIM_LLAMA_RUNTIME_FLAVOR") == "turboquant",
             "turboquant-defaults: worker should use turboquant runtime flavor");
      const auto runtime_config = json::parse(
          naim::RenderInferRuntimeConfigJsonForInstance(state, "infer-turboquant-defaults"));
      Expect(runtime_config.at("inference").at("runtime_flavor").get<std::string>() ==
                 "turboquant",
             "turboquant-defaults: infer runtime config should use turboquant flavor");
      std::cout << "ok: turboquant-defaults" << '\n';
    }

    {
      ExpectInvalid(
          json{
              {"version", 2},
              {"plane_name", "turboquant-invalid-enum"},
              {"plane_mode", "llm"},
              {"model",
               {
                   {"source", {{"type", "local"}, {"path", "/models/qwen"}}},
                   {"materialization", {{"mode", "reference"}, {"local_path", "/models/qwen"}}},
                   {"served_model_name", "qwen-invalid"},
               }},
              {"features",
               {{"turboquant",
                 {{"enabled", true}, {"cache_type_k", "broken3"}, {"cache_type_v", "f16"}}}}},
              {"runtime",
               {{"engine", "llama.cpp"},
                {"distributed_backend", "llama_rpc"},
                {"workers", 1}}},
              {"infer", {{"replicas", 1}}},
              {"app", {{"enabled", false}}},
          },
          "turboquant-invalid-enum");
    }

    {
      ExpectInvalid(
          json{
              {"version", 2},
              {"plane_name", "turboquant-compute"},
              {"plane_mode", "compute"},
              {"features", {{"turboquant", {{"enabled", true}}}}},
              {"runtime", {{"engine", "custom"}, {"workers", 1}}},
              {"app", {{"enabled", false}}},
          },
          "turboquant-compute");
    }

    {
      const json context_compression_defaults{
          {"version", 2},
          {"plane_name", "context-compression-defaults"},
          {"plane_mode", "llm"},
          {"model",
           {
               {"source", {{"type", "local"}, {"path", "/models/qwen"}}},
               {"materialization", {{"mode", "reference"}, {"local_path", "/models/qwen"}}},
               {"served_model_name", "qwen-context-compression"},
           }},
          {"features", {{"context_compression", {{"enabled", true}}}}},
          {"runtime",
           {{"engine", "llama.cpp"}, {"distributed_backend", "llama_rpc"}, {"workers", 1}}},
          {"infer", {{"replicas", 1}}},
          {"app", {{"enabled", false}}},
      };
      const auto state = RenderValid(
          context_compression_defaults,
          "context-compression-defaults");
      Expect(
          state.context_compression.has_value(),
          "context-compression-defaults: context_compression missing");
      Expect(
          state.context_compression->enabled,
          "context-compression-defaults: feature should be enabled");
      Expect(
          state.context_compression->mode == "auto",
          "context-compression-defaults: mode should default to auto");
      Expect(
          state.context_compression->target == "dialog_and_knowledge",
          "context-compression-defaults: target should default to dialog_and_knowledge");
      Expect(
          state.context_compression->memory_priority == "balanced",
          "context-compression-defaults: memory_priority should default to balanced");
      std::cout << "ok: context-compression-defaults" << '\n';
    }

    {
      ExpectInvalid(
          json{
              {"version", 2},
              {"plane_name", "context-compression-invalid-mode"},
              {"plane_mode", "llm"},
              {"model",
               {
                   {"source", {{"type", "local"}, {"path", "/models/qwen"}}},
                   {"materialization", {{"mode", "reference"}, {"local_path", "/models/qwen"}}},
                   {"served_model_name", "qwen-invalid-mode"},
               }},
              {"features",
               {{"context_compression",
                 {{"enabled", true},
                  {"mode", "manual"},
                  {"target", "dialog_and_knowledge"},
                  {"memory_priority", "balanced"}}}}},
              {"runtime",
               {{"engine", "llama.cpp"},
                {"distributed_backend", "llama_rpc"},
                {"workers", 1}}},
              {"infer", {{"replicas", 1}}},
              {"app", {{"enabled", false}}},
          },
          "context-compression-invalid-mode");
    }

    {
      ExpectInvalid(
          json{
              {"version", 2},
              {"plane_name", "context-compression-compute"},
              {"plane_mode", "compute"},
              {"features", {{"context_compression", {{"enabled", true}}}}},
              {"runtime", {{"engine", "custom"}, {"workers", 1}}},
              {"app", {{"enabled", false}}},
          },
          "context-compression-compute");
    }

    {
      const json state_file_v2{
          {"version", 2},
          {"plane_name", "thinking-flag"},
          {"plane_mode", "llm"},
          {"model",
           {
               {"source", {{"type", "local"}, {"path", "/models/qwen"}}},
               {"materialization", {{"mode", "reference"}, {"local_path", "/models/qwen"}}},
               {"served_model_name", "qwen-thinking"},
           }},
          {"interaction",
           {
               {"thinking_enabled", true},
               {"default_response_language", "ru"},
               {"follow_user_language", true},
               {"supported_response_languages", json::array({"ru", "en"})},
           }},
          {"runtime",
           {
               {"engine", "llama.cpp"},
               {"distributed_backend", "llama_rpc"},
               {"workers", 1},
           }},
          {"infer", {{"replicas", 1}}},
          {"app", {{"enabled", false}}},
      };
      const auto temp_path =
          std::filesystem::temp_directory_path() / "naim-state-v2-thinking-flag.json";
      {
        std::ofstream output(temp_path);
        output << state_file_v2.dump(2) << '\n';
      }
      const auto loaded = naim::LoadDesiredStateJson(temp_path.string());
      std::filesystem::remove(temp_path);
      Expect(loaded.has_value(), "state-file-v2-thinking: state should load");
      Expect(loaded->interaction.has_value(), "state-file-v2-thinking: interaction missing");
      Expect(loaded->interaction->thinking_enabled,
             "state-file-v2-thinking: thinking_enabled should survive apply-state-file path");
      std::cout << "ok: state-file-v2-thinking" << '\n';
    }

    {
      const json legacy_state_file{
          {"plane_name", "legacy-state-file"},
          {"plane_mode", "llm"},
      };
      const auto temp_path =
          std::filesystem::temp_directory_path() / "naim-state-legacy.json";
      {
        std::ofstream output(temp_path);
        output << legacy_state_file.dump(2) << '\n';
      }
      bool rejected = false;
      try {
        static_cast<void>(naim::LoadDesiredStateJson(temp_path.string()));
      } catch (const std::exception& ex) {
        rejected =
            std::string_view(ex.what()).find("version=2") != std::string_view::npos;
      }
      std::filesystem::remove(temp_path);
      Expect(rejected,
             "state-file-v2-only: legacy desired state should be rejected");
      std::cout << "ok-invalid: state-file-v2-only" << '\n';
    }

    {
      const json llm_backend_only{
          {"version", 2},
          {"plane_name", "maglev-backend"},
          {"plane_mode", "llm"},
          {"model",
           {
               {"source", {{"type", "local"}, {"path", "/models/qwen"}}},
               {"materialization", {{"mode", "reference"}, {"local_path", "/models/qwen"}}},
               {"served_model_name", "qwen-maglev"},
           }},
          {"runtime",
           {{"engine", "llama.cpp"}, {"distributed_backend", "llama_rpc"}, {"workers", 1}}},
          {"infer", {{"replicas", 1}}},
          {"network",
           {{"gateway_port", 18184}, {"inference_port", 18194}, {"server_name", "maglev"}}},
          {"app", {{"enabled", false}}},
      };
      const auto state = RenderValid(llm_backend_only, "llm-backend-only");
      Expect(state.bootstrap_model.has_value(), "llm-backend-only: bootstrap_model missing");
      Expect(state.bootstrap_model->local_path == std::optional<std::string>("/models/qwen"),
             "llm-backend-only: local path not rendered");
      Expect(state.worker_group.expected_workers == 1,
             "llm-backend-only: expected_workers should be 1");
      Expect(state.instances.size() == 3,
             "llm-backend-only: expected aggregator + leaf infer + worker");
      std::cout << "ok: llm-backend-only" << '\n';
    }

    {
      const json llama_ctx_runtime{
          {"version", 2},
          {"plane_name", "llama-ctx-runtime"},
          {"plane_mode", "llm"},
          {"model",
           {
               {"source", {{"type", "local"}, {"path", "/models/qwen"}}},
               {"materialization", {{"mode", "reference"}, {"local_path", "/models/qwen"}}},
               {"served_model_name", "qwen-ctx"},
           }},
          {"runtime",
           {{"engine", "llama.cpp"},
            {"distributed_backend", "llama_rpc"},
            {"workers", 1},
            {"max_model_len", 4096},
            {"llama_ctx_size", 4096}}},
          {"infer", {{"replicas", 1}}},
          {"app", {{"enabled", false}}},
      };
      const auto state = RenderValid(llama_ctx_runtime, "llama-ctx-runtime");
      Expect(state.inference.max_model_len == 4096,
             "llama-ctx-runtime: max_model_len should render");
      Expect(state.inference.llama_ctx_size == 4096,
             "llama-ctx-runtime: llama_ctx_size should render");
      const auto runtime_json =
          nlohmann::json::parse(naim::RenderInferRuntimeConfigJson(state));
      Expect(runtime_json.at("inference").at("max_model_len").get<int>() == 4096,
             "llama-ctx-runtime: runtime max_model_len mismatch");
      Expect(runtime_json.at("inference").at("llama_ctx_size").get<int>() == 4096,
             "llama-ctx-runtime: runtime llama_ctx_size mismatch");
      std::cout << "ok: llama-ctx-runtime" << '\n';
    }

    {
      const json llm_with_factory_skills{
          {"version", 2},
          {"plane_name", "llm-with-factory-skills"},
          {"plane_mode", "llm"},
          {"model",
           {
               {"source", {{"type", "local"}, {"path", "/models/qwen"}}},
               {"materialization", {{"mode", "reference"}, {"local_path", "/models/qwen"}}},
               {"served_model_name", "qwen-skills"},
           }},
          {"runtime",
           {{"engine", "llama.cpp"}, {"distributed_backend", "llama_rpc"}, {"workers", 1}}},
          {"infer", {{"replicas", 1}}},
          {"skills",
           {
               {"enabled", true},
               {"factory_skill_ids", json::array({"skill-alpha", "skill-beta"})},
           }},
          {"app", {{"enabled", false}}},
      };
      const auto state = RenderValid(llm_with_factory_skills, "llm-with-factory-skills");
      Expect(state.skills.has_value(), "llm-with-factory-skills: skills should render");
      Expect(
          state.skills->factory_skill_ids ==
              std::vector<std::string>({"skill-alpha", "skill-beta"}),
          "llm-with-factory-skills: factory_skill_ids should render");
      std::cout << "ok: llm-with-factory-skills" << '\n';
    }

    {
      ExpectInvalid(
          json{
              {"version", 2},
              {"plane_name", "duplicate-factory-skills"},
              {"plane_mode", "llm"},
              {"model",
               {
                   {"source", {{"type", "local"}, {"path", "/models/qwen"}}},
                   {"materialization", {{"mode", "reference"}, {"local_path", "/models/qwen"}}},
                   {"served_model_name", "qwen-dup"},
               }},
              {"runtime",
               {{"engine", "llama.cpp"},
                {"distributed_backend", "llama_rpc"},
                {"workers", 1}}},
              {"infer", {{"replicas", 1}}},
              {"skills",
               {
                   {"enabled", true},
                   {"factory_skill_ids", json::array({"skill-alpha", "skill-alpha"})},
               }},
              {"app", {{"enabled", false}}},
          },
          "duplicate-factory-skill-ids");
    }

    {
      ExpectInvalid(
          json{
              {"version", 2},
              {"plane_name", "factory-skills-without-enable"},
              {"plane_mode", "llm"},
              {"model",
               {
                   {"source", {{"type", "local"}, {"path", "/models/qwen"}}},
                   {"materialization", {{"mode", "reference"}, {"local_path", "/models/qwen"}}},
                   {"served_model_name", "qwen-disabled"},
               }},
              {"runtime",
               {{"engine", "llama.cpp"},
                {"distributed_backend", "llama_rpc"},
                {"workers", 1}}},
              {"infer", {{"replicas", 1}}},
              {"skills",
               {
                   {"enabled", false},
                   {"factory_skill_ids", json::array({"skill-alpha"})},
               }},
              {"app", {{"enabled", false}}},
          },
          "factory-skill-ids-require-enabled-skills");
    }

    {
      const json split_topology{
          {"version", 2},
          {"plane_name", "split-backend"},
          {"plane_mode", "llm"},
          {"model",
           {
               {"source",
                {{"type", "url"},
                 {"urls",
                  json::array(
                      {"https://example.invalid/model.part1",
                       "https://example.invalid/model.part2"})}}},
               {"materialization", {{"mode", "download"}}},
               {"served_model_name", "split-backend"},
               {"target_filename", "model.gguf"},
           }},
          {"runtime",
           {{"engine", "llama.cpp"}, {"distributed_backend", "llama_rpc"}, {"workers", 2}}},
          {"topology",
           {{"nodes",
             json::array(
                 {{{"name", "infer-hostd"}, {"execution_mode", "infer-only"}},
                  {{"name", "worker-hostd-a"},
                   {"execution_mode", "worker-only"},
                   {"gpu_memory_mb", {{"0", 24576}}}},
                  {{"name", "worker-hostd-b"},
                   {"execution_mode", "worker-only"},
                   {"gpu_memory_mb", {{"0", 24576}}}}})}}},
          {"infer", {{"node", "infer-hostd"}, {"replicas", 2}}},
          {"worker",
           {{"assignments",
             json::array(
                 {{{"node", "worker-hostd-a"}, {"gpu_device", "0"}},
                  {{"node", "worker-hostd-b"}, {"gpu_device", "0"}}})}}},
          {"app", {{"enabled", false}}},
      };
      const auto state = RenderValid(split_topology, "split-topology-with-url-parts");
      Expect(state.nodes.size() == 3, "split-topology: expected 3 nodes");
      Expect(state.bootstrap_model.has_value(), "split-topology: bootstrap_model missing");
      Expect(state.bootstrap_model->source_urls.size() == 2,
             "split-topology: source_urls should contain 2 items");
      Expect(state.bootstrap_model->target_filename == std::optional<std::string>("model.gguf"),
             "split-topology: target_filename mismatch");
      Expect(state.instances.size() == 5,
             "split-topology: expected aggregator + 2 leaf infers + 2 workers");
      Expect(FindInstance(state, "infer-split-backend").node_name == "infer-hostd",
             "split-topology: infer node mismatch");
      Expect(FindInstance(state, "infer-split-backend-a").node_name == "worker-hostd-a",
             "split-topology: leaf a infer node mismatch");
      Expect(FindInstance(state, "infer-split-backend-b").node_name == "worker-hostd-b",
             "split-topology: leaf b infer node mismatch");
      Expect(FindInstance(state, "worker-split-backend-a").node_name == "worker-hostd-a",
             "split-topology: worker 0 node mismatch");
      Expect(FindInstance(state, "worker-split-backend-b").node_name == "worker-hostd-b",
             "split-topology: worker 1 node mismatch");
      Expect(FindInstance(state, "worker-split-backend-a").gpu_device ==
                 std::optional<std::string>("0"),
             "split-topology: worker 0 gpu mismatch");
      Expect(state.disks.front().node_name == "infer-hostd",
             "split-topology: shared disk should follow infer node");
      Expect(FindDiskOnNode(state, "plane-split-backend-shared", "worker-hostd-a").kind ==
                 naim::DiskKind::PlaneShared,
             "split-topology: shared disk should be available on worker-hostd-a");
      Expect(FindDiskOnNode(state, "plane-split-backend-shared", "worker-hostd-b").kind ==
                 naim::DiskKind::PlaneShared,
             "split-topology: shared disk should be available on worker-hostd-b");
      const auto persisted_v2 = naim::DeserializeDesiredStateJson(
          naim::SerializeDesiredStateV2Json(state));
      Expect(persisted_v2.plane_name == state.plane_name,
             "split-topology: v2 persistence should preserve plane_name");
      Expect(persisted_v2.nodes.size() == state.nodes.size(),
             "split-topology: v2 persistence should preserve nodes after placement fallback");
      Expect(persisted_v2.instances.size() == state.instances.size(),
             "split-topology: v2 persistence should preserve instances after placement fallback");
      std::cout << "ok: split-topology-with-url-parts" << '\n';
    }

    {
      const json llama_rpc_backend{
          {"version", 2},
          {"plane_name", "llama-rpc-backend"},
          {"plane_mode", "llm"},
          {"model",
           {
               {"source", {{"type", "local"}, {"path", "/models/qwen"}}},
               {"materialization", {{"mode", "reference"}, {"local_path", "/models/qwen"}}},
               {"served_model_name", "qwen-rpc"},
           }},
          {"runtime",
           {
               {"engine", "llama.cpp"},
               {"distributed_backend", "llama_rpc"},
               {"workers", 2},
           }},
          {"infer", {{"replicas", 1}}},
          {"topology",
           {{"nodes",
             json::array(
                 {{{"name", "local-hostd"},
                   {"execution_mode", "mixed"},
                   {"gpu_memory_mb", {{"0", 24576}}}},
                  {{"name", "remote-hostd"},
                   {"execution_mode", "worker-only"},
                   {"gpu_memory_mb", {{"0", 24576}}}}})}}},
          {"worker",
           {{"assignments",
             json::array(
                 {{{"node", "local-hostd"}, {"gpu_device", "0"}},
                  {{"node", "remote-hostd"}, {"gpu_device", "0"}}})}}},
          {"app", {{"enabled", false}}},
      };
      const auto state = RenderValid(llama_rpc_backend, "llama-rpc-backend");
      Expect(state.worker_group.distributed_backend == "llama_rpc",
             "llama-rpc-backend: worker group backend mismatch");
      Expect(!state.worker_group.members.empty(),
             "llama-rpc-backend: expected at least one worker group member");
      const int expected_rpc_port = naim::StableLlamaRpcWorkerPort(
          state.plane_name,
          "worker-llama-rpc-backend-a");
      Expect(state.worker_group.members.front().rpc_port == expected_rpc_port,
             "llama-rpc-backend: worker rpc_port should use stable plane-scoped port");
      Expect(FindInstance(state, "worker-llama-rpc-backend-a")
                     .environment.at("NAIM_WORKER_BOOT_MODE") == "llama-rpc",
             "llama-rpc-backend: worker boot mode mismatch");
      Expect(FindInstance(state, "worker-llama-rpc-backend-a")
                     .environment.at("NAIM_LLAMA_RUNTIME_FLAVOR") == "default",
             "llama-rpc-backend: worker runtime flavor mismatch");
      Expect(FindInstance(state, "worker-llama-rpc-backend-a")
                     .environment.at("NAIM_WORKER_RPC_PORT") ==
                 std::to_string(expected_rpc_port),
             "llama-rpc-backend: worker rpc env mismatch");
      Expect(FindInstance(state, "infer-llama-rpc-backend")
                     .environment.at("NAIM_INFER_RUNTIME_BACKEND") == "llama-rpc-head",
             "llama-rpc-backend: infer backend mismatch");
      Expect(FindInstance(state, "infer-llama-rpc-backend")
                     .environment.at("NAIM_LLAMA_RUNTIME_FLAVOR") == "default",
             "llama-rpc-backend: infer runtime flavor mismatch");
      Expect(FindInstance(state, "infer-llama-rpc-backend")
                     .environment.at("NAIM_INSTANCE_SUBROLE") == "aggregator",
             "llama-rpc-backend: primary infer should be aggregator");
      Expect(FindInstance(state, "infer-llama-rpc-backend-a")
                     .environment.at("NAIM_INSTANCE_SUBROLE") == "infer",
             "llama-rpc-backend: leaf infer should be rendered for single replica");
      Expect(FindInstance(state, "worker-llama-rpc-backend-a")
                     .environment.at("NAIM_INFER_INSTANCE_NAME") == "infer-llama-rpc-backend-a",
             "llama-rpc-backend: worker should target leaf infer");
      Expect(
          FindInstance(state, "infer-llama-rpc-backend")
                  .environment.at("NAIM_INFER_RUNTIME_CONFIG") ==
              "/naim/shared/control/llama-rpc-backend/infer/infer-llama-rpc-backend/infer-runtime.json",
          "llama-rpc-backend: infer runtime config path mismatch");
      auto stale_state = state;
      auto& stale_worker = FindMutableInstance(&stale_state, "worker-llama-rpc-backend-a");
      stale_worker.environment["NAIM_WORKER_RPC_PORT"] = "29600";
      stale_worker.environment["NAIM_WORKER_RPC_ENDPOINT"] =
          "worker-llama-rpc-backend-a:29600";
      stale_state.worker_group.members.front().rpc_port = 29600;
      const auto stale_runtime_config = json::parse(
          naim::RenderInferRuntimeConfigJsonForInstance(
              stale_state,
              "infer-llama-rpc-backend-a"));
      Expect(stale_runtime_config.at("worker_group").at("members").at(0).at("rpc_port").get<int>() ==
                 expected_rpc_port,
             "llama-rpc-backend: infer runtime config should heal legacy worker rpc ports");
      std::cout << "ok: llama-rpc-backend" << '\n';
    }

    {
      const json inferred_head_placement{
          {"version", 2},
          {"plane_name", "llama-rpc-default-head"},
          {"plane_mode", "llm"},
          {"model",
           {
               {"source", {{"type", "local"}, {"path", "/models/qwen"}}},
               {"materialization", {{"mode", "reference"}, {"local_path", "/models/qwen"}}},
               {"served_model_name", "qwen-rpc-default-head"},
           }},
          {"runtime",
           {
               {"engine", "llama.cpp"},
               {"distributed_backend", "llama_rpc"},
               {"workers", 1},
           }},
          {"topology",
           {{"nodes",
             json::array(
                 {{{"name", "remote-worker-a"},
                   {"execution_mode", "worker-only"},
                   {"gpu_memory_mb", {{"0", 24576}}}},
                  {{"name", "local-head"},
                   {"execution_mode", "mixed"},
                   {"gpu_memory_mb", {{"0", 24576}}}}})}}},
          {"infer", {{"replicas", 1}}},
          {"worker",
           {{"assignments",
             json::array(
                 {{{"node", "remote-worker-a"}, {"gpu_device", "0"}}})}}},
          {"app", {{"enabled", false}}},
      };
      const auto state = RenderValid(inferred_head_placement, "llama-rpc-default-head");
      Expect(state.inference.primary_infer_node == "local-head",
             "llama-rpc-default-head: primary infer node should skip worker-only nodes");
      Expect(FindInstance(state, "infer-llama-rpc-default-head").node_name == "local-head",
             "llama-rpc-default-head: infer instance should land on non-worker node");
      std::cout << "ok: llama-rpc-default-head" << '\n';
    }

    {
      const json infer_port_overrides{
          {"version", 2},
          {"plane_name", "llama-rpc-ports"},
          {"plane_mode", "llm"},
          {"model",
           {
               {"source", {{"type", "local"}, {"path", "/models/qwen"}}},
               {"materialization", {{"mode", "reference"}, {"local_path", "/models/qwen"}}},
               {"served_model_name", "qwen-rpc-ports"},
           }},
          {"runtime",
           {
               {"engine", "llama.cpp"},
               {"distributed_backend", "llama_rpc"},
               {"workers", 1},
           }},
          {"infer",
           {{"replicas", 1},
            {"env",
             {
                 {"NAIM_INFERENCE_PORT", "19180"},
                 {"NAIM_GATEWAY_PORT", "19181"},
                 {"NAIM_LLAMA_PORT", "19182"},
             }}}},
          {"app", {{"enabled", false}}},
      };
      const auto state = RenderValid(infer_port_overrides, "llama-rpc-infer-ports");
      const auto infer_name = FindInstance(state, "infer-llama-rpc-ports").name;
      const auto runtime_config = json::parse(
          naim::RenderInferRuntimeConfigJsonForInstance(state, infer_name));
      Expect(runtime_config.at("inference").at("api_port").get<int>() == 19180,
             "llama-rpc-infer-ports: api_port override mismatch");
      Expect(runtime_config.at("inference").at("llama_port").get<int>() == 19182,
             "llama-rpc-infer-ports: llama_port override mismatch");
      Expect(runtime_config.at("gateway").at("listen_port").get<int>() == 19181,
             "llama-rpc-infer-ports: gateway port override mismatch");
      std::cout << "ok: llama-rpc-infer-ports" << '\n';
    }

    {
      const json llama_rpc_replica_upstreams{
          {"version", 2},
          {"plane_name", "llama-rpc-upstreams"},
          {"plane_mode", "llm"},
          {"model",
           {
               {"source", {{"type", "local"}, {"path", "/models/qwen"}}},
               {"materialization", {{"mode", "reference"}, {"local_path", "/models/qwen"}}},
               {"served_model_name", "qwen-rpc-upstreams"},
           }},
          {"runtime",
           {
               {"engine", "llama.cpp"},
               {"distributed_backend", "llama_rpc"},
               {"workers", 1},
           }},
          {"infer",
           {{"replicas", 1},
            {"env",
             {
                 {"NAIM_REPLICA_UPSTREAMS",
                  "http://127.0.0.1:19190,http://127.0.0.1:19191"},
             }}}},
          {"app", {{"enabled", false}}},
      };
      const auto state = RenderValid(llama_rpc_replica_upstreams, "llama-rpc-replica-upstreams");
      const auto infer_name = FindInstance(state, "infer-llama-rpc-upstreams").name;
      const auto runtime_config = json::parse(
          naim::RenderInferRuntimeConfigJsonForInstance(state, infer_name));
      const auto upstreams = runtime_config.at("replica_upstreams");
      Expect(upstreams.is_array() && upstreams.size() == 2,
             "llama-rpc-replica-upstreams: expected 2 upstreams");
      Expect(upstreams.at(0).get<std::string>() == "http://127.0.0.1:19190",
             "llama-rpc-replica-upstreams: first upstream mismatch");
      Expect(upstreams.at(1).get<std::string>() == "http://127.0.0.1:19191",
             "llama-rpc-replica-upstreams: second upstream mismatch");
      std::cout << "ok: llama-rpc-replica-upstreams" << '\n';
    }

    {
      const json llama_rpc_replicas{
          {"version", 2},
          {"plane_name", "llama-rpc-replicas"},
          {"plane_mode", "llm"},
          {"model",
           {
               {"source", {{"type", "local"}, {"path", "/models/qwen"}}},
               {"materialization", {{"mode", "reference"}, {"local_path", "/models/qwen"}}},
               {"served_model_name", "qwen-rpc-replicas"},
           }},
          {"runtime",
           {
               {"engine", "llama.cpp"},
               {"distributed_backend", "llama_rpc"},
               {"workers", 3},
           }},
          {"topology",
           {{"nodes",
             json::array(
                 {{{"name", "local-hostd"},
                   {"execution_mode", "mixed"},
                   {"gpu_memory_mb", {{"0", 24576}, {"1", 24576}, {"2", 24576}}}}})}}},
          {"infer", {{"replicas", 3}}},
          {"worker",
           {{"assignments",
             json::array(
                 {{{"node", "local-hostd"}, {"gpu_device", "0"}},
                  {{"node", "local-hostd"}, {"gpu_device", "1"}},
                  {{"node", "local-hostd"}, {"gpu_device", "2"}}})}}},
          {"app", {{"enabled", false}}},
      };
      const auto state = RenderValid(llama_rpc_replicas, "llama-rpc-replicas");
      Expect(FindInstance(state, "infer-llama-rpc-replicas").environment.count("NAIM_REPLICA_UPSTREAMS") == 1,
             "llama-rpc-replicas: aggregator should have NAIM_REPLICA_UPSTREAMS");
      Expect(FindInstance(state, "infer-llama-rpc-replicas-a").environment.at("NAIM_GATEWAY_PORT") == "81",
             "llama-rpc-replicas: first leaf gateway port mismatch");
      Expect(FindInstance(state, "worker-llama-rpc-replicas-a").environment.at("NAIM_INFER_INSTANCE_NAME") ==
                 "infer-llama-rpc-replicas-a",
             "llama-rpc-replicas: worker a infer binding mismatch");
      Expect(FindInstance(state, "worker-llama-rpc-replicas-c").environment.at("NAIM_INFER_INSTANCE_NAME") ==
                 "infer-llama-rpc-replicas-c",
             "llama-rpc-replicas: worker c infer binding mismatch");
      const auto runtime_config = json::parse(
          naim::RenderInferRuntimeConfigJsonForInstance(state, "infer-llama-rpc-replicas"));
      Expect(runtime_config.at("replica_upstreams").is_array() &&
                 runtime_config.at("replica_upstreams").size() == 3,
             "llama-rpc-replicas: aggregator runtime config should expose 3 replica upstreams");
      Expect(
          runtime_config.at("replica_upstreams").at(0).get<std::string>() ==
              "http://infer-llama-rpc-replicas-a:81",
          "llama-rpc-replicas: replica a upstream mismatch");
      Expect(
          runtime_config.at("replica_upstreams").at(1).get<std::string>() ==
              "http://infer-llama-rpc-replicas-b:82",
          "llama-rpc-replicas: replica b upstream mismatch");
      Expect(
          runtime_config.at("replica_upstreams").at(2).get<std::string>() ==
              "http://infer-llama-rpc-replicas-c:83",
          "llama-rpc-replicas: replica c upstream mismatch");
      const auto host_plans =
          naim::BuildNodeExecutionPlans(std::nullopt, state, "/tmp/naim-artifacts");
      Expect(host_plans.size() == 1,
             "llama-rpc-replicas: expected a single host plan for single-node topology");
      std::vector<std::string> write_runtime_details;
      for (const auto& operation : host_plans.front().operations) {
        if (operation.kind == naim::HostOperationKind::WriteInferRuntimeConfig) {
          write_runtime_details.push_back(operation.details);
        }
      }
      Expect(write_runtime_details.size() == 4,
             "llama-rpc-replicas: expected aggregator plus 3 leaf infer runtime writes");
      Expect(
          std::find(
              write_runtime_details.begin(),
              write_runtime_details.end(),
              "infer-llama-rpc-replicas") != write_runtime_details.end(),
          "llama-rpc-replicas: missing aggregator infer runtime write");
      Expect(
          std::find(
              write_runtime_details.begin(),
              write_runtime_details.end(),
              "infer-llama-rpc-replicas-a") != write_runtime_details.end(),
          "llama-rpc-replicas: missing replica a infer runtime write");
      Expect(
          std::find(
              write_runtime_details.begin(),
              write_runtime_details.end(),
              "infer-llama-rpc-replicas-b") != write_runtime_details.end(),
          "llama-rpc-replicas: missing replica b infer runtime write");
      Expect(
          std::find(
              write_runtime_details.begin(),
              write_runtime_details.end(),
              "infer-llama-rpc-replicas-c") != write_runtime_details.end(),
          "llama-rpc-replicas: missing replica c infer runtime write");
      auto current_state = state;
      for (auto& instance : current_state.instances) {
        if (instance.name == "infer-llama-rpc-replicas-b") {
          instance.image = "naim/infer-runtime:previous";
        }
      }
      const auto refresh_plans =
          naim::BuildNodeExecutionPlans(current_state, state, "/tmp/naim-artifacts");
      Expect(refresh_plans.size() == 1,
             "llama-rpc-replicas: expected host plan when infer service image changes");
      std::vector<std::string> refresh_runtime_details;
      bool saw_compose_up = false;
      for (const auto& operation : refresh_plans.front().operations) {
        if (operation.kind == naim::HostOperationKind::WriteInferRuntimeConfig) {
          refresh_runtime_details.push_back(operation.details);
        }
        if (operation.kind == naim::HostOperationKind::ComposeUp) {
          saw_compose_up = true;
        }
      }
      Expect(saw_compose_up,
             "llama-rpc-replicas: expected compose up when infer service image changes");
      Expect(refresh_runtime_details.size() == 4,
             "llama-rpc-replicas: compose refresh should rewrite aggregator plus 3 leaf infer runtimes");
      Expect(
          std::find(
              refresh_runtime_details.begin(),
              refresh_runtime_details.end(),
              "infer-llama-rpc-replicas") != refresh_runtime_details.end(),
          "llama-rpc-replicas: compose refresh missing aggregator infer runtime write");
      Expect(
          std::find(
              refresh_runtime_details.begin(),
              refresh_runtime_details.end(),
              "infer-llama-rpc-replicas-a") != refresh_runtime_details.end(),
          "llama-rpc-replicas: compose refresh missing replica a infer runtime write");
      Expect(
          std::find(
              refresh_runtime_details.begin(),
              refresh_runtime_details.end(),
              "infer-llama-rpc-replicas-b") != refresh_runtime_details.end(),
          "llama-rpc-replicas: compose refresh missing replica b infer runtime write");
      Expect(
          std::find(
              refresh_runtime_details.begin(),
              refresh_runtime_details.end(),
              "infer-llama-rpc-replicas-c") != refresh_runtime_details.end(),
          "llama-rpc-replicas: compose refresh missing replica c infer runtime write");
      const auto compose_plans = naim::BuildNodeComposePlans(state);
      Expect(compose_plans.size() == 1,
             "llama-rpc-replicas: expected a single compose plan for single-node topology");
      std::set<int> seen_infer_gateway_ports;
      std::set<int> seen_worker_rpc_ports;
      const std::set<int> expected_worker_rpc_ports{
          naim::StableLlamaRpcWorkerPort(state.plane_name, "worker-llama-rpc-replicas-a"),
          naim::StableLlamaRpcWorkerPort(state.plane_name, "worker-llama-rpc-replicas-b"),
          naim::StableLlamaRpcWorkerPort(state.plane_name, "worker-llama-rpc-replicas-c"),
      };
      bool saw_aggregator = false;
      bool saw_leaf_a = false;
      bool saw_leaf_b = false;
      bool saw_leaf_c = false;
      for (const auto& service : compose_plans.front().services) {
        if (service.name.rfind("infer-llama-rpc-replicas", 0) == 0) {
          std::set<int> host_ports;
          for (const auto& port : service.published_ports) {
            host_ports.insert(port.host_port);
          }
          if (service.name == "infer-llama-rpc-replicas") {
            saw_aggregator = true;
            Expect(host_ports.count(80) == 1 && host_ports.count(8000) == 1,
                   "llama-rpc-replicas: aggregator should publish default gateway/api ports");
          } else if (service.name == "infer-llama-rpc-replicas-a") {
            saw_leaf_a = true;
            Expect(host_ports.count(81) == 1 && host_ports.count(8001) == 1,
                   "llama-rpc-replicas: leaf a ports mismatch");
            seen_infer_gateway_ports.insert(81);
          } else if (service.name == "infer-llama-rpc-replicas-b") {
            saw_leaf_b = true;
            Expect(host_ports.count(82) == 1 && host_ports.count(8002) == 1,
                   "llama-rpc-replicas: leaf b ports mismatch");
            seen_infer_gateway_ports.insert(82);
          } else if (service.name == "infer-llama-rpc-replicas-c") {
            saw_leaf_c = true;
            Expect(host_ports.count(83) == 1 && host_ports.count(8003) == 1,
                   "llama-rpc-replicas: leaf c ports mismatch");
            seen_infer_gateway_ports.insert(83);
          }
        }
        if (service.name == "worker-llama-rpc-replicas-a") {
          Expect(service.published_ports.size() == 1,
                 "llama-rpc-replicas: worker a should publish one rpc port");
          seen_worker_rpc_ports.insert(service.published_ports.front().host_port);
        } else if (service.name == "worker-llama-rpc-replicas-b") {
          Expect(service.published_ports.size() == 1,
                 "llama-rpc-replicas: worker b should publish one rpc port");
          seen_worker_rpc_ports.insert(service.published_ports.front().host_port);
        } else if (service.name == "worker-llama-rpc-replicas-c") {
          Expect(service.published_ports.size() == 1,
                 "llama-rpc-replicas: worker c should publish one rpc port");
          seen_worker_rpc_ports.insert(service.published_ports.front().host_port);
        }
      }
      Expect(saw_aggregator && saw_leaf_a && saw_leaf_b && saw_leaf_c,
             "llama-rpc-replicas: expected compose services for aggregator and all leaf replicas");
      Expect(seen_infer_gateway_ports.size() == 3,
             "llama-rpc-replicas: leaf gateway ports should be unique");
      Expect(seen_worker_rpc_ports == expected_worker_rpc_ports,
             "llama-rpc-replicas: worker rpc ports should use stable plane-scoped ports");
      auto stale_state = state;
      {
        auto& worker_a = FindMutableInstance(&stale_state, "worker-llama-rpc-replicas-a");
        worker_a.environment["NAIM_WORKER_RPC_PORT"] = "29600";
        worker_a.environment["NAIM_WORKER_RPC_ENDPOINT"] =
            "worker-llama-rpc-replicas-a:29600";
        auto& worker_b = FindMutableInstance(&stale_state, "worker-llama-rpc-replicas-b");
        worker_b.environment["NAIM_WORKER_RPC_PORT"] = "29601";
        worker_b.environment["NAIM_WORKER_RPC_ENDPOINT"] =
            "worker-llama-rpc-replicas-b:29601";
        auto& worker_c = FindMutableInstance(&stale_state, "worker-llama-rpc-replicas-c");
        worker_c.environment["NAIM_WORKER_RPC_PORT"] = "29602";
        worker_c.environment["NAIM_WORKER_RPC_ENDPOINT"] =
            "worker-llama-rpc-replicas-c:29602";
      }
      stale_state.worker_group.members.at(0).rpc_port = 29600;
      stale_state.worker_group.members.at(1).rpc_port = 29601;
      stale_state.worker_group.members.at(2).rpc_port = 29602;
      const auto stale_compose_plans = naim::BuildNodeComposePlans(stale_state);
      std::set<int> healed_worker_rpc_ports;
      for (const auto& service : stale_compose_plans.front().services) {
        if (service.name.rfind("worker-llama-rpc-replicas-", 0) != 0) {
          continue;
        }
        Expect(service.published_ports.size() == 1,
               "llama-rpc-replicas: healed worker should publish one rpc port");
        healed_worker_rpc_ports.insert(service.published_ports.front().host_port);
        Expect(service.environment.at("NAIM_WORKER_RPC_PORT") ==
                   std::to_string(service.published_ports.front().host_port),
               "llama-rpc-replicas: healed worker env should match published rpc port");
      }
      Expect(healed_worker_rpc_ports == expected_worker_rpc_ports,
             "llama-rpc-replicas: compose plans should heal legacy worker rpc ports");
      std::cout << "ok: llama-rpc-replicas" << '\n';
    }

    {
      const auto build_plane = [](const std::string& plane_name) {
        return json{
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
            {"app", {{"enabled", false}}},
        };
      };
      const auto alpha = RenderValid(build_plane("alpha-plane"), "llama-rpc-alpha-plane");
      const auto beta = RenderValid(build_plane("beta-plane"), "llama-rpc-beta-plane");
      const int alpha_rpc_port = alpha.worker_group.members.front().rpc_port;
      const int beta_rpc_port = beta.worker_group.members.front().rpc_port;
      Expect(alpha_rpc_port != beta_rpc_port,
             "llama-rpc-plane-port-isolation: separate planes should not share worker rpc ports");
      std::cout << "ok: llama-rpc-plane-port-isolation" << '\n';
    }

    {
      const json gpu_worker{
          {"version", 2},
          {"plane_name", "gpu-job"},
          {"plane_mode", "compute"},
          {"runtime", {{"engine", "custom"}, {"workers", 2}}},
          {"worker",
           {
               {"image", "nvidia/cuda:12.9.1-runtime-ubuntu24.04"},
               {"start", {{"type", "command"}, {"command", "bash /app/run.sh"}}},
           }},
          {"app", {{"enabled", false}}},
      };
      const auto state = RenderValid(gpu_worker, "gpu-worker");
      Expect(!state.bootstrap_model.has_value(), "gpu-worker: model should be absent");
      Expect(state.instances.size() == 2, "gpu-worker: expected 2 worker instances only");
      Expect(state.worker_group.expected_workers == 2,
             "gpu-worker: expected_workers should match runtime.workers");
      const auto persisted_v2 = naim::DeserializeDesiredStateJson(
          naim::SerializeDesiredStateV2Json(state));
      Expect(persisted_v2.plane_name == state.plane_name,
             "gpu-worker: v2 persistence should preserve plane_name");
      Expect(!persisted_v2.nodes.empty(),
             "gpu-worker: v2 persistence should preserve nodes");
      Expect(persisted_v2.instances.size() == state.instances.size(),
             "gpu-worker: v2 persistence should preserve instances");
      std::cout << "ok: gpu-worker" << '\n';
    }

    {
      const json llm_with_app{
          {"version", 2},
          {"plane_name", "llm-app"},
          {"plane_mode", "llm"},
          {"model",
           {
               {"source",
                {{"type", "local"},
                 {"path", "/models/qwen/Qwen3.5-9B-Q4_K_M.gguf"},
                 {"ref", "Qwen/Qwen3.5-9B"}}},
               {"materialization",
                {{"mode", "reference"},
                 {"local_path", "/models/qwen/Qwen3.5-9B-Q4_K_M.gguf"}}},
               {"served_model_name", "qwen-app"},
           }},
          {"runtime",
           {{"engine", "llama.cpp"}, {"distributed_backend", "llama_rpc"}, {"workers", 1}}},
          {"infer", {{"replicas", 1}}},
          {"network",
           {{"gateway_port", 18084}, {"inference_port", 18094}, {"server_name", "llm-app"}}},
          {"app",
           {
               {"enabled", true},
               {"image", "example/app:dev"},
               {"start", {{"type", "script"}, {"script_ref", "bundle://deploy/run.sh"}}},
           }},
      };
      const auto state = RenderValid(llm_with_app, "llm-with-app");
      Expect(state.instances.size() == 4,
             "llm-with-app: expected app + aggregator + leaf infer + worker");
      Expect(FindInstance(state, "app-llm-app").image == "example/app:dev",
             "llm-with-app: app image mismatch");
      std::cout << "ok: llm-with-app" << '\n';
    }

    {
      const json llm_without_infer_storage{
          {"version", 2},
          {"plane_name", "llm-no-infer-private"},
          {"plane_mode", "llm"},
          {"model",
           {
               {"source", {{"type", "local"}, {"path", "/models/qwen"}}},
               {"materialization", {{"mode", "reference"}, {"local_path", "/models/qwen"}}},
               {"served_model_name", "qwen-no-private"},
           }},
          {"runtime",
           {{"engine", "llama.cpp"}, {"distributed_backend", "llama_rpc"}, {"workers", 1}}},
          {"infer", {{"replicas", 1}}},
          {"app", {{"enabled", false}}},
      };
      const auto state = RenderValid(llm_without_infer_storage, "llm-no-infer-private");
      Expect(
          std::none_of(
              state.disks.begin(),
              state.disks.end(),
              [](const naim::DiskSpec& disk) { return disk.kind == naim::DiskKind::InferPrivate; }),
          "llm-no-infer-private: infer private disk should be absent by default");
      const auto worker_disk_it = std::find_if(
          state.disks.begin(),
          state.disks.end(),
          [](const naim::DiskSpec& disk) { return disk.kind == naim::DiskKind::WorkerPrivate; });
      Expect(worker_disk_it != state.disks.end(),
             "llm-no-infer-private: worker private disk should still exist");
      Expect(worker_disk_it->size_gb == 2,
             "llm-no-infer-private: worker private disk default size should be 2 GB");

      const auto compose_plans = naim::BuildNodeComposePlans(state);
      const auto infer_it = std::find_if(
          state.instances.begin(),
          state.instances.end(),
          [](const naim::InstanceSpec& instance) { return instance.role == naim::InstanceRole::Infer; });
      Expect(infer_it != state.instances.end(),
             "llm-no-infer-private: infer instance should exist");
      const auto service_it = std::find_if(
          compose_plans.front().services.begin(),
          compose_plans.front().services.end(),
          [&](const naim::ComposeService& service) { return service.name == infer_it->name; });
      Expect(service_it != compose_plans.front().services.end(),
             "llm-no-infer-private: infer service missing from compose plan");
      Expect(
          std::none_of(
              service_it->volumes.begin(),
              service_it->volumes.end(),
              [](const naim::ComposeVolume& volume) { return volume.target == "/naim/private"; }),
          "llm-no-infer-private: infer compose service should not mount /naim/private by default");
      std::cout << "ok: llm-no-infer-private" << '\n';
    }

    {
      const json llm_with_skills{
          {"version", 2},
          {"plane_name", "llm-with-skills"},
          {"plane_mode", "llm"},
          {"model",
           {
               {"source", {{"type", "local"}, {"path", "/models/qwen"}}},
               {"materialization", {{"mode", "reference"}, {"local_path", "/models/qwen"}}},
               {"served_model_name", "qwen-skills"},
           }},
          {"runtime",
           {{"engine", "llama.cpp"}, {"distributed_backend", "llama_rpc"}, {"workers", 1}}},
          {"infer", {{"replicas", 1}}},
          {"skills",
           {
               {"enabled", true},
               {"image", "example/skills:dev"},
               {"env", {{"NAIM_CUSTOM_SKILLS_FLAG", "enabled"}}},
               {"storage", {{"size_gb", 9}, {"mount_path", "/srv/skills"}}},
               {"publish",
                json::array(
                    {{{"host_ip", "127.0.0.1"}, {"host_port", 19120}, {"container_port", 18120}}})},
           }},
          {"app", {{"enabled", false}}},
      };
      const auto state = RenderValid(llm_with_skills, "llm-with-skills");
      Expect(state.skills.has_value() && state.skills->enabled,
             "llm-with-skills: state.skills.enabled should be true");
      Expect(state.instances.size() == 4,
             "llm-with-skills: expected aggregator + leaf infer + worker + skills");
      const auto skills = FindInstance(state, "skills-llm-with-skills");
      Expect(skills.role == naim::InstanceRole::Skills,
             "llm-with-skills: skills instance role mismatch");
      Expect(skills.image == "example/skills:dev",
             "llm-with-skills: custom skills image mismatch");
      Expect(skills.environment.count("NAIM_CUSTOM_SKILLS_FLAG") == 1 &&
                 skills.environment.at("NAIM_CUSTOM_SKILLS_FLAG") == "enabled",
             "llm-with-skills: custom skills env mismatch");
      Expect(!skills.published_ports.empty() &&
                 skills.published_ports.front().host_port == 19120 &&
                 skills.published_ports.front().container_port == 18120,
             "llm-with-skills: published port mismatch");
      const auto skills_disk = FindDisk(state, "skills-llm-with-skills-private");
      Expect(skills_disk.kind == naim::DiskKind::SkillsPrivate,
             "llm-with-skills: skills disk kind mismatch");
      Expect(skills_disk.container_path == "/srv/skills",
             "llm-with-skills: skills disk mount path mismatch");
      Expect(skills_disk.size_gb == 9,
             "llm-with-skills: skills disk size mismatch");

      const auto compose_plans = naim::BuildNodeComposePlans(state);
      Expect(compose_plans.size() == 1,
             "llm-with-skills: expected a single compose plan for single-node topology");
      const auto service_it = std::find_if(
          compose_plans.front().services.begin(),
          compose_plans.front().services.end(),
          [](const naim::ComposeService& service) {
            return service.name == "skills-llm-with-skills";
          });
      Expect(service_it != compose_plans.front().services.end(),
             "llm-with-skills: skills service missing from compose plan");
      Expect(service_it->healthcheck == "CMD-SHELL test -f /tmp/naim-ready",
             "llm-with-skills: compose healthcheck should use readiness file");
      Expect(
          std::none_of(
              service_it->volumes.begin(),
              service_it->volumes.end(),
              [](const naim::ComposeVolume& volume) { return volume.target == "/naim/shared"; }),
          "llm-with-skills: skills service should not mount shared disk by default");
      std::cout << "ok: llm-with-skills" << '\n';
    }

    {
      const json llm_with_browsing{
          {"version", 2},
          {"plane_name", "llm-with-browsing"},
          {"plane_mode", "llm"},
          {"model",
           {
               {"source", {{"type", "local"}, {"path", "/models/qwen"}}},
               {"materialization", {{"mode", "reference"}, {"local_path", "/models/qwen"}}},
               {"served_model_name", "qwen-browsing"},
           }},
          {"runtime",
           {{"engine", "llama.cpp"}, {"distributed_backend", "llama_rpc"}, {"workers", 1}}},
          {"infer", {{"replicas", 1}}},
          {"browsing",
           {
               {"enabled", true},
               {"node", "browse-hostd"},
               {"image", "example/webgateway:dev"},
               {"env", {{"NAIM_WEBGATEWAY_DEBUG", "1"}}},
               {"policy",
                {{"browser_session_enabled", true},
                 {"rendered_browser_enabled", false},
                 {"login_enabled", false},
                 {"allowed_domains", json::array({"example.com", "openai.com"})},
                 {"blocked_domains", json::array({"localhost", "internal"})},
                 {"max_search_results", 5},
                 {"max_fetch_bytes", 16384}}},
               {"publish",
                json::array(
                    {{{"host_ip", "127.0.0.1"}, {"host_port", 19130}, {"container_port", 18130}}})},
               {"storage", {{"size_gb", 7}, {"mount_path", "/srv/browsing"}}},
           }},
          {"topology",
           {{"nodes",
             json::array(
                 {{{"name", "infer-hostd"},
                   {"execution_mode", "mixed"},
                   {"gpu_memory_mb", {{"0", 24576}}}},
                  {{"name", "browse-hostd"},
                   {"execution_mode", "mixed"},
                   {"gpu_memory_mb", {{"1", 24576}}}}})}}},
          {"app", {{"enabled", false}}},
      };
      const auto state = RenderValid(llm_with_browsing, "llm-with-browsing");
      Expect(state.browsing.has_value() && state.browsing->enabled,
             "llm-with-browsing: state.browsing.enabled should be true");
      Expect(state.instances.size() == 4,
             "llm-with-browsing: expected aggregator + leaf infer + worker + browsing");
      const auto browsing = FindInstance(state, "webgateway-llm-with-browsing");
      Expect(browsing.role == naim::InstanceRole::Browsing,
             "llm-with-browsing: browsing instance role mismatch");
      Expect(browsing.image == "example/webgateway:dev",
             "llm-with-browsing: custom browsing image mismatch");
      Expect(browsing.environment.count("NAIM_WEBGATEWAY_DEBUG") == 1 &&
                 browsing.environment.at("NAIM_WEBGATEWAY_DEBUG") == "1",
             "llm-with-browsing: custom browsing env mismatch");
      Expect(browsing.environment.count("NAIM_WEBGATEWAY_POLICY_JSON") == 1,
             "llm-with-browsing: browsing policy env should be rendered");
      Expect(!browsing.published_ports.empty() &&
                 browsing.published_ports.front().host_port == 19130 &&
                 browsing.published_ports.front().container_port == 18130,
             "llm-with-browsing: published port mismatch");
      const auto browsing_disk = FindDisk(state, "webgateway-llm-with-browsing-private");
      Expect(browsing_disk.kind == naim::DiskKind::BrowsingPrivate,
             "llm-with-browsing: browsing disk kind mismatch");
      Expect(browsing_disk.container_path == "/srv/browsing",
             "llm-with-browsing: browsing disk mount path mismatch");
      Expect(browsing_disk.size_gb == 7,
             "llm-with-browsing: browsing disk size mismatch");

      const auto compose_plans = naim::BuildNodeComposePlans(state);
      Expect(compose_plans.size() == 2,
             "llm-with-browsing: expected separate compose plans for infer and browsing nodes");
      const auto browse_plan_it = std::find_if(
          compose_plans.begin(),
          compose_plans.end(),
          [](const naim::NodeComposePlan& plan) { return plan.node_name == "browse-hostd"; });
      Expect(browse_plan_it != compose_plans.end(),
             "llm-with-browsing: browsing node compose plan missing");
      const auto service_it = std::find_if(
          browse_plan_it->services.begin(),
          browse_plan_it->services.end(),
          [](const naim::ComposeService& service) {
            return service.name == "webgateway-llm-with-browsing";
          });
      Expect(service_it != browse_plan_it->services.end(),
             "llm-with-browsing: browsing service missing from compose plan");
      Expect(service_it->healthcheck == "CMD-SHELL test -f /tmp/naim-ready",
             "llm-with-browsing: compose healthcheck should use readiness file");
      Expect(
          std::find(
              service_it->security_opts.begin(),
              service_it->security_opts.end(),
              "no-new-privileges:true") != service_it->security_opts.end(),
          "llm-with-browsing: compose service should enable no-new-privileges");
      Expect(
          std::find(
              service_it->security_opts.begin(),
              service_it->security_opts.end(),
              "apparmor=unconfined") != service_it->security_opts.end(),
          "llm-with-browsing: compose service should relax apparmor for CEF");
      Expect(
          std::find(
              service_it->security_opts.begin(),
              service_it->security_opts.end(),
              "seccomp=unconfined") != service_it->security_opts.end(),
          "llm-with-browsing: compose service should relax seccomp for CEF");
      Expect(
          std::none_of(
              service_it->volumes.begin(),
              service_it->volumes.end(),
              [](const naim::ComposeVolume& volume) { return volume.target == "/naim/shared"; }),
          "llm-with-browsing: browsing service should not mount shared disk by default");
      std::cout << "ok: llm-with-browsing" << '\n';
    }

    {
      const json llm_with_webgateway_public_publish{
          {"version", 2},
          {"plane_name", "webgateway-public-publish"},
          {"plane_mode", "llm"},
          {"model",
           {
               {"source", {{"type", "local"}, {"path", "/models/qwen"}}},
               {"materialization", {{"mode", "reference"}, {"local_path", "/models/qwen"}}},
               {"served_model_name", "qwen-webgateway-public"},
           }},
          {"runtime",
           {{"engine", "llama.cpp"}, {"distributed_backend", "llama_rpc"}, {"workers", 1}}},
          {"infer", {{"replicas", 1}}},
          {"webgateway",
           {
               {"enabled", true},
               {"publish",
                json::array(
                    {{{"host_ip", "0.0.0.0"}, {"host_port", 19130}, {"container_port", 18130}}})},
           }},
          {"topology",
           {{"nodes",
             json::array(
                 {{{"name", "infer-hostd"},
                   {"execution_mode", "mixed"},
                   {"gpu_memory_mb", {{"0", 24576}}}}})}}},
          {"app", {{"enabled", false}}},
      };
      ExpectInvalid(
          llm_with_webgateway_public_publish,
          "webgateway-public-publish");
    }

    ExpectInvalid(
        json{
            {"version", 2},
            {"plane_name", "missing-model"},
            {"plane_mode", "llm"},
            {"runtime",
             {{"engine", "llama.cpp"}, {"distributed_backend", "llama_rpc"}, {"workers", 1}}},
            {"infer", {{"replicas", 1}}},
        },
        "llm-without-model");

    ExpectInvalid(
        json{
            {"version", 2},
            {"plane_name", "missing-replicas"},
            {"plane_mode", "llm"},
            {"model",
             {
                 {"source", {{"type", "local"}, {"path", "/models/qwen"}}},
                 {"materialization", {{"mode", "reference"}, {"local_path", "/models/qwen"}}},
                 {"served_model_name", "qwen"},
             }},
            {"runtime",
             {{"engine", "llama.cpp"}, {"distributed_backend", "llama_rpc"}, {"workers", 1}}},
        },
        "llm-without-infer-replicas");

    ExpectInvalid(
        json{
            {"version", 2},
            {"plane_name", "bad-assignments"},
            {"plane_mode", "compute"},
            {"runtime", {{"engine", "custom"}, {"workers", 2}}},
            {"worker",
             {{"assignments",
               json::array({{{"node", "worker-hostd-a"}, {"gpu_device", "0"}}})}}},
        },
        "worker-assignments-size-mismatch");

    ExpectInvalid(
        json{
            {"version", 2},
            {"plane_name", "bad-llama-rpc-dp"},
            {"plane_mode", "llm"},
            {"model",
             {
                 {"source", {{"type", "local"}, {"path", "/models/qwen"}}},
                 {"materialization", {{"mode", "reference"}, {"local_path", "/models/qwen"}}},
                 {"served_model_name", "qwen-bad"},
             }},
            {"runtime",
             {
                 {"engine", "llama.cpp"},
                 {"distributed_backend", "llama_rpc"},
                 {"workers", 2},
                 {"data_parallel_mode", "auto_replicas"},
             }},
        },
        "llama-rpc-with-data-parallel-mode");

    ExpectInvalid(
        json{
            {"version", 2},
            {"plane_name", "compute-with-browsing"},
            {"plane_mode", "compute"},
            {"runtime", {{"engine", "custom"}, {"workers", 1}}},
            {"browsing", {{"enabled", true}}},
            {"app", {{"enabled", false}}},
        },
        "compute-plane-with-browsing");

    ExpectInvalid(
        json{
            {"version", 2},
            {"plane_name", "browsing-dup-domain"},
            {"plane_mode", "llm"},
            {"model",
             {
                 {"source", {{"type", "local"}, {"path", "/models/qwen"}}},
                 {"materialization", {{"mode", "reference"}, {"local_path", "/models/qwen"}}},
                 {"served_model_name", "qwen-browsing-dup-domain"},
             }},
            {"runtime",
             {{"engine", "llama.cpp"}, {"distributed_backend", "llama_rpc"}, {"workers", 1}}},
            {"infer", {{"replicas", 1}}},
            {"browsing",
             {{"enabled", true},
              {"policy", {{"allowed_domains", json::array({"example.com", "example.com"})}}}}},
            {"app", {{"enabled", false}}},
        },
        "browsing-duplicate-allowed-domain");

    ExpectInvalid(
        json{
            {"version", 2},
            {"plane_name", "browsing-bad-search-limit"},
            {"plane_mode", "llm"},
            {"model",
             {
                 {"source", {{"type", "local"}, {"path", "/models/qwen"}}},
                 {"materialization", {{"mode", "reference"}, {"local_path", "/models/qwen"}}},
                 {"served_model_name", "qwen-browsing-bad-search-limit"},
             }},
            {"runtime",
             {{"engine", "llama.cpp"}, {"distributed_backend", "llama_rpc"}, {"workers", 1}}},
            {"infer", {{"replicas", 1}}},
            {"browsing",
             {{"enabled", true}, {"policy", {{"max_search_results", 0}}}}},
            {"app", {{"enabled", false}}},
        },
        "browsing-invalid-max-search-results");

    ExpectInvalid(
        json{
            {"version", 2},
            {"plane_name", "browsing-worker-node"},
            {"plane_mode", "llm"},
            {"model",
             {
                 {"source", {{"type", "local"}, {"path", "/models/qwen"}}},
                 {"materialization", {{"mode", "reference"}, {"local_path", "/models/qwen"}}},
                 {"served_model_name", "qwen-browsing-worker-node"},
             }},
            {"runtime",
             {{"engine", "llama.cpp"}, {"distributed_backend", "llama_rpc"}, {"workers", 1}}},
            {"infer", {{"replicas", 1}}},
            {"topology",
             {{"nodes",
               json::array(
                   {{{"name", "worker-hostd"},
                     {"execution_mode", "worker-only"},
                     {"gpu_memory_mb", {{"0", 24576}}}}})}}},
            {"browsing", {{"enabled", true}, {"node", "worker-hostd"}}},
            {"app", {{"enabled", false}}},
        },
        "browsing-rejects-worker-only-node");

    ExpectInvalid(
        json{
            {"version", 2},
            {"plane_name", "bad-llama-rpc-replicas"},
            {"plane_mode", "llm"},
            {"model",
             {
                 {"source", {{"type", "local"}, {"path", "/models/qwen"}}},
                 {"materialization", {{"mode", "reference"}, {"local_path", "/models/qwen"}}},
                 {"served_model_name", "qwen-bad-replicas"},
             }},
            {"runtime",
             {
                 {"engine", "llama.cpp"},
                 {"distributed_backend", "llama_rpc"},
                 {"workers", 3},
             }},
            {"infer", {{"replicas", 2}}},
        },
        "llama-rpc-replicas-require-divisible-workers");

    ExpectInvalid(
        json{
            {"version", 2},
            {"plane_name", "bad-app"},
            {"plane_mode", "compute"},
            {"runtime", {{"engine", "custom"}, {"workers", 1}}},
            {"app", {{"enabled", true}, {"image", "example/app:dev"}, {"start", {{"type", "script"}}}}},
        },
        "app-script-without-script-ref");

    ExpectInvalid(
        json{
            {"version", 2},
            {"plane_name", "compute-with-skills"},
            {"plane_mode", "compute"},
            {"runtime", {{"engine", "custom"}, {"workers", 1}}},
            {"skills", {{"enabled", true}}},
        },
        "skills-require-llm-plane-mode");

    ExpectInvalid(
        json{
            {"version", 2},
            {"plane_name", "bad-dp"},
            {"plane_mode", "compute"},
            {"runtime",
             {{"engine", "custom"},
              {"workers", 1},
              {"data_parallel_mode", "auto_replicas"},
              {"data_parallel_lb_mode", "hybrid"}}},
        },
        "data-parallel-mode-removed");

    ExpectInvalid(
        json{
            {"version", 2},
            {"plane_name", "bad-exclusive-fraction"},
            {"plane_mode", "llm"},
            {"model",
             {
                 {"source", {{"type", "local"}, {"path", "/models/qwen"}}},
                 {"materialization", {{"mode", "reference"}, {"local_path", "/models/qwen"}}},
                 {"served_model_name", "qwen"},
             }},
            {"runtime",
             {{"engine", "llama.cpp"}, {"distributed_backend", "llama_rpc"}, {"workers", 1}}},
            {"infer", {{"replicas", 1}}},
            {"resources",
             {{"worker",
               {{"share_mode", "exclusive"}, {"gpu_fraction", 0.5}, {"memory_cap_mb", 24576}}}}},
        },
        "exclusive-share-mode-requires-full-gpu");

    {
      const json placement_execution_node{
          {"version", 2},
          {"plane_name", "placement-execution-node"},
          {"plane_mode", "llm"},
          {"placement", {{"execution_node", "remote-worker-a"}}},
          {"model",
           {
               {"source", {{"type", "local"}, {"path", "/models/qwen"}}},
               {"materialization", {{"mode", "reference"}, {"local_path", "/models/qwen"}}},
               {"served_model_name", "qwen-placement"},
           }},
          {"runtime",
           {{"engine", "llama.cpp"}, {"distributed_backend", "llama_rpc"}, {"workers", 2}}},
          {"infer", {{"replicas", 1}}},
          {"app", {{"enabled", false}}},
      };
      const auto state = RenderValid(placement_execution_node, "placement-execution-node");
      Expect(state.placement_target == std::optional<std::string>("node:remote-worker-a"),
             "placement-execution-node: placement_target should render from placement.execution_node");
      Expect(!state.nodes.empty() && state.nodes.front().name == "remote-worker-a",
             "placement-execution-node: renderer should synthesize node inventory");
      const auto projected = naim::DesiredStateV2Projector::Project(state);
      Expect(projected.contains("placement") && projected.at("placement").is_object(),
             "placement-execution-node: projector should emit placement block");
      Expect(projected.at("placement").at("execution_node").get<std::string>() == "remote-worker-a",
             "placement-execution-node: projector execution_node mismatch");
      Expect(!projected.contains("topology"),
             "placement-execution-node: projector should suppress topology when placement_target is set");
      const auto persisted = naim::DeserializeDesiredStateJson(
          naim::SerializeDesiredStateJson(state));
      Expect(
          persisted.placement_target == std::optional<std::string>("node:remote-worker-a"),
          "placement-execution-node: placement_target should survive desired-state persistence");
      const auto persisted_v2 = naim::DeserializeDesiredStateJson(
          naim::SerializeDesiredStateV2Json(state));
      Expect(persisted_v2.plane_name == state.plane_name,
             "placement-execution-node: v2 persistence should preserve plane_name");
      Expect(!persisted_v2.nodes.empty(),
             "placement-execution-node: v2 persistence should preserve nodes");
      std::cout << "ok: placement-execution-node" << '\n';
    }

    {
      const json legacy_placement_primary_node_alias{
          {"version", 2},
          {"plane_name", "legacy-placement-primary-node-alias"},
          {"plane_mode", "compute"},
          {"placement", {{"primary_node", "remote-worker-a"}}},
          {"runtime", {{"engine", "custom"}, {"workers", 1}}},
      };
      const auto state = RenderValid(
          legacy_placement_primary_node_alias,
          "legacy-placement-primary-node-alias");
      Expect(state.placement_target == std::optional<std::string>("node:remote-worker-a"),
             "legacy placement.primary_node alias should still render");
      const auto projected = naim::DesiredStateV2Projector::Project(state);
      Expect(projected.at("placement").at("execution_node").get<std::string>() == "remote-worker-a",
             "legacy placement.primary_node alias should project as execution_node");
      std::cout << "ok: legacy-placement-primary-node-alias" << '\n';
    }

    {
      const json legacy_topology_compatibility{
          {"version", 2},
          {"plane_name", "legacy-topology-compatibility"},
          {"plane_mode", "llm"},
          {"topology",
           {{"nodes",
             json::array(
                 {{{"name", "controller-node"}, {"execution_mode", "mixed"}},
                  {{"name", "worker-node-a"}, {"execution_mode", "worker-only"}}})}}},
          {"model",
           {
               {"source", {{"type", "local"}, {"path", "/models/qwen"}}},
               {"materialization", {{"mode", "reference"}, {"local_path", "/models/qwen"}}},
               {"served_model_name", "qwen-legacy-topology"},
           }},
          {"runtime", {{"engine", "llama.cpp"}, {"distributed_backend", "llama_rpc"}, {"workers", 1}}},
          {"infer", {{"replicas", 1}}},
          {"app", {{"enabled", true}, {"image", "example/app:dev"}, {"node", "controller-node"}}},
          {"skills", {{"enabled", true}, {"image", "example/skills:dev"}, {"node", "controller-node"}}},
          {"worker", {{"assignments", json::array({{{"node", "worker-node-a"}, {"gpu_device", "0"}}})}}},
      };
      const auto state = RenderValid(
          legacy_topology_compatibility,
          "legacy-topology-compatibility");
      const auto worker_it = std::find_if(
          state.instances.begin(),
          state.instances.end(),
          [](const naim::InstanceSpec& instance) {
            return instance.role == naim::InstanceRole::Worker;
          });
      Expect(worker_it != state.instances.end(),
             "legacy-topology-compatibility: worker instance should render");
      Expect(worker_it->node_name == "worker-node-a",
             "legacy-topology-compatibility: worker assignment node mismatch");
      Expect(worker_it->gpu_device == std::optional<std::string>("0"),
             "legacy-topology-compatibility: worker assignment gpu_device mismatch");
      const auto skills_it = std::find_if(
          state.instances.begin(),
          state.instances.end(),
          [](const naim::InstanceSpec& instance) {
            return instance.role == naim::InstanceRole::Skills;
          });
      Expect(skills_it != state.instances.end(),
             "legacy-topology-compatibility: skills instance should render");
      Expect(skills_it->node_name == "controller-node",
             "legacy-topology-compatibility: skills node mismatch");
      std::cout << "ok: legacy-topology-compatibility" << '\n';
    }

    ExpectInvalid(
        json{
            {"version", 2},
            {"plane_name", "bad-legacy-infer-node-without-topology"},
            {"plane_mode", "llm"},
            {"placement", {{"execution_node", "worker-a"}}},
            {"model",
             {
                 {"source", {{"type", "local"}, {"path", "/models/qwen"}}},
                 {"materialization", {{"mode", "reference"}, {"local_path", "/models/qwen"}}},
                 {"served_model_name", "qwen-legacy-infer"},
             }},
            {"runtime", {{"engine", "llama.cpp"}, {"distributed_backend", "llama_rpc"}, {"workers", 1}}},
            {"infer", {{"replicas", 1}, {"node", "other-node"}}},
        },
        "legacy-infer-node-requires-topology");

    ExpectInvalid(
        json{
            {"version", 2},
            {"plane_name", "bad-legacy-worker-assignments-without-topology"},
            {"plane_mode", "compute"},
            {"placement", {{"execution_node", "worker-a"}}},
            {"runtime", {{"engine", "custom"}, {"workers", 1}}},
            {"worker", {{"assignments", json::array({{{"node", "worker-a"}, {"gpu_device", "0"}}})}}},
        },
        "legacy-worker-assignments-require-topology");

    {
      const json placement_app_host{
          {"version", 2},
          {"plane_name", "placement-app-host"},
          {"plane_mode", "llm"},
          {"placement",
           {{"execution_node", "worker-a"},
            {"app_host", {{"address", "10.0.0.15"}, {"ssh_key_path", "/tmp/id_ed25519"}}}}},
          {"model",
           {
               {"source", {{"type", "local"}, {"path", "/models/qwen"}}},
               {"materialization", {{"mode", "reference"}, {"local_path", "/models/qwen"}}},
               {"served_model_name", "qwen-placement-app-host"},
           }},
          {"runtime", {{"engine", "llama.cpp"}, {"distributed_backend", "llama_rpc"}, {"workers", 1}}},
          {"infer", {{"replicas", 1}}},
          {"app", {{"enabled", true}, {"image", "example/app:dev"}}},
          {"skills", {{"enabled", true}, {"image", "example/skills:dev"}}},
      };
      const auto state = RenderValid(placement_app_host, "placement-app-host");
      Expect(state.app_host.has_value(), "placement-app-host: app_host should render");
      Expect(state.app_host->address == "10.0.0.15",
             "placement-app-host: app_host address mismatch");
      const auto app_it = std::find_if(
          state.instances.begin(),
          state.instances.end(),
          [](const naim::InstanceSpec& instance) {
            return instance.role == naim::InstanceRole::App;
          });
      Expect(app_it != state.instances.end(),
             "placement-app-host: app instance should render");
      Expect(app_it->environment.at("NAIM_EXTERNAL_APP_HOST_ADDRESS") == "10.0.0.15",
             "placement-app-host: app should include external host address");
      Expect(app_it->labels.at("naim.deployment.target") == "external-app-host",
             "placement-app-host: app should mark external deployment target");
      const auto skills_it = std::find_if(
          state.instances.begin(),
          state.instances.end(),
          [](const naim::InstanceSpec& instance) {
            return instance.role == naim::InstanceRole::Skills;
          });
      Expect(skills_it != state.instances.end(),
             "placement-app-host: skills instance should render");
      Expect(skills_it->environment.at("NAIM_EXTERNAL_APP_HOST_BINDING") == "skills-follow-app",
             "placement-app-host: skills should follow app host binding");
      const auto projected = naim::DesiredStateV2Projector::Project(state);
      Expect(projected.at("placement").at("app_host").at("ssh_key_path").get<std::string>() ==
                 "/tmp/id_ed25519",
             "placement-app-host: projector should preserve ssh_key_path");
      std::cout << "ok: placement-app-host" << '\n';
    }

    ExpectInvalid(
        json{
            {"version", 2},
            {"plane_name", "bad-app-host-auth"},
            {"plane_mode", "compute"},
            {"placement",
             {{"execution_node", "worker-a"},
              {"app_host",
               {{"address", "10.0.0.15"},
                {"ssh_key_path", "/tmp/id_ed25519"},
                {"username", "root"},
                {"password", "secret"}}}}},
            {"runtime", {{"engine", "custom"}, {"workers", 1}}},
            {"app", {{"enabled", true}, {"image", "example/app:dev"}}},
        },
        "placement-app-host-rejects-mixed-auth");

    return 0;
  } catch (const std::exception& ex) {
    std::cerr << "desired_state_v2_validator_tests failed: " << ex.what() << '\n';
    return 1;
  }
}
