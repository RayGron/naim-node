#include <algorithm>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <set>
#include <stdexcept>
#include <string>
#include <vector>

#include <nlohmann/json.hpp>

#include "comet/planning/planner.h"
#include "comet/planning/execution_plan.h"
#include "comet/runtime/infer_runtime_config.h"
#include "comet/state/state_json.h"
#include "comet/state/desired_state_v2_renderer.h"
#include "comet/state/desired_state_v2_validator.h"
#include "comet/state/worker_group_topology.h"

namespace {

using nlohmann::json;

comet::DesiredState RenderValid(const json& value, const std::string& name) {
  try {
    comet::DesiredStateV2Validator::ValidateOrThrow(value);
    auto state = comet::DesiredStateV2Renderer::Render(value);
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
    comet::DesiredStateV2Validator::ValidateOrThrow(value);
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

const comet::InstanceSpec& FindInstance(
    const comet::DesiredState& state,
    const std::string& name) {
  for (const auto& instance : state.instances) {
    if (instance.name == name) {
      return instance;
    }
  }
  throw std::runtime_error("instance not found: " + name);
}

comet::InstanceSpec& FindMutableInstance(
    comet::DesiredState* state,
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

const comet::DiskSpec& FindDisk(
    const comet::DesiredState& state,
    const std::string& name) {
  for (const auto& disk : state.disks) {
    if (disk.name == name) {
      return disk;
    }
  }
  throw std::runtime_error("disk not found: " + name);
}

}  // namespace

int main() {
  try {
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
          std::filesystem::temp_directory_path() / "comet-state-v2-thinking-flag.json";
      {
        std::ofstream output(temp_path);
        output << state_file_v2.dump(2) << '\n';
      }
      const auto loaded = comet::LoadDesiredStateJson(temp_path.string());
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
          std::filesystem::temp_directory_path() / "comet-state-legacy.json";
      {
        std::ofstream output(temp_path);
        output << legacy_state_file.dump(2) << '\n';
      }
      bool rejected = false;
      try {
        static_cast<void>(comet::LoadDesiredStateJson(temp_path.string()));
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
          nlohmann::json::parse(comet::RenderInferRuntimeConfigJson(state));
      Expect(runtime_json.at("inference").at("max_model_len").get<int>() == 4096,
             "llama-ctx-runtime: runtime max_model_len mismatch");
      Expect(runtime_json.at("inference").at("llama_ctx_size").get<int>() == 4096,
             "llama-ctx-runtime: runtime llama_ctx_size mismatch");
      std::cout << "ok: llama-ctx-runtime" << '\n';
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
      const int expected_rpc_port = comet::StableLlamaRpcWorkerPort(
          state.plane_name,
          "worker-llama-rpc-backend-a");
      Expect(state.worker_group.members.front().rpc_port == expected_rpc_port,
             "llama-rpc-backend: worker rpc_port should use stable plane-scoped port");
      Expect(FindInstance(state, "worker-llama-rpc-backend-a")
                     .environment.at("COMET_WORKER_BOOT_MODE") == "llama-rpc",
             "llama-rpc-backend: worker boot mode mismatch");
      Expect(FindInstance(state, "worker-llama-rpc-backend-a")
                     .environment.at("COMET_WORKER_RPC_PORT") ==
                 std::to_string(expected_rpc_port),
             "llama-rpc-backend: worker rpc env mismatch");
      Expect(FindInstance(state, "infer-llama-rpc-backend")
                     .environment.at("COMET_INFER_RUNTIME_BACKEND") == "llama-rpc-head",
             "llama-rpc-backend: infer backend mismatch");
      Expect(FindInstance(state, "infer-llama-rpc-backend")
                     .environment.at("COMET_INSTANCE_SUBROLE") == "aggregator",
             "llama-rpc-backend: primary infer should be aggregator");
      Expect(FindInstance(state, "infer-llama-rpc-backend-a")
                     .environment.at("COMET_INSTANCE_SUBROLE") == "infer",
             "llama-rpc-backend: leaf infer should be rendered for single replica");
      Expect(FindInstance(state, "worker-llama-rpc-backend-a")
                     .environment.at("COMET_INFER_INSTANCE_NAME") == "infer-llama-rpc-backend-a",
             "llama-rpc-backend: worker should target leaf infer");
      Expect(
          FindInstance(state, "infer-llama-rpc-backend")
                  .environment.at("COMET_INFER_RUNTIME_CONFIG") ==
              "/comet/shared/control/llama-rpc-backend/infer/infer-llama-rpc-backend/infer-runtime.json",
          "llama-rpc-backend: infer runtime config path mismatch");
      auto stale_state = state;
      auto& stale_worker = FindMutableInstance(&stale_state, "worker-llama-rpc-backend-a");
      stale_worker.environment["COMET_WORKER_RPC_PORT"] = "29600";
      stale_worker.environment["COMET_WORKER_RPC_ENDPOINT"] =
          "worker-llama-rpc-backend-a:29600";
      stale_state.worker_group.members.front().rpc_port = 29600;
      const auto stale_runtime_config = json::parse(
          comet::RenderInferRuntimeConfigJsonForInstance(
              stale_state,
              "infer-llama-rpc-backend-a"));
      Expect(stale_runtime_config.at("worker_group").at("members").at(0).at("rpc_port").get<int>() ==
                 expected_rpc_port,
             "llama-rpc-backend: infer runtime config should heal legacy worker rpc ports");
      const auto stale_compose_plans = comet::BuildNodeComposePlans(stale_state);
      bool found_stale_worker_service = false;
      for (const auto& service : stale_compose_plans.front().services) {
        if (service.name != "worker-llama-rpc-backend-a") {
          continue;
        }
        found_stale_worker_service = true;
        Expect(service.environment.at("COMET_WORKER_RPC_PORT") ==
                   std::to_string(expected_rpc_port),
               "llama-rpc-backend: compose worker env should heal legacy rpc port");
        Expect(service.published_ports.size() == 1 &&
                   service.published_ports.front().host_port == expected_rpc_port,
               "llama-rpc-backend: compose worker publish should heal legacy rpc port");
      }
      Expect(found_stale_worker_service,
             "llama-rpc-backend: expected compose service for healed worker");
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
                 {"COMET_INFERENCE_PORT", "19180"},
                 {"COMET_GATEWAY_PORT", "19181"},
                 {"COMET_LLAMA_PORT", "19182"},
             }}}},
          {"app", {{"enabled", false}}},
      };
      const auto state = RenderValid(infer_port_overrides, "llama-rpc-infer-ports");
      const auto infer_name = FindInstance(state, "infer-llama-rpc-ports").name;
      const auto runtime_config = json::parse(
          comet::RenderInferRuntimeConfigJsonForInstance(state, infer_name));
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
                 {"COMET_REPLICA_UPSTREAMS",
                  "http://127.0.0.1:19190,http://127.0.0.1:19191"},
             }}}},
          {"app", {{"enabled", false}}},
      };
      const auto state = RenderValid(llama_rpc_replica_upstreams, "llama-rpc-replica-upstreams");
      const auto infer_name = FindInstance(state, "infer-llama-rpc-upstreams").name;
      const auto runtime_config = json::parse(
          comet::RenderInferRuntimeConfigJsonForInstance(state, infer_name));
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
      Expect(FindInstance(state, "infer-llama-rpc-replicas").environment.count("COMET_REPLICA_UPSTREAMS") == 1,
             "llama-rpc-replicas: aggregator should have COMET_REPLICA_UPSTREAMS");
      Expect(FindInstance(state, "infer-llama-rpc-replicas-a").environment.at("COMET_GATEWAY_PORT") == "81",
             "llama-rpc-replicas: first leaf gateway port mismatch");
      Expect(FindInstance(state, "worker-llama-rpc-replicas-a").environment.at("COMET_INFER_INSTANCE_NAME") ==
                 "infer-llama-rpc-replicas-a",
             "llama-rpc-replicas: worker a infer binding mismatch");
      Expect(FindInstance(state, "worker-llama-rpc-replicas-c").environment.at("COMET_INFER_INSTANCE_NAME") ==
                 "infer-llama-rpc-replicas-c",
             "llama-rpc-replicas: worker c infer binding mismatch");
      const auto runtime_config = json::parse(
          comet::RenderInferRuntimeConfigJsonForInstance(state, "infer-llama-rpc-replicas"));
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
          comet::BuildNodeExecutionPlans(std::nullopt, state, "/tmp/comet-artifacts");
      Expect(host_plans.size() == 1,
             "llama-rpc-replicas: expected a single host plan for single-node topology");
      std::vector<std::string> write_runtime_details;
      for (const auto& operation : host_plans.front().operations) {
        if (operation.kind == comet::HostOperationKind::WriteInferRuntimeConfig) {
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
          instance.image = "comet/infer-runtime:previous";
        }
      }
      const auto refresh_plans =
          comet::BuildNodeExecutionPlans(current_state, state, "/tmp/comet-artifacts");
      Expect(refresh_plans.size() == 1,
             "llama-rpc-replicas: expected host plan when infer service image changes");
      std::vector<std::string> refresh_runtime_details;
      bool saw_compose_up = false;
      for (const auto& operation : refresh_plans.front().operations) {
        if (operation.kind == comet::HostOperationKind::WriteInferRuntimeConfig) {
          refresh_runtime_details.push_back(operation.details);
        }
        if (operation.kind == comet::HostOperationKind::ComposeUp) {
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
      const auto compose_plans = comet::BuildNodeComposePlans(state);
      Expect(compose_plans.size() == 1,
             "llama-rpc-replicas: expected a single compose plan for single-node topology");
      std::set<int> seen_infer_gateway_ports;
      std::set<int> seen_worker_rpc_ports;
      const std::set<int> expected_worker_rpc_ports{
          comet::StableLlamaRpcWorkerPort(state.plane_name, "worker-llama-rpc-replicas-a"),
          comet::StableLlamaRpcWorkerPort(state.plane_name, "worker-llama-rpc-replicas-b"),
          comet::StableLlamaRpcWorkerPort(state.plane_name, "worker-llama-rpc-replicas-c"),
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
               {"env", {{"COMET_CUSTOM_SKILLS_FLAG", "enabled"}}},
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
      const auto& skills = FindInstance(state, "skills-llm-with-skills");
      Expect(skills.role == comet::InstanceRole::Skills,
             "llm-with-skills: skills instance role mismatch");
      Expect(skills.image == "example/skills:dev",
             "llm-with-skills: custom skills image mismatch");
      Expect(skills.environment.count("COMET_CUSTOM_SKILLS_FLAG") == 1 &&
                 skills.environment.at("COMET_CUSTOM_SKILLS_FLAG") == "enabled",
             "llm-with-skills: custom skills env mismatch");
      Expect(!skills.published_ports.empty() &&
                 skills.published_ports.front().host_port == 19120 &&
                 skills.published_ports.front().container_port == 18120,
             "llm-with-skills: published port mismatch");
      const auto& skills_disk = FindDisk(state, "skills-llm-with-skills-private");
      Expect(skills_disk.kind == comet::DiskKind::SkillsPrivate,
             "llm-with-skills: skills disk kind mismatch");
      Expect(skills_disk.container_path == "/srv/skills",
             "llm-with-skills: skills disk mount path mismatch");
      Expect(skills_disk.size_gb == 9,
             "llm-with-skills: skills disk size mismatch");

      const auto compose_plans = comet::BuildNodeComposePlans(state);
      Expect(compose_plans.size() == 1,
             "llm-with-skills: expected a single compose plan for single-node topology");
      const auto service_it = std::find_if(
          compose_plans.front().services.begin(),
          compose_plans.front().services.end(),
          [](const comet::ComposeService& service) {
            return service.name == "skills-llm-with-skills";
          });
      Expect(service_it != compose_plans.front().services.end(),
             "llm-with-skills: skills service missing from compose plan");
      Expect(service_it->healthcheck.find("COMET_SKILLS_PORT") != std::string::npos &&
                 service_it->healthcheck.find("/health") != std::string::npos,
             "llm-with-skills: compose healthcheck should target skills health endpoint");
      std::cout << "ok: llm-with-skills" << '\n';
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
                 {"data_parallel_mode", "vllm_native"},
             }},
        },
        "llama-rpc-with-data-parallel-mode");

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

    return 0;
  } catch (const std::exception& ex) {
    std::cerr << "desired_state_v2_validator_tests failed: " << ex.what() << '\n';
    return 1;
  }
}
