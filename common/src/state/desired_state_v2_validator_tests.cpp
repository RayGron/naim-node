#include <iostream>
#include <stdexcept>
#include <string>

#include <nlohmann/json.hpp>

#include "comet/state/desired_state_v2_renderer.h"
#include "comet/state/desired_state_v2_validator.h"

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

}  // namespace

int main() {
  try {
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
          {"runtime", {{"engine", "vllm"}, {"workers", 1}}},
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
      Expect(state.instances.size() == 2, "llm-backend-only: expected infer + worker");
      std::cout << "ok: llm-backend-only" << '\n';
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
          {"runtime", {{"engine", "vllm"}, {"workers", 2}}},
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
          {"infer", {{"node", "infer-hostd"}}},
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
      Expect(state.instances.size() == 3, "split-topology: expected infer + 2 workers");
      Expect(FindInstance(state, "infer-split-backend").node_name == "infer-hostd",
             "split-topology: infer node mismatch");
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
               {"source", {{"type", "huggingface"}, {"ref", "Qwen/Qwen2.5-0.5B-Instruct"}}},
               {"materialization", {{"mode", "download"}}},
               {"served_model_name", "qwen-app"},
           }},
          {"runtime", {{"engine", "vllm"}, {"workers", 1}}},
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
      Expect(state.instances.size() == 3, "llm-with-app: expected infer + worker + app");
      Expect(FindInstance(state, "app-llm-app").image == "example/app:dev",
             "llm-with-app: app image mismatch");
      std::cout << "ok: llm-with-app" << '\n';
    }

    ExpectInvalid(
        json{
            {"version", 2},
            {"plane_name", "missing-model"},
            {"plane_mode", "llm"},
            {"runtime", {{"engine", "vllm"}, {"workers", 1}}},
        },
        "llm-without-model");

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
            {"plane_name", "bad-app"},
            {"plane_mode", "compute"},
            {"runtime", {{"engine", "custom"}, {"workers", 1}}},
            {"app", {{"enabled", true}, {"image", "example/app:dev"}, {"start", {{"type", "script"}}}}},
        },
        "app-script-without-script-ref");

    ExpectInvalid(
        json{
            {"version", 2},
            {"plane_name", "bad-dp"},
            {"plane_mode", "compute"},
            {"runtime",
             {{"engine", "custom"},
              {"workers", 1},
              {"data_parallel_mode", "vllm_native"},
              {"data_parallel_lb_mode", "hybrid"}}},
        },
        "native-dp-non-vllm");

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
            {"runtime", {{"engine", "vllm"}, {"workers", 1}}},
            {"resources",
             {{"worker",
               {{"share_mode", "exclusive"}, {"gpu_fraction", 0.5}, {"memory_cap_mb", 24576}}}}},
        },
        "exclusive-share-mode-requires-full-gpu");

    ExpectInvalid(
        json{
            {"version", 2},
            {"plane_name", "bad-hybrid-shared-gpu"},
            {"plane_mode", "llm"},
            {"model",
             {
                 {"source", {{"type", "local"}, {"path", "/models/qwen"}}},
                 {"materialization", {{"mode", "reference"}, {"local_path", "/models/qwen"}}},
                 {"served_model_name", "qwen"},
             }},
            {"runtime",
             {{"engine", "vllm"},
              {"workers", 4},
              {"data_parallel_mode", "vllm_native"},
              {"data_parallel_lb_mode", "hybrid"}}},
            {"worker",
             {{"assignments",
               json::array(
                   {{{"node", "local-hostd"}, {"gpu_device", "0"}},
                    {{"node", "local-hostd"}, {"gpu_device", "2"}},
                    {{"node", "local-hostd"}, {"gpu_device", "3"}},
                    {{"node", "local-hostd"}, {"gpu_device", "0"}}})}}},
        },
        "hybrid-worker-assignments-require-unique-gpus");

    return 0;
  } catch (const std::exception& ex) {
    std::cerr << "desired_state_v2_validator_tests failed: " << ex.what() << '\n';
    return 1;
  }
}
