#include <iostream>
#include <stdexcept>
#include <string>

#include <nlohmann/json.hpp>

#include "comet/state/desired_state_v2_projector.h"
#include "comet/state/desired_state_v2_renderer.h"
#include "comet/state/desired_state_v2_validator.h"

namespace {

using nlohmann::json;

void Expect(bool condition, const std::string& message) {
  if (!condition) {
    throw std::runtime_error(message);
  }
}

void ExpectRoundTrip(const json& source, const std::string& name) {
  const auto rendered = comet::DesiredStateV2Renderer::Render(source);
  const auto projected = comet::DesiredStateV2Projector::Project(rendered);
  comet::DesiredStateV2Validator::ValidateOrThrow(projected);
  const auto rerendered = comet::DesiredStateV2Renderer::Render(projected);

  Expect(rerendered.plane_name == rendered.plane_name, name + ": plane_name mismatch");
  Expect(rerendered.plane_mode == rendered.plane_mode, name + ": plane_mode mismatch");
  Expect(rerendered.instances.size() == rendered.instances.size(),
         name + ": instance count mismatch");
  Expect(rerendered.disks.size() == rendered.disks.size(), name + ": disk count mismatch");
  Expect(rerendered.inference.runtime_engine == rendered.inference.runtime_engine,
         name + ": runtime_engine mismatch");
  Expect(rerendered.inference.data_parallel_mode == rendered.inference.data_parallel_mode,
         name + ": data_parallel_mode mismatch");
  if (rendered.bootstrap_model.has_value()) {
    Expect(rerendered.bootstrap_model.has_value(), name + ": bootstrap_model missing after rerender");
    Expect(rerendered.bootstrap_model->model_id == rendered.bootstrap_model->model_id,
           name + ": model_id mismatch");
    Expect(rerendered.bootstrap_model->served_model_name ==
               rendered.bootstrap_model->served_model_name,
           name + ": served_model_name mismatch");
    Expect(rerendered.bootstrap_model->target_filename ==
               rendered.bootstrap_model->target_filename,
           name + ": target_filename mismatch");
    Expect(rerendered.bootstrap_model->source_urls == rendered.bootstrap_model->source_urls,
           name + ": source_urls mismatch");
  }
  std::cout << "ok-roundtrip: " << name << '\n';
}

}  // namespace

int main() {
  try {
    ExpectRoundTrip(
        json{
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
        },
        "llm-backend-only");

    ExpectRoundTrip(
        json{
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
        },
        "split-topology-with-url-parts");

    ExpectRoundTrip(
        json{
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
                 {"env", {{"HOST", "0.0.0.0"}, {"PORT", "8080"}}},
                 {"publish",
                  json::array({{{"host_ip", "127.0.0.1"}, {"host_port", 18010}, {"container_port", 8080}}})},
                 {"volumes",
                  json::array({{{"name", "private-data"},
                                 {"type", "persistent"},
                                 {"size_gb", 8},
                                 {"mount_path", "/comet/private"},
                                 {"access", "rw"}}})},
             }},
        },
        "llm-with-app");

    return 0;
  } catch (const std::exception& ex) {
    std::cerr << "desired_state_v2_projector_tests failed: " << ex.what() << '\n';
    return 1;
  }
}
