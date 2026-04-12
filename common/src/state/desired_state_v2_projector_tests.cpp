#include <iostream>
#include <stdexcept>
#include <string>

#include <nlohmann/json.hpp>

#include "naim/state/desired_state_v2_projector.h"
#include "naim/state/desired_state_v2_renderer.h"
#include "naim/state/desired_state_v2_validator.h"

namespace {

using nlohmann::json;

void Expect(bool condition, const std::string& message) {
  if (!condition) {
    throw std::runtime_error(message);
  }
}

void ExpectRoundTrip(const json& source, const std::string& name) {
  const auto rendered = naim::DesiredStateV2Renderer::Render(source);
  const auto projected = naim::DesiredStateV2Projector::Project(rendered);
  naim::DesiredStateV2Validator::ValidateOrThrow(projected);
  const auto rerendered = naim::DesiredStateV2Renderer::Render(projected);

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
    Expect(rerendered.bootstrap_model->source_node_name ==
               rendered.bootstrap_model->source_node_name,
           name + ": source_node_name mismatch");
    Expect(rerendered.bootstrap_model->source_paths ==
               rendered.bootstrap_model->source_paths,
           name + ": source_paths mismatch");
    Expect(rerendered.bootstrap_model->source_urls == rendered.bootstrap_model->source_urls,
           name + ": source_urls mismatch");
  }
  if (rendered.skills.has_value()) {
    Expect(rerendered.skills.has_value(), name + ": skills missing after rerender");
    Expect(rerendered.skills->enabled == rendered.skills->enabled,
           name + ": skills.enabled mismatch");
    Expect(rerendered.skills->factory_skill_ids == rendered.skills->factory_skill_ids,
           name + ": skills.factory_skill_ids mismatch");
  }
  if (rendered.browsing.has_value()) {
    Expect(rerendered.browsing.has_value(), name + ": browsing missing after rerender");
    Expect(rerendered.browsing->enabled == rendered.browsing->enabled,
           name + ": browsing.enabled mismatch");
    Expect(rerendered.browsing->policy.has_value() == rendered.browsing->policy.has_value(),
           name + ": browsing.policy presence mismatch");
    if (rendered.browsing->policy.has_value()) {
      Expect(
          rerendered.browsing->policy->browser_session_enabled ==
              rendered.browsing->policy->browser_session_enabled,
          name + ": browsing.policy.browser_session_enabled mismatch");
      Expect(
          rerendered.browsing->policy->rendered_browser_enabled ==
              rendered.browsing->policy->rendered_browser_enabled,
          name + ": browsing.policy.rendered_browser_enabled mismatch");
      Expect(
          rerendered.browsing->policy->login_enabled ==
              rendered.browsing->policy->login_enabled,
          name + ": browsing.policy.login_enabled mismatch");
      Expect(
          rerendered.browsing->policy->allowed_domains ==
              rendered.browsing->policy->allowed_domains,
          name + ": browsing.policy.allowed_domains mismatch");
      Expect(
          rerendered.browsing->policy->blocked_domains ==
              rendered.browsing->policy->blocked_domains,
          name + ": browsing.policy.blocked_domains mismatch");
      Expect(
          rerendered.browsing->policy->max_search_results ==
              rendered.browsing->policy->max_search_results,
          name + ": browsing.policy.max_search_results mismatch");
      Expect(
          rerendered.browsing->policy->max_fetch_bytes ==
              rendered.browsing->policy->max_fetch_bytes,
          name + ": browsing.policy.max_fetch_bytes mismatch");
    }
  }
  if (source.contains("skills")) {
    Expect(projected.contains("skills"), name + ": skills block missing after projection");
    Expect(projected.at("skills").value("enabled", false) ==
               source.at("skills").value("enabled", false),
           name + ": skills.enabled projection mismatch");
    if (source.at("skills").contains("node")) {
      Expect(projected.at("skills").at("node") == source.at("skills").at("node"),
             name + ": skills.node mismatch");
    }
    if (source.at("skills").contains("image")) {
      Expect(projected.at("skills").at("image") == source.at("skills").at("image"),
             name + ": skills.image mismatch");
    }
    if (source.at("skills").contains("env") && projected.at("skills").contains("env")) {
      Expect(projected.at("skills").at("env") == source.at("skills").at("env"),
             name + ": skills.env mismatch");
    }
    if (source.at("skills").contains("storage")) {
      Expect(projected.at("skills").at("storage") == source.at("skills").at("storage"),
             name + ": skills.storage mismatch");
    }
    if (source.at("skills").contains("factory_skill_ids")) {
      Expect(
          projected.at("skills").at("factory_skill_ids") ==
              source.at("skills").at("factory_skill_ids"),
          name + ": skills.factory_skill_ids mismatch");
    }
  }
  const auto* webgateway_source =
      source.contains("webgateway")
          ? &source.at("webgateway")
          : (source.contains("browsing") ? &source.at("browsing") : nullptr);
  if (webgateway_source != nullptr) {
    Expect(projected.contains("webgateway"), name + ": webgateway block missing after projection");
    Expect(projected.at("webgateway").value("enabled", false) ==
               webgateway_source->value("enabled", false),
           name + ": webgateway.enabled projection mismatch");
    if (webgateway_source->contains("node")) {
      Expect(projected.at("webgateway").at("node") == webgateway_source->at("node"),
             name + ": webgateway.node mismatch");
    }
    if (webgateway_source->contains("image")) {
      Expect(projected.at("webgateway").at("image") == webgateway_source->at("image"),
             name + ": webgateway.image mismatch");
    }
    if (webgateway_source->contains("env") && projected.at("webgateway").contains("env")) {
      Expect(projected.at("webgateway").at("env") == webgateway_source->at("env"),
             name + ": webgateway.env mismatch");
    }
    if (webgateway_source->contains("publish")) {
      Expect(projected.at("webgateway").at("publish") == webgateway_source->at("publish"),
             name + ": webgateway.publish mismatch");
    }
    if (webgateway_source->contains("storage")) {
      Expect(projected.at("webgateway").at("storage") == webgateway_source->at("storage"),
             name + ": webgateway.storage mismatch");
    }
    if (webgateway_source->contains("policy")) {
      Expect(projected.at("webgateway").at("policy") == webgateway_source->at("policy"),
             name + ": webgateway.policy mismatch");
    }
  }
  std::cout << "ok-roundtrip: " << name << '\n';
}

void ExpectExecutionNodeProjection(const json& source, const std::string& name) {
  const auto rendered = naim::DesiredStateV2Renderer::Render(source);
  const auto projected = naim::DesiredStateV2Projector::Project(rendered);
  naim::DesiredStateV2Validator::ValidateOrThrow(projected);

  Expect(projected.contains("placement"), name + ": placement block missing");
  Expect(!projected.contains("topology"), name + ": topology must be suppressed");
  if (projected.contains("infer")) {
    Expect(!projected.at("infer").contains("node"), name + ": infer.node must be suppressed");
  }
  if (projected.contains("worker")) {
    Expect(!projected.at("worker").contains("node"), name + ": worker.node must be suppressed");
    Expect(
        !projected.at("worker").contains("assignments"),
        name + ": worker.assignments must be suppressed");
  }
  if (projected.contains("app")) {
    Expect(!projected.at("app").contains("node"), name + ": app.node must be suppressed");
  }
  if (projected.contains("skills")) {
    Expect(!projected.at("skills").contains("node"), name + ": skills.node must be suppressed");
  }
  if (projected.contains("webgateway")) {
    Expect(
        !projected.at("webgateway").contains("node"),
        name + ": webgateway.node must be suppressed");
  }
  std::cout << "ok-execution-node: " << name << '\n';
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
            {"runtime",
             {{"engine", "llama.cpp"}, {"distributed_backend", "llama_rpc"}, {"workers", 1}}},
            {"infer", {{"replicas", 1}}},
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
        },
        "split-topology-with-url-parts");

    ExpectRoundTrip(
        json{
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
                 {"env", {{"HOST", "0.0.0.0"}, {"PORT", "8080"}}},
                 {"publish",
                  json::array({{{"host_ip", "127.0.0.1"}, {"host_port", 18010}, {"container_port", 8080}}})},
                 {"volumes",
                  json::array({{{"name", "private-data"},
                                 {"type", "persistent"},
                                 {"size_gb", 8},
                                 {"mount_path", "/naim/private"},
                                 {"access", "rw"}}})},
             }},
        },
        "llm-with-app");

    ExpectRoundTrip(
        json{
            {"version", 2},
            {"plane_name", "llama-rpc-replicas"},
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
                 {"served_model_name", "qwen-rpc"},
             }},
            {"runtime",
             {
                 {"engine", "llama.cpp"},
                 {"distributed_backend", "llama_rpc"},
                 {"workers", 3},
                 {"max_model_len", 8192},
                 {"llama_ctx_size", 8192},
                 {"max_num_seqs", 16},
                 {"gpu_memory_utilization", 0.85},
             }},
            {"topology",
             {{"nodes",
               json::array(
                   {{{"name", "local-hostd"},
                     {"execution_mode", "mixed"},
                     {"gpu_memory_mb", {{"0", 24576}, {"2", 24576}, {"3", 24576}}}}})}}},
            {"infer", {{"replicas", 3}}},
            {"worker",
             {{"assignments",
               json::array(
                   {{{"node", "local-hostd"}, {"gpu_device", "0"}},
                    {{"node", "local-hostd"}, {"gpu_device", "2"}},
                    {{"node", "local-hostd"}, {"gpu_device", "3"}}})}}},
            {"network",
             {{"gateway_port", 18984},
              {"inference_port", 18994},
              {"server_name", "llama-rpc-replicas.internal"}}},
            {"app", {{"enabled", false}}},
            {"resources",
             {{"worker",
               {{"placement_mode", "manual"},
                {"share_mode", "exclusive"},
                {"gpu_fraction", 1.0},
                {"memory_cap_mb", 24576}}},
              {"shared_disk_gb", 40}}},
        },
        "llama-rpc-replicas");

    ExpectRoundTrip(
        json{
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
            {"infer", {{"node", "infer-hostd"}, {"replicas", 1}}},
            {"skills",
             {
                 {"enabled", true},
                 {"factory_skill_ids", json::array({"skill-alpha", "skill-beta"})},
                 {"node", "skills-hostd"},
                 {"image", "example/skills:dev"},
                 {"env", {{"SKILLS_CUSTOM_FLAG", "enabled"}}},
                 {"storage", {{"size_gb", 9}, {"mount_path", "/srv/skills"}}},
             }},
            {"topology",
             {{"nodes",
               json::array(
                   {{{"name", "infer-hostd"},
                     {"execution_mode", "mixed"},
                     {"gpu_memory_mb", {{"0", 24576}}}},
                    {{"name", "skills-hostd"},
                     {"execution_mode", "mixed"},
                     {"gpu_memory_mb", {{"1", 24576}}}}})}}},
            {"app", {{"enabled", false}}},
        },
        "llm-with-skills");

    ExpectRoundTrip(
        json{
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
        },
        "llm-with-browsing");

    ExpectExecutionNodeProjection(
        json{
            {"version", 2},
            {"plane_name", "execution-node-clean"},
            {"plane_mode", "llm"},
            {"placement",
             {{"execution_node", "worker-node-a"},
              {"app_host", {{"address", "10.0.0.15"}, {"ssh_key_path", "/tmp/id_ed25519"}}}}},
            {"model",
             {
                 {"source", {{"type", "local"}, {"path", "/models/qwen"}}},
                 {"materialization", {{"mode", "reference"}, {"local_path", "/models/qwen"}}},
                 {"served_model_name", "qwen-placement-clean"},
             }},
            {"runtime",
             {{"engine", "llama.cpp"}, {"distributed_backend", "llama_rpc"}, {"workers", 1}}},
            {"infer", {{"replicas", 1}}},
            {"app", {{"enabled", true}, {"image", "example/app:dev"}}},
            {"skills", {{"enabled", true}, {"image", "example/skills:dev"}}},
        },
        "execution-node-clean");

    return 0;
  } catch (const std::exception& ex) {
    std::cerr << "desired_state_v2_projector_tests failed: " << ex.what() << '\n';
    return 1;
  }
}
