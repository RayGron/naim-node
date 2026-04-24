#include <algorithm>
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
    Expect(rerendered.bootstrap_model->source_format ==
               rendered.bootstrap_model->source_format,
           name + ": source_format mismatch");
    Expect(rerendered.bootstrap_model->desired_output_format ==
               rendered.bootstrap_model->desired_output_format,
           name + ": desired_output_format mismatch");
    Expect(rerendered.bootstrap_model->quantization ==
               rendered.bootstrap_model->quantization,
           name + ": quantization mismatch");
    Expect(rerendered.bootstrap_model->keep_source ==
               rendered.bootstrap_model->keep_source,
           name + ": keep_source mismatch");
    Expect(rerendered.bootstrap_model->writeback_enabled ==
               rendered.bootstrap_model->writeback_enabled,
           name + ": writeback_enabled mismatch");
    Expect(rerendered.bootstrap_model->writeback_target_node_name ==
               rendered.bootstrap_model->writeback_target_node_name,
           name + ": writeback_target_node_name mismatch");
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
  if (rendered.knowledge.has_value()) {
    Expect(rerendered.knowledge.has_value(), name + ": knowledge missing after rerender");
    Expect(rerendered.knowledge->enabled == rendered.knowledge->enabled,
           name + ": knowledge.enabled mismatch");
    Expect(rerendered.knowledge->service_id == rendered.knowledge->service_id,
           name + ": knowledge.service_id mismatch");
    Expect(rerendered.knowledge->selection_mode == rendered.knowledge->selection_mode,
           name + ": knowledge.selection_mode mismatch");
    Expect(
        rerendered.knowledge->selected_knowledge_ids ==
            rendered.knowledge->selected_knowledge_ids,
        name + ": knowledge.selected_knowledge_ids mismatch");
  }
  if (rendered.turboquant.has_value()) {
    Expect(rerendered.turboquant.has_value(), name + ": turboquant missing after rerender");
    Expect(rerendered.turboquant->enabled == rendered.turboquant->enabled,
           name + ": turboquant.enabled mismatch");
    Expect(rerendered.turboquant->cache_type_k == rendered.turboquant->cache_type_k,
           name + ": turboquant.cache_type_k mismatch");
    Expect(rerendered.turboquant->cache_type_v == rendered.turboquant->cache_type_v,
           name + ": turboquant.cache_type_v mismatch");
  }
  if (rendered.context_compression.has_value()) {
    Expect(rerendered.context_compression.has_value(),
           name + ": context_compression missing after rerender");
    Expect(rerendered.context_compression->enabled == rendered.context_compression->enabled,
           name + ": context_compression.enabled mismatch");
    Expect(rerendered.context_compression->mode == rendered.context_compression->mode,
           name + ": context_compression.mode mismatch");
    Expect(rerendered.context_compression->target == rendered.context_compression->target,
           name + ": context_compression.target mismatch");
    Expect(
        rerendered.context_compression->memory_priority ==
            rendered.context_compression->memory_priority,
        name + ": context_compression.memory_priority mismatch");
  }
  if (source.contains("features") && source.at("features").contains("turboquant")) {
    Expect(projected.contains("features"), name + ": features block missing after projection");
    Expect(projected.at("features").contains("turboquant"),
           name + ": turboquant block missing after projection");
    const auto& source_turboquant = source.at("features").at("turboquant");
    const auto& projected_turboquant = projected.at("features").at("turboquant");
    Expect(projected_turboquant.value("enabled", false) ==
               source_turboquant.value("enabled", false),
           name + ": turboquant.enabled projection mismatch");
    if (source_turboquant.contains("cache_type_k")) {
      Expect(projected_turboquant.at("cache_type_k") == source_turboquant.at("cache_type_k"),
             name + ": turboquant.cache_type_k mismatch");
    }
    if (source_turboquant.contains("cache_type_v")) {
      Expect(projected_turboquant.at("cache_type_v") == source_turboquant.at("cache_type_v"),
             name + ": turboquant.cache_type_v mismatch");
    }
  }
  if (source.contains("features") && source.at("features").contains("context_compression")) {
    Expect(projected.contains("features"), name + ": features block missing after projection");
    Expect(projected.at("features").contains("context_compression"),
           name + ": context_compression block missing after projection");
    Expect(projected.at("features").at("context_compression") ==
               source.at("features").at("context_compression"),
           name + ": context_compression projection mismatch");
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
  if (source.contains("knowledge")) {
    Expect(projected.contains("knowledge"), name + ": knowledge block missing after projection");
    Expect(projected.at("knowledge").value("enabled", false) ==
               source.at("knowledge").value("enabled", false),
           name + ": knowledge.enabled projection mismatch");
    Expect(projected.at("knowledge").at("selected_knowledge_ids") ==
               source.at("knowledge").at("selected_knowledge_ids"),
           name + ": knowledge.selected_knowledge_ids mismatch");
  }
  if (source.contains("interaction")) {
    Expect(projected.contains("interaction"), name + ": interaction block missing after projection");
    if (source.at("interaction").contains("image")) {
      Expect(projected.at("interaction").at("image") == source.at("interaction").at("image"),
             name + ": interaction.image mismatch");
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
            {"plane_name", "turboquant-enabled"},
            {"plane_mode", "llm"},
            {"model",
             {
                 {"source", {{"type", "local"}, {"path", "/models/qwen"}}},
                 {"materialization", {{"mode", "reference"}, {"local_path", "/models/qwen"}}},
                 {"served_model_name", "qwen-turboquant"},
             }},
            {"features",
             {{"turboquant",
               {{"enabled", true}, {"cache_type_k", "turbo4"}, {"cache_type_v", "turbo4"}}}}},
            {"runtime",
             {{"engine", "llama.cpp"}, {"distributed_backend", "llama_rpc"}, {"workers", 1}}},
            {"infer", {{"replicas", 1}}},
            {"app", {{"enabled", false}}},
        },
        "turboquant-enabled");

    ExpectRoundTrip(
        json{
            {"version", 2},
            {"plane_name", "context-compression-enabled"},
            {"plane_mode", "llm"},
            {"model",
             {
                 {"source", {{"type", "local"}, {"path", "/models/qwen"}}},
                 {"materialization", {{"mode", "reference"}, {"local_path", "/models/qwen"}}},
                 {"served_model_name", "qwen-context-compression"},
             }},
            {"features",
             {{"context_compression",
               {{"enabled", true},
                {"mode", "auto"},
                {"target", "dialog_and_knowledge"},
                {"memory_priority", "balanced"}}}}},
            {"runtime",
             {{"engine", "llama.cpp"}, {"distributed_backend", "llama_rpc"}, {"workers", 1}}},
            {"infer", {{"replicas", 1}}},
            {"app", {{"enabled", false}}},
        },
        "context-compression-enabled");

    ExpectRoundTrip(
        json{
            {"version", 2},
            {"plane_name", "combined-features"},
            {"plane_mode", "llm"},
            {"model",
             {
                 {"source", {{"type", "local"}, {"path", "/models/qwen"}}},
                 {"materialization", {{"mode", "reference"}, {"local_path", "/models/qwen"}}},
                 {"served_model_name", "qwen-combined"},
             }},
            {"features",
             {{"context_compression",
               {{"enabled", true},
                {"mode", "auto"},
                {"target", "dialog_and_knowledge"},
                {"memory_priority", "balanced"}}},
              {"turboquant",
               {{"enabled", true}, {"cache_type_k", "turbo4"}, {"cache_type_v", "turbo4"}}}}},
            {"runtime",
             {{"engine", "llama.cpp"}, {"distributed_backend", "llama_rpc"}, {"workers", 1}}},
            {"infer", {{"replicas", 1}}},
            {"app", {{"enabled", false}}},
        },
        "combined-features");

    ExpectRoundTrip(
        json{
            {"version", 2},
            {"plane_name", "interaction-image-override"},
            {"plane_mode", "llm"},
            {"model",
             {
                 {"source", {{"type", "local"}, {"path", "/models/qwen"}}},
                 {"materialization", {{"mode", "reference"}, {"local_path", "/models/qwen"}}},
                 {"served_model_name", "qwen-interaction-image"},
             }},
            {"interaction",
             {
                 {"image",
                  "chainzano.com/naim/interaction-runtime@sha256:feedface"},
                 {"thinking_enabled", false},
             }},
            {"runtime",
             {{"engine", "llama.cpp"}, {"distributed_backend", "llama_rpc"}, {"workers", 1}}},
            {"infer", {{"replicas", 1}}},
            {"app", {{"enabled", false}}},
        },
        "interaction-image-override");

    {
      const auto rendered = naim::DesiredStateV2Renderer::Render(
          json{
              {"version", 2},
              {"plane_name", "interaction-shared-disk"},
              {"plane_mode", "llm"},
              {"model",
               {
                   {"source", {{"type", "local"}, {"path", "/models/qwen"}}},
                   {"materialization", {{"mode", "reference"}, {"local_path", "/models/qwen"}}},
                   {"served_model_name", "qwen-interaction-shared"},
               }},
              {"runtime",
               {{"engine", "llama.cpp"}, {"distributed_backend", "llama_rpc"}, {"workers", 1}}},
              {"infer", {{"replicas", 1}}},
              {"app", {{"enabled", false}}},
          });
      const auto interaction_it = std::find_if(
          rendered.instances.begin(),
          rendered.instances.end(),
          [](const naim::InstanceSpec& instance) {
            return instance.role == naim::InstanceRole::Interaction;
          });
      Expect(interaction_it != rendered.instances.end(),
             "interaction-shared-disk: interaction instance missing");
      Expect(interaction_it->shared_disk_name == rendered.plane_shared_disk_name,
             "interaction-shared-disk: interaction instance must mount plane shared disk");
      std::cout << "ok-roundtrip: interaction-shared-disk\n";
    }

    ExpectRoundTrip(
        json{
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
                 {"selected_knowledge_ids", json::array({"knowledge.alpha", "knowledge.beta"})},
                 {"context_policy",
                  {{"include_graph", true}, {"max_graph_depth", 1}, {"token_budget", 12000}}},
             }},
            {"runtime",
             {{"engine", "llama.cpp"}, {"distributed_backend", "llama_rpc"}, {"workers", 1}}},
            {"infer", {{"replicas", 1}}},
            {"app", {{"enabled", false}}},
        },
        "knowledge-enabled");

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
            {"plane_name", "llm-prepare-on-worker"},
            {"plane_mode", "llm"},
            {"model",
             {
                 {"source", {{"type", "library"}, {"ref", "Qwen/Qwen3"}, {"path", "/storage/qwen"}}},
                 {"materialization",
                  {{"mode", "prepare_on_worker"},
                   {"local_path", "/storage/qwen"},
                   {"source_node_name", "storage-a"},
                   {"source_paths", json::array({"/storage/qwen"})},
                   {"source_format", "model-directory"},
                   {"desired_output_format", "gguf"},
                   {"quantization", "Q4_K_M"},
                   {"keep_source", false},
                   {"writeback",
                    {{"enabled", true},
                     {"if_missing", true},
                     {"target_node_name", "storage-a"}}}}},
                 {"served_model_name", "qwen-worker-prepared"},
             }},
            {"runtime",
             {{"engine", "llama.cpp"}, {"distributed_backend", "llama_rpc"}, {"workers", 1}}},
            {"infer", {{"replicas", 1}}},
            {"app", {{"enabled", false}}},
        },
        "llm-prepare-on-worker");

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

    {
      const json multi_app_plane{
          {"version", 2},
          {"plane_name", "multi-app-plane"},
          {"plane_mode", "llm"},
          {"model",
           {
               {"source", {{"type", "local"}, {"path", "/models/qwen"}}},
               {"materialization", {{"mode", "reference"}, {"local_path", "/models/qwen"}}},
               {"served_model_name", "qwen-multi-app"},
           }},
          {"runtime",
           {{"engine", "llama.cpp"}, {"distributed_backend", "llama_rpc"}, {"workers", 1}}},
          {"infer", {{"replicas", 1}}},
          {"apps",
           json::array(
               {{{"name", "chat"},
                 {"primary", true},
                 {"enabled", true},
                 {"image", "example/app:dev"},
                 {"start", {{"type", "script"}, {"value", "node server.js"}}}},
                {{"name", "market-ingest"},
                 {"enabled", true},
                 {"image", "example/app:dev"},
                 {"start", {{"type", "script"}, {"value", "node market-collector.js"}}},
                 {"node", "worker-node-a"}}})},
      };
      const auto rendered = naim::DesiredStateV2Renderer::Render(multi_app_plane);
      const auto projected = naim::DesiredStateV2Projector::Project(rendered);
      Expect(projected.contains("apps"), "multi-app-plane: apps should project");
      Expect(projected.at("apps").is_array(), "multi-app-plane: apps should be array");
      Expect(projected.at("apps").size() == 2, "multi-app-plane: expected two apps");
      Expect(!projected.contains("app"), "multi-app-plane: legacy app should be suppressed");
      const auto& primary_app = projected.at("apps").at(0);
      Expect(primary_app.at("name").get<std::string>() == "chat",
             "multi-app-plane: primary app name mismatch");
      Expect(primary_app.value("primary", false),
             "multi-app-plane: primary app flag mismatch");
      const auto& collector_app = projected.at("apps").at(1);
      Expect(collector_app.at("name").get<std::string>() == "market-ingest",
             "multi-app-plane: collector app name mismatch");
      Expect(!collector_app.value("primary", false),
             "multi-app-plane: collector should not be primary");
      ExpectRoundTrip(multi_app_plane, "multi-app-plane");
    }

    return 0;
  } catch (const std::exception& ex) {
    std::cerr << "desired_state_v2_projector_tests failed: " << ex.what() << '\n';
    return 1;
  }
}
