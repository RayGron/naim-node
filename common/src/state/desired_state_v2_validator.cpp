#include "comet/state/desired_state_v2_validator.h"

#include <map>
#include <set>
#include <stdexcept>
#include <string>

namespace comet {

namespace {

bool FieldEnabledByDefault(const nlohmann::json& service_json, bool default_value) {
  return service_json.value("enabled", default_value);
}

}  // namespace

void DesiredStateV2Validator::ValidateOrThrow(const nlohmann::json& value) {
  DesiredStateV2Validator(value).Validate();
}

DesiredStateV2Validator::DesiredStateV2Validator(const nlohmann::json& value) : value_(value) {}

void DesiredStateV2Validator::Validate() {
  ValidateTopLevel();
  ValidateModel();
  ValidateRuntime();
  ValidateTopology();
  ValidateInfer();
  ValidateWorker();
  ValidateApp();
  ValidateSkills();
  ValidateBrowsing();
  ValidateHooks();
}

void DesiredStateV2Validator::ValidateTopLevel() const {
  if (!value_.is_object()) {
    throw std::runtime_error("desired-state v2 must be a JSON object");
  }
  if (value_.value("version", 0) != 2) {
    throw std::runtime_error("desired-state v2 requires version=2");
  }
  if (value_.value("plane_name", std::string{}).empty()) {
    throw std::runtime_error("desired-state v2 requires non-empty plane_name");
  }
  if (!value_.contains("runtime") || !value_.at("runtime").is_object()) {
    throw std::runtime_error("desired-state v2 requires runtime object");
  }
}

void DesiredStateV2Validator::ValidateModel() const {
  const std::string plane_mode = value_.value("plane_mode", std::string("llm"));
  if (!value_.contains("model")) {
    if (plane_mode == "llm") {
      throw std::runtime_error("desired-state v2 requires model for plane_mode=llm");
    }
    return;
  }
  if (!value_.at("model").is_object()) {
    throw std::runtime_error("desired-state v2 model must be an object");
  }

  const auto& model = value_.at("model");
  if (!model.contains("source") || !model.at("source").is_object()) {
    throw std::runtime_error("desired-state v2 model requires source object");
  }
  const auto& source = model.at("source");
  const std::string source_type = source.value("type", "");
  if (source_type.empty()) {
    throw std::runtime_error("desired-state v2 model.source requires type");
  }
  if ((source_type == "catalog" || source_type == "huggingface") &&
      source.value("ref", std::string{}).empty()) {
    throw std::runtime_error(
        "desired-state v2 model.source.ref is required for catalog/huggingface models");
  }
  if (source_type == "url" && source.value("url", std::string{}).empty()) {
    if (!source.contains("urls") || !source.at("urls").is_array() || source.at("urls").empty()) {
      throw std::runtime_error(
          "desired-state v2 url model requires source.url or non-empty source.urls");
    }
  }
  if (source_type == "local" && source.value("path", std::string{}).empty() &&
      source.value("ref", std::string{}).empty()) {
    throw std::runtime_error(
        "desired-state v2 local model requires source.path or source.ref");
  }
  if (plane_mode == "llm" && model.value("served_model_name", std::string{}).empty()) {
    throw std::runtime_error("desired-state v2 LLM model requires served_model_name");
  }
  if (model.contains("materialization") && model.at("materialization").is_object()) {
    const auto& materialization = model.at("materialization");
    if (materialization.value("mode", std::string{}) == "reference" &&
        materialization.value("local_path", std::string{}).empty() &&
        source.value("path", std::string{}).empty()) {
      throw std::runtime_error(
          "desired-state v2 reference materialization requires local_path or source.path");
    }
  }
}

void DesiredStateV2Validator::ValidateRuntime() const {
  const auto& runtime = value_.at("runtime");
  const std::string plane_mode = value_.value("plane_mode", std::string("llm"));
  if (runtime.value("engine", std::string{}).empty()) {
    throw std::runtime_error("desired-state v2 runtime requires engine");
  }
  if (!runtime.contains("workers") || !runtime.at("workers").is_number_integer() ||
      runtime.at("workers").get<int>() <= 0) {
    throw std::runtime_error("desired-state v2 runtime.workers must be a positive integer");
  }
  const std::string engine = runtime.value("engine", std::string{});
  const std::string data_parallel_mode = runtime.value("data_parallel_mode", std::string("off"));
  const std::string distributed_backend =
      runtime.value("distributed_backend", engine == "llama.cpp" ? std::string("llama_rpc")
                                                                  : std::string("local"));
  if (plane_mode == "llm" && engine != "llama.cpp") {
    throw std::runtime_error(
        "desired-state v2 llm planes require runtime.engine=llama.cpp");
  }
  if (plane_mode == "llm" && distributed_backend != "llama_rpc") {
    throw std::runtime_error(
        "desired-state v2 llm planes require runtime.distributed_backend=llama_rpc");
  }
  if (data_parallel_mode != "off") {
    throw std::runtime_error(
        "desired-state v2 no longer supports data_parallel_mode; use infer.replicas with "
        "llama.cpp runtime.distributed_backend=llama_rpc");
  }
  if (runtime.contains("data_parallel_lb_mode")) {
    throw std::runtime_error(
        "desired-state v2 no longer supports data_parallel_lb_mode; use infer.replicas with "
        "llama.cpp runtime.distributed_backend=llama_rpc");
  }
  if (engine == "llama.cpp" && distributed_backend == "llama_rpc" &&
      runtime.at("workers").get<int>() <= 0) {
    throw std::runtime_error(
        "desired-state v2 llama_rpc runtime requires at least one worker");
  }
  if (engine == "llama.cpp" && distributed_backend == "llama_rpc" &&
      runtime.contains("data_parallel_lb_mode")) {
    throw std::runtime_error(
        "desired-state v2 llama_rpc runtime does not use data_parallel_lb_mode");
  }
}

void DesiredStateV2Validator::ValidateTopology() const {
  if (!value_.contains("topology")) {
    return;
  }
  RequireObject("topology");
  const auto& topology = value_.at("topology");
  if (!topology.contains("nodes")) {
    throw std::runtime_error("desired-state v2 topology requires nodes array");
  }
  if (!topology.at("nodes").is_array() || topology.at("nodes").empty()) {
    throw std::runtime_error("desired-state v2 topology.nodes must be a non-empty array");
  }
  std::set<std::string> node_names;
  for (const auto& node : topology.at("nodes")) {
    if (!node.is_object()) {
      throw std::runtime_error("desired-state v2 topology.nodes items must be objects");
    }
    const std::string node_name = node.value("name", std::string{});
    if (node_name.empty()) {
      throw std::runtime_error("desired-state v2 topology node requires name");
    }
    if (!node_names.insert(node_name).second) {
      throw std::runtime_error("desired-state v2 topology node names must be unique");
    }
  }
}

void DesiredStateV2Validator::ValidateInfer() const {
  const std::string plane_mode = value_.value("plane_mode", std::string("llm"));
  const auto& runtime = value_.at("runtime");
  const std::string engine = runtime.value("engine", std::string{});
  const std::string distributed_backend =
      runtime.value("distributed_backend", engine == "llama.cpp" ? std::string("llama_rpc")
                                                                  : std::string("local"));
  if (plane_mode == "llm") {
    if (!value_.contains("infer") || !value_.at("infer").is_object()) {
      throw std::runtime_error(
          "desired-state v2 llm planes require infer object with replicas");
    }
    if (!value_.at("infer").contains("replicas")) {
      throw std::runtime_error(
          "desired-state v2 llm planes require infer.replicas for llama replica-parallel layout");
    }
  }
  if (!value_.contains("infer")) {
    return;
  }
  RequireObject("infer");
  const auto& infer = value_.at("infer");
  if (infer.contains("node") && !infer.at("node").is_string()) {
    throw std::runtime_error("desired-state v2 infer.node must be a string");
  }
  if (infer.contains("node") && infer.at("node").is_string()) {
    ValidateNodeRoleCompatibility(infer.at("node").get<std::string>(), "infer", "infer");
  }
  if (infer.contains("replicas")) {
    if (!infer.at("replicas").is_number_integer() || infer.at("replicas").get<int>() <= 0) {
      throw std::runtime_error("desired-state v2 infer.replicas must be a positive integer");
    }
    if (!(engine == "llama.cpp" && distributed_backend == "llama_rpc")) {
      throw std::runtime_error(
          "desired-state v2 infer.replicas is currently supported only for llama.cpp "
          "runtime.distributed_backend=llama_rpc");
    }
    const int workers = runtime.at("workers").get<int>();
    const int replicas = infer.at("replicas").get<int>();
    if (workers % replicas != 0) {
      throw std::runtime_error(
          "desired-state v2 infer.replicas requires runtime.workers to be divisible by replicas");
    }
  }
  ValidateStartBlock(infer, "infer");
}

void DesiredStateV2Validator::ValidateWorker() const {
  if (!value_.contains("worker")) {
    ValidateWorkerResources();
    return;
  }
  RequireObject("worker");
  const auto& worker = value_.at("worker");
  if (worker.contains("node") && !worker.at("node").is_string()) {
    throw std::runtime_error("desired-state v2 worker.node must be a string");
  }
  if (worker.contains("gpu_device") && !worker.at("gpu_device").is_string()) {
    throw std::runtime_error("desired-state v2 worker.gpu_device must be a string");
  }
  if (worker.contains("assignments")) {
    if (!worker.at("assignments").is_array()) {
      throw std::runtime_error("desired-state v2 worker.assignments must be an array");
    }
    const int expected_workers = value_.at("runtime").at("workers").get<int>();
    if (static_cast<int>(worker.at("assignments").size()) != expected_workers) {
      throw std::runtime_error(
          "desired-state v2 worker.assignments size must match runtime.workers");
    }
    for (const auto& assignment : worker.at("assignments")) {
      if (!assignment.is_object()) {
        throw std::runtime_error(
            "desired-state v2 worker.assignments items must be objects");
      }
      if (!assignment.contains("node") || !assignment.at("node").is_string()) {
        throw std::runtime_error(
            "desired-state v2 worker.assignments items require string node");
      }
      ValidateNodeRoleCompatibility(
          assignment.at("node").get<std::string>(),
          "worker assignment",
          "worker");
      if (assignment.contains("gpu_device") && !assignment.at("gpu_device").is_string()) {
        throw std::runtime_error(
            "desired-state v2 worker.assignments gpu_device must be a string");
      }
    }
  }
  if (worker.contains("node") && worker.at("node").is_string()) {
    ValidateNodeRoleCompatibility(worker.at("node").get<std::string>(), "worker", "worker");
  }
  ValidateStartBlock(value_.at("worker"), "worker");
  ValidateWorkerResources();
}

void DesiredStateV2Validator::ValidateWorkerResources() const {
  if (!value_.contains("resources") || !value_.at("resources").is_object()) {
    return;
  }
  const auto& resources = value_.at("resources");
  if (!resources.contains("worker") || !resources.at("worker").is_object()) {
    return;
  }
  const auto& worker_resources = resources.at("worker");
  const std::string share_mode = worker_resources.value("share_mode", std::string{});
  if (share_mode == "exclusive" && worker_resources.contains("gpu_fraction")) {
    const double gpu_fraction = worker_resources.at("gpu_fraction").get<double>();
    if (std::abs(gpu_fraction - 1.0) > 1e-9) {
      throw std::runtime_error(
          "desired-state v2 resources.worker.gpu_fraction must be 1.0 when share_mode=exclusive");
    }
  }
}

void DesiredStateV2Validator::ValidateApp() const {
  if (!value_.contains("app")) {
    return;
  }
  RequireObject("app");
  const auto& app = value_.at("app");
  if (!FieldEnabledByDefault(app, true)) {
    return;
  }
  if (app.contains("node") && !app.at("node").is_string()) {
    throw std::runtime_error("desired-state v2 app.node must be a string");
  }
  if (app.contains("node") && app.at("node").is_string()) {
    ValidateNodeRoleCompatibility(app.at("node").get<std::string>(), "app", "app");
  }
  if (app.value("image", std::string{}).empty()) {
    throw std::runtime_error("desired-state v2 enabled app requires image");
  }
  ValidateStartBlock(app, "app");
  if (app.contains("volumes")) {
    if (!app.at("volumes").is_array()) {
      throw std::runtime_error("desired-state v2 app.volumes must be an array");
    }
    if (app.at("volumes").size() > 1) {
      throw std::runtime_error("desired-state v2 currently supports at most one app volume");
    }
  }
}

void DesiredStateV2Validator::ValidateSkills() const {
  if (!value_.contains("skills")) {
    return;
  }
  RequireObject("skills");
  const std::string plane_mode = value_.value("plane_mode", std::string("llm"));
  if (plane_mode != "llm") {
    throw std::runtime_error("desired-state v2 skills are supported only for plane_mode=llm");
  }
  const auto& skills = value_.at("skills");
  const bool skills_enabled = FieldEnabledByDefault(skills, false);
  if (!skills_enabled && skills.contains("factory_skill_ids")) {
    throw std::runtime_error(
        "desired-state v2 skills.factory_skill_ids require skills.enabled=true");
  }
  if (!skills_enabled) {
    return;
  }
  if (skills.contains("factory_skill_ids")) {
    if (!skills.at("factory_skill_ids").is_array()) {
      throw std::runtime_error("desired-state v2 skills.factory_skill_ids must be an array");
    }
    std::set<std::string> seen_skill_ids;
    for (const auto& item : skills.at("factory_skill_ids")) {
      if (!item.is_string() || item.get<std::string>().empty()) {
        throw std::runtime_error(
            "desired-state v2 skills.factory_skill_ids items must be non-empty strings");
      }
      if (!seen_skill_ids.insert(item.get<std::string>()).second) {
        throw std::runtime_error(
            "desired-state v2 skills.factory_skill_ids items must be unique");
      }
    }
  }
  if (skills.contains("node") && !skills.at("node").is_string()) {
    throw std::runtime_error("desired-state v2 skills.node must be a string");
  }
  if (skills.contains("node") && skills.at("node").is_string()) {
    ValidateNodeRoleCompatibility(skills.at("node").get<std::string>(), "skills", "skills");
  }
  ValidateStartBlock(skills, "skills");
}

void DesiredStateV2Validator::ValidateBrowsing() const {
  if (!value_.contains("browsing")) {
    return;
  }
  RequireObject("browsing");
  const std::string plane_mode = value_.value("plane_mode", std::string("llm"));
  if (plane_mode != "llm") {
    throw std::runtime_error("desired-state v2 browsing is supported only for plane_mode=llm");
  }
  const auto& browsing = value_.at("browsing");
  const bool browsing_enabled = FieldEnabledByDefault(browsing, false);
  if (!browsing_enabled) {
    return;
  }
  if (browsing.contains("node") && !browsing.at("node").is_string()) {
    throw std::runtime_error("desired-state v2 browsing.node must be a string");
  }
  if (browsing.contains("node") && browsing.at("node").is_string()) {
    ValidateNodeRoleCompatibility(
        browsing.at("node").get<std::string>(),
        "browsing",
        "browsing");
  }
  if (browsing.contains("policy")) {
    if (!browsing.at("policy").is_object()) {
      throw std::runtime_error("desired-state v2 browsing.policy must be an object");
    }
    const auto& policy = browsing.at("policy");
    if (policy.contains("browser_session_enabled") &&
        !policy.at("browser_session_enabled").is_boolean()) {
      throw std::runtime_error(
          "desired-state v2 browsing.policy.browser_session_enabled must be a boolean");
    }
    if (policy.contains("rendered_browser_enabled") &&
        !policy.at("rendered_browser_enabled").is_boolean()) {
      throw std::runtime_error(
          "desired-state v2 browsing.policy.rendered_browser_enabled must be a boolean");
    }
    if (policy.contains("login_enabled") &&
        !policy.at("login_enabled").is_boolean()) {
      throw std::runtime_error(
          "desired-state v2 browsing.policy.login_enabled must be a boolean");
    }
    const auto validate_domain_list = [&](const char* field_name) {
      if (!policy.contains(field_name)) {
        return;
      }
      if (!policy.at(field_name).is_array()) {
        throw std::runtime_error(
            std::string("desired-state v2 browsing.policy.") + field_name + " must be an array");
      }
      std::set<std::string> seen_domains;
      for (const auto& item : policy.at(field_name)) {
        if (!item.is_string() || item.get<std::string>().empty()) {
          throw std::runtime_error(
              std::string("desired-state v2 browsing.policy.") + field_name +
              " items must be non-empty strings");
        }
        if (!seen_domains.insert(item.get<std::string>()).second) {
          throw std::runtime_error(
              std::string("desired-state v2 browsing.policy.") + field_name +
              " items must be unique");
        }
      }
    };
    validate_domain_list("allowed_domains");
    validate_domain_list("blocked_domains");
    if (policy.contains("max_search_results") &&
        (!policy.at("max_search_results").is_number_integer() ||
         policy.at("max_search_results").get<int>() <= 0)) {
      throw std::runtime_error(
          "desired-state v2 browsing.policy.max_search_results must be a positive integer");
    }
    if (policy.contains("max_fetch_bytes") &&
        (!policy.at("max_fetch_bytes").is_number_integer() ||
         policy.at("max_fetch_bytes").get<int>() <= 0)) {
      throw std::runtime_error(
          "desired-state v2 browsing.policy.max_fetch_bytes must be a positive integer");
    }
  }
  ValidateStartBlock(browsing, "browsing");
}

void DesiredStateV2Validator::ValidateHooks() const {
  if (!value_.contains("hooks")) {
    return;
  }
  RequireObject("hooks");
}

void DesiredStateV2Validator::RequireObject(const char* field_name) const {
  if (!value_.at(field_name).is_object()) {
    throw std::runtime_error(std::string("desired-state v2 ") + field_name + " must be an object");
  }
}

void DesiredStateV2Validator::ValidateStartBlock(
    const nlohmann::json& service_json,
    const char* service_name) const {
  if (!service_json.contains("start")) {
    return;
  }
  if (!service_json.at("start").is_object()) {
    throw std::runtime_error(
        std::string("desired-state v2 ") + service_name + ".start must be an object");
  }
  const auto& start = service_json.at("start");
  const std::string start_type = start.value("type", "");
  if (start_type == "script" && start.value("script_ref", std::string{}).empty()) {
    throw std::runtime_error(
        std::string("desired-state v2 ") + service_name + ".start.script_ref is required");
  }
  if (start_type == "command" && start.value("command", std::string{}).empty()) {
    throw std::runtime_error(
        std::string("desired-state v2 ") + service_name + ".start.command is required");
  }
}

std::optional<std::string> DesiredStateV2Validator::TopologyNodeExecutionMode(
    const std::string& node_name) const {
  if (!value_.contains("topology") || !value_.at("topology").is_object()) {
    return std::nullopt;
  }
  const auto& topology = value_.at("topology");
  if (!topology.contains("nodes") || !topology.at("nodes").is_array()) {
    return std::nullopt;
  }
  for (const auto& node : topology.at("nodes")) {
    if (node.value("name", std::string{}) == node_name) {
      return node.value("execution_mode", std::string("mixed"));
    }
  }
  return std::nullopt;
}

void DesiredStateV2Validator::ValidateNodeRoleCompatibility(
    const std::string& node_name,
    const char* service_name,
    const char* required_role) const {
  const std::string required(required_role);
  const auto mode = TopologyNodeExecutionMode(node_name);
  if (!mode.has_value()) {
    throw std::runtime_error(
        std::string("desired-state v2 ") + service_name + " references unknown node '" +
        node_name + "'");
  }
  if (required == "infer" || required == "app") {
    if (*mode == "worker-only") {
      throw std::runtime_error(
          std::string("desired-state v2 ") + service_name +
          " cannot target worker-only node '" + node_name + "'");
    }
    return;
  }
  if (required == "skills") {
    if (*mode == "worker-only") {
      throw std::runtime_error(
          std::string("desired-state v2 ") + service_name +
          " cannot target worker-only node '" + node_name + "'");
    }
    return;
  }
  if (required == "browsing") {
    if (*mode == "worker-only") {
      throw std::runtime_error(
          std::string("desired-state v2 ") + service_name +
          " cannot target worker-only node '" + node_name + "'");
    }
    return;
  }
  if (required == "worker" && *mode == "infer-only") {
    throw std::runtime_error(
        std::string("desired-state v2 ") + service_name +
        " cannot target infer-only node '" + node_name + "'");
  }
}

}  // namespace comet
