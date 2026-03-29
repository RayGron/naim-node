#include "comet/state/desired_state_v2_validator.h"

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
  if (runtime.value("engine", std::string{}).empty()) {
    throw std::runtime_error("desired-state v2 runtime requires engine");
  }
  if (!runtime.contains("workers") || !runtime.at("workers").is_number_integer() ||
      runtime.at("workers").get<int>() <= 0) {
    throw std::runtime_error("desired-state v2 runtime.workers must be a positive integer");
  }
  const std::string engine = runtime.value("engine", std::string{});
  const std::string data_parallel_mode = runtime.value("data_parallel_mode", std::string("off"));
  if ((data_parallel_mode == "vllm_native" || data_parallel_mode == "auto_replicas") &&
      engine != "vllm") {
    throw std::runtime_error(
        "desired-state v2 native data parallel mode requires runtime.engine=vllm");
  }
  if (runtime.contains("data_parallel_lb_mode") &&
      data_parallel_mode != "vllm_native" && data_parallel_mode != "auto_replicas") {
    throw std::runtime_error(
        "desired-state v2 data_parallel_lb_mode requires native data parallel mode");
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
  ValidateStartBlock(infer, "infer");
}

void DesiredStateV2Validator::ValidateWorker() const {
  if (!value_.contains("worker")) {
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
  if (required == "worker" && *mode == "infer-only") {
    throw std::runtime_error(
        std::string("desired-state v2 ") + service_name +
        " cannot target infer-only node '" + node_name + "'");
  }
}

}  // namespace comet
