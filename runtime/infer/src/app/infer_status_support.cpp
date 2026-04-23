#include "app/infer_status_support.h"

#include <algorithm>
#include <array>
#include <cctype>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <set>
#include <string>
#include <vector>

#include <nlohmann/json.hpp>

#include <unistd.h>

#include "runtime/infer_control_support.h"
#include "runtime/infer_replica_support.h"
#include "runtime/infer_runtime_support.h"
#include "naim/runtime/runtime_status.h"

namespace naim::infer::status_support {

namespace fs = std::filesystem;
using control_support::BuildControlPaths;
using control_support::EnabledGpuNodeCount;
using control_support::LoadActiveModel;
using control_support::LoadGatewayPlan;
using control_support::LoadRegistry;
using runtime_support::LoadWorkerGroupStatus;
using runtime_support::RuntimeGatewayHealthUrl;
using runtime_support::RuntimeUpstreamHealthUrl;
using runtime_support::RuntimeUpstreamModelsUrl;
using nlohmann::json;
namespace replica_support = naim::infer::replica_support;

namespace {

bool StringContains(const std::string& haystack, const std::string& needle) {
  return !needle.empty() && haystack.find(needle) != std::string::npos;
}

std::string Join(const std::vector<std::string>& values, const std::string& delimiter) {
  std::string joined;
  for (std::size_t index = 0; index < values.size(); ++index) {
    if (index > 0) {
      joined += delimiter;
    }
    joined += values[index];
  }
  return joined;
}

std::vector<std::string> SplitCsv(const std::string& value) {
  std::vector<std::string> parts;
  std::string current;
  for (char ch : value) {
    if (ch == ',') {
      if (!current.empty()) {
        parts.push_back(current);
        current.clear();
      }
    } else if (!std::isspace(static_cast<unsigned char>(ch))) {
      current.push_back(ch);
    }
  }
  if (!current.empty()) {
    parts.push_back(current);
  }
  return parts;
}

std::string ResolveLlamaServerPath(const RuntimeConfig& config) {
  const char* override_path = std::getenv("NAIM_LLAMA_SERVER_BIN");
  if (override_path != nullptr && *override_path != '\0') {
    return override_path;
  }
  if (config.runtime_flavor == "turboquant") {
    return "/runtime/bin/turboquant/llama-server";
  }
  return "/runtime/bin/llama-server";
}

bool CommandExists(const std::string& command) {
  const std::string test = "command -v " + command + " >/dev/null 2>&1";
  return std::system(test.c_str()) == 0;
}

bool IsLiveRuntimePhase(const std::string& phase) {
  return phase == "starting" || phase == "running" || phase == "stopping";
}

naim::RuntimeStatus BuildRuntimeStatus(
    const RuntimeConfig& config,
    const std::string& backend,
    const std::string& phase,
    bool inference_ready,
    bool gateway_ready,
    int supervisor_pid,
    const std::string& started_at);

std::string ShellQuote(const std::string& value) {
  std::string quoted = "'";
  for (const char ch : value) {
    if (ch == '\'') {
      quoted += "'\\''";
      continue;
    }
    quoted.push_back(ch);
  }
  quoted.push_back('\'');
  return quoted;
}

std::optional<std::string> CaptureCommandOutput(const std::string& command) {
  FILE* pipe = popen(command.c_str(), "r");
  if (pipe == nullptr) {
    return std::nullopt;
  }
  std::string output;
  std::array<char, 4096> buffer{};
  while (std::fgets(buffer.data(), static_cast<int>(buffer.size()), pipe) != nullptr) {
    output += buffer.data();
  }
  const int rc = pclose(pipe);
  if (rc != 0 && output.empty()) {
    return std::nullopt;
  }
  return output;
}

std::optional<std::string> ValidateTurboQuantSupport(const RuntimeConfig& config) {
  const json active_model = LoadActiveModel(config);
  if (!active_model.value("turboquant_enabled", false)) {
    return std::nullopt;
  }
  const std::string cache_type_k = active_model.value("active_cache_type_k", std::string{});
  const std::string cache_type_v = active_model.value("active_cache_type_v", std::string{});
  const std::string llama_server = ResolveLlamaServerPath(config);
  const auto help_output =
      CaptureCommandOutput(ShellQuote(llama_server) + " --help 2>&1");
  if (!help_output.has_value()) {
    return "failed to inspect llama-server at " + llama_server;
  }
  if (!StringContains(*help_output, "--cache-type-k") ||
      !StringContains(*help_output, "--cache-type-v")) {
    return "llama-server does not advertise --cache-type-k/--cache-type-v support: " +
           llama_server;
  }
  if (!cache_type_k.empty() && !StringContains(*help_output, cache_type_k)) {
    return "llama-server help does not mention requested K cache type '" + cache_type_k + "'";
  }
  if (!cache_type_v.empty() && !StringContains(*help_output, cache_type_v)) {
    return "llama-server help does not mention requested V cache type '" + cache_type_v + "'";
  }
  return std::nullopt;
}

void SaveDoctorFailureStatus(
    const RuntimeConfig& config,
    const std::string& backend,
    const std::string& reason,
    const std::string& detail) {
  naim::RuntimeStatus status =
      BuildRuntimeStatus(config, backend, "planned", false, false, 0, "");
  status.status_reason = reason;
  status.failure_detail = detail;
  status.inference_ready = false;
  status.gateway_ready = false;
  status.launch_ready = false;
  status.ready = false;
  naim::SaveRuntimeStatusJson(status, BuildControlPaths(config).runtime_status_path.string());
}

json BuildGatewayPayload(const RuntimeConfig& config) {
  const json active_model = LoadActiveModel(config);
  const std::string upstream_health_url = RuntimeUpstreamHealthUrl(config);
  const std::string upstream_models_url = RuntimeUpstreamModelsUrl(config);
  return json{
      {"version", 1},
      {"plane_name", config.plane_name},
      {"listen_host", config.gateway_listen_host},
      {"listen_port", config.gateway_listen_port},
      {"server_name", config.gateway_server_name},
      {"proxy_health_url", upstream_health_url},
      {"upstream_health_url", upstream_health_url},
      {"upstream_models_url", upstream_models_url},
      {"active_served_model_name", active_model.value("served_model_name", std::string{})},
      {"active_model_id", active_model.value("model_id", std::string{})},
  };
}

naim::RuntimeStatus BuildRuntimeStatus(
    const RuntimeConfig& config,
    const std::string& backend,
    const std::string& phase,
    bool inference_ready,
    bool gateway_ready,
    int supervisor_pid,
    const std::string& started_at) {
  const json registry = LoadRegistry(config);
  const json active_model = LoadActiveModel(config);
  const json gateway_plan = LoadGatewayPlan(config);
  const auto topology = replica_support::InspectReplicaTopology(config);
  naim::RuntimeStatus status;
  status.plane_name = config.plane_name;
  status.control_root = config.control_root;
  status.controller_url = config.controller_url;
  status.primary_infer_node = config.primary_infer_node;
  if (const char* instance_name = std::getenv("NAIM_INSTANCE_NAME")) {
    status.instance_name = instance_name;
  }
  if (const char* instance_role = std::getenv("NAIM_INSTANCE_ROLE")) {
    status.instance_role = instance_role;
  }
  if (const char* node_name = std::getenv("NAIM_NODE_NAME")) {
    status.node_name = node_name;
  }
  status.runtime_backend = backend;
  status.runtime_phase = phase;
  status.data_parallel_mode = topology.data_parallel_mode;
  status.data_parallel_lb_mode = topology.data_parallel_lb_mode;
  status.data_parallel_size = topology.data_parallel_size;
  status.data_parallel_size_local_max = topology.data_parallel_size_local_max;
  status.replica_groups_expected = topology.replica_groups_expected;
  status.replica_groups_ready = topology.replica_groups_ready;
  status.replica_groups_degraded = topology.replica_groups_degraded;
  status.api_endpoints_expected = topology.api_endpoints_expected;
  status.api_endpoints_ready = topology.api_endpoints_ready;
  status.enabled_gpu_nodes = EnabledGpuNodeCount(config);
  status.registry_entries = topology.ready_worker_members > 0
                                ? topology.ready_worker_members
                                : static_cast<int>(registry.value("entries", json::array()).size());
  status.supervisor_pid = supervisor_pid;
  status.runtime_pid = supervisor_pid;
  status.engine_pid = supervisor_pid;
  status.active_model_id = active_model.value("model_id", std::string{});
  status.active_served_model_name = active_model.value("served_model_name", std::string{});
  status.active_runtime_profile = active_model.value("runtime_profile", std::string{});
  status.turboquant_enabled = active_model.value("turboquant_enabled", false);
  status.active_cache_type_k = active_model.value("active_cache_type_k", std::string{});
  status.active_cache_type_v = active_model.value("active_cache_type_v", std::string{});
  status.cached_local_model_path = active_model.value(
      "cached_runtime_model_path",
      active_model.value("cached_local_model_path", std::string{}));
  status.model_path = status.cached_local_model_path;
  status.gateway_listen =
      config.gateway_listen_host + ":" + std::to_string(config.gateway_listen_port);
  status.upstream_models_url = RuntimeUpstreamModelsUrl(config);
  status.inference_health_url = RuntimeUpstreamHealthUrl(config);
  status.gateway_health_url = RuntimeGatewayHealthUrl(config);
  status.started_at = started_at;
  status.last_activity_at = started_at;
  const bool replica_topology_ready =
      topology.replica_groups_expected == 0 ||
      topology.replica_groups_ready >= topology.replica_groups_expected;
  status.active_model_ready = !active_model.empty();
  status.gateway_plan_ready = !gateway_plan.empty();
  status.inference_ready = inference_ready;
  status.gateway_ready = gateway_ready;
  status.launch_ready = status.active_model_ready && status.inference_ready &&
                        status.gateway_ready && replica_topology_ready;
  status.ready = status.launch_ready;
  for (const auto& entry : registry.value("entries", json::array())) {
    if (entry.is_object() && entry.contains("alias")) {
      const std::string alias = entry.at("alias").get<std::string>();
      if (!alias.empty()) {
        status.aliases.push_back(alias);
      }
    }
  }
  std::sort(status.aliases.begin(), status.aliases.end());
  return status;
}

naim::RuntimeStatus MergeWithObservedRuntimeStatus(
    naim::RuntimeStatus status,
    const std::string& path) {
  const std::optional<naim::RuntimeStatus> observed = naim::LoadRuntimeStatusJson(path);
  if (!observed.has_value()) {
    return status;
  }

  if (!observed->runtime_backend.empty()) {
    status.runtime_backend = observed->runtime_backend;
  }
  if (!observed->runtime_phase.empty()) {
    status.runtime_phase = observed->runtime_phase;
  }
  status.turboquant_enabled = observed->turboquant_enabled || status.turboquant_enabled;
  if (!observed->active_cache_type_k.empty()) {
    status.active_cache_type_k = observed->active_cache_type_k;
  }
  if (!observed->active_cache_type_v.empty()) {
    status.active_cache_type_v = observed->active_cache_type_v;
  }
  if (!observed->status_reason.empty()) {
    status.status_reason = observed->status_reason;
  }
  if (!observed->failure_detail.empty()) {
    status.failure_detail = observed->failure_detail;
  }
  status.supervisor_pid = observed->supervisor_pid;
  if (!observed->started_at.empty()) {
    status.started_at = observed->started_at;
  }

  if (IsLiveRuntimePhase(observed->runtime_phase) || observed->runtime_phase == "stopped") {
    status.inference_ready = observed->inference_ready;
    status.gateway_ready = observed->gateway_ready;
    status.launch_ready =
        status.active_model_ready && status.inference_ready && status.gateway_ready;
  }
  if (observed->kv_cache_bytes.has_value()) {
    status.kv_cache_bytes = observed->kv_cache_bytes;
  }

  return status;
}

}  // namespace

void PrintGatewayPlan(const RuntimeConfig& config, bool apply) {
  const json payload = BuildGatewayPayload(config);
  std::cout << "gateway-plan:\n";
  std::cout << "  listen=" << payload.at("listen_host").get<std::string>() << ":"
            << payload.at("listen_port").get<int>() << "\n";
  std::cout << "  server_name=" << payload.at("server_name").get<std::string>() << "\n";
  std::cout << "  upstream_models_url=" << payload.at("upstream_models_url").get<std::string>()
            << "\n";
  std::cout << "  upstream_health_url=" << payload.at("upstream_health_url").get<std::string>()
            << "\n";
  std::cout << "  proxy_health_url=" << payload.at("proxy_health_url").get<std::string>() << "\n";
  std::cout << "  active_model=" << payload.value("active_model_id", std::string{"(empty)"})
            << " served="
            << payload.value("active_served_model_name", std::string{"(empty)"}) << "\n";
  std::cout << "  gateway_plan_path=" << BuildControlPaths(config).gateway_plan_path.string()
            << "\n";
  if (apply) {
    naim::SaveRuntimeStatusJson(
        BuildRuntimeStatus(config, "gateway-plan", "planned", false, false, 0, ""),
        BuildControlPaths(config).runtime_status_path.string());
    std::ofstream output(BuildControlPaths(config).gateway_plan_path);
    output << payload.dump(2) << "\n";
  }
}

int PrintGatewayStatus(const RuntimeConfig& config) {
  const json payload = LoadGatewayPlan(config);
  if (payload.empty()) {
    std::cout << "(empty)\n";
    return 1;
  }
  std::cout << "gateway-status:\n";
  std::cout << "  listen=" << payload.value("listen_host", std::string{}) << ":"
            << payload.value("listen_port", 0) << "\n";
  std::cout << "  server_name=" << payload.value("server_name", std::string{}) << "\n";
  std::cout << "  upstream_models_url=" << payload.value("upstream_models_url", std::string{})
            << "\n";
  std::cout << "  upstream_health_url=" << payload.value("upstream_health_url", std::string{})
            << "\n";
  std::cout << "  proxy_health_url=" << payload.value("proxy_health_url", std::string{}) << "\n";
  std::cout << "  active_model=" << payload.value("active_model_id", std::string{"(empty)"})
            << " served="
            << payload.value("active_served_model_name", std::string{"(empty)"}) << "\n";
  return 0;
}

int PrintStatus(const RuntimeConfig& config, const std::string& backend, bool apply) {
  const auto paths = BuildControlPaths(config);
  naim::RuntimeStatus status =
      BuildRuntimeStatus(config, backend, "planned", false, false, 0, "");
  status = MergeWithObservedRuntimeStatus(std::move(status), paths.runtime_status_path.string());
  std::cout << "[runtime]\n";
  std::cout << "plane=" << status.plane_name << "\n";
  std::cout << "control_root=" << status.control_root << "\n";
  std::cout << "controller_url=" << status.controller_url << "\n";
  std::cout << "primary_infer_node=" << status.primary_infer_node << "\n";
  std::cout << "runtime_phase=" << status.runtime_phase << "\n";
  std::cout << "data_parallel_mode=" << status.data_parallel_mode << "\n";
  std::cout << "data_parallel_lb_mode=" << status.data_parallel_lb_mode << "\n";
  std::cout << "data_parallel_size=" << status.data_parallel_size << "\n";
  std::cout << "data_parallel_size_local_max=" << status.data_parallel_size_local_max << "\n";
  std::cout << "enabled_gpu_nodes=" << status.enabled_gpu_nodes << "\n\n";
  std::cout << "[cache]\n";
  std::cout << "registry_entries=" << status.registry_entries << "\n";
  std::cout << "aliases=" << (status.aliases.empty() ? "(empty)" : Join(status.aliases, ","))
            << "\n\n";
  std::cout << "[active-model]\n";
  if (status.active_model_ready) {
    std::cout << "model_id=" << status.active_model_id << "\n";
    std::cout << "served_model_name=" << status.active_served_model_name << "\n";
    std::cout << "runtime_profile=" << status.active_runtime_profile << "\n";
    std::cout << "cached_local_model_path="
              << (status.cached_local_model_path.empty() ? "(empty)"
                                                         : status.cached_local_model_path)
              << "\n";
    const json active_model = LoadActiveModel(config);
    std::cout << "cached_runtime_model_path="
              << active_model.value(
                     "cached_runtime_model_path",
                     active_model.value("cached_local_model_path", std::string{"(empty)"}))
              << "\n";
    std::cout << "turboquant_enabled=" << (status.turboquant_enabled ? "yes" : "no") << "\n";
    std::cout << "active_cache_type_k="
              << (status.active_cache_type_k.empty() ? "(empty)" : status.active_cache_type_k)
              << "\n";
    std::cout << "active_cache_type_v="
              << (status.active_cache_type_v.empty() ? "(empty)" : status.active_cache_type_v)
              << "\n";
  } else {
    std::cout << "state=(empty)\n";
  }
  std::cout << "\n[gateway]\n";
  if (status.gateway_plan_ready) {
    std::cout << "listen=" << status.gateway_listen << "\n";
    std::cout << "upstream_models_url=" << status.upstream_models_url << "\n";
    std::cout << "active_model="
              << (status.active_model_id.empty() ? "(empty)" : status.active_model_id)
              << " served="
              << (status.active_served_model_name.empty() ? "(empty)"
                                                          : status.active_served_model_name)
              << "\n";
  } else {
    std::cout << "state=(empty)\n";
  }
  std::cout << "\n[readiness]\n";
  std::cout << "active_model_ready=" << (status.active_model_ready ? "yes" : "no") << "\n";
  std::cout << "gateway_plan_ready=" << (status.gateway_plan_ready ? "yes" : "no") << "\n";
  std::cout << "inference_ready=" << (status.inference_ready ? "yes" : "no") << "\n";
  std::cout << "gateway_ready=" << (status.gateway_ready ? "yes" : "no") << "\n";
  std::cout << "replica_groups_expected=" << status.replica_groups_expected << "\n";
  std::cout << "replica_groups_ready=" << status.replica_groups_ready << "\n";
  std::cout << "replica_groups_degraded=" << status.replica_groups_degraded << "\n";
  std::cout << "api_endpoints_expected=" << status.api_endpoints_expected << "\n";
  std::cout << "api_endpoints_ready=" << status.api_endpoints_ready << "\n";
  std::cout << "launch_ready=" << (status.launch_ready ? "yes" : "no") << "\n";
  std::cout << "status_reason="
            << (status.status_reason.empty() ? "(empty)" : status.status_reason) << "\n";
  std::cout << "failure_detail="
            << (status.failure_detail.empty() ? "(empty)" : status.failure_detail) << "\n";
  std::cout << "kv_cache_bytes="
            << (status.kv_cache_bytes.has_value()
                    ? std::to_string(*status.kv_cache_bytes)
                    : std::string("n/a"))
            << "\n";
  if (apply) {
    naim::SaveRuntimeStatusJson(status, paths.runtime_status_path.string());
  }
  return 0;
}

void StopRuntime(const RuntimeConfig& config, bool apply, const std::string& backend) {
  const auto paths = BuildControlPaths(config);
  std::cout << "stop-plan:\n";
  std::cout << "  active_model_path=" << paths.active_model_path.string() << "\n";
  std::cout << "  gateway_plan_path=" << paths.gateway_plan_path.string() << "\n";
  std::cout << "  runtime_status_path=" << paths.runtime_status_path.string() << "\n";
  std::cout << "  clear_active_model=yes\n";
  std::cout << "  clear_gateway_plan=yes\n";
  std::cout << "  clear_runtime_status=no\n";
  if (apply) {
    fs::remove(paths.active_model_path);
    fs::remove(paths.gateway_plan_path);
    naim::SaveRuntimeStatusJson(
        BuildRuntimeStatus(config, backend, "stopped", false, false, 0, ""),
        paths.runtime_status_path.string());
    PrintStatus(config, backend, false);
  }
}

int RunDoctor(const RuntimeConfig& config, const std::string& checks) {
  const std::vector<std::string> selected_checks = SplitCsv(checks);
  const std::set<std::string> selected(selected_checks.begin(), selected_checks.end());
  int rc = 0;
  if (selected.count("config") > 0) {
    std::cout << "[doctor config]\n";
    std::cout << "  plane=" << config.plane_name << " OK\n";
    std::cout << "  control_root=" << config.control_root << " OK\n";
    std::cout << "  models_root=" << config.models_root << " OK\n";
  }
  if (selected.count("topology") > 0) {
    std::cout << "[doctor topology]\n";
    const json observed_worker_group = LoadWorkerGroupStatus(config);
    const auto topology = replica_support::InspectReplicaTopology(config);
    const int expected_workers =
        config.worker_group.value(
            "expected_workers",
            static_cast<int>(config.worker_group.value("members", json::array()).size()));
    int ready_worker_members = 0;
    for (const auto& member : observed_worker_group.value("members", json::array())) {
      if (member.is_object() && member.value("ready", false)) {
        ++ready_worker_members;
      }
    }
    int enabled_serving_workers = 0;
    int colocated_serving_workers = 0;
    for (const auto& serving_worker : config.serving_workers) {
      if (!serving_worker.value("enabled", true)) {
        continue;
      }
      ++enabled_serving_workers;
      const std::string node_name = serving_worker.value("node_name", std::string{});
      const bool colocated_with_primary_infer =
          serving_worker.value("colocated_with_primary_infer", node_name == config.primary_infer_node);
      if (colocated_with_primary_infer) {
        ++colocated_serving_workers;
      }
    }
    std::cout << "  enabled serving workers: "
              << (enabled_serving_workers > 0 ? "OK" : "FAIL")
              << " (" << enabled_serving_workers << ")\n";
    if (enabled_serving_workers == 0) {
      rc = 1;
    }
    const bool primary_configured = !config.primary_infer_node.empty();
    std::cout << "  primary infer node configured: "
              << (primary_configured ? "OK" : "FAIL")
              << " (" << config.primary_infer_node << ")\n";
    if (!primary_configured) {
      rc = 1;
    }
    std::cout << "  data parallel mode: " << topology.data_parallel_mode << "\n";
    std::cout << "  data parallel lb mode: " << topology.data_parallel_lb_mode << "\n";
    std::cout << "  data parallel size: " << topology.data_parallel_size << "\n";
    std::cout << "  max local dp size: " << topology.data_parallel_size_local_max << "\n";
    std::cout << "  workers per replica: " << topology.workers_per_replica << "\n";
    std::cout << "  replica groups expected: " << topology.replica_groups_expected << "\n";
    std::cout << "  replica groups ready: " << topology.replica_groups_ready << "\n";
    std::cout << "  replica groups degraded: " << topology.replica_groups_degraded << "\n";
    std::cout << "  api endpoints expected: " << topology.api_endpoints_expected << "\n";
    std::cout << "  api endpoints ready: " << topology.api_endpoints_ready << "\n";
    std::cout << "  leader endpoints: "
              << (topology.ready_replica_base_urls.empty()
                      ? std::string("(empty)")
                      : Join(topology.ready_replica_base_urls, ","))
              << "\n";
    std::cout << "  worker group bootstrap: "
              << (topology.replica_groups_expected == 0 ||
                          topology.replica_groups_ready >= topology.replica_groups_expected
                      ? "OK"
                      : "FAIL")
              << " (" << ready_worker_members << "/" << expected_workers
              << " ready workers; " << topology.replica_groups_ready << "/"
              << topology.replica_groups_expected << " ready replicas)\n";
    if (topology.replica_groups_expected > 0 &&
        topology.replica_groups_ready < topology.replica_groups_expected) {
      rc = 1;
    }
    std::cout << "  colocated serving workers: INFO (" << colocated_serving_workers << ")\n";
  }
  if (selected.count("filesystem") > 0) {
    std::cout << "[doctor filesystem]\n";
    for (const auto& path : control_support::RuntimeDirs(config)) {
      const fs::path parent = fs::exists(path) ? path : path.parent_path();
      const bool writable =
          !parent.empty() && fs::exists(parent) && access(parent.c_str(), W_OK) == 0;
      const std::string state = fs::exists(path) ? "OK" : writable ? "PENDING" : "FAIL";
      std::cout << "  " << path.string() << ": " << state << "\n";
      if (state == "FAIL") {
        rc = 1;
      }
    }
  }
  if (selected.count("tools") > 0) {
    std::cout << "[doctor tools]\n";
    std::cout << "  llama-library: OK\n";
    for (const auto& command : {"bash", "docker", "nvidia-smi"}) {
      const bool exists = CommandExists(command);
      const std::string status =
          exists ? "OK"
                 : (std::string(command) == "docker" || std::string(command) == "nvidia-smi")
                       ? "WARN"
                       : "FAIL";
      std::cout << "  " << command << ": " << status << "\n";
      if (status == "FAIL") {
        rc = 1;
      }
    }
    if (const auto turboquant_error = ValidateTurboQuantSupport(config);
        turboquant_error.has_value()) {
      std::cout << "  turboquant: FAIL (" << *turboquant_error << ")\n";
      SaveDoctorFailureStatus(
          config,
          "llama-rpc-head",
          "turboquant_unsupported",
          *turboquant_error);
      rc = 1;
    } else {
      const json active_model = LoadActiveModel(config);
      if (active_model.value("turboquant_enabled", false)) {
        std::cout << "  turboquant: OK ("
                  << active_model.value("active_cache_type_k", std::string{"(empty)"}) << "/"
                  << active_model.value("active_cache_type_v", std::string{"(empty)"}) << ")\n";
      }
    }
  }
  if (selected.count("gateway") > 0) {
    std::cout << "[doctor gateway]\n";
    const bool gateway_ok = !config.gateway_listen_host.empty() && config.gateway_listen_port > 0;
    std::cout << "  listen=" << config.gateway_listen_host << ":" << config.gateway_listen_port
              << " server_name=" << config.gateway_server_name << " "
              << (gateway_ok ? "OK" : "FAIL") << "\n";
    if (!gateway_ok) {
      rc = 1;
    }
  }
  return rc;
}

}  // namespace naim::infer::status_support
