#include "comet/runtime_status.h"

#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <signal.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <cctype>
#include <csignal>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <functional>
#include <iomanip>
#include <iostream>
#include <map>
#include <mutex>
#include <optional>
#include <set>
#include <sstream>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include <nlohmann/json.hpp>

#include <llama-cpp.h>

namespace fs = std::filesystem;
using nlohmann::json;

namespace {

struct Args {
  std::string command;
  std::string config_path;
  std::string profiles_path;
  std::string profile = "generic";
  std::string checks = "config,topology,filesystem,tools,gateway";
  std::string alias;
  std::string source_model_id;
  std::string local_model_path;
  std::string runtime_model_path;
  std::string model_id;
  std::string served_model_name;
  std::string backend = "auto";
  int tp = 1;
  int pp = 1;
  double gpu_memory_utilization = 0.9;
  bool apply = false;
  std::string probe_url;
};

struct RuntimeConfig {
  json raw;
  std::string plane_name;
  std::string control_root;
  std::string controller_url;
  std::string primary_infer_node;
  std::string net_if;
  std::string models_root;
  std::string gguf_cache_dir;
  std::string infer_log_dir;
  int llama_port = 8000;
  int llama_ctx_size = 8192;
  int llama_threads = 8;
  int llama_gpu_layers = 99;
  std::string gateway_listen_host;
  int gateway_listen_port = 80;
  std::string gateway_server_name;
  json gpu_nodes = json::array();
};

struct RuntimeProfile {
  std::string name;
  std::vector<std::string> llama_args;
  std::optional<int> ctx_size;
  std::optional<int> batch_size;
  std::optional<int> gpu_layers;
};

struct ControlPaths {
  fs::path control_root;
  fs::path runtime_assets_path;
  fs::path registry_path;
  fs::path active_model_path;
  fs::path gateway_plan_path;
  fs::path runtime_status_path;
};

struct SimpleResponse {
  int status_code = 200;
  std::string content_type = "application/json";
  std::string body;
};

struct HttpRequest {
  std::string method = "GET";
  std::string path = "/";
  std::map<std::string, std::string> headers;
  std::string body;
};

std::atomic<bool> g_stop_requested{false};

[[noreturn]] void Throw(const std::string& message) {
  throw std::runtime_error(message);
}

std::string ExpandUserPath(const std::string& value) {
  if (value.empty() || value[0] != '~') {
    return value;
  }
  const char* home = std::getenv("HOME");
  if (home == nullptr || std::strlen(home) == 0) {
    return value;
  }
  if (value == "~") {
    return home;
  }
  if (value.size() > 1 && value[1] == '/') {
    return std::string(home) + value.substr(1);
  }
  return value;
}

json LoadJsonFile(const fs::path& path) {
  std::ifstream input(path);
  if (!input.is_open()) {
    Throw("failed to open json file: " + path.string());
  }
  json value;
  input >> value;
  return value;
}

json LoadJsonOrDefault(const fs::path& path, json fallback) {
  if (!fs::exists(path)) {
    return fallback;
  }
  return LoadJsonFile(path);
}

void SaveJsonFile(const fs::path& path, const json& value) {
  if (path.has_parent_path()) {
    fs::create_directories(path.parent_path());
  }
  std::ofstream output(path, std::ios::binary | std::ios::trunc);
  if (!output.is_open()) {
    Throw("failed to open json file for write: " + path.string());
  }
  output << value.dump(2) << "\n";
}

template <typename T>
T Require(const json& object, const char* key, const char* context) {
  if (!object.contains(key)) {
    Throw(std::string("missing ") + context + "." + key);
  }
  try {
    return object.at(key).get<T>();
  } catch (const std::exception&) {
    Throw(std::string("invalid ") + context + "." + key);
  }
}

std::string DefaultConfigPath() {
  if (const char* path = std::getenv("COMET_INFER_RUNTIME_CONFIG")) {
    if (std::strlen(path) > 0) {
      return path;
    }
  }
  const char* control_root = std::getenv("COMET_CONTROL_ROOT");
  const char* plane_name = std::getenv("COMET_PLANE_NAME");
  const std::string root =
      control_root != nullptr && std::strlen(control_root) > 0
          ? control_root
          : std::string("/comet/shared/control/") +
                ((plane_name != nullptr && std::strlen(plane_name) > 0) ? plane_name : "unknown");
  return root + "/infer-runtime.json";
}

std::string DefaultProfilesPath() {
  if (const char* path = std::getenv("COMET_INFER_PROFILES_PATH")) {
    if (std::strlen(path) > 0) {
      return path;
    }
  }
  if (fs::exists("/runtime/infer/runtime-profiles.json")) {
    return "/runtime/infer/runtime-profiles.json";
  }
  return "runtime/infer/runtime-profiles.json";
}

void PrintUsage() {
  std::cout
      << "Usage:\n"
      << "  comet-inferctl list-profiles [--profiles <path>]\n"
      << "  comet-inferctl show-config [--config <path>]\n"
      << "  comet-inferctl show-active-model [--config <path>]\n"
      << "  comet-inferctl validate-config [--config <path>]\n"
      << "  comet-inferctl prepare-runtime [--config <path>] [--apply]\n"
      << "  comet-inferctl bootstrap-runtime [--config <path>] [--profile <name>] [--profiles <path>] [--apply]\n"
      << "  comet-inferctl runtime-assets-status [--config <path>]\n"
      << "  comet-inferctl preload-model [--config <path>] --alias <name> --source-model-id <id> --local-model-path <path> [--runtime-model-path <path>] [--apply]\n"
      << "  comet-inferctl cache-status [--config <path>] --alias <name> --local-model-path <path>\n"
      << "  comet-inferctl switch-model [--config <path>] --model-id <id> [--served-model-name <name>] [--tp <n>] [--pp <n>] [--gpu-memory-utilization <0-1>] [--runtime-profile <name>] [--apply]\n"
      << "  comet-inferctl gateway-plan [--config <path>] [--apply]\n"
      << "  comet-inferctl gateway-status [--config <path>]\n"
      << "  comet-inferctl status [--config <path>] [--apply]\n"
      << "  comet-inferctl stop [--config <path>] [--apply]\n"
      << "  comet-inferctl plan-launch [--config <path>]\n"
      << "  comet-inferctl doctor [--config <path>] [--checks <csv>]\n"
      << "  comet-inferctl bootstrap-dry-run [--config <path>] [--profile <name>] [--profiles <path>] [--apply]\n"
      << "  comet-inferctl launch-embedded-runtime [--config <path>]\n"
      << "  comet-inferctl launch-runtime [--config <path>] [--backend <auto|embedded|llama>]\n"
      << "  comet-inferctl probe-url <url>\n";
}

Args ParseArgs(int argc, char** argv) {
  if (argc < 2) {
    PrintUsage();
    std::exit(2);
  }
  Args args;
  args.command = argv[1];
  args.config_path = DefaultConfigPath();
  args.profiles_path = DefaultProfilesPath();
  if (const char* backend = std::getenv("COMET_INFER_RUNTIME_BACKEND")) {
    if (std::strlen(backend) > 0) {
      args.backend = backend;
    }
  }

  int index = 2;
  if (args.command == "probe-url") {
    if (argc < 3) {
      Throw("probe-url requires a url");
    }
    args.probe_url = argv[2];
    return args;
  }

  while (index < argc) {
    const std::string_view option = argv[index];
    auto need_value = [&](const char* name) -> std::string {
      if (index + 1 >= argc) {
        Throw(std::string(name) + " requires a value");
      }
      ++index;
      return argv[index];
    };

    if (option == "--config") {
      args.config_path = need_value("--config");
    } else if (option == "--profiles") {
      args.profiles_path = need_value("--profiles");
    } else if (option == "--profile" || option == "--runtime-profile") {
      args.profile = need_value("--profile");
    } else if (option == "--checks") {
      args.checks = need_value("--checks");
    } else if (option == "--alias") {
      args.alias = need_value("--alias");
    } else if (option == "--source-model-id") {
      args.source_model_id = need_value("--source-model-id");
    } else if (option == "--local-model-path") {
      args.local_model_path = need_value("--local-model-path");
    } else if (option == "--runtime-model-path") {
      args.runtime_model_path = need_value("--runtime-model-path");
    } else if (option == "--model-id") {
      args.model_id = need_value("--model-id");
    } else if (option == "--served-model-name") {
      args.served_model_name = need_value("--served-model-name");
    } else if (option == "--tp") {
      args.tp = std::stoi(need_value("--tp"));
    } else if (option == "--pp") {
      args.pp = std::stoi(need_value("--pp"));
    } else if (option == "--gpu-memory-utilization") {
      args.gpu_memory_utilization = std::stod(need_value("--gpu-memory-utilization"));
    } else if (option == "--backend") {
      args.backend = need_value("--backend");
    } else if (option == "--apply") {
      args.apply = true;
    } else if (option == "-h" || option == "--help") {
      PrintUsage();
      std::exit(0);
    } else {
      Throw("unknown argument: " + std::string(option));
    }
    ++index;
  }

  return args;
}

RuntimeConfig LoadRuntimeConfig(const std::string& path_str) {
  const fs::path path(path_str);
  if (!fs::exists(path)) {
    Throw("config not found: " + path.string());
  }
  const json root = LoadJsonFile(path);
  const json plane = Require<json>(root, "plane", "root");
  const json control = Require<json>(root, "control", "root");
  const json inference = Require<json>(root, "inference", "root");
  const json gateway = Require<json>(root, "gateway", "root");

  RuntimeConfig config;
  config.raw = root;
  config.plane_name = Require<std::string>(plane, "name", "plane");
  config.control_root = Require<std::string>(control, "root", "control");
  config.controller_url = Require<std::string>(control, "controller_url", "control");
  config.primary_infer_node =
      Require<std::string>(inference, "primary_infer_node", "inference");
  config.net_if = Require<std::string>(inference, "net_if", "inference");
  config.models_root =
      ExpandUserPath(Require<std::string>(inference, "models_root", "inference"));
  config.gguf_cache_dir =
      ExpandUserPath(Require<std::string>(inference, "gguf_cache_dir", "inference"));
  config.infer_log_dir = ExpandUserPath(Require<std::string>(inference, "infer_log_dir", "inference"));
  config.llama_port = Require<int>(inference, "llama_port", "inference");
  config.llama_ctx_size = inference.value("llama_ctx_size", config.llama_ctx_size);
  config.llama_threads = inference.value("llama_threads", config.llama_threads);
  config.llama_gpu_layers = inference.value("llama_gpu_layers", config.llama_gpu_layers);
  config.gateway_listen_host = Require<std::string>(gateway, "listen_host", "gateway");
  config.gateway_listen_port = Require<int>(gateway, "listen_port", "gateway");
  config.gateway_server_name = Require<std::string>(gateway, "server_name", "gateway");
  config.gpu_nodes = Require<json>(root, "gpu_nodes", "root");
  return config;
}

json LoadProfiles(const std::string& path_str) {
  const fs::path path(path_str);
  if (!fs::exists(path)) {
    Throw("runtime profiles not found: " + path.string());
  }
  return LoadJsonFile(path);
}

RuntimeProfile ResolveProfile(const json& profiles_json, const std::string& name) {
  const json profiles = Require<json>(profiles_json, "profiles", "root");
  if (!profiles.contains(name) || !profiles.at(name).is_object()) {
    Throw("unknown runtime profile: " + name);
  }
  const json profile_json = profiles.at(name);
  RuntimeProfile profile;
  profile.name = name;
  profile.llama_args = profile_json.value("llama_args", std::vector<std::string>{});
  if (profile_json.contains("ctx_size")) {
    profile.ctx_size = profile_json.at("ctx_size").get<int>();
  }
  if (profile_json.contains("batch_size")) {
    profile.batch_size = profile_json.at("batch_size").get<int>();
  }
  if (profile_json.contains("gpu_layers")) {
    profile.gpu_layers = profile_json.at("gpu_layers").get<int>();
  }
  return profile;
}

ControlPaths BuildControlPaths(const RuntimeConfig& config) {
  const fs::path root(config.control_root);
  return ControlPaths{
      root,
      root / "runtime-assets.json",
      root / "model-cache-registry.json",
      root / "active-model.json",
      root / "gateway-plan.json",
      root / "runtime-status.json",
  };
}

std::vector<fs::path> RuntimeDirs(const RuntimeConfig& config) {
  return {
      fs::path(config.control_root),
      fs::path(config.models_root),
      fs::path(config.gguf_cache_dir),
      fs::path(config.infer_log_dir),
  };
}

json LoadActiveModel(const RuntimeConfig& config) {
  return LoadJsonOrDefault(BuildControlPaths(config).active_model_path, json::object());
}

json LoadGatewayPlan(const RuntimeConfig& config) {
  return LoadJsonOrDefault(BuildControlPaths(config).gateway_plan_path, json::object());
}

json LoadRegistry(const RuntimeConfig& config) {
  return LoadJsonOrDefault(
      BuildControlPaths(config).registry_path,
      json{{"version", 1}, {"entries", json::array()}});
}

json LoadRuntimeAssetsStatus(const RuntimeConfig& config) {
  return LoadJsonOrDefault(BuildControlPaths(config).runtime_assets_path, json::object());
}

int EnabledGpuNodeCount(const RuntimeConfig& config) {
  int enabled = 0;
  for (const auto& gpu_node : config.gpu_nodes) {
    if (!gpu_node.is_object()) {
      continue;
    }
    if (gpu_node.value("enabled", true)) {
      ++enabled;
    }
  }
  return enabled;
}

std::string Join(const std::vector<std::string>& values, const std::string& delimiter) {
  std::ostringstream out;
  for (std::size_t index = 0; index < values.size(); ++index) {
    if (index > 0) {
      out << delimiter;
    }
    out << values[index];
  }
  return out.str();
}

std::vector<std::string> SplitCsv(const std::string& value) {
  std::vector<std::string> result;
  std::stringstream stream(value);
  std::string item;
  while (std::getline(stream, item, ',')) {
    if (!item.empty()) {
      result.push_back(item);
    }
  }
  return result;
}

bool CommandExists(const std::string& command) {
  if (command == "docker") {
    const std::string windows_docker =
        "/mnt/c/Program Files/Docker/Docker/resources/bin/docker.exe";
    if (fs::exists(windows_docker) &&
        std::system(("'" + windows_docker + "' version >/dev/null 2>&1").c_str()) == 0) {
      return true;
    }
  }
  const char* path = std::getenv("PATH");
  if (path == nullptr) {
    return false;
  }
  std::stringstream stream(path);
  std::string directory;
  while (std::getline(stream, directory, ':')) {
    const fs::path candidate = fs::path(directory) / command;
    if (fs::exists(candidate) && access(candidate.c_str(), X_OK) == 0) {
      return true;
    }
  }
  return false;
}

json BuildGatewayPayload(const RuntimeConfig& config) {
  const json active_model = LoadActiveModel(config);
  return json{
      {"version", 1},
      {"plane_name", config.plane_name},
      {"listen_host", config.gateway_listen_host},
      {"listen_port", config.gateway_listen_port},
      {"server_name", config.gateway_server_name},
      {"proxy_health_url", "http://127.0.0.1:8001/health"},
      {"upstream_health_url", "http://127.0.0.1:" + std::to_string(config.llama_port) + "/health"},
      {"upstream_models_url",
       "http://127.0.0.1:" + std::to_string(config.llama_port) + "/v1/models"},
      {"active_served_model_name", active_model.value("served_model_name", std::string{})},
      {"active_model_id", active_model.value("model_id", std::string{})},
  };
}

comet::RuntimeStatus BuildRuntimeStatus(
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
  comet::RuntimeStatus status;
  status.plane_name = config.plane_name;
  status.control_root = config.control_root;
  status.controller_url = config.controller_url;
  status.primary_infer_node = config.primary_infer_node;
  if (const char* instance_name = std::getenv("COMET_INSTANCE_NAME")) {
    status.instance_name = instance_name;
  }
  if (const char* instance_role = std::getenv("COMET_INSTANCE_ROLE")) {
    status.instance_role = instance_role;
  }
  if (const char* node_name = std::getenv("COMET_NODE_NAME")) {
    status.node_name = node_name;
  }
  status.runtime_backend = backend;
  status.runtime_phase = phase;
  status.enabled_gpu_nodes = EnabledGpuNodeCount(config);
  status.registry_entries = static_cast<int>(registry.value("entries", json::array()).size());
  status.supervisor_pid = supervisor_pid;
  status.runtime_pid = supervisor_pid;
  status.engine_pid = supervisor_pid;
  status.active_model_id = active_model.value("model_id", std::string{});
  status.active_served_model_name =
      active_model.value("served_model_name", std::string{});
  status.active_runtime_profile =
      active_model.value("runtime_profile", std::string{});
  status.cached_local_model_path =
      active_model.value(
          "cached_runtime_model_path",
          active_model.value("cached_local_model_path", std::string{}));
  status.model_path = status.cached_local_model_path;
  status.gateway_listen =
      config.gateway_listen_host + ":" + std::to_string(config.gateway_listen_port);
  status.upstream_models_url =
      "http://127.0.0.1:" + std::to_string(config.llama_port) + "/v1/models";
  status.inference_health_url =
      "http://127.0.0.1:" + std::to_string(config.llama_port) + "/health";
  status.gateway_health_url =
      "http://127.0.0.1:" + std::to_string(config.gateway_listen_port) + "/health";
  status.started_at = started_at;
  status.last_activity_at = started_at;
  status.ready = inference_ready && gateway_ready;
  status.active_model_ready = !active_model.empty();
  status.gateway_plan_ready = !gateway_plan.empty();
  status.inference_ready = inference_ready;
  status.gateway_ready = gateway_ready;
  status.launch_ready = status.active_model_ready && status.inference_ready && status.gateway_ready;
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

void WriteRuntimeStatus(
    const RuntimeConfig& config,
    const std::string& backend,
    const std::string& phase,
    bool inference_ready,
    bool gateway_ready,
    int supervisor_pid,
    const std::string& started_at) {
  comet::SaveRuntimeStatusJson(
      BuildRuntimeStatus(
          config,
          backend,
          phase,
          inference_ready,
          gateway_ready,
          supervisor_pid,
          started_at),
      BuildControlPaths(config).runtime_status_path.string());
}

bool IsLiveRuntimePhase(const std::string& phase) {
  return phase == "starting" || phase == "running" || phase == "stopping";
}

comet::RuntimeStatus MergeWithObservedRuntimeStatus(
    comet::RuntimeStatus status,
    const std::string& path) {
  const std::optional<comet::RuntimeStatus> observed = comet::LoadRuntimeStatusJson(path);
  if (!observed.has_value()) {
    return status;
  }

  if (!observed->runtime_backend.empty()) {
    status.runtime_backend = observed->runtime_backend;
  }
  if (!observed->runtime_phase.empty()) {
    status.runtime_phase = observed->runtime_phase;
  }
  status.supervisor_pid = observed->supervisor_pid;
  if (!observed->started_at.empty()) {
    status.started_at = observed->started_at;
  }

  if (IsLiveRuntimePhase(observed->runtime_phase) || observed->runtime_phase == "stopped") {
    status.inference_ready = observed->inference_ready;
    status.gateway_ready = observed->gateway_ready;
    status.launch_ready = status.active_model_ready && status.inference_ready && status.gateway_ready;
  }

  return status;
}

std::string SafeServedModelName(const std::string& model_id) {
  const std::size_t slash = model_id.find_last_of('/');
  return slash == std::string::npos ? model_id : model_id.substr(slash + 1);
}

json BuildModelListPayload(const RuntimeConfig& config) {
  const json active_model = LoadActiveModel(config);
  const std::optional<comet::RuntimeStatus> runtime_status =
      comet::LoadRuntimeStatusJson(BuildControlPaths(config).runtime_status_path.string());
  const std::string owner =
      runtime_status.has_value() && runtime_status->runtime_backend == "llama"
          ? "comet-llama-runtime"
          : "comet-embedded-runtime";
  json data = json::array();
  const std::string model_id = active_model.value("model_id", std::string{});
  if (!model_id.empty()) {
    data.push_back(json{
        {"id", active_model.value("served_model_name", SafeServedModelName(model_id))},
        {"object", "model"},
        {"created", 0},
        {"owned_by", owner},
        {"root", model_id},
    });
  }
  return json{{"object", "list"}, {"data", data}};
}

void TouchReadyFile() {
  std::ofstream("/tmp/comet-ready") << "ready\n";
}

std::string UtcNowIso() {
  const auto now = std::chrono::system_clock::now();
  const std::time_t time = std::chrono::system_clock::to_time_t(now);
  std::tm tm{};
  gmtime_r(&time, &tm);
  std::ostringstream out;
  out << std::put_time(&tm, "%Y-%m-%dT%H:%M:%SZ");
  return out.str();
}

int CreateListenSocket(const std::string& host, int port) {
  const int fd = socket(AF_INET, SOCK_STREAM, 0);
  if (fd < 0) {
    Throw("socket failed");
  }
  int opt = 1;
  setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_port = htons(static_cast<uint16_t>(port));
  if (host.empty() || host == "0.0.0.0") {
    addr.sin_addr.s_addr = INADDR_ANY;
  } else {
    if (inet_pton(AF_INET, host.c_str(), &addr.sin_addr) != 1) {
      close(fd);
      Throw("invalid listen host: " + host);
    }
  }
  if (bind(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0) {
    const std::string message = "bind failed on " + host + ":" + std::to_string(port) +
                                " errno=" + std::to_string(errno);
    close(fd);
    Throw(message);
  }
  if (listen(fd, 128) != 0) {
    close(fd);
    Throw("listen failed");
  }
  return fd;
}

SimpleResponse BuildHealthResponse(const std::string& service_name) {
  return SimpleResponse{200, "application/json", json{{"status", "ok"}, {"service", service_name}}.dump()};
}

SimpleResponse BuildJsonResponse(int status_code, json body) {
  return SimpleResponse{status_code, "application/json", body.dump()};
}

std::string Trim(const std::string& value) {
  std::size_t start = 0;
  while (start < value.size() && std::isspace(static_cast<unsigned char>(value[start])) != 0) {
    ++start;
  }
  std::size_t end = value.size();
  while (end > start && std::isspace(static_cast<unsigned char>(value[end - 1])) != 0) {
    --end;
  }
  return value.substr(start, end - start);
}

std::string Lowercase(std::string value) {
  std::transform(value.begin(), value.end(), value.begin(), [](unsigned char ch) {
    return static_cast<char>(std::tolower(ch));
  });
  return value;
}

std::optional<std::string> ResolveGgufPath(const json& active_model) {
  const std::string cached_path =
      active_model.value(
          "cached_runtime_model_path",
          active_model.value("cached_local_model_path", std::string{}));
  if (cached_path.empty()) {
    return std::nullopt;
  }
  const fs::path path(ExpandUserPath(cached_path));
  if (fs::is_regular_file(path) && path.extension() == ".gguf") {
    return path.string();
  }
  if (!fs::exists(path) || !fs::is_directory(path)) {
    return std::nullopt;
  }
  std::vector<fs::path> candidates;
  for (const auto& entry : fs::recursive_directory_iterator(path)) {
    if (entry.is_regular_file() && entry.path().extension() == ".gguf") {
      candidates.push_back(entry.path());
    }
  }
  if (candidates.empty()) {
    return std::nullopt;
  }
  std::sort(candidates.begin(), candidates.end());
  return candidates.front().string();
}

std::string JsonString(const json& object, const char* key) {
  if (!object.contains(key) || object.at(key).is_null()) {
    return {};
  }
  if (object.at(key).is_string()) {
    return object.at(key).get<std::string>();
  }
  return object.at(key).dump();
}

class LlamaLibraryEngine {
 public:
  struct GenerationResult {
    std::string text;
    int prompt_tokens = 0;
    int completion_tokens = 0;
    std::string finish_reason = "stop";
  };

  LlamaLibraryEngine(const RuntimeConfig& config, const json& active_model)
      : config_(config), active_model_(active_model) {
    gguf_path_ = ResolveGgufPath(active_model_);
    if (!gguf_path_.has_value()) {
      Throw("active model does not resolve to a local GGUF file");
    }
    EnsureBackendsLoaded();
    llama_model_params model_params = llama_model_default_params();
    model_params.n_gpu_layers = config_.llama_gpu_layers;
    model_.reset(llama_model_load_from_file(gguf_path_->c_str(), model_params));
    if (!model_) {
      Throw("failed to load llama model from " + *gguf_path_);
    }
    vocab_ = llama_model_get_vocab(model_.get());
    if (vocab_ == nullptr) {
      Throw("llama model loaded without vocab");
    }
  }

  std::string ModelPath() const {
    return *gguf_path_;
  }

  GenerationResult GenerateText(const std::string& prompt, int max_tokens) {
    std::lock_guard<std::mutex> guard(mutex_);
    const int bounded_max_tokens = std::max(1, std::min(max_tokens, 256));
    std::vector<llama_token> prompt_tokens = Tokenize(prompt, true);
    if (prompt_tokens.empty()) {
      Throw("prompt tokenization produced zero tokens");
    }

    llama_context_params ctx_params = llama_context_default_params();
    ctx_params.n_ctx = static_cast<uint32_t>(
        std::max(config_.llama_ctx_size, static_cast<int>(prompt_tokens.size()) + bounded_max_tokens + 16));
    ctx_params.n_batch = static_cast<uint32_t>(std::min<std::size_t>(prompt_tokens.size(), 512));
    ctx_params.n_threads = static_cast<uint32_t>(std::max(1, config_.llama_threads));
    ctx_params.n_threads_batch = static_cast<uint32_t>(std::max(1, config_.llama_threads));
    ctx_params.no_perf = true;

    llama_context_ptr ctx(llama_init_from_model(model_.get(), ctx_params));
    if (!ctx) {
      Throw("failed to create llama context");
    }

    llama_sampler_chain_params sampler_params = llama_sampler_chain_default_params();
    sampler_params.no_perf = true;
    llama_sampler_ptr sampler(llama_sampler_chain_init(sampler_params));
    if (!sampler) {
      Throw("failed to create llama sampler");
    }
    llama_sampler_chain_add(sampler.get(), llama_sampler_init_greedy());

    llama_batch batch =
        llama_batch_get_one(prompt_tokens.data(), static_cast<int32_t>(prompt_tokens.size()));
    if (llama_model_has_encoder(model_.get())) {
      if (llama_encode(ctx.get(), batch) != 0) {
        Throw("llama_encode failed");
      }
      llama_token decoder_start_token_id = llama_model_decoder_start_token(model_.get());
      if (decoder_start_token_id == LLAMA_TOKEN_NULL) {
        decoder_start_token_id = llama_vocab_bos(vocab_);
      }
      batch = llama_batch_get_one(&decoder_start_token_id, 1);
    }

    std::string output;
    int n_pos = 0;
    int completion_tokens = 0;
    for (; n_pos + batch.n_tokens < static_cast<int>(prompt_tokens.size()) + bounded_max_tokens;) {
      if (llama_decode(ctx.get(), batch) != 0) {
        Throw("llama_decode failed");
      }
      n_pos += batch.n_tokens;
      llama_token next_token = llama_sampler_sample(sampler.get(), ctx.get(), -1);
      if (llama_vocab_is_eog(vocab_, next_token)) {
        break;
      }
      output += TokenToPiece(next_token);
      ++completion_tokens;
      batch = llama_batch_get_one(&next_token, 1);
    }

    return GenerationResult{
        output,
        static_cast<int>(prompt_tokens.size()),
        completion_tokens,
        "stop",
    };
  }

 private:
  static void EnsureBackendsLoaded() {
    static std::once_flag once;
    std::call_once(once, []() { ggml_backend_load_all(); });
  }

  std::vector<llama_token> Tokenize(const std::string& text, bool add_bos) const {
    const int needed = -llama_tokenize(
        vocab_,
        text.c_str(),
        static_cast<int32_t>(text.size()),
        nullptr,
        0,
        add_bos,
        true);
    if (needed <= 0) {
      Throw("failed to size llama tokenization buffer");
    }
    std::vector<llama_token> tokens(static_cast<std::size_t>(needed));
    if (llama_tokenize(
            vocab_,
            text.c_str(),
            static_cast<int32_t>(text.size()),
            tokens.data(),
            static_cast<int32_t>(tokens.size()),
            add_bos,
            true) < 0) {
      Throw("llama_tokenize failed");
    }
    return tokens;
  }

  std::string TokenToPiece(llama_token token) const {
    std::array<char, 256> buffer{};
    int n = llama_token_to_piece(vocab_, token, buffer.data(), buffer.size(), 0, true);
    if (n < 0) {
      std::string dynamic(static_cast<std::size_t>(-n), '\0');
      n = llama_token_to_piece(vocab_, token, dynamic.data(), dynamic.size(), 0, true);
      if (n < 0) {
        Throw("llama_token_to_piece failed");
      }
      dynamic.resize(static_cast<std::size_t>(n));
      return dynamic;
    }
    return std::string(buffer.data(), static_cast<std::size_t>(n));
  }

  RuntimeConfig config_;
  json active_model_;
  std::optional<std::string> gguf_path_;
  llama_model_ptr model_;
  const llama_vocab* vocab_ = nullptr;
  std::mutex mutex_;
};

std::string CompletionPromptFromRequest(const HttpRequest& request) {
  if (request.body.empty()) {
    Throw("request body is empty");
  }
  const json payload = json::parse(request.body);
  if (request.path == "/v1/completions") {
    const std::string prompt = JsonString(payload, "prompt");
    if (prompt.empty()) {
      Throw("completion request is missing prompt");
    }
    return prompt;
  }
  if (request.path == "/v1/chat/completions") {
    if (!payload.contains("messages") || !payload.at("messages").is_array()) {
      Throw("chat completion request is missing messages");
    }
    std::ostringstream prompt;
    for (const auto& message : payload.at("messages")) {
      if (!message.is_object()) {
        continue;
      }
      const std::string role = message.value("role", std::string{"user"});
      const std::string content = JsonString(message, "content");
      if (!content.empty()) {
        prompt << role << ": " << content << "\n";
      }
    }
    prompt << "assistant: ";
    const std::string result = prompt.str();
    if (result.empty()) {
      Throw("chat completion request contains no usable messages");
    }
    return result;
  }
  Throw("unsupported inference path: " + request.path);
}

int MaxTokensFromRequest(const HttpRequest& request) {
  if (request.body.empty()) {
    return 64;
  }
  const json payload = json::parse(request.body);
  return payload.value("max_tokens", payload.value("max_completion_tokens", 64));
}

SimpleResponse BuildCompletionResponse(
    const RuntimeConfig& config,
    const HttpRequest& request,
    const LlamaLibraryEngine::GenerationResult& result) {
  const json active_model = LoadActiveModel(config);
  const std::string served_model_name =
      active_model.value("served_model_name", active_model.value("model_id", std::string{"(unknown)"}));
  if (request.path == "/v1/chat/completions") {
    return BuildJsonResponse(
        200,
        json{
            {"id", "chatcmpl-comet"},
            {"object", "chat.completion"},
            {"model", served_model_name},
            {"choices",
             json::array({json{
                 {"index", 0},
                 {"message", json{{"role", "assistant"}, {"content", result.text}}},
                 {"finish_reason", result.finish_reason},
             }})},
            {"usage",
             json{{"prompt_tokens", result.prompt_tokens},
                  {"completion_tokens", result.completion_tokens},
                  {"total_tokens", result.prompt_tokens + result.completion_tokens}}},
        });
  }
  return BuildJsonResponse(
      200,
      json{
          {"id", "cmpl-comet"},
          {"object", "text_completion"},
          {"model", served_model_name},
          {"choices",
           json::array({json{
               {"index", 0},
               {"text", result.text},
               {"finish_reason", result.finish_reason},
           }})},
          {"usage",
           json{{"prompt_tokens", result.prompt_tokens},
                {"completion_tokens", result.completion_tokens},
                {"total_tokens", result.prompt_tokens + result.completion_tokens}}},
      });
}

SimpleResponse HandleLocalRequest(
    const RuntimeConfig& config,
    const HttpRequest& request,
    const std::string& service_name,
    LlamaLibraryEngine* engine) {
  if (request.path == "/health") {
    return BuildHealthResponse(service_name);
  }
  if (request.path == "/v1/models") {
    return SimpleResponse{200, "application/json", BuildModelListPayload(config).dump()};
  }
  if ((request.path == "/v1/completions" || request.path == "/v1/chat/completions") &&
      request.method == "POST") {
    if (engine == nullptr) {
      return BuildJsonResponse(503, json{{"status", "unavailable"}, {"reason", "llama engine is not loaded"}});
    }
    try {
      const std::string prompt = CompletionPromptFromRequest(request);
      const int max_tokens = MaxTokensFromRequest(request);
      return BuildCompletionResponse(config, request, engine->GenerateText(prompt, max_tokens));
    } catch (const std::exception& error) {
      return BuildJsonResponse(400, json{{"status", "bad_request"}, {"error", error.what()}});
    }
  }
  return BuildJsonResponse(404, json{{"status", "not_found"}, {"path", request.path}});
}

void SendResponse(int client_fd, const SimpleResponse& response) {
  std::ostringstream out;
  out << "HTTP/1.1 " << response.status_code
      << (response.status_code == 200 ? " OK" : response.status_code == 404 ? " Not Found" : " Error")
      << "\r\n";
  out << "Content-Type: " << response.content_type << "\r\n";
  out << "Content-Length: " << response.body.size() << "\r\n";
  out << "Connection: close\r\n\r\n";
  out << response.body;
  const std::string payload = out.str();
  const char* data = payload.c_str();
  std::size_t remaining = payload.size();
  while (remaining > 0) {
    const ssize_t written = send(client_fd, data, remaining, 0);
    if (written <= 0) {
      break;
    }
    data += written;
    remaining -= static_cast<std::size_t>(written);
  }
}

HttpRequest ParseHttpRequest(const std::string& request_text) {
  HttpRequest request;
  const std::size_t headers_end = request_text.find("\r\n\r\n");
  const std::string header_text =
      headers_end == std::string::npos ? request_text : request_text.substr(0, headers_end);
  request.body = headers_end == std::string::npos ? std::string{} : request_text.substr(headers_end + 4);
  const std::size_t line_end = header_text.find("\r\n");
  const std::string first_line =
      line_end == std::string::npos ? header_text : header_text.substr(0, line_end);
  std::stringstream stream(first_line);
  stream >> request.method >> request.path;
  if (request.path.empty()) {
    request.path = "/";
  }

  std::size_t offset = line_end == std::string::npos ? header_text.size() : line_end + 2;
  while (offset < header_text.size()) {
    const std::size_t next = header_text.find("\r\n", offset);
    const std::string line = header_text.substr(offset, next == std::string::npos ? std::string::npos : next - offset);
    const std::size_t colon = line.find(':');
    if (colon != std::string::npos) {
      request.headers.emplace(
          Lowercase(Trim(line.substr(0, colon))),
          Trim(line.substr(colon + 1)));
    }
    if (next == std::string::npos) {
      break;
    }
    offset = next + 2;
  }
  return request;
}

class LocalHttpServer {
 public:
  LocalHttpServer(
      std::string host,
      int port,
      std::string service_name,
      const RuntimeConfig& config,
      LlamaLibraryEngine* engine)
      : host_(std::move(host)),
        port_(port),
        service_name_(std::move(service_name)),
        config_(config),
        engine_(engine) {}

  void Start() {
    listen_fd_ = CreateListenSocket(host_, port_);
    worker_ = std::thread([this]() { AcceptLoop(); });
  }

  void Stop() {
    running_ = false;
    if (listen_fd_ >= 0) {
      shutdown(listen_fd_, SHUT_RDWR);
      close(listen_fd_);
      listen_fd_ = -1;
    }
    if (worker_.joinable()) {
      worker_.join();
    }
  }

 private:
  void AcceptLoop() {
    running_ = true;
    while (running_ && !g_stop_requested.load()) {
      const int client_fd = accept(listen_fd_, nullptr, nullptr);
      if (client_fd < 0) {
        if (!running_) {
          return;
        }
        if (errno == EINTR) {
          continue;
        }
        return;
      }
      std::thread(&LocalHttpServer::HandleClient, this, client_fd).detach();
    }
  }

  void HandleClient(int client_fd) {
    std::string request_data;
    std::array<char, 8192> buffer{};
    std::size_t content_length = 0;
    bool headers_parsed = false;
    while (true) {
      const ssize_t read_count = recv(client_fd, buffer.data(), buffer.size(), 0);
      if (read_count <= 0) {
        break;
      }
      request_data.append(buffer.data(), static_cast<std::size_t>(read_count));
      if (!headers_parsed) {
        const std::size_t headers_end = request_data.find("\r\n\r\n");
        if (headers_end != std::string::npos) {
          headers_parsed = true;
          const HttpRequest partial = ParseHttpRequest(request_data);
          const auto it = partial.headers.find("content-length");
          if (it != partial.headers.end()) {
            content_length = static_cast<std::size_t>(std::max(0, std::stoi(it->second)));
          }
          const std::size_t body_bytes = request_data.size() - (headers_end + 4);
          if (body_bytes >= content_length) {
            break;
          }
        }
      } else {
        const std::size_t headers_end = request_data.find("\r\n\r\n");
        const std::size_t body_bytes = headers_end == std::string::npos ? 0 : request_data.size() - (headers_end + 4);
        if (body_bytes >= content_length) {
          break;
        }
      }
    }
    if (!request_data.empty()) {
      const HttpRequest request = ParseHttpRequest(request_data);
      const SimpleResponse response = HandleLocalRequest(config_, request, service_name_, engine_);
      SendResponse(client_fd, response);
    }
    shutdown(client_fd, SHUT_RDWR);
    close(client_fd);
  }

  std::string host_;
  int port_ = 0;
  std::string service_name_;
  RuntimeConfig config_;
  LlamaLibraryEngine* engine_ = nullptr;
  std::thread worker_;
  std::atomic<bool> running_{false};
  int listen_fd_ = -1;
};

void SignalHandler(int) {
  g_stop_requested.store(true);
}

bool ProbeUrl(const std::string& url) {
  constexpr std::string_view kHttpPrefix = "http://";
  if (url.rfind(std::string(kHttpPrefix), 0) != 0) {
    return false;
  }
  std::string remainder = url.substr(kHttpPrefix.size());
  const std::size_t slash = remainder.find('/');
  const std::string authority = slash == std::string::npos ? remainder : remainder.substr(0, slash);
  const std::string path = slash == std::string::npos ? "/" : remainder.substr(slash);
  const std::size_t colon = authority.find(':');
  const std::string host = colon == std::string::npos ? authority : authority.substr(0, colon);
  const int port = colon == std::string::npos ? 80 : std::stoi(authority.substr(colon + 1));

  const int fd = socket(AF_INET, SOCK_STREAM, 0);
  if (fd < 0) {
    return false;
  }

  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_port = htons(static_cast<uint16_t>(port));
  if (inet_pton(AF_INET, host.c_str(), &addr.sin_addr) != 1) {
    close(fd);
    return false;
  }
  if (connect(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0) {
    close(fd);
    return false;
  }

  const std::string request =
      "GET " + path + " HTTP/1.1\r\nHost: " + host + "\r\nConnection: close\r\n\r\n";
  if (send(fd, request.c_str(), request.size(), 0) < 0) {
    close(fd);
    return false;
  }
  std::array<char, 1024> buffer{};
  const ssize_t read_count = recv(fd, buffer.data(), buffer.size() - 1, 0);
  close(fd);
  if (read_count <= 0) {
    return false;
  }
  const std::string response(buffer.data(), static_cast<std::size_t>(read_count));
  return response.rfind("HTTP/1.1 200", 0) == 0 || response.rfind("HTTP/1.0 200", 0) == 0;
}

void PrintConfigSummary(const RuntimeConfig& config) {
  std::cout << "infer runtime config: OK\n";
  std::cout << "plane_name=" << config.plane_name << "\n";
  std::cout << "control_root=" << config.control_root << "\n";
  std::cout << "controller_url=" << config.controller_url << "\n";
  std::cout << "gpu_node_count=" << config.gpu_nodes.size() << "\n";
  std::cout << "enabled_gpu_node_count=" << EnabledGpuNodeCount(config) << "\n";
  std::cout << "primary_infer_node=" << config.primary_infer_node << "\n";
  std::cout << "models_root=" << config.models_root << "\n";
  std::cout << "llama_port=" << config.llama_port << "\n";
  std::cout << "gateway=" << config.gateway_listen_host << ":" << config.gateway_listen_port << "\n";
}

void PrintPrepareRuntime(const RuntimeConfig& config, bool apply) {
  const char* verb = apply ? "created" : "would-create";
  for (const auto& path : RuntimeDirs(config)) {
    std::cout << verb << "=" << path.string() << "\n";
    if (apply) {
      fs::create_directories(path);
    }
  }
}

void PrintListProfiles(const json& profiles_json) {
  std::cout << "runtime profiles:\n";
  const json profiles = Require<json>(profiles_json, "profiles", "root");
  std::vector<std::string> names;
  for (auto it = profiles.begin(); it != profiles.end(); ++it) {
    names.push_back(it.key());
  }
  std::sort(names.begin(), names.end());
  for (const auto& name : names) {
    const RuntimeProfile profile = ResolveProfile(profiles_json, name);
    std::cout << "  - " << name << "\n";
    std::cout << "    llama_args="
              << (profile.llama_args.empty() ? "(empty)" : Join(profile.llama_args, ","))
              << "\n";
    if (profile.ctx_size.has_value()) {
      std::cout << "    ctx_size=" << *profile.ctx_size << "\n";
    }
    if (profile.batch_size.has_value()) {
      std::cout << "    batch_size=" << *profile.batch_size << "\n";
    }
    if (profile.gpu_layers.has_value()) {
      std::cout << "    gpu_layers=" << *profile.gpu_layers << "\n";
    }
  }
}

void BootstrapRuntime(
    const RuntimeConfig& config,
    const RuntimeProfile& profile,
    bool apply) {
  std::cout << "bootstrap-runtime:\n";
  std::cout << "  profile=" << profile.name << "\n";
  std::cout << "  plane=" << config.plane_name << "\n";
  std::cout << "  control_root=" << config.control_root << "\n";
  std::cout << "  models_root=" << config.models_root << "\n";
  std::cout << "  gguf_cache_dir=" << config.gguf_cache_dir << "\n";
  std::cout << "  runtime_mode=llama-library\n";
  std::cout << "  llama_args="
            << (profile.llama_args.empty() ? "(empty)" : Join(profile.llama_args, ","))
            << "\n";
  if (profile.ctx_size.has_value()) {
    std::cout << "  ctx_size=" << *profile.ctx_size << "\n";
  }
  if (profile.batch_size.has_value()) {
    std::cout << "  batch_size=" << *profile.batch_size << "\n";
  }
  if (profile.gpu_layers.has_value()) {
    std::cout << "  gpu_layers=" << *profile.gpu_layers << "\n";
  }
  if (apply) {
    SaveJsonFile(
        BuildControlPaths(config).runtime_assets_path,
        json{
            {"version", 1},
            {"plane_name", config.plane_name},
            {"runtime_profile", profile.name},
            {"runtime_mode", "llama-library"},
            {"llama_args", profile.llama_args},
            {"models_root", config.models_root},
            {"gguf_cache_dir", config.gguf_cache_dir},
        });
  }
  PrintPrepareRuntime(config, apply);
}

int PrintRuntimeAssetsStatus(const RuntimeConfig& config) {
  const json payload = LoadRuntimeAssetsStatus(config);
  if (payload.empty()) {
    std::cout << "(empty)\n";
    return 1;
  }
  std::cout << "runtime-assets-status:\n";
  std::cout << "  runtime_profile=" << payload.value("runtime_profile", std::string{"(empty)"}) << "\n";
  std::cout << "  runtime_mode=" << payload.value("runtime_mode", std::string{"llama-library"}) << "\n";
  std::cout << "  models_root=" << payload.value("models_root", std::string{"(empty)"}) << "\n";
  std::cout << "  gguf_cache_dir=" << payload.value("gguf_cache_dir", std::string{"(empty)"}) << "\n";
  std::cout << "  llama_args="
            << Join(payload.value("llama_args", std::vector<std::string>{}), ",") << "\n";
  return 0;
}

json FindCachedEntry(
    const json& registry,
    const std::string& alias,
    const std::string& local_model_path) {
  for (const auto& entry : registry.value("entries", json::array())) {
    if (entry.value("alias", std::string{}) == alias ||
        entry.value("local_model_path", std::string{}) == local_model_path) {
      return entry;
    }
  }
  return json::object();
}

void PreloadModel(const RuntimeConfig& config, const Args& args, bool apply) {
  if (args.alias.empty() || args.source_model_id.empty() || args.local_model_path.empty()) {
    Throw("preload-model requires --alias, --source-model-id, and --local-model-path");
  }
  const fs::path local_model_path(ExpandUserPath(args.local_model_path));
  const std::string runtime_model_path =
      args.runtime_model_path.empty() ? local_model_path.string() : args.runtime_model_path;
  const fs::path marker_root = local_model_path.has_extension() ? local_model_path.parent_path() : local_model_path;
  const fs::path marker_path = marker_root / ".comet-model-cache.json";
  const ControlPaths paths = BuildControlPaths(config);
  json registry = LoadRegistry(config);
  const json existing = FindCachedEntry(registry, args.alias, local_model_path.string());

  json payload{
      {"alias", args.alias},
      {"source_model_id", args.source_model_id},
      {"local_model_path", local_model_path.string()},
      {"runtime_model_path", runtime_model_path},
      {"marker_path", marker_path.string()},
      {"status", "prepared"},
  };
  std::cout << "preload-model-plan:\n";
  std::cout << "  alias=" << args.alias << "\n";
  std::cout << "  source_model_id=" << args.source_model_id << "\n";
  std::cout << "  local_model_path=" << local_model_path.string() << "\n";
  std::cout << "  runtime_model_path=" << runtime_model_path << "\n";
  std::cout << "  registry_path=" << paths.registry_path.string() << "\n";
  std::cout << "  marker_path=" << marker_path.string() << "\n";
  std::cout << "  registry_entry=" << (existing.empty() ? "missing" : "present") << "\n";
  if (apply) {
    if (local_model_path.has_extension()) {
      if (marker_root.empty()) {
        Throw("local model file path must have a parent directory");
      }
      fs::create_directories(marker_root);
    } else {
      fs::create_directories(local_model_path);
    }
    SaveJsonFile(marker_path, payload);
    json next_entries = json::array();
    for (const auto& entry : registry.value("entries", json::array())) {
      if (entry.value("alias", std::string{}) != args.alias) {
        next_entries.push_back(entry);
      }
    }
    next_entries.push_back(payload);
    registry = json{{"version", 1}, {"plane_name", config.plane_name}, {"entries", next_entries}};
    SaveJsonFile(paths.registry_path, registry);
  }
}

int CacheStatus(const RuntimeConfig& config, const Args& args) {
  if (args.alias.empty() || args.local_model_path.empty()) {
    Throw("cache-status requires --alias and --local-model-path");
  }
  const fs::path local_model_path(ExpandUserPath(args.local_model_path));
  const fs::path marker_root = local_model_path.has_extension() ? local_model_path.parent_path() : local_model_path;
  const fs::path marker_path = marker_root / ".comet-model-cache.json";
  const json registry = LoadRegistry(config);
  const json entry = FindCachedEntry(registry, args.alias, local_model_path.string());
  std::cout << "cache-status:\n";
  std::cout << "  alias=" << args.alias << "\n";
  std::cout << "  local_model_path=" << local_model_path.string() << "\n";
  std::cout << "  path_exists=" << (fs::exists(local_model_path) ? "yes" : "no") << "\n";
  std::cout << "  marker_exists=" << (fs::exists(marker_path) ? "yes" : "no") << "\n";
  std::cout << "  registry=" << (entry.empty() ? "missing" : "present") << "\n";
  if (!entry.empty()) {
    std::cout << "  source_model_id=" << entry.value("source_model_id", std::string{}) << "\n";
    std::cout << "  runtime_model_path=" << entry.value("runtime_model_path", std::string{}) << "\n";
    std::cout << "  status=" << entry.value("status", std::string{"unknown"}) << "\n";
  }
  return (!entry.empty() || fs::exists(marker_path) || fs::exists(local_model_path)) ? 0 : 1;
}

void SwitchModel(
    const RuntimeConfig& config,
    const RuntimeProfile& profile,
    const Args& args,
    bool apply) {
  if (args.model_id.empty()) {
    Throw("switch-model requires --model-id");
  }
  if (args.tp <= 0 || args.pp <= 0) {
    Throw("--tp and --pp must be positive");
  }
  const int required_slots = args.tp * args.pp;
  const int available_slots = EnabledGpuNodeCount(config);
  if (available_slots < required_slots) {
    Throw("insufficient enabled gpu nodes");
  }
  const json registry = LoadRegistry(config);
  json cached_entry = json::object();
  for (const auto& entry : registry.value("entries", json::array())) {
    if (entry.value("alias", std::string{}) == args.model_id ||
        entry.value("source_model_id", std::string{}) == args.model_id) {
      cached_entry = entry;
      break;
    }
  }
  const std::string served_model_name =
      args.served_model_name.empty() ? SafeServedModelName(args.model_id) : args.served_model_name;
  json payload{
      {"model_id", args.model_id},
      {"served_model_name", served_model_name},
      {"tensor_parallel_size", args.tp},
      {"pipeline_parallel_size", args.pp},
      {"required_gpu_slots", required_slots},
      {"available_gpu_slots", available_slots},
      {"gpu_memory_utilization", args.gpu_memory_utilization},
      {"runtime_profile", profile.name},
      {"primary_infer_node", config.primary_infer_node},
      {"llama_port", config.llama_port},
      {"cached_local_model_path", cached_entry.value("local_model_path", std::string{})},
      {"cached_runtime_model_path",
       cached_entry.value(
           "runtime_model_path",
           cached_entry.value("local_model_path", std::string{}))},
      {"llama_args", profile.llama_args},
  };
  std::cout << "switch-model-plan:\n";
  for (const auto& key : {"model_id",
                          "served_model_name",
                          "tensor_parallel_size",
                          "pipeline_parallel_size",
                          "required_gpu_slots",
                          "available_gpu_slots",
                          "gpu_memory_utilization",
                          "runtime_profile",
                          "primary_infer_node",
                          "llama_port",
                          "cached_local_model_path",
                          "cached_runtime_model_path"}) {
    std::cout << "  " << key << "=" << payload.at(key) << "\n";
  }
  const std::vector<std::string> extra_args = payload.at("llama_args").get<std::vector<std::string>>();
  std::cout << "  llama_args=" << (extra_args.empty() ? "(empty)" : Join(extra_args, ",")) << "\n";
  for (const auto& gpu_node : config.gpu_nodes) {
    if (!gpu_node.value("enabled", true)) {
      continue;
    }
    std::cout << "  gpu_node="
              << gpu_node.value("node_name", std::string{}) << "::"
              << gpu_node.value("name", std::string{}) << "::"
              << gpu_node.value("gpu_device", std::string{}) << " fraction="
              << gpu_node.value("gpu_fraction", 0.0) << "\n";
  }
  if (apply) {
    json active_model = json{{"version", 1}, {"plane_name", config.plane_name}};
    active_model.update(payload);
    SaveJsonFile(BuildControlPaths(config).active_model_path, active_model);
  }
}

void PrintGatewayPlan(const RuntimeConfig& config, bool apply) {
  const json payload = BuildGatewayPayload(config);
  std::cout << "gateway-plan:\n";
  std::cout << "  listen=" << payload.at("listen_host").get<std::string>() << ":"
            << payload.at("listen_port").get<int>() << "\n";
  std::cout << "  server_name=" << payload.at("server_name").get<std::string>() << "\n";
  std::cout << "  upstream_models_url=" << payload.at("upstream_models_url").get<std::string>() << "\n";
  std::cout << "  upstream_health_url=" << payload.at("upstream_health_url").get<std::string>() << "\n";
  std::cout << "  proxy_health_url=" << payload.at("proxy_health_url").get<std::string>() << "\n";
  std::cout << "  active_model=" << payload.value("active_model_id", std::string{"(empty)"})
            << " served=" << payload.value("active_served_model_name", std::string{"(empty)"}) << "\n";
  std::cout << "  gateway_plan_path=" << BuildControlPaths(config).gateway_plan_path.string() << "\n";
  if (apply) {
    SaveJsonFile(BuildControlPaths(config).gateway_plan_path, payload);
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
  std::cout << "  upstream_models_url=" << payload.value("upstream_models_url", std::string{}) << "\n";
  std::cout << "  upstream_health_url=" << payload.value("upstream_health_url", std::string{}) << "\n";
  std::cout << "  proxy_health_url=" << payload.value("proxy_health_url", std::string{}) << "\n";
  std::cout << "  active_model=" << payload.value("active_model_id", std::string{"(empty)"})
            << " served=" << payload.value("active_served_model_name", std::string{"(empty)"}) << "\n";
  return 0;
}

int PrintStatus(const RuntimeConfig& config, const std::string& backend, bool apply) {
  const ControlPaths paths = BuildControlPaths(config);
  comet::RuntimeStatus status = BuildRuntimeStatus(config, backend, "planned", false, false, 0, "");
  status = MergeWithObservedRuntimeStatus(std::move(status), paths.runtime_status_path.string());
  std::cout << "[runtime]\n";
  std::cout << "plane=" << status.plane_name << "\n";
  std::cout << "control_root=" << status.control_root << "\n";
  std::cout << "controller_url=" << status.controller_url << "\n";
  std::cout << "primary_infer_node=" << status.primary_infer_node << "\n";
  std::cout << "runtime_phase=" << status.runtime_phase << "\n";
  std::cout << "enabled_gpu_nodes=" << status.enabled_gpu_nodes << "\n\n";
  std::cout << "[cache]\n";
  std::cout << "registry_entries=" << status.registry_entries << "\n";
  std::cout << "aliases=" << (status.aliases.empty() ? "(empty)" : Join(status.aliases, ",")) << "\n\n";
  std::cout << "[active-model]\n";
  if (status.active_model_ready) {
    std::cout << "model_id=" << status.active_model_id << "\n";
    std::cout << "served_model_name=" << status.active_served_model_name << "\n";
    std::cout << "runtime_profile=" << status.active_runtime_profile << "\n";
    std::cout << "cached_local_model_path="
              << (status.cached_local_model_path.empty() ? "(empty)" : status.cached_local_model_path)
              << "\n";
    const json active_model = LoadActiveModel(config);
    std::cout << "cached_runtime_model_path="
              << active_model.value(
                     "cached_runtime_model_path",
                     active_model.value("cached_local_model_path", std::string{"(empty)"}))
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
              << (status.active_served_model_name.empty() ? "(empty)" : status.active_served_model_name)
              << "\n";
  } else {
    std::cout << "state=(empty)\n";
  }
  std::cout << "\n[readiness]\n";
  std::cout << "active_model_ready=" << (status.active_model_ready ? "yes" : "no") << "\n";
  std::cout << "gateway_plan_ready=" << (status.gateway_plan_ready ? "yes" : "no") << "\n";
  std::cout << "inference_ready=" << (status.inference_ready ? "yes" : "no") << "\n";
  std::cout << "gateway_ready=" << (status.gateway_ready ? "yes" : "no") << "\n";
  std::cout << "launch_ready=" << (status.launch_ready ? "yes" : "no") << "\n";
  if (apply) {
    comet::SaveRuntimeStatusJson(status, paths.runtime_status_path.string());
  }
  return 0;
}

void StopRuntime(const RuntimeConfig& config, bool apply, const std::string& backend) {
  const ControlPaths paths = BuildControlPaths(config);
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
    comet::SaveRuntimeStatusJson(
        BuildRuntimeStatus(config, backend, "stopped", false, false, 0, ""),
        paths.runtime_status_path.string());
    PrintStatus(config, backend, false);
  }
}

void PrintLaunchPlan(const RuntimeConfig& config) {
  std::cout << "launch-plan:\n";
  std::cout << "  primary-infer=node:" << config.primary_infer_node
            << " llama_port:" << config.llama_port
            << " ctx_size:" << config.llama_ctx_size
            << " threads:" << config.llama_threads
            << " gpu_layers:" << config.llama_gpu_layers
            << " net_if:" << config.net_if << "\n";
  for (const auto& gpu_node : config.gpu_nodes) {
    if (!gpu_node.value("enabled", true)) {
      continue;
    }
    const std::string node_name = gpu_node.value("node_name", std::string{});
    const std::string name = gpu_node.value("name", std::string{});
    const std::string gpu_device = gpu_node.value("gpu_device", std::string{});
    const double gpu_fraction = gpu_node.value("gpu_fraction", 0.0);
    if (node_name == config.primary_infer_node) {
      std::cout << "  primary-infer-local-worker=node:" << node_name << " worker:" << name
                << " gpu:" << gpu_device << " fraction:" << gpu_fraction << "\n";
    } else {
      std::cout << "  inference-worker=node:" << node_name << " worker:" << name
                << " gpu:" << gpu_device << " fraction:" << gpu_fraction << "\n";
    }
  }
  std::cout << "  llama.cpp=head:" << config.primary_infer_node << " port:" << config.llama_port
            << " gguf_cache_dir:" << config.gguf_cache_dir << " log_dir:" << config.infer_log_dir
            << "\n";
  std::cout << "  gateway=listen:" << config.gateway_listen_host << ":" << config.gateway_listen_port
            << " server_name:" << config.gateway_server_name << "\n";
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
    const int enabled = EnabledGpuNodeCount(config);
    std::cout << "  enabled gpu nodes: " << (enabled > 0 ? "OK" : "FAIL") << " (" << enabled << ")\n";
    if (enabled == 0) {
      rc = 1;
    }
    bool primary_present = false;
    for (const auto& gpu_node : config.gpu_nodes) {
      if (gpu_node.value("enabled", true) &&
          gpu_node.value("node_name", std::string{}) == config.primary_infer_node) {
        primary_present = true;
        break;
      }
    }
    std::cout << "  primary infer node: " << (primary_present ? "OK" : "FAIL")
              << " (" << config.primary_infer_node << ")\n";
    if (!primary_present) {
      rc = 1;
    }
  }
  if (selected.count("filesystem") > 0) {
    std::cout << "[doctor filesystem]\n";
    for (const auto& path : RuntimeDirs(config)) {
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
          exists ? "OK" : (std::string(command) == "docker" || std::string(command) == "nvidia-smi") ? "WARN" : "FAIL";
      std::cout << "  " << command << ": " << status << "\n";
      if (status == "FAIL") {
        rc = 1;
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

class LocalRuntime {
 public:
  LocalRuntime(
      const RuntimeConfig& config,
      std::string backend,
      std::string started_at,
      std::unique_ptr<LlamaLibraryEngine> engine)
      : config_(config),
        backend_(std::move(backend)),
        started_at_(std::move(started_at)),
        engine_(std::move(engine)),
        inference_server_("0.0.0.0", config.llama_port, "comet-llama-local", config, engine_.get()),
        gateway_server_(
            config.gateway_listen_host,
            config.gateway_listen_port,
            "comet-gateway-local",
            config,
            engine_.get()) {}

  int Run() {
    std::signal(SIGINT, SignalHandler);
    std::signal(SIGTERM, SignalHandler);
    TouchReadyFile();
    WriteRuntimeStatus(config_, backend_, "starting", false, false, static_cast<int>(getpid()), started_at_);
    inference_server_.Start();
    gateway_server_.Start();
    WriteRuntimeStatus(config_, backend_, "running", true, true, static_cast<int>(getpid()), started_at_);
    while (!g_stop_requested.load()) {
      std::this_thread::sleep_for(std::chrono::seconds(2));
      WriteRuntimeStatus(config_, backend_, "running", true, true, static_cast<int>(getpid()), started_at_);
    }
    WriteRuntimeStatus(config_, backend_, "stopping", false, false, static_cast<int>(getpid()), started_at_);
    gateway_server_.Stop();
    inference_server_.Stop();
    WriteRuntimeStatus(config_, backend_, "stopped", false, false, static_cast<int>(getpid()), started_at_);
    return 0;
  }

 private:
  RuntimeConfig config_;
  std::string backend_;
  std::string started_at_;
  std::unique_ptr<LlamaLibraryEngine> engine_;
  LocalHttpServer inference_server_;
  LocalHttpServer gateway_server_;
};

bool HasResolvableGgufModel(const RuntimeConfig& config) {
  const json active_model = LoadActiveModel(config);
  return active_model.is_object() && ResolveGgufPath(active_model).has_value();
}

int LaunchEmbeddedRuntime(const RuntimeConfig& config, const std::string& backend) {
  LocalRuntime runtime(config, backend, UtcNowIso(), nullptr);
  return runtime.Run();
}

int LaunchLlamaRuntime(const RuntimeConfig& config) {
  const json active_model = LoadActiveModel(config);
  if (active_model.empty()) {
    Throw("llama backend requires an active model");
  }
  auto engine = std::make_unique<LlamaLibraryEngine>(config, active_model);
  LocalRuntime runtime(config, "llama", UtcNowIso(), std::move(engine));
  return runtime.Run();
}

int LaunchRuntime(const RuntimeConfig& config, const std::string& requested_backend) {
  if (requested_backend == "embedded") {
    return LaunchEmbeddedRuntime(config, "embedded");
  }
  if (requested_backend == "llama") {
    return LaunchLlamaRuntime(config);
  }
  if (requested_backend != "auto") {
    Throw("unsupported backend: " + requested_backend);
  }
  const json active_model = LoadActiveModel(config);
  if (active_model.empty()) {
    std::cout << "[comet-inferctl] auto backend fallback to embedded: no active model\n";
    return LaunchEmbeddedRuntime(config, "embedded");
  }
  if (!HasResolvableGgufModel(config)) {
    std::cout << "[comet-inferctl] auto backend fallback to embedded: active model has no local GGUF\n";
    return LaunchEmbeddedRuntime(config, "embedded");
  }
  return LaunchLlamaRuntime(config);
}

void PrintJsonOrEmpty(const json& value) {
  if (value.empty()) {
    std::cout << "(empty)\n";
    return;
  }
  std::cout << value.dump(2) << "\n";
}

}  // namespace

int main(int argc, char** argv) {
  try {
    const Args args = ParseArgs(argc, argv);
    if (args.command == "probe-url") {
      return ProbeUrl(args.probe_url) ? 0 : 1;
    }

    if (args.command == "list-profiles") {
      PrintListProfiles(LoadProfiles(args.profiles_path));
      return 0;
    }

    const RuntimeConfig config = LoadRuntimeConfig(args.config_path);
    const json profiles_json = LoadProfiles(args.profiles_path);

    if (args.command == "show-config") {
      std::cout << config.raw.dump(2) << "\n";
      return 0;
    }
    if (args.command == "show-active-model") {
      PrintJsonOrEmpty(LoadActiveModel(config));
      return 0;
    }
    if (args.command == "validate-config") {
      PrintConfigSummary(config);
      return 0;
    }
    if (args.command == "prepare-runtime") {
      PrintPrepareRuntime(config, args.apply);
      return 0;
    }
    if (args.command == "bootstrap-runtime") {
      BootstrapRuntime(config, ResolveProfile(profiles_json, args.profile), args.apply);
      return 0;
    }
    if (args.command == "runtime-assets-status") {
      return PrintRuntimeAssetsStatus(config);
    }
    if (args.command == "preload-model") {
      PreloadModel(config, args, args.apply);
      return 0;
    }
    if (args.command == "cache-status") {
      return CacheStatus(config, args);
    }
    if (args.command == "switch-model") {
      SwitchModel(config, ResolveProfile(profiles_json, args.profile), args, args.apply);
      return 0;
    }
    if (args.command == "gateway-plan") {
      PrintGatewayPlan(config, args.apply);
      return 0;
    }
    if (args.command == "gateway-status") {
      return PrintGatewayStatus(config);
    }
    if (args.command == "status") {
      return PrintStatus(config, args.backend, args.apply);
    }
    if (args.command == "stop") {
      StopRuntime(config, args.apply, args.backend);
      return 0;
    }
    if (args.command == "plan-launch") {
      PrintLaunchPlan(config);
      return 0;
    }
    if (args.command == "doctor") {
      return RunDoctor(config, args.checks);
    }
    if (args.command == "bootstrap-dry-run") {
      PrintConfigSummary(config);
      BootstrapRuntime(config, ResolveProfile(profiles_json, args.profile), args.apply);
      const int doctor_rc = RunDoctor(config, args.checks);
      PrintLaunchPlan(config);
      PrintGatewayPlan(config, args.apply);
      PrintStatus(config, args.backend, args.apply);
      return doctor_rc;
    }
    if (args.command == "launch-embedded-runtime") {
      return LaunchEmbeddedRuntime(config, "embedded");
    }
    if (args.command == "launch-runtime") {
      return LaunchRuntime(config, args.backend);
    }

    Throw("unsupported command: " + args.command);
  } catch (const std::exception& error) {
    std::cerr << "error: " << error.what() << "\n";
    return 1;
  }
}
