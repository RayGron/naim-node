#include "app/infer_app.h"
#include "app/infer_command_line.h"
#include "app/infer_cli_output_support.h"
#include "app/infer_model_cache_support.h"
#include "app/infer_status_support.h"
#include "http/infer_http_types.h"
#include "http/local_http_server.h"
#include "platform/infer_signal_service.h"
#include "runtime/infer_control_support.h"
#include "runtime/infer_prewarm_support.h"
#include "runtime/infer_replica_support.h"
#include "runtime/infer_runtime_support.h"
#include "runtime/infer_runtime_types.h"
#include "runtime/llama_library_engine.h"
#include "runtime/local_runtime.h"
#include "comet/runtime/runtime_status.h"

#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <netdb.h>
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
#include <cstdint>
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

using InferSignalService = comet::infer::InferSignalService;
using InferCommandLineOptions = comet::infer::InferCommandLineOptions;
using LlamaLibraryEngine = comet::infer::LlamaLibraryEngine;
using RuntimeConfig = comet::infer::RuntimeConfig;
using RuntimeProfile = comet::infer::control_support::RuntimeProfile;
using HttpRequest = comet::infer::HttpRequest;
using LocalRuntime = comet::infer::LocalRuntime;
using SimpleResponse = comet::infer::SimpleResponse;
using UpstreamTarget = comet::infer::UpstreamTarget;
using ControlPaths = comet::infer::control_support::ControlPaths;
namespace cli_output_support = comet::infer::cli_output_support;
namespace model_cache_support = comet::infer::model_cache_support;
namespace status_support = comet::infer::status_support;
namespace prewarm_support = comet::infer::prewarm_support;
namespace replica_support = comet::infer::replica_support;

struct AssistantTextFilterState {
  std::string raw_text;
  std::string emitted_text;
};

json LoadWorkerGroupStatus(const RuntimeConfig& config);
std::string Trim(const std::string& value);
std::string JsonString(const json& object, const char* key);
std::string Lowercase(std::string value);

void SendResponse(int client_fd, const SimpleResponse& response);

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

std::string ToLowerCopy(std::string value) {
  std::transform(
      value.begin(),
      value.end(),
      value.begin(),
      [](unsigned char ch) { return static_cast<char>(std::tolower(ch)); });
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

UpstreamTarget ParseHttpUrl(const std::string& url) {
  constexpr std::string_view kHttpPrefix = "http://";
  if (url.rfind(std::string(kHttpPrefix), 0) != 0) {
    Throw("unsupported url (expected http://): " + url);
  }
  std::string remainder = url.substr(kHttpPrefix.size());
  const std::size_t slash = remainder.find('/');
  const std::string authority =
      slash == std::string::npos ? remainder : remainder.substr(0, slash);
  const std::size_t colon = authority.rfind(':');
  if (authority.empty()) {
    Throw("http url is missing host: " + url);
  }
  UpstreamTarget target;
  if (colon == std::string::npos) {
    target.host = authority;
    target.port = 80;
  } else {
    target.host = authority.substr(0, colon);
    target.port = std::stoi(authority.substr(colon + 1));
  }
  return target;
}

std::vector<std::string> ObservedRuntimeUpstreamBaseUrls(const RuntimeConfig& config) {
  const char* worker_vllm_upstream = std::getenv("COMET_INFER_VLLM_UPSTREAM_URL");
  if (worker_vllm_upstream != nullptr && std::strlen(worker_vllm_upstream) > 0) {
    return {std::string(worker_vllm_upstream)};
  }
  const auto ready_leaders = prewarm_support::ObservedReadyReplicaLeaderBaseUrls(config);
  if (!ready_leaders.empty()) {
    return ready_leaders;
  }
  if (config.runtime_engine != "vllm") {
    return {"http://127.0.0.1:" + std::to_string(config.api_port)};
  }
  return {};
}

std::optional<std::string> ResolveRuntimeObservedUpstreamBaseUrl(const RuntimeConfig& config) {
  const auto base_urls = ObservedRuntimeUpstreamBaseUrls(config);
  if (!base_urls.empty()) {
    return base_urls.front();
  }
  return std::nullopt;
}

std::optional<UpstreamTarget> ResolveRuntimeUpstreamTarget(const RuntimeConfig& config) {
  const auto observed_base_urls = prewarm_support::ObservedReadyReplicaLeaderBaseUrls(config);
  const auto routable_base_urls =
      prewarm_support::FilterPrewarmedReplicaBaseUrls(config, observed_base_urls);
  if (!routable_base_urls.empty()) {
    static std::atomic<std::uint64_t> next_replica{0};
    const std::uint64_t slot = next_replica.fetch_add(1, std::memory_order_relaxed);
    const std::string& base_url =
        routable_base_urls[slot % routable_base_urls.size()];
    return ParseHttpUrl(base_url);
  }
  if (!observed_base_urls.empty()) {
    return std::nullopt;
  }
  const auto base_url = ResolveRuntimeObservedUpstreamBaseUrl(config);
  if (!base_url.has_value()) {
    return std::nullopt;
  }
  return ParseHttpUrl(*base_url);
}

int ConnectTcpHost(const std::string& host, int port) {
  addrinfo hints{};
  hints.ai_family = AF_UNSPEC;
  hints.ai_socktype = SOCK_STREAM;
  hints.ai_protocol = IPPROTO_TCP;

  addrinfo* results = nullptr;
  const std::string service = std::to_string(port);
  const int rc = getaddrinfo(host.c_str(), service.c_str(), &hints, &results);
  if (rc != 0) {
    return -1;
  }

  int fd = -1;
  for (addrinfo* current = results; current != nullptr; current = current->ai_next) {
    fd = socket(current->ai_family, current->ai_socktype, current->ai_protocol);
    if (fd < 0) {
      continue;
    }
    if (connect(fd, current->ai_addr, current->ai_addrlen) == 0) {
      break;
    }
    close(fd);
    fd = -1;
  }

  freeaddrinfo(results);
  return fd;
}

bool ActiveModelLooksLikeQwen(const json& active_model) {
  const std::string model_id = ToLowerCopy(active_model.value("model_id", std::string{}));
  const std::string served_model_name =
      ToLowerCopy(active_model.value("served_model_name", std::string{}));
  const std::string model_path = ToLowerCopy(active_model.value("model_path", std::string{}));
  return model_id.find("qwen") != std::string::npos ||
         served_model_name.find("qwen") != std::string::npos ||
         model_path.find("qwen") != std::string::npos;
}

std::string NormalizeChatRole(const std::string& role) {
  const std::string normalized = ToLowerCopy(role);
  if (normalized == "system" || normalized == "user" || normalized == "assistant" ||
      normalized == "tool") {
    return normalized;
  }
  return "user";
}

std::string BuildLegacyChatPrompt(const json& payload) {
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
  return prompt.str();
}

std::string BuildQwenChatPrompt(const json& payload) {
  std::ostringstream prompt;
  for (const auto& message : payload.at("messages")) {
    if (!message.is_object()) {
      continue;
    }
    const std::string content = JsonString(message, "content");
    if (content.empty()) {
      continue;
    }
    prompt << "<|im_start|>" << NormalizeChatRole(message.value("role", std::string{"user"}))
           << "\n"
           << content << "<|im_end|>\n";
  }
  prompt << "<|im_start|>assistant\n";
  return prompt.str();
}

std::string TrimLeadingWhitespace(std::string value) {
  while (!value.empty() &&
         std::isspace(static_cast<unsigned char>(value.front())) != 0) {
    value.erase(value.begin());
  }
  return value;
}

std::string StripRepeatedAssistantPrefixes(std::string value) {
  while (true) {
    const std::string trimmed = TrimLeadingWhitespace(value);
    if (trimmed.rfind("assistant:", 0) == 0) {
      value = trimmed.substr(std::string("assistant:").size());
      continue;
    }
    if (trimmed.rfind("assistant\n", 0) == 0) {
      value = trimmed.substr(std::string("assistant\n").size());
      continue;
    }
    if (trimmed.rfind("<|im_start|>assistant", 0) == 0) {
      value = trimmed.substr(std::string("<|im_start|>assistant").size());
      continue;
    }
    return TrimLeadingWhitespace(value);
  }
}

bool StartsWithAssistantMarker(const std::string& line) {
  const std::string trimmed = TrimLeadingWhitespace(line);
  return trimmed.rfind("assistant:", 0) == 0 ||
         trimmed.rfind("<|im_start|>assistant", 0) == 0 ||
         trimmed == "assistant";
}

std::string StripAssistantMarkerFromLine(const std::string& line) {
  std::string trimmed = TrimLeadingWhitespace(line);
  if (trimmed.rfind("assistant:", 0) == 0) {
    return Trim(trimmed.substr(std::string("assistant:").size()));
  }
  if (trimmed.rfind("<|im_start|>assistant", 0) == 0) {
    return Trim(trimmed.substr(std::string("<|im_start|>assistant").size()));
  }
  if (trimmed == "assistant") {
    return "";
  }
  return Trim(trimmed);
}

std::string CollapseAssistantTaggedTranscript(const std::string& value) {
  std::stringstream stream(value);
  std::string line;
  std::vector<std::string> cleaned_lines;
  bool saw_assistant_line = false;
  while (std::getline(stream, line)) {
    if (!line.empty() && line.back() == '\r') {
      line.pop_back();
    }
    if (StartsWithAssistantMarker(line)) {
      saw_assistant_line = true;
    }
    if (!saw_assistant_line) {
      continue;
    }
    const std::string cleaned = StripAssistantMarkerFromLine(line);
    if (cleaned.empty()) {
      continue;
    }
    if (!cleaned_lines.empty() && cleaned_lines.back() == cleaned) {
      continue;
    }
    cleaned_lines.push_back(cleaned);
  }
  if (!saw_assistant_line || cleaned_lines.empty()) {
    return value;
  }
  std::ostringstream out;
  for (std::size_t index = 0; index < cleaned_lines.size(); ++index) {
    if (index > 0) {
      out << "\n";
    }
    out << cleaned_lines[index];
  }
  return out.str();
}

std::string TruncateAtFirstMarker(
    const std::string& value,
    const std::vector<std::string>& markers) {
  std::size_t cut = value.size();
  for (const auto& marker : markers) {
    const std::size_t pos = value.find(marker);
    if (pos != std::string::npos) {
      cut = std::min(cut, pos);
    }
  }
  return value.substr(0, cut);
}

std::string RemoveThinkBlocks(std::string value) {
  while (true) {
    const std::size_t begin = value.find("<think>");
    if (begin == std::string::npos) {
      return value;
    }
    const std::size_t end = value.find("</think>", begin);
    if (end == std::string::npos) {
      return value.substr(0, begin);
    }
    value.erase(begin, end + std::string("</think>").size() - begin);
  }
}

std::string SanitizeAssistantText(const std::string& raw_text) {
  std::string sanitized = raw_text;
  sanitized = RemoveThinkBlocks(sanitized);
  sanitized = TruncateAtFirstMarker(
      sanitized,
      {
          "<|im_end|>",
          "<|endoftext|>",
          "<|eot_id|>",
          "\nuser:",
          "\nsystem:",
          "\n<|im_start|>user",
          "\n<|im_start|>system",
      });
  sanitized = CollapseAssistantTaggedTranscript(sanitized);
  sanitized = StripRepeatedAssistantPrefixes(sanitized);
  return sanitized;
}

std::string UpdateAssistantTextFilter(
    AssistantTextFilterState& state,
    const std::string& appended_piece) {
  if (!appended_piece.empty()) {
    state.raw_text += appended_piece;
  }
  const std::string sanitized = SanitizeAssistantText(state.raw_text);
  if (sanitized.size() < state.emitted_text.size()) {
    return "";
  }
  if (sanitized.compare(0, state.emitted_text.size(), state.emitted_text) != 0) {
    state.emitted_text = sanitized;
    return sanitized;
  }
  const std::string delta = sanitized.substr(state.emitted_text.size());
  state.emitted_text = sanitized;
  return delta;
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
  config.runtime_engine =
      inference.value("runtime_engine", config.runtime_engine);
  config.data_parallel_mode =
      inference.value("data_parallel_mode", config.data_parallel_mode);
  config.data_parallel_lb_mode =
      inference.value("data_parallel_lb_mode", config.data_parallel_lb_mode);
  config.api_server_count =
      inference.value("api_server_count", config.api_server_count);
  config.worker_group = root.value("worker_group", json::object());
  config.net_if = Require<std::string>(inference, "net_if", "inference");
  config.models_root =
      ExpandUserPath(Require<std::string>(inference, "models_root", "inference"));
  config.model_cache_dir =
      ExpandUserPath(inference.value("model_cache_dir", config.models_root));
  config.gguf_cache_dir =
      ExpandUserPath(Require<std::string>(inference, "gguf_cache_dir", "inference"));
  config.infer_log_dir = ExpandUserPath(Require<std::string>(inference, "infer_log_dir", "inference"));
  config.api_port = inference.value("api_port", config.api_port);
  config.llama_port = Require<int>(inference, "llama_port", "inference");
  config.llama_ctx_size = inference.value("llama_ctx_size", config.llama_ctx_size);
  config.llama_threads = inference.value("llama_threads", config.llama_threads);
  config.llama_gpu_layers = inference.value("llama_gpu_layers", config.llama_gpu_layers);
  config.gateway_listen_host = Require<std::string>(gateway, "listen_host", "gateway");
  config.gateway_listen_port = Require<int>(gateway, "listen_port", "gateway");
  config.gateway_server_name = Require<std::string>(gateway, "server_name", "gateway");
  config.gpu_nodes = Require<json>(root, "gpu_nodes", "root");
  config.serving_workers = root.value("serving_workers", config.gpu_nodes);
  return config;
}

json LoadProfiles(const std::string& path_str) {
  return comet::infer::control_support::LoadProfiles(path_str);
}

RuntimeProfile ResolveProfile(const json& profiles_json, const std::string& name) {
  return comet::infer::control_support::ResolveProfile(profiles_json, name);
}

ControlPaths BuildControlPaths(const RuntimeConfig& config) {
  return comet::infer::control_support::BuildControlPaths(config);
}

json LoadWorkerGroupStatus(const RuntimeConfig& config) {
  return comet::infer::control_support::LoadWorkerGroupStatus(config);
}

json LoadActiveModel(const RuntimeConfig& config) {
  return comet::infer::control_support::LoadActiveModel(config);
}

json LoadGatewayPlan(const RuntimeConfig& config) {
  return comet::infer::control_support::LoadGatewayPlan(config);
}

json LoadRegistry(const RuntimeConfig& config) {
  return comet::infer::control_support::LoadRegistry(config);
}

int EnabledGpuNodeCount(const RuntimeConfig& config) {
  return comet::infer::control_support::EnabledGpuNodeCount(config);
}

std::string RuntimeUpstreamHealthUrl(const RuntimeConfig& config) {
  const auto base_url = ResolveRuntimeObservedUpstreamBaseUrl(config);
  return base_url.has_value() ? *base_url + "/health" : std::string{};
}

std::string RuntimeUpstreamModelsUrl(const RuntimeConfig& config) {
  const auto base_url = ResolveRuntimeObservedUpstreamBaseUrl(config);
  return base_url.has_value() ? *base_url + "/v1/models" : std::string{};
}

std::string RuntimeGatewayHealthUrl(const RuntimeConfig& config) {
  return "http://127.0.0.1:" + std::to_string(config.gateway_listen_port) + "/health";
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
  const auto topology = replica_support::InspectReplicaTopology(config);
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

std::string CompletionPromptFromRequest(const RuntimeConfig& config, const HttpRequest& request) {
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
    const json active_model = LoadActiveModel(config);
    const std::string result =
        ActiveModelLooksLikeQwen(active_model) ? BuildQwenChatPrompt(payload)
                                               : BuildLegacyChatPrompt(payload);
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
                 {"message",
                  json{{"role", "assistant"},
                       {"content", SanitizeAssistantText(result.text)}}},
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

json ParseRequestPayload(const HttpRequest& request) {
  if (request.body.empty()) {
    return json::object();
  }
  return json::parse(request.body);
}

bool RequestWantsStream(const HttpRequest& request) {
  if (request.path != "/v1/chat/completions" || request.method != "POST") {
    return false;
  }
  const json payload = ParseRequestPayload(request);
  return payload.value("stream", false);
}

bool SendAll(int fd, const std::string& payload) {
  const char* data = payload.c_str();
  std::size_t remaining = payload.size();
  while (remaining > 0) {
    const ssize_t written = send(fd, data, remaining, 0);
    if (written <= 0) {
      return false;
    }
    data += written;
    remaining -= static_cast<std::size_t>(written);
  }
  return true;
}

std::string ForceConnectionClose(const std::string& request_text) {
  const std::size_t headers_end = request_text.find("\r\n\r\n");
  if (headers_end == std::string::npos) {
    return request_text;
  }
  const std::string header_text = request_text.substr(0, headers_end);
  const std::string body = request_text.substr(headers_end + 4);
  std::stringstream stream(header_text);
  std::string first_line;
  std::getline(stream, first_line);
  if (!first_line.empty() && first_line.back() == '\r') {
    first_line.pop_back();
  }

  std::ostringstream out;
  out << first_line << "\r\n";
  bool wrote_connection_header = false;
  std::string line;
  while (std::getline(stream, line)) {
    if (!line.empty() && line.back() == '\r') {
      line.pop_back();
    }
    if (line.empty()) {
      continue;
    }
    const std::size_t colon = line.find(':');
    if (colon == std::string::npos) {
      out << line << "\r\n";
      continue;
    }
    const std::string key = Lowercase(Trim(line.substr(0, colon)));
    if (key == "connection") {
      out << "Connection: close\r\n";
      wrote_connection_header = true;
      continue;
    }
    out << line << "\r\n";
  }
  if (!wrote_connection_header) {
    out << "Connection: close\r\n";
  }
  out << "\r\n" << body;
  return out.str();
}

bool ProxyHttpRequest(
    int client_fd,
    const std::string& request_data,
    const UpstreamTarget& upstream) {
  const int upstream_fd = ConnectTcpHost(upstream.host, upstream.port);
  if (upstream_fd < 0) {
    return false;
  }

  const std::string proxied_request = ForceConnectionClose(request_data);
  if (!SendAll(upstream_fd, proxied_request)) {
    shutdown(upstream_fd, SHUT_RDWR);
    close(upstream_fd);
    return false;
  }

  std::array<char, 8192> buffer{};
  bool received_any_response = false;
  while (true) {
    const ssize_t read_count = recv(upstream_fd, buffer.data(), buffer.size(), 0);
    if (read_count < 0) {
      shutdown(upstream_fd, SHUT_RDWR);
      close(upstream_fd);
      return false;
    }
    if (read_count == 0) {
      break;
    }
    received_any_response = true;
    if (!SendAll(client_fd, std::string(buffer.data(), static_cast<std::size_t>(read_count)))) {
      shutdown(upstream_fd, SHUT_RDWR);
      close(upstream_fd);
      return false;
    }
  }

  shutdown(upstream_fd, SHUT_RDWR);
  close(upstream_fd);
  return received_any_response;
}

bool SendSseHeaders(int client_fd) {
  std::ostringstream out;
  out << "HTTP/1.1 200 OK\r\n";
  out << "Content-Type: text/event-stream\r\n";
  out << "Cache-Control: no-cache\r\n";
  out << "Connection: close\r\n";
  out << "X-Accel-Buffering: no\r\n\r\n";
  return SendAll(client_fd, out.str());
}

bool IsUtf8ContinuationByte(unsigned char value) {
  return (value & 0xC0) == 0x80;
}

std::size_t Utf8SequenceLength(unsigned char lead) {
  if ((lead & 0x80) == 0) {
    return 1;
  }
  if ((lead & 0xE0) == 0xC0) {
    return 2;
  }
  if ((lead & 0xF0) == 0xE0) {
    return 3;
  }
  if ((lead & 0xF8) == 0xF0) {
    return 4;
  }
  return 0;
}

std::size_t ValidUtf8PrefixLength(const std::string& value) {
  std::size_t index = 0;
  while (index < value.size()) {
    const unsigned char lead = static_cast<unsigned char>(value[index]);
    const std::size_t sequence_length = Utf8SequenceLength(lead);
    if (sequence_length == 0) {
      break;
    }
    if (index + sequence_length > value.size()) {
      break;
    }
    bool valid = true;
    for (std::size_t offset = 1; offset < sequence_length; ++offset) {
      if (!IsUtf8ContinuationByte(static_cast<unsigned char>(value[index + offset]))) {
        valid = false;
        break;
      }
    }
    if (!valid) {
      break;
    }
    index += sequence_length;
  }
  return index;
}

bool CanEncodeJsonUtf8(const std::string& value) {
  try {
    json payload{{"delta", value}};
    static_cast<void>(payload.dump());
    return true;
  } catch (const std::exception&) {
    return false;
  }
}

bool SendSseEvent(int client_fd, const std::string& event_name, const json& payload) {
  std::ostringstream frame;
  frame << "event: " << event_name << "\n";
  std::stringstream lines(payload.dump().append("\n"));
  std::string line;
  while (std::getline(lines, line)) {
    if (!line.empty() && line.back() == '\r') {
      line.pop_back();
    }
    frame << "data: " << line << "\n";
  }
  frame << "\n";
  return SendAll(client_fd, frame.str());
}

bool SendSseDone(int client_fd) {
  return SendAll(client_fd, "data: [DONE]\n\n");
}

void SendErrorResponse(
    int client_fd,
    int status_code,
    const std::string& message) {
  const SimpleResponse response = BuildJsonResponse(
      status_code,
      json{{"status", "error"}, {"message", message}});
  SendResponse(client_fd, response);
}

void HandleStreamingChatRequest(
    int client_fd,
    const RuntimeConfig& config,
    const HttpRequest& request,
    LlamaLibraryEngine* engine) {
  if (engine == nullptr) {
    SendErrorResponse(client_fd, 503, "llama engine is not loaded");
    return;
  }
  bool sse_started = false;
  try {
    const std::string prompt = CompletionPromptFromRequest(config, request);
    const int max_tokens = MaxTokensFromRequest(request);
    const json active_model = LoadActiveModel(config);
    const std::string served_model_name =
        active_model.value(
            "served_model_name",
            active_model.value("model_id", std::string{"(unknown)"}));
    if (!SendSseHeaders(client_fd)) {
      return;
    }
    sse_started = true;
    const auto started_at = std::chrono::steady_clock::now();
    std::string pending_utf8;
    AssistantTextFilterState filter_state;
    const auto emit_delta = [&](const std::string& chunk) {
      if (chunk.empty()) {
        return;
      }
      SendSseEvent(
          client_fd,
          "delta",
          json{
              {"model", served_model_name},
              {"delta", chunk},
          });
    };
    const auto flush_pending = [&](bool final_flush) {
      while (!pending_utf8.empty()) {
        if (CanEncodeJsonUtf8(pending_utf8)) {
          emit_delta(UpdateAssistantTextFilter(filter_state, pending_utf8));
          pending_utf8.clear();
          return;
        }
        const std::size_t valid_prefix_length = ValidUtf8PrefixLength(pending_utf8);
        if (valid_prefix_length > 0) {
          std::string prefix = pending_utf8.substr(0, valid_prefix_length);
          while (!prefix.empty() && !CanEncodeJsonUtf8(prefix)) {
            prefix.pop_back();
          }
          if (!prefix.empty()) {
            emit_delta(UpdateAssistantTextFilter(filter_state, prefix));
            pending_utf8.erase(0, prefix.size());
            continue;
          }
        }
        if (!final_flush) {
          return;
        }
        emit_delta(UpdateAssistantTextFilter(filter_state, "?"));
        pending_utf8.erase(0, 1);
      }
    };
    const auto result = engine->GenerateTextStream(
        prompt,
        max_tokens,
        [&](const std::string& piece) {
          if (piece.empty()) {
            return;
          }
          pending_utf8 += piece;
          flush_pending(false);
        });
    flush_pending(true);
    const auto finished_at = std::chrono::steady_clock::now();
    const auto latency_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                                finished_at - started_at)
                                .count();
    SendSseEvent(
        client_fd,
        "complete",
        json{
            {"model", served_model_name},
            {"finish_reason", result.finish_reason},
            {"latency_ms", latency_ms},
            {"usage",
             json{{"prompt_tokens", result.prompt_tokens},
                  {"completion_tokens", result.completion_tokens},
                  {"total_tokens", result.prompt_tokens + result.completion_tokens}}},
        });
    SendSseDone(client_fd);
  } catch (const std::exception& error) {
    if (sse_started) {
      try {
        SendSseEvent(
            client_fd,
            "error",
            json{
                {"message", error.what()},
            });
        SendSseDone(client_fd);
      } catch (const std::exception&) {
      }
    } else {
      SendErrorResponse(client_fd, 400, error.what());
    }
  }
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
      const std::string prompt = CompletionPromptFromRequest(config, request);
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

bool ProbeUrl(const std::string& url) {
  constexpr std::string_view kHttpPrefix = "http://";
  if (url.rfind(std::string(kHttpPrefix), 0) != 0) {
    return false;
  }
  std::string remainder = url.substr(kHttpPrefix.size());
  const std::size_t slash = remainder.find('/');
  const std::string path = slash == std::string::npos ? "/" : remainder.substr(slash);
  const UpstreamTarget target = ParseHttpUrl(url);

  const int fd = ConnectTcpHost(target.host, target.port);
  if (fd < 0) {
    return false;
  }

  const std::string request =
      "GET " + path + " HTTP/1.1\r\nHost: " + target.host + "\r\nConnection: close\r\n\r\n";
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

bool ProbeModelsUrl(const std::string& url) {
  constexpr std::string_view kHttpPrefix = "http://";
  if (url.rfind(std::string(kHttpPrefix), 0) != 0) {
    return false;
  }
  const UpstreamTarget target = ParseHttpUrl(url);
  const int fd = ConnectTcpHost(target.host, target.port);
  if (fd < 0) {
    return false;
  }

  const std::string request =
      "GET /v1/models HTTP/1.1\r\nHost: " + target.host +
      "\r\nConnection: close\r\n\r\n";
  if (send(fd, request.c_str(), request.size(), 0) < 0) {
    close(fd);
    return false;
  }

  std::string response;
  std::array<char, 4096> buffer{};
  while (true) {
    const ssize_t read_count = recv(fd, buffer.data(), buffer.size(), 0);
    if (read_count <= 0) {
      break;
    }
    response.append(buffer.data(), static_cast<std::size_t>(read_count));
    if (response.size() > 65536) {
      break;
    }
  }
  close(fd);
  if (!(response.rfind("HTTP/1.1 200", 0) == 0 || response.rfind("HTTP/1.0 200", 0) == 0)) {
    return false;
  }
  const std::size_t body_pos = response.find("\r\n\r\n");
  if (body_pos == std::string::npos) {
    return false;
  }
  json payload = json::parse(response.substr(body_pos + 4), nullptr, false);
  if (!payload.is_object() || !payload.contains("data") || !payload.at("data").is_array()) {
    return false;
  }
  return !payload.at("data").empty();
}

bool HasResolvableGgufModel(const RuntimeConfig& config) {
  const json active_model = LoadActiveModel(config);
  return active_model.is_object() && ResolveGgufPath(active_model).has_value();
}

int LaunchEmbeddedRuntime(
    const RuntimeConfig& config,
    const std::string& backend,
    InferSignalService& signal_service) {
  LocalRuntime runtime(config, backend, UtcNowIso(), nullptr, signal_service);
  return runtime.Run();
}

int LaunchLlamaRuntime(const RuntimeConfig& config, InferSignalService& signal_service) {
  const json active_model = LoadActiveModel(config);
  if (active_model.empty()) {
    Throw("llama backend requires an active model");
  }
  auto engine = std::make_unique<LlamaLibraryEngine>(config, active_model);
  LocalRuntime runtime(config, "llama", UtcNowIso(), std::move(engine), signal_service);
  return runtime.Run();
}

int LaunchWorkerVllmRuntime(const RuntimeConfig& config, InferSignalService& signal_service) {
  LocalRuntime runtime(
      config,
      "worker-vllm",
      UtcNowIso(),
      nullptr,
      signal_service,
      true);
  return runtime.Run();
}

int LaunchRuntime(
    const RuntimeConfig& config,
    const std::string& requested_backend,
    InferSignalService& signal_service) {
  if (requested_backend == "embedded") {
    return LaunchEmbeddedRuntime(config, "embedded", signal_service);
  }
  if (requested_backend == "llama") {
    return LaunchLlamaRuntime(config, signal_service);
  }
  if (requested_backend == "worker-vllm" || requested_backend == "vllm") {
    return LaunchWorkerVllmRuntime(config, signal_service);
  }
  if (requested_backend != "auto") {
    Throw("unsupported backend: " + requested_backend);
  }
  if (config.runtime_engine == "vllm") {
    return LaunchWorkerVllmRuntime(config, signal_service);
  }
  const json active_model = LoadActiveModel(config);
  if (active_model.empty()) {
    std::cout << "[comet-inferctl] auto backend fallback to embedded: no active model\n";
    return LaunchEmbeddedRuntime(config, "embedded", signal_service);
  }
  if (!HasResolvableGgufModel(config)) {
    std::cout << "[comet-inferctl] auto backend fallback to embedded: active model has no local GGUF\n";
    return LaunchEmbeddedRuntime(config, "embedded", signal_service);
  }
  return LaunchLlamaRuntime(config, signal_service);
}

void PrintJsonOrEmpty(const json& value) {
  if (value.empty()) {
    std::cout << "(empty)\n";
    return;
  }
  std::cout << value.dump(2) << "\n";
}

}  // namespace

namespace comet::infer::runtime_support {

int CreateListenSocket(const std::string& host, int port) {
  return ::CreateListenSocket(host, port);
}

std::optional<UpstreamTarget> ResolveRuntimeUpstreamTarget(const RuntimeConfig& config) {
  return ::ResolveRuntimeUpstreamTarget(config);
}

bool ProxyHttpRequest(
    int client_fd,
    const std::string& request_data,
    const UpstreamTarget& upstream) {
  return ::ProxyHttpRequest(client_fd, request_data, upstream);
}

void SendErrorResponse(int client_fd, int status_code, const std::string& message) {
  ::SendErrorResponse(client_fd, status_code, message);
}

bool RequestWantsStream(const HttpRequest& request) {
  return ::RequestWantsStream(request);
}

void HandleStreamingChatRequest(
    int client_fd,
    const RuntimeConfig& config,
    const HttpRequest& request,
    LlamaLibraryEngine* engine) {
  ::HandleStreamingChatRequest(client_fd, config, request, engine);
}

SimpleResponse HandleLocalRequest(
    const RuntimeConfig& config,
    const HttpRequest& request,
    const std::string& service_name,
    LlamaLibraryEngine* engine) {
  return ::HandleLocalRequest(config, request, service_name, engine);
}

void SendResponse(int client_fd, const SimpleResponse& response) {
  ::SendResponse(client_fd, response);
}

HttpRequest ParseHttpRequest(const std::string& request_text) {
  return ::ParseHttpRequest(request_text);
}

bool ProbeUrl(const std::string& url) {
  return ::ProbeUrl(url);
}

bool ProbeModelsUrl(const std::string& url) {
  return ::ProbeModelsUrl(url);
}

std::string RuntimeUpstreamHealthUrl(const RuntimeConfig& config) {
  return ::RuntimeUpstreamHealthUrl(config);
}

std::string RuntimeUpstreamModelsUrl(const RuntimeConfig& config) {
  return ::RuntimeUpstreamModelsUrl(config);
}

std::string RuntimeGatewayHealthUrl(const RuntimeConfig& config) {
  return ::RuntimeGatewayHealthUrl(config);
}

nlohmann::json LoadWorkerGroupStatus(const RuntimeConfig& config) {
  return ::LoadWorkerGroupStatus(config);
}

void TouchReadyFile() {
  ::TouchReadyFile();
}

void WriteInferRuntimeStatus(
    const RuntimeConfig& config,
    const std::string& backend,
    const std::string& phase,
    bool inference_ready,
    bool gateway_ready,
    int supervisor_pid,
    const std::string& started_at) {
  ::WriteRuntimeStatus(
      config,
      backend,
      phase,
      inference_ready,
      gateway_ready,
      supervisor_pid,
      started_at);
}

}  // namespace comet::infer::runtime_support

namespace comet::infer {

InferApp::InferApp(int argc, char** argv) : argc_(argc), argv_(argv) {}

int InferApp::Run() {
  try {
    InferSignalService signal_service;
    const InferCommandLineOptions args = InferCommandLine().Parse(argc_, argv_);
    if (args.command == "probe-url") {
      return ProbeUrl(args.probe_url) ? 0 : 1;
    }

    if (args.command == "list-profiles") {
      cli_output_support::PrintListProfiles(LoadProfiles(args.profiles_path));
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
      cli_output_support::PrintConfigSummary(config);
      return 0;
    }
    if (args.command == "prepare-runtime") {
      cli_output_support::PrintPrepareRuntime(config, args.apply);
      return 0;
    }
    if (args.command == "bootstrap-runtime") {
      cli_output_support::BootstrapRuntime(
          config,
          ResolveProfile(profiles_json, args.profile),
          args.apply);
      return 0;
    }
    if (args.command == "runtime-assets-status") {
      return cli_output_support::PrintRuntimeAssetsStatus(config);
    }
    if (args.command == "preload-model") {
      model_cache_support::PreloadModel(config, args, args.apply);
      return 0;
    }
    if (args.command == "cache-status") {
      return model_cache_support::CacheStatus(config, args);
    }
    if (args.command == "switch-model") {
      model_cache_support::SwitchModel(
          config,
          ResolveProfile(profiles_json, args.profile),
          args,
          args.apply);
      return 0;
    }
    if (args.command == "gateway-plan") {
      status_support::PrintGatewayPlan(config, args.apply);
      return 0;
    }
    if (args.command == "gateway-status") {
      return status_support::PrintGatewayStatus(config);
    }
    if (args.command == "status") {
      return status_support::PrintStatus(config, args.backend, args.apply);
    }
    if (args.command == "stop") {
      status_support::StopRuntime(config, args.apply, args.backend);
      return 0;
    }
    if (args.command == "plan-launch") {
      cli_output_support::PrintLaunchPlan(config);
      return 0;
    }
    if (args.command == "doctor") {
      return status_support::RunDoctor(config, args.checks);
    }
    if (args.command == "bootstrap-dry-run") {
      cli_output_support::PrintConfigSummary(config);
      cli_output_support::BootstrapRuntime(
          config,
          ResolveProfile(profiles_json, args.profile),
          args.apply);
      const int doctor_rc = status_support::RunDoctor(config, args.checks);
      cli_output_support::PrintLaunchPlan(config);
      status_support::PrintGatewayPlan(config, args.apply);
      status_support::PrintStatus(config, args.backend, args.apply);
      return doctor_rc;
    }
    if (args.command == "launch-embedded-runtime") {
      return LaunchEmbeddedRuntime(config, "embedded", signal_service);
    }
    if (args.command == "launch-runtime") {
      return LaunchRuntime(config, args.backend, signal_service);
    }

    Throw("unsupported command: " + args.command);
  } catch (const std::exception& error) {
    std::cerr << "error: " << error.what() << "\n";
    return 1;
  }
}

}  // namespace comet::infer
