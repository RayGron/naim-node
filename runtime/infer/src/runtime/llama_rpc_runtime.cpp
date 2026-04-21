#include "runtime/llama_rpc_runtime.h"

#include <arpa/inet.h>
#include <netdb.h>
#include <signal.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <cctype>
#include <chrono>
#include <cmath>
#include <cstdlib>
#include <filesystem>
#include <iostream>
#include <sstream>
#include <string_view>
#include <stdexcept>
#include <thread>
#include <utility>
#include <vector>

#include <nlohmann/json.hpp>

#include "naim/runtime/model_adapter.h"
#include "runtime/infer_control_support.h"
#include "runtime/local_runtime.h"

namespace naim::infer {

namespace {

namespace fs = std::filesystem;
using control_support::LoadActiveModel;
using nlohmann::json;

std::string ExpandUserPath(const std::string& value) {
  if (value.empty() || value[0] != '~') {
    return value;
  }
  const char* home = std::getenv("HOME");
  if (home == nullptr || *home == '\0') {
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

std::optional<std::string> ResolveGgufPath(const std::string& path_text) {
  if (path_text.empty()) {
    return std::nullopt;
  }
  const fs::path path(ExpandUserPath(path_text));
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

std::optional<std::string> OptionalString(const json& value, const char* key) {
  if (!value.contains(key) || !value.at(key).is_string()) {
    return std::nullopt;
  }
  return value.at(key).get<std::string>();
}

std::vector<std::string> SplitCommaSeparated(std::string_view text) {
  std::vector<std::string> result;
  std::string current;
  for (const char ch : text) {
    if (ch == ',') {
      if (!current.empty()) {
        result.push_back(current);
        current.clear();
      }
      continue;
    }
    current.push_back(ch);
  }
  if (!current.empty()) {
    result.push_back(current);
  }
  return result;
}

std::string ResolveExecutablePath(const char* env_name, const char* fallback) {
  const char* value = std::getenv(env_name);
  if (value != nullptr && *value != '\0') {
    return value;
  }
  return fallback;
}

std::string Trim(std::string value) {
  while (!value.empty() && std::isspace(static_cast<unsigned char>(value.front()))) {
    value.erase(value.begin());
  }
  while (!value.empty() && std::isspace(static_cast<unsigned char>(value.back()))) {
    value.pop_back();
  }
  return value;
}

std::optional<std::uint64_t> ParseKvCacheBytesLine(std::string_view line) {
  constexpr std::string_view kMarker = "KV buffer size =";
  const std::size_t marker = line.find(kMarker);
  if (marker == std::string_view::npos) {
    return std::nullopt;
  }

  std::string payload(line.substr(marker + kMarker.size()));
  payload = Trim(std::move(payload));
  if (payload.empty()) {
    return std::nullopt;
  }

  std::istringstream stream(payload);
  double amount = 0.0;
  std::string unit;
  if (!(stream >> amount >> unit) || !std::isfinite(amount) || amount < 0.0) {
    return std::nullopt;
  }

  while (!unit.empty() && !std::isalnum(static_cast<unsigned char>(unit.back()))) {
    unit.pop_back();
  }

  double multiplier = 0.0;
  if (unit == "B") {
    multiplier = 1.0;
  } else if (unit == "KiB") {
    multiplier = 1024.0;
  } else if (unit == "MiB") {
    multiplier = 1024.0 * 1024.0;
  } else if (unit == "GiB") {
    multiplier = 1024.0 * 1024.0 * 1024.0;
  } else if (unit == "KB") {
    multiplier = 1000.0;
  } else if (unit == "MB") {
    multiplier = 1000.0 * 1000.0;
  } else if (unit == "GB") {
    multiplier = 1000.0 * 1000.0 * 1000.0;
  } else {
    return std::nullopt;
  }

  return static_cast<std::uint64_t>(std::llround(amount * multiplier));
}

int ResolveParallelSlots(const RuntimeConfig& config) {
  return std::max(1, config.max_num_seqs);
}

int ResolveServerCtxSize(const RuntimeConfig& config) {
  const int per_slot_ctx = std::max(1, std::max(config.llama_ctx_size, config.max_model_len));
  const int parallel_slots = ResolveParallelSlots(config);
  return per_slot_ctx * parallel_slots;
}

std::string BuildRpcDeviceList(const RuntimeConfig& config) {
  const json members = config.worker_group.value("members", json::array());
  std::ostringstream out;
  std::size_t rpc_index = 0;
  for (const auto& member : members) {
    if (!member.is_object() || !member.value("enabled", true)) {
      continue;
    }
    const int rpc_port = member.value("rpc_port", 0);
    const std::string rpc_endpoint = member.value("rpc_endpoint", std::string{});
    if (rpc_port <= 0 && rpc_endpoint.empty()) {
      continue;
    }
    if (rpc_index > 0) {
      out << ",";
    }
    out << "RPC" << rpc_index;
    ++rpc_index;
  }
  return out.str();
}

bool CanConnectToEndpoint(std::string_view endpoint) {
  const std::size_t separator = endpoint.rfind(':');
  if (separator == std::string_view::npos || separator == 0 || separator + 1 >= endpoint.size()) {
    return false;
  }
  const std::string host(endpoint.substr(0, separator));
  const std::string service(endpoint.substr(separator + 1));
  addrinfo hints{};
  hints.ai_family = AF_UNSPEC;
  hints.ai_socktype = SOCK_STREAM;
  hints.ai_protocol = IPPROTO_TCP;
  addrinfo* results = nullptr;
  if (getaddrinfo(host.c_str(), service.c_str(), &hints, &results) != 0) {
    return false;
  }
  bool connected = false;
  for (addrinfo* current = results; current != nullptr; current = current->ai_next) {
    const int fd = socket(current->ai_family, current->ai_socktype, current->ai_protocol);
    if (fd < 0) {
      continue;
    }
    const int rc = connect(fd, current->ai_addr, current->ai_addrlen);
    close(fd);
    if (rc == 0) {
      connected = true;
      break;
    }
  }
  freeaddrinfo(results);
  return connected;
}

}  // namespace

LlamaRpcRuntime::LlamaRpcRuntime(
    const RuntimeConfig& config,
    std::string started_at,
    InferSignalService& signal_service)
    : config_(config),
      started_at_(std::move(started_at)),
      signal_service_(signal_service) {}

int LlamaRpcRuntime::Run() {
  signal_service_.RegisterHandlers();

  const std::string model_path = ResolveModelPath();
  const std::string rpc_servers = BuildRpcServerList();
  if (!WaitForRpcServersReady(rpc_servers, 60)) {
    throw std::runtime_error("timed out waiting for rpc workers: " + rpc_servers);
  }
  const std::string rpc_devices = BuildRpcDeviceList(config_);
  kv_cache_bytes_.reset();
  kv_cache_log_lines_.clear();

  int stderr_pipe[2] = {-1, -1};
  if (pipe(stderr_pipe) != 0) {
    throw std::runtime_error("failed to create llama-server stderr pipe");
  }

  pid_t child = fork();
  if (child < 0) {
    close(stderr_pipe[0]);
    close(stderr_pipe[1]);
    throw std::runtime_error("failed to fork llama-server process");
  }
  if (child == 0) {
    close(stderr_pipe[0]);
    if (dup2(stderr_pipe[1], STDERR_FILENO) < 0) {
      std::perror("dup2 stderr");
      _exit(127);
    }
    close(stderr_pipe[1]);
    const json active_model = LoadActiveModel(config_);
    naim::runtime::ModelIdentity model_identity =
        naim::runtime::ModelAdapter::IdentityFromActiveModelJson(active_model);
    if (model_identity.cached_runtime_model_path.empty()) {
      model_identity.cached_runtime_model_path = model_path;
    }
    std::vector<std::string> args = {
        ResolveExecutablePath("NAIM_LLAMA_SERVER_BIN", "/runtime/bin/llama-server"),
        "--host",
        "127.0.0.1",
        "--port",
        std::to_string(config_.llama_port),
        "--model",
        model_path,
        "--ctx-size",
        std::to_string(ResolveServerCtxSize(config_)),
        "--threads",
        std::to_string(std::max(1, config_.llama_threads)),
        "--gpu-layers",
        std::to_string(std::max(0, config_.llama_gpu_layers)),
        "--parallel",
        std::to_string(ResolveParallelSlots(config_)),
        "--cont-batching",
        "--fit",
        "off",
        "--rpc",
        rpc_servers,
    };
    naim::runtime::ModelAdapter::AdaptLaunchArgs(&args, model_identity);
    if (!rpc_devices.empty()) {
      args.push_back("--device");
      args.push_back(rpc_devices);
    }
    if (const auto served_model_name = OptionalString(active_model, "served_model_name");
        served_model_name.has_value() && !served_model_name->empty()) {
      args.push_back("--alias");
      args.push_back(*served_model_name);
    }
    {
      std::ostringstream command_line;
      for (std::size_t index = 0; index < args.size(); ++index) {
        if (index > 0) {
          command_line << ' ';
        }
        command_line << args[index];
      }
      std::cerr << "[naim-inferctl] launching llama-server: "
                << command_line.str() << std::endl;
    }
    std::vector<char*> argv;
    argv.reserve(args.size() + 1);
    for (auto& arg : args) {
      argv.push_back(arg.data());
    }
    argv.push_back(nullptr);
    execv(argv.front(), argv.data());
    std::perror("execv llama-server");
    _exit(127);
  }

  close(stderr_pipe[1]);
  StartServerLogPump(stderr_pipe[0]);
  child_pid_ = child;
  if (!EnsureLocalServerReady(120)) {
    StopServer();
    throw std::runtime_error(
        "timed out waiting for llama-server on 127.0.0.1:" + std::to_string(config_.llama_port));
  }

  LocalRuntime runtime(
      config_,
      "llama-rpc-head",
      started_at_,
      nullptr,
      signal_service_,
      false,
      UpstreamTarget{"127.0.0.1", config_.llama_port},
      [this]() { return CurrentKvCacheBytes(); });
  const int runtime_rc = runtime.Run();
  StopServer();
  return runtime_rc;
}

std::string LlamaRpcRuntime::ResolveModelPath() const {
  const json active_model = LoadActiveModel(config_);
  if (active_model.empty()) {
    throw std::runtime_error("llama-rpc backend requires an active model");
  }
  const std::vector<std::string> candidates = {
      active_model.value("cached_runtime_model_path", std::string{}),
      active_model.value("runtime_model_path", std::string{}),
      active_model.value("cached_local_model_path", std::string{}),
      active_model.value("model_path", std::string{}),
  };
  for (const auto& candidate : candidates) {
    if (const auto resolved = ResolveGgufPath(candidate); resolved.has_value()) {
      return *resolved;
    }
  }
  throw std::runtime_error("llama-rpc backend requires a resolvable local GGUF model");
}

std::string LlamaRpcRuntime::BuildRpcServerList() const {
  const json members = config_.worker_group.value("members", json::array());
  std::vector<std::string> endpoints;
  const char* local_node_name = std::getenv("NAIM_NODE_NAME");
  for (const auto& member : members) {
    if (!member.is_object() || !member.value("enabled", true)) {
      continue;
    }
    const std::string explicit_endpoint = member.value("rpc_endpoint", std::string{});
    if (!explicit_endpoint.empty()) {
      endpoints.push_back(explicit_endpoint);
      continue;
    }
    const std::string member_name = member.value("name", std::string{});
    const std::string node_name = member.value("node_name", std::string{});
    const int rpc_port = member.value("rpc_port", 0);
    if (rpc_port <= 0) {
      continue;
    }
    const bool colocated_with_primary_infer =
        member.value("colocated_with_primary_infer", false);
    std::string host;
    if (!member_name.empty() &&
        (colocated_with_primary_infer ||
         (!config_.primary_infer_node.empty() && node_name == config_.primary_infer_node) ||
         (local_node_name != nullptr && node_name == local_node_name))) {
      host = member_name;
    } else if (!node_name.empty()) {
      host = node_name;
    } else if (!member_name.empty()) {
      host = member_name;
    }
    if (host.empty()) {
      continue;
    }
    endpoints.push_back(host + ":" + std::to_string(rpc_port));
  }
  if (endpoints.empty()) {
    throw std::runtime_error("llama-rpc backend requires at least one rpc worker endpoint");
  }
  std::ostringstream out;
  for (std::size_t index = 0; index < endpoints.size(); ++index) {
    if (index > 0) {
      out << ",";
    }
    out << endpoints[index];
  }
  return out.str();
}

bool LlamaRpcRuntime::EnsureLocalServerReady(int timeout_sec) {
  const auto deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(timeout_sec);
  while (std::chrono::steady_clock::now() < deadline &&
         !signal_service_.StopRequested()) {
    if (child_pid_.has_value()) {
      int child_status = 0;
      const pid_t wait_result = waitpid(*child_pid_, &child_status, WNOHANG);
      if (wait_result == *child_pid_) {
        child_pid_.reset();
        return false;
      }
    }

    addrinfo hints{};
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_protocol = IPPROTO_TCP;
    addrinfo* results = nullptr;
    const std::string service = std::to_string(config_.llama_port);
    if (getaddrinfo("127.0.0.1", service.c_str(), &hints, &results) == 0) {
      for (addrinfo* current = results; current != nullptr; current = current->ai_next) {
        const int fd = socket(current->ai_family, current->ai_socktype, current->ai_protocol);
        if (fd < 0) {
          continue;
        }
        const int connected = connect(fd, current->ai_addr, current->ai_addrlen);
        close(fd);
        if (connected == 0) {
          freeaddrinfo(results);
          return true;
        }
      }
      freeaddrinfo(results);
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(250));
  }
  return false;
}

bool LlamaRpcRuntime::WaitForRpcServersReady(
    const std::string& rpc_servers,
    int timeout_sec) {
  const std::vector<std::string> endpoints = SplitCommaSeparated(rpc_servers);
  if (endpoints.empty()) {
    return false;
  }
  const auto deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(timeout_sec);
  while (std::chrono::steady_clock::now() < deadline &&
         !signal_service_.StopRequested()) {
    bool all_ready = true;
    for (const auto& endpoint : endpoints) {
      if (!CanConnectToEndpoint(endpoint)) {
        all_ready = false;
        break;
      }
    }
    if (all_ready) {
      return true;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(250));
  }
  return false;
}

void LlamaRpcRuntime::StartServerLogPump(int read_fd) {
  child_stderr_fd_ = read_fd;
  child_stderr_thread_ = std::thread([this, read_fd]() {
    std::string pending;
    char buffer[4096];
    while (true) {
      const ssize_t bytes_read = read(read_fd, buffer, sizeof(buffer));
      if (bytes_read == 0) {
        break;
      }
      if (bytes_read < 0) {
        if (errno == EINTR) {
          continue;
        }
        break;
      }

      std::cerr.write(buffer, bytes_read);
      std::cerr.flush();
      pending.append(buffer, static_cast<std::size_t>(bytes_read));

      std::size_t newline = std::string::npos;
      while ((newline = pending.find('\n')) != std::string::npos) {
        std::string line = pending.substr(0, newline);
        if (!line.empty() && line.back() == '\r') {
          line.pop_back();
        }
        ProcessServerLogLine(line);
        pending.erase(0, newline + 1);
      }
    }

    if (!pending.empty()) {
      if (!pending.empty() && pending.back() == '\r') {
        pending.pop_back();
      }
      ProcessServerLogLine(pending);
    }

    close(read_fd);
  });
}

void LlamaRpcRuntime::StopServerLogPump() {
  if (child_stderr_thread_.joinable()) {
    child_stderr_thread_.join();
  }
  child_stderr_fd_.reset();
}

void LlamaRpcRuntime::ProcessServerLogLine(const std::string& line) {
  const auto parsed = ParseKvCacheBytesLine(line);
  if (!parsed.has_value()) {
    return;
  }

  std::lock_guard<std::mutex> guard(metrics_mutex_);
  if (!kv_cache_log_lines_.insert(line).second) {
    return;
  }
  kv_cache_bytes_ = kv_cache_bytes_.value_or(0) + *parsed;
}

std::optional<std::uint64_t> LlamaRpcRuntime::CurrentKvCacheBytes() const {
  std::lock_guard<std::mutex> guard(metrics_mutex_);
  return kv_cache_bytes_;
}

void LlamaRpcRuntime::StopServer() {
  if (!child_pid_.has_value()) {
    StopServerLogPump();
    return;
  }
  const pid_t pid = *child_pid_;
  kill(pid, SIGTERM);
  const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(15);
  while (std::chrono::steady_clock::now() < deadline) {
    int child_status = 0;
    const pid_t wait_result = waitpid(pid, &child_status, WNOHANG);
    if (wait_result == pid) {
      child_pid_.reset();
      StopServerLogPump();
      return;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }
  kill(pid, SIGKILL);
  int child_status = 0;
  waitpid(pid, &child_status, 0);
  child_pid_.reset();
  StopServerLogPump();
}

}  // namespace naim::infer
