#include "devtool/http_client.h"

#include <algorithm>
#include <array>
#include <chrono>
#include <cmath>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <map>
#include <mutex>
#include <optional>
#include <regex>
#include <set>
#include <sstream>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include <nlohmann/json.hpp>

#ifndef _WIN32
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/wait.h>
#include <unistd.h>
#endif

namespace {

using naim::devtool::HttpRequest;
using naim::devtool::HttpResponse;
using nlohmann::json;

struct RunStats {
  int requests = 0;
  int prompt_tokens = 0;
  int completion_tokens = 0;
  int total_tokens = 0;
  int failures = 0;
};

struct CommandResult {
  int exit_code = 0;
  std::string output;
};

std::string RequireValue(
    const std::map<std::string, std::string>& options,
    const std::string& key) {
  const auto it = options.find(key);
  if (it == options.end() || it->second.empty()) {
    throw std::runtime_error("missing required option --" + key);
  }
  return it->second;
}

std::optional<std::string> OptionalValue(
    const std::map<std::string, std::string>& options,
    const std::string& key) {
  const auto it = options.find(key);
  if (it == options.end()) {
    return std::nullopt;
  }
  return it->second;
}

bool FlagEnabled(
    const std::map<std::string, std::string>& options,
    const std::string& key) {
  const auto value = OptionalValue(options, key);
  if (!value.has_value()) {
    return false;
  }
  return value.value() != "no" && value.value() != "false" && value.value() != "0";
}

std::vector<std::string> ParseList(const std::string& value) {
  std::vector<std::string> items;
  std::string current;
  std::istringstream stream(value);
  while (std::getline(stream, current, ',')) {
    if (!current.empty()) {
      items.push_back(current);
    }
  }
  return items;
}

json LoadJsonFile(const std::filesystem::path& path) {
  std::ifstream input(path);
  if (!input) {
    throw std::runtime_error("failed to open " + path.string());
  }
  json value;
  input >> value;
  return value;
}

void WriteJsonFile(const std::filesystem::path& path, const json& value) {
  std::filesystem::create_directories(path.parent_path());
  std::ofstream output(path);
  if (!output) {
    throw std::runtime_error("failed to write " + path.string());
  }
  output << value.dump(2) << '\n';
}

std::string Sanitize(const std::string& value) {
  return std::regex_replace(value, std::regex("[^A-Za-z0-9._-]+"), "_");
}

std::optional<std::filesystem::path> FindExistingFileByName(
    const std::filesystem::path& root,
    const std::string& filename) {
  if (filename.empty() || !std::filesystem::exists(root)) {
    return std::nullopt;
  }
  std::vector<std::filesystem::path> candidates;
  for (const auto& entry : std::filesystem::recursive_directory_iterator(root)) {
    if (entry.is_regular_file() && entry.path().filename() == filename) {
      candidates.push_back(entry.path());
    }
  }
  if (candidates.empty()) {
    return std::nullopt;
  }
  std::sort(candidates.begin(), candidates.end());
  return candidates.front();
}

std::string UrlFilename(const std::string& url) {
  const std::size_t scheme = url.find("://");
  const std::size_t path_start =
      scheme == std::string::npos ? url.find('/') : url.find('/', scheme + 3);
  if (path_start == std::string::npos) {
    return {};
  }
  const std::string path = url.substr(path_start);
  const std::size_t slash = path.find_last_of('/');
  return slash == std::string::npos ? path : path.substr(slash + 1);
}

int ParseInt(const std::string& value) {
  return std::stoi(value);
}

double ParseDouble(const std::string& value) {
  return std::stod(value);
}

std::string ShellEscape(const std::string& value) {
  std::string escaped = "'";
  for (const char ch : value) {
    if (ch == '\'') {
      escaped += "'\\''";
    } else {
      escaped.push_back(ch);
    }
  }
  escaped.push_back('\'');
  return escaped;
}

CommandResult RunCommandCapture(const std::string& command) {
  std::array<char, 4096> buffer{};
  std::string output;
  FILE* pipe = ::popen(command.c_str(), "r");
  if (pipe == nullptr) {
    throw std::runtime_error("failed to execute command");
  }
  while (fgets(buffer.data(), static_cast<int>(buffer.size()), pipe) != nullptr) {
    output.append(buffer.data());
  }
  const int status = ::pclose(pipe);
  CommandResult result;
  result.exit_code = WIFEXITED(status) ? WEXITSTATUS(status) : status;
  result.output = std::move(output);
  return result;
}

void AssertTrue(bool condition, const std::string& message) {
  if (!condition) {
    throw std::runtime_error(message);
  }
}

json ParseJsonBody(const HttpResponse& response) {
  if (response.body.empty()) {
    return nullptr;
  }
  return json::parse(response.body);
}

json PerformJsonRequest(
    const std::string& method,
    const std::string& url,
    const json& payload,
    int timeout_seconds,
    HttpResponse* response_out = nullptr) {
  HttpRequest request;
  request.method = method;
  request.url = url;
  request.timeout_seconds = timeout_seconds;
  if (!payload.is_null()) {
    request.body = payload.dump();
    request.headers["Content-Type"] = "application/json";
  }
  HttpResponse response = naim::devtool::PerformHttpRequest(request);
  if (response_out != nullptr) {
    *response_out = response;
  }
  return ParseJsonBody(response);
}

std::string BuildPrompt(
    const std::string& base_prompt,
    int request_index,
    bool unique_prompts) {
  if (!unique_prompts) {
    return base_prompt;
  }
  return base_prompt + "\n\nUnique request marker: " + std::to_string(request_index) +
         ".\nUse the marker only as context disambiguation and do not mention it in the answer.";
}

void ExecuteBenchmark(
    const std::vector<std::string>& base_urls,
    const std::string& request_path,
    const std::string& model,
    const std::string& prompt,
    int max_tokens,
    int concurrency,
    int requests_per_worker,
    int timeout_seconds,
    bool unique_prompts) {
  RunStats stats;
  std::mutex stats_mutex;
  std::mutex index_mutex;
  int next_request_index = 0;
  const auto started_at = std::chrono::steady_clock::now();
  std::vector<std::thread> threads;

  auto worker = [&]() {
    RunStats local;
    for (int request_count = 0; request_count < requests_per_worker; ++request_count) {
      int request_index = 0;
      {
        std::scoped_lock lock(index_mutex);
        request_index = next_request_index++;
      }
      const std::string base_url =
          base_urls[static_cast<std::size_t>(request_index) % base_urls.size()];
      const std::string request_prompt = BuildPrompt(prompt, request_index, unique_prompts);
      const json payload = {
          {"model", model},
          {"messages", json::array({{{"role", "user"}, {"content", request_prompt}}})},
          {"max_tokens", max_tokens},
      };
      try {
        HttpResponse response;
        const json body = PerformJsonRequest(
            "POST", base_url + request_path, payload, timeout_seconds, &response);
        if (response.status_code != 200) {
          ++local.failures;
          continue;
        }
        const json usage = body.value("usage", json::object());
        ++local.requests;
        local.prompt_tokens += usage.value("prompt_tokens", 0);
        local.completion_tokens += usage.value("completion_tokens", 0);
        local.total_tokens += usage.value("total_tokens", 0);
      } catch (...) {
        ++local.failures;
      }
    }
    std::scoped_lock lock(stats_mutex);
    stats.requests += local.requests;
    stats.prompt_tokens += local.prompt_tokens;
    stats.completion_tokens += local.completion_tokens;
    stats.total_tokens += local.total_tokens;
    stats.failures += local.failures;
  };

  for (int thread_index = 0; thread_index < std::max(1, concurrency); ++thread_index) {
    threads.emplace_back(worker);
  }
  for (auto& thread : threads) {
    thread.join();
  }

  const auto finished_at = std::chrono::steady_clock::now();
  const double elapsed_seconds = std::max(
      0.001,
      std::chrono::duration_cast<std::chrono::milliseconds>(finished_at - started_at).count() /
          1000.0);

  const json report = {
      {"base_urls", base_urls},
      {"model", model},
      {"concurrency", concurrency},
      {"requests_per_worker", requests_per_worker},
      {"unique_prompts", unique_prompts},
      {"elapsed_sec", std::round(elapsed_seconds * 1000.0) / 1000.0},
      {"successful_requests", stats.requests},
      {"failed_requests", stats.failures},
      {"prompt_tokens", stats.prompt_tokens},
      {"completion_tokens", stats.completion_tokens},
      {"total_tokens", stats.total_tokens},
      {"aggregate_tokens_per_sec", std::round((stats.total_tokens / elapsed_seconds) * 1000.0) / 1000.0},
      {"aggregate_completion_tokens_per_sec",
       std::round((stats.completion_tokens / elapsed_seconds) * 1000.0) / 1000.0},
  };
  std::cout << report.dump(2) << '\n';
  if (stats.requests <= 0) {
    throw std::runtime_error("benchmark completed with zero successful requests");
  }
}

std::vector<std::pair<std::string, std::string>> ParseSseEvents(const std::string& body) {
  std::vector<std::pair<std::string, std::string>> events;
  std::istringstream stream(body);
  std::string line;
  std::string event_name;
  std::vector<std::string> data_lines;
  while (std::getline(stream, line)) {
    if (!line.empty() && line.back() == '\r') {
      line.pop_back();
    }
    if (line.starts_with("event: ")) {
      event_name = line.substr(7);
    } else if (line.starts_with("data: ")) {
      data_lines.push_back(line.substr(6));
    } else if (line.empty()) {
      std::ostringstream data;
      for (std::size_t index = 0; index < data_lines.size(); ++index) {
        if (index > 0) {
          data << '\n';
        }
        data << data_lines[index];
      }
      events.emplace_back(event_name, data.str());
      event_name.clear();
      data_lines.clear();
    }
  }
  if (!event_name.empty() || !data_lines.empty()) {
    std::ostringstream data;
    for (std::size_t index = 0; index < data_lines.size(); ++index) {
      if (index > 0) {
        data << '\n';
      }
      data << data_lines[index];
    }
    events.emplace_back(event_name, data.str());
  }
  return events;
}

void AssertErrorEnvelope(const json& payload, const std::optional<std::string>& expected_code) {
  AssertTrue(payload.is_object(), "expected JSON object error payload");
  AssertTrue(payload.value("status", std::string()) == "error", "expected status=error");
  AssertTrue(payload.contains("error") && payload.at("error").is_object(), "expected error object");
  AssertTrue(payload.contains("request_id") && payload.at("request_id").is_string() &&
                 !payload.at("request_id").get<std::string>().empty(),
             "expected request_id");
  if (expected_code.has_value()) {
    AssertTrue(
        payload.at("error").value("code", std::string()) == *expected_code,
        "unexpected error.code");
  }
}

std::string WriteTempConfig(const json& payload) {
  const auto temp_dir = std::filesystem::temp_directory_path();
  const auto file_path =
      temp_dir / ("naim-devtool-maglev-" + std::to_string(::getpid()) + ".json");
  WriteJsonFile(file_path, payload);
  return file_path.string();
}

std::string MaybeRunMaglev(
    const std::string& controller_url,
    const std::string& plane_name,
    const std::string& maglev_repo) {
  const std::filesystem::path binary = std::filesystem::path(maglev_repo) / "target" /
                                       "linux-x64" / "debug" / "maglev";
  if (!std::filesystem::is_regular_file(binary)) {
    return "skipped: maglev binary not found";
  }
  const json config = {
      {"defaultBackendMode", "openai_compat"},
      {"defaultOpenAiCompatProfile", "naim-node-contract"},
      {"openaiCompat",
       {
           {"requestTimeoutMs", 120000},
           {"structuredRequestTimeoutMs", 180000},
           {"chatResponseProfile", {{"temperature", 0.2}, {"maxTokens", 128}}},
           {"jsonResponseProfile", {{"temperature", 0.2}, {"maxTokens", json::array({512, 1024})}}},
           {"promptProfiles",
            {
                {"chat", {{"systemPrompt", "Reply directly and briefly."}}},
                {"taskPlan",
                 {{"systemPrompt",
                   "Return JSON only. Schema: {\"summary\":string,\"steps\":[]}. No markdown."}}},
                {"edit", {{"systemPrompt", "Return JSON only. Schema: []. No markdown."}}},
                {"commit",
                 {{"systemPrompt", "Return JSON only. Schema: {\"title\":string,\"body\":string|null}."}}},
                {"deploy",
                 {{"systemPrompt",
                   "Return JSON only. Schema: {\"host\":string,\"repoPath\":string,\"branch\":string,"
                   "\"restartCommand\":string|null}."}}},
                {"status", {{"systemPrompt", "Return JSON only. Schema: {\"message\":string}."}}},
                {"repair", {{"systemPrompt", "Repair malformed JSON. Return JSON only."}}},
            }},
       }},
      {"openaiCompatProfiles",
       {{"naim-node-contract",
         {{"apiBaseUrl", controller_url + "/api/v1/planes/" + plane_name + "/interaction"},
          {"model", plane_name},
          {"chatModel", plane_name}}}}},
  };
  const std::string config_path = WriteTempConfig(config);
  const std::string command =
      "cd " + ShellEscape(maglev_repo) + " && " + ShellEscape(binary.string()) +
      " --config-file " + ShellEscape(config_path) +
      " --endpoint-profile naim-node-contract --task " +
      ShellEscape("Какая модель сейчас активна?");
  const CommandResult result = RunCommandCapture(command);
  std::filesystem::remove(config_path);
  AssertTrue(result.exit_code == 0, "maglev smoke failed:\n" + result.output);
  AssertTrue(!result.output.empty(), "maglev smoke returned empty output");
  return "ok";
}

void CommandFreePort() {
#ifdef _WIN32
  throw std::runtime_error("free-port is not implemented on Windows");
#else
  const int fd = ::socket(AF_INET, SOCK_STREAM, 0);
  if (fd < 0) {
    throw std::runtime_error("socket() failed");
  }
  sockaddr_in address{};
  address.sin_family = AF_INET;
  address.sin_port = 0;
  address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  if (::bind(fd, reinterpret_cast<sockaddr*>(&address), sizeof(address)) != 0) {
    ::close(fd);
    throw std::runtime_error("bind() failed");
  }
  socklen_t length = sizeof(address);
  if (::getsockname(fd, reinterpret_cast<sockaddr*>(&address), &length) != 0) {
    ::close(fd);
    throw std::runtime_error("getsockname() failed");
  }
  std::cout << ntohs(address.sin_port) << '\n';
  ::close(fd);
#endif
}

void CommandConfigSummary(const std::map<std::string, std::string>& options) {
  const auto payload = LoadJsonFile(RequireValue(options, "config"));
  const auto& paths = payload.value("paths", json::object());
  const std::filesystem::path storage_root =
      paths.value("storage_root", std::string("/var/lib/naim"));
  std::string model_cache_root = paths.value("model_cache_root", std::string{});
  if (model_cache_root.empty()) {
    model_cache_root = (storage_root.parent_path() / "models").string();
  }
  std::cout << storage_root.string() << '\n' << model_cache_root << '\n';
}

void CommandPrepareRunPlaneState(const std::map<std::string, std::string>& options) {
  json state = LoadJsonFile(RequireValue(options, "input"));
  auto bootstrap = state.value("bootstrap_model", json::object());
  const std::filesystem::path model_cache_root = RequireValue(options, "model-cache-root");
  const std::string plane_name = RequireValue(options, "plane-name");
  const bool force_cpu = OptionalValue(options, "force-cpu").value_or("no") == "yes";

  const std::string source_url = bootstrap.value("source_url", std::string{});
  const json source_urls = bootstrap.value("source_urls", json::array());
  if (!source_url.empty() && !bootstrap.contains("local_path")) {
    std::string filename = bootstrap.value("target_filename", std::string{});
    if (filename.empty()) {
      filename = UrlFilename(source_url);
    }
    if (!filename.empty()) {
      if (const auto existing = FindExistingFileByName(model_cache_root, filename); existing.has_value()) {
        bootstrap["local_path"] = existing->string();
      } else {
        const std::string model_dir_name = Sanitize(bootstrap.value("model_id", plane_name));
        bootstrap["local_path"] = (model_cache_root / model_dir_name / filename).string();
      }
    }
  }

  if (force_cpu) {
    state["inference"]["llama_gpu_layers"] = 0;
  }
  if (!source_urls.empty()) {
    bootstrap["source_urls"] = source_urls;
  }
  state["bootstrap_model"] = bootstrap;
  WriteJsonFile(RequireValue(options, "output"), state);
}

void CommandRunPlaneFields(const std::map<std::string, std::string>& options) {
  const auto state = LoadJsonFile(RequireValue(options, "state"));
  const auto bootstrap = state.value("bootstrap_model", json::object());
  const bool multipart =
      bootstrap.contains("source_urls") && bootstrap.at("source_urls").is_array() &&
      !bootstrap.at("source_urls").empty();
  std::cout << bootstrap.value("local_path", std::string{}) << '\n'
            << (multipart ? "yes" : "no") << '\n'
            << state.value("gateway", json::object()).value("listen_port", 0) << '\n'
            << state.value("inference", json::object()).value("runtime_engine", "llama.cpp")
            << '\n'
            << bootstrap.value("model_id", std::string{}) << '\n'
            << bootstrap.value("source_url", std::string{}) << '\n';
}

void CommandWriteApplyPayload(const std::map<std::string, std::string>& options) {
  const auto desired_state = LoadJsonFile(RequireValue(options, "desired-state"));
  const json payload = {
      {"desired_state", desired_state},
      {"artifacts_root", RequireValue(options, "artifacts-root")},
  };
  WriteJsonFile(RequireValue(options, "output"), payload);
}

void WriteWorkerBundle(
    const std::filesystem::path& repo_root,
    const std::filesystem::path& bundle_root,
    const std::vector<std::string>& workers,
    int worker_private_disk_gb) {
  std::filesystem::create_directories(bundle_root / "workers");
  for (const auto& worker_name : workers) {
    auto worker =
        LoadJsonFile(repo_root / "config" / "demo-plane" / "workers" / (worker_name + ".json"));
    worker["private_disk_gb"] = worker_private_disk_gb;
    WriteJsonFile(bundle_root / "workers" / (worker_name + ".json"), worker);
  }
}

void CommandPrepareDemoBundle(const std::map<std::string, std::string>& options) {
  const std::filesystem::path repo_root = RequireValue(options, "repo-root");
  const std::filesystem::path output = RequireValue(options, "output");
  const int shared_disk_gb = ParseInt(RequireValue(options, "shared-disk-gb"));
  const int infer_private_disk_gb = ParseInt(RequireValue(options, "infer-private-disk-gb"));
  const int worker_private_disk_gb = ParseInt(RequireValue(options, "worker-private-disk-gb"));
  const std::vector<std::string> workers =
      ParseList(OptionalValue(options, "workers").value_or("worker-a,worker-b"));

  auto plane = LoadJsonFile(repo_root / "config" / "demo-plane" / "plane.json");
  plane["shared_disk_gb"] = shared_disk_gb;
  if (const auto plane_name = OptionalValue(options, "plane-name"); plane_name.has_value()) {
    plane["name"] = *plane_name;
  }
  if (const auto control_root = OptionalValue(options, "control-root"); control_root.has_value()) {
    plane["control_root"] = *control_root;
  }
  WriteJsonFile(output / "plane.json", plane);

  auto infer = LoadJsonFile(repo_root / "config" / "demo-plane" / "infer.json");
  infer["private_disk_gb"] = infer_private_disk_gb;
  if (const auto infer_node = OptionalValue(options, "infer-node"); infer_node.has_value()) {
    infer["node"] = *infer_node;
  }
  WriteJsonFile(output / "infer.json", infer);

  WriteWorkerBundle(repo_root, output, workers, worker_private_disk_gb);
}

void CommandRewriteInferRuntimeConfig(const std::map<std::string, std::string>& options) {
  auto payload = LoadJsonFile(RequireValue(options, "input"));
  payload["plane"]["control_root"] = RequireValue(options, "control-root");
  payload["control"]["root"] = RequireValue(options, "control-root");
  payload["inference"]["models_root"] = RequireValue(options, "models-root");
  payload["inference"]["gguf_cache_dir"] = RequireValue(options, "gguf-cache-dir");
  payload["inference"]["infer_log_dir"] = RequireValue(options, "infer-log-dir");
  WriteJsonFile(RequireValue(options, "output"), payload);
}

void CommandPrepareLlamaRpcReplicas(const std::map<std::string, std::string>& options) {
  const std::filesystem::path bench_root = RequireValue(options, "bench-root");
  const std::string model_path = RequireValue(options, "model-path");
  const std::string served_model = RequireValue(options, "served-model");
  const int ctx_size = ParseInt(RequireValue(options, "ctx-size"));
  const int threads = ParseInt(RequireValue(options, "threads"));
  const int gpu_layers = ParseInt(RequireValue(options, "gpu-layers"));
  const int replica_count = ParseInt(RequireValue(options, "replica-count"));
  const int base_port = ParseInt(RequireValue(options, "base-port"));
  const int rpc_base_port = ParseInt(RequireValue(options, "rpc-base-port"));
  const int max_num_seqs = ParseInt(RequireValue(options, "max-num-seqs"));
  const double gpu_memory_utilization =
      ParseDouble(RequireValue(options, "gpu-memory-utilization"));

  for (int index = 0; index < replica_count; ++index) {
    const std::filesystem::path replica_root = bench_root / ("replica-" + std::to_string(index));
    const std::filesystem::path control_root = replica_root / "control";
    std::filesystem::create_directories(control_root / "worker-group");
    std::filesystem::create_directories(replica_root / "worker-private");
    std::filesystem::create_directories(replica_root / "infer-logs");

    const json active_model = {
        {"model_id", "Qwen/Qwen3.5-9B"},
        {"served_model_name", served_model},
        {"cached_runtime_model_path", model_path},
        {"cached_local_model_path", model_path},
        {"runtime_model_path", model_path},
        {"model_path", model_path},
    };
    WriteJsonFile(control_root / "active-model.json", active_model);
    WriteJsonFile(control_root / "gateway-plan.json", json{{"version", 1}, {"status", "applied"}});

    const int gateway_port = base_port + index * 100;
    const int llama_port = gateway_port + 1;
    const int api_port = gateway_port + 2;
    const int rpc_port = rpc_base_port + index * 100;
    const int rendezvous_port = 29500 + index;

    const json config = {
        {"plane", {{"name", "llama-rpc-replica-" + std::to_string(index)}, {"control_root", control_root.string()}}},
        {"control", {{"root", control_root.string()}, {"controller_url", "http://127.0.0.1:18080"}}},
        {"gpu_nodes", json::array()},
        {"serving_workers", json::array()},
        {"inference",
         {{"primary_infer_node", "local-hostd"},
          {"runtime_engine", "llama.cpp"},
          {"distributed_backend", "llama_rpc"},
          {"data_parallel_mode", "off"},
          {"data_parallel_lb_mode", "external"},
          {"api_server_count", 1},
          {"worker_group_id", "llama-rpc-group-" + std::to_string(index)},
          {"worker_selection_policy", "prefer-free-then-share"},
          {"net_if", "lo"},
          {"models_root", (replica_root / "models").string()},
          {"model_cache_dir", (replica_root / "models" / "cache").string()},
          {"gguf_cache_dir", std::filesystem::path(model_path).parent_path().string()},
          {"infer_log_dir", (replica_root / "infer-logs").string()},
          {"api_port", api_port},
          {"llama_port", llama_port},
          {"max_model_len", ctx_size},
          {"max_num_seqs", max_num_seqs},
          {"gpu_memory_utilization", gpu_memory_utilization},
          {"llama_ctx_size", ctx_size},
          {"llama_threads", threads},
          {"llama_gpu_layers", gpu_layers},
          {"rendezvous_port", rendezvous_port}}},
        {"worker_group",
         {{"group_id", "llama-rpc-group-" + std::to_string(index)},
          {"infer_instance_name", "infer-llama-rpc-replica-" + std::to_string(index)},
          {"distributed_backend", "llama_rpc"},
          {"rendezvous_host", "infer-llama-rpc-replica-" + std::to_string(index)},
          {"rendezvous_port", rendezvous_port},
          {"expected_workers", 1},
          {"worker_selection_policy", "prefer-free-then-share"},
          {"members",
           json::array({{
               {"name", "worker-" + std::to_string(index)},
               {"instance_name", "worker-" + std::to_string(index)},
               {"node_name", "local-hostd"},
               {"gpu_device", ""},
               {"rank", 0},
               {"replica_group_id", "llama-rpc-group-" + std::to_string(index)},
               {"replica_index", 0},
               {"replica_size", 1},
               {"replica_leader", true},
               {"data_parallel_rank", 0},
               {"data_parallel_size", 1},
               {"data_parallel_size_local", 1},
               {"data_parallel_start_rank", 0},
               {"data_parallel_api_endpoint", false},
               {"data_parallel_head_address", ""},
               {"data_parallel_rpc_port", rpc_port},
               {"rpc_port", rpc_port},
               {"rpc_endpoint", "127.0.0.1:" + std::to_string(rpc_port)},
               {"colocated_with_primary_infer", true},
               {"gpu_fraction", 1.0},
               {"share_mode", "exclusive"},
               {"priority", 100},
               {"preemptible", false},
               {"enabled", true},
               {"leader", true},
           }})}}},
        {"gateway",
         {{"listen_host", "127.0.0.1"},
          {"listen_port", gateway_port},
          {"server_name", "llama-rpc-replica-" + std::to_string(index) + ".local"}}},
    };
    WriteJsonFile(replica_root / "infer-runtime.json", config);
  }
}

void CommandBenchmarkOpenAiMultiBase(const std::map<std::string, std::string>& options) {
  ExecuteBenchmark(
      ParseList(RequireValue(options, "base-urls")),
      "/v1/chat/completions",
      RequireValue(options, "model"),
      OptionalValue(options, "prompt")
          .value_or("Reply with a concise paragraph about throughput benchmarking."),
      ParseInt(OptionalValue(options, "max-tokens").value_or("128")),
      ParseInt(OptionalValue(options, "concurrency").value_or("4")),
      ParseInt(OptionalValue(options, "requests-per-worker").value_or("4")),
      ParseInt(OptionalValue(options, "timeout").value_or("600")),
      FlagEnabled(options, "unique-prompts"));
}

void CommandBenchmarkDataParallelThroughput(const std::map<std::string, std::string>& options) {
  const std::string controller = RequireValue(options, "controller");
  const std::string plane = RequireValue(options, "plane");
  const std::string url =
      controller.substr(controller.size() - 1) == "/"
          ? controller + "api/v1/planes/" + plane + "/interaction"
          : controller + "/api/v1/planes/" + plane + "/interaction";
  ExecuteBenchmark(
      {url},
      std::string(),
      RequireValue(options, "model"),
      OptionalValue(options, "prompt")
          .value_or("Reply with a concise paragraph about throughput benchmarking."),
      ParseInt(OptionalValue(options, "max-tokens").value_or("128")),
      ParseInt(OptionalValue(options, "concurrency").value_or("4")),
      ParseInt(OptionalValue(options, "requests-per-worker").value_or("4")),
      ParseInt(OptionalValue(options, "timeout").value_or("600")),
      false);
}

void CommandBenchmarkDataParallelDiagnostic(const std::map<std::string, std::string>& options) {
  const std::string controller = RequireValue(options, "controller");
  const std::string plane = RequireValue(options, "plane");
  const std::string url =
      controller.substr(controller.size() - 1) == "/"
          ? controller + "api/v1/planes/" + plane + "/interaction"
          : controller + "/api/v1/planes/" + plane + "/interaction";
  ExecuteBenchmark(
      {url},
      std::string(),
      RequireValue(options, "model"),
      OptionalValue(options, "prompt")
          .value_or("Reply with a concise paragraph about throughput benchmarking."),
      ParseInt(OptionalValue(options, "max-tokens").value_or("128")),
      ParseInt(OptionalValue(options, "concurrency").value_or("4")),
      ParseInt(OptionalValue(options, "requests-per-worker").value_or("4")),
      ParseInt(OptionalValue(options, "timeout").value_or("600")),
      FlagEnabled(options, "unique-prompts"));
}

void CommandCheckExternalInferenceContract(const std::map<std::string, std::string>& options) {
  const std::string controller_url =
      OptionalValue(options, "controller-url").value_or("http://127.0.0.1:18080");
  const std::string plane = RequireValue(options, "plane");
  const auto compute_plane = OptionalValue(options, "compute-plane");
  const auto maglev_repo = OptionalValue(options, "maglev-repo");

  const std::string status_url =
      controller_url + "/api/v1/planes/" + plane + "/interaction/status";
  const std::string models_url =
      controller_url + "/api/v1/planes/" + plane + "/interaction/models";
  const std::string chat_url =
      controller_url + "/api/v1/planes/" + plane + "/interaction/chat/completions";
  const std::string stream_url =
      controller_url + "/api/v1/planes/" + plane + "/interaction/chat/completions/stream";

  HttpResponse response;
  json payload = PerformJsonRequest("GET", status_url, nullptr, 60, &response);
  AssertTrue(response.status_code == 200, "status endpoint failed");
  AssertTrue(payload.contains("ready"), "status missing ready");
  AssertTrue(payload.contains("degraded"), "status missing degraded");
  AssertTrue(payload.contains("reason") && payload.at("reason").is_string() &&
                 !payload.at("reason").get<std::string>().empty(),
             "status missing reason");
  AssertTrue(payload.contains("request_id"), "status missing request_id");
  AssertTrue(payload.contains("naim") && payload.at("naim").is_object(), "status missing naim metadata");
  AssertTrue(
      response.headers.find("x-naim-request-id") != response.headers.end(),
      "status header missing request id");

  payload = PerformJsonRequest("GET", models_url, nullptr, 60, &response);
  AssertTrue(response.status_code == 200, "models endpoint failed");
  AssertTrue(payload.contains("data") && payload.at("data").is_array(), "models payload missing data list");
  AssertTrue(payload.contains("request_id"), "models missing request_id");
  AssertTrue(payload.contains("naim") && payload.at("naim").is_object(), "models missing naim metadata");
  AssertTrue(
      response.headers.find("x-naim-request-id") != response.headers.end(),
      "models header missing request id");
  const std::string active_model = payload.at("data").at(0).at("id").get<std::string>();

  payload = PerformJsonRequest(
      "POST",
      chat_url,
      json{{"model", active_model},
           {"messages",
            json::array({{{"role", "user"},
                          {"content", "Назови активную модель одним коротким предложением."}}})}},
      180,
      &response);
  AssertTrue(response.status_code == 200, "chat completion failed");
  AssertTrue(payload.contains("request_id"), "chat missing request_id");
  AssertTrue(payload.contains("session") && payload.at("session").is_object(), "chat missing session metadata");
  AssertTrue(payload.contains("naim") && payload.at("naim").is_object(), "chat missing naim metadata");
  AssertTrue(payload.contains("choices") && !payload.at("choices").empty(), "chat missing choices");
  AssertTrue(
      response.headers.find("x-naim-request-id") != response.headers.end(),
      "chat header missing request id");

  payload = PerformJsonRequest(
      "POST",
      chat_url,
      json{{"model", active_model + "-mismatch"},
           {"messages", json::array({{{"role", "user"}, {"content", "Hi"}}})}},
      60,
      &response);
  AssertTrue(response.status_code == 409, "model mismatch expected 409");
  AssertErrorEnvelope(payload, "model_mismatch");

  HttpRequest stream_request;
  stream_request.method = "POST";
  stream_request.url = stream_url;
  stream_request.timeout_seconds = 180;
  stream_request.headers["Content-Type"] = "application/json";
  stream_request.headers["Accept"] = "text/event-stream";
  stream_request.body = json{
      {"messages", json::array({{{"role", "user"}, {"content", "Кратко объясни, что ты умеешь."}}})}}
                            .dump();
  response = naim::devtool::PerformHttpRequest(stream_request);
  const auto events = ParseSseEvents(response.body);
  std::vector<std::string> event_names;
  for (const auto& event : events) {
    if (!event.first.empty()) {
      event_names.push_back(event.first);
    }
  }
  AssertTrue(
      std::find(event_names.begin(), event_names.end(), "session_started") != event_names.end(),
      "stream missing session_started");
  AssertTrue(
      std::find(event_names.begin(), event_names.end(), "segment_started") != event_names.end(),
      "stream missing segment_started");
  AssertTrue(std::find(event_names.begin(), event_names.end(), "delta") != event_names.end(), "stream missing delta");
  AssertTrue(
      std::find(event_names.begin(), event_names.end(), "session_complete") != event_names.end() ||
          std::find(event_names.begin(), event_names.end(), "session_failed") != event_names.end(),
      "stream missing terminal session event");
  AssertTrue(!events.empty() && events.back().second == "[DONE]", "stream missing DONE marker");

  payload = PerformJsonRequest(
      "POST",
      chat_url,
      json{{"messages",
            json::array({{{"role", "user"}, {"content", "Return exactly {\"message\":\"ok\"}."}}})},
           {"response_format", {{"type", "json_object"}}}},
      180,
      &response);
  AssertTrue(response.status_code == 200, "structured output request failed");
  AssertTrue(
      payload.value("structured_output", json::object()).value("valid", false),
      "structured output not marked valid");
  AssertTrue(
      payload.value("structured_output", json::object()).contains("json") &&
          payload.at("structured_output").at("json").is_object(),
      "structured output missing parsed json");

  payload = PerformJsonRequest(
      "POST",
      chat_url,
      json{{"messages", json::array({{{"role", "user"}, {"content", "Hi"}}})},
           {"response_format", {{"type", "json_schema"}}}},
      60,
      &response);
  AssertTrue(response.status_code == 400, "unsupported response_format expected 400");
  AssertErrorEnvelope(payload, "unsupported_response_format");

  payload = PerformJsonRequest(
      "POST",
      chat_url,
      json{{"messages", json::array({{{"role", "user"}, {"content", "Hi"}}})},
           {"tools", json::array({{{"type", "function"},
                                    {"function", {{"name", "x"}, {"parameters", json::object()}}}}})}},
      60,
      &response);
  AssertTrue(response.status_code == 400, "unsupported tools expected 400");
  AssertErrorEnvelope(payload, "unsupported_field");

  payload = PerformJsonRequest(
      "POST",
      controller_url + "/api/v1/planes/does-not-exist/interaction/chat/completions",
      json{{"messages", json::array({{{"role", "user"}, {"content", "Hi"}}})}},
      60,
      &response);
  AssertTrue(response.status_code == 404, "unknown plane expected 404");
  AssertTrue(
      response.headers.find("x-naim-request-id") != response.headers.end(),
      "unknown plane response missing request id header");
  AssertErrorEnvelope(payload, "plane_not_found");

  if (compute_plane.has_value()) {
    payload = PerformJsonRequest(
        "POST",
        controller_url + "/api/v1/planes/" + *compute_plane + "/interaction/chat/completions",
        json{{"messages", json::array({{{"role", "user"}, {"content", "Hi"}}})}},
        60,
        &response);
    AssertTrue(response.status_code == 409, "compute plane expected 409");
    AssertErrorEnvelope(payload, "interaction_disabled");
  }

  if (maglev_repo.has_value() && !maglev_repo->empty()) {
    std::cout << "maglev_smoke=" << MaybeRunMaglev(controller_url, plane, *maglev_repo) << '\n';
  }

  std::cout << "external inference contract checks: ok\n";
}

std::pair<std::string, std::map<std::string, std::string>> ParseArgs(int argc, char** argv) {
  if (argc < 2) {
    throw std::runtime_error("missing command");
  }
  const std::string command = argv[1];
  std::map<std::string, std::string> options;
  for (int index = 2; index < argc; ++index) {
    std::string arg = argv[index];
    if (!arg.starts_with("--")) {
      throw std::runtime_error("unexpected argument: " + arg);
    }
    arg.erase(0, 2);
    std::string value = "yes";
    const std::size_t separator = arg.find('=');
    if (separator != std::string::npos) {
      value = arg.substr(separator + 1);
      arg = arg.substr(0, separator);
    } else if (index + 1 < argc && !std::string_view(argv[index + 1]).starts_with("--")) {
      value = argv[++index];
    }
    options[arg] = value;
  }
  return {command, options};
}

}  // namespace

int main(int argc, char** argv) {
  try {
    const auto [command, options] = ParseArgs(argc, argv);
    if (command == "free-port") {
      CommandFreePort();
      return 0;
    }
    if (command == "config-summary") {
      CommandConfigSummary(options);
      return 0;
    }
    if (command == "prepare-run-plane-state") {
      CommandPrepareRunPlaneState(options);
      return 0;
    }
    if (command == "run-plane-fields") {
      CommandRunPlaneFields(options);
      return 0;
    }
    if (command == "write-apply-payload") {
      CommandWriteApplyPayload(options);
      return 0;
    }
    if (command == "prepare-demo-bundle") {
      CommandPrepareDemoBundle(options);
      return 0;
    }
    if (command == "rewrite-infer-runtime-config") {
      CommandRewriteInferRuntimeConfig(options);
      return 0;
    }
    if (command == "prepare-llama-rpc-replicas") {
      CommandPrepareLlamaRpcReplicas(options);
      return 0;
    }
    if (command == "benchmark-openai-multi-base") {
      CommandBenchmarkOpenAiMultiBase(options);
      return 0;
    }
    if (command == "benchmark-data-parallel-throughput") {
      CommandBenchmarkDataParallelThroughput(options);
      return 0;
    }
    if (command == "benchmark-data-parallel-diagnostic") {
      CommandBenchmarkDataParallelDiagnostic(options);
      return 0;
    }
    if (command == "check-external-inference-contract") {
      CommandCheckExternalInferenceContract(options);
      return 0;
    }
    throw std::runtime_error("unknown command: " + command);
  } catch (const std::exception& error) {
    std::cerr << "naim-devtool: " << error.what() << '\n';
    return 1;
  }
}
