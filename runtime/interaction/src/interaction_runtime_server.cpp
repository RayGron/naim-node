#include "interaction/interaction_runtime_server.h"

#include <array>
#include <cctype>
#include <csignal>
#include <fstream>
#include <map>
#include <stdexcept>
#include <thread>

#include "http/controller_http_server_support.h"
#include "infra/controller_network_manager.h"
#include "interaction/interaction_completion_policy_support.h"
#include "interaction/interaction_context_compression_service.h"
#include "interaction/interaction_payload_builder.h"
#include "interaction/interaction_runtime_request_codec.h"
#include "interaction/interaction_text_post_processor.h"
#include "naim/state/state_json.h"
#include "naim/runtime/runtime_status.h"

namespace naim::interaction_runtime {

namespace {

std::atomic<bool>* g_stop_requested = nullptr;

void SignalHandler(int) {
  if (g_stop_requested != nullptr) {
    g_stop_requested->store(true);
  }
}

std::string LowercaseCopy(std::string value) {
  for (char& ch : value) {
    ch = static_cast<char>(std::tolower(static_cast<unsigned char>(ch)));
  }
  return value;
}

bool RequestWantsStream(const HttpRequest& request) {
  if (request.path == "/v1/chat/completions/stream") {
    return true;
  }
  try {
    const auto wrapped_request =
        naim::controller::InteractionRuntimeRequestCodec{}.Deserialize(request.body);
    if (wrapped_request.force_stream) {
      return true;
    }
    return wrapped_request.payload.value("stream", false);
  } catch (const std::exception&) {
  }
  try {
    const auto payload = nlohmann::json::parse(request.body);
    return payload.is_object() && payload.value("stream", false);
  } catch (const std::exception&) {
  }
  return false;
}

std::optional<std::string> FindRequestHeader(
    const HttpRequest& request,
    const std::string& key) {
  const auto it = request.headers.find(LowercaseCopy(key));
  if (it == request.headers.end() || it->second.empty()) {
    return std::nullopt;
  }
  return it->second;
}

std::map<std::string, std::string> BuildCompressionHeaders(
    const naim::controller::InteractionRequestContext& request_context) {
  std::map<std::string, std::string> headers;
  if (!request_context.payload.contains(naim::controller::kInteractionSessionContextStatePayloadKey) ||
      !request_context.payload.at(naim::controller::kInteractionSessionContextStatePayloadKey)
           .is_object()) {
    return headers;
  }
  const auto& context_state =
      request_context.payload.at(naim::controller::kInteractionSessionContextStatePayloadKey);
  if (!context_state.contains("context_compression") ||
      !context_state.at("context_compression").is_object()) {
    return headers;
  }
  const auto& compression = context_state.at("context_compression");
  headers["x-naim-context-compression-enabled"] =
      compression.value("enabled", false) ? "true" : "false";
  headers["x-naim-context-compression-status"] =
      compression.value("status", std::string("none"));
  headers["x-naim-dialog-estimate-before"] =
      std::to_string(compression.value("dialog_estimate_before", 0));
  headers["x-naim-dialog-estimate-after"] =
      std::to_string(compression.value("dialog_estimate_after", 0));
  headers["x-naim-context-compression-ratio"] =
      std::to_string(compression.value("compression_ratio", 1.0));
  return headers;
}

HttpResponse SanitizeJsonChatResponse(HttpResponse response) {
  if (response.status_code < 200 || response.status_code >= 300 || response.body.empty()) {
    return response;
  }
  nlohmann::json payload = nlohmann::json::parse(response.body, nullptr, false);
  if (payload.is_discarded() || !payload.is_object()) {
    return response;
  }
  const naim::controller::InteractionTextPostProcessor post_processor;
  if (payload.contains("choices") && payload.at("choices").is_array() &&
      !payload.at("choices").empty() && payload.at("choices").at(0).is_object()) {
    auto& first_choice = payload.at("choices").at(0);
    if (first_choice.contains("message") && first_choice.at("message").is_object()) {
      auto& message = first_choice.at("message");
      message["content"] = post_processor.ExtractInteractionText(
          nlohmann::json{{"choices", nlohmann::json::array({first_choice})}});
      response.body = payload.dump();
      return response;
    }
    if (first_choice.contains("text") && first_choice.at("text").is_string()) {
      first_choice["text"] = post_processor.SanitizeInteractionText(
          first_choice.at("text").get<std::string>());
      response.body = payload.dump();
    }
  }
  return response;
}

}  // namespace

InteractionRuntimeServer::InteractionRuntimeServer(InteractionRuntimeConfig config)
    : config_(std::move(config)) {}

InteractionRuntimeServer::~InteractionRuntimeServer() {
  RequestStop();
}

int InteractionRuntimeServer::Run() {
  std::filesystem::create_directories(config_.status_path.parent_path());
  WriteRuntimeStatus("starting", false);
  SetReadyFile(false);

  g_stop_requested = &stop_requested_;
  std::signal(SIGINT, SignalHandler);
  std::signal(SIGTERM, SignalHandler);

  listen_fd_ = naim::controller::ControllerNetworkManager::CreateListenSocket(
      config_.listen_host,
      config_.port);
  WriteRuntimeStatus("running", true);
  SetReadyFile(true);

  try {
    AcceptLoop();
  } catch (...) {
    WriteRuntimeStatus("stopped", false);
    SetReadyFile(false);
    naim::controller::ControllerNetworkManager::ShutdownAndCloseSocket(listen_fd_);
    listen_fd_ = naim::platform::kInvalidSocket;
    throw;
  }

  WriteRuntimeStatus("stopped", false);
  SetReadyFile(false);
  naim::controller::ControllerNetworkManager::ShutdownAndCloseSocket(listen_fd_);
  listen_fd_ = naim::platform::kInvalidSocket;
  return 0;
}

void InteractionRuntimeServer::RequestStop() {
  const bool was_requested = stop_requested_.exchange(true);
  if (!was_requested && naim::platform::IsSocketValid(listen_fd_)) {
    WriteRuntimeStatus("stopping", false);
    SetReadyFile(false);
    naim::controller::ControllerNetworkManager::ShutdownAndCloseSocket(listen_fd_);
  }
}

void InteractionRuntimeServer::AcceptLoop() {
  while (!stop_requested_.load()) {
    const auto client_fd = accept(listen_fd_, nullptr, nullptr);
    if (!naim::platform::IsSocketValid(client_fd)) {
      if (stop_requested_.load() || naim::platform::LastSocketErrorWasInterrupted()) {
        continue;
      }
      throw std::runtime_error(
          "accept failed: " + naim::controller::ControllerNetworkManager::SocketErrorMessage());
    }
    std::thread(&InteractionRuntimeServer::HandleClient, this, client_fd).detach();
  }
}

void InteractionRuntimeServer::HandleClient(naim::platform::SocketHandle client_fd) {
  std::string request_data;
  std::array<char, 8192> buffer{};
  std::size_t expected_request_bytes = 0;
  while (true) {
    const ssize_t read_count = recv(client_fd, buffer.data(), buffer.size(), 0);
    if (read_count <= 0) {
      break;
    }
    request_data.append(buffer.data(), static_cast<std::size_t>(read_count));
    if (expected_request_bytes == 0) {
      expected_request_bytes =
          naim::controller::ControllerHttpServerSupport::ExpectedRequestBytes(request_data);
    }
    if (expected_request_bytes != 0 && request_data.size() >= expected_request_bytes) {
      break;
    }
  }

  if (request_data.empty()) {
    naim::controller::ControllerNetworkManager::ShutdownAndCloseSocket(client_fd);
    return;
  }

  try {
    const HttpRequest request =
        naim::controller::ControllerHttpServerSupport::ParseHttpRequest(request_data);
    if (request.method == "POST" &&
        (request.path == "/v1/chat/completions" ||
         request.path == "/v1/chat/completions/stream")) {
      if (RequestWantsStream(request)) {
        ExecuteStream(client_fd, request);
        return;
      }
    }
    naim::controller::ControllerNetworkManager::SendHttpResponse(
        client_fd,
        HandleRequest(request));
  } catch (const std::exception& error) {
    naim::controller::ControllerNetworkManager::SendHttpResponse(
        client_fd,
        BuildJsonResponse(
            500,
            nlohmann::json{
                {"error", "internal_error"},
                {"message", error.what()},
            }));
  }
  naim::controller::ControllerNetworkManager::ShutdownAndCloseSocket(client_fd);
}

HttpResponse InteractionRuntimeServer::HandleRequest(const HttpRequest& request) {
  if (request.method == "GET") {
    return HandleGet(request);
  }
  if (request.method == "POST") {
    return HandlePost(request);
  }
  return BuildJsonResponse(
      405,
      nlohmann::json{{"error", "method_not_allowed"}, {"message", "method not allowed"}});
}

HttpResponse InteractionRuntimeServer::HandleGet(const HttpRequest& request) {
  const auto parts = SplitPath(request.path);
  if (parts.size() == 1 && parts[0] == "health") {
    return BuildJsonResponse(200, nlohmann::json{{"ok", true}, {"ready", true}});
  }
  if (parts.size() == 2 && parts[0] == "v1" && parts[1] == "models") {
    return SendControllerHttpRequest(UpstreamTarget(), "GET", "/models");
  }
  return BuildJsonResponse(
      404,
      nlohmann::json{{"error", "not_found"}, {"message", "route not found"}});
}

HttpResponse InteractionRuntimeServer::HandlePost(const HttpRequest& request) {
  if (request.path == "/v1/chat/completions" ||
      request.path == "/v1/chat/completions/stream") {
    return ExecuteNonStream(request);
  }
  return BuildJsonResponse(
      404,
      nlohmann::json{{"error", "not_found"}, {"message", "route not found"}});
}

HttpResponse InteractionRuntimeServer::ExecuteNonStream(const HttpRequest& request) {
  if (ShouldProxyRawRequestThroughController(request)) {
    return ProxyRawRequestThroughController(
        request,
        "/api/v1/planes/" + config_.plane_name + "/interaction/chat/completions");
  }
  auto execution = BuildRuntimeExecution(request);
  naim::controller::InteractionContextCompressionService().Apply(
      execution.resolution,
      &execution.request_context);
  const std::string upstream_body = naim::controller::BuildInteractionUpstreamBodyPayload(
      execution.resolution,
      execution.request_context.payload,
      execution.force_stream,
      execution.resolved_policy,
      execution.structured_output_json);
  HttpResponse response = SendControllerHttpRequest(
      UpstreamTarget(),
      "POST",
      "/chat/completions",
      upstream_body,
      {{"Accept", "application/json"}});
  for (const auto& [name, value] : BuildCompressionHeaders(execution.request_context)) {
    response.headers[name] = value;
  }
  return SanitizeJsonChatResponse(std::move(response));
}

void InteractionRuntimeServer::ExecuteStream(
    naim::platform::SocketHandle client_fd,
    const HttpRequest& request) {
  if (ShouldProxyRawRequestThroughController(request)) {
    ProxyRawStreamThroughController(
        client_fd,
        request,
        "/api/v1/planes/" + config_.plane_name + "/interaction/chat/completions/stream");
    return;
  }
  auto execution = BuildRuntimeExecution(request);
  execution.force_stream = true;
  naim::controller::InteractionContextCompressionService().Apply(
      execution.resolution,
      &execution.request_context);
  const std::string upstream_body = naim::controller::BuildInteractionUpstreamBodyPayload(
      execution.resolution,
      execution.request_context.payload,
      true,
      execution.resolved_policy,
      execution.structured_output_json);
  auto upstream = OpenInteractionStreamRequest(
      UpstreamTarget(),
      "interaction-runtime",
      upstream_body);
  if (!naim::controller::ControllerNetworkManager::SendSseHeaders(
          client_fd,
          BuildCompressionHeaders(execution.request_context))) {
    upstream.close();
    naim::controller::ControllerNetworkManager::ShutdownAndCloseSocket(client_fd);
    return;
  }
  if (!upstream.initial_body.empty() &&
      !naim::controller::ControllerNetworkManager::SendAll(client_fd, upstream.initial_body)) {
    upstream.close();
    naim::controller::ControllerNetworkManager::ShutdownAndCloseSocket(client_fd);
    return;
  }
  while (true) {
    const std::string chunk = upstream.read_next_chunk();
    if (chunk.empty()) {
      break;
    }
    if (!naim::controller::ControllerNetworkManager::SendAll(client_fd, chunk)) {
      break;
    }
  }
  upstream.close();
  naim::controller::ControllerNetworkManager::ShutdownAndCloseSocket(client_fd);
}

bool InteractionRuntimeServer::ShouldProxyRawRequestThroughController(
    const HttpRequest& request) const {
  if (TryBuildWrappedRuntimeExecution(request.body).has_value()) {
    return false;
  }
  return !config_.controller_url.empty();
}

HttpResponse InteractionRuntimeServer::ProxyRawRequestThroughController(
    const HttpRequest& request,
    const std::string& controller_path) const {
  std::vector<std::pair<std::string, std::string>> headers{
      {"Accept", "application/json"},
      {"Content-Type", "application/json"},
  };
  if (const auto token = FindRequestHeader(request, "x-naim-session-token");
      token.has_value()) {
    headers.emplace_back("X-Naim-Session-Token", *token);
  }
  if (const auto request_id = FindRequestHeader(request, "x-naim-request-id");
      request_id.has_value()) {
    headers.emplace_back("X-Naim-Request-Id", *request_id);
  }
  return SendControllerHttpRequest(
      ParseControllerEndpointTarget(config_.controller_url),
      "POST",
      controller_path,
      request.body,
      headers);
}

void InteractionRuntimeServer::ProxyRawStreamThroughController(
    naim::platform::SocketHandle client_fd,
    const HttpRequest& request,
    const std::string& controller_path) const {
  std::vector<std::pair<std::string, std::string>> headers{
      {"Accept", "text/event-stream"},
      {"Content-Type", "application/json"},
  };
  const std::string request_id =
      FindRequestHeader(request, "x-naim-request-id").value_or("interaction-runtime");
  if (const auto token = FindRequestHeader(request, "x-naim-session-token");
      token.has_value()) {
    headers.emplace_back("X-Naim-Session-Token", *token);
  }
  auto upstream = OpenInteractionStreamRequest(
      ParseControllerEndpointTarget(config_.controller_url),
      request_id,
      request.body,
      controller_path,
      headers);
  if (!naim::controller::ControllerNetworkManager::SendSseHeaders(client_fd, {})) {
    upstream.close();
    naim::controller::ControllerNetworkManager::ShutdownAndCloseSocket(client_fd);
    return;
  }
  if (!upstream.initial_body.empty() &&
      !naim::controller::ControllerNetworkManager::SendAll(client_fd, upstream.initial_body)) {
    upstream.close();
    naim::controller::ControllerNetworkManager::ShutdownAndCloseSocket(client_fd);
    return;
  }
  while (true) {
    const std::string chunk = upstream.read_next_chunk();
    if (chunk.empty()) {
      break;
    }
    if (!naim::controller::ControllerNetworkManager::SendAll(client_fd, chunk)) {
      break;
    }
  }
  upstream.close();
  naim::controller::ControllerNetworkManager::ShutdownAndCloseSocket(client_fd);
}

InteractionRuntimeServer::RuntimeExecution
InteractionRuntimeServer::BuildRuntimeExecution(const HttpRequest& request) const {
  if (const auto wrapped = TryBuildWrappedRuntimeExecution(request.body);
      wrapped.has_value()) {
    return *wrapped;
  }
  return BuildDirectRuntimeExecution(request);
}

std::optional<InteractionRuntimeServer::RuntimeExecution>
InteractionRuntimeServer::TryBuildWrappedRuntimeExecution(
    const std::string& body) const {
  if (body.empty()) {
    return std::nullopt;
  }
  try {
    const auto request =
        naim::controller::InteractionRuntimeRequestCodec{}.Deserialize(body);
    RuntimeExecution execution;
    execution.resolution.desired_state = request.desired_state;
    execution.resolution.status_payload = request.status_payload;
    execution.request_context.original_payload = request.payload;
    execution.request_context.payload = request.payload;
    execution.request_context.structured_output_json = request.structured_output_json;
    execution.resolved_policy = request.resolved_policy;
    execution.force_stream = request.force_stream;
    execution.structured_output_json = request.structured_output_json;
    return std::optional<RuntimeExecution>(std::move(execution));
  } catch (const std::exception&) {
    return std::nullopt;
  }
}

InteractionRuntimeServer::RuntimeExecution
InteractionRuntimeServer::BuildDirectRuntimeExecution(const HttpRequest& request) const {
  const nlohmann::json state_payload = LoadPlaneStatePayload();
  if (!state_payload.contains("desired_state") ||
      !state_payload.at("desired_state").is_object()) {
    throw std::runtime_error(
        "interaction-runtime plane state payload is missing desired_state");
  }

  const nlohmann::json payload =
      request.body.empty() ? nlohmann::json::object() : nlohmann::json::parse(request.body);
  if (!payload.is_object()) {
    throw std::runtime_error("interaction-runtime request body must be a JSON object");
  }

  RuntimeExecution execution;
  execution.resolution.desired_state =
      naim::DeserializeDesiredStateJson(state_payload.at("desired_state").dump());
  execution.resolution.status_payload = state_payload;
  execution.request_context.original_payload = payload;
  execution.request_context.payload = payload;
  execution.request_context.structured_output_json =
      payload.contains("response_format") && payload.at("response_format").is_object();
  execution.resolved_policy =
      naim::controller::InteractionCompletionPolicySupport{}.ResolvePolicy(
          execution.resolution.desired_state,
          execution.request_context.payload);
  execution.force_stream =
      request.path == "/v1/chat/completions/stream" || payload.value("stream", false);
  execution.structured_output_json =
      execution.request_context.structured_output_json;
  return execution;
}

nlohmann::json InteractionRuntimeServer::LoadPlaneStatePayload() const {
  if (const auto snapshot = LoadPlaneStatePayloadFromSnapshot(); snapshot.is_object()) {
    return snapshot;
  }
  if (config_.controller_url.empty()) {
    throw std::runtime_error("interaction-runtime controller_url is not configured");
  }
  const auto target = ParseControllerEndpointTarget(config_.controller_url);
  const auto response = SendControllerHttpRequest(
      target,
      "GET",
      "/api/v1/planes/" + config_.plane_name);
  if (response.status_code < 200 || response.status_code >= 300) {
    throw std::runtime_error(
        "interaction-runtime failed to load plane state for '" + config_.plane_name +
        "' from controller");
  }
  const auto payload =
      response.body.empty() ? nlohmann::json::object() : nlohmann::json::parse(response.body);
  if (!payload.is_object()) {
    throw std::runtime_error("interaction-runtime plane state response must be a JSON object");
  }
  return payload;
}

nlohmann::json InteractionRuntimeServer::LoadPlaneStatePayloadFromSnapshot() const {
  if (config_.control_root.empty()) {
    return nlohmann::json();
  }
  const std::filesystem::path snapshot_path =
      std::filesystem::path(config_.control_root) / "desired-state.v2.json";
  if (!std::filesystem::exists(snapshot_path)) {
    return nlohmann::json();
  }
  std::ifstream input(snapshot_path);
  if (!input.is_open()) {
    throw std::runtime_error(
        "interaction-runtime failed to open local desired-state snapshot: " +
        snapshot_path.string());
  }
  const std::string state_json{
      std::istreambuf_iterator<char>(input),
      std::istreambuf_iterator<char>()};
  if (state_json.empty()) {
    throw std::runtime_error(
        "interaction-runtime local desired-state snapshot is empty: " +
        snapshot_path.string());
  }
  const auto desired_state = naim::DeserializeDesiredStateJson(state_json);
  return nlohmann::json{
      {"plane_name", desired_state.plane_name},
      {"desired_state", nlohmann::json::parse(state_json)},
      {"ready", true},
      {"interaction_enabled", true},
      {"status", "ok"},
      {"reason", "local_desired_state_snapshot"},
      {"active_model_id",
       desired_state.bootstrap_model.has_value()
           ? nlohmann::json(desired_state.bootstrap_model->model_id)
           : nlohmann::json(nullptr)},
      {"served_model_name",
       desired_state.bootstrap_model.has_value()
           ? nlohmann::json(
                 desired_state.bootstrap_model->served_model_name.value_or(
                     desired_state.bootstrap_model->model_id))
           : nlohmann::json(nullptr)},
  };
}

HttpResponse InteractionRuntimeServer::BuildJsonResponse(
    int status_code,
    const nlohmann::json& payload) const {
  HttpResponse response;
  response.status_code = status_code;
  response.content_type = "application/json";
  response.body = payload.dump();
  return response;
}

naim::controller::ControllerEndpointTarget InteractionRuntimeServer::UpstreamTarget() const {
  return ParseControllerEndpointTarget(config_.upstream_base);
}

std::vector<std::string> InteractionRuntimeServer::SplitPath(const std::string& path) const {
  std::vector<std::string> parts;
  std::size_t start = 0;
  while (start < path.size()) {
    while (start < path.size() && path[start] == '/') {
      ++start;
    }
    if (start >= path.size()) {
      break;
    }
    const std::size_t end = path.find('/', start);
    parts.push_back(path.substr(start, end == std::string::npos ? std::string::npos : end - start));
    if (end == std::string::npos) {
      break;
    }
    start = end + 1;
  }
  return parts;
}

void InteractionRuntimeServer::WriteRuntimeStatus(
    const std::string& phase,
    bool ready) const {
  naim::RuntimeStatus status;
  status.plane_name = config_.plane_name;
  status.control_root = config_.control_root;
  status.controller_url = config_.controller_url;
  status.instance_name = config_.instance_name;
  status.instance_role = config_.instance_role;
  status.node_name = config_.node_name;
  status.runtime_backend = "interaction-runtime";
  status.runtime_phase = phase;
  status.gateway_listen = config_.listen_host + ":" + std::to_string(config_.port);
  status.gateway_health_url =
      "http://127.0.0.1:" + std::to_string(config_.port) + "/health";
  status.upstream_models_url = config_.upstream_base + "/models";
  status.ready = ready;
  status.gateway_ready = ready;
  status.inference_ready = ready;
  status.launch_ready = ready;
  status.active_model_ready = ready;
  naim::SaveRuntimeStatusJson(status, config_.status_path.string());
}

void InteractionRuntimeServer::SetReadyFile(bool ready) const {
  const std::filesystem::path ready_path("/tmp/naim-ready");
  std::error_code error;
  if (ready) {
    std::filesystem::create_directories(ready_path.parent_path(), error);
    std::ofstream ready_file(ready_path.string());
    ready_file << '\n';
  } else {
    std::filesystem::remove(ready_path, error);
  }
}

}  // namespace naim::interaction_runtime
