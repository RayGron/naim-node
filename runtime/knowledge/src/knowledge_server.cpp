#include "knowledge/knowledge_server.h"

#include <array>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <stdexcept>
#include <thread>

#include "http/controller_http_server_support.h"
#include "infra/controller_network_manager.h"
#include "knowledge/knowledge_server_signal_handler.h"

namespace naim::knowledge_runtime {

KnowledgeServer::ApiError::ApiError(int status, std::string code, std::string message)
    : std::runtime_error(std::move(message)), status_(status), code_(std::move(code)) {}

KnowledgeServer::KnowledgeServer(KnowledgeRuntimeConfig config)
    : config_(std::move(config)), store_(config_.store_path) {}

KnowledgeServer::~KnowledgeServer() {
  RequestStop();
}

int KnowledgeServer::Run() {
  std::filesystem::create_directories(config_.store_path);
  std::filesystem::create_directories(config_.status_path.parent_path());
  SetReadyFile(false);
  store_.Open();
  WriteRuntimeStatus("starting", false);

  KnowledgeServerSignalHandler::Install(stop_requested_);

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

void KnowledgeServer::RequestStop() {
  const bool was_requested = stop_requested_.exchange(true);
  if (!was_requested && naim::platform::IsSocketValid(listen_fd_)) {
    WriteRuntimeStatus("stopping", false);
    SetReadyFile(false);
    naim::controller::ControllerNetworkManager::ShutdownAndCloseSocket(listen_fd_);
  }
}

void KnowledgeServer::AcceptLoop() {
  while (!stop_requested_.load()) {
    naim::platform::PollFd fd_state{};
    fd_state.fd = listen_fd_;
    fd_state.events = POLLIN;
    const int poll_result = naim::platform::Poll(&fd_state, 1, 250);
    if (poll_result < 0) {
      if (stop_requested_.load() || naim::platform::LastSocketErrorWasInterrupted()) {
        continue;
      }
      throw std::runtime_error(
          "poll failed: " + naim::controller::ControllerNetworkManager::SocketErrorMessage());
    }
    if (poll_result == 0 || (fd_state.revents & POLLIN) == 0) {
      continue;
    }

    const auto client_fd = accept(listen_fd_, nullptr, nullptr);
    if (!naim::platform::IsSocketValid(client_fd)) {
      if (stop_requested_.load() || naim::platform::LastSocketErrorWasInterrupted()) {
        continue;
      }
      throw std::runtime_error(
          "accept failed: " + naim::controller::ControllerNetworkManager::SocketErrorMessage());
    }
    std::thread(&KnowledgeServer::HandleClient, this, client_fd).detach();
  }
}

void KnowledgeServer::HandleClient(naim::platform::SocketHandle client_fd) {
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

  if (!request_data.empty()) {
    try {
      const HttpRequest request =
          naim::controller::ControllerHttpServerSupport::ParseHttpRequest(request_data);
      naim::controller::ControllerNetworkManager::SendHttpResponse(
          client_fd,
          HandleRequest(request));
    } catch (const ApiError& error) {
      naim::controller::ControllerNetworkManager::SendHttpResponse(
          client_fd,
          BuildJsonResponse(
              error.status(),
              nlohmann::json{{"error", error.code()}, {"message", error.what()}}));
    } catch (const std::exception& error) {
      naim::controller::ControllerNetworkManager::SendHttpResponse(
          client_fd,
          BuildJsonResponse(
              500,
              nlohmann::json{{"error", "internal_error"}, {"message", error.what()}}));
    }
  }
  naim::controller::ControllerNetworkManager::ShutdownAndCloseSocket(client_fd);
}

HttpResponse KnowledgeServer::HandleRequest(const HttpRequest& request) {
  if (request.method == "GET") {
    return HandleGet(request);
  }
  if (request.method == "POST") {
    return HandlePost(request);
  }
  if (request.method == "PUT") {
    return HandlePut(request);
  }
  throw ApiError(405, "method_not_allowed", "method not allowed");
}

HttpResponse KnowledgeServer::HandleGet(const HttpRequest& request) {
  const auto parts = SplitPath(request.path);
  if (parts.size() == 1 && parts[0] == "health") {
    return BuildJsonResponse(200, nlohmann::json{{"ok", true}, {"ready", true}});
  }
  if (parts.size() == 2 && parts[0] == "v1" && parts[1] == "status") {
    return BuildJsonResponse(200, store_.Status(config_.service_id));
  }
  if (parts.size() == 3 && parts[0] == "v1" && parts[1] == "blocks") {
    const auto result = store_.ReadBlock(parts[2]);
    if (result.contains("error")) {
      throw ApiError(404, result.value("error", "not_found"), result.value("message", "not found"));
    }
    return BuildJsonResponse(200, result);
  }
  if (parts.size() == 3 && parts[0] == "v1" && parts[1] == "heads") {
    return BuildJsonResponse(200, store_.ResolveHead(parts[2]));
  }
  if (parts.size() == 4 && parts[0] == "v1" && parts[1] == "blocks" &&
      parts[3] == "neighbors") {
    return BuildJsonResponse(200, store_.Neighbors(parts[2]));
  }
  if (parts.size() == 3 && parts[0] == "v1" && parts[1] == "capsules") {
    const auto result = store_.ReadCapsule(parts[2]);
    if (result.contains("error")) {
      throw ApiError(404, result.value("error", "not_found"), result.value("message", "not found"));
    }
    return BuildJsonResponse(200, result);
  }
  if (parts.size() == 2 && parts[0] == "v1" && parts[1] == "reviews") {
    return BuildJsonResponse(
        200,
        store_.ListReviewItems(nlohmann::json{
            {"status", request.query_params.count("status") == 0
                           ? std::string("pending")
                           : request.query_params.at("status")}}));
  }
  if (parts.size() == 2 && parts[0] == "v1" && parts[1] == "catalog") {
    return BuildJsonResponse(
        200,
        store_.CatalogQuery(nlohmann::json{
            {"term", request.query_params.count("term") == 0 ? std::string{} : request.query_params.at("term")}}));
  }
  if (parts.size() == 3 && parts[0] == "v1" && parts[1] == "replica-merges" &&
      parts[2] == "status") {
    const auto it = request.query_params.find("plane_id");
    return BuildJsonResponse(
        200,
        store_.ReplicaMergeStatus(it == request.query_params.end() ? "" : it->second));
  }
  throw ApiError(404, "not_found", "route not found");
}

HttpResponse KnowledgeServer::HandlePost(const HttpRequest& request) {
  const auto parts = SplitPath(request.path);
  if (parts.size() == 2 && parts[0] == "v1" && parts[1] == "blocks") {
    return BuildJsonResponse(201, store_.WriteBlock(ParseJsonBody(request)));
  }
  if (parts.size() == 2 && parts[0] == "v1" && parts[1] == "relations") {
    return BuildJsonResponse(201, store_.WriteRelation(ParseJsonBody(request)));
  }
  if (parts.size() == 2 && parts[0] == "v1" && parts[1] == "search") {
    return BuildJsonResponse(200, store_.Search(ParseJsonBody(request)));
  }
  if (parts.size() == 2 && parts[0] == "v1" && parts[1] == "context") {
    return BuildJsonResponse(200, store_.Context(ParseJsonBody(request)));
  }
  if (parts.size() == 2 && parts[0] == "v1" && parts[1] == "query-route") {
    return BuildJsonResponse(200, store_.QueryRoute(ParseJsonBody(request)));
  }
  if (parts.size() == 2 && parts[0] == "v1" && parts[1] == "source-ingest") {
    return BuildJsonResponse(200, store_.IngestSource(ParseJsonBody(request)));
  }
  if (parts.size() == 2 && parts[0] == "v1" && parts[1] == "capsules") {
    return BuildJsonResponse(201, store_.BuildCapsule(ParseJsonBody(request)));
  }
  if (parts.size() == 2 && parts[0] == "v1" && parts[1] == "overlays") {
    return BuildJsonResponse(201, store_.WriteOverlay(ParseJsonBody(request)));
  }
  if (parts.size() == 3 && parts[0] == "v1" && parts[1] == "replica-merges" &&
      parts[2] == "schedule") {
    return BuildJsonResponse(200, store_.ScheduleReplicaMerge(ParseJsonBody(request)));
  }
  if (parts.size() == 3 && parts[0] == "v1" && parts[1] == "replica-merges" &&
      parts[2] == "run-due") {
    return BuildJsonResponse(200, store_.RunScheduledReplicaMerges(ParseJsonBody(request)));
  }
  if (parts.size() == 3 && parts[0] == "v1" && parts[1] == "replica-merges" &&
      parts[2] == "reconcile-daily") {
    return BuildJsonResponse(200, store_.ReconcileDailyReplicaSchedules(ParseJsonBody(request)));
  }
  if (parts.size() == 3 && parts[0] == "v1" && parts[1] == "replica-merges" &&
      parts[2] == "trigger") {
    return BuildJsonResponse(200, store_.TriggerReplicaMerge(ParseJsonBody(request)));
  }
  if (parts.size() == 2 && parts[0] == "v1" && parts[1] == "repair") {
    return BuildJsonResponse(200, store_.RunRepair(ParseJsonBody(request)));
  }
  if (parts.size() == 2 && parts[0] == "v1" && parts[1] == "markdown-export") {
    return BuildJsonResponse(200, store_.MarkdownExport(ParseJsonBody(request)));
  }
  if (parts.size() == 2 && parts[0] == "v1" && parts[1] == "markdown-import") {
    return BuildJsonResponse(200, store_.MarkdownImport(ParseJsonBody(request)));
  }
  if (parts.size() == 2 && parts[0] == "v1" && parts[1] == "graph-neighborhood") {
    return BuildJsonResponse(200, store_.GraphNeighborhood(ParseJsonBody(request)));
  }
  if (parts.size() == 2 && parts[0] == "v1" && parts[1] == "catalog") {
    return BuildJsonResponse(200, store_.CatalogUpsert(ParseJsonBody(request)));
  }
  throw ApiError(404, "not_found", "route not found");
}

HttpResponse KnowledgeServer::HandlePut(const HttpRequest& request) {
  const auto parts = SplitPath(request.path);
  if (parts.size() == 3 && parts[0] == "v1" && parts[1] == "heads") {
    return BuildJsonResponse(200, store_.UpdateHead(parts[2], ParseJsonBody(request)));
  }
  if (parts.size() == 3 && parts[0] == "v1" && parts[1] == "reviews") {
    const auto result = store_.DecideReviewItem(parts[2], ParseJsonBody(request));
    if (result.contains("error")) {
      throw ApiError(404, result.value("error", "not_found"), result.value("message", "not found"));
    }
    return BuildJsonResponse(200, result);
  }
  throw ApiError(404, "not_found", "route not found");
}

std::vector<std::string> KnowledgeServer::SplitPath(const std::string& path) const {
  std::vector<std::string> parts;
  std::string current;
  for (char ch : path) {
    if (ch == '/') {
      if (!current.empty()) {
        parts.push_back(current);
        current.clear();
      }
      continue;
    }
    current.push_back(ch);
  }
  if (!current.empty()) {
    parts.push_back(current);
  }
  return parts;
}

nlohmann::json KnowledgeServer::ParseJsonBody(const HttpRequest& request) {
  if (request.body.empty()) {
    return nlohmann::json::object();
  }
  const auto payload = nlohmann::json::parse(request.body, nullptr, false);
  if (payload.is_discarded()) {
    throw ApiError(400, "invalid_json", "request body is not valid JSON");
  }
  return payload;
}

HttpResponse KnowledgeServer::BuildJsonResponse(
    int status_code,
    const nlohmann::json& payload) const {
  HttpResponse response;
  response.status_code = status_code;
  response.content_type = "application/json";
  response.body = payload.dump();
  response.headers["Cache-Control"] = "no-store";
  return response;
}

void KnowledgeServer::WriteRuntimeStatus(const std::string& phase, bool ready) const {
  nlohmann::json status = store_.Status(config_.service_id);
  status["runtime_phase"] = phase;
  status["ready"] = ready;
  status["node_name"] = config_.node_name;
  status["listen_host"] = config_.listen_host;
  status["port"] = config_.port;
  std::ofstream output(config_.status_path);
  output << status.dump(2) << "\n";
}

void KnowledgeServer::SetReadyFile(bool ready) const {
  std::error_code error;
  if (ready) {
    std::ofstream output(config_.ready_path);
    output << "ready\n";
  } else {
    std::filesystem::remove(config_.ready_path, error);
  }
}

}  // namespace naim::knowledge_runtime
