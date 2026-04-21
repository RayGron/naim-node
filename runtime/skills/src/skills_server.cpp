#include "skills/skills_server.h"

#include <array>
#include <cctype>
#include <csignal>
#include <fstream>
#include <filesystem>
#include <iostream>
#include <set>
#include <stdexcept>

#include "http/controller_http_server_support.h"
#include "http/controller_http_transport.h"
#include "infra/controller_network_manager.h"

namespace naim::skills {

namespace {

std::atomic<bool>* g_stop_requested = nullptr;

void SignalHandler(int) {
  if (g_stop_requested != nullptr) {
    g_stop_requested->store(true);
  }
}

std::string PercentEncodePathSegment(const std::string& value) {
  constexpr char kHex[] = "0123456789ABCDEF";
  std::string encoded;
  encoded.reserve(value.size());
  for (const auto raw : value) {
    const auto ch = static_cast<unsigned char>(raw);
    if (std::isalnum(ch) || ch == '-' || ch == '_' || ch == '.' || ch == '~') {
      encoded.push_back(static_cast<char>(ch));
      continue;
    }
    encoded.push_back('%');
    encoded.push_back(kHex[(ch >> 4) & 0x0F]);
    encoded.push_back(kHex[ch & 0x0F]);
  }
  return encoded;
}

}  // namespace

SkillsServer::SkillsServer(SkillsRuntimeConfig config)
    : config_(std::move(config)), store_(config_.db_path) {}

SkillsServer::~SkillsServer() {
  RequestStop();
}

int SkillsServer::Run() {
  std::filesystem::create_directories(config_.db_path.parent_path());
  std::filesystem::create_directories(config_.status_path.parent_path());

  WriteRuntimeStatus("starting", false);
  SetReadyFile(false);

  g_stop_requested = &stop_requested_;
  std::signal(SIGINT, SignalHandler);
  std::signal(SIGTERM, SignalHandler);

  SyncFromController();

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

void SkillsServer::RequestStop() {
  const bool was_requested = stop_requested_.exchange(true);
  if (!was_requested && naim::platform::IsSocketValid(listen_fd_)) {
    WriteRuntimeStatus("stopping", false);
    SetReadyFile(false);
    naim::controller::ControllerNetworkManager::ShutdownAndCloseSocket(listen_fd_);
  }
}

void SkillsServer::AcceptLoop() {
  while (!stop_requested_.load()) {
    const auto client_fd = accept(listen_fd_, nullptr, nullptr);
    if (!naim::platform::IsSocketValid(client_fd)) {
      if (stop_requested_.load() || naim::platform::LastSocketErrorWasInterrupted()) {
        continue;
      }
      throw std::runtime_error(
          "accept failed: " + naim::controller::ControllerNetworkManager::SocketErrorMessage());
    }
    std::thread(&SkillsServer::HandleClient, this, client_fd).detach();
  }
}

void SkillsServer::HandleClient(naim::platform::SocketHandle client_fd) {
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
              nlohmann::json{{"error", error.code()}, {"message", error.message()}}));
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

HttpResponse SkillsServer::HandleRequest(const HttpRequest& request) {
  if (request.method == "GET") {
    return HandleGet(request);
  }
  if (request.method == "POST") {
    return HandlePost(request);
  }
  if (request.method == "PUT") {
    return HandlePut(request);
  }
  if (request.method == "PATCH") {
    return HandlePatch(request);
  }
  if (request.method == "DELETE") {
    return HandleDelete(request);
  }
  throw ApiError(405, "method_not_allowed", "method not allowed");
}

HttpResponse SkillsServer::HandleGet(const HttpRequest& request) {
  const auto parts = SplitPath(request.path);
  if (parts.size() == 1 && parts[0] == "health") {
    return BuildJsonResponse(200, nlohmann::json{{"ok", true}, {"ready", true}});
  }
  if (parts.size() == 2 && parts[0] == "v1" && parts[1] == "skills") {
    return BuildJsonResponse(200, nlohmann::json{{"skills", store_.ListSkills()}});
  }
  if (parts.size() == 3 && parts[0] == "v1" && parts[1] == "skills") {
    return BuildJsonResponse(200, store_.GetSkill(parts[2]));
  }
  throw ApiError(404, "not_found", "route not found");
}

HttpResponse SkillsServer::HandlePost(const HttpRequest& request) {
  const auto parts = SplitPath(request.path);
  if (parts.size() == 2 && parts[0] == "v1" && parts[1] == "skills") {
    return BuildJsonResponse(201, store_.CreateSkill(ParseJsonBody(request)));
  }
  if (parts.size() == 3 && parts[0] == "v1" && parts[1] == "skills" && parts[2] == "resolve") {
    return BuildJsonResponse(200, store_.ResolveSkills(ParseJsonBody(request)));
  }
  throw ApiError(404, "not_found", "route not found");
}

HttpResponse SkillsServer::HandlePut(const HttpRequest& request) {
  const auto parts = SplitPath(request.path);
  if (parts.size() == 3 && parts[0] == "v1" && parts[1] == "skills") {
    return BuildJsonResponse(200, store_.ReplaceSkill(parts[2], ParseJsonBody(request), false));
  }
  throw ApiError(404, "not_found", "route not found");
}

HttpResponse SkillsServer::HandlePatch(const HttpRequest& request) {
  const auto parts = SplitPath(request.path);
  if (parts.size() == 3 && parts[0] == "v1" && parts[1] == "skills") {
    return BuildJsonResponse(200, store_.ReplaceSkill(parts[2], ParseJsonBody(request), true));
  }
  throw ApiError(404, "not_found", "route not found");
}

HttpResponse SkillsServer::HandleDelete(const HttpRequest& request) {
  const auto parts = SplitPath(request.path);
  if (parts.size() == 3 && parts[0] == "v1" && parts[1] == "skills") {
    store_.DeleteSkill(parts[2]);
    return BuildJsonResponse(200, nlohmann::json{{"deleted", true}, {"id", parts[2]}});
  }
  throw ApiError(404, "not_found", "route not found");
}

std::vector<std::string> SkillsServer::SplitPath(const std::string& path) const {
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

nlohmann::json SkillsServer::ParseJsonBody(const HttpRequest& request) {
  if (request.body.empty()) {
    return nlohmann::json::object();
  }
  const auto payload = nlohmann::json::parse(request.body, nullptr, false);
  if (payload.is_discarded()) {
    throw ApiError(400, "invalid_json", "invalid JSON body");
  }
  return payload;
}

HttpResponse SkillsServer::BuildJsonResponse(int status_code, const nlohmann::json& payload) const {
  HttpResponse response;
  response.status_code = status_code;
  response.content_type = "application/json";
  response.body = payload.dump();
  return response;
}

void SkillsServer::SyncFromController() {
  if (config_.controller_url.empty() || config_.plane_name.empty() ||
      config_.plane_name == "unknown") {
    return;
  }

  try {
    const auto target = ParseControllerEndpointTarget(config_.controller_url);
    const std::string path =
        "/api/v1/planes/" + PercentEncodePathSegment(config_.plane_name) + "/skills";
    const auto response = SendControllerHttpRequest(
        target,
        "GET",
        path,
        "",
        {{"Accept", "application/json"}, {"Cache-Control", "no-store"}});
    if (response.status_code != 200) {
      std::cerr << "[naim-skills] controller sync skipped: status="
                << response.status_code << " path=" << path << "\n";
      return;
    }

    const auto payload = nlohmann::json::parse(response.body, nullptr, false);
    if (payload.is_discarded() || !payload.is_object() ||
        !payload.contains("skills") || !payload.at("skills").is_array()) {
      std::cerr << "[naim-skills] controller sync skipped: malformed payload\n";
      return;
    }

    std::set<std::string> selected_ids;
    int synced_count = 0;
    for (const auto& item : payload.at("skills")) {
      if (!item.is_object() || !item.contains("id") || !item.at("id").is_string()) {
        continue;
      }
      const std::string skill_id = item.at("id").get<std::string>();
      if (skill_id.empty()) {
        continue;
      }
      selected_ids.insert(skill_id);
      store_.ReplaceSkill(skill_id, item, false);
      ++synced_count;
    }

    const auto existing = store_.ListSkills();
    if (existing.is_array()) {
      for (const auto& item : existing) {
        if (!item.is_object() || !item.contains("id") || !item.at("id").is_string()) {
          continue;
        }
        const std::string skill_id = item.at("id").get<std::string>();
        if (!skill_id.empty() && selected_ids.count(skill_id) == 0) {
          store_.DeleteSkill(skill_id);
        }
      }
    }

    std::cout << "[naim-skills] controller sync ok skills=" << synced_count << "\n";
  } catch (const std::exception& error) {
    std::cerr << "[naim-skills] controller sync failed: " << error.what() << "\n";
  }
}

void SkillsServer::WriteRuntimeStatus(const std::string& phase, bool ready) const {
  naim::RuntimeStatus status;
  status.plane_name = config_.plane_name;
  status.control_root = config_.control_root;
  status.controller_url = config_.controller_url;
  status.instance_name = config_.instance_name;
  status.instance_role = config_.instance_role;
  status.node_name = config_.node_name;
  status.runtime_backend = "sqlite-http";
  status.runtime_phase = phase;
  status.started_at = SkillsStore::UtcNow();
  status.last_activity_at = status.started_at;
  status.ready = ready;
  status.active_model_ready = true;
  status.gateway_plan_ready = false;
  status.inference_ready = ready;
  status.gateway_ready = false;
  status.launch_ready = ready;
  SaveRuntimeStatusJson(status, config_.status_path.string());
}

void SkillsServer::SetReadyFile(bool ready) const {
  if (ready) {
    std::filesystem::create_directories(config_.ready_path.parent_path());
    std::ofstream output(config_.ready_path);
    output << "ready\n";
    return;
  }
  std::error_code error;
  std::filesystem::remove(config_.ready_path, error);
}

}  // namespace naim::skills
