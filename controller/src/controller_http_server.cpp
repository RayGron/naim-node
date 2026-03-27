#include "../include/controller_http_server.h"

#include <algorithm>
#include <array>
#include <chrono>
#include <csignal>
#include <iostream>
#include <stdexcept>
#include <thread>

#include "../include/controller_http_server_support.h"
#include "../include/controller_network_manager.h"

using nlohmann::json;

namespace comet::controller {
namespace {

std::atomic<bool>* g_controller_http_server_stop_requested = nullptr;

void ControllerHttpServerSignalHandler(int) {
  if (g_controller_http_server_stop_requested != nullptr) {
    g_controller_http_server_stop_requested->store(true);
  }
}

struct SseStreamRequest {
  std::optional<std::string> plane_name;
  std::optional<std::string> node_name;
  std::optional<std::string> worker_name;
  std::optional<std::string> category;
  int limit = 100;
  std::optional<int> last_event_id;
};

std::optional<std::string> FindQueryString(
    const HttpRequest& request,
    const std::string& key) {
  const auto it = request.query_params.find(key);
  if (it == request.query_params.end() || it->second.empty()) {
    return std::nullopt;
  }
  return it->second;
}

std::optional<int> FindQueryInt(
    const HttpRequest& request,
    const std::string& key) {
  const auto value = FindQueryString(request, key);
  if (!value.has_value()) {
    return std::nullopt;
  }
  return std::stoi(*value);
}

SseStreamRequest ParseSseStreamRequest(const HttpRequest& request) {
  SseStreamRequest stream_request;
  stream_request.plane_name = FindQueryString(request, "plane");
  stream_request.node_name = FindQueryString(request, "node");
  stream_request.worker_name = FindQueryString(request, "worker");
  stream_request.category = FindQueryString(request, "category");
  stream_request.limit = FindQueryInt(request, "limit").value_or(100);
  stream_request.last_event_id = FindQueryInt(request, "since_id");
  if (!stream_request.last_event_id.has_value()) {
    const auto header_value =
        ControllerHttpServerSupport::FindHeaderString(request, "last-event-id");
    if (header_value.has_value()) {
      stream_request.last_event_id = std::stoi(*header_value);
    }
  }
  return stream_request;
}

}  // namespace

ControllerHttpServer::ControllerHttpServer(Deps deps) : deps_(std::move(deps)) {}

void ControllerHttpServer::InstallSignalHandlers(
    std::atomic<bool>* stop_requested) {
  g_controller_http_server_stop_requested = stop_requested;
  ::signal(SIGINT, ControllerHttpServerSignalHandler);
  ::signal(SIGTERM, ControllerHttpServerSignalHandler);
}

std::string ControllerHttpServer::BuildSseEventName(
    const comet::EventRecord& event) {
  if (!event.category.empty() && !event.event_type.empty()) {
    return event.category + "." + event.event_type;
  }
  if (!event.category.empty()) {
    return event.category;
  }
  if (!event.event_type.empty()) {
    return event.event_type;
  }
  return "event";
}

void ControllerHttpServer::StreamEventsSse(
    const SocketHandle client_fd,
    const std::string& db_path,
    const HttpRequest& request,
    const BuildEventPayloadItemFn build_event_payload_item,
    std::shared_ptr<SharedState> state) {
  const SseStreamRequest stream_request = ParseSseStreamRequest(request);
  int last_event_id = stream_request.last_event_id.value_or(0);
  if (!ControllerNetworkManager::SendSseHeaders(client_fd)) {
    ControllerNetworkManager::ShutdownAndCloseSocket(client_fd);
    return;
  }
  if (!ControllerNetworkManager::SendSseCommentFrame(client_fd, " connected")) {
    ControllerNetworkManager::ShutdownAndCloseSocket(client_fd);
    return;
  }

  auto last_keepalive = std::chrono::steady_clock::now();
  while (!state->stop_requested.load()) {
    comet::ControllerStore store(db_path);
    store.Initialize();
    const auto events = store.LoadEvents(
        stream_request.plane_name,
        stream_request.node_name,
        stream_request.worker_name,
        stream_request.category,
        stream_request.limit,
        last_event_id > 0 ? std::optional<int>(last_event_id) : std::nullopt,
        true);
    for (const auto& event : events) {
      const std::string payload = build_event_payload_item(event).dump();
      if (!ControllerNetworkManager::SendSseEventFrame(
              client_fd,
              event.id,
              BuildSseEventName(event),
              payload)) {
        ControllerNetworkManager::ShutdownAndCloseSocket(client_fd);
        return;
      }
      last_event_id = std::max(last_event_id, event.id);
      last_keepalive = std::chrono::steady_clock::now();
    }

    const auto now = std::chrono::steady_clock::now();
    if (now - last_keepalive >= std::chrono::seconds(5)) {
      if (!ControllerNetworkManager::SendSseCommentFrame(
              client_fd,
              " keepalive")) {
        ControllerNetworkManager::ShutdownAndCloseSocket(client_fd);
        return;
      }
      last_keepalive = now;
    }

    comet::platform::PollFd fd_state{};
    fd_state.fd = client_fd;
    fd_state.events = POLLIN | POLLERR | POLLHUP;
    const int poll_result = comet::platform::Poll(&fd_state, 1, 1000);
    if (poll_result < 0) {
      if (comet::platform::LastSocketErrorWasInterrupted()) {
        continue;
      }
      break;
    }
    if (poll_result == 0) {
      continue;
    }
    if ((fd_state.revents & (POLLERR | POLLHUP | POLLNVAL)) != 0) {
      break;
    }
    if ((fd_state.revents & POLLIN) != 0) {
      char scratch[1];
      const ssize_t read_count = recv(client_fd, scratch, sizeof(scratch), MSG_PEEK);
      if (read_count <= 0) {
        break;
      }
    }
  }

  ControllerNetworkManager::ShutdownAndCloseSocket(client_fd);
}

int ControllerHttpServer::Serve(const Config& config) {
  auto state = std::make_shared<SharedState>();
  state->stop_requested.store(false);
  InstallSignalHandlers(&state->stop_requested);

  comet::ControllerStore store(config.db_path);
  store.Initialize();

  const SocketHandle listen_fd = ControllerNetworkManager::CreateListenSocket(
      config.listen_host,
      config.listen_port);
  std::cout << "comet-controller serve\n";
  std::cout << "listen=" << config.listen_host << ":" << config.listen_port
            << "\n";
  std::cout << "db=" << config.db_path << "\n";
  std::cout << "artifacts_root=" << config.artifacts_root << "\n";
  if (config.ui_root.has_value()) {
    std::cout << "ui_root=" << config.ui_root->string() << "\n";
  }
  std::cout << "routes=" << config.routes_summary << "\n";
  std::cout.flush();

  while (!state->stop_requested.load()) {
    comet::platform::PollFd fd_state{};
    fd_state.fd = listen_fd;
    fd_state.events = POLLIN;
    const int poll_result = comet::platform::Poll(&fd_state, 1, 250);
    if (poll_result < 0) {
      if (state->stop_requested.load() ||
          comet::platform::LastSocketErrorWasInterrupted()) {
        continue;
      }
      const std::string error = ControllerNetworkManager::SocketErrorMessage();
      ControllerNetworkManager::CloseSocket(listen_fd);
      throw std::runtime_error("poll failed: " + error);
    }
    if (poll_result == 0) {
      continue;
    }
    if ((fd_state.revents & POLLIN) == 0) {
      continue;
    }

    const SocketHandle client_fd = accept(listen_fd, nullptr, nullptr);
    if (!comet::platform::IsSocketValid(client_fd)) {
      if (state->stop_requested.load() ||
          comet::platform::LastSocketErrorWasInterrupted()) {
        continue;
      }
      const std::string error = ControllerNetworkManager::SocketErrorMessage();
      ControllerNetworkManager::CloseSocket(listen_fd);
      throw std::runtime_error("accept failed: " + error);
    }

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
            ControllerHttpServerSupport::ExpectedRequestBytes(request_data);
      }
      if (expected_request_bytes != 0 &&
          request_data.size() >= expected_request_bytes) {
        break;
      }
    }

    if (request_data.empty()) {
      ControllerNetworkManager::ShutdownAndCloseSocket(client_fd);
      continue;
    }

    const HttpRequest request =
        ControllerHttpServerSupport::ParseHttpRequest(request_data);
    if (request.method == "GET" && request.path == "/api/v1/events/stream") {
      std::thread(
          [client_fd,
           db_path = config.db_path,
           request,
           build_event_payload_item = deps_.build_event_payload_item,
           state]() {
            StreamEventsSse(
                client_fd,
                db_path,
                request,
                build_event_payload_item,
                std::move(state));
          })
          .detach();
      continue;
    }
    if (deps_.parse_interaction_stream_plane_name(request.method, request.path)
            .has_value()) {
      std::thread(
          [client_fd,
           db_path = config.db_path,
           request,
           stream_plane_interaction_sse = deps_.stream_plane_interaction_sse]() {
            stream_plane_interaction_sse(client_fd, db_path, request);
          })
          .detach();
      continue;
    }
    std::thread(
        [client_fd,
         request,
         handle_request = deps_.handle_request]() {
          try {
            const HttpResponse response = handle_request(request);
            ControllerNetworkManager::SendHttpResponse(client_fd, response);
          } catch (const std::exception& error) {
            try {
              HttpResponse response;
              response.status_code = 500;
              response.body =
                  json{{"status", "error"}, {"message", error.what()}}.dump();
              ControllerNetworkManager::SendHttpResponse(client_fd, response);
            } catch (const std::exception&) {
            }
          }
          ControllerNetworkManager::ShutdownAndCloseSocket(client_fd);
        })
        .detach();
  }

  ControllerNetworkManager::CloseSocket(listen_fd);
  return 0;
}

}  // namespace comet::controller
