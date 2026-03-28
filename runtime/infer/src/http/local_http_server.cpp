#include "http/local_http_server.h"

#include <algorithm>
#include <array>
#include <cerrno>
#include <string>
#include <sys/socket.h>
#include <thread>
#include <unistd.h>

#include "runtime/infer_runtime_support.h"

namespace comet::infer {

LocalHttpServer::LocalHttpServer(
    std::string host,
    int port,
    std::string service_name,
    const RuntimeConfig& config,
    LlamaLibraryEngine* engine,
    const InferSignalService& signal_service,
    bool dynamic_upstream,
    std::optional<UpstreamTarget> upstream)
    : host_(std::move(host)),
      port_(port),
      service_name_(std::move(service_name)),
      config_(config),
      engine_(engine),
      signal_service_(signal_service),
      dynamic_upstream_(dynamic_upstream),
      upstream_(std::move(upstream)) {}

LocalHttpServer::~LocalHttpServer() {
  Stop();
}

void LocalHttpServer::Start() {
  listen_fd_ = runtime_support::CreateListenSocket(host_, port_);
  worker_ = std::thread([this]() { AcceptLoop(); });
}

void LocalHttpServer::Stop() {
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

void LocalHttpServer::AcceptLoop() {
  running_ = true;
  while (running_ && !signal_service_.StopRequested()) {
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

void LocalHttpServer::HandleClient(int client_fd) {
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
        const HttpRequest partial = runtime_support::ParseHttpRequest(request_data);
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
      const std::size_t body_bytes =
          headers_end == std::string::npos ? 0 : request_data.size() - (headers_end + 4);
      if (body_bytes >= content_length) {
        break;
      }
    }
  }
  if (!request_data.empty()) {
    if (dynamic_upstream_) {
      const auto upstream = runtime_support::ResolveRuntimeUpstreamTarget(config_);
      if (!upstream.has_value() ||
          !runtime_support::ProxyHttpRequest(client_fd, request_data, *upstream)) {
        runtime_support::SendErrorResponse(client_fd, 503, "upstream runtime is unavailable");
      }
    } else if (upstream_.has_value()) {
      if (!runtime_support::ProxyHttpRequest(client_fd, request_data, *upstream_)) {
        runtime_support::SendErrorResponse(client_fd, 503, "upstream runtime is unavailable");
      }
    } else {
      const HttpRequest request = runtime_support::ParseHttpRequest(request_data);
      if (runtime_support::RequestWantsStream(request)) {
        runtime_support::HandleStreamingChatRequest(client_fd, config_, request, engine_);
      } else {
        const SimpleResponse response =
            runtime_support::HandleLocalRequest(config_, request, service_name_, engine_);
        runtime_support::SendResponse(client_fd, response);
      }
    }
  }
  shutdown(client_fd, SHUT_RDWR);
  close(client_fd);
}

}  // namespace comet::infer
