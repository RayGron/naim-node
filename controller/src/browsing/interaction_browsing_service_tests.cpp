#include <atomic>
#include <iostream>
#include <sstream>
#include <stdexcept>
#include <string>
#include <thread>

#include <nlohmann/json.hpp>

#include "browsing/interaction_browsing_service.h"
#include "naim/core/platform_compat.h"

namespace {

using nlohmann::json;

void Expect(bool condition, const std::string& message) {
  if (!condition) {
    throw std::runtime_error(message);
  }
}

class FixedResolveServer {
 public:
  explicit FixedResolveServer(json resolve_response)
      : resolve_response_(std::move(resolve_response)) {
    naim::platform::EnsureSocketsInitialized();
    listen_fd_ = socket(AF_INET, SOCK_STREAM, 0);
    if (!naim::platform::IsSocketValid(listen_fd_)) {
      throw std::runtime_error("failed to create fixed resolve server socket");
    }

    int yes = 1;
#if defined(_WIN32)
    setsockopt(
        listen_fd_,
        SOL_SOCKET,
        SO_REUSEADDR,
        reinterpret_cast<const char*>(&yes),
        sizeof(yes));
#else
    setsockopt(listen_fd_, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes));
#endif

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(0);
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    if (bind(listen_fd_, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0) {
      const auto error = naim::platform::LastSocketErrorMessage();
      naim::platform::CloseSocket(listen_fd_);
      throw std::runtime_error("failed to bind fixed resolve server: " + error);
    }
    if (listen(listen_fd_, 8) != 0) {
      const auto error = naim::platform::LastSocketErrorMessage();
      naim::platform::CloseSocket(listen_fd_);
      throw std::runtime_error("failed to listen fixed resolve server: " + error);
    }

    sockaddr_in bound_addr{};
    socklen_t bound_size = sizeof(bound_addr);
    if (getsockname(
            listen_fd_,
            reinterpret_cast<sockaddr*>(&bound_addr),
            &bound_size) != 0) {
      const auto error = naim::platform::LastSocketErrorMessage();
      naim::platform::CloseSocket(listen_fd_);
      throw std::runtime_error("failed to inspect fixed resolve server: " + error);
    }
    port_ = ntohs(bound_addr.sin_port);
    thread_ = std::thread([this]() { Serve(); });
  }

  ~FixedResolveServer() {
    stop_requested_.store(true);
    if (port_ > 0) {
      const auto wake_fd = socket(AF_INET, SOCK_STREAM, 0);
      if (naim::platform::IsSocketValid(wake_fd)) {
        sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_port = htons(static_cast<uint16_t>(port_));
        addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        (void)connect(wake_fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr));
        naim::platform::CloseSocket(wake_fd);
      }
    }
    if (naim::platform::IsSocketValid(listen_fd_)) {
      naim::platform::CloseSocket(listen_fd_);
    }
    if (thread_.joinable()) {
      thread_.join();
    }
  }

  int port() const { return port_; }

 private:
  void WriteJsonResponse(
      naim::platform::SocketHandle client_fd,
      int status_code,
      const json& payload) {
    const std::string body = payload.dump();
    std::ostringstream response;
    response << "HTTP/1.1 " << status_code << " OK\r\n";
    response << "Content-Type: application/json\r\n";
    response << "Content-Length: " << body.size() << "\r\n";
    response << "Connection: close\r\n\r\n";
    response << body;
    const std::string serialized = response.str();
    const char* data = serialized.c_str();
    std::size_t remaining = serialized.size();
    while (remaining > 0) {
      const auto written = send(client_fd, data, remaining, 0);
      if (written <= 0) {
        break;
      }
      data += written;
      remaining -= static_cast<std::size_t>(written);
    }
  }

  void Serve() {
    while (true) {
      sockaddr_in client_addr{};
      socklen_t client_size = sizeof(client_addr);
      const auto client_fd = accept(
          listen_fd_, reinterpret_cast<sockaddr*>(&client_addr), &client_size);
      if (!naim::platform::IsSocketValid(client_fd)) {
        return;
      }
      if (stop_requested_.load()) {
        naim::platform::CloseSocket(client_fd);
        return;
      }

      char buffer[4096];
      const auto read_count = recv(client_fd, buffer, sizeof(buffer), 0);
      const std::string request =
          read_count > 0 ? std::string(buffer, static_cast<std::size_t>(read_count)) : "";
      if (request.rfind("GET /health ", 0) == 0) {
        WriteJsonResponse(client_fd, 200, json{{"status", "ok"}});
      } else if (request.rfind("POST /v1/webgateway/resolve ", 0) == 0) {
        WriteJsonResponse(client_fd, 200, resolve_response_);
      } else {
        WriteJsonResponse(client_fd, 404, json{{"status", "error"}});
      }
      naim::platform::CloseSocket(client_fd);
    }
  }

  json resolve_response_;
  naim::platform::SocketHandle listen_fd_ = naim::platform::kInvalidSocket;
  int port_ = 0;
  std::atomic<bool> stop_requested_{false};
  std::thread thread_;
};

naim::DesiredState BuildDesiredStateWithBrowsingPort(
    const std::string& host_ip,
    int host_port) {
  naim::DesiredState desired_state;
  desired_state.plane_name = "maglev";
  desired_state.plane_mode = naim::PlaneMode::Llm;
  naim::BrowsingSettings settings;
  settings.enabled = true;
  naim::BrowsingPolicySettings policy;
  policy.browser_session_enabled = true;
  policy.rendered_browser_enabled = true;
  settings.policy = policy;
  desired_state.browsing = settings;

  naim::InstanceSpec browsing;
  browsing.name = "webgateway-maglev";
  browsing.plane_name = "maglev";
  browsing.node_name = "local-hostd";
  browsing.role = naim::InstanceRole::Browsing;
  naim::PublishedPort published_port;
  published_port.host_ip = host_ip;
  published_port.host_port = host_port;
  published_port.container_port = 18130;
  browsing.published_ports.push_back(published_port);
  desired_state.instances.push_back(browsing);
  return desired_state;
}

void TestPersistedEnabledModeBuildsIdleFallbackWithoutUpstream() {
  naim::controller::InteractionBrowsingService service;
  naim::controller::InteractionRequestContext context;
  context.session_context_state = json{{"browsing_mode", "enabled"}};
  context.payload = json{
      {naim::controller::kInteractionSessionContextStatePayloadKey,
       json{{"browsing_mode", "enabled"}}},
      {"messages",
       json::array({json{{"role", "user"}, {"content", "Explain TCP handshakes."}}})},
  };
  naim::controller::PlaneInteractionResolution resolution;
  resolution.desired_state = BuildDesiredStateWithBrowsingPort("127.0.0.1", 18130);

  const auto error = service.ResolveInteractionBrowsing(resolution, &context);
  Expect(!error.has_value(), "persisted enabled mode should not require upstream for idle prompt");
  const auto& summary =
      context.payload.at(naim::controller::InteractionBrowsingService::kSummaryPayloadKey);
  Expect(summary.at("mode").get<std::string>() == "enabled",
         "persisted enabled mode should stay enabled");
  Expect(summary.at("lookup_state").get<std::string>() == "enabled_not_needed",
         "persisted enabled mode should expose enabled_not_needed fallback");
  Expect(
      context.payload
          .at(naim::controller::InteractionBrowsingService::kSystemInstructionPayloadKey)
          .get<std::string>()
          .find("WebGateway determined that no web lookup was needed for this request") !=
          std::string::npos,
      "persisted enabled fallback should inject no-lookup system instruction");
  Expect(
      context.payload
          .at(naim::controller::kInteractionSessionContextStatePayloadKey)
          .at("browsing_mode")
          .get<std::string>() == "enabled",
      "persisted enabled fallback should preserve session browsing mode");
  std::cout << "ok: interaction-browsing-persisted-enabled-idle-fallback" << '\n';
}

void TestResolvePayloadPersistsBrowsingModeFromUpstreamContext() {
  FixedResolveServer server(json{
      {"decision", "not_needed"},
      {"model_instruction", "test browsing instruction"},
      {"response_policy", json::object()},
      {"context",
       json{{"mode", "enabled"},
            {"mode_source", "toggle"},
            {"plane_enabled", true},
            {"ready", true},
            {"session_backend", "cef"},
            {"rendered_browser_enabled", true},
            {"rendered_browser_ready", true},
            {"login_enabled", false},
            {"toggle_only", true},
            {"decision", "not_needed"},
            {"reason", "toggle_only"},
            {"lookup_state", "enabled_toggle_only"},
            {"lookup_attempted", false},
            {"lookup_required", false},
            {"evidence_attached", false},
            {"searches", json::array()},
            {"sources", json::array()},
            {"errors", json::array()},
            {"refusal", nullptr},
            {"response_policy", json::object()},
            {"indicator", json{{"compact", "web:on"}}}}}});

  naim::controller::InteractionBrowsingService service;
  naim::controller::InteractionRequestContext context;
  context.payload = json{
      {"messages",
       json::array(
           {json{{"role", "user"}, {"content", "Enable web for this chat."}}})},
  };
  naim::controller::PlaneInteractionResolution resolution;
  resolution.desired_state = BuildDesiredStateWithBrowsingPort("127.0.0.1", server.port());

  const auto error = service.ResolveInteractionBrowsing(resolution, &context);
  Expect(!error.has_value(), "upstream browsing resolve should succeed");
  Expect(
      context.payload
          .at(naim::controller::kInteractionSessionContextStatePayloadKey)
          .at("browsing_mode")
          .get<std::string>() == "enabled",
      "upstream enabled context should persist enabled session mode");
  Expect(
      context.session_context_state.at("browsing_mode").get<std::string>() == "enabled",
      "upstream enabled context should sync in-memory session mode");
  std::cout << "ok: interaction-browsing-persists-upstream-mode" << '\n';
}

}  // namespace

int main() {
  try {
    TestPersistedEnabledModeBuildsIdleFallbackWithoutUpstream();
    TestResolvePayloadPersistsBrowsingModeFromUpstreamContext();
    return 0;
  } catch (const std::exception& error) {
    std::cerr << "interaction_browsing_service_tests failed: "
              << error.what() << '\n';
    return 1;
  }
}
