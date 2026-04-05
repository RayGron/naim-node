#include <filesystem>
#include <iostream>
#include <sstream>
#include <atomic>
#include <stdexcept>
#include <string>
#include <thread>

#include <nlohmann/json.hpp>

#include "comet/core/platform_compat.h"
#include "comet/state/sqlite_store.h"
#include "browsing/interaction_browsing_service.h"
#include "interaction/interaction_service.h"
#include "browsing/plane_browsing_service.h"
#include "plane/plane_dashboard_skills_summary_service.h"
#include "skills/plane_skill_contextual_resolver_service.h"
#include "skills/plane_skills_service.h"

namespace {

namespace fs = std::filesystem;
using nlohmann::json;

void Expect(bool condition, const std::string& message) {
  if (!condition) {
    throw std::runtime_error(message);
  }
}

class SkillRuntimeTestServer {
 public:
  explicit SkillRuntimeTestServer(json skills_payload)
      : skills_payload_(std::move(skills_payload)) {
    comet::platform::EnsureSocketsInitialized();
    listen_fd_ = socket(AF_INET, SOCK_STREAM, 0);
    if (!comet::platform::IsSocketValid(listen_fd_)) {
      throw std::runtime_error("failed to create test runtime socket");
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
      const auto error = comet::platform::LastSocketErrorMessage();
      comet::platform::CloseSocket(listen_fd_);
      throw std::runtime_error("failed to bind test runtime socket: " + error);
    }
    if (listen(listen_fd_, 8) != 0) {
      const auto error = comet::platform::LastSocketErrorMessage();
      comet::platform::CloseSocket(listen_fd_);
      throw std::runtime_error("failed to listen on test runtime socket: " + error);
    }

    sockaddr_in bound_addr{};
    socklen_t bound_size = sizeof(bound_addr);
    if (getsockname(
            listen_fd_,
            reinterpret_cast<sockaddr*>(&bound_addr),
            &bound_size) != 0) {
      const auto error = comet::platform::LastSocketErrorMessage();
      comet::platform::CloseSocket(listen_fd_);
      throw std::runtime_error("failed to inspect test runtime socket: " + error);
    }
    port_ = ntohs(bound_addr.sin_port);
    thread_ = std::thread([this]() { Serve(); });
  }

  ~SkillRuntimeTestServer() {
    stop_requested_.store(true);
    if (port_ > 0) {
      const auto wake_fd = socket(AF_INET, SOCK_STREAM, 0);
      if (comet::platform::IsSocketValid(wake_fd)) {
        sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_port = htons(static_cast<uint16_t>(port_));
        addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        (void)connect(wake_fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr));
        comet::platform::CloseSocket(wake_fd);
      }
    }
    if (comet::platform::IsSocketValid(listen_fd_)) {
      comet::platform::CloseSocket(listen_fd_);
    }
    if (thread_.joinable()) {
      thread_.join();
    }
  }

  int port() const { return port_; }

 private:
  void Serve() {
    while (true) {
      sockaddr_in client_addr{};
      socklen_t client_size = sizeof(client_addr);
      const auto client_fd = accept(
          listen_fd_, reinterpret_cast<sockaddr*>(&client_addr), &client_size);
      if (!comet::platform::IsSocketValid(client_fd)) {
        return;
      }
      if (stop_requested_.load()) {
        comet::platform::CloseSocket(client_fd);
        return;
      }

      char buffer[4096];
      (void)recv(client_fd, buffer, sizeof(buffer), 0);

      const std::string body = json{{"skills", skills_payload_}}.dump();
      std::ostringstream response;
      response << "HTTP/1.1 200 OK\r\n";
      response << "Content-Type: application/json\r\n";
      response << "Content-Length: " << body.size() << "\r\n";
      response << "Connection: close\r\n\r\n";
      response << body;
      const auto payload = response.str();
      const char* data = payload.c_str();
      std::size_t remaining = payload.size();
      while (remaining > 0) {
        const auto written = send(client_fd, data, remaining, 0);
        if (written <= 0) {
          break;
        }
        data += written;
        remaining -= static_cast<std::size_t>(written);
      }
      comet::platform::CloseSocket(client_fd);
    }
  }

  json skills_payload_;
  std::atomic<bool> stop_requested_{false};
  comet::platform::SocketHandle listen_fd_ = comet::platform::kInvalidSocket;
  int port_ = 0;
  std::thread thread_;
};

class BrowsingRuntimeTestServer {
 public:
  BrowsingRuntimeTestServer() {
    comet::platform::EnsureSocketsInitialized();
    listen_fd_ = socket(AF_INET, SOCK_STREAM, 0);
    if (!comet::platform::IsSocketValid(listen_fd_)) {
      throw std::runtime_error("failed to create browsing test runtime socket");
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
      const auto error = comet::platform::LastSocketErrorMessage();
      comet::platform::CloseSocket(listen_fd_);
      throw std::runtime_error("failed to bind browsing test runtime socket: " + error);
    }
    if (listen(listen_fd_, 8) != 0) {
      const auto error = comet::platform::LastSocketErrorMessage();
      comet::platform::CloseSocket(listen_fd_);
      throw std::runtime_error("failed to listen on browsing test runtime socket: " + error);
    }

    sockaddr_in bound_addr{};
    socklen_t bound_size = sizeof(bound_addr);
    if (getsockname(
            listen_fd_,
            reinterpret_cast<sockaddr*>(&bound_addr),
            &bound_size) != 0) {
      const auto error = comet::platform::LastSocketErrorMessage();
      comet::platform::CloseSocket(listen_fd_);
      throw std::runtime_error("failed to inspect browsing test runtime socket: " + error);
    }
    port_ = ntohs(bound_addr.sin_port);
    thread_ = std::thread([this]() { Serve(); });
  }

  ~BrowsingRuntimeTestServer() {
    stop_requested_.store(true);
    if (port_ > 0) {
      const auto wake_fd = socket(AF_INET, SOCK_STREAM, 0);
      if (comet::platform::IsSocketValid(wake_fd)) {
        sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_port = htons(static_cast<uint16_t>(port_));
        addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        (void)connect(wake_fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr));
        comet::platform::CloseSocket(wake_fd);
      }
    }
    if (comet::platform::IsSocketValid(listen_fd_)) {
      comet::platform::CloseSocket(listen_fd_);
    }
    if (thread_.joinable()) {
      thread_.join();
    }
  }

  int port() const { return port_; }

 private:
  void WriteJsonResponse(
      comet::platform::SocketHandle client_fd,
      int status_code,
      const json& payload) {
    const std::string body = payload.dump();
    std::ostringstream response;
    response << "HTTP/1.1 " << status_code
             << (status_code >= 200 && status_code < 300 ? " OK" : " ERROR") << "\r\n";
    response << "Content-Type: application/json\r\n";
    response << "Content-Length: " << body.size() << "\r\n";
    response << "Connection: close\r\n\r\n";
    response << body;
    const auto serialized = response.str();
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
      if (!comet::platform::IsSocketValid(client_fd)) {
        return;
      }
      if (stop_requested_.load()) {
        comet::platform::CloseSocket(client_fd);
        return;
      }

      char buffer[4096];
      const auto read_count = recv(client_fd, buffer, sizeof(buffer), 0);
      const std::string request =
          read_count > 0 ? std::string(buffer, static_cast<std::size_t>(read_count)) : "";

      if (request.rfind("GET /health ", 0) == 0) {
        WriteJsonResponse(client_fd, 200, json{{"status", "ok"}});
      } else if (request.rfind("GET /v1/browsing/status ", 0) == 0) {
        WriteJsonResponse(
            client_fd,
            200,
            json{{"status", "ok"},
                 {"service", "comet-browsing"},
                 {"ready", true},
                 {"active_session_count", 0}});
      } else if (request.rfind("POST /v1/browsing/search ", 0) == 0) {
        WriteJsonResponse(
            client_fd,
            200,
            json{{"query", "example"}, {"results", json::array()}});
      } else {
        WriteJsonResponse(
            client_fd,
            404,
            json{{"status", "error"}, {"error", {{"code", "not_found"}}}});
      }
      comet::platform::CloseSocket(client_fd);
    }
  }

  std::atomic<bool> stop_requested_{false};
  comet::platform::SocketHandle listen_fd_ = comet::platform::kInvalidSocket;
  int port_ = 0;
  std::thread thread_;
};

class InteractionBrowsingRuntimeTestServer {
 public:
  InteractionBrowsingRuntimeTestServer() {
    comet::platform::EnsureSocketsInitialized();
    listen_fd_ = socket(AF_INET, SOCK_STREAM, 0);
    if (!comet::platform::IsSocketValid(listen_fd_)) {
      throw std::runtime_error("failed to create interaction browsing test socket");
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
      const auto error = comet::platform::LastSocketErrorMessage();
      comet::platform::CloseSocket(listen_fd_);
      throw std::runtime_error("failed to bind interaction browsing test socket: " + error);
    }
    if (listen(listen_fd_, 8) != 0) {
      const auto error = comet::platform::LastSocketErrorMessage();
      comet::platform::CloseSocket(listen_fd_);
      throw std::runtime_error("failed to listen on interaction browsing test socket: " + error);
    }

    sockaddr_in bound_addr{};
    socklen_t bound_size = sizeof(bound_addr);
    if (getsockname(
            listen_fd_,
            reinterpret_cast<sockaddr*>(&bound_addr),
            &bound_size) != 0) {
      const auto error = comet::platform::LastSocketErrorMessage();
      comet::platform::CloseSocket(listen_fd_);
      throw std::runtime_error("failed to inspect interaction browsing socket: " + error);
    }
    port_ = ntohs(bound_addr.sin_port);
    thread_ = std::thread([this]() { Serve(); });
  }

  ~InteractionBrowsingRuntimeTestServer() {
    stop_requested_.store(true);
    if (port_ > 0) {
      const auto wake_fd = socket(AF_INET, SOCK_STREAM, 0);
      if (comet::platform::IsSocketValid(wake_fd)) {
        sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_port = htons(static_cast<uint16_t>(port_));
        addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        (void)connect(wake_fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr));
        comet::platform::CloseSocket(wake_fd);
      }
    }
    if (comet::platform::IsSocketValid(listen_fd_)) {
      comet::platform::CloseSocket(listen_fd_);
    }
    if (thread_.joinable()) {
      thread_.join();
    }
  }

  int port() const { return port_; }

  int search_count() const { return search_count_.load(); }
  int fetch_count() const { return fetch_count_.load(); }

 private:
  static std::string ExtractBody(const std::string& request) {
    const std::size_t split = request.find("\r\n\r\n");
    if (split == std::string::npos) {
      return "";
    }
    return request.substr(split + 4);
  }

  void WriteJsonResponse(
      comet::platform::SocketHandle client_fd,
      int status_code,
      const json& payload) {
    const std::string body = payload.dump();
    std::ostringstream response;
    response << "HTTP/1.1 " << status_code
             << (status_code >= 200 && status_code < 300 ? " OK" : " ERROR") << "\r\n";
    response << "Content-Type: application/json\r\n";
    response << "Content-Length: " << body.size() << "\r\n";
    response << "Connection: close\r\n\r\n";
    response << body;
    const auto serialized = response.str();
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
      if (!comet::platform::IsSocketValid(client_fd)) {
        return;
      }
      if (stop_requested_.load()) {
        comet::platform::CloseSocket(client_fd);
        return;
      }

      char buffer[8192];
      const auto read_count = recv(client_fd, buffer, sizeof(buffer), 0);
      const std::string request =
          read_count > 0 ? std::string(buffer, static_cast<std::size_t>(read_count)) : "";
      const std::string body = ExtractBody(request);

      if (request.rfind("GET /health ", 0) == 0) {
        WriteJsonResponse(client_fd, 200, json{{"status", "ok"}});
      } else if (request.rfind("GET /v1/browsing/status ", 0) == 0) {
        WriteJsonResponse(
            client_fd,
            200,
            json{{"status", "ok"},
                 {"service", "comet-browsing"},
                 {"ready", true},
                 {"search_enabled", true},
                 {"fetch_enabled", true}});
      } else if (request.rfind("POST /v1/browsing/search ", 0) == 0) {
        ++search_count_;
        const json search_payload =
            body.empty() ? json::object() : json::parse(body, nullptr, false);
        WriteJsonResponse(
            client_fd,
            200,
            json{{"query", search_payload.value("query", std::string{})},
                 {"results",
                  json::array({json{{"url", "https://example.com/article"},
                                    {"domain", "example.com"},
                                    {"title", "Example Article"},
                                    {"snippet", "Example snippet"},
                                    {"score", 0.9}}})}});
      } else if (request.rfind("POST /v1/browsing/fetch ", 0) == 0) {
        ++fetch_count_;
        const json fetch_payload =
            body.empty() ? json::object() : json::parse(body, nullptr, false);
        const std::string requested_url = fetch_payload.value("url", std::string{});
        WriteJsonResponse(
            client_fd,
            200,
            json{{"url", requested_url},
                 {"final_url", requested_url},
                 {"content_type", "text/html"},
                 {"title", "Example Article"},
                 {"visible_text",
                  "Example visible text about the requested topic from a safe test page."},
                 {"citations", json::array({requested_url})},
                 {"injection_flags", json::array()}});
      } else {
        WriteJsonResponse(
            client_fd,
            404,
            json{{"status", "error"}, {"error", {{"code", "not_found"}}}});
      }
      comet::platform::CloseSocket(client_fd);
    }
  }

  std::atomic<bool> stop_requested_{false};
  std::atomic<int> search_count_{0};
  std::atomic<int> fetch_count_{0};
  comet::platform::SocketHandle listen_fd_ = comet::platform::kInvalidSocket;
  int port_ = 0;
  std::thread thread_;
};

comet::DesiredState BuildDesiredStateWithSkillsPort(
    const std::string& host_ip,
    const int host_port) {
  comet::DesiredState desired_state;
  desired_state.plane_name = "maglev";
  desired_state.plane_mode = comet::PlaneMode::Llm;
  comet::SkillsSettings skills_settings;
  skills_settings.enabled = true;
  desired_state.skills = skills_settings;

  comet::InstanceSpec skills;
  skills.name = "skills-maglev";
  skills.plane_name = "maglev";
  skills.node_name = "local-hostd";
  skills.role = comet::InstanceRole::Skills;
  comet::PublishedPort published_port;
  published_port.host_ip = host_ip;
  published_port.host_port = host_port;
  published_port.container_port = 18120;
  skills.published_ports.push_back(published_port);
  desired_state.instances.push_back(skills);
  return desired_state;
}

comet::DesiredState BuildDesiredStateWithBrowsingPort(
    const std::string& host_ip,
    const int host_port) {
  comet::DesiredState desired_state;
  desired_state.plane_name = "maglev";
  desired_state.plane_mode = comet::PlaneMode::Llm;
  comet::BrowsingSettings settings;
  settings.enabled = true;
  comet::BrowsingPolicySettings policy;
  policy.browser_session_enabled = true;
  settings.policy = policy;
  desired_state.browsing = settings;

  comet::InstanceSpec browsing;
  browsing.name = "browsing-maglev";
  browsing.plane_name = "maglev";
  browsing.node_name = "local-hostd";
  browsing.role = comet::InstanceRole::Browsing;
  comet::PublishedPort published_port;
  published_port.host_ip = host_ip;
  published_port.host_port = host_port;
  published_port.container_port = 18130;
  browsing.published_ports.push_back(published_port);
  desired_state.instances.push_back(browsing);
  return desired_state;
}

comet::DesiredState BuildDesiredState(
    const std::string& plane_name,
    const std::vector<std::string>& factory_skill_ids,
    bool skills_enabled = true) {
  comet::DesiredState desired_state;
  desired_state.plane_name = plane_name;
  desired_state.plane_mode = comet::PlaneMode::Llm;
  if (skills_enabled || !factory_skill_ids.empty()) {
    comet::SkillsSettings settings;
    settings.enabled = skills_enabled;
    settings.factory_skill_ids = factory_skill_ids;
    desired_state.skills = settings;
  }
  return desired_state;
}

std::string MakeTempDbPath() {
  const auto root =
      fs::temp_directory_path() / "comet-plane-skills-service-tests";
  fs::create_directories(root);
  return (root / "controller.sqlite").string();
}

}  // namespace

int main() {
  try {
    comet::controller::PlaneSkillsService service;

    {
      const auto target =
          service.ResolveTarget(BuildDesiredStateWithSkillsPort("127.0.0.1", 27978));
      Expect(target.has_value(), "skills target should resolve when a published port exists");
      Expect(target->host == "127.0.0.1", "skills target host should use published host_ip");
      Expect(target->port == 27978, "skills target port should use published host_port");
      Expect(
          target->raw == "http://127.0.0.1:27978",
          "skills target raw URL should use normalized published endpoint");
      std::cout << "ok: published-host-ip-target" << '\n';
    }

    {
      const auto target =
          service.ResolveTarget(BuildDesiredStateWithSkillsPort("0.0.0.0", 27978));
      Expect(target.has_value(), "skills target should resolve for wildcard published host_ip");
      Expect(
          target->host == "127.0.0.1",
          "skills target host should normalize wildcard host_ip for controller probes");
      Expect(
          target->raw == "http://127.0.0.1:27978",
          "skills target raw URL should normalize wildcard host_ip");
      std::cout << "ok: wildcard-host-ip-normalization" << '\n';
    }

    {
      comet::controller::PlaneBrowsingService browsing_service;
      const auto target =
          browsing_service.ResolveTarget(BuildDesiredStateWithBrowsingPort("127.0.0.1", 28130));
      Expect(target.has_value(), "browsing target should resolve when a published port exists");
      Expect(target->host == "127.0.0.1", "browsing target host should use published host_ip");
      Expect(target->port == 28130, "browsing target port should use published host_port");
      Expect(
          target->raw == "http://127.0.0.1:28130",
          "browsing target raw URL should use normalized published endpoint");
      std::cout << "ok: browsing-published-host-ip-target" << '\n';
    }

    {
      comet::controller::PlaneBrowsingService browsing_service;
      const auto target =
          browsing_service.ResolveTarget(BuildDesiredStateWithBrowsingPort("0.0.0.0", 28130));
      Expect(target.has_value(), "browsing target should resolve for wildcard host_ip");
      Expect(
          target->host == "127.0.0.1",
          "browsing target host should normalize wildcard host_ip to loopback");
      std::cout << "ok: browsing-wildcard-host-ip-normalization" << '\n';
    }

    {
      BrowsingRuntimeTestServer runtime;
      comet::controller::PlaneBrowsingService browsing_service;
      const auto desired_state =
          BuildDesiredStateWithBrowsingPort("127.0.0.1", runtime.port());
      const auto payload =
          browsing_service.BuildStatusPayload(desired_state, std::optional<std::string>("running"));
      Expect(payload.value("browsing_enabled", false),
             "browsing status should mark browsing as enabled");
      Expect(payload.value("browsing_ready", false),
             "browsing status should probe runtime readiness");
      Expect(payload.value("service", std::string{}) == "comet-browsing",
             "browsing status should merge runtime payload");
      Expect(payload.value("active_session_count", -1) == 0,
             "browsing status should expose runtime active_session_count");
      std::cout << "ok: browsing-status-merges-runtime-payload" << '\n';
    }

    {
      BrowsingRuntimeTestServer runtime;
      comet::controller::PlaneBrowsingService browsing_service;
      const auto desired_state =
          BuildDesiredStateWithBrowsingPort("127.0.0.1", runtime.port());
      std::string error_code;
      std::string error_message;
      const auto response = browsing_service.ProxyPlaneBrowsingRequest(
          desired_state,
          "POST",
          "/search",
          R"({"query":"example"})",
          &error_code,
          &error_message);
      Expect(response.has_value(), "browsing proxy should return upstream response");
      Expect(response->status_code == 200, "browsing proxy should preserve upstream status");
      const auto payload = json::parse(response->body);
      Expect(payload.at("query").get<std::string>() == "example",
             "browsing proxy should return upstream payload body");
      Expect(payload.at("results").is_array(),
             "browsing proxy should return upstream result list");
      std::cout << "ok: browsing-proxy-search" << '\n';
    }

    {
      SkillRuntimeTestServer runtime(json::array(
          {json{{"id", "code-agent-root-cause-debug"},
                {"name", "root-cause-debug"},
                {"description",
                 "Debug bugs by reproducing them, isolating the invariant, and validating the root cause."},
                {"content",
                 "When handling a bug or regression, reproduce the issue, validate the root cause, and confirm the path."},
                {"enabled", true}}}));
      auto desired_state = BuildDesiredState("catalog-plane", {});
      desired_state.instances = BuildDesiredStateWithSkillsPort("127.0.0.1", runtime.port()).instances;

      comet::controller::PlaneInteractionResolution resolution;
      resolution.desired_state = desired_state;

      const auto selection =
          comet::controller::PlaneSkillContextualResolverService().Resolve(
              "",
              resolution,
              json{{"messages",
                    json::array(
                        {json{{"role", "user"},
                              {"content",
                               "Please debug this regression and find the root cause."}}})}});
      Expect(
          selection.mode == "contextual",
          "resolver should select a contextual skill for a matching prompt");
      Expect(
          selection.candidate_count == 1,
          "resolver should count enabled plane-local candidates");
      Expect(
          selection.selected_skill_ids.size() == 1 &&
              selection.selected_skill_ids.front() ==
                  "code-agent-root-cause-debug",
          "resolver should return the matching plane-local skill id");
      std::cout << "ok: contextual-resolver-selects-plane-local-skill" << '\n';
    }

    {
      SkillRuntimeTestServer runtime(json::array(
          {json{{"id", "code-agent-safe-change"},
                {"name", "safe-change"},
                {"description", "Limit changes to the smallest safe patch."},
                {"content", "Keep the patch minimal and avoid unrelated edits."},
                {"enabled", true}}}));
      auto desired_state = BuildDesiredState("catalog-plane", {});
      desired_state.instances = BuildDesiredStateWithSkillsPort("127.0.0.1", runtime.port()).instances;

      comet::controller::PlaneInteractionResolution resolution;
      resolution.desired_state = desired_state;

      const auto selection =
          comet::controller::PlaneSkillContextualResolverService().Resolve(
              "",
              resolution,
              json{{"messages",
                    json::array(
                        {json{{"role", "user"},
                              {"content",
                               "Summarize the recent rollout status for this plane."}}})}});
      Expect(
          selection.mode == "none" && selection.candidate_count == 1 &&
              selection.selected_skill_ids.empty(),
          "resolver should return none when no candidate clears the score threshold");
      std::cout << "ok: contextual-resolver-no-match" << '\n';
    }

    {
      SkillRuntimeTestServer runtime(json::array(
          {json{{"id", "code-agent-repo-map"},
                {"name", "repo-map"},
                {"description",
                 "Build a fast repository map before non-trivial changes."},
                {"content",
                 "Map the repository structure, entry points, and major dependencies before editing."},
                {"enabled", true}},
           json{{"id", "code-agent-test-first-fix"},
                {"name", "test-first-fix"},
                {"description",
                 "Explain a test-first bug-fix approach without starting execution unless the user explicitly asks for it."},
                {"content",
                 "First define the failing regression test, then make the smallest safe patch."},
                {"enabled", true}}}));
      auto desired_state = BuildDesiredState("catalog-plane", {});
      desired_state.instances =
          BuildDesiredStateWithSkillsPort("127.0.0.1", runtime.port()).instances;

      comet::controller::PlaneInteractionResolution resolution;
      resolution.desired_state = desired_state;

      const auto selection =
          comet::controller::PlaneSkillContextualResolverService().Resolve(
              "",
              resolution,
              json{{"messages",
                    json::array({json{{"role", "user"},
                                      {"content",
                                       "Построй карту репозитория перед тем, как вносить изменения."}}})}});
      Expect(
          selection.mode == "contextual",
          "resolver should support Cyrillic prompts for contextual selection");
      Expect(
          selection.selected_skill_ids.size() == 1 &&
              selection.selected_skill_ids.front() == "code-agent-repo-map",
          "resolver should select repo-map instead of drifting into another skill");
      std::cout << "ok: contextual-resolver-selects-cyrillic-repo-map" << '\n';
    }

    {
      SkillRuntimeTestServer runtime(json::array(
          {json{{"id", "code-agent-deploy-path-check"},
                {"name", "deploy-path-check"},
                {"description",
                 "Explain the real deployment path and required live verification steps before executing them."},
                {"content",
                 "Describe rollout order, rebuild requirements, restarts, pull steps, and live verification."},
                {"enabled", true}}}));
      auto desired_state = BuildDesiredState("catalog-plane", {});
      desired_state.instances =
          BuildDesiredStateWithSkillsPort("127.0.0.1", runtime.port()).instances;

      comet::controller::PlaneInteractionResolution resolution;
      resolution.desired_state = desired_state;

      const auto selection =
          comet::controller::PlaneSkillContextualResolverService().Resolve(
              "",
              resolution,
              json{{"messages",
                    json::array({json{{"role", "user"},
                                      {"content",
                                       "Explain the deployment path and the live verification steps before rollout."}}})}});
      Expect(
          selection.mode == "contextual" &&
              selection.selected_skill_ids.size() == 1 &&
              selection.selected_skill_ids.front() == "code-agent-deploy-path-check",
          "resolver should select deploy-path-check for deployment path prompts");
      std::cout << "ok: contextual-resolver-selects-deploy-path-check" << '\n';
    }

    {
      SkillRuntimeTestServer runtime(json::array(
          {json{{"id", "code-agent-state-schema-guard"},
                {"name", "state-schema-guard"},
                {"description",
                 "Protect desired-state, projectors, validators, renderers, and store changes as one contract."},
                {"content",
                 "Review desired-state schema changes together with the projector, validator, renderer, and store."},
                {"enabled", true}}}));
      auto desired_state = BuildDesiredState("catalog-plane", {});
      desired_state.instances =
          BuildDesiredStateWithSkillsPort("127.0.0.1", runtime.port()).instances;

      comet::controller::PlaneInteractionResolution resolution;
      resolution.desired_state = desired_state;

      const auto selection =
          comet::controller::PlaneSkillContextualResolverService().Resolve(
              "",
              resolution,
              json{{"messages",
                    json::array({json{{"role", "user"},
                                      {"content",
                                       "Check the desired-state schema together with the projector, validator, renderer, and store."}}})}});
      Expect(
          selection.mode == "contextual" &&
              selection.selected_skill_ids.size() == 1 &&
              selection.selected_skill_ids.front() == "code-agent-state-schema-guard",
          "resolver should select state-schema-guard for schema contract prompts");
      std::cout << "ok: contextual-resolver-selects-state-schema-guard" << '\n';
    }

    {
      const auto desired_state = BuildDesiredState("catalog-plane", {});

      comet::controller::PlaneInteractionResolution resolution;
      resolution.desired_state = desired_state;

      const auto selection =
          comet::controller::PlaneSkillContextualResolverService().Resolve(
              "",
              resolution,
              json{{"messages",
                    json::array(
                        {json{{"role", "user"},
                              {"content",
                               "Please map the repository before making changes."}}})}});
      Expect(
          selection.mode == "none" && selection.candidate_count == 0,
          "resolver should ignore skills present only in SkillsFactory");
      std::cout << "ok: contextual-resolver-ignores-factory-only-skills" << '\n';
    }

    {
      SkillRuntimeTestServer runtime(json::array(
          {json{{"id", "code-agent-root-cause-debug"},
                {"name", "root-cause-debug"},
                {"description",
                 "Debug bugs by reproducing them, isolating the invariant, and validating the root cause."},
                {"content",
                 "When handling a bug or regression, reproduce the issue, validate the root cause, and confirm the path."},
                {"enabled", false}}}));
      auto desired_state = BuildDesiredState("catalog-plane", {});
      desired_state.instances = BuildDesiredStateWithSkillsPort("127.0.0.1", runtime.port()).instances;

      comet::controller::PlaneInteractionResolution resolution;
      resolution.desired_state = desired_state;

      const auto selection =
          comet::controller::PlaneSkillContextualResolverService().Resolve(
              "",
              resolution,
              json{{"messages",
                    json::array(
                        {json{{"role", "user"},
                              {"content",
                               "Please debug this regression and find the root cause."}}})}});
      Expect(
          selection.mode == "none" && selection.candidate_count == 0,
          "resolver should exclude disabled plane-local skills");
      std::cout << "ok: contextual-resolver-excludes-disabled-skills" << '\n';
    }

    {
      SkillRuntimeTestServer runtime(json::array(
          {json{{"id", "code-agent-root-cause-debug"},
                {"name", "root-cause-debug"},
                {"description",
                 "Debug bugs by reproducing them, isolating the invariant, and validating the root cause."},
                {"content",
                 "When handling a bug or regression, reproduce the issue, validate the root cause, and confirm the path."},
                {"enabled", true}}}));
      auto desired_state = BuildDesiredState("catalog-plane", {});
      desired_state.instances = BuildDesiredStateWithSkillsPort("127.0.0.1", runtime.port()).instances;

      comet::controller::PlaneInteractionResolution resolution;
      resolution.desired_state = desired_state;

      const auto payload =
          comet::controller::PlaneSkillContextualResolverService().BuildDebugPayload(
              "",
              resolution,
              json{{"prompt", "Please debug this regression and find the root cause."}});
      Expect(
          payload.at("skill_resolution_mode").get<std::string>() == "contextual",
          "debug payload should report contextual mode for a match");
      Expect(
          payload.at("candidate_count").get<int>() == 1,
          "debug payload should include the plane-local candidate count");
      Expect(
          payload.at("selected_skill_ids").size() == 1 &&
              payload.at("selected_skill_ids").front().get<std::string>() ==
                  "code-agent-root-cause-debug",
          "debug payload should report the selected skill id");
      std::cout << "ok: contextual-resolver-debug-payload" << '\n';
    }

    {
      SkillRuntimeTestServer runtime(json::array(
          {json{{"id", "lt-cypher-localtrade-auth-session"},
                {"name", "localtrade-auth-session"},
                {"description",
                 "Use for LocalTrade sign-in state, Access cookie reuse, GET /auth/me, and session validation."},
                {"content",
                 "Check whether the active LocalTrade session is valid and whether the Access cookie is required."},
                {"enabled", true}},
           json{{"id", "lt-cypher-localtrade-user-streams"},
                {"name", "localtrade-user-streams"},
                {"description",
                 "Use for LocalTrade Socket.IO rooms such as balances and user_orders."},
                {"content",
                 "Explain protected rooms, public rooms, and Socket.IO room names."},
                {"enabled", true}},
           json{{"id", "lt-cypher-localtrade-account-balances"},
                {"name", "localtrade-account-balances"},
                {"description",
                 "Use for LocalTrade balances, available balances, and totals."},
                {"content",
                 "Explain balance endpoints and when login is required."},
                {"enabled", true}}}));
      auto desired_state = BuildDesiredState("catalog-plane", {});
      desired_state.instances =
          BuildDesiredStateWithSkillsPort("127.0.0.1", runtime.port()).instances;

      comet::controller::PlaneInteractionResolution resolution;
      resolution.desired_state = desired_state;

      const auto selection =
          comet::controller::PlaneSkillContextualResolverService().Resolve(
              "",
              resolution,
              json{{"prompt",
                    "Проверь авторизацию LocalTrade, активную сессию и Access cookie."}});
      Expect(
          selection.mode == "contextual" &&
              selection.selected_skill_ids.size() == 1 &&
              selection.selected_skill_ids.front() ==
                  "lt-cypher-localtrade-auth-session",
          "resolver should prefer auth-session over other LocalTrade skills for session prompts");
      std::cout << "ok: contextual-resolver-selects-localtrade-auth-session" << '\n';
    }

    {
      SkillRuntimeTestServer runtime(json::array(
          {json{{"id", "skill-alpha"},
                {"name", "generic skill alpha"},
                {"description", "Generic description with no domain hints."},
                {"content", "Generic content."},
                {"match_terms", json::array({"logout", "session end", "выйди", "выйти"})},
                {"enabled", true}},
           json{{"id", "skill-beta"},
                {"name", "generic skill beta"},
                {"description", "Generic description with no domain hints."},
                {"content", "Generic content."},
                {"match_terms", json::array({"subscribe", "follow", "подпиши"})},
                {"enabled", true}}}));
      auto desired_state = BuildDesiredState("catalog-plane", {});
      desired_state.instances =
          BuildDesiredStateWithSkillsPort("127.0.0.1", runtime.port()).instances;

      comet::controller::PlaneInteractionResolution resolution;
      resolution.desired_state = desired_state;

      const auto selection =
          comet::controller::PlaneSkillContextualResolverService().Resolve(
              "",
              resolution,
              json{{"prompt", "Выйди из моей текущей сессии."}});
      Expect(
          selection.mode == "contextual" &&
              selection.selected_skill_ids.size() == 1 &&
              selection.selected_skill_ids.front() == "skill-alpha",
          "resolver should honor skill match_terms from runtime payload");
      std::cout << "ok: contextual-resolver-prefers-runtime-match-terms" << '\n';
    }

    {
      SkillRuntimeTestServer runtime(json::array(
          {json{{"id", "skill-internal"},
                {"name", "technical support skill"},
                {"description", "Internal-only orchestration helper."},
                {"content", "Never expose this support-layer skill directly to end users."},
                {"match_terms", json::array({"session", "cookie", "handshake"})},
                {"internal", true},
                {"enabled", true}},
           json{{"id", "skill-public"},
                {"name", "public skill"},
                {"description", "Normal user-facing skill."},
                {"content", "Handle public requests."},
                {"match_terms", json::array({"public"})},
                {"enabled", true}}}));
      auto desired_state = BuildDesiredState("catalog-plane", {});
      desired_state.instances =
          BuildDesiredStateWithSkillsPort("127.0.0.1", runtime.port()).instances;

      comet::controller::PlaneInteractionResolution resolution;
      resolution.desired_state = desired_state;

      const auto selection =
          comet::controller::PlaneSkillContextualResolverService().Resolve(
              "",
              resolution,
              json{{"prompt", "Проверь сессию, куки и handshake авторизации."}});
      Expect(
          selection.mode == "none" &&
              selection.candidate_count == 1 &&
              selection.selected_skill_ids.empty(),
          "resolver should exclude internal skills from ordinary user-triggered matching");
      std::cout << "ok: contextual-resolver-excludes-internal-skills-by-default" << '\n';
    }

    {
      SkillRuntimeTestServer runtime(json::array(
          {json{{"id", "skill-internal"},
                {"name", "technical support skill"},
                {"description", "Internal-only orchestration helper."},
                {"content", "Never expose this support-layer skill directly to end users."},
                {"match_terms", json::array({"session", "cookie", "handshake"})},
                {"internal", true},
                {"enabled", true}}}));
      auto desired_state = BuildDesiredState("catalog-plane", {});
      desired_state.instances =
          BuildDesiredStateWithSkillsPort("127.0.0.1", runtime.port()).instances;

      comet::controller::PlaneInteractionResolution resolution;
      resolution.desired_state = desired_state;

      const auto selection =
          comet::controller::PlaneSkillContextualResolverService().Resolve(
              "",
              resolution,
              json{{"prompt", "Проверь сессию, куки и handshake авторизации."},
                   {"include_internal", true}});
      Expect(
          selection.mode == "contextual" &&
              selection.candidate_count == 1 &&
              selection.selected_skill_ids.size() == 1 &&
              selection.selected_skill_ids.front() == "skill-internal",
          "resolver should allow internal skills only when explicitly requested by system callers");
      std::cout << "ok: contextual-resolver-allows-internal-skills-with-opt-in" << '\n';
    }

    {
      SkillRuntimeTestServer runtime(json::array(
          {json{{"id", "lt-cypher-localtrade-spot-order-clarification"},
                {"name", "localtrade-spot-order-clarification"},
                {"description",
                 "Use when the user wants to place, inspect, or cancel a LocalTrade spot order."},
                {"content",
                 "Clarify limit order parameters, pairId, side, amount, rate, and require confirmation."},
                {"enabled", true}},
           json{{"id", "lt-cypher-localtrade-market-data"},
                {"name", "localtrade-market-data"},
                {"description",
                 "Use for public pairs, charts, trades, and order book."},
                {"content",
                 "Explain pairs, chart endpoints, and public market-data rooms."},
                {"enabled", true}}}));
      auto desired_state = BuildDesiredState("catalog-plane", {});
      desired_state.instances =
          BuildDesiredStateWithSkillsPort("127.0.0.1", runtime.port()).instances;

      comet::controller::PlaneInteractionResolution resolution;
      resolution.desired_state = desired_state;

      const auto selection =
          comet::controller::PlaneSkillContextualResolverService().Resolve(
              "",
              resolution,
              json{{"prompt",
                    "Хочу поставить лимитный ордер на покупку BTC и собрать параметры перед подтверждением."}});
      Expect(
          selection.mode == "contextual" &&
              !selection.selected_skill_ids.empty() &&
              selection.selected_skill_ids.front() ==
                  "lt-cypher-localtrade-spot-order-clarification",
          "resolver should prefer the LocalTrade spot-order clarification skill for Russian order prompts");
      std::cout << "ok: contextual-resolver-selects-localtrade-spot-order" << '\n';
    }

    {
      SkillRuntimeTestServer runtime(json::array(
          {json{{"id", "lt-cypher-localtrade-market-data"},
                {"name", "localtrade-market-data"},
                {"description",
                 "Use for public LocalTrade pairs, charts, trades, order book, and pairs state without account data."},
                {"content",
                 "Prefer this skill for public pairs, public order book, public trades, and public market streams."},
                {"enabled", true}},
           json{{"id", "lt-cypher-localtrade-user-streams"},
                {"name", "localtrade-user-streams"},
                {"description",
                 "Use for authenticated LocalTrade Socket.IO user rooms such as balances, user_orders, and user_trades."},
                {"content",
                 "Protected rooms require the Access cookie and are for account-specific streams."},
                {"enabled", true}},
           json{{"id", "lt-cypher-localtrade-spot-order-clarification"},
                {"name", "localtrade-spot-order-clarification"},
                {"description",
                 "Use when the user wants to create, inspect, or cancel a LocalTrade spot order."},
                {"content",
                 "Collect limit-order parameters and require explicit confirmation."},
                {"enabled", true}}}));
      auto desired_state = BuildDesiredState("catalog-plane", {});
      desired_state.instances =
          BuildDesiredStateWithSkillsPort("127.0.0.1", runtime.port()).instances;

      comet::controller::PlaneInteractionResolution resolution;
      resolution.desired_state = desired_state;

      const auto selection =
          comet::controller::PlaneSkillContextualResolverService().Resolve(
              "",
              resolution,
              json{{"prompt",
                    "Дай публичные пары и поток order book по BTC/USDT без данных моего аккаунта."}});
      Expect(
          selection.mode == "contextual" &&
              !selection.selected_skill_ids.empty() &&
              selection.selected_skill_ids.front() ==
                  "lt-cypher-localtrade-market-data",
          "resolver should prefer LocalTrade market-data over user-streams for public market prompts");
      std::cout << "ok: contextual-resolver-prefers-localtrade-market-data" << '\n';
    }

    {
      SkillRuntimeTestServer runtime(json::array(
          {json{{"id", "lt-cypher-localtrade-account-balances"},
                {"name", "localtrade-account-balances"},
                {"description",
                 "Use for LocalTrade balances, available balances, and totals."},
                {"content",
                 "Explain balances endpoints and whether login is required."},
                {"enabled", true}},
           json{{"id", "lt-cypher-localtrade-auth-session"},
                {"name", "localtrade-auth-session"},
                {"description",
                 "Use for LocalTrade sign-in state, Access cookie reuse, GET /auth/me, and session validation."},
                {"content",
                 "Check whether the active LocalTrade session is valid and whether the Access cookie is required."},
                {"enabled", true}}}));
      auto desired_state = BuildDesiredState("catalog-plane", {});
      desired_state.instances =
          BuildDesiredStateWithSkillsPort("127.0.0.1", runtime.port()).instances;

      comet::controller::PlaneInteractionResolution resolution;
      resolution.desired_state = desired_state;

      const auto selection =
          comet::controller::PlaneSkillContextualResolverService().Resolve(
              "",
              resolution,
              json{{"prompt",
                    "Покажи мои доступные балансы на LocalTrade и скажи, нужен ли логин."}});
      Expect(
          selection.mode == "contextual" &&
              !selection.selected_skill_ids.empty() &&
              selection.selected_skill_ids.front() ==
                  "lt-cypher-localtrade-account-balances",
          "resolver should prefer LocalTrade account-balances over auth-session for balance prompts");
      std::cout << "ok: contextual-resolver-prefers-localtrade-account-balances" << '\n';
    }

    {
      SkillRuntimeTestServer runtime(json::array(
          {json{{"id", "lt-cypher-localtrade-copy-trading-discovery"},
                {"name", "localtrade-copy-trading-discovery"},
                {"description",
                 "Use to discover, compare, and filter LocalTrade copy-trading traders by ROI, drawdown, sharpe ratio, subscribers, and PnL."},
                {"content",
                 "Discovery is read-only and should not be confused with follow or subscribe actions."},
                {"enabled", true}},
           json{{"id", "lt-cypher-localtrade-copy-trading-actions"},
                {"name", "localtrade-copy-trading-actions"},
                {"description",
                 "Use to follow, unfollow, or subscribe to a LocalTrade trader and require confirmation before any write action."},
                {"content",
                 "These are write actions and need explicit confirmation."},
                {"enabled", true}}}));
      auto desired_state = BuildDesiredState("catalog-plane", {});
      desired_state.instances =
          BuildDesiredStateWithSkillsPort("127.0.0.1", runtime.port()).instances;

      comet::controller::PlaneInteractionResolution resolution;
      resolution.desired_state = desired_state;

      const auto selection =
          comet::controller::PlaneSkillContextualResolverService().Resolve(
              "",
              resolution,
              json{{"prompt",
                    "Найди и сравни сильных трейдеров для копитрейдинга по ROI и drawdown."}});
      Expect(
          selection.mode == "contextual" &&
              !selection.selected_skill_ids.empty() &&
              selection.selected_skill_ids.front() ==
                  "lt-cypher-localtrade-copy-trading-discovery",
          "resolver should prefer LocalTrade copy-trading discovery over write actions for comparison prompts");
      std::cout << "ok: contextual-resolver-prefers-localtrade-copy-discovery" << '\n';
    }

    {
      SkillRuntimeTestServer runtime(json::array(
          {json{{"id", "lt-cypher-localtrade-copy-trading-actions"},
                {"name", "localtrade-copy-trading-actions"},
                {"description",
                 "Use to follow, unfollow, or subscribe to a LocalTrade trader and require confirmation before any write action."},
                {"content",
                 "These are write actions and need explicit confirmation."},
                {"enabled", true}},
           json{{"id", "lt-cypher-localtrade-copy-trading-discovery"},
                {"name", "localtrade-copy-trading-discovery"},
                {"description",
                 "Use to discover, compare, and filter LocalTrade copy-trading traders by ROI, drawdown, sharpe ratio, subscribers, and PnL."},
                {"content",
                 "Discovery is read-only and should not be confused with follow or subscribe actions."},
                {"enabled", true}}}));
      auto desired_state = BuildDesiredState("catalog-plane", {});
      desired_state.instances =
          BuildDesiredStateWithSkillsPort("127.0.0.1", runtime.port()).instances;

      comet::controller::PlaneInteractionResolution resolution;
      resolution.desired_state = desired_state;

      const auto selection =
          comet::controller::PlaneSkillContextualResolverService().Resolve(
              "",
              resolution,
              json{{"prompt",
                    "Подпиши меня на этого трейдера и сначала запроси подтверждение."}});
      Expect(
          selection.mode == "contextual" &&
              !selection.selected_skill_ids.empty() &&
              selection.selected_skill_ids.front() ==
                  "lt-cypher-localtrade-copy-trading-actions",
          "resolver should prefer LocalTrade copy-trading actions for subscribe prompts");
      std::cout << "ok: contextual-resolver-prefers-localtrade-copy-actions" << '\n';
    }

    {
      SkillRuntimeTestServer runtime(json::array(
          {json{{"id", "lt-cypher-localtrade-auth-session"},
                {"name", "localtrade-auth-session"},
                {"description",
                 "Use for LocalTrade sign-in state, Access cookie reuse, GET /auth/me, and logout confirmation."},
                {"content",
                 "Check the current session, explain logout, and require confirmation before state-changing session actions."},
                {"enabled", true}},
           json{{"id", "lt-cypher-localtrade-copy-trading-actions"},
                {"name", "localtrade-copy-trading-actions"},
                {"description",
                 "Use to follow, unfollow, or subscribe to a LocalTrade trader and require confirmation before any write action."},
                {"content",
                 "These are write actions and need explicit confirmation."},
                {"enabled", true}}}));
      auto desired_state = BuildDesiredState("catalog-plane", {});
      desired_state.instances =
          BuildDesiredStateWithSkillsPort("127.0.0.1", runtime.port()).instances;

      comet::controller::PlaneInteractionResolution resolution;
      resolution.desired_state = desired_state;

      const auto selection =
          comet::controller::PlaneSkillContextualResolverService().Resolve(
              "",
              resolution,
              json{{"prompt",
                    "Выйди из моей сессии LocalTrade и сначала запроси подтверждение."}});
      Expect(
          selection.mode == "contextual" &&
              !selection.selected_skill_ids.empty() &&
              selection.selected_skill_ids.front() ==
                  "lt-cypher-localtrade-auth-session",
          "resolver should prefer LocalTrade auth-session for logout prompts");
      std::cout << "ok: contextual-resolver-prefers-localtrade-logout" << '\n';
    }

    {
      SkillRuntimeTestServer runtime(json::array(
          {json{{"id", "lt-cypher-localtrade-user-streams"},
                {"name", "localtrade-user-streams"},
                {"description",
                 "Use for protected LocalTrade Socket.IO user rooms such as balances, user_orders, user_trades, and authenticated private channels."},
                {"content",
                 "Explain protected user rooms and the Access cookie requirement."},
                {"enabled", true}},
           json{{"id", "lt-cypher-localtrade-account-balances"},
                {"name", "localtrade-account-balances"},
                {"description",
                 "Use for LocalTrade balances, available balances, and totals."},
                {"content",
                 "Explain balances endpoints and whether login is required."},
                {"enabled", true}},
           json{{"id", "lt-cypher-localtrade-market-data"},
                {"name", "localtrade-market-data"},
                {"description",
                 "Use for public LocalTrade pairs, charts, trades, order book, and pairs state without account data."},
                {"content",
                 "Prefer this skill for public market feeds."},
                {"enabled", true}}}));
      auto desired_state = BuildDesiredState("catalog-plane", {});
      desired_state.instances =
          BuildDesiredStateWithSkillsPort("127.0.0.1", runtime.port()).instances;

      comet::controller::PlaneInteractionResolution resolution;
      resolution.desired_state = desired_state;

      const auto selection =
          comet::controller::PlaneSkillContextualResolverService().Resolve(
              "",
              resolution,
              json{{"prompt",
                    "Какие пользовательские websocket-каналы нужны для приватных обновлений балансов и ордеров?"}});
      Expect(
          selection.mode == "contextual" &&
              !selection.selected_skill_ids.empty() &&
              selection.selected_skill_ids.front() ==
                  "lt-cypher-localtrade-user-streams",
          "resolver should prefer LocalTrade user-streams over balances and public market data for private channel prompts");
      std::cout << "ok: contextual-resolver-prefers-localtrade-user-streams" << '\n';
    }

    {
      SkillRuntimeTestServer runtime(json::array(
          {json{{"id", "lt-cypher-market-overview-report"},
                {"name", "market-overview-report"},
                {"description",
                 "Use when a request asks for the current state of the broader crypto market or a market overview."},
                {"content",
                 "Answer with a broad market view using the supplied market package."},
                {"match_terms",
                 json::array({"обзор рынка", "состояние рынка", "market overview"})},
                {"enabled", true}},
           json{{"id", "lt-cypher-market-asset-report"},
                {"name", "asset-market-report"},
                {"description",
                 "Use when a request asks for the current state of one tracked asset such as BTC or ETH."},
                {"content",
                 "Answer with a single-asset report and keep broader and venue data distinct."},
                {"match_terms",
                 json::array({"отчет по btc", "report on btc", "state of eth"})},
                {"enabled", true}}}));
      auto desired_state = BuildDesiredState("catalog-plane", {});
      desired_state.instances =
          BuildDesiredStateWithSkillsPort("127.0.0.1", runtime.port()).instances;

      comet::controller::PlaneInteractionResolution resolution;
      resolution.desired_state = desired_state;

      const auto selection =
          comet::controller::PlaneSkillContextualResolverService().Resolve(
              "",
              resolution,
              json{{"prompt", "Сделай обзор рынка по основным валютам"}});
      Expect(
          selection.mode == "contextual" &&
              !selection.selected_skill_ids.empty() &&
              selection.selected_skill_ids.front() ==
                  "lt-cypher-market-overview-report",
          "resolver should ignore stopwords and prefer the broad market overview skill");
      Expect(
          std::find(
              selection.selected_skill_ids.begin(),
              selection.selected_skill_ids.end(),
              "lt-cypher-market-asset-report") ==
              selection.selected_skill_ids.end(),
          "resolver should not select a single-asset report only because of stopwords in match terms");
      std::cout << "ok: contextual-resolver-ignores-stopword-market-overlaps" << '\n';
    }

    {
      SkillRuntimeTestServer runtime(json::array(
          {json{{"id", "lt-cypher-market-forecast"},
                {"name", "asset-market-forecast"},
                {"description",
                 "Use when a request asks for a short-term forecast or directional view for BTC or ETH using CoinGecko and LocalTrade."},
                {"content",
                 "Give a probabilistic short-term forecast and state the main invalidation risk."},
                {"match_terms",
                 json::array({"прогноз", "куда дальше", "bullish or bearish"})},
                {"enabled", true}},
           json{{"id", "lt-cypher-market-asset-report"},
                {"name", "asset-market-report"},
                {"description",
                 "Use when a request asks for the current state of one tracked asset such as BTC or ETH."},
                {"content",
                 "Answer with a single-asset report and keep broader and venue data distinct."},
                {"match_terms",
                 json::array({"отчет по btc", "report on btc", "state of eth"})},
                {"enabled", true}},
           json{{"id", "lt-cypher-market-source-mix"},
                {"name", "asset-source-mix"},
                {"description",
                 "Use when a request explicitly asks to compare or mix CoinGecko and LocalTrade for the same asset."},
                {"content",
                 "Explain where CoinGecko and LocalTrade agree or diverge."},
                {"match_terms",
                 json::array({"смешай coingecko и localtrade", "coingecko vs localtrade"})},
                {"enabled", true}}}));
      auto desired_state = BuildDesiredState("catalog-plane", {});
      desired_state.instances =
          BuildDesiredStateWithSkillsPort("127.0.0.1", runtime.port()).instances;

      comet::controller::PlaneInteractionResolution resolution;
      resolution.desired_state = desired_state;

      const auto selection =
          comet::controller::PlaneSkillContextualResolverService().Resolve(
              "",
              resolution,
              json{{"prompt",
                    "Сделай прогноз по BTC и смешай CoinGecko с данными LocalTrade"}});
      Expect(
          selection.mode == "contextual" &&
              selection.selected_skill_ids.size() == 1 &&
              selection.selected_skill_ids.front() == "lt-cypher-market-forecast",
          "resolver should keep only the dominant forecast skill when weaker market skills trail far behind");
      std::cout << "ok: contextual-resolver-prefers-single-dominant-market-skill" << '\n';
    }

    {
      const std::string db_path = MakeTempDbPath();
      fs::remove(db_path);
      comet::ControllerStore store(db_path);
      store.Initialize();
      const auto desired_state = BuildDesiredState(
          "catalog-plane", {"code-agent-root-cause-debug"});
      store.ReplaceDesiredState(desired_state, 1);
      comet::SkillsFactorySkillRecord record;
      record.id = "code-agent-root-cause-debug";
      record.name = "root-cause-debug";
      record.description =
          "Debug bugs by reproducing them, isolating the invariant, and validating the root cause.";
      record.content =
          "When handling a bug or regression, reproduce the issue, validate the root cause, and confirm the path.";
      store.UpsertSkillsFactorySkill(record);

      comet::controller::PlaneInteractionResolution resolution;
      resolution.db_path = db_path;
      resolution.desired_state = desired_state;

      const auto selection =
          comet::controller::PlaneSkillContextualResolverService().Resolve(
              db_path,
              resolution,
              json{{"messages",
                    json::array(
                        {json{{"role", "user"},
                              {"content",
                               "Please debug this regression and find the root cause."}}})}});
      Expect(
          selection.mode == "none" && selection.candidate_count == 0,
          "resolver should ignore controller catalog entries that are not present in runtime skillsd");
      std::cout << "ok: contextual-resolver-requires-runtime-skill-copy" << '\n';
    }

    {
      const auto payload =
          comet::controller::PlaneDashboardSkillsSummaryService::BuildPayload(
          BuildDesiredState("dashboard-plane", {"skill-alpha", "skill-beta"}, true),
          std::vector<comet::PlaneSkillBindingRecord>{
              comet::PlaneSkillBindingRecord{
                  "dashboard-plane", "skill-alpha", false, {}, {}, "", ""},
              comet::PlaneSkillBindingRecord{
                  "dashboard-plane", "skill-beta", true, {}, {}, "", ""},
          });
      Expect(
          payload.at("enabled").get<bool>(),
          "dashboard skills payload should report enabled state");
      Expect(
          payload.at("enabled_count").get<int>() == 1,
          "dashboard skills payload should count enabled plane-local skills");
      Expect(
          payload.at("total_count").get<int>() == 2,
          "dashboard skills payload should count attached plane-local skills");
      std::cout << "ok: dashboard-skills-counts" << '\n';
    }

    {
      const auto payload =
          comet::controller::PlaneDashboardSkillsSummaryService::BuildPayload(
          BuildDesiredState("dashboard-plane", {"skill-alpha"}, false),
          {});
      Expect(
          !payload.at("enabled").get<bool>(),
          "dashboard skills payload should report disabled state");
      Expect(
          payload.at("enabled_count").get<int>() == 0,
          "dashboard skills payload should show zero enabled count when disabled");
      std::cout << "ok: dashboard-skills-disabled" << '\n';
    }

    {
      comet::controller::InteractionRequestValidator validator;
      comet::controller::InteractionRequestContext request_context;
      comet::controller::PlaneInteractionResolution resolution;
      resolution.desired_state = BuildDesiredState("interaction-plane", {});
      resolution.status_payload = json::object();
      const auto error = validator.ValidateAndNormalizeRequest(
          resolution,
          json{{"messages", json::array()}},
          &request_context);
      Expect(!error.has_value(), "validator should accept a basic interaction request");
      Expect(
          request_context.payload.at("auto_skills").get<bool>(),
          "validator should default auto_skills to true");
      std::cout << "ok: interaction-validator-default-auto-skills" << '\n';
    }

    {
      comet::controller::InteractionRequestValidator validator;
      comet::controller::InteractionRequestContext request_context;
      comet::controller::PlaneInteractionResolution resolution;
      resolution.desired_state = BuildDesiredState("interaction-plane", {});
      resolution.status_payload = json::object();
      const auto error = validator.ValidateAndNormalizeRequest(
          resolution,
          json{{"messages", json::array()},
               {"auto_skills", "yes"}},
          &request_context);
      Expect(
          error.has_value() && error->code == "malformed_request",
          "validator should reject non-boolean auto_skills");
      std::cout << "ok: interaction-validator-rejects-malformed-auto-skills" << '\n';
    }

    {
      comet::controller::InteractionRequestValidator validator;
      comet::controller::InteractionRequestContext request_context;
      comet::controller::PlaneInteractionResolution resolution;
      resolution.desired_state = BuildDesiredState("interaction-plane", {});
      resolution.status_payload = json::object();
      const auto error = validator.ValidateAndNormalizeRequest(
          resolution,
          json{{"messages", json::array()},
               {"auto_skills", false}},
          &request_context);
      Expect(!error.has_value(), "validator should accept auto_skills=false");
      Expect(
          !request_context.payload.at("auto_skills").get<bool>(),
          "validator should preserve explicit auto_skills=false");
      std::cout << "ok: interaction-validator-preserves-auto-skills-false" << '\n';
    }

    {
      comet::controller::InteractionSessionPresenter presenter;
      comet::controller::InteractionRequestContext request_context;
      request_context.request_id = "req-1";
      request_context.payload = json{
          {comet::controller::PlaneSkillsService::kAppliedSkillsPayloadKey,
           json::array({json{{"id", "skill-alpha"}, {"name", "safe-change"}}})},
          {comet::controller::PlaneSkillsService::kAutoAppliedSkillsPayloadKey,
           json::array({json{{"id", "skill-alpha"}, {"name", "safe-change"}}})},
          {comet::controller::PlaneSkillsService::kSkillResolutionModePayloadKey,
           "contextual"},
      };
      comet::controller::InteractionSessionResult result;
      result.session_id = "sess-1";
      result.model = "demo-model";
      result.content = "ok";
      result.completion_status = "completed";
      result.final_finish_reason = "stop";
      result.stop_reason = "natural_stop";
      comet::controller::PlaneInteractionResolution resolution;
      resolution.status_payload = json::object();
      const auto response = presenter.BuildResponseSpec(
          resolution,
          request_context,
          result);
      Expect(
          response.payload.at("auto_applied_skills").is_array(),
          "interaction response should expose auto_applied_skills");
      Expect(
          response.payload.at("skill_resolution_mode").get<std::string>() ==
              "contextual",
          "interaction response should expose skill_resolution_mode");
      std::cout << "ok: interaction-response-includes-skill-resolution-fields" << '\n';
    }

    {
      comet::controller::InteractionBrowsingService browsing_service;
      comet::controller::InteractionRequestContext request_context;
      request_context.payload = json{
          {"messages",
           json::array({json{{"role", "user"}, {"content", "Explain TCP handshakes."}}})},
      };
      comet::controller::PlaneInteractionResolution resolution;
      resolution.desired_state = BuildDesiredState("interaction-plane", {});
      const auto error =
          browsing_service.ResolveInteractionBrowsing(resolution, &request_context);
      Expect(!error.has_value(), "browsing resolver should not fail for default-off mode");
      const auto summary =
          request_context.payload.at(
              comet::controller::InteractionBrowsingService::kSummaryPayloadKey);
      Expect(
          summary.at("mode").get<std::string>() == "disabled",
          "browsing resolver should keep web mode disabled by default");
      Expect(
          summary.at("decision").get<std::string>() == "disabled",
          "browsing resolver should mark disabled decision when web mode is off");
      std::cout << "ok: interaction-browsing-default-off" << '\n';
    }

    {
      comet::controller::InteractionBrowsingService browsing_service;
      comet::controller::InteractionRequestContext request_context;
      request_context.payload = json{
          {"messages",
           json::array({json{{"role", "user"},
                             {"content", "Включи веб для этого разговора."}}})},
      };
      comet::controller::PlaneInteractionResolution resolution;
      resolution.desired_state = BuildDesiredStateWithBrowsingPort("127.0.0.1", 18130);
      const auto error =
          browsing_service.ResolveInteractionBrowsing(resolution, &request_context);
      Expect(!error.has_value(), "browsing resolver should accept a toggle-only request");
      const auto summary =
          request_context.payload.at(
              comet::controller::InteractionBrowsingService::kSummaryPayloadKey);
      Expect(
          summary.at("mode").get<std::string>() == "enabled",
          "browsing resolver should enable web mode from Russian toggle text");
      Expect(
          summary.at("decision").get<std::string>() == "not_needed",
          "toggle-only requests should not trigger a web lookup");
      Expect(
          request_context.payload.at(
              comet::controller::InteractionBrowsingService::kSystemInstructionPayloadKey)
                  .get<std::string>()
                  .find("enabled") != std::string::npos,
          "toggle-only requests should inject an enable acknowledgement instruction");
      std::cout << "ok: interaction-browsing-toggle-only-enable" << '\n';
    }

    {
      InteractionBrowsingRuntimeTestServer runtime;
      comet::controller::InteractionBrowsingService browsing_service;
      comet::controller::InteractionRequestContext request_context;
      request_context.payload = json{
          {"messages",
           json::array(
               {json{{"role", "user"}, {"content", "Enable web for this chat."}},
                json{{"role", "user"},
                     {"content", "What is the latest update on OpenAI models?"}}})},
      };
      comet::controller::PlaneInteractionResolution resolution;
      resolution.desired_state =
          BuildDesiredStateWithBrowsingPort("127.0.0.1", runtime.port());
      const auto error =
          browsing_service.ResolveInteractionBrowsing(resolution, &request_context);
      Expect(!error.has_value(), "browsing resolver should complete search/fetch enrichment");
      const auto summary =
          request_context.payload.at(
              comet::controller::InteractionBrowsingService::kSummaryPayloadKey);
      Expect(
          summary.at("mode").get<std::string>() == "enabled",
          "web toggle should persist across message history");
      Expect(
          summary.at("decision").get<std::string>() == "search_and_fetch",
          "latest info prompts should trigger search-and-fetch browsing");
      Expect(runtime.search_count() == 1, "search-and-fetch should call search once");
      Expect(runtime.fetch_count() >= 1, "search-and-fetch should fetch at least one result");
      Expect(
          summary.at("sources").is_array() && !summary.at("sources").empty(),
          "search-and-fetch should expose fetched sources");
      std::cout << "ok: interaction-browsing-search-and-fetch" << '\n';
    }

    {
      InteractionBrowsingRuntimeTestServer runtime;
      comet::controller::InteractionBrowsingService browsing_service;
      comet::controller::InteractionRequestContext request_context;
      request_context.payload = json{
          {"messages",
           json::array(
               {json{{"role", "user"},
                     {"content", "Use the web and check https://example.com/article"}}})},
      };
      comet::controller::PlaneInteractionResolution resolution;
      resolution.desired_state =
          BuildDesiredStateWithBrowsingPort("127.0.0.1", runtime.port());
      const auto error =
          browsing_service.ResolveInteractionBrowsing(resolution, &request_context);
      Expect(!error.has_value(), "direct fetch browsing should succeed");
      const auto summary =
          request_context.payload.at(
              comet::controller::InteractionBrowsingService::kSummaryPayloadKey);
      Expect(
          summary.at("decision").get<std::string>() == "direct_fetch",
          "explicit URL requests should skip search and fetch directly");
      Expect(runtime.search_count() == 0, "direct fetch should not call search");
      Expect(runtime.fetch_count() == 1, "direct fetch should fetch the referenced URL");
      std::cout << "ok: interaction-browsing-direct-fetch" << '\n';
    }

    {
      InteractionBrowsingRuntimeTestServer runtime;
      comet::controller::InteractionBrowsingService browsing_service;
      comet::controller::InteractionRequestContext request_context;
      request_context.request_id = "req-1";
      request_context.payload = json{
          {"messages",
           json::array(
               {json{{"role", "user"}, {"content", "Enable web."}},
                json{{"role", "user"},
                     {"content", "Disable web. What is the latest update on OpenAI models?"}}})},
      };
      comet::controller::PlaneInteractionResolution resolution;
      resolution.desired_state =
          BuildDesiredStateWithBrowsingPort("127.0.0.1", runtime.port());
      const auto error =
          browsing_service.ResolveInteractionBrowsing(resolution, &request_context);
      Expect(!error.has_value(), "disable override should not fail");
      const auto summary =
          request_context.payload.at(
              comet::controller::InteractionBrowsingService::kSummaryPayloadKey);
      Expect(
          summary.at("mode").get<std::string>() == "disabled",
          "later disable directive should override earlier enable directive");
      Expect(runtime.search_count() == 0, "disabled web mode should prevent search");
      Expect(runtime.fetch_count() == 0, "disabled web mode should prevent fetch");

      comet::controller::InteractionSessionPresenter presenter;
      comet::controller::InteractionSessionResult result;
      result.session_id = "sess-web";
      result.model = "demo-model";
      result.content = "ok";
      result.completion_status = "completed";
      result.final_finish_reason = "stop";
      result.stop_reason = "natural_stop";
      resolution.status_payload = json::object();
      const auto response =
          presenter.BuildResponseSpec(resolution, request_context, result);
      Expect(
          response.payload.at("browsing").at("mode").get<std::string>() == "disabled",
          "interaction response should expose browsing summary at top level");
      Expect(
          response.payload.at("session").at("browsing").at("decision").get<std::string>() ==
              "disabled",
          "interaction session payload should expose browsing summary");
      std::cout << "ok: interaction-response-includes-browsing-fields" << '\n';
    }

    return 0;
  } catch (const std::exception& error) {
    std::cerr << "plane_skills_service_tests failed: " << error.what() << '\n';
    return 1;
  }
}
