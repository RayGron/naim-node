#include <filesystem>
#include <iostream>
#include <atomic>
#include <cctype>
#include <map>
#include <mutex>
#include <set>
#include <sstream>
#include <stdexcept>
#include <string>
#include <thread>

#include <nlohmann/json.hpp>

#include "comet/core/platform_compat.h"
#include "comet/state/sqlite_store.h"
#include "browsing/interaction_browsing_service.h"
#include "interaction/interaction_payload_builder.h"
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

std::string LowercaseAscii(std::string value) {
  for (char& ch : value) {
    ch = static_cast<char>(std::tolower(static_cast<unsigned char>(ch)));
  }
  return value;
}

bool ContainsAscii(
    const std::string& haystack,
    const std::string& needle) {
  return LowercaseAscii(haystack).find(LowercaseAscii(needle)) != std::string::npos;
}

[[maybe_unused]] bool ContainsAnyAscii(
    const std::string& haystack,
    const std::vector<std::string>& needles) {
  return std::any_of(
      needles.begin(),
      needles.end(),
      [&](const std::string& needle) { return ContainsAscii(haystack, needle); });
}

[[maybe_unused]] std::vector<std::string> ExtractUrls(const std::string& text) {
  std::vector<std::string> urls;
  std::size_t position = 0;
  while (position < text.size()) {
    const std::size_t http_pos = text.find("http://", position);
    const std::size_t https_pos = text.find("https://", position);
    const std::size_t start =
        http_pos == std::string::npos ? https_pos
                                      : (https_pos == std::string::npos ? http_pos
                                                                        : std::min(http_pos, https_pos));
    if (start == std::string::npos) {
      break;
    }
    std::size_t end = start;
    while (end < text.size() && !std::isspace(static_cast<unsigned char>(text[end]))) {
      ++end;
    }
    urls.push_back(text.substr(start, end - start));
    position = end;
  }
  return urls;
}

std::string TrimAscii(const std::string& value) {
  std::size_t start = 0;
  while (start < value.size() && std::isspace(static_cast<unsigned char>(value[start]))) {
    ++start;
  }
  std::size_t end = value.size();
  while (end > start && std::isspace(static_cast<unsigned char>(value[end - 1]))) {
    --end;
  }
  return value.substr(start, end - start);
}

[[maybe_unused]] std::string SanitizeSearchQuery(const std::string& raw) {
  std::string query = raw;
  const auto lower = LowercaseAscii(query);
  const auto additional_details = lower.find("additional details:");
  if (additional_details != std::string::npos) {
    query = query.substr(0, additional_details);
  }
  const auto user_message = lower.find("user message:");
  if (user_message != std::string::npos) {
    query = query.substr(user_message + std::string("user message:").size());
  }
  const std::vector<std::string> removals = {
      "reply to the user in chat mode.",
      "reply to the user in chat mode",
      "search the web for",
      "search online for",
      "use the web and check",
      "use the web",
      "enable web for this chat.",
      "enable web for this chat",
      "используй веб.",
      "используй веб",
  };
  for (const auto& removal : removals) {
    const auto lowered = LowercaseAscii(query);
    const auto found = lowered.find(LowercaseAscii(removal));
    if (found != std::string::npos) {
      query.erase(found, removal.size());
    }
  }
  query = TrimAscii(query);
  while (!query.empty() &&
         (query.front() == '.' || query.front() == ':' || query.front() == '-' ||
          std::isspace(static_cast<unsigned char>(query.front())))) {
    query.erase(query.begin());
  }
  return TrimAscii(query);
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
      } else if (request.rfind("GET /v1/webgateway/status ", 0) == 0) {
        WriteJsonResponse(
            client_fd,
            200,
            json{{"status", "ok"},
                 {"service", "comet-webgateway"},
                 {"ready", true},
                 {"active_session_count", 0}});
      } else if (request.rfind("POST /v1/webgateway/search ", 0) == 0) {
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

json DefaultInteractionBrowsingStatusPayload() {
  return json{{"status", "ok"},
              {"service", "comet-webgateway"},
              {"ready", true},
              {"search_enabled", true},
              {"fetch_enabled", true},
              {"session_backend", "cef"},
              {"rendered_browser_enabled", true},
              {"rendered_browser_ready", true},
              {"rendered_fetch_enabled", true},
              {"login_enabled", false}};
}

json DefaultInteractionBrowsingSearchResults() {
  return json::array({json{{"url", "https://example.com/article"},
                           {"domain", "example.com"},
                           {"title", "Example Article"},
                           {"snippet", "Example snippet"},
                           {"score", 0.9}}});
}

json DefaultInteractionBrowsingFetchPayload(const std::string& requested_url) {
  return json{{"url", requested_url},
              {"final_url", requested_url},
              {"backend", "browser_render"},
              {"rendered", true},
              {"content_type", "text/html"},
              {"title", "Example Article"},
              {"visible_text",
               "Example visible text about the requested topic from a safe test page."},
              {"citations", json::array({requested_url})},
              {"injection_flags", json::array()}};
}

struct InteractionBrowsingRuntimeServerConfig {
  int status_code = 200;
  json status_payload = DefaultInteractionBrowsingStatusPayload();
  int search_status_code = 200;
  json search_results = DefaultInteractionBrowsingSearchResults();
  json search_response_payload = json{};
  std::string search_backend = "broker_search";
  std::set<std::string> fetch_fail_urls;
  std::map<std::string, int> fetch_status_overrides;
  std::map<std::string, json> fetch_payload_overrides;
};

class InteractionBrowsingRuntimeTestServer {
 public:
  explicit InteractionBrowsingRuntimeTestServer(
      InteractionBrowsingRuntimeServerConfig config)
      : config_(std::move(config)) {
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

  explicit InteractionBrowsingRuntimeTestServer(
      json search_results = json::array({json{{"url", "https://example.com/article"},
                                              {"domain", "example.com"},
                                              {"title", "Example Article"},
                                              {"snippet", "Example snippet"},
                                              {"score", 0.9}}}),
      std::set<std::string> fetch_fail_urls = {})
      : InteractionBrowsingRuntimeTestServer([&]() {
          InteractionBrowsingRuntimeServerConfig config;
          config.search_results = std::move(search_results);
          config.fetch_fail_urls = std::move(fetch_fail_urls);
          return config;
        }()) {}

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
  std::vector<std::string> search_queries() const {
    std::lock_guard<std::mutex> lock(records_mutex_);
    return search_queries_;
  }
  std::vector<std::string> fetched_urls() const {
    std::lock_guard<std::mutex> lock(records_mutex_);
    return fetched_urls_;
  }

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
      } else if (request.rfind("GET /v1/webgateway/status ", 0) == 0) {
        WriteJsonResponse(client_fd, config_.status_code, config_.status_payload);
      } else if (request.rfind("POST /v1/webgateway/search ", 0) == 0) {
        ++search_count_;
        const json search_payload =
            body.empty() ? json::object() : json::parse(body, nullptr, false);
        {
          std::lock_guard<std::mutex> lock(records_mutex_);
          search_queries_.push_back(search_payload.value("query", std::string{}));
        }
        if (config_.search_status_code != 200) {
          const json payload =
              config_.search_response_payload.is_object() &&
                      !config_.search_response_payload.empty()
                  ? config_.search_response_payload
                  : json{{"status", "error"},
                         {"error",
                          {{"code", "search_failed"},
                           {"message", "simulated search failure"}}}};
          WriteJsonResponse(client_fd, config_.search_status_code, payload);
          comet::platform::CloseSocket(client_fd);
          continue;
        }
        if (config_.search_response_payload.is_object() &&
            !config_.search_response_payload.empty()) {
          WriteJsonResponse(client_fd, 200, config_.search_response_payload);
          comet::platform::CloseSocket(client_fd);
          continue;
        }
        WriteJsonResponse(
            client_fd,
            200,
            json{{"query", search_payload.value("query", std::string{})},
                 {"backend", config_.search_backend},
                 {"results", config_.search_results}});
      } else if (request.rfind("POST /v1/webgateway/fetch ", 0) == 0) {
        ++fetch_count_;
        const json fetch_payload =
            body.empty() ? json::object() : json::parse(body, nullptr, false);
        const std::string requested_url = fetch_payload.value("url", std::string{});
        {
          std::lock_guard<std::mutex> lock(records_mutex_);
          fetched_urls_.push_back(requested_url);
        }
        const auto status_override = config_.fetch_status_overrides.find(requested_url);
        if (status_override != config_.fetch_status_overrides.end() &&
            status_override->second != 200) {
          const auto payload_override = config_.fetch_payload_overrides.find(requested_url);
          const json payload =
              payload_override != config_.fetch_payload_overrides.end()
                  ? payload_override->second
                  : json{{"status", "error"},
                         {"error",
                          {{"code", "fetch_failed"},
                           {"message", "simulated fetch failure"},
                           {"url", requested_url}}}};
          WriteJsonResponse(client_fd, status_override->second, payload);
          comet::platform::CloseSocket(client_fd);
          continue;
        }
        if (fetch_fail_urls_.count(requested_url) > 0) {
          WriteJsonResponse(
              client_fd,
              502,
              json{{"status", "error"},
                   {"error",
                    {{"code", "fetch_failed"},
                     {"message", "simulated fetch failure"},
                     {"url", requested_url}}}});
          comet::platform::CloseSocket(client_fd);
          continue;
        }
        json response_payload = DefaultInteractionBrowsingFetchPayload(requested_url);
        if (const auto override = config_.fetch_payload_overrides.find(requested_url);
            override != config_.fetch_payload_overrides.end() &&
            override->second.is_object()) {
          for (const auto& [key, value] : override->second.items()) {
            response_payload[key] = value;
          }
        }
        WriteJsonResponse(client_fd, 200, response_payload);
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
  InteractionBrowsingRuntimeServerConfig config_;
  std::set<std::string> fetch_fail_urls_ = config_.fetch_fail_urls;
  mutable std::mutex records_mutex_;
  std::vector<std::string> search_queries_;
  std::vector<std::string> fetched_urls_;
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
  policy.rendered_browser_enabled = true;
  policy.login_enabled = false;
  settings.policy = policy;
  desired_state.browsing = settings;

  comet::InstanceSpec browsing;
  browsing.name = "webgateway-maglev";
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

comet::controller::InteractionRequestContext BuildBrowsingRequestContext(
    const std::vector<std::string>& user_messages) {
  comet::controller::InteractionRequestContext request_context;
  json messages = json::array();
  for (const auto& message : user_messages) {
    messages.push_back(json{{"role", "user"}, {"content", message}});
  }
  request_context.payload = json{{"messages", std::move(messages)}};
  return request_context;
}

comet::controller::PlaneInteractionResolution BuildBrowsingResolution(int port) {
  comet::controller::PlaneInteractionResolution resolution;
  resolution.desired_state = BuildDesiredStateWithBrowsingPort("127.0.0.1", port);
  return resolution;
}

const json& BrowsingSummary(
    const comet::controller::InteractionRequestContext& request_context) {
  return request_context.payload.at(
      comet::controller::InteractionBrowsingService::kSummaryPayloadKey);
}

std::string MakeTempDbPath() {
  const auto root =
      fs::temp_directory_path() / "comet-plane-skills-service-tests";
  fs::create_directories(root);
  return (root / "controller.sqlite").string();
}

std::string MakeInvalidUtf8Text() {
  return std::string("broken-\xC3\x28-value", 15);
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
      Expect(payload.value("webgateway_enabled", false),
             "webgateway status should mark webgateway as enabled");
      Expect(payload.value("webgateway_ready", false),
             "webgateway status should probe runtime readiness");
      Expect(payload.value("service", std::string{}) == "comet-webgateway",
             "webgateway status should merge runtime payload");
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
          summary.at("lookup_state").get<std::string>() == "enabled_toggle_only",
          "toggle-only requests should expose a dedicated enabled-toggle-only state");
      Expect(
          summary.at("indicator").at("compact").get<std::string>() == "web:on",
          "toggle-only requests should expose a compact web:on indicator");
      Expect(
          summary.at("trace").is_array() &&
              summary.at("trace").at(0).at("compact").get<std::string>() == "web:on",
          "toggle-only requests should expose a compact trace");
      Expect(
          request_context.payload.at(
              comet::controller::InteractionBrowsingService::kSystemInstructionPayloadKey)
                  .get<std::string>()
                  .find("Controller browsing state: enabled_toggle_only.") != std::string::npos,
          "toggle-only requests should inject an enable acknowledgement instruction");
      std::cout << "ok: interaction-browsing-toggle-only-enable" << '\n';
    }

    {
      comet::controller::InteractionBrowsingService browsing_service;
      comet::controller::InteractionRequestContext request_context;
      request_context.payload = json{
          {"messages",
           json::array(
               {json{{"role", "user"}, {"content", "Enable web for this chat."}},
                json{{"role", "user"}, {"content", "Explain TCP handshakes."}}})},
      };
      comet::controller::PlaneInteractionResolution resolution;
      resolution.desired_state = BuildDesiredStateWithBrowsingPort("127.0.0.1", 18130);
      const auto error =
          browsing_service.ResolveInteractionBrowsing(resolution, &request_context);
      Expect(!error.has_value(), "enabled web mode should allow no-lookup offline answers");
      const auto summary =
          request_context.payload.at(
              comet::controller::InteractionBrowsingService::kSummaryPayloadKey);
      Expect(
          summary.at("decision").get<std::string>() == "not_needed",
          "offline prompts should still avoid lookup when web context is unnecessary");
      Expect(
          summary.at("lookup_state").get<std::string>() == "enabled_not_needed",
          "enabled offline prompts should expose enabled_not_needed state");
      Expect(
          summary.at("indicator").at("compact").get<std::string>() == "web:on idle",
          "enabled offline prompts should expose a compact idle indicator");
      Expect(
          !summary.at("lookup_attempted").get<bool>(),
          "enabled offline prompts should not mark lookup_attempted");
      Expect(
          request_context.payload.at(
              comet::controller::InteractionBrowsingService::kSystemInstructionPayloadKey)
                  .get<std::string>()
                  .find("web access may remain available, but no web lookup was needed") !=
              std::string::npos,
          "enabled offline prompts should tell the model that web stayed available but unused");
      std::cout << "ok: interaction-browsing-enabled-not-needed" << '\n';
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
      Expect(
          summary.at("lookup_state").get<std::string>() == "evidence_attached",
          "successful web enrichment should expose evidence_attached state");
      Expect(
          summary.at("indicator").at("compact").get<std::string>() == "web:search ok",
          "successful search enrichment should expose a compact search-ok indicator");
      Expect(summary.at("lookup_attempted").get<bool>(),
             "successful web enrichment should mark lookup_attempted");
      Expect(summary.at("evidence_attached").get<bool>(),
             "successful web enrichment should mark evidence_attached");
      Expect(summary.at("session_backend").get<std::string>() == "cef",
             "successful web enrichment should expose rendered session backend");
      Expect(summary.at("rendered_browser_ready").get<bool>(),
             "successful web enrichment should expose rendered browser readiness");
      Expect(runtime.search_count() == 1, "search-and-fetch should call search once");
      Expect(runtime.fetch_count() >= 1, "search-and-fetch should fetch at least one result");
      Expect(
          summary.at("sources").is_array() && !summary.at("sources").empty(),
          "search-and-fetch should expose fetched sources");
      Expect(
          summary.at("sources").at(0).at("backend").get<std::string>() == "browser_render",
          "search-and-fetch should preserve fetch backend provenance");
      Expect(
          summary.at("trace").is_array() &&
              std::any_of(
                  summary.at("trace").begin(),
                  summary.at("trace").end(),
                  [](const json& item) {
                    return item.value("stage", std::string{}) == "browser_render";
                  }),
          "search-and-fetch should expose browser render trace stage");
      std::cout << "ok: interaction-browsing-search-and-fetch" << '\n';
    }

    {
      InteractionBrowsingRuntimeTestServer runtime(
          json::array({json{{"url", "https://example.com/fail"},
                            {"domain", "example.com"},
                            {"title", "Unreachable Result"},
                            {"snippet", "This page will fail to fetch."},
                            {"score", 0.95}},
                       json{{"url", "https://example.com/article"},
                            {"domain", "example.com"},
                            {"title", "Reachable Result"},
                            {"snippet", "This page is reachable."},
                            {"score", 0.85}}}),
          {"https://example.com/fail"});
      comet::controller::InteractionBrowsingService browsing_service;
      comet::controller::InteractionRequestContext request_context;
      request_context.payload = json{
          {"messages",
           json::array(
               {json{{"role", "user"}, {"content", "Enable web for this chat."}},
                json{{"role", "user"},
                     {"content", "Find the latest safe example result online."}}})},
      };
      comet::controller::PlaneInteractionResolution resolution;
      resolution.desired_state =
          BuildDesiredStateWithBrowsingPort("127.0.0.1", runtime.port());
      const auto error =
          browsing_service.ResolveInteractionBrowsing(resolution, &request_context);
      Expect(!error.has_value(), "search fallback after failed fetch should succeed");
      const auto summary =
          request_context.payload.at(
              comet::controller::InteractionBrowsingService::kSummaryPayloadKey);
      Expect(runtime.search_count() == 1, "search fallback should still search once");
      Expect(runtime.fetch_count() >= 2,
             "search fallback should continue to later results after a failed fetch");
      Expect(
          summary.at("sources").is_array() && !summary.at("sources").empty(),
          "search fallback should expose a later fetched source");
      Expect(
          summary.at("sources").at(0).at("url").get<std::string>() ==
              "https://example.com/article",
          "search fallback should preserve the first successfully fetched source");
      std::cout << "ok: interaction-browsing-search-fetch-fallback" << '\n';
    }

    {
      InteractionBrowsingRuntimeTestServer runtime(
          json::array({json{{"url", "https://example.com/fail-1"},
                            {"domain", "example.com"},
                            {"title", "Snippet Result One"},
                            {"snippet", "Summary from the first safe result."},
                            {"score", 0.95}},
                       json{{"url", "https://example.com/fail-2"},
                            {"domain", "example.com"},
                            {"title", "Snippet Result Two"},
                            {"snippet", "Summary from the second safe result."},
                            {"score", 0.90}}}),
          {"https://example.com/fail-1", "https://example.com/fail-2"});
      comet::controller::InteractionBrowsingService browsing_service;
      comet::controller::InteractionRequestContext request_context;
      request_context.payload = json{
          {"messages",
           json::array(
               {json{{"role", "user"}, {"content", "Enable web for this chat."}},
                json{{"role", "user"},
                     {"content", "Search online for a safe example summary."}}})},
      };
      comet::controller::PlaneInteractionResolution resolution;
      resolution.desired_state =
          BuildDesiredStateWithBrowsingPort("127.0.0.1", runtime.port());
      const auto error =
          browsing_service.ResolveInteractionBrowsing(resolution, &request_context);
      Expect(!error.has_value(), "snippet fallback should not fail the interaction");
      const auto summary =
          request_context.payload.at(
              comet::controller::InteractionBrowsingService::kSummaryPayloadKey);
      Expect(runtime.search_count() == 1, "snippet fallback should search once");
      Expect(runtime.fetch_count() >= 2, "snippet fallback should try the fetches before degrading");
      Expect(
          summary.at("decision").get<std::string>() == "search_and_fetch",
          "snippet fallback should keep the request in search-and-fetch mode");
      Expect(
          summary.at("sources").is_array() && !summary.at("sources").empty(),
          "snippet fallback should still provide controller browsing evidence");
      Expect(
          summary.at("sources").at(0).at("snippet_only").get<bool>(),
          "snippet fallback should mark evidence that came only from search results");
      Expect(
          summary.at("lookup_state").get<std::string>() == "evidence_attached",
          "snippet fallback should still count as evidence_attached");
      Expect(
          summary.at("trace").is_array() &&
              summary.at("trace").back().at("compact").get<std::string>() == "evidence:yes",
          "snippet fallback should end with an evidence:yes trace step");
      std::cout << "ok: interaction-browsing-search-snippet-fallback" << '\n';
    }

    {
      InteractionBrowsingRuntimeTestServer runtime(json::array());
      comet::controller::InteractionBrowsingService browsing_service;
      comet::controller::InteractionRequestContext request_context;
      request_context.payload = json{
          {"messages",
           json::array(
               {json{{"role", "user"}, {"content", "Enable web for this chat."}},
                json{{"role", "user"},
                     {"content", "What is the latest safe example update online?"}}})},
      };
      comet::controller::PlaneInteractionResolution resolution;
      resolution.desired_state =
          BuildDesiredStateWithBrowsingPort("127.0.0.1", runtime.port());
      const auto error =
          browsing_service.ResolveInteractionBrowsing(resolution, &request_context);
      Expect(!error.has_value(), "empty search results should not fail the interaction");
      const auto summary =
          request_context.payload.at(
              comet::controller::InteractionBrowsingService::kSummaryPayloadKey);
      Expect(
          summary.at("decision").get<std::string>() == "search_and_fetch",
          "empty search results should preserve the fact that a web lookup was attempted");
      Expect(
          summary.at("reason").get<std::string>() == "search_returned_no_sources",
          "empty search results should explain why no evidence was attached");
      Expect(
          summary.at("lookup_state").get<std::string>() == "attempted_no_evidence",
          "empty search results should expose attempted_no_evidence state");
      Expect(
          summary.at("indicator").at("compact").get<std::string>() == "web:search none",
          "empty search results should expose a compact no-evidence indicator");
      Expect(summary.at("lookup_attempted").get<bool>(),
             "empty search results should still mark lookup_attempted");
      Expect(!summary.at("evidence_attached").get<bool>(),
             "empty search results should not mark evidence_attached");
      Expect(
          summary.at("searches").is_array() && !summary.at("searches").empty() &&
              summary.at("searches").at(0).at("result_count").get<int>() == 0,
          "empty search results should still expose the attempted search summary");
      std::cout << "ok: interaction-browsing-search-no-results" << '\n';
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
      Expect(
          summary.at("lookup_state").get<std::string>() == "evidence_attached",
          "direct fetch with a usable page should expose evidence_attached state");
      Expect(
          summary.at("indicator").at("compact").get<std::string>() == "web:fetch ok",
          "direct fetch should expose a compact fetch-ok indicator");
      Expect(summary.at("sources").at(0).at("backend").get<std::string>() == "browser_render",
             "direct fetch should preserve rendered backend provenance");
      Expect(
          summary.at("trace").is_array() &&
              summary.at("trace").at(2).at("stage").get<std::string>() == "browsing_status",
          "direct fetch trace should retain browsing status stage");
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
      Expect(
          summary.at("lookup_state").get<std::string>() == "disabled_by_user",
          "later disable directive should expose disabled_by_user state");
      Expect(
          summary.at("indicator").at("compact").get<std::string>() == "web:off user",
          "later disable directive should expose a compact user-disabled indicator");
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
          response.payload.at("webgateway").at("mode").get<std::string>() == "disabled",
          "interaction response should expose webgateway summary at top level");
      Expect(
          response.payload.at("session").at("webgateway").at("decision").get<std::string>() ==
              "disabled",
          "interaction session payload should expose webgateway summary");
      std::cout << "ok: interaction-response-includes-webgateway-fields" << '\n';
    }

    {
      InteractionBrowsingRuntimeServerConfig config;
      config.status_payload["ready"] = false;
      config.status_payload["rendered_browser_ready"] = false;
      InteractionBrowsingRuntimeTestServer runtime(std::move(config));
      comet::controller::InteractionBrowsingService browsing_service;
      auto request_context = BuildBrowsingRequestContext(
          {"Enable web for this chat.", "What is the latest OpenAI update online?"});
      auto resolution = BuildBrowsingResolution(runtime.port());
      const auto error =
          browsing_service.ResolveInteractionBrowsing(resolution, &request_context);
      Expect(!error.has_value(), "not-ready browsing runtime should not fail the interaction");
      const auto& summary = BrowsingSummary(request_context);
      Expect(summary.at("decision").get<std::string>() == "unavailable",
             "not-ready browsing runtime should surface unavailable decision");
      Expect(summary.at("lookup_state").get<std::string>() == "required_but_unavailable",
             "not-ready browsing runtime should expose required_but_unavailable state");
      Expect(!summary.at("lookup_attempted").get<bool>(),
             "not-ready browsing runtime should not mark lookup_attempted");
      Expect(runtime.search_count() == 0, "not-ready browsing runtime should prevent search");
      Expect(runtime.fetch_count() == 0, "not-ready browsing runtime should prevent fetch");
      std::cout << "ok: interaction-browsing-status-not-ready" << '\n';
    }

    {
      InteractionBrowsingRuntimeServerConfig config;
      config.status_code = 503;
      config.status_payload = json{{"status", "error"},
                                   {"error",
                                    {{"code", "status_unavailable"},
                                     {"message", "status unavailable"}}}};
      InteractionBrowsingRuntimeTestServer runtime(std::move(config));
      comet::controller::InteractionBrowsingService browsing_service;
      auto request_context = BuildBrowsingRequestContext(
          {"Enable web for this chat.", "What is the latest OpenAI update online?"});
      auto resolution = BuildBrowsingResolution(runtime.port());
      const auto error =
          browsing_service.ResolveInteractionBrowsing(resolution, &request_context);
      Expect(!error.has_value(), "status upstream errors should not fail the interaction");
      const auto& summary = BrowsingSummary(request_context);
      Expect(summary.at("decision").get<std::string>() == "unavailable",
             "status upstream errors should surface unavailable decision");
      Expect(summary.at("lookup_state").get<std::string>() == "required_but_unavailable",
             "status upstream errors should expose required_but_unavailable state");
      Expect(summary.at("errors").is_array() && !summary.at("errors").empty(),
             "status upstream errors should expose an error entry");
      Expect(summary.at("errors").at(0).at("code").get<std::string>() == "browsing_not_ready",
             "status upstream errors should normalize to browsing_not_ready");
      std::cout << "ok: interaction-browsing-status-error" << '\n';
    }

    {
      InteractionBrowsingRuntimeServerConfig config;
      config.search_status_code = 502;
      config.search_response_payload = json{{"status", "error"},
                                            {"error",
                                             {{"code", "search_failed"},
                                              {"message", "simulated search failure"}}}};
      InteractionBrowsingRuntimeTestServer runtime(std::move(config));
      comet::controller::InteractionBrowsingService browsing_service;
      auto request_context = BuildBrowsingRequestContext(
          {"Enable web for this chat.", "Search online for the latest OpenAI update."});
      auto resolution = BuildBrowsingResolution(runtime.port());
      const auto error =
          browsing_service.ResolveInteractionBrowsing(resolution, &request_context);
      Expect(!error.has_value(), "search failures should not fail the interaction outright");
      const auto& summary = BrowsingSummary(request_context);
      Expect(summary.at("decision").get<std::string>() == "error",
             "search failures should surface error decision");
      Expect(summary.at("reason").get<std::string>() == "browsing_lookup_failed",
             "search failures should explain browsing_lookup_failed");
      Expect(summary.at("lookup_state").get<std::string>() == "required_but_unavailable",
             "search failures should expose required_but_unavailable state");
      Expect(summary.at("errors").at(0).at("code").get<std::string>() == "search_failed",
             "search failures should preserve normalized search_failed code");
      Expect(runtime.search_count() == 1, "search failures should still attempt one search");
      Expect(runtime.fetch_count() == 0, "search failures should not attempt fetch");
      std::cout << "ok: interaction-browsing-search-error" << '\n';
    }

    {
      InteractionBrowsingRuntimeServerConfig config;
      config.fetch_fail_urls.insert("https://example.com/bad");
      InteractionBrowsingRuntimeTestServer runtime(std::move(config));
      comet::controller::InteractionBrowsingService browsing_service;
      auto request_context = BuildBrowsingRequestContext(
          {"Use the web and check https://example.com/bad"});
      auto resolution = BuildBrowsingResolution(runtime.port());
      const auto error =
          browsing_service.ResolveInteractionBrowsing(resolution, &request_context);
      Expect(!error.has_value(), "direct fetch failures should not fail the interaction outright");
      const auto& summary = BrowsingSummary(request_context);
      Expect(summary.at("decision").get<std::string>() == "error",
             "direct fetch failures should surface error decision");
      Expect(summary.at("reason").get<std::string>() == "browsing_lookup_failed",
             "direct fetch failures should explain browsing_lookup_failed");
      Expect(summary.at("lookup_state").get<std::string>() == "required_but_unavailable",
             "direct fetch failures should expose required_but_unavailable state");
      Expect(summary.at("errors").at(0).at("code").get<std::string>() == "fetch_failed",
             "direct fetch failures should preserve normalized fetch_failed code");
      Expect(runtime.search_count() == 0, "direct fetch failures should not use search");
      Expect(runtime.fetch_count() == 1, "direct fetch failures should still attempt one fetch");
      std::cout << "ok: interaction-browsing-direct-fetch-error" << '\n';
    }

    {
      InteractionBrowsingRuntimeTestServer runtime;
      comet::controller::InteractionBrowsingService browsing_service;
      auto request_context = BuildBrowsingRequestContext(
          {"Enable web for this chat.", "Search the web for OpenAI API pricing today."});
      auto resolution = BuildBrowsingResolution(runtime.port());
      const auto error =
          browsing_service.ResolveInteractionBrowsing(resolution, &request_context);
      Expect(!error.has_value(), "search query sanitization should not fail");
      const auto queries = runtime.search_queries();
      Expect(queries.size() == 1, "sanitization test should issue exactly one search query");
      Expect(queries.front().find("search the web") == std::string::npos,
             "sanitized query should remove explicit search-the-web marker");
      Expect(queries.front().find("openai api pricing") != std::string::npos,
             "sanitized query should retain the substantive topic");
      std::cout << "ok: interaction-browsing-search-query-sanitization" << '\n';
    }

    {
      InteractionBrowsingRuntimeTestServer runtime;
      comet::controller::InteractionBrowsingService browsing_service;
      auto request_context = BuildBrowsingRequestContext(
          {"Enable web for this chat.",
           "Reply to the user in chat mode.\nUser message: Используй веб. Посмотри поведение Bitcoin за последние 7 дней и дай короткий вывод по импульсу рынка."});
      auto resolution = BuildBrowsingResolution(runtime.port());
      const auto error =
          browsing_service.ResolveInteractionBrowsing(resolution, &request_context);
      Expect(!error.has_value(), "maglev wrapper query sanitization should not fail");
      const auto queries = runtime.search_queries();
      Expect(queries.size() == 1,
             "maglev wrapper sanitization test should issue exactly one search query");
      Expect(queries.front().find("reply to the user in chat mode") == std::string::npos,
             "sanitized query should strip maglev wrapper prefix");
      Expect(queries.front().find("user message") == std::string::npos,
             "sanitized query should strip user message label");
      Expect(queries.front().find("используй веб") == std::string::npos,
             "sanitized query should remove explicit web toggle marker");
      Expect(queries.front().find("bitcoin за последние 7 дней") != std::string::npos,
             "sanitized query should preserve the substantive market prompt");
      Expect(!queries.front().empty() && queries.front().front() != '.',
             "sanitized query should not keep leading punctuation");
      std::cout << "ok: interaction-browsing-maglev-wrapper-query-sanitization" << '\n';
    }

    {
      InteractionBrowsingRuntimeTestServer runtime;
      comet::controller::InteractionBrowsingService browsing_service;
      auto request_context = BuildBrowsingRequestContext(
          {"Enable web for this chat.",
           "Используй веб. Проверь текущую цену ETH и коротко сравни изменение ETH и BTC за последние 24 часа.\n"
           "Additional details:\n"
           "workspaceRoot: /Users/vladislavhalasiuk/Projects/Repos/maglev\n"
           "platform: linux\n"
           "compiler: gcc-13"});
      auto resolution = BuildBrowsingResolution(runtime.port());
      const auto error =
          browsing_service.ResolveInteractionBrowsing(resolution, &request_context);
      Expect(!error.has_value(), "metadata suffix sanitization should not fail");
      const auto queries = runtime.search_queries();
      Expect(queries.size() == 1,
             "metadata suffix sanitization test should issue exactly one search query");
      Expect(queries.front().find("additional details") == std::string::npos,
             "sanitized query should strip the additional details header");
      Expect(queries.front().find("workspaceroot") == std::string::npos,
             "sanitized query should strip workspaceRoot metadata");
      Expect(queries.front().find("platform:") == std::string::npos,
             "sanitized query should strip platform metadata");
      Expect(queries.front().find("compiler:") == std::string::npos,
             "sanitized query should strip compiler metadata");
      Expect(queries.front().find("цену eth") != std::string::npos,
             "sanitized query should preserve the substantive ETH request");
      std::cout << "ok: interaction-browsing-metadata-suffix-query-sanitization" << '\n';
    }

    {
      InteractionBrowsingRuntimeServerConfig config;
      config.search_results = json::array(
          {json{{"url", "https://example.com/shared"},
                {"domain", "example.com"},
                {"title", "Shared Result"},
                {"snippet", "Duplicate one."},
                {"score", 0.95}},
           json{{"url", "https://example.com/shared"},
                {"domain", "example.com"},
                {"title", "Shared Result Again"},
                {"snippet", "Duplicate two."},
                {"score", 0.90}},
           json{{"url", "https://example.com/second"},
                {"domain", "example.com"},
                {"title", "Second Result"},
                {"snippet", "Second unique result."},
                {"score", 0.85}}});
      InteractionBrowsingRuntimeTestServer runtime(std::move(config));
      comet::controller::InteractionBrowsingService browsing_service;
      auto request_context = BuildBrowsingRequestContext(
          {"Enable web for this chat.", "Search online for the latest example update."});
      auto resolution = BuildBrowsingResolution(runtime.port());
      const auto error =
          browsing_service.ResolveInteractionBrowsing(resolution, &request_context);
      Expect(!error.has_value(), "duplicate search results should not fail");
      const auto fetched_urls = runtime.fetched_urls();
      Expect(runtime.fetch_count() == 2, "duplicate search results should dedupe fetches");
      Expect(
          static_cast<int>(std::count(
              fetched_urls.begin(),
              fetched_urls.end(),
              "https://example.com/shared")) == 1,
          "duplicate search results should fetch a shared URL only once");
      std::cout << "ok: interaction-browsing-dedupes-search-results" << '\n';
    }

    {
      InteractionBrowsingRuntimeTestServer runtime;
      comet::controller::InteractionBrowsingService browsing_service;
      auto request_context = BuildBrowsingRequestContext(
          {"Use the web and check https://example.com/one https://example.com/two "
           "https://example.com/three"});
      auto resolution = BuildBrowsingResolution(runtime.port());
      const auto error =
          browsing_service.ResolveInteractionBrowsing(resolution, &request_context);
      Expect(!error.has_value(), "multi-url direct fetch should not fail");
      const auto& summary = BrowsingSummary(request_context);
      Expect(summary.at("decision").get<std::string>() == "direct_fetch",
             "multi-url request should stay in direct_fetch mode");
      Expect(runtime.fetch_count() == 2, "multi-url direct fetch should stop at two fetches");
      Expect(summary.at("sources").size() == 2,
             "multi-url direct fetch should expose only the first two fetched sources");
      std::cout << "ok: interaction-browsing-direct-fetch-limits-to-two-urls" << '\n';
    }

    {
      InteractionBrowsingRuntimeServerConfig config;
      config.fetch_payload_overrides["https://example.com/nulls"] =
          json{{"title", nullptr},
               {"content_type", nullptr},
               {"visible_text", nullptr},
               {"citations", nullptr},
               {"injection_flags", nullptr}};
      InteractionBrowsingRuntimeTestServer runtime(std::move(config));
      comet::controller::InteractionBrowsingService browsing_service;
      auto request_context =
          BuildBrowsingRequestContext({"Use the web and check https://example.com/nulls"});
      auto resolution = BuildBrowsingResolution(runtime.port());
      const auto error =
          browsing_service.ResolveInteractionBrowsing(resolution, &request_context);
      Expect(!error.has_value(), "nullable fetch fields should not fail");
      const auto& source = BrowsingSummary(request_context).at("sources").at(0);
      Expect(source.at("title").get<std::string>().empty(),
             "nullable fetch title should normalize to empty string");
      Expect(source.at("content_type").get<std::string>().empty(),
             "nullable fetch content_type should normalize to empty string");
      Expect(source.at("excerpt").get<std::string>().empty(),
             "nullable fetch visible_text should normalize to empty excerpt");
      Expect(source.at("citations").is_array() && source.at("citations").empty(),
             "nullable fetch citations should normalize to empty array");
      Expect(source.at("injection_flags").is_array() && source.at("injection_flags").empty(),
             "nullable fetch injection_flags should normalize to empty array");
      std::cout << "ok: interaction-browsing-null-fetch-fields-safe" << '\n';
    }

    {
      InteractionBrowsingRuntimeServerConfig config;
      config.search_response_payload =
          json{{"query", nullptr},
               {"backend", nullptr},
               {"results",
                json::array({json{{"url", "https://example.com/article"},
                                  {"title", nullptr},
                                  {"snippet", "Snippet from nullable result."}}})}};
      InteractionBrowsingRuntimeTestServer runtime(std::move(config));
      comet::controller::InteractionBrowsingService browsing_service;
      auto request_context = BuildBrowsingRequestContext(
          {"Enable web for this chat.", "Search online for the latest OpenAI API update."});
      auto resolution = BuildBrowsingResolution(runtime.port());
      const auto error =
          browsing_service.ResolveInteractionBrowsing(resolution, &request_context);
      Expect(!error.has_value(), "nullable search fields should not fail");
      const auto& summary = BrowsingSummary(request_context);
      Expect(summary.at("searches").at(0).at("backend").get<std::string>() == "broker_search",
             "nullable search backend should fall back to broker_search");
      Expect(!summary.at("searches").at(0).at("query").get<std::string>().empty(),
             "nullable search query should fall back to requested query");
      Expect(summary.at("sources").at(0).at("url").get<std::string>() == "https://example.com/article",
             "nullable search fields should still lead to a usable fetched source");
      std::cout << "ok: interaction-browsing-null-search-fields-safe" << '\n';
    }

    {
      InteractionBrowsingRuntimeServerConfig config;
      config.fetch_payload_overrides["https://example.com/injected"] =
          json{{"injection_flags", json::array({"prompt_injection_detected"})}};
      InteractionBrowsingRuntimeTestServer runtime(std::move(config));
      comet::controller::InteractionBrowsingService browsing_service;
      auto request_context =
          BuildBrowsingRequestContext({"Use the web and check https://example.com/injected"});
      auto resolution = BuildBrowsingResolution(runtime.port());
      const auto error =
          browsing_service.ResolveInteractionBrowsing(resolution, &request_context);
      Expect(!error.has_value(), "injection flag propagation should not fail");
      const auto& summary = BrowsingSummary(request_context);
      Expect(summary.at("sources").at(0).at("injection_flags").size() == 1,
             "injection flag propagation should preserve source markers");
      Expect(
          request_context.payload
                  .at(comet::controller::InteractionBrowsingService::kSystemInstructionPayloadKey)
                  .get<std::string>()
                  .find("prompt-injection markers were detected") != std::string::npos,
          "injection flag propagation should warn the model in the system instruction");
      std::cout << "ok: interaction-browsing-injection-warning" << '\n';
    }

    {
      InteractionBrowsingRuntimeServerConfig config;
      config.status_payload["session_backend"] = "broker_fallback";
      config.status_payload["rendered_browser_enabled"] = false;
      config.status_payload["rendered_browser_ready"] = false;
      config.status_payload["rendered_fetch_enabled"] = false;
      config.fetch_payload_overrides["https://example.com/brokered"] =
          json{{"backend", "broker_fetch"}, {"rendered", false}};
      InteractionBrowsingRuntimeTestServer runtime(std::move(config));
      comet::controller::InteractionBrowsingService browsing_service;
      auto request_context =
          BuildBrowsingRequestContext({"Use the web and check https://example.com/brokered"});
      auto resolution = BuildBrowsingResolution(runtime.port());
      const auto error =
          browsing_service.ResolveInteractionBrowsing(resolution, &request_context);
      Expect(!error.has_value(), "broker fallback provenance should not fail");
      const auto& summary = BrowsingSummary(request_context);
      Expect(summary.at("session_backend").get<std::string>() == "broker_fallback",
             "broker fallback status should preserve session backend provenance");
      Expect(summary.at("sources").at(0).at("backend").get<std::string>() == "broker_fetch",
             "broker fallback status should preserve source backend provenance");
      Expect(
          std::none_of(
              summary.at("trace").begin(),
              summary.at("trace").end(),
              [](const json& item) {
                return item.value("stage", std::string{}) == "browser_render";
              }),
          "broker fallback responses should not inject rendered browser trace stages");
      std::cout << "ok: interaction-browsing-broker-provenance" << '\n';
    }

    {
      InteractionBrowsingRuntimeServerConfig config;
      config.search_results = json::array(
          {json{{"url", "https://example.com/1"},
                {"domain", "example.com"},
                {"title", "Result One"},
                {"snippet", "One"},
                {"score", 0.99}},
           json{{"url", "https://example.com/2"},
                {"domain", "example.com"},
                {"title", "Result Two"},
                {"snippet", "Two"},
                {"score", 0.95}},
           json{{"url", "https://example.com/3"},
                {"domain", "example.com"},
                {"title", "Result Three"},
                {"snippet", "Three"},
                {"score", 0.90}},
           json{{"url", "https://example.com/4"},
                {"domain", "example.com"},
                {"title", "Result Four"},
                {"snippet", "Four"},
                {"score", 0.85}}});
      InteractionBrowsingRuntimeTestServer runtime(std::move(config));
      comet::controller::InteractionBrowsingService browsing_service;
      auto request_context = BuildBrowsingRequestContext(
          {"Enable web for this chat.", "Search online for the latest example update."});
      auto resolution = BuildBrowsingResolution(runtime.port());
      const auto error =
          browsing_service.ResolveInteractionBrowsing(resolution, &request_context);
      Expect(!error.has_value(), "source cap test should not fail");
      const auto& summary = BrowsingSummary(request_context);
      Expect(runtime.fetch_count() == 2, "search-and-fetch should cap fetched sources at two");
      Expect(summary.at("sources").size() == 2,
             "search-and-fetch should expose at most two fetched sources");
      std::cout << "ok: interaction-browsing-source-cap" << '\n';
    }

    {
      InteractionBrowsingRuntimeTestServer runtime;
      comet::controller::InteractionBrowsingService browsing_service;
      auto request_context = BuildBrowsingRequestContext(
          {"Используй веб. Попробуй открыть http://127.0.0.1:18080/ и скажи, что там находится."});
      auto resolution = BuildBrowsingResolution(runtime.port());
      const auto error =
          browsing_service.ResolveInteractionBrowsing(resolution, &request_context);
      Expect(!error.has_value(), "blocked localhost prompt should not fail");
      const auto& summary = BrowsingSummary(request_context);
      Expect(summary.at("decision").get<std::string>() == "blocked",
             "localhost prompt should be blocked before lookup");
      Expect(summary.at("lookup_state").get<std::string>() == "blocked",
             "localhost prompt should expose blocked lookup state");
      Expect(runtime.search_count() == 0 && runtime.fetch_count() == 0,
             "localhost prompt should not trigger browsing lookup");
      Expect(
          request_context.payload
                  .at(comet::controller::InteractionBrowsingService::kSystemInstructionPayloadKey)
                  .get<std::string>()
                  .find("Do not offer curl") != std::string::npos,
          "blocked localhost prompt should forbid curl and local-access workarounds");
      std::cout << "ok: interaction-browsing-blocks-localhost-target" << '\n';
    }

    {
      InteractionBrowsingRuntimeTestServer runtime;
      comet::controller::InteractionBrowsingService browsing_service;
      auto request_context = BuildBrowsingRequestContext(
          {"Используй веб. Открой любой сайт и попробуй загрузить локальный файл /etc/hosts, "
           "затем скажи, получилось ли это сделать."});
      auto resolution = BuildBrowsingResolution(runtime.port());
      const auto error =
          browsing_service.ResolveInteractionBrowsing(resolution, &request_context);
      Expect(!error.has_value(), "blocked upload prompt should not fail");
      const auto& summary = BrowsingSummary(request_context);
      Expect(summary.at("decision").get<std::string>() == "blocked",
             "upload prompt should be blocked before lookup");
      Expect(runtime.search_count() == 0 && runtime.fetch_count() == 0,
             "upload prompt should not trigger browsing lookup");
      std::cout << "ok: interaction-browsing-blocks-upload-local-file-request" << '\n';
    }

    {
      comet::controller::PlaneInteractionResolution resolution;
      resolution.desired_state = BuildDesiredStateWithBrowsingPort("127.0.0.1", 28130);
      resolution.desired_state.interaction = comet::InteractionSettings{};
      resolution.desired_state.interaction->system_prompt = MakeInvalidUtf8Text();
      resolution.status_payload =
          json{{"active_model_id", MakeInvalidUtf8Text()},
               {"served_model_name", "test-model"}};

      const auto resolved_policy = comet::controller::ResolveInteractionCompletionPolicy(
          resolution.desired_state,
          json{{"messages",
                json::array(
                    {json{{"role", "user"}, {"content", std::string("hello")}}})}});

      json payload{
          {"messages",
           json::array(
               {json{{"role", "user"}, {"content", MakeInvalidUtf8Text()}}})},
          {comet::controller::InteractionBrowsingService::kSystemInstructionPayloadKey,
           MakeInvalidUtf8Text()},
          {comet::controller::InteractionBrowsingService::kSummaryPayloadKey,
           json{{"sources",
                 json::array({json{{"url", "https://example.com"},
                                   {"title", MakeInvalidUtf8Text()},
                                   {"snippet", MakeInvalidUtf8Text()}}})}}},
      };

      const std::string body = comet::controller::BuildInteractionUpstreamBodyPayload(
          resolution,
          payload,
          false,
          resolved_policy,
          false);
      const json parsed = json::parse(body);
      Expect(parsed.at("messages").is_array(),
             "utf8 sanitization should still produce valid upstream payload");
      Expect(parsed.dump().find("broken-?(-value") != std::string::npos,
             "utf8 sanitization should replace invalid bytes before dump");
      std::cout << "ok: interaction-payload-builder-sanitizes-invalid-utf8" << '\n';
    }

    return 0;
  } catch (const std::exception& error) {
    std::cerr << "plane_skills_service_tests failed: " << error.what() << '\n';
    return 1;
  }
}
