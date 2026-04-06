#pragma once

#include <atomic>
#include <filesystem>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <set>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include <nlohmann/json.hpp>

#include "comet/core/platform_compat.h"
#include "http/controller_http_types.h"
#include "http/controller_http_transport.h"

namespace comet::browsing {

class CefBrowserBackend;

struct BrowsingPolicy {
  bool browser_session_enabled = false;
  std::vector<std::string> allowed_domains;
  std::vector<std::string> blocked_domains;
  int max_search_results = 8;
  int max_fetch_bytes = 262144;
};

struct SearchResult {
  std::string url;
  std::string domain;
  std::string title;
  std::string snippet;
  std::optional<std::string> published_at;
  double score = 0.0;
};

struct FetchResult {
  std::string url;
  std::string final_url;
  std::string content_type;
  std::string fetched_at;
  std::string backend = "broker_fetch";
  bool rendered = false;
  std::optional<std::string> title;
  std::optional<std::string> screenshot_path;
  std::string visible_text;
  std::string response_hash;
  nlohmann::json citations = nlohmann::json::array();
  std::vector<std::string> injection_flags;
};

struct BrowsingRuntimeConfig {
  std::string plane_name = "unknown";
  std::string instance_name = "browsing-unknown";
  std::string instance_role = "browsing";
  std::string node_name = "unknown";
  std::string control_root;
  std::string controller_url = "http://controller.internal:18080";
  std::filesystem::path status_path = "/comet/private/browsing-runtime-status.json";
  std::filesystem::path state_root = "/comet/private/sessions";
  std::filesystem::path ready_path = "/tmp/comet-ready";
  std::string listen_host = "0.0.0.0";
  int port = 18130;
  BrowsingPolicy policy;
};

class BrowsingServer final {
 public:
  explicit BrowsingServer(BrowsingRuntimeConfig config);
  ~BrowsingServer();

  BrowsingServer(const BrowsingServer&) = delete;
  BrowsingServer& operator=(const BrowsingServer&) = delete;

  int Run();
  void RequestStop();

  static BrowsingPolicy ParsePolicyJson(const std::string& json_text);
  static bool IsSafeBrowsingUrl(
      const std::string& url,
      const BrowsingPolicy& policy,
      std::string* reason = nullptr,
      std::string* normalized_host = nullptr);
  static std::vector<SearchResult> ParseBingRssResults(
      const std::string& rss_xml,
      const BrowsingPolicy& policy,
      const std::vector<std::string>& requested_domains,
      int limit);
  static FetchResult SanitizeFetchedDocument(
      const std::string& requested_url,
      const std::string& final_url,
      const std::string& content_type,
      const std::string& body,
      const BrowsingPolicy& policy);
  static std::vector<std::string> DetectInjectionFlags(const std::string& text);

 private:
  struct SessionRecord {
    std::string id;
    std::string current_url;
    std::filesystem::path root;
    std::string created_at;
    std::string updated_at;
    std::set<std::string> visited_domains;
  };

  void AcceptLoop();
  void HandleClient(comet::platform::SocketHandle client_fd);
  HttpResponse HandleRequest(const HttpRequest& request);
  HttpResponse HandleGet(const HttpRequest& request);
  HttpResponse HandlePost(const HttpRequest& request);
  HttpResponse HandleDelete(const HttpRequest& request);
  std::vector<std::string> SplitPath(const std::string& path) const;
  static nlohmann::json ParseJsonBody(const HttpRequest& request);
  HttpResponse BuildJsonResponse(int status_code, const nlohmann::json& payload) const;
  void WriteRuntimeStatus(const std::string& phase, bool ready) const;
  void SetReadyFile(bool ready) const;
  std::string UtcNow() const;
  void AppendAuditLog(const nlohmann::json& payload) const;

  nlohmann::json BuildStatusPayload() const;
  nlohmann::json HandleSearchPayload(const nlohmann::json& payload);
  nlohmann::json HandleFetchPayload(const nlohmann::json& payload);
  nlohmann::json CreateSession(const nlohmann::json& payload);
  nlohmann::json ReadSession(const std::string& session_id) const;
  nlohmann::json ApplySessionAction(const std::string& session_id, const nlohmann::json& payload);
  nlohmann::json DeleteSession(const std::string& session_id);

  std::string NewSessionId() const;
  std::optional<FetchResult> FetchUrlViaBroker(
      const std::string& url,
      std::string* error_code,
      std::string* error_message) const;
  std::optional<FetchResult> FetchUrl(
      const std::string& url,
      std::string* error_code,
      std::string* error_message) const;

  BrowsingRuntimeConfig config_;
  std::atomic<bool> stop_requested_{false};
  comet::platform::SocketHandle listen_fd_ = comet::platform::kInvalidSocket;
  std::unique_ptr<CefBrowserBackend> cef_backend_;
  mutable std::mutex sessions_mutex_;
  std::unordered_map<std::string, SessionRecord> sessions_;
};

}  // namespace comet::browsing
