#pragma once

#include <filesystem>
#include <memory>
#include <optional>
#include <string>

namespace naim::browsing {

struct CefRenderedDocument {
  std::string final_url;
  std::string content_type = "text/html; charset=utf-8";
  std::optional<std::string> title;
  std::string visible_text;
  std::string html_source;
  std::optional<std::filesystem::path> screenshot_path;
};

class CefBrowserBackend final {
 public:
  explicit CefBrowserBackend(std::filesystem::path state_root);
  ~CefBrowserBackend();

  CefBrowserBackend(const CefBrowserBackend&) = delete;
  CefBrowserBackend& operator=(const CefBrowserBackend&) = delete;

  bool IsAvailable() const;

  std::optional<CefRenderedDocument> FetchPage(
      const std::string& url,
      const std::filesystem::path& worker_root,
      std::string* error_message,
      bool include_html_source = false) const;

  std::optional<CefRenderedDocument> OpenSession(
      const std::string& session_id,
      const std::filesystem::path& session_root,
      const std::string& url,
      std::string* error_message);

  std::optional<CefRenderedDocument> SnapshotSession(
      const std::string& session_id,
      std::string* error_message) const;

  std::optional<CefRenderedDocument> ExtractSession(
      const std::string& session_id,
      std::string* error_message) const;

  void DeleteSession(const std::string& session_id);

 private:
  class Impl;

  std::filesystem::path state_root_;
  std::unique_ptr<Impl> impl_;
};

}  // namespace naim::browsing
