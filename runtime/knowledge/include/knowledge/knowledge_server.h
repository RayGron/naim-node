#pragma once

#include <atomic>
#include <filesystem>
#include <string>

#include "http/controller_http_transport.h"
#include "http/controller_http_types.h"
#include "naim/core/platform_compat.h"
#include "knowledge/knowledge_store.h"

namespace naim::knowledge_runtime {

struct KnowledgeRuntimeConfig {
  std::string service_id = "kv_default";
  std::string node_name = "unknown";
  std::filesystem::path db_path = "/naim/knowledge/knowledge.sqlite";
  std::filesystem::path status_path = "/naim/knowledge/status.json";
  std::filesystem::path ready_path = "/tmp/naim-ready";
  std::string listen_host = "127.0.0.1";
  int port = 18200;
};

class KnowledgeServer final {
 public:
  explicit KnowledgeServer(KnowledgeRuntimeConfig config);
  ~KnowledgeServer();

  KnowledgeServer(const KnowledgeServer&) = delete;
  KnowledgeServer& operator=(const KnowledgeServer&) = delete;

  int Run();
  void RequestStop();

 private:
  class ApiError final : public std::runtime_error {
   public:
    ApiError(int status, std::string code, std::string message);
    int status() const { return status_; }
    const std::string& code() const { return code_; }

   private:
    int status_;
    std::string code_;
  };

  void AcceptLoop();
  void HandleClient(naim::platform::SocketHandle client_fd);
  HttpResponse HandleRequest(const HttpRequest& request);
  HttpResponse HandleGet(const HttpRequest& request);
  HttpResponse HandlePost(const HttpRequest& request);
  HttpResponse HandlePut(const HttpRequest& request);
  std::vector<std::string> SplitPath(const std::string& path) const;
  static nlohmann::json ParseJsonBody(const HttpRequest& request);
  HttpResponse BuildJsonResponse(int status_code, const nlohmann::json& payload) const;
  void WriteRuntimeStatus(const std::string& phase, bool ready) const;
  void SetReadyFile(bool ready) const;

  KnowledgeRuntimeConfig config_;
  KnowledgeStore store_;
  std::atomic<bool> stop_requested_{false};
  naim::platform::SocketHandle listen_fd_ = naim::platform::kInvalidSocket;
};

}  // namespace naim::knowledge_runtime
