#pragma once

#include <atomic>
#include <filesystem>
#include <string>
#include <thread>

#include "naim/core/platform_compat.h"
#include "naim/runtime/runtime_status.h"
#include "http/controller_http_types.h"
#include "http/controller_http_transport.h"
#include "skills/skills_store.h"

namespace naim::skills {

struct SkillsRuntimeConfig {
  std::string plane_name = "unknown";
  std::string instance_name = "skills-unknown";
  std::string instance_role = "skills";
  std::string node_name = "unknown";
  std::string control_root;
  std::string controller_url = "http://controller.internal:18080";
  std::filesystem::path db_path = "/naim/private/skills.sqlite";
  std::filesystem::path status_path = "/naim/private/skills-runtime-status.json";
  std::filesystem::path ready_path = "/tmp/naim-ready";
  std::string listen_host = "0.0.0.0";
  int port = 18120;
};

class SkillsServer final {
 public:
  explicit SkillsServer(SkillsRuntimeConfig config);
  ~SkillsServer();

  SkillsServer(const SkillsServer&) = delete;
  SkillsServer& operator=(const SkillsServer&) = delete;

  int Run();
  void RequestStop();

 private:
  void AcceptLoop();
  void HandleClient(naim::platform::SocketHandle client_fd);
  HttpResponse HandleRequest(const HttpRequest& request);
  HttpResponse HandleGet(const HttpRequest& request);
  HttpResponse HandlePost(const HttpRequest& request);
  HttpResponse HandlePut(const HttpRequest& request);
  HttpResponse HandlePatch(const HttpRequest& request);
  HttpResponse HandleDelete(const HttpRequest& request);
  std::vector<std::string> SplitPath(const std::string& path) const;
  static nlohmann::json ParseJsonBody(const HttpRequest& request);
  HttpResponse BuildJsonResponse(int status_code, const nlohmann::json& payload) const;
  void SyncFromController();
  void WriteRuntimeStatus(const std::string& phase, bool ready) const;
  void SetReadyFile(bool ready) const;

  SkillsRuntimeConfig config_;
  SkillsStore store_;
  std::atomic<bool> stop_requested_{false};
  naim::platform::SocketHandle listen_fd_ = naim::platform::kInvalidSocket;
};

}  // namespace naim::skills
