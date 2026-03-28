#pragma once

#include <atomic>
#include <optional>
#include <string>
#include <thread>

#include "http/infer_http_types.h"
#include "platform/infer_signal_service.h"
#include "runtime/infer_runtime_types.h"
#include "runtime/llama_library_engine.h"

namespace comet::infer {

class LocalHttpServer final {
 public:
  LocalHttpServer(
      std::string host,
      int port,
      std::string service_name,
      const RuntimeConfig& config,
      LlamaLibraryEngine* engine,
      const InferSignalService& signal_service,
      bool dynamic_upstream = false,
      std::optional<UpstreamTarget> upstream = std::nullopt);
  ~LocalHttpServer();

  LocalHttpServer(const LocalHttpServer&) = delete;
  LocalHttpServer& operator=(const LocalHttpServer&) = delete;

  void Start();
  void Stop();

 private:
  void AcceptLoop();
  void HandleClient(int client_fd);

  std::string host_;
  int port_ = 0;
  std::string service_name_;
  RuntimeConfig config_;
  LlamaLibraryEngine* engine_ = nullptr;
  const InferSignalService& signal_service_;
  bool dynamic_upstream_ = false;
  std::optional<UpstreamTarget> upstream_;
  std::thread worker_;
  std::atomic<bool> running_{false};
  int listen_fd_ = -1;
};

}  // namespace comet::infer
