#pragma once

#include <optional>
#include <string>
#include <sys/types.h>

#include "http/infer_http_types.h"
#include "platform/infer_signal_service.h"
#include "runtime/infer_runtime_types.h"

namespace comet::infer {

class LlamaRpcRuntime final {
 public:
  LlamaRpcRuntime(
      const RuntimeConfig& config,
      std::string started_at,
      InferSignalService& signal_service);

  LlamaRpcRuntime(const LlamaRpcRuntime&) = delete;
  LlamaRpcRuntime& operator=(const LlamaRpcRuntime&) = delete;

  int Run();

 private:
  std::string ResolveModelPath() const;
  std::string BuildRpcServerList() const;
  bool WaitForRpcServersReady(const std::string& rpc_servers, int timeout_sec);
  bool EnsureLocalServerReady(int timeout_sec);
  void StopServer();

  RuntimeConfig config_;
  std::string started_at_;
  InferSignalService& signal_service_;
  std::optional<pid_t> child_pid_;
};

}  // namespace comet::infer
