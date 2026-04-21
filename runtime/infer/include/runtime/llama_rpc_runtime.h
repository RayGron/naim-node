#pragma once

#include <cstdint>
#include <mutex>
#include <optional>
#include <string>
#include <sys/types.h>
#include <thread>
#include <unordered_set>

#include "http/infer_http_types.h"
#include "platform/infer_signal_service.h"
#include "runtime/infer_runtime_types.h"

namespace naim::infer {

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
  void StartServerLogPump(int read_fd);
  void StopServerLogPump();
  void ProcessServerLogLine(const std::string& line);
  std::optional<std::uint64_t> CurrentKvCacheBytes() const;
  void StopServer();

  RuntimeConfig config_;
  std::string started_at_;
  InferSignalService& signal_service_;
  std::optional<pid_t> child_pid_;
  std::optional<int> child_stderr_fd_;
  std::thread child_stderr_thread_;
  mutable std::mutex metrics_mutex_;
  std::optional<std::uint64_t> kv_cache_bytes_;
  std::unordered_set<std::string> kv_cache_log_lines_;
};

}  // namespace naim::infer
