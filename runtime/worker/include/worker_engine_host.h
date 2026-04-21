#pragma once

#include <optional>
#include <string>
#include <sys/types.h>

#include "worker_config.h"
#include "worker_model_resolver.h"
#include "worker_signal_service.h"
#include "worker_status_service.h"

namespace naim::worker {

class WorkerEngineHost final {
 public:
  explicit WorkerEngineHost(WorkerConfig config);
  ~WorkerEngineHost();

  WorkerEngineHost(const WorkerEngineHost&) = delete;
  WorkerEngineHost& operator=(const WorkerEngineHost&) = delete;

  int Run();

 private:
  int RunIdleWorker();
  int RunRpcWorker();
  bool EnsureTcpEndpointReady(const std::string& host, int port, int timeout_sec) const;
  void StopChildProcess();

  WorkerConfig config_;
  WorkerSignalService signal_service_;
  WorkerModelResolver model_resolver_;
  WorkerStatusService status_service_;
  std::string started_at_;
  std::optional<std::string> loaded_model_path_;
  std::optional<pid_t> child_pid_;
};

}  // namespace naim::worker
