#pragma once

#include <atomic>

namespace naim::worker {

class WorkerSignalService final {
 public:
  WorkerSignalService() = default;
  ~WorkerSignalService();

  WorkerSignalService(const WorkerSignalService&) = delete;
  WorkerSignalService& operator=(const WorkerSignalService&) = delete;

  void RegisterHandlers();
  bool StopRequested() const;

 private:
  static void HandleSignal(int signal);

  static WorkerSignalService* instance_;
  std::atomic<bool> stop_requested_{false};
};

}  // namespace naim::worker
