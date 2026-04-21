#pragma once

#include <atomic>
#include <mutex>
#include <optional>
#include <vector>

namespace naim::launcher {

class SignalManager {
 public:
  SignalManager();
  ~SignalManager();

  void RegisterHandlers();

  bool stop_requested() const;
  void RequestStop();

  void TrackChild(int process_id);
  void RemoveChild(int process_id);
  void TerminateChildProcess(int process_id);
  void TerminateTrackedChildren();
  std::optional<int> WaitForAnyChildProcess(int* status);

 private:
  static void HandleSignal(int signal_value);
  void RequestStopFromSignal();
  std::vector<int> SnapshotTrackedChildren() const;

  static SignalManager* active_manager_;

  std::atomic<bool> stop_requested_{false};
  mutable std::mutex mutex_;
  std::vector<int> child_pids_;
};

}  // namespace naim::launcher
