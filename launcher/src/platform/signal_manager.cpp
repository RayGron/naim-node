#include "platform/signal_manager.h"

#include <algorithm>
#include <csignal>
#include <stdexcept>

#if defined(_WIN32)
#include <windows.h>
#else
#include <sys/wait.h>
#include <unistd.h>
#endif

namespace naim::launcher {

SignalManager* SignalManager::active_manager_ = nullptr;

SignalManager::SignalManager() = default;

SignalManager::~SignalManager() {
  if (active_manager_ == this) {
    active_manager_ = nullptr;
  }
}

void SignalManager::RegisterHandlers() {
  active_manager_ = this;
#if defined(_WIN32)
  std::signal(SIGINT, HandleSignal);
  std::signal(SIGTERM, HandleSignal);
#else
  struct sigaction action {};
  action.sa_handler = HandleSignal;
  sigemptyset(&action.sa_mask);
  action.sa_flags = 0;
  sigaction(SIGINT, &action, nullptr);
  sigaction(SIGTERM, &action, nullptr);
#endif
}

bool SignalManager::stop_requested() const {
  return stop_requested_.load();
}

void SignalManager::RequestStop() {
  RequestStopFromSignal();
}

void SignalManager::TrackChild(const int process_id) {
  std::lock_guard<std::mutex> lock(mutex_);
  child_pids_.push_back(process_id);
}

void SignalManager::RemoveChild(const int process_id) {
  std::lock_guard<std::mutex> lock(mutex_);
  child_pids_.erase(
      std::remove(child_pids_.begin(), child_pids_.end(), process_id),
      child_pids_.end());
}

void SignalManager::TerminateChildProcess(const int process_id) {
#if defined(_WIN32)
  if (process_id <= 0) {
    return;
  }
  TerminateProcess(reinterpret_cast<HANDLE>(process_id), 1);
#else
  if (process_id > 0) {
    kill(process_id, SIGTERM);
  }
#endif
}

void SignalManager::TerminateTrackedChildren() {
  for (const int process_id : SnapshotTrackedChildren()) {
    TerminateChildProcess(process_id);
  }
}

std::optional<int> SignalManager::WaitForAnyChildProcess(int* status) {
#if defined(_WIN32)
  const auto handles_snapshot = SnapshotTrackedChildren();
  if (handles_snapshot.empty()) {
    return std::nullopt;
  }

  std::vector<HANDLE> handles;
  handles.reserve(handles_snapshot.size());
  for (const int child : handles_snapshot) {
    handles.push_back(reinterpret_cast<HANDLE>(child));
  }
  const DWORD wait_result = WaitForMultipleObjects(
      static_cast<DWORD>(handles.size()),
      handles.data(),
      FALSE,
      INFINITE);
  if (wait_result < WAIT_OBJECT_0 ||
      wait_result >= WAIT_OBJECT_0 + handles.size()) {
    throw std::runtime_error("WaitForMultipleObjects failed");
  }
  const std::size_t index = static_cast<std::size_t>(wait_result - WAIT_OBJECT_0);
  DWORD exit_code = 0;
  GetExitCodeProcess(handles[index], &exit_code);
  if (status != nullptr) {
    *status = static_cast<int>(exit_code);
  }
  CloseHandle(handles[index]);
  return handles_snapshot[index];
#else
  int local_status = 0;
  const pid_t pid = waitpid(-1, &local_status, 0);
  if (pid < 0) {
    return std::nullopt;
  }
  if (status != nullptr) {
    *status = local_status;
  }
  return static_cast<int>(pid);
#endif
}

void SignalManager::HandleSignal(int) {
  if (active_manager_ != nullptr) {
    active_manager_->RequestStopFromSignal();
  }
}

void SignalManager::RequestStopFromSignal() {
  stop_requested_.store(true);
  TerminateTrackedChildren();
}

std::vector<int> SignalManager::SnapshotTrackedChildren() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return child_pids_;
}

}  // namespace naim::launcher
