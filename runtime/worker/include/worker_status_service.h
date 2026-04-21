#pragma once

#include <optional>
#include <string>

#include "naim/runtime/runtime_status.h"

#include "worker_config.h"

namespace naim::worker {

class WorkerStatusService final {
 public:
  WorkerStatusService() = default;

  std::string CurrentTimestamp() const;

  void MarkStarting(
      const WorkerConfig& config,
      const std::string& started_at,
      const std::optional<std::string>& model_path) const;
  void MarkWaitingForModel(
      const WorkerConfig& config,
      const std::string& started_at,
      const std::optional<std::string>& model_path) const;
  void MarkRunning(
      const WorkerConfig& config,
      const std::string& started_at,
      const std::string& model_path) const;
  void MarkFailed(
      const WorkerConfig& config,
      const std::string& started_at,
      const std::optional<std::string>& model_path) const;
  void MarkStopped(
      const WorkerConfig& config,
      const std::string& started_at,
      const std::optional<std::string>& model_path) const;

 private:
  static void TouchReadyFile(bool ready);
  static naim::RuntimeStatus BuildStatus(
      const WorkerConfig& config,
      const std::string& phase,
      bool ready,
      const std::string& started_at,
      const std::string& last_activity_at,
      const std::string& model_path);
  static void WriteStatus(const naim::RuntimeStatus& status, const std::string& path);
};

}  // namespace naim::worker
