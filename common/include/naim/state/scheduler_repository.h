#pragma once

#include <optional>
#include <string>
#include <vector>

#include <sqlite3.h>

#include "naim/state/sqlite_store.h"

namespace naim {

class SchedulerRepository final {
 public:
  explicit SchedulerRepository(sqlite3* db);

  void ReplaceRolloutActions(
      const std::string& plane_name,
      int desired_generation,
      const std::vector<SchedulerRolloutAction>& actions);
  std::vector<RolloutActionRecord> LoadRolloutActions(
      const std::optional<std::string>& plane_name,
      const std::optional<std::string>& target_node_name,
      const std::optional<RolloutActionStatus>& status) const;
  bool UpdateRolloutActionStatus(
      int action_id,
      RolloutActionStatus status,
      const std::string& status_message);

  void UpsertSchedulerPlaneRuntime(const SchedulerPlaneRuntime& runtime);
  std::optional<SchedulerPlaneRuntime> LoadSchedulerPlaneRuntime(
      const std::string& plane_name) const;
  void ClearSchedulerPlaneRuntime(const std::string& plane_name);

  void UpsertSchedulerWorkerRuntime(const SchedulerWorkerRuntime& runtime);
  std::optional<SchedulerWorkerRuntime> LoadSchedulerWorkerRuntime(
      const std::string& worker_name) const;
  std::vector<SchedulerWorkerRuntime> LoadSchedulerWorkerRuntimes(
      const std::optional<std::string>& plane_name) const;

  void UpsertSchedulerNodeRuntime(const SchedulerNodeRuntime& runtime);
  std::optional<SchedulerNodeRuntime> LoadSchedulerNodeRuntime(
      const std::string& node_name) const;
  std::vector<SchedulerNodeRuntime> LoadSchedulerNodeRuntimes(
      const std::optional<std::string>& plane_name) const;

 private:
  static RolloutActionRecord ReadRolloutAction(sqlite3_stmt* statement);
  static SchedulerPlaneRuntime ReadSchedulerPlaneRuntime(sqlite3_stmt* statement);
  static SchedulerWorkerRuntime ReadSchedulerWorkerRuntime(sqlite3_stmt* statement);
  static SchedulerNodeRuntime ReadSchedulerNodeRuntime(sqlite3_stmt* statement);

  sqlite3* db_;
};

}  // namespace naim
