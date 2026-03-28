#pragma once

#include <optional>
#include <string>
#include <vector>

#include <sqlite3.h>

#include "comet/state/sqlite_store.h"

namespace comet {

class AssignmentRepository final {
 public:
  explicit AssignmentRepository(sqlite3* db);

  void ReplaceHostAssignments(const std::vector<HostAssignment>& assignments);
  void EnqueueHostAssignments(
      const std::vector<HostAssignment>& assignments,
      const std::string& supersede_reason);
  int SupersedeHostAssignmentsForPlane(
      const std::string& plane_name,
      const std::string& status_message);
  std::optional<HostAssignment> LoadHostAssignment(int assignment_id) const;
  std::vector<HostAssignment> LoadHostAssignments(
      const std::optional<std::string>& node_name,
      const std::optional<HostAssignmentStatus>& status,
      const std::optional<std::string>& plane_name) const;
  std::optional<HostAssignment> ClaimNextHostAssignment(const std::string& node_name);
  bool UpdateHostAssignmentProgress(int assignment_id, const std::string& progress_json);
  bool TransitionClaimedHostAssignment(
      int assignment_id,
      HostAssignmentStatus status,
      const std::string& status_message);
  bool RetryFailedHostAssignment(int assignment_id, const std::string& status_message);
  void UpdateHostAssignmentStatus(
      int assignment_id,
      HostAssignmentStatus status,
      const std::string& status_message);

 private:
  void InsertAssignment(const HostAssignment& assignment);
  static HostAssignment ReadHostAssignment(sqlite3_stmt* statement);

  sqlite3* db_;
};

}  // namespace comet
