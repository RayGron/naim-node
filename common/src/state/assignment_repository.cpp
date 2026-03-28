#include "comet/state/assignment_repository.h"

#include "comet/state/sqlite_statement.h"

#include <stdexcept>

namespace comet {

namespace {

using Statement = SqliteStatement;

std::string ToColumnText(sqlite3_stmt* statement, int column_index) {
  const unsigned char* text = sqlite3_column_text(statement, column_index);
  if (text == nullptr) {
    return "";
  }
  return reinterpret_cast<const char*>(text);
}

void Exec(sqlite3* db, const std::string& sql) {
  char* error_message = nullptr;
  const int rc = sqlite3_exec(db, sql.c_str(), nullptr, nullptr, &error_message);
  if (rc != SQLITE_OK) {
    const std::string message = error_message == nullptr ? "unknown sqlite error" : error_message;
    sqlite3_free(error_message);
    throw std::runtime_error("sqlite exec failed: " + message);
  }
}

}  // namespace

AssignmentRepository::AssignmentRepository(sqlite3* db) : db_(db) {}

void AssignmentRepository::ReplaceHostAssignments(
    const std::vector<HostAssignment>& assignments) {
  Exec(db_, "BEGIN IMMEDIATE TRANSACTION;");

  try {
    if (!assignments.empty()) {
      Statement supersede_statement(
          db_,
          "UPDATE host_assignments "
          "SET status = 'superseded', "
          "status_message = ?2, "
          "updated_at = CURRENT_TIMESTAMP "
          "WHERE plane_name = ?1 AND status IN ('pending', 'claimed');");
      supersede_statement.BindText(1, assignments.front().plane_name);
      supersede_statement.BindText(
          2,
          "superseded by desired generation " +
              std::to_string(assignments.front().desired_generation));
      supersede_statement.StepDone();
    }

    for (const auto& assignment : assignments) {
      InsertAssignment(assignment);
    }

    Exec(db_, "COMMIT;");
  } catch (...) {
    Exec(db_, "ROLLBACK;");
    throw;
  }
}

void AssignmentRepository::EnqueueHostAssignments(
    const std::vector<HostAssignment>& assignments,
    const std::string& supersede_reason) {
  if (assignments.empty()) {
    return;
  }

  Exec(db_, "BEGIN IMMEDIATE TRANSACTION;");

  try {
    for (const auto& assignment : assignments) {
      Statement supersede_statement(
          db_,
          "UPDATE host_assignments "
          "SET status = 'superseded', "
          "status_message = ?3, "
          "updated_at = CURRENT_TIMESTAMP "
          "WHERE plane_name = ?1 AND node_name = ?2 AND status IN ('pending', 'claimed');");
      supersede_statement.BindText(1, assignment.plane_name);
      supersede_statement.BindText(2, assignment.node_name);
      supersede_statement.BindText(
          3,
          supersede_reason.empty()
              ? "superseded by manual resync for desired generation " +
                    std::to_string(assignment.desired_generation)
              : supersede_reason);
      supersede_statement.StepDone();

      InsertAssignment(assignment);
    }

    Exec(db_, "COMMIT;");
  } catch (...) {
    Exec(db_, "ROLLBACK;");
    throw;
  }
}

int AssignmentRepository::SupersedeHostAssignmentsForPlane(
    const std::string& plane_name,
    const std::string& status_message) {
  Statement statement(
      db_,
      "UPDATE host_assignments "
      "SET status = 'superseded', status_message = ?2, updated_at = CURRENT_TIMESTAMP "
      "WHERE plane_name = ?1 AND status IN ('pending', 'claimed');");
  statement.BindText(1, plane_name);
  statement.BindText(2, status_message);
  statement.StepDone();
  return sqlite3_changes(db_);
}

std::optional<HostAssignment> AssignmentRepository::LoadHostAssignment(
    int assignment_id) const {
  Statement statement(
      db_,
      "SELECT id, node_name, plane_name, desired_generation, attempt_count, max_attempts, "
      "assignment_type, desired_state_json, artifacts_root, status, status_message, progress_json "
      "FROM host_assignments WHERE id = ?1;");
  statement.BindInt(1, assignment_id);
  if (!statement.StepRow()) {
    return std::nullopt;
  }
  return ReadHostAssignment(statement.raw());
}

std::vector<HostAssignment> AssignmentRepository::LoadHostAssignments(
    const std::optional<std::string>& node_name,
    const std::optional<HostAssignmentStatus>& status,
    const std::optional<std::string>& plane_name) const {
  std::vector<HostAssignment> assignments;

  const bool has_node = node_name.has_value();
  const bool has_status = status.has_value();
  const bool has_plane = plane_name.has_value();

  std::string sql =
      "SELECT id, node_name, plane_name, desired_generation, attempt_count, max_attempts, "
      "assignment_type, desired_state_json, artifacts_root, status, status_message, progress_json "
      "FROM host_assignments";
  int bind_index = 1;
  if (has_node || has_status || has_plane) {
    sql += " WHERE ";
    if (has_node) {
      sql += "node_name = ?" + std::to_string(bind_index++);
    }
    if (has_node && (has_status || has_plane)) {
      sql += " AND ";
    }
    if (has_status) {
      sql += "status = ?" + std::to_string(bind_index++);
    }
    if (has_plane) {
      if (has_node || has_status) {
        sql += " AND ";
      }
      sql += "plane_name = ?" + std::to_string(bind_index++);
    }
  }
  sql += " ORDER BY id ASC;";

  Statement statement(db_, sql);
  bind_index = 1;
  if (has_node) {
    statement.BindText(bind_index++, *node_name);
  }
  if (has_status) {
    statement.BindText(bind_index++, ToString(*status));
  }
  if (has_plane) {
    statement.BindText(bind_index++, *plane_name);
  }

  while (statement.StepRow()) {
    assignments.push_back(ReadHostAssignment(statement.raw()));
  }
  return assignments;
}

std::optional<HostAssignment> AssignmentRepository::ClaimNextHostAssignment(
    const std::string& node_name) {
  Exec(db_, "BEGIN IMMEDIATE TRANSACTION;");

  try {
    std::optional<HostAssignment> assignment;
    {
      Statement statement(
          db_,
          "SELECT id, node_name, plane_name, desired_generation, attempt_count, max_attempts, "
          "assignment_type, desired_state_json, artifacts_root, status, status_message, progress_json "
          "FROM host_assignments "
          "WHERE node_name = ?1 AND status = 'pending' AND attempt_count < max_attempts "
          "ORDER BY id ASC LIMIT 1;");
      statement.BindText(1, node_name);
      if (statement.StepRow()) {
        assignment = ReadHostAssignment(statement.raw());
      }
    }

    if (!assignment.has_value()) {
      Exec(db_, "COMMIT;");
      return std::nullopt;
    }

    {
      Statement statement(
          db_,
          "UPDATE host_assignments "
          "SET status = 'claimed', "
          "status_message = '', "
          "progress_json = '{\"phase\":\"queued\",\"title\":\"Assignment claimed\",\"detail\":\"Hostd accepted the assignment and is starting work.\",\"percent\":5}', "
          "attempt_count = attempt_count + 1, "
          "updated_at = CURRENT_TIMESTAMP "
          "WHERE id = ?1 AND status = 'pending';");
      statement.BindInt(1, assignment->id);
      statement.StepDone();
    }

    Exec(db_, "COMMIT;");
    assignment->status = HostAssignmentStatus::Claimed;
    assignment->attempt_count += 1;
    assignment->progress_json =
        "{\"phase\":\"queued\",\"title\":\"Assignment claimed\",\"detail\":"
        "\"Hostd accepted the assignment and is starting work.\",\"percent\":5}";
    return assignment;
  } catch (...) {
    Exec(db_, "ROLLBACK;");
    throw;
  }
}

bool AssignmentRepository::UpdateHostAssignmentProgress(
    int assignment_id,
    const std::string& progress_json) {
  Statement statement(
      db_,
      "UPDATE host_assignments "
      "SET progress_json = ?2, updated_at = CURRENT_TIMESTAMP "
      "WHERE id = ?1 AND status = 'claimed';");
  statement.BindInt(1, assignment_id);
  statement.BindText(2, progress_json.empty() ? "{}" : progress_json);
  statement.StepDone();
  return sqlite3_changes(db_) > 0;
}

bool AssignmentRepository::TransitionClaimedHostAssignment(
    int assignment_id,
    HostAssignmentStatus status,
    const std::string& status_message) {
  Statement statement(
      db_,
      "UPDATE host_assignments "
      "SET status = ?2, status_message = ?3, updated_at = CURRENT_TIMESTAMP "
      "WHERE id = ?1 AND status = 'claimed';");
  statement.BindInt(1, assignment_id);
  statement.BindText(2, ToString(status));
  statement.BindText(3, status_message);
  statement.StepDone();
  return sqlite3_changes(db_) > 0;
}

bool AssignmentRepository::RetryFailedHostAssignment(
    int assignment_id,
    const std::string& status_message) {
  Statement statement(
      db_,
      "UPDATE host_assignments "
      "SET status = 'pending', "
      "status_message = ?2, "
      "max_attempts = CASE "
      "  WHEN max_attempts < attempt_count + 1 THEN attempt_count + 1 "
      "  ELSE max_attempts "
      "END, "
      "updated_at = CURRENT_TIMESTAMP "
      "WHERE id = ?1 AND status = 'failed';");
  statement.BindInt(1, assignment_id);
  statement.BindText(2, status_message);
  statement.StepDone();
  return sqlite3_changes(db_) > 0;
}

void AssignmentRepository::UpdateHostAssignmentStatus(
    int assignment_id,
    HostAssignmentStatus status,
    const std::string& status_message) {
  Statement statement(
      db_,
      "UPDATE host_assignments "
      "SET status = ?2, status_message = ?3, updated_at = CURRENT_TIMESTAMP "
      "WHERE id = ?1;");
  statement.BindInt(1, assignment_id);
  statement.BindText(2, ToString(status));
  statement.BindText(3, status_message);
  statement.StepDone();
}

void AssignmentRepository::InsertAssignment(const HostAssignment& assignment) {
  Statement statement(
      db_,
      "INSERT INTO host_assignments("
      "node_name, plane_name, desired_generation, attempt_count, max_attempts, "
      "assignment_type, desired_state_json, artifacts_root, status, status_message, progress_json, "
      "updated_at"
      ") VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, CURRENT_TIMESTAMP);");
  statement.BindText(1, assignment.node_name);
  statement.BindText(2, assignment.plane_name);
  statement.BindInt(3, assignment.desired_generation);
  statement.BindInt(4, assignment.attempt_count);
  statement.BindInt(5, assignment.max_attempts);
  statement.BindText(6, assignment.assignment_type);
  statement.BindText(7, assignment.desired_state_json);
  statement.BindText(8, assignment.artifacts_root);
  statement.BindText(9, ToString(assignment.status));
  statement.BindText(10, assignment.status_message);
  statement.BindText(11, assignment.progress_json.empty() ? "{}" : assignment.progress_json);
  statement.StepDone();
}

HostAssignment AssignmentRepository::ReadHostAssignment(sqlite3_stmt* statement) {
  HostAssignment assignment;
  assignment.id = sqlite3_column_int(statement, 0);
  assignment.node_name = ToColumnText(statement, 1);
  assignment.plane_name = ToColumnText(statement, 2);
  assignment.desired_generation = sqlite3_column_int(statement, 3);
  assignment.attempt_count = sqlite3_column_int(statement, 4);
  assignment.max_attempts = sqlite3_column_int(statement, 5);
  assignment.assignment_type = ToColumnText(statement, 6);
  assignment.desired_state_json = ToColumnText(statement, 7);
  assignment.artifacts_root = ToColumnText(statement, 8);
  assignment.status = ParseHostAssignmentStatus(ToColumnText(statement, 9));
  assignment.status_message = ToColumnText(statement, 10);
  assignment.progress_json = ToColumnText(statement, 11);
  return assignment;
}

}  // namespace comet
