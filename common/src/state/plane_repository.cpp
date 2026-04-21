#include "naim/state/plane_repository.h"

#include "naim/state/sqlite_statement.h"

#include <array>
#include <stdexcept>

namespace naim {

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

PlaneRepository::PlaneRepository(sqlite3* db) : db_(db) {}

std::optional<int> PlaneRepository::LoadPlaneRebalanceIteration(
    const std::string& plane_name) const {
  Statement statement(
      db_,
      "SELECT rebalance_iteration FROM planes WHERE name = ?1;");
  statement.BindText(1, plane_name);
  if (!statement.StepRow()) {
    return std::nullopt;
  }
  return sqlite3_column_int(statement.raw(), 0);
}

std::vector<PlaneRecord> PlaneRepository::LoadPlanes() const {
  std::vector<PlaneRecord> planes;
  Statement statement(
      db_,
      "SELECT name, shared_disk_name, control_root, artifacts_root, plane_mode, generation, applied_generation, "
      "rebalance_iteration, state, created_at FROM planes ORDER BY name ASC;");
  while (statement.StepRow()) {
    planes.push_back(ReadPlane(statement.raw()));
  }
  return planes;
}

std::optional<PlaneRecord> PlaneRepository::LoadPlane(const std::string& plane_name) const {
  Statement statement(
      db_,
      "SELECT name, shared_disk_name, control_root, artifacts_root, plane_mode, generation, applied_generation, "
      "rebalance_iteration, state, created_at FROM planes WHERE name = ?1;");
  statement.BindText(1, plane_name);
  if (!statement.StepRow()) {
    return std::nullopt;
  }
  return ReadPlane(statement.raw());
}

bool PlaneRepository::UpdatePlaneState(
    const std::string& plane_name,
    const std::string& state) {
  Statement statement(
      db_,
      "UPDATE planes SET state = ?2 WHERE name = ?1;");
  statement.BindText(1, plane_name);
  statement.BindText(2, state);
  statement.StepDone();
  return sqlite3_changes(db_) > 0;
}

bool PlaneRepository::UpdatePlaneAppliedGeneration(
    const std::string& plane_name,
    int applied_generation) {
  Statement statement(
      db_,
      "UPDATE planes SET applied_generation = ?2 WHERE name = ?1;");
  statement.BindText(1, plane_name);
  statement.BindInt(2, applied_generation);
  statement.StepDone();
  return sqlite3_changes(db_) > 0;
}

bool PlaneRepository::UpdatePlaneArtifactsRoot(
    const std::string& plane_name,
    const std::string& artifacts_root) {
  Statement statement(
      db_,
      "UPDATE planes SET artifacts_root = ?2 WHERE name = ?1;");
  statement.BindText(1, plane_name);
  statement.BindText(2, artifacts_root);
  statement.StepDone();
  return sqlite3_changes(db_) > 0;
}

void PlaneRepository::DeletePlane(const std::string& plane_name) {
  Exec(db_, "BEGIN IMMEDIATE TRANSACTION;");
  try {
    const std::array<std::string, 9> delete_sql = {
        "DELETE FROM host_assignments WHERE plane_name = ?1;",
        "DELETE FROM rollout_actions WHERE plane_name = ?1;",
        "DELETE FROM disk_runtime_state WHERE plane_name = ?1;",
        "DELETE FROM event_log WHERE plane_name = ?1;",
        "DELETE FROM scheduler_plane_runtime WHERE plane_name = ?1;",
        "DELETE FROM scheduler_worker_runtime WHERE plane_name = ?1;",
        "DELETE FROM scheduler_node_runtime WHERE plane_name = ?1;",
        "UPDATE host_observations "
        "SET plane_name = CASE WHEN plane_name = ?1 THEN '' ELSE plane_name END, "
        "    applied_generation = CASE WHEN plane_name = ?1 THEN NULL ELSE applied_generation END, "
        "    last_assignment_id = CASE WHEN plane_name = ?1 THEN NULL ELSE last_assignment_id END, "
        "    updated_at = CURRENT_TIMESTAMP "
        "WHERE plane_name = ?1;",
        "DELETE FROM planes WHERE name = ?1;",
    };
    for (const auto& sql : delete_sql) {
      Statement statement(db_, sql);
      statement.BindText(1, plane_name);
      statement.StepDone();
    }
    Exec(db_, "COMMIT;");
  } catch (...) {
    Exec(db_, "ROLLBACK;");
    throw;
  }
}

PlaneRecord PlaneRepository::ReadPlane(sqlite3_stmt* statement) {
  PlaneRecord plane;
  plane.name = ToColumnText(statement, 0);
  plane.shared_disk_name = ToColumnText(statement, 1);
  plane.control_root = ToColumnText(statement, 2);
  plane.artifacts_root = ToColumnText(statement, 3);
  plane.plane_mode = ToColumnText(statement, 4);
  plane.generation = sqlite3_column_int(statement, 5);
  plane.applied_generation = sqlite3_column_int(statement, 6);
  plane.rebalance_iteration = sqlite3_column_int(statement, 7);
  plane.state = ToColumnText(statement, 8);
  plane.created_at = ToColumnText(statement, 9);
  return plane;
}

}  // namespace naim
