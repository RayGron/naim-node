#include "comet/state/scheduler_repository.h"

#include "comet/state/sqlite_statement.h"

#include <stdexcept>

#include <nlohmann/json.hpp>

namespace comet {

namespace {

using Statement = SqliteStatement;
using nlohmann::json;

std::string ToColumnText(sqlite3_stmt* statement, int column_index) {
  const unsigned char* text = sqlite3_column_text(statement, column_index);
  if (text == nullptr) {
    return "";
  }
  return reinterpret_cast<const char*>(text);
}

std::optional<int> ToOptionalColumnInt(sqlite3_stmt* statement, int column_index) {
  if (sqlite3_column_type(statement, column_index) == SQLITE_NULL) {
    return std::nullopt;
  }
  return sqlite3_column_int(statement, column_index);
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

std::vector<std::string> DeserializeStringArray(const std::string& json_text) {
  std::vector<std::string> values;
  if (json_text.empty()) {
    return values;
  }
  const json parsed = json::parse(json_text);
  if (!parsed.is_array()) {
    return values;
  }
  for (const auto& item : parsed) {
    if (item.is_string()) {
      values.push_back(item.get<std::string>());
    }
  }
  return values;
}

std::string SerializeStringArray(const std::vector<std::string>& values) {
  return json(values).dump();
}

}  // namespace

SchedulerRepository::SchedulerRepository(sqlite3* db) : db_(db) {}

void SchedulerRepository::ReplaceRolloutActions(
    const std::string& plane_name,
    int desired_generation,
    const std::vector<SchedulerRolloutAction>& actions) {
  Exec(db_, "BEGIN IMMEDIATE TRANSACTION;");
  try {
    {
      Statement delete_statement(
          db_,
          "DELETE FROM rollout_actions WHERE plane_name = ?1;");
      delete_statement.BindText(1, plane_name);
      delete_statement.StepDone();
    }

    for (const auto& action : actions) {
      Statement insert_statement(
          db_,
          "INSERT INTO rollout_actions("
          "plane_name, desired_generation, step, worker_name, action, target_node_name, "
          "target_gpu_device, victim_worker_names_json, reason, status, status_message, "
          "created_at, updated_at"
          ") VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP);");
      insert_statement.BindText(1, plane_name);
      insert_statement.BindInt(2, desired_generation);
      insert_statement.BindInt(3, action.step);
      insert_statement.BindText(4, action.worker_name);
      insert_statement.BindText(5, action.action);
      insert_statement.BindText(6, action.target_node_name);
      insert_statement.BindText(7, action.target_gpu_device);
      insert_statement.BindText(8, SerializeStringArray(action.victim_worker_names));
      insert_statement.BindText(9, action.reason);
      insert_statement.BindText(10, ToString(RolloutActionStatus::Pending));
      insert_statement.BindText(11, "");
      insert_statement.StepDone();
    }

    Exec(db_, "COMMIT;");
  } catch (...) {
    Exec(db_, "ROLLBACK;");
    throw;
  }
}

std::vector<RolloutActionRecord> SchedulerRepository::LoadRolloutActions(
    const std::optional<std::string>& plane_name,
    const std::optional<std::string>& target_node_name,
    const std::optional<RolloutActionStatus>& status) const {
  std::string sql =
      "SELECT id, plane_name, desired_generation, step, worker_name, action, target_node_name, "
      "target_gpu_device, victim_worker_names_json, reason, status, status_message "
      "FROM rollout_actions";
  int bind_index = 1;
  bool has_where = false;
  if (plane_name.has_value()) {
    sql += " WHERE plane_name = ?" + std::to_string(bind_index++);
    has_where = true;
  }
  if (target_node_name.has_value()) {
    sql += has_where ? " AND " : " WHERE ";
    sql += "target_node_name = ?" + std::to_string(bind_index++);
    has_where = true;
  }
  if (status.has_value()) {
    sql += has_where ? " AND " : " WHERE ";
    sql += "status = ?" + std::to_string(bind_index++);
  }
  sql += " ORDER BY plane_name ASC, desired_generation ASC, step ASC, id ASC;";

  Statement statement(db_, sql);
  bind_index = 1;
  if (plane_name.has_value()) {
    statement.BindText(bind_index++, *plane_name);
  }
  if (target_node_name.has_value()) {
    statement.BindText(bind_index++, *target_node_name);
  }
  if (status.has_value()) {
    statement.BindText(bind_index++, ToString(*status));
  }

  std::vector<RolloutActionRecord> actions;
  while (statement.StepRow()) {
    actions.push_back(ReadRolloutAction(statement.raw()));
  }
  return actions;
}

bool SchedulerRepository::UpdateRolloutActionStatus(
    int action_id,
    RolloutActionStatus status,
    const std::string& status_message) {
  Statement statement(
      db_,
      "UPDATE rollout_actions "
      "SET status = ?2, status_message = ?3, updated_at = CURRENT_TIMESTAMP "
      "WHERE id = ?1;");
  statement.BindInt(1, action_id);
  statement.BindText(2, ToString(status));
  statement.BindText(3, status_message);
  statement.StepDone();
  return sqlite3_changes(db_) == 1;
}

void SchedulerRepository::UpsertSchedulerPlaneRuntime(
    const SchedulerPlaneRuntime& runtime) {
  Statement statement(
      db_,
      "INSERT INTO scheduler_plane_runtime("
      "plane_name, active_action, active_worker_name, phase, action_generation, "
      "stable_samples, rollback_attempt_count, source_node_name, source_gpu_device, "
      "target_node_name, target_gpu_device, previous_state_json, status_message, "
      "started_at, updated_at"
      ") VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, "
      "COALESCE(NULLIF(?14, ''), CURRENT_TIMESTAMP), CURRENT_TIMESTAMP) "
      "ON CONFLICT(plane_name) DO UPDATE SET "
      "active_action = excluded.active_action, "
      "active_worker_name = excluded.active_worker_name, "
      "phase = excluded.phase, "
      "action_generation = excluded.action_generation, "
      "stable_samples = excluded.stable_samples, "
      "rollback_attempt_count = excluded.rollback_attempt_count, "
      "source_node_name = excluded.source_node_name, "
      "source_gpu_device = excluded.source_gpu_device, "
      "target_node_name = excluded.target_node_name, "
      "target_gpu_device = excluded.target_gpu_device, "
      "previous_state_json = excluded.previous_state_json, "
      "status_message = excluded.status_message, "
      "started_at = COALESCE(NULLIF(excluded.started_at, ''), scheduler_plane_runtime.started_at, CURRENT_TIMESTAMP), "
      "updated_at = CURRENT_TIMESTAMP;");
  statement.BindText(1, runtime.plane_name);
  statement.BindText(2, runtime.active_action);
  statement.BindText(3, runtime.active_worker_name);
  statement.BindText(4, runtime.phase);
  statement.BindInt(5, runtime.action_generation);
  statement.BindInt(6, runtime.stable_samples);
  statement.BindInt(7, runtime.rollback_attempt_count);
  statement.BindText(8, runtime.source_node_name);
  statement.BindText(9, runtime.source_gpu_device);
  statement.BindText(10, runtime.target_node_name);
  statement.BindText(11, runtime.target_gpu_device);
  statement.BindText(12, runtime.previous_state_json);
  statement.BindText(13, runtime.status_message);
  statement.BindText(14, runtime.started_at);
  statement.StepDone();
}

std::optional<SchedulerPlaneRuntime> SchedulerRepository::LoadSchedulerPlaneRuntime(
    const std::string& plane_name) const {
  Statement statement(
      db_,
      "SELECT plane_name, active_action, active_worker_name, phase, action_generation, "
      "stable_samples, rollback_attempt_count, source_node_name, source_gpu_device, "
      "target_node_name, target_gpu_device, previous_state_json, status_message, "
      "started_at, updated_at "
      "FROM scheduler_plane_runtime WHERE plane_name = ?1;");
  statement.BindText(1, plane_name);
  if (!statement.StepRow()) {
    return std::nullopt;
  }
  return ReadSchedulerPlaneRuntime(statement.raw());
}

void SchedulerRepository::ClearSchedulerPlaneRuntime(const std::string& plane_name) {
  Statement statement(
      db_,
      "DELETE FROM scheduler_plane_runtime WHERE plane_name = ?1;");
  statement.BindText(1, plane_name);
  statement.StepDone();
}

void SchedulerRepository::UpsertSchedulerWorkerRuntime(
    const SchedulerWorkerRuntime& runtime) {
  Statement statement(
      db_,
      "INSERT INTO scheduler_worker_runtime("
      "worker_name, plane_name, last_move_at, last_eviction_at, "
      "last_verified_generation, last_scheduler_phase, last_status_message, "
      "manual_intervention_required, updated_at"
      ") VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, CURRENT_TIMESTAMP) "
      "ON CONFLICT(worker_name) DO UPDATE SET "
      "plane_name = excluded.plane_name, "
      "last_move_at = excluded.last_move_at, "
      "last_eviction_at = excluded.last_eviction_at, "
      "last_verified_generation = excluded.last_verified_generation, "
      "last_scheduler_phase = excluded.last_scheduler_phase, "
      "last_status_message = excluded.last_status_message, "
      "manual_intervention_required = excluded.manual_intervention_required, "
      "updated_at = CURRENT_TIMESTAMP;");
  statement.BindText(1, runtime.worker_name);
  statement.BindText(2, runtime.plane_name);
  statement.BindText(3, runtime.last_move_at);
  statement.BindText(4, runtime.last_eviction_at);
  statement.BindOptionalInt(5, runtime.last_verified_generation);
  statement.BindText(6, runtime.last_scheduler_phase);
  statement.BindText(7, runtime.last_status_message);
  statement.BindInt(8, runtime.manual_intervention_required ? 1 : 0);
  statement.StepDone();
}

std::optional<SchedulerWorkerRuntime> SchedulerRepository::LoadSchedulerWorkerRuntime(
    const std::string& worker_name) const {
  Statement statement(
      db_,
      "SELECT worker_name, plane_name, last_move_at, last_eviction_at, "
      "last_verified_generation, last_scheduler_phase, last_status_message, "
      "manual_intervention_required, updated_at "
      "FROM scheduler_worker_runtime WHERE worker_name = ?1;");
  statement.BindText(1, worker_name);
  if (!statement.StepRow()) {
    return std::nullopt;
  }
  return ReadSchedulerWorkerRuntime(statement.raw());
}

std::vector<SchedulerWorkerRuntime> SchedulerRepository::LoadSchedulerWorkerRuntimes(
    const std::optional<std::string>& plane_name) const {
  std::string sql =
      "SELECT worker_name, plane_name, last_move_at, last_eviction_at, "
      "last_verified_generation, last_scheduler_phase, last_status_message, "
      "manual_intervention_required, updated_at "
      "FROM scheduler_worker_runtime";
  if (plane_name.has_value()) {
    sql += " WHERE plane_name = ?1";
  }
  sql += " ORDER BY worker_name ASC;";
  Statement statement(db_, sql);
  if (plane_name.has_value()) {
    statement.BindText(1, *plane_name);
  }
  std::vector<SchedulerWorkerRuntime> runtimes;
  while (statement.StepRow()) {
    runtimes.push_back(ReadSchedulerWorkerRuntime(statement.raw()));
  }
  return runtimes;
}

void SchedulerRepository::UpsertSchedulerNodeRuntime(
    const SchedulerNodeRuntime& runtime) {
  Statement statement(
      db_,
      "INSERT INTO scheduler_node_runtime("
      "node_name, plane_name, last_move_at, last_verified_generation, updated_at"
      ") VALUES(?1, ?2, ?3, ?4, CURRENT_TIMESTAMP) "
      "ON CONFLICT(node_name) DO UPDATE SET "
      "plane_name = excluded.plane_name, "
      "last_move_at = excluded.last_move_at, "
      "last_verified_generation = excluded.last_verified_generation, "
      "updated_at = CURRENT_TIMESTAMP;");
  statement.BindText(1, runtime.node_name);
  statement.BindText(2, runtime.plane_name);
  statement.BindText(3, runtime.last_move_at);
  statement.BindOptionalInt(4, runtime.last_verified_generation);
  statement.StepDone();
}

std::optional<SchedulerNodeRuntime> SchedulerRepository::LoadSchedulerNodeRuntime(
    const std::string& node_name) const {
  Statement statement(
      db_,
      "SELECT node_name, plane_name, last_move_at, last_verified_generation, updated_at "
      "FROM scheduler_node_runtime WHERE node_name = ?1;");
  statement.BindText(1, node_name);
  if (!statement.StepRow()) {
    return std::nullopt;
  }
  return ReadSchedulerNodeRuntime(statement.raw());
}

std::vector<SchedulerNodeRuntime> SchedulerRepository::LoadSchedulerNodeRuntimes(
    const std::optional<std::string>& plane_name) const {
  std::string sql =
      "SELECT node_name, plane_name, last_move_at, last_verified_generation, updated_at "
      "FROM scheduler_node_runtime";
  if (plane_name.has_value()) {
    sql += " WHERE plane_name = ?1";
  }
  sql += " ORDER BY node_name ASC;";
  Statement statement(db_, sql);
  if (plane_name.has_value()) {
    statement.BindText(1, *plane_name);
  }
  std::vector<SchedulerNodeRuntime> runtimes;
  while (statement.StepRow()) {
    runtimes.push_back(ReadSchedulerNodeRuntime(statement.raw()));
  }
  return runtimes;
}

RolloutActionRecord SchedulerRepository::ReadRolloutAction(sqlite3_stmt* statement) {
  RolloutActionRecord action;
  action.id = sqlite3_column_int(statement, 0);
  action.plane_name = ToColumnText(statement, 1);
  action.desired_generation = sqlite3_column_int(statement, 2);
  action.step = sqlite3_column_int(statement, 3);
  action.worker_name = ToColumnText(statement, 4);
  action.action = ToColumnText(statement, 5);
  action.target_node_name = ToColumnText(statement, 6);
  action.target_gpu_device = ToColumnText(statement, 7);
  action.victim_worker_names = DeserializeStringArray(ToColumnText(statement, 8));
  action.reason = ToColumnText(statement, 9);
  action.status = ParseRolloutActionStatus(ToColumnText(statement, 10));
  action.status_message = ToColumnText(statement, 11);
  return action;
}

SchedulerPlaneRuntime SchedulerRepository::ReadSchedulerPlaneRuntime(
    sqlite3_stmt* statement) {
  SchedulerPlaneRuntime runtime;
  runtime.plane_name = ToColumnText(statement, 0);
  runtime.active_action = ToColumnText(statement, 1);
  runtime.active_worker_name = ToColumnText(statement, 2);
  runtime.phase = ToColumnText(statement, 3);
  runtime.action_generation = sqlite3_column_int(statement, 4);
  runtime.stable_samples = sqlite3_column_int(statement, 5);
  runtime.rollback_attempt_count = sqlite3_column_int(statement, 6);
  runtime.source_node_name = ToColumnText(statement, 7);
  runtime.source_gpu_device = ToColumnText(statement, 8);
  runtime.target_node_name = ToColumnText(statement, 9);
  runtime.target_gpu_device = ToColumnText(statement, 10);
  runtime.previous_state_json = ToColumnText(statement, 11);
  runtime.status_message = ToColumnText(statement, 12);
  runtime.started_at = ToColumnText(statement, 13);
  runtime.updated_at = ToColumnText(statement, 14);
  return runtime;
}

SchedulerWorkerRuntime SchedulerRepository::ReadSchedulerWorkerRuntime(
    sqlite3_stmt* statement) {
  SchedulerWorkerRuntime runtime;
  runtime.worker_name = ToColumnText(statement, 0);
  runtime.plane_name = ToColumnText(statement, 1);
  runtime.last_move_at = ToColumnText(statement, 2);
  runtime.last_eviction_at = ToColumnText(statement, 3);
  runtime.last_verified_generation = ToOptionalColumnInt(statement, 4);
  runtime.last_scheduler_phase = ToColumnText(statement, 5);
  runtime.last_status_message = ToColumnText(statement, 6);
  runtime.manual_intervention_required = sqlite3_column_int(statement, 7) != 0;
  runtime.updated_at = ToColumnText(statement, 8);
  return runtime;
}

SchedulerNodeRuntime SchedulerRepository::ReadSchedulerNodeRuntime(
    sqlite3_stmt* statement) {
  SchedulerNodeRuntime runtime;
  runtime.node_name = ToColumnText(statement, 0);
  runtime.plane_name = ToColumnText(statement, 1);
  runtime.last_move_at = ToColumnText(statement, 2);
  runtime.last_verified_generation = ToOptionalColumnInt(statement, 3);
  runtime.updated_at = ToColumnText(statement, 4);
  return runtime;
}

}  // namespace comet
