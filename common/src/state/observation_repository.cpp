#include "naim/state/observation_repository.h"

#include "naim/state/sqlite_statement.h"

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

std::optional<int> ToOptionalColumnInt(sqlite3_stmt* statement, int column_index) {
  if (sqlite3_column_type(statement, column_index) == SQLITE_NULL) {
    return std::nullopt;
  }
  return sqlite3_column_int(statement, column_index);
}

}  // namespace

ObservationRepository::ObservationRepository(sqlite3* db) : db_(db) {}

void ObservationRepository::UpsertHostObservation(const HostObservation& observation) {
  Statement statement(
      db_,
      "INSERT INTO host_observations("
      "node_name, plane_name, applied_generation, last_assignment_id, status, "
      "status_message, observed_state_json, runtime_status_json, "
      "instance_runtime_json, gpu_telemetry_json, disk_telemetry_json, "
      "network_telemetry_json, cpu_telemetry_json, heartbeat_at, updated_at"
      ") VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP) "
      "ON CONFLICT(node_name) DO UPDATE SET "
      "plane_name = excluded.plane_name, "
      "applied_generation = excluded.applied_generation, "
      "last_assignment_id = excluded.last_assignment_id, "
      "status = excluded.status, "
      "status_message = excluded.status_message, "
      "observed_state_json = excluded.observed_state_json, "
      "runtime_status_json = excluded.runtime_status_json, "
      "instance_runtime_json = excluded.instance_runtime_json, "
      "gpu_telemetry_json = excluded.gpu_telemetry_json, "
      "disk_telemetry_json = excluded.disk_telemetry_json, "
      "network_telemetry_json = excluded.network_telemetry_json, "
      "cpu_telemetry_json = excluded.cpu_telemetry_json, "
      "heartbeat_at = CURRENT_TIMESTAMP, "
      "updated_at = CURRENT_TIMESTAMP;");
  statement.BindText(1, observation.node_name);
  statement.BindText(2, observation.plane_name);
  statement.BindOptionalInt(3, observation.applied_generation);
  statement.BindOptionalInt(4, observation.last_assignment_id);
  statement.BindText(5, ToString(observation.status));
  statement.BindText(6, observation.status_message);
  statement.BindText(7, observation.observed_state_json);
  statement.BindText(8, observation.runtime_status_json);
  statement.BindText(9, observation.instance_runtime_json);
  statement.BindText(10, observation.gpu_telemetry_json);
  statement.BindText(11, observation.disk_telemetry_json);
  statement.BindText(12, observation.network_telemetry_json);
  statement.BindText(13, observation.cpu_telemetry_json);
  statement.StepDone();
}

std::optional<HostObservation> ObservationRepository::LoadHostObservation(
    const std::string& node_name) const {
  Statement statement(
      db_,
      "SELECT node_name, plane_name, applied_generation, last_assignment_id, status, "
      "status_message, observed_state_json, runtime_status_json, "
      "instance_runtime_json, gpu_telemetry_json, disk_telemetry_json, network_telemetry_json, cpu_telemetry_json, heartbeat_at "
      "FROM host_observations WHERE node_name = ?1;");
  statement.BindText(1, node_name);
  if (!statement.StepRow()) {
    return std::nullopt;
  }
  return ReadHostObservation(statement.raw());
}

std::vector<HostObservation> ObservationRepository::LoadHostObservations(
    const std::optional<std::string>& node_name,
    const std::optional<std::string>& plane_name) const {
  std::vector<HostObservation> observations;

  std::string sql =
      "SELECT node_name, plane_name, applied_generation, last_assignment_id, status, "
      "status_message, observed_state_json, runtime_status_json, "
      "instance_runtime_json, gpu_telemetry_json, disk_telemetry_json, network_telemetry_json, cpu_telemetry_json, heartbeat_at "
      "FROM host_observations";
  int bind_index = 1;
  bool has_where = false;
  if (node_name.has_value()) {
    sql += " WHERE node_name = ?" + std::to_string(bind_index++);
    has_where = true;
  }
  if (plane_name.has_value()) {
    sql += has_where ? " AND " : " WHERE ";
    sql += "plane_name = ?" + std::to_string(bind_index++);
  }
  sql += " ORDER BY node_name ASC;";

  Statement statement(db_, sql);
  bind_index = 1;
  if (node_name.has_value()) {
    statement.BindText(bind_index++, *node_name);
  }
  if (plane_name.has_value()) {
    statement.BindText(bind_index++, *plane_name);
  }

  while (statement.StepRow()) {
    observations.push_back(ReadHostObservation(statement.raw()));
  }
  return observations;
}

HostObservation ObservationRepository::ReadHostObservation(sqlite3_stmt* statement) {
  HostObservation observation;
  observation.node_name = ToColumnText(statement, 0);
  observation.plane_name = ToColumnText(statement, 1);
  observation.applied_generation = ToOptionalColumnInt(statement, 2);
  observation.last_assignment_id = ToOptionalColumnInt(statement, 3);
  observation.status = ParseHostObservationStatus(ToColumnText(statement, 4));
  observation.status_message = ToColumnText(statement, 5);
  observation.observed_state_json = ToColumnText(statement, 6);
  observation.runtime_status_json = ToColumnText(statement, 7);
  observation.instance_runtime_json = ToColumnText(statement, 8);
  observation.gpu_telemetry_json = ToColumnText(statement, 9);
  observation.disk_telemetry_json = ToColumnText(statement, 10);
  observation.network_telemetry_json = ToColumnText(statement, 11);
  observation.cpu_telemetry_json = ToColumnText(statement, 12);
  observation.heartbeat_at = ToColumnText(statement, 13);
  return observation;
}

}  // namespace naim
