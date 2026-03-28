#include "comet/state/disk_runtime_repository.h"

#include "comet/state/sqlite_statement.h"

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

}  // namespace

DiskRuntimeRepository::DiskRuntimeRepository(sqlite3* db) : db_(db) {}

void DiskRuntimeRepository::UpsertDiskRuntimeState(
    const DiskRuntimeState& runtime_state) {
  Statement statement(
      db_,
      "INSERT INTO disk_runtime_state("
      "disk_name, plane_name, node_name, image_path, filesystem_type, loop_device, "
      "mount_point, runtime_state, attached_at, mounted_at, last_verified_at, "
      "status_message, updated_at"
      ") VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, "
      "COALESCE(NULLIF(?11, ''), CURRENT_TIMESTAMP), ?12, CURRENT_TIMESTAMP) "
      "ON CONFLICT(disk_name, node_name) DO UPDATE SET "
      "plane_name = excluded.plane_name, "
      "image_path = excluded.image_path, "
      "filesystem_type = excluded.filesystem_type, "
      "loop_device = excluded.loop_device, "
      "mount_point = excluded.mount_point, "
      "runtime_state = excluded.runtime_state, "
      "attached_at = excluded.attached_at, "
      "mounted_at = excluded.mounted_at, "
      "last_verified_at = COALESCE(NULLIF(excluded.last_verified_at, ''), disk_runtime_state.last_verified_at, CURRENT_TIMESTAMP), "
      "status_message = excluded.status_message, "
      "updated_at = CURRENT_TIMESTAMP;");
  statement.BindText(1, runtime_state.disk_name);
  statement.BindText(2, runtime_state.plane_name);
  statement.BindText(3, runtime_state.node_name);
  statement.BindText(4, runtime_state.image_path);
  statement.BindText(5, runtime_state.filesystem_type);
  statement.BindText(6, runtime_state.loop_device);
  statement.BindText(7, runtime_state.mount_point);
  statement.BindText(8, runtime_state.runtime_state);
  statement.BindText(9, runtime_state.attached_at);
  statement.BindText(10, runtime_state.mounted_at);
  statement.BindText(11, runtime_state.last_verified_at);
  statement.BindText(12, runtime_state.status_message);
  statement.StepDone();
}

std::optional<DiskRuntimeState> DiskRuntimeRepository::LoadDiskRuntimeState(
    const std::string& disk_name,
    const std::string& node_name) const {
  Statement statement(
      db_,
      "SELECT disk_name, plane_name, node_name, image_path, filesystem_type, loop_device, "
      "mount_point, runtime_state, attached_at, mounted_at, last_verified_at, "
      "status_message, updated_at "
      "FROM disk_runtime_state WHERE disk_name = ?1 AND node_name = ?2;");
  statement.BindText(1, disk_name);
  statement.BindText(2, node_name);
  if (!statement.StepRow()) {
    return std::nullopt;
  }
  return ReadDiskRuntimeState(statement.raw());
}

std::vector<DiskRuntimeState> DiskRuntimeRepository::LoadDiskRuntimeStates(
    const std::optional<std::string>& plane_name,
    const std::optional<std::string>& node_name) const {
  std::vector<DiskRuntimeState> runtime_states;
  std::string sql =
      "SELECT disk_name, plane_name, node_name, image_path, filesystem_type, loop_device, "
      "mount_point, runtime_state, attached_at, mounted_at, last_verified_at, "
      "status_message, updated_at "
      "FROM disk_runtime_state";

  int bind_index = 1;
  if (plane_name.has_value() || node_name.has_value()) {
    sql += " WHERE ";
    bool wrote_condition = false;
    if (plane_name.has_value()) {
      sql += "plane_name = ?" + std::to_string(bind_index++);
      wrote_condition = true;
    }
    if (node_name.has_value()) {
      if (wrote_condition) {
        sql += " AND ";
      }
      sql += "node_name = ?" + std::to_string(bind_index++);
    }
  }
  sql += " ORDER BY plane_name ASC, node_name ASC, disk_name ASC;";

  Statement statement(db_, sql);
  bind_index = 1;
  if (plane_name.has_value()) {
    statement.BindText(bind_index++, *plane_name);
  }
  if (node_name.has_value()) {
    statement.BindText(bind_index++, *node_name);
  }
  while (statement.StepRow()) {
    runtime_states.push_back(ReadDiskRuntimeState(statement.raw()));
  }
  return runtime_states;
}

DiskRuntimeState DiskRuntimeRepository::ReadDiskRuntimeState(sqlite3_stmt* statement) {
  DiskRuntimeState runtime_state;
  runtime_state.disk_name = ToColumnText(statement, 0);
  runtime_state.plane_name = ToColumnText(statement, 1);
  runtime_state.node_name = ToColumnText(statement, 2);
  runtime_state.image_path = ToColumnText(statement, 3);
  runtime_state.filesystem_type = ToColumnText(statement, 4);
  runtime_state.loop_device = ToColumnText(statement, 5);
  runtime_state.mount_point = ToColumnText(statement, 6);
  runtime_state.runtime_state = ToColumnText(statement, 7);
  runtime_state.attached_at = ToColumnText(statement, 8);
  runtime_state.mounted_at = ToColumnText(statement, 9);
  runtime_state.last_verified_at = ToColumnText(statement, 10);
  runtime_state.status_message = ToColumnText(statement, 11);
  runtime_state.updated_at = ToColumnText(statement, 12);
  return runtime_state;
}

}  // namespace comet
