#include "comet/state/sqlite_store_schema.h"

#include <string>
#include <vector>

#include "comet/state/sqlite_statement.h"
#include "comet/state/sqlite_store_support.h"
#include "comet/state/state_json.h"

namespace comet::sqlite_store_schema {

namespace {

using Statement = SqliteStatement;
using sqlite_store_support::EnsureColumn;
using sqlite_store_support::Exec;
using sqlite_store_support::ToColumnText;

void BackfillDesiredStateJson(sqlite3* db, const LoadDesiredStateFn& load_desired_state) {
  Statement plane_statement(
      db,
      "SELECT name FROM planes WHERE desired_state_json = '' ORDER BY name ASC;");
  std::vector<std::string> plane_names;
  while (plane_statement.StepRow()) {
    plane_names.push_back(ToColumnText(plane_statement.raw(), 0));
  }
  for (const auto& plane_name : plane_names) {
    const auto desired_state = load_desired_state(plane_name);
    if (!desired_state.has_value()) {
      continue;
    }
    Statement update_statement(
        db,
        "UPDATE planes SET desired_state_json = ?2 WHERE name = ?1;");
    update_statement.BindText(1, plane_name);
    update_statement.BindText(2, SerializeDesiredStateJson(*desired_state));
    update_statement.StepDone();
  }
  Statement clear_legacy_statement(
      db,
      "UPDATE planes SET interaction_settings_json = '' WHERE interaction_settings_json != '';");
  clear_legacy_statement.StepDone();
}

void BackfillRolloutActionPlaneName(sqlite3* db) {
  Statement blank_rollout_statement(
      db,
      "SELECT 1 FROM rollout_actions WHERE plane_name = '' LIMIT 1;");
  if (!blank_rollout_statement.StepRow()) {
    return;
  }
  Statement plane_statement(
      db,
      "SELECT name FROM planes ORDER BY generation DESC, created_at DESC LIMIT 1;");
  if (!plane_statement.StepRow()) {
    return;
  }
  const std::string plane_name = ToColumnText(plane_statement.raw(), 0);
  if (plane_name.empty()) {
    return;
  }
  Statement backfill_statement(
      db,
      "UPDATE rollout_actions SET plane_name = ?1 WHERE plane_name = '';");
  backfill_statement.BindText(1, plane_name);
  backfill_statement.StepDone();
}

}  // namespace

void InitializeSchema(
    sqlite3* db,
    std::string_view bootstrap_sql,
    const LoadDesiredStateFn& load_desired_state) {
  Exec(db, std::string(bootstrap_sql));
  EnsureColumn(db, "planes", "control_root", "control_root TEXT NOT NULL DEFAULT ''");
  EnsureColumn(db, "planes", "artifacts_root", "artifacts_root TEXT NOT NULL DEFAULT ''");
  EnsureColumn(db, "planes", "plane_mode", "plane_mode TEXT NOT NULL DEFAULT 'compute'");
  EnsureColumn(
      db,
      "planes",
      "bootstrap_model_json",
      "bootstrap_model_json TEXT NOT NULL DEFAULT ''");
  EnsureColumn(
      db,
      "planes",
      "interaction_settings_json",
      "interaction_settings_json TEXT NOT NULL DEFAULT ''");
  EnsureColumn(
      db,
      "planes",
      "desired_state_json",
      "desired_state_json TEXT NOT NULL DEFAULT ''");
  EnsureColumn(
      db,
      "planes",
      "inference_config_json",
      "inference_config_json TEXT NOT NULL DEFAULT ''");
  EnsureColumn(
      db,
      "planes",
      "gateway_config_json",
      "gateway_config_json TEXT NOT NULL DEFAULT ''");
  EnsureColumn(
      db,
      "planes",
      "runtime_gpu_nodes_json",
      "runtime_gpu_nodes_json TEXT NOT NULL DEFAULT ''");
  EnsureColumn(
      db,
      "planes",
      "applied_generation",
      "applied_generation INTEGER NOT NULL DEFAULT 0");
  EnsureColumn(
      db,
      "planes",
      "rebalance_iteration",
      "rebalance_iteration INTEGER NOT NULL DEFAULT 0");
  EnsureColumn(
      db,
      "host_assignments",
      "progress_json",
      "progress_json TEXT NOT NULL DEFAULT '{}'");
  EnsureColumn(
      db,
      "host_observations",
      "runtime_status_json",
      "runtime_status_json TEXT NOT NULL DEFAULT ''");
  EnsureColumn(db, "node_gpus", "memory_mb", "memory_mb INTEGER NOT NULL DEFAULT 0");
  EnsureColumn(db, "instances", "share_mode", "share_mode TEXT NOT NULL DEFAULT 'exclusive'");
  EnsureColumn(db, "instances", "placement_mode", "placement_mode TEXT NOT NULL DEFAULT 'manual'");
  EnsureColumn(db, "instances", "priority", "priority INTEGER NOT NULL DEFAULT 100");
  EnsureColumn(db, "instances", "preemptible", "preemptible INTEGER NOT NULL DEFAULT 0");
  EnsureColumn(db, "instances", "memory_cap_mb", "memory_cap_mb INTEGER");
  EnsureColumn(
      db,
      "host_observations",
      "instance_runtime_json",
      "instance_runtime_json TEXT NOT NULL DEFAULT ''");
  EnsureColumn(
      db,
      "host_observations",
      "gpu_telemetry_json",
      "gpu_telemetry_json TEXT NOT NULL DEFAULT ''");
  EnsureColumn(
      db,
      "host_observations",
      "disk_telemetry_json",
      "disk_telemetry_json TEXT NOT NULL DEFAULT ''");
  EnsureColumn(
      db,
      "host_observations",
      "network_telemetry_json",
      "network_telemetry_json TEXT NOT NULL DEFAULT ''");
  EnsureColumn(
      db,
      "host_observations",
      "cpu_telemetry_json",
      "cpu_telemetry_json TEXT NOT NULL DEFAULT ''");
  BackfillDesiredStateJson(db, load_desired_state);
  EnsureColumn(
      db,
      "rollout_actions",
      "plane_name",
      "plane_name TEXT NOT NULL DEFAULT ''");
  BackfillRolloutActionPlaneName(db);
  EnsureColumn(
      db,
      "nodes",
      "execution_mode",
      "execution_mode TEXT NOT NULL DEFAULT 'mixed'");
  EnsureColumn(
      db,
      "scheduler_worker_runtime",
      "last_scheduler_phase",
      "last_scheduler_phase TEXT NOT NULL DEFAULT ''");
  EnsureColumn(
      db,
      "scheduler_worker_runtime",
      "last_status_message",
      "last_status_message TEXT NOT NULL DEFAULT ''");
  EnsureColumn(
      db,
      "scheduler_plane_runtime",
      "status_message",
      "status_message TEXT NOT NULL DEFAULT ''");
  EnsureColumn(
      db,
      "registered_hosts",
      "advertised_address",
      "advertised_address TEXT NOT NULL DEFAULT ''");
  EnsureColumn(
      db,
      "registered_hosts",
      "public_key_base64",
      "public_key_base64 TEXT NOT NULL DEFAULT ''");
  EnsureColumn(
      db,
      "registered_hosts",
      "controller_public_key_fingerprint",
      "controller_public_key_fingerprint TEXT NOT NULL DEFAULT ''");
  EnsureColumn(
      db,
      "registered_hosts",
      "transport_mode",
      "transport_mode TEXT NOT NULL DEFAULT 'out'");
  EnsureColumn(
      db,
      "registered_hosts",
      "execution_mode",
      "execution_mode TEXT NOT NULL DEFAULT 'mixed'");
  EnsureColumn(
      db,
      "registered_hosts",
      "registration_state",
      "registration_state TEXT NOT NULL DEFAULT 'registered'");
  EnsureColumn(
      db,
      "registered_hosts",
      "session_state",
      "session_state TEXT NOT NULL DEFAULT 'disconnected'");
  EnsureColumn(
      db,
      "registered_hosts",
      "session_token",
      "session_token TEXT NOT NULL DEFAULT ''");
  EnsureColumn(
      db,
      "registered_hosts",
      "session_expires_at",
      "session_expires_at TEXT NOT NULL DEFAULT ''");
  EnsureColumn(
      db,
      "registered_hosts",
      "session_host_sequence",
      "session_host_sequence INTEGER NOT NULL DEFAULT 0");
  EnsureColumn(
      db,
      "registered_hosts",
      "session_controller_sequence",
      "session_controller_sequence INTEGER NOT NULL DEFAULT 0");
  EnsureColumn(
      db,
      "registered_hosts",
      "capabilities_json",
      "capabilities_json TEXT NOT NULL DEFAULT '{}'");
  EnsureColumn(
      db,
      "registered_hosts",
      "status_message",
      "status_message TEXT NOT NULL DEFAULT ''");
  EnsureColumn(
      db,
      "registered_hosts",
      "last_session_at",
      "last_session_at TEXT NOT NULL DEFAULT ''");
  EnsureColumn(
      db,
      "registered_hosts",
      "last_heartbeat_at",
      "last_heartbeat_at TEXT NOT NULL DEFAULT ''");
}

}  // namespace comet::sqlite_store_schema
