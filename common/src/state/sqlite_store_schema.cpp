#include "naim/state/sqlite_store_schema.h"

#include <string>
#include <vector>

#include "naim/state/sqlite_statement.h"
#include "naim/state/sqlite_store_support.h"
#include "naim/state/state_json.h"

namespace naim::sqlite_store_schema {

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
    update_statement.BindText(2, SerializeDesiredStateV2Json(*desired_state));
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
  EnsureColumn(
      db,
      "model_library_download_jobs",
      "job_kind",
      "job_kind TEXT NOT NULL DEFAULT 'download'");
  EnsureColumn(
      db,
      "model_library_download_jobs",
      "phase",
      "phase TEXT NOT NULL DEFAULT 'queued'");
  EnsureColumn(
      db,
      "model_library_download_jobs",
      "detected_source_format",
      "detected_source_format TEXT NOT NULL DEFAULT ''");
  EnsureColumn(
      db,
      "model_library_download_jobs",
      "desired_output_format",
      "desired_output_format TEXT NOT NULL DEFAULT ''");
  EnsureColumn(
      db,
      "model_library_download_jobs",
      "quantizations_json",
      "quantizations_json TEXT NOT NULL DEFAULT '[]'");
  EnsureColumn(
      db,
      "model_library_download_jobs",
      "retained_output_paths_json",
      "retained_output_paths_json TEXT NOT NULL DEFAULT '[]'");
  EnsureColumn(
      db,
      "model_library_download_jobs",
      "staging_directory",
      "staging_directory TEXT NOT NULL DEFAULT ''");
  EnsureColumn(
      db,
      "model_library_download_jobs",
      "keep_base_gguf",
      "keep_base_gguf INTEGER NOT NULL DEFAULT 1");
  EnsureColumn(
      db,
      "model_library_download_jobs",
      "node_name",
      "node_name TEXT NOT NULL DEFAULT ''");
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
      "skills_factory_skills",
      "group_path",
      "group_path TEXT NOT NULL DEFAULT ''");
  EnsureColumn(
      db,
      "skills_factory_skills",
      "match_terms_json",
      "match_terms_json TEXT NOT NULL DEFAULT '[]'");
  EnsureColumn(
      db,
      "skills_factory_skills",
      "internal",
      "internal INTEGER NOT NULL DEFAULT 0");
  Exec(
      db,
      "CREATE TABLE IF NOT EXISTS skills_factory_groups("
      "path TEXT PRIMARY KEY,"
      "created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,"
      "updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP"
      ");");
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
  Exec(
      db,
      "CREATE TABLE IF NOT EXISTS host_peer_links("
      "observer_node_name TEXT NOT NULL,"
      "peer_node_name TEXT NOT NULL,"
      "peer_endpoint TEXT NOT NULL DEFAULT '',"
      "local_interface TEXT NOT NULL DEFAULT '',"
      "remote_address TEXT NOT NULL DEFAULT '',"
      "seen_udp INTEGER NOT NULL DEFAULT 0,"
      "tcp_reachable INTEGER NOT NULL DEFAULT 0,"
      "rtt_ms INTEGER NOT NULL DEFAULT 0,"
      "last_seen_at TEXT NOT NULL DEFAULT '',"
      "last_probe_at TEXT NOT NULL DEFAULT '',"
      "updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,"
      "PRIMARY KEY(observer_node_name, peer_node_name)"
      ");");
  Exec(
      db,
      "CREATE INDEX IF NOT EXISTS idx_host_peer_links_peer "
      "ON host_peer_links(peer_node_name, observer_node_name);");
  Exec(
      db,
      "CREATE TABLE IF NOT EXISTS file_transfer_tickets("
      "ticket_id TEXT PRIMARY KEY,"
      "source_node_name TEXT NOT NULL,"
      "requester_node_name TEXT NOT NULL,"
      "source_paths_json TEXT NOT NULL DEFAULT '[]',"
      "expires_at TEXT NOT NULL,"
      "max_chunk_bytes INTEGER NOT NULL DEFAULT 0,"
      "created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,"
      "last_validated_at TEXT NOT NULL DEFAULT ''"
      ");");
  Exec(
      db,
      "CREATE TABLE IF NOT EXISTS file_upload_tickets("
      "ticket_id TEXT PRIMARY KEY,"
      "target_node_name TEXT NOT NULL,"
      "uploader_node_name TEXT NOT NULL,"
      "target_relative_path TEXT NOT NULL,"
      "sha256 TEXT NOT NULL DEFAULT '',"
      "size_bytes INTEGER NOT NULL DEFAULT 0,"
      "if_missing INTEGER NOT NULL DEFAULT 1,"
      "expires_at TEXT NOT NULL,"
      "max_chunk_bytes INTEGER NOT NULL DEFAULT 0,"
      "created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,"
      "last_validated_at TEXT NOT NULL DEFAULT ''"
      ");");
  Exec(
      db,
      "CREATE TABLE IF NOT EXISTS interaction_sessions("
      "session_id TEXT PRIMARY KEY,"
      "plane_name TEXT NOT NULL,"
      "owner_kind TEXT NOT NULL DEFAULT 'anonymous',"
      "owner_user_id INTEGER,"
      "auth_session_kind TEXT NOT NULL DEFAULT '',"
      "state TEXT NOT NULL DEFAULT 'active',"
      "last_used_at TEXT NOT NULL DEFAULT '',"
      "archived_at TEXT NOT NULL DEFAULT '',"
      "archive_path TEXT NOT NULL DEFAULT '',"
      "archive_codec TEXT NOT NULL DEFAULT '',"
      "archive_sha256 TEXT NOT NULL DEFAULT '',"
      "context_state_json TEXT NOT NULL DEFAULT '{}',"
      "latest_prompt_tokens INTEGER NOT NULL DEFAULT 0,"
      "estimated_context_tokens INTEGER NOT NULL DEFAULT 0,"
      "compression_state TEXT NOT NULL DEFAULT 'none',"
      "version INTEGER NOT NULL DEFAULT 1,"
      "created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,"
      "updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,"
      "FOREIGN KEY (plane_name) REFERENCES planes(name) ON DELETE CASCADE,"
      "FOREIGN KEY (owner_user_id) REFERENCES users(id) ON DELETE CASCADE"
      ");");
  Exec(
      db,
      "CREATE INDEX IF NOT EXISTS idx_interaction_sessions_owner "
      "ON interaction_sessions(plane_name, owner_kind, owner_user_id, updated_at DESC);");
  Exec(
      db,
      "CREATE TABLE IF NOT EXISTS interaction_messages("
      "session_id TEXT NOT NULL,"
      "seq INTEGER NOT NULL,"
      "role TEXT NOT NULL,"
      "kind TEXT NOT NULL,"
      "content_json TEXT NOT NULL DEFAULT 'null',"
      "usage_json TEXT NOT NULL DEFAULT '{}',"
      "created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,"
      "PRIMARY KEY (session_id, seq),"
      "FOREIGN KEY (session_id) REFERENCES interaction_sessions(session_id) ON DELETE CASCADE"
      ");");
  Exec(
      db,
      "CREATE TABLE IF NOT EXISTS interaction_summaries("
      "id INTEGER PRIMARY KEY AUTOINCREMENT,"
      "session_id TEXT NOT NULL,"
      "turn_range_start INTEGER NOT NULL DEFAULT 0,"
      "turn_range_end INTEGER NOT NULL DEFAULT 0,"
      "summary_json TEXT NOT NULL DEFAULT '{}',"
      "created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,"
      "FOREIGN KEY (session_id) REFERENCES interaction_sessions(session_id) ON DELETE CASCADE"
      ");");
  Exec(
      db,
      "CREATE TABLE IF NOT EXISTS interaction_archives("
      "session_id TEXT PRIMARY KEY,"
      "plane_name TEXT NOT NULL,"
      "owner_kind TEXT NOT NULL DEFAULT 'anonymous',"
      "owner_user_id INTEGER,"
      "archive_path TEXT NOT NULL DEFAULT '',"
      "archive_codec TEXT NOT NULL DEFAULT '',"
      "archive_sha256 TEXT NOT NULL DEFAULT '',"
      "archived_at TEXT NOT NULL DEFAULT '',"
      "restore_state TEXT NOT NULL DEFAULT 'archived',"
      "created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,"
      "updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,"
      "FOREIGN KEY (session_id) REFERENCES interaction_sessions(session_id) ON DELETE CASCADE,"
      "FOREIGN KEY (plane_name) REFERENCES planes(name) ON DELETE CASCADE,"
      "FOREIGN KEY (owner_user_id) REFERENCES users(id) ON DELETE CASCADE"
      ");");
  EnsureColumn(
      db,
      "interaction_sessions",
      "owner_kind",
      "owner_kind TEXT NOT NULL DEFAULT 'anonymous'");
  EnsureColumn(
      db,
      "interaction_sessions",
      "owner_user_id",
      "owner_user_id INTEGER");
  EnsureColumn(
      db,
      "interaction_sessions",
      "auth_session_kind",
      "auth_session_kind TEXT NOT NULL DEFAULT ''");
  EnsureColumn(db, "interaction_sessions", "state", "state TEXT NOT NULL DEFAULT 'active'");
  EnsureColumn(
      db,
      "interaction_sessions",
      "last_used_at",
      "last_used_at TEXT NOT NULL DEFAULT ''");
  EnsureColumn(
      db,
      "interaction_sessions",
      "archived_at",
      "archived_at TEXT NOT NULL DEFAULT ''");
  EnsureColumn(
      db,
      "interaction_sessions",
      "archive_path",
      "archive_path TEXT NOT NULL DEFAULT ''");
  EnsureColumn(
      db,
      "interaction_sessions",
      "archive_codec",
      "archive_codec TEXT NOT NULL DEFAULT ''");
  EnsureColumn(
      db,
      "interaction_sessions",
      "archive_sha256",
      "archive_sha256 TEXT NOT NULL DEFAULT ''");
  EnsureColumn(
      db,
      "interaction_sessions",
      "context_state_json",
      "context_state_json TEXT NOT NULL DEFAULT '{}'");
  EnsureColumn(
      db,
      "interaction_sessions",
      "latest_prompt_tokens",
      "latest_prompt_tokens INTEGER NOT NULL DEFAULT 0");
  EnsureColumn(
      db,
      "interaction_sessions",
      "estimated_context_tokens",
      "estimated_context_tokens INTEGER NOT NULL DEFAULT 0");
  EnsureColumn(
      db,
      "interaction_sessions",
      "compression_state",
      "compression_state TEXT NOT NULL DEFAULT 'none'");
  EnsureColumn(
      db,
      "interaction_sessions",
      "version",
      "version INTEGER NOT NULL DEFAULT 1");
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
      "onboarding_key_hash",
      "onboarding_key_hash TEXT NOT NULL DEFAULT ''");
  EnsureColumn(
      db,
      "registered_hosts",
      "onboarding_state",
      "onboarding_state TEXT NOT NULL DEFAULT 'none'");
  EnsureColumn(
      db,
      "registered_hosts",
      "derived_role",
      "derived_role TEXT NOT NULL DEFAULT 'ineligible'");
  EnsureColumn(
      db,
      "registered_hosts",
      "role_reason",
      "role_reason TEXT NOT NULL DEFAULT ''");
  EnsureColumn(
      db,
      "registered_hosts",
      "storage_role_enabled",
      "storage_role_enabled INTEGER NOT NULL DEFAULT 0");
  EnsureColumn(
      db,
      "registered_hosts",
      "last_inventory_scan_at",
      "last_inventory_scan_at TEXT NOT NULL DEFAULT ''");
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
  EnsureColumn(
      db,
      "model_library_download_jobs",
      "node_name",
      "node_name TEXT NOT NULL DEFAULT ''");
  EnsureColumn(
      db,
      "model_library_download_jobs",
      "hidden",
      "hidden INTEGER NOT NULL DEFAULT 0");
}

}  // namespace naim::sqlite_store_schema
