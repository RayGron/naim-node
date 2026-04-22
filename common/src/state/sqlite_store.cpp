#include "naim/state/auth_repository.h"
#include "naim/state/assignment_repository.h"
#include "naim/state/controller_settings_repository.h"
#include "naim/state/desired_state_repository.h"
#include "naim/state/desired_state_sqlite_codec.h"
#include "naim/state/disk_runtime_repository.h"
#include "naim/state/event_repository.h"
#include "naim/state/interaction_repository.h"
#include "naim/state/model_library_repository.h"
#include "naim/state/node_availability_repository.h"
#include "naim/state/observation_repository.h"
#include "naim/state/plane_repository.h"
#include "naim/state/registered_host_repository.h"
#include "naim/state/scheduler_repository.h"
#include "naim/state/skills_factory_repository.h"
#include "naim/state/sqlite_statement.h"
#include "naim/state/sqlite_store.h"
#include "naim/state/sqlite_store_schema.h"
#include "naim/state/sqlite_store_support.h"
#include "naim/state/state_json.h"

#include <array>
#include <filesystem>
#include <map>
#include <set>
#include <stdexcept>
#include <string>
#include <utility>

#include <sqlite3.h>

namespace naim {

namespace {

using sqlite_store_support::AsSqlite;
using sqlite_store_support::AvailabilityOverrideFromStatement;
using sqlite_store_support::EnsureColumn;
using sqlite_store_support::Exec;
using sqlite_store_support::ToColumnText;

constexpr const char* kBootstrapSql = R"SQL(
PRAGMA journal_mode=WAL;
PRAGMA foreign_keys=ON;

CREATE TABLE IF NOT EXISTS planes (
    name TEXT PRIMARY KEY,
    shared_disk_name TEXT NOT NULL,
    control_root TEXT NOT NULL DEFAULT '',
    artifacts_root TEXT NOT NULL DEFAULT '',
    plane_mode TEXT NOT NULL DEFAULT 'compute',
    bootstrap_model_json TEXT NOT NULL DEFAULT '',
    interaction_settings_json TEXT NOT NULL DEFAULT '',
    desired_state_json TEXT NOT NULL DEFAULT '',
    inference_config_json TEXT NOT NULL DEFAULT '',
    gateway_config_json TEXT NOT NULL DEFAULT '',
    runtime_gpu_nodes_json TEXT NOT NULL DEFAULT '',
    generation INTEGER NOT NULL DEFAULT 1,
    applied_generation INTEGER NOT NULL DEFAULT 0,
    rebalance_iteration INTEGER NOT NULL DEFAULT 0,
    state TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS nodes (
    name TEXT PRIMARY KEY,
    platform TEXT NOT NULL,
    execution_mode TEXT NOT NULL DEFAULT 'mixed',
    state TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS registered_hosts (
    node_name TEXT PRIMARY KEY,
    advertised_address TEXT NOT NULL DEFAULT '',
    public_key_base64 TEXT NOT NULL DEFAULT '',
    controller_public_key_fingerprint TEXT NOT NULL DEFAULT '',
    transport_mode TEXT NOT NULL DEFAULT 'out',
    execution_mode TEXT NOT NULL DEFAULT 'mixed',
    registration_state TEXT NOT NULL DEFAULT 'registered',
    onboarding_key_hash TEXT NOT NULL DEFAULT '',
    onboarding_state TEXT NOT NULL DEFAULT 'none',
    derived_role TEXT NOT NULL DEFAULT 'ineligible',
    role_reason TEXT NOT NULL DEFAULT '',
    storage_role_enabled INTEGER NOT NULL DEFAULT 0,
    last_inventory_scan_at TEXT NOT NULL DEFAULT '',
    session_state TEXT NOT NULL DEFAULT 'disconnected',
    session_token TEXT NOT NULL DEFAULT '',
    session_expires_at TEXT NOT NULL DEFAULT '',
    session_host_sequence INTEGER NOT NULL DEFAULT 0,
    session_controller_sequence INTEGER NOT NULL DEFAULT 0,
    capabilities_json TEXT NOT NULL DEFAULT '{}',
    status_message TEXT NOT NULL DEFAULT '',
    last_session_at TEXT NOT NULL DEFAULT '',
    last_heartbeat_at TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS host_peer_links (
    observer_node_name TEXT NOT NULL,
    peer_node_name TEXT NOT NULL,
    peer_endpoint TEXT NOT NULL DEFAULT '',
    local_interface TEXT NOT NULL DEFAULT '',
    remote_address TEXT NOT NULL DEFAULT '',
    seen_udp INTEGER NOT NULL DEFAULT 0,
    tcp_reachable INTEGER NOT NULL DEFAULT 0,
    rtt_ms INTEGER NOT NULL DEFAULT 0,
    last_seen_at TEXT NOT NULL DEFAULT '',
    last_probe_at TEXT NOT NULL DEFAULT '',
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY(observer_node_name, peer_node_name)
);

CREATE INDEX IF NOT EXISTS idx_host_peer_links_peer
    ON host_peer_links(peer_node_name, observer_node_name);

CREATE TABLE IF NOT EXISTS file_transfer_tickets (
    ticket_id TEXT PRIMARY KEY,
    source_node_name TEXT NOT NULL,
    requester_node_name TEXT NOT NULL,
    source_paths_json TEXT NOT NULL DEFAULT '[]',
    expires_at TEXT NOT NULL,
    max_chunk_bytes INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    last_validated_at TEXT NOT NULL DEFAULT ''
);

CREATE TABLE IF NOT EXISTS file_upload_tickets (
    ticket_id TEXT PRIMARY KEY,
    target_node_name TEXT NOT NULL,
    uploader_node_name TEXT NOT NULL,
    target_relative_path TEXT NOT NULL,
    sha256 TEXT NOT NULL DEFAULT '',
    size_bytes INTEGER NOT NULL DEFAULT 0,
    if_missing INTEGER NOT NULL DEFAULT 1,
    expires_at TEXT NOT NULL,
    max_chunk_bytes INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    last_validated_at TEXT NOT NULL DEFAULT ''
);

CREATE TABLE IF NOT EXISTS users (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    username TEXT NOT NULL UNIQUE,
    role TEXT NOT NULL,
    password_hash TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    last_login_at TEXT NOT NULL DEFAULT ''
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_users_single_admin
    ON users(role)
    WHERE role = 'admin';

CREATE TABLE IF NOT EXISTS webauthn_credentials (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER NOT NULL,
    credential_id TEXT NOT NULL UNIQUE,
    public_key TEXT NOT NULL,
    counter INTEGER NOT NULL DEFAULT 0,
    transports_json TEXT NOT NULL DEFAULT '[]',
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    last_used_at TEXT NOT NULL DEFAULT '',
    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS registration_invites (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    token TEXT NOT NULL UNIQUE,
    created_by_user_id INTEGER NOT NULL,
    expires_at TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    used_by_user_id INTEGER,
    used_at TEXT NOT NULL DEFAULT '',
    revoked_at TEXT NOT NULL DEFAULT '',
    FOREIGN KEY (created_by_user_id) REFERENCES users(id) ON DELETE CASCADE,
    FOREIGN KEY (used_by_user_id) REFERENCES users(id) ON DELETE SET NULL
);

CREATE TABLE IF NOT EXISTS user_ssh_keys (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER NOT NULL,
    label TEXT NOT NULL DEFAULT '',
    public_key TEXT NOT NULL,
    fingerprint TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    revoked_at TEXT NOT NULL DEFAULT '',
    last_used_at TEXT NOT NULL DEFAULT '',
    UNIQUE (user_id, fingerprint),
    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS auth_sessions (
    token TEXT PRIMARY KEY,
    user_id INTEGER NOT NULL,
    session_kind TEXT NOT NULL,
    plane_name TEXT NOT NULL DEFAULT '',
    expires_at TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    revoked_at TEXT NOT NULL DEFAULT '',
    last_used_at TEXT NOT NULL DEFAULT '',
    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS model_library_download_jobs (
    id TEXT PRIMARY KEY,
    job_kind TEXT NOT NULL DEFAULT 'download',
    status TEXT NOT NULL DEFAULT 'queued',
    phase TEXT NOT NULL DEFAULT 'queued',
    model_id TEXT NOT NULL DEFAULT '',
    node_name TEXT NOT NULL DEFAULT '',
    target_root TEXT NOT NULL DEFAULT '',
    target_subdir TEXT NOT NULL DEFAULT '',
    detected_source_format TEXT NOT NULL DEFAULT '',
    desired_output_format TEXT NOT NULL DEFAULT '',
    source_urls_json TEXT NOT NULL DEFAULT '[]',
    target_paths_json TEXT NOT NULL DEFAULT '[]',
    quantizations_json TEXT NOT NULL DEFAULT '[]',
    retained_output_paths_json TEXT NOT NULL DEFAULT '[]',
    current_item TEXT NOT NULL DEFAULT '',
    staging_directory TEXT NOT NULL DEFAULT '',
    bytes_total INTEGER,
    bytes_done INTEGER NOT NULL DEFAULT 0,
    part_count INTEGER NOT NULL DEFAULT 0,
    keep_base_gguf INTEGER NOT NULL DEFAULT 1,
    error_message TEXT NOT NULL DEFAULT '',
    hidden INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS skills_factory_skills (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    group_path TEXT NOT NULL DEFAULT '',
    description TEXT NOT NULL,
    content TEXT NOT NULL,
    match_terms_json TEXT NOT NULL DEFAULT '[]',
    internal INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS skills_factory_groups (
    path TEXT PRIMARY KEY,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS plane_skill_bindings (
    plane_name TEXT NOT NULL,
    skill_id TEXT NOT NULL,
    enabled INTEGER NOT NULL DEFAULT 1,
    session_ids_json TEXT NOT NULL DEFAULT '[]',
    naim_links_json TEXT NOT NULL DEFAULT '[]',
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (plane_name, skill_id),
    FOREIGN KEY (plane_name) REFERENCES planes(name) ON DELETE CASCADE,
    FOREIGN KEY (skill_id) REFERENCES skills_factory_skills(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS interaction_sessions (
    session_id TEXT PRIMARY KEY,
    plane_name TEXT NOT NULL,
    owner_kind TEXT NOT NULL DEFAULT 'anonymous',
    owner_user_id INTEGER,
    auth_session_kind TEXT NOT NULL DEFAULT '',
    state TEXT NOT NULL DEFAULT 'active',
    last_used_at TEXT NOT NULL DEFAULT '',
    archived_at TEXT NOT NULL DEFAULT '',
    archive_path TEXT NOT NULL DEFAULT '',
    archive_codec TEXT NOT NULL DEFAULT '',
    archive_sha256 TEXT NOT NULL DEFAULT '',
    context_state_json TEXT NOT NULL DEFAULT '{}',
    latest_prompt_tokens INTEGER NOT NULL DEFAULT 0,
    estimated_context_tokens INTEGER NOT NULL DEFAULT 0,
    compression_state TEXT NOT NULL DEFAULT 'none',
    version INTEGER NOT NULL DEFAULT 1,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (plane_name) REFERENCES planes(name) ON DELETE CASCADE,
    FOREIGN KEY (owner_user_id) REFERENCES users(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_interaction_sessions_owner
    ON interaction_sessions(plane_name, owner_kind, owner_user_id, updated_at DESC);

CREATE TABLE IF NOT EXISTS interaction_messages (
    session_id TEXT NOT NULL,
    seq INTEGER NOT NULL,
    role TEXT NOT NULL,
    kind TEXT NOT NULL,
    content_json TEXT NOT NULL DEFAULT 'null',
    usage_json TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (session_id, seq),
    FOREIGN KEY (session_id) REFERENCES interaction_sessions(session_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS interaction_summaries (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id TEXT NOT NULL,
    turn_range_start INTEGER NOT NULL DEFAULT 0,
    turn_range_end INTEGER NOT NULL DEFAULT 0,
    summary_json TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (session_id) REFERENCES interaction_sessions(session_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS interaction_archives (
    session_id TEXT PRIMARY KEY,
    plane_name TEXT NOT NULL,
    owner_kind TEXT NOT NULL DEFAULT 'anonymous',
    owner_user_id INTEGER,
    archive_path TEXT NOT NULL DEFAULT '',
    archive_codec TEXT NOT NULL DEFAULT '',
    archive_sha256 TEXT NOT NULL DEFAULT '',
    archived_at TEXT NOT NULL DEFAULT '',
    restore_state TEXT NOT NULL DEFAULT 'archived',
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (session_id) REFERENCES interaction_sessions(session_id) ON DELETE CASCADE,
    FOREIGN KEY (plane_name) REFERENCES planes(name) ON DELETE CASCADE,
    FOREIGN KEY (owner_user_id) REFERENCES users(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS controller_settings (
    setting_key TEXT PRIMARY KEY,
    setting_value TEXT NOT NULL DEFAULT '',
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS plane_nodes (
    plane_name TEXT NOT NULL,
    node_name TEXT NOT NULL,
    PRIMARY KEY (plane_name, node_name),
    FOREIGN KEY (plane_name) REFERENCES planes(name) ON DELETE CASCADE,
    FOREIGN KEY (node_name) REFERENCES nodes(name) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS node_gpus (
    node_name TEXT NOT NULL,
    gpu_device TEXT NOT NULL,
    memory_mb INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (node_name, gpu_device),
    FOREIGN KEY (node_name) REFERENCES nodes(name) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS virtual_disks (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    plane_name TEXT NOT NULL,
    owner_name TEXT NOT NULL,
    node_name TEXT NOT NULL,
    kind TEXT NOT NULL,
    host_path TEXT NOT NULL,
    container_path TEXT NOT NULL,
    size_gb INTEGER NOT NULL,
    state TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (name, node_name),
    FOREIGN KEY (plane_name) REFERENCES planes(name) ON DELETE CASCADE,
    FOREIGN KEY (node_name) REFERENCES nodes(name) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS disk_runtime_state (
    disk_name TEXT NOT NULL,
    plane_name TEXT NOT NULL,
    node_name TEXT NOT NULL,
    image_path TEXT NOT NULL DEFAULT '',
    filesystem_type TEXT NOT NULL DEFAULT '',
    loop_device TEXT NOT NULL DEFAULT '',
    mount_point TEXT NOT NULL DEFAULT '',
    runtime_state TEXT NOT NULL DEFAULT '',
    attached_at TEXT NOT NULL DEFAULT '',
    mounted_at TEXT NOT NULL DEFAULT '',
    last_verified_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    status_message TEXT NOT NULL DEFAULT '',
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (disk_name, node_name),
    FOREIGN KEY (node_name) REFERENCES nodes(name) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS instances (
    name TEXT PRIMARY KEY,
    plane_name TEXT NOT NULL,
    node_name TEXT NOT NULL,
    role TEXT NOT NULL,
    state TEXT NOT NULL,
    image TEXT NOT NULL,
    command TEXT NOT NULL,
    private_disk_name TEXT NOT NULL,
    shared_disk_name TEXT NOT NULL,
    gpu_device TEXT,
    placement_mode TEXT NOT NULL DEFAULT 'manual',
    share_mode TEXT NOT NULL DEFAULT 'exclusive',
    gpu_fraction REAL NOT NULL DEFAULT 0,
    priority INTEGER NOT NULL DEFAULT 100,
    preemptible INTEGER NOT NULL DEFAULT 0,
    memory_cap_mb INTEGER,
    private_disk_size_gb INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (plane_name) REFERENCES planes(name) ON DELETE CASCADE,
    FOREIGN KEY (node_name) REFERENCES nodes(name) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS instance_dependencies (
    instance_name TEXT NOT NULL,
    dependency_name TEXT NOT NULL,
    PRIMARY KEY (instance_name, dependency_name),
    FOREIGN KEY (instance_name) REFERENCES instances(name) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS instance_environment (
    instance_name TEXT NOT NULL,
    env_key TEXT NOT NULL,
    env_value TEXT NOT NULL,
    PRIMARY KEY (instance_name, env_key),
    FOREIGN KEY (instance_name) REFERENCES instances(name) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS instance_labels (
    instance_name TEXT NOT NULL,
    label_key TEXT NOT NULL,
    label_value TEXT NOT NULL,
    PRIMARY KEY (instance_name, label_key),
    FOREIGN KEY (instance_name) REFERENCES instances(name) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS instance_published_ports (
    instance_name TEXT NOT NULL,
    host_ip TEXT NOT NULL,
    host_port INTEGER NOT NULL,
    container_port INTEGER NOT NULL,
    PRIMARY KEY (instance_name, host_ip, host_port, container_port),
    FOREIGN KEY (instance_name) REFERENCES instances(name) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS host_assignments (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    node_name TEXT NOT NULL,
    plane_name TEXT NOT NULL,
    desired_generation INTEGER NOT NULL,
    attempt_count INTEGER NOT NULL DEFAULT 0,
    max_attempts INTEGER NOT NULL DEFAULT 3,
    assignment_type TEXT NOT NULL,
    desired_state_json TEXT NOT NULL,
    artifacts_root TEXT NOT NULL,
    status TEXT NOT NULL,
    status_message TEXT NOT NULL DEFAULT '',
    progress_json TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS node_availability_overrides (
    node_name TEXT PRIMARY KEY,
    availability TEXT NOT NULL,
    status_message TEXT NOT NULL DEFAULT '',
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS rollout_actions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    plane_name TEXT NOT NULL DEFAULT '',
    desired_generation INTEGER NOT NULL,
    step INTEGER NOT NULL,
    worker_name TEXT NOT NULL,
    action TEXT NOT NULL,
    target_node_name TEXT NOT NULL,
    target_gpu_device TEXT NOT NULL,
    victim_worker_names_json TEXT NOT NULL DEFAULT '[]',
    reason TEXT NOT NULL DEFAULT '',
    status TEXT NOT NULL,
    status_message TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS host_observations (
    node_name TEXT PRIMARY KEY,
    plane_name TEXT NOT NULL,
    applied_generation INTEGER,
    last_assignment_id INTEGER,
    status TEXT NOT NULL,
    status_message TEXT NOT NULL DEFAULT '',
    observed_state_json TEXT NOT NULL DEFAULT '',
    runtime_status_json TEXT NOT NULL DEFAULT '',
    instance_runtime_json TEXT NOT NULL DEFAULT '',
    gpu_telemetry_json TEXT NOT NULL DEFAULT '',
    disk_telemetry_json TEXT NOT NULL DEFAULT '',
    network_telemetry_json TEXT NOT NULL DEFAULT '',
    cpu_telemetry_json TEXT NOT NULL DEFAULT '',
    heartbeat_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS event_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    plane_name TEXT NOT NULL DEFAULT '',
    node_name TEXT NOT NULL DEFAULT '',
    worker_name TEXT NOT NULL DEFAULT '',
    assignment_id INTEGER,
    rollout_action_id INTEGER,
    category TEXT NOT NULL,
    event_type TEXT NOT NULL,
    severity TEXT NOT NULL DEFAULT 'info',
    message TEXT NOT NULL DEFAULT '',
    payload_json TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_event_log_created_at
    ON event_log(created_at DESC, id DESC);
CREATE INDEX IF NOT EXISTS idx_event_log_plane_name
    ON event_log(plane_name);
CREATE INDEX IF NOT EXISTS idx_event_log_node_name
    ON event_log(node_name);
CREATE INDEX IF NOT EXISTS idx_event_log_worker_name
    ON event_log(worker_name);
CREATE INDEX IF NOT EXISTS idx_event_log_category
    ON event_log(category);

CREATE TABLE IF NOT EXISTS knowledge_vault_services (
    service_id TEXT PRIMARY KEY,
    node_name TEXT NOT NULL,
    image TEXT NOT NULL,
    endpoint_host TEXT NOT NULL,
    endpoint_port INTEGER NOT NULL,
    desired_state_json TEXT NOT NULL DEFAULT '{}',
    status TEXT NOT NULL DEFAULT 'stopped',
    status_message TEXT NOT NULL DEFAULT '',
    schema_version TEXT NOT NULL DEFAULT '',
    index_epoch TEXT NOT NULL DEFAULT '',
    latest_event_sequence INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS knowledge_replica_merge_schedules (
    plane_id TEXT NOT NULL,
    capsule_id TEXT NOT NULL,
    service_id TEXT NOT NULL,
    cadence TEXT NOT NULL DEFAULT 'daily',
    next_run_at TEXT NOT NULL DEFAULT '',
    last_checkpoint_json TEXT NOT NULL DEFAULT '{}',
    status TEXT NOT NULL DEFAULT 'scheduled',
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (plane_id, capsule_id, service_id)
);

CREATE TABLE IF NOT EXISTS scheduler_plane_runtime (
    plane_name TEXT PRIMARY KEY,
    active_action TEXT NOT NULL DEFAULT '',
    active_worker_name TEXT NOT NULL DEFAULT '',
    phase TEXT NOT NULL DEFAULT '',
    action_generation INTEGER NOT NULL DEFAULT 0,
    stable_samples INTEGER NOT NULL DEFAULT 0,
    rollback_attempt_count INTEGER NOT NULL DEFAULT 0,
    source_node_name TEXT NOT NULL DEFAULT '',
    source_gpu_device TEXT NOT NULL DEFAULT '',
    target_node_name TEXT NOT NULL DEFAULT '',
    target_gpu_device TEXT NOT NULL DEFAULT '',
    previous_state_json TEXT NOT NULL DEFAULT '',
    status_message TEXT NOT NULL DEFAULT '',
    started_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS scheduler_worker_runtime (
    worker_name TEXT PRIMARY KEY,
    plane_name TEXT NOT NULL,
    last_move_at TEXT NOT NULL DEFAULT '',
    last_eviction_at TEXT NOT NULL DEFAULT '',
    last_verified_generation INTEGER,
    last_scheduler_phase TEXT NOT NULL DEFAULT '',
    last_status_message TEXT NOT NULL DEFAULT '',
    manual_intervention_required INTEGER NOT NULL DEFAULT 0,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS scheduler_node_runtime (
    node_name TEXT PRIMARY KEY,
    plane_name TEXT NOT NULL,
    last_move_at TEXT NOT NULL DEFAULT '',
    last_verified_generation INTEGER,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);
)SQL";

}  // namespace

ControllerStore::ControllerStore(std::string db_path) : db_path_(std::move(db_path)) {
  const std::filesystem::path path(db_path_);
  if (path.has_parent_path()) {
    std::filesystem::create_directories(path.parent_path());
  }

  sqlite3* db = nullptr;
  if (sqlite3_open(db_path_.c_str(), &db) != SQLITE_OK) {
    const std::string message = db == nullptr ? "unknown sqlite open error" : sqlite3_errmsg(db);
    if (db != nullptr) {
      sqlite3_close(db);
    }
    throw std::runtime_error("failed to open sqlite db '" + db_path_ + "': " + message);
  }
  if (sqlite3_busy_timeout(db, 10000) != SQLITE_OK) {
    const std::string message = sqlite3_errmsg(db);
    sqlite3_close(db);
    throw std::runtime_error(
        "failed to configure sqlite busy timeout for '" + db_path_ + "': " + message);
  }
  db_ = db;
}

ControllerStore::~ControllerStore() {
  if (db_ != nullptr) {
    sqlite3_close(AsSqlite(db_));
  }
}

void ControllerStore::Initialize() {
  sqlite_store_schema::InitializeSchema(
      AsSqlite(db_),
      kBootstrapSql,
      [&](const std::string& plane_name) {
        return LoadDesiredState(plane_name);
      });
}

void ControllerStore::ReplaceDesiredState(const DesiredState& state, int generation) {
  ReplaceDesiredState(state, generation, 0);
}

void ControllerStore::ReplaceDesiredState(
    const DesiredState& state,
    int generation,
    int rebalance_iteration) {
  DesiredStateRepository(AsSqlite(db_))
      .ReplaceDesiredState(state, generation, rebalance_iteration);
}

void ControllerStore::ReplaceDesiredState(const DesiredState& state) {
  ReplaceDesiredState(state, 1);
}

std::optional<DesiredState> ControllerStore::LoadDesiredState() const {
  return DesiredStateRepository(AsSqlite(db_)).LoadDesiredState();
}

std::optional<DesiredState> ControllerStore::LoadDesiredState(const std::string& plane_name) const {
  return DesiredStateRepository(AsSqlite(db_)).LoadDesiredState(plane_name);
}

std::vector<DesiredState> ControllerStore::LoadDesiredStates() const {
  return DesiredStateRepository(AsSqlite(db_)).LoadDesiredStates();
}

std::optional<int> ControllerStore::LoadDesiredGeneration() const {
  return DesiredStateRepository(AsSqlite(db_)).LoadDesiredGeneration();
}

std::optional<int> ControllerStore::LoadDesiredGeneration(const std::string& plane_name) const {
  return DesiredStateRepository(AsSqlite(db_)).LoadDesiredGeneration(plane_name);
}

std::optional<int> ControllerStore::LoadRebalanceIteration() const {
  return DesiredStateRepository(AsSqlite(db_)).LoadRebalanceIteration();
}

std::optional<int> ControllerStore::LoadRebalanceIteration(
    const std::string& plane_name) const {
  return DesiredStateRepository(AsSqlite(db_)).LoadRebalanceIteration(plane_name);
}

std::vector<PlaneRecord> ControllerStore::LoadPlanes() const {
  return PlaneRepository(AsSqlite(db_)).LoadPlanes();
}

std::optional<PlaneRecord> ControllerStore::LoadPlane(const std::string& plane_name) const {
  return PlaneRepository(AsSqlite(db_)).LoadPlane(plane_name);
}

void ControllerStore::UpsertRegisteredHost(const RegisteredHostRecord& host) {
  RegisteredHostRepository(AsSqlite(db_)).UpsertRegisteredHost(host);
}

std::optional<RegisteredHostRecord> ControllerStore::LoadRegisteredHost(
    const std::string& node_name) const {
  return RegisteredHostRepository(AsSqlite(db_)).LoadRegisteredHost(node_name);
}

std::vector<RegisteredHostRecord> ControllerStore::LoadRegisteredHosts(
    const std::optional<std::string>& node_name) const {
  return RegisteredHostRepository(AsSqlite(db_)).LoadRegisteredHosts(node_name);
}

void ControllerStore::UpsertHostPeerLink(const HostPeerLinkRecord& link) {
  SqliteStatement statement(
      AsSqlite(db_),
      "INSERT INTO host_peer_links("
      "observer_node_name, peer_node_name, peer_endpoint, local_interface, remote_address,"
      "seen_udp, tcp_reachable, rtt_ms, last_seen_at, last_probe_at, updated_at"
      ") VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, CURRENT_TIMESTAMP)"
      " ON CONFLICT(observer_node_name, peer_node_name) DO UPDATE SET"
      " peer_endpoint = excluded.peer_endpoint,"
      " local_interface = excluded.local_interface,"
      " remote_address = excluded.remote_address,"
      " seen_udp = excluded.seen_udp,"
      " tcp_reachable = excluded.tcp_reachable,"
      " rtt_ms = excluded.rtt_ms,"
      " last_seen_at = excluded.last_seen_at,"
      " last_probe_at = excluded.last_probe_at,"
      " updated_at = CURRENT_TIMESTAMP;");
  statement.BindText(1, link.observer_node_name);
  statement.BindText(2, link.peer_node_name);
  statement.BindText(3, link.peer_endpoint);
  statement.BindText(4, link.local_interface);
  statement.BindText(5, link.remote_address);
  statement.BindInt(6, link.seen_udp ? 1 : 0);
  statement.BindInt(7, link.tcp_reachable ? 1 : 0);
  statement.BindInt(8, link.rtt_ms);
  statement.BindText(9, link.last_seen_at);
  statement.BindText(10, link.last_probe_at);
  statement.StepDone();
}

int ControllerStore::DeleteStaleHostPeerLinks(const std::string& cutoff_timestamp) {
  SqliteStatement statement(
      AsSqlite(db_),
      "DELETE FROM host_peer_links "
      "WHERE (last_seen_at = '' OR last_seen_at < ?1) "
      "AND (last_probe_at = '' OR last_probe_at < ?1);");
  statement.BindText(1, cutoff_timestamp);
  statement.StepDone();
  return sqlite3_changes(AsSqlite(db_));
}

std::vector<HostPeerLinkRecord> ControllerStore::LoadHostPeerLinks(
    const std::optional<std::string>& observer_node_name,
    const std::optional<std::string>& peer_node_name) const {
  std::string sql =
      "SELECT observer_node_name, peer_node_name, peer_endpoint, local_interface,"
      " remote_address, seen_udp, tcp_reachable, rtt_ms, last_seen_at, last_probe_at,"
      " updated_at FROM host_peer_links";
  std::vector<std::string> clauses;
  if (observer_node_name.has_value()) {
    clauses.push_back("observer_node_name = ?" + std::to_string(clauses.size() + 1));
  }
  if (peer_node_name.has_value()) {
    clauses.push_back("peer_node_name = ?" + std::to_string(clauses.size() + 1));
  }
  if (!clauses.empty()) {
    sql += " WHERE ";
    for (std::size_t index = 0; index < clauses.size(); ++index) {
      if (index > 0) {
        sql += " AND ";
      }
      sql += clauses[index];
    }
  }
  sql += " ORDER BY observer_node_name ASC, peer_node_name ASC;";

  SqliteStatement statement(AsSqlite(db_), sql);
  int bind_index = 1;
  if (observer_node_name.has_value()) {
    statement.BindText(bind_index++, *observer_node_name);
  }
  if (peer_node_name.has_value()) {
    statement.BindText(bind_index++, *peer_node_name);
  }

  std::vector<HostPeerLinkRecord> links;
  while (statement.StepRow()) {
    HostPeerLinkRecord link;
    link.observer_node_name = ToColumnText(statement.raw(), 0);
    link.peer_node_name = ToColumnText(statement.raw(), 1);
    link.peer_endpoint = ToColumnText(statement.raw(), 2);
    link.local_interface = ToColumnText(statement.raw(), 3);
    link.remote_address = ToColumnText(statement.raw(), 4);
    link.seen_udp = sqlite3_column_int(statement.raw(), 5) != 0;
    link.tcp_reachable = sqlite3_column_int(statement.raw(), 6) != 0;
    link.rtt_ms = sqlite3_column_int(statement.raw(), 7);
    link.last_seen_at = ToColumnText(statement.raw(), 8);
    link.last_probe_at = ToColumnText(statement.raw(), 9);
    link.updated_at = ToColumnText(statement.raw(), 10);
    links.push_back(std::move(link));
  }
  return links;
}

void ControllerStore::InsertFileTransferTicket(const FileTransferTicketRecord& ticket) {
  SqliteStatement statement(
      AsSqlite(db_),
      "INSERT INTO file_transfer_tickets("
      "ticket_id, source_node_name, requester_node_name, source_paths_json, expires_at,"
      "max_chunk_bytes"
      ") VALUES (?1, ?2, ?3, ?4, ?5, ?6);");
  statement.BindText(1, ticket.ticket_id);
  statement.BindText(2, ticket.source_node_name);
  statement.BindText(3, ticket.requester_node_name);
  statement.BindText(4, ticket.source_paths_json);
  statement.BindText(5, ticket.expires_at);
  statement.BindInt(6, static_cast<int>(ticket.max_chunk_bytes));
  statement.StepDone();
}

std::optional<FileTransferTicketRecord> ControllerStore::LoadFileTransferTicket(
    const std::string& ticket_id) const {
  SqliteStatement statement(
      AsSqlite(db_),
      "SELECT ticket_id, source_node_name, requester_node_name, source_paths_json,"
      " expires_at, max_chunk_bytes, created_at, last_validated_at"
      " FROM file_transfer_tickets WHERE ticket_id = ?1;");
  statement.BindText(1, ticket_id);
  if (!statement.StepRow()) {
    return std::nullopt;
  }
  FileTransferTicketRecord ticket;
  ticket.ticket_id = ToColumnText(statement.raw(), 0);
  ticket.source_node_name = ToColumnText(statement.raw(), 1);
  ticket.requester_node_name = ToColumnText(statement.raw(), 2);
  ticket.source_paths_json = ToColumnText(statement.raw(), 3);
  ticket.expires_at = ToColumnText(statement.raw(), 4);
  ticket.max_chunk_bytes = static_cast<std::uintmax_t>(sqlite3_column_int64(statement.raw(), 5));
  ticket.created_at = ToColumnText(statement.raw(), 6);
  ticket.last_validated_at = ToColumnText(statement.raw(), 7);
  return ticket;
}

bool ControllerStore::MarkFileTransferTicketValidated(
    const std::string& ticket_id,
    const std::string& validated_at) {
  SqliteStatement statement(
      AsSqlite(db_),
      "UPDATE file_transfer_tickets SET last_validated_at = ?2 WHERE ticket_id = ?1;");
  statement.BindText(1, ticket_id);
  statement.BindText(2, validated_at);
  statement.StepDone();
  return sqlite3_changes(AsSqlite(db_)) > 0;
}

void ControllerStore::InsertFileUploadTicket(const FileUploadTicketRecord& ticket) {
  SqliteStatement statement(
      AsSqlite(db_),
      "INSERT INTO file_upload_tickets("
      "ticket_id, target_node_name, uploader_node_name, target_relative_path, sha256,"
      "size_bytes, if_missing, expires_at, max_chunk_bytes"
      ") VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9);");
  statement.BindText(1, ticket.ticket_id);
  statement.BindText(2, ticket.target_node_name);
  statement.BindText(3, ticket.uploader_node_name);
  statement.BindText(4, ticket.target_relative_path);
  statement.BindText(5, ticket.sha256);
  statement.BindInt64(6, static_cast<sqlite3_int64>(ticket.size_bytes));
  statement.BindInt(7, ticket.if_missing ? 1 : 0);
  statement.BindText(8, ticket.expires_at);
  statement.BindInt64(9, static_cast<sqlite3_int64>(ticket.max_chunk_bytes));
  statement.StepDone();
}

std::optional<FileUploadTicketRecord> ControllerStore::LoadFileUploadTicket(
    const std::string& ticket_id) const {
  SqliteStatement statement(
      AsSqlite(db_),
      "SELECT ticket_id, target_node_name, uploader_node_name, target_relative_path,"
      " sha256, size_bytes, if_missing, expires_at, max_chunk_bytes, created_at,"
      " last_validated_at"
      " FROM file_upload_tickets WHERE ticket_id = ?1;");
  statement.BindText(1, ticket_id);
  if (!statement.StepRow()) {
    return std::nullopt;
  }
  FileUploadTicketRecord ticket;
  ticket.ticket_id = ToColumnText(statement.raw(), 0);
  ticket.target_node_name = ToColumnText(statement.raw(), 1);
  ticket.uploader_node_name = ToColumnText(statement.raw(), 2);
  ticket.target_relative_path = ToColumnText(statement.raw(), 3);
  ticket.sha256 = ToColumnText(statement.raw(), 4);
  ticket.size_bytes = static_cast<std::uintmax_t>(sqlite3_column_int64(statement.raw(), 5));
  ticket.if_missing = sqlite3_column_int(statement.raw(), 6) != 0;
  ticket.expires_at = ToColumnText(statement.raw(), 7);
  ticket.max_chunk_bytes = static_cast<std::uintmax_t>(sqlite3_column_int64(statement.raw(), 8));
  ticket.created_at = ToColumnText(statement.raw(), 9);
  ticket.last_validated_at = ToColumnText(statement.raw(), 10);
  return ticket;
}

bool ControllerStore::MarkFileUploadTicketValidated(
    const std::string& ticket_id,
    const std::string& validated_at) {
  SqliteStatement statement(
      AsSqlite(db_),
      "UPDATE file_upload_tickets SET last_validated_at = ?2 WHERE ticket_id = ?1;");
  statement.BindText(1, ticket_id);
  statement.BindText(2, validated_at);
  statement.StepDone();
  return sqlite3_changes(AsSqlite(db_)) > 0;
}

int ControllerStore::LoadUserCount() const {
  return AuthRepository(AsSqlite(db_)).LoadUserCount();
}

std::optional<UserRecord> ControllerStore::LoadUserById(int user_id) const {
  return AuthRepository(AsSqlite(db_)).LoadUserById(user_id);
}

std::optional<UserRecord> ControllerStore::LoadUserByUsername(const std::string& username) const {
  return AuthRepository(AsSqlite(db_)).LoadUserByUsername(username);
}

std::vector<UserRecord> ControllerStore::LoadUsers() const {
  return AuthRepository(AsSqlite(db_)).LoadUsers();
}

UserRecord ControllerStore::CreateBootstrapAdmin(
    const std::string& username,
    const std::string& password_hash) {
  return AuthRepository(AsSqlite(db_)).CreateBootstrapAdmin(username, password_hash);
}

UserRecord ControllerStore::CreateInvitedUser(
    const std::string& invite_token,
    const std::string& username,
    const std::string& password_hash) {
  return AuthRepository(AsSqlite(db_)).CreateInvitedUser(
      invite_token, username, password_hash);
}

void ControllerStore::UpdateUserLastLoginAt(int user_id, const std::string& last_login_at) {
  AuthRepository(AsSqlite(db_)).UpdateUserLastLoginAt(user_id, last_login_at);
}

void ControllerStore::InsertWebAuthnCredential(const WebAuthnCredentialRecord& credential) {
  AuthRepository(AsSqlite(db_)).InsertWebAuthnCredential(credential);
}

void ControllerStore::UpdateWebAuthnCredentialCounter(
    const std::string& credential_id,
    std::uint32_t counter,
    const std::string& last_used_at) {
  AuthRepository(AsSqlite(db_))
      .UpdateWebAuthnCredentialCounter(credential_id, counter, last_used_at);
}

std::vector<WebAuthnCredentialRecord> ControllerStore::LoadWebAuthnCredentialsForUser(int user_id) const {
  return AuthRepository(AsSqlite(db_)).LoadWebAuthnCredentialsForUser(user_id);
}

std::optional<WebAuthnCredentialRecord> ControllerStore::LoadWebAuthnCredentialById(
    const std::string& credential_id) const {
  return AuthRepository(AsSqlite(db_)).LoadWebAuthnCredentialById(credential_id);
}

RegistrationInviteRecord ControllerStore::CreateRegistrationInvite(
    int created_by_user_id,
    const std::string& token,
    const std::string& expires_at) {
  return AuthRepository(AsSqlite(db_))
      .CreateRegistrationInvite(created_by_user_id, token, expires_at);
}

std::optional<RegistrationInviteRecord> ControllerStore::LoadRegistrationInviteByToken(
    const std::string& token) const {
  return AuthRepository(AsSqlite(db_)).LoadRegistrationInviteByToken(token);
}

std::vector<RegistrationInviteRecord> ControllerStore::LoadActiveRegistrationInvites() const {
  return AuthRepository(AsSqlite(db_)).LoadActiveRegistrationInvites();
}

bool ControllerStore::MarkRegistrationInviteUsed(
    const std::string& token,
    int used_by_user_id,
    const std::string& used_at) {
  return AuthRepository(AsSqlite(db_))
      .MarkRegistrationInviteUsed(token, used_by_user_id, used_at);
}

bool ControllerStore::RevokeRegistrationInvite(
    int invite_id,
    const std::string& revoked_at) {
  return AuthRepository(AsSqlite(db_)).RevokeRegistrationInvite(invite_id, revoked_at);
}

void ControllerStore::InsertUserSshKey(const UserSshKeyRecord& ssh_key) {
  AuthRepository(AsSqlite(db_)).InsertUserSshKey(ssh_key);
}

std::vector<UserSshKeyRecord> ControllerStore::LoadActiveUserSshKeys(int user_id) const {
  return AuthRepository(AsSqlite(db_)).LoadActiveUserSshKeys(user_id);
}

std::optional<UserSshKeyRecord> ControllerStore::LoadActiveUserSshKeyByFingerprint(
    int user_id,
    const std::string& fingerprint) const {
  return AuthRepository(AsSqlite(db_))
      .LoadActiveUserSshKeyByFingerprint(user_id, fingerprint);
}

std::optional<UserSshKeyRecord> ControllerStore::LoadActiveUserSshKeyById(int ssh_key_id) const {
  return AuthRepository(AsSqlite(db_)).LoadActiveUserSshKeyById(ssh_key_id);
}

bool ControllerStore::RevokeUserSshKey(
    int ssh_key_id,
    const std::string& revoked_at) {
  return AuthRepository(AsSqlite(db_)).RevokeUserSshKey(ssh_key_id, revoked_at);
}

void ControllerStore::TouchUserSshKey(
    int ssh_key_id,
    const std::string& last_used_at) {
  AuthRepository(AsSqlite(db_)).TouchUserSshKey(ssh_key_id, last_used_at);
}

void ControllerStore::InsertAuthSession(const AuthSessionRecord& session) {
  AuthRepository(AsSqlite(db_)).InsertAuthSession(session);
}

std::optional<AuthSessionRecord> ControllerStore::LoadActiveAuthSession(
    const std::string& token,
    const std::optional<std::string>& session_kind,
    const std::optional<std::string>& plane_name) const {
  return AuthRepository(AsSqlite(db_))
      .LoadActiveAuthSession(token, session_kind, plane_name);
}

bool ControllerStore::RevokeAuthSession(
    const std::string& token,
    const std::string& revoked_at) {
  return AuthRepository(AsSqlite(db_)).RevokeAuthSession(token, revoked_at);
}

bool ControllerStore::TouchAuthSession(
    const std::string& token,
    const std::string& last_used_at) {
  return AuthRepository(AsSqlite(db_)).TouchAuthSession(token, last_used_at);
}

void ControllerStore::UpsertModelLibraryDownloadJob(
    const ModelLibraryDownloadJobRecord& job) {
  ModelLibraryRepository(AsSqlite(db_)).UpsertModelLibraryDownloadJob(job);
}

std::optional<ModelLibraryDownloadJobRecord> ControllerStore::LoadModelLibraryDownloadJob(
    const std::string& job_id) const {
  return ModelLibraryRepository(AsSqlite(db_))
      .LoadModelLibraryDownloadJob(job_id);
}

std::vector<ModelLibraryDownloadJobRecord> ControllerStore::LoadModelLibraryDownloadJobs(
    const std::optional<std::string>& status) const {
  return ModelLibraryRepository(AsSqlite(db_))
      .LoadModelLibraryDownloadJobs(status);
}

void ControllerStore::UpsertSkillsFactorySkill(const SkillsFactorySkillRecord& skill) {
  SkillsFactoryRepository(AsSqlite(db_)).UpsertSkillsFactorySkill(skill);
}

std::optional<SkillsFactorySkillRecord> ControllerStore::LoadSkillsFactorySkill(
    const std::string& skill_id) const {
  return SkillsFactoryRepository(AsSqlite(db_)).LoadSkillsFactorySkill(skill_id);
}

std::vector<SkillsFactorySkillRecord> ControllerStore::LoadSkillsFactorySkills() const {
  return SkillsFactoryRepository(AsSqlite(db_)).LoadSkillsFactorySkills();
}

bool ControllerStore::DeleteSkillsFactorySkill(const std::string& skill_id) {
  return SkillsFactoryRepository(AsSqlite(db_)).DeleteSkillsFactorySkill(skill_id);
}

void ControllerStore::UpsertSkillsFactoryGroup(const SkillsFactoryGroupRecord& group) {
  SkillsFactoryRepository(AsSqlite(db_)).UpsertSkillsFactoryGroup(group);
}

std::vector<SkillsFactoryGroupRecord> ControllerStore::LoadSkillsFactoryGroups() const {
  return SkillsFactoryRepository(AsSqlite(db_)).LoadSkillsFactoryGroups();
}

bool ControllerStore::DeleteSkillsFactoryGroup(const std::string& path) {
  return SkillsFactoryRepository(AsSqlite(db_)).DeleteSkillsFactoryGroup(path);
}

void ControllerStore::UpsertPlaneSkillBinding(const PlaneSkillBindingRecord& binding) {
  SkillsFactoryRepository(AsSqlite(db_)).UpsertPlaneSkillBinding(binding);
}

std::optional<PlaneSkillBindingRecord> ControllerStore::LoadPlaneSkillBinding(
    const std::string& plane_name,
    const std::string& skill_id) const {
  return SkillsFactoryRepository(AsSqlite(db_)).LoadPlaneSkillBinding(plane_name, skill_id);
}

std::vector<PlaneSkillBindingRecord> ControllerStore::LoadPlaneSkillBindings(
    const std::optional<std::string>& plane_name,
    const std::optional<std::string>& skill_id) const {
  return SkillsFactoryRepository(AsSqlite(db_)).LoadPlaneSkillBindings(plane_name, skill_id);
}

bool ControllerStore::DeletePlaneSkillBinding(
    const std::string& plane_name,
    const std::string& skill_id) {
  return SkillsFactoryRepository(AsSqlite(db_)).DeletePlaneSkillBinding(plane_name, skill_id);
}

int ControllerStore::DeletePlaneSkillBindingsForSkill(const std::string& skill_id) {
  return SkillsFactoryRepository(AsSqlite(db_)).DeletePlaneSkillBindingsForSkill(skill_id);
}

void ControllerStore::UpsertInteractionSession(const InteractionSessionRecord& session) {
  InteractionRepository(AsSqlite(db_)).UpsertInteractionSession(session);
}

bool ControllerStore::UpdateInteractionSessionVersioned(
    const InteractionSessionRecord& session,
    int expected_version) {
  return InteractionRepository(AsSqlite(db_))
      .UpdateInteractionSessionVersioned(session, expected_version);
}

std::optional<InteractionSessionRecord> ControllerStore::LoadInteractionSessionForOwner(
    const std::string& plane_name,
    const std::string& session_id,
    const std::string& owner_kind,
    const std::optional<int>& owner_user_id) const {
  return InteractionRepository(AsSqlite(db_))
      .LoadInteractionSessionForOwner(plane_name, session_id, owner_kind, owner_user_id);
}

std::optional<InteractionSessionRecord>
ControllerStore::LoadInteractionSessionForOwnerAnyPlane(
    const std::string& session_id,
    const std::string& owner_kind,
    const std::optional<int>& owner_user_id) const {
  return InteractionRepository(AsSqlite(db_))
      .LoadInteractionSessionForOwnerAnyPlane(session_id, owner_kind, owner_user_id);
}

std::vector<InteractionSessionRecord> ControllerStore::LoadInteractionSessionsForUser(
    const std::string& plane_name,
    int user_id) const {
  return InteractionRepository(AsSqlite(db_))
      .LoadInteractionSessionsForUser(plane_name, user_id);
}

std::vector<InteractionSessionRecord>
ControllerStore::LoadArchiveEligibleInteractionSessions(
    const std::string& cutoff_timestamp,
    int limit) const {
  return InteractionRepository(AsSqlite(db_))
      .LoadArchiveEligibleInteractionSessions(cutoff_timestamp, limit);
}

void ControllerStore::ReplaceInteractionMessages(
    const std::string& session_id,
    const std::vector<InteractionMessageRecord>& messages) {
  InteractionRepository(AsSqlite(db_)).ReplaceInteractionMessages(session_id, messages);
}

std::vector<InteractionMessageRecord> ControllerStore::LoadInteractionMessages(
    const std::string& session_id) const {
  return InteractionRepository(AsSqlite(db_)).LoadInteractionMessages(session_id);
}

void ControllerStore::ReplaceInteractionSummaries(
    const std::string& session_id,
    const std::vector<InteractionSummaryRecord>& summaries) {
  InteractionRepository(AsSqlite(db_)).ReplaceInteractionSummaries(session_id, summaries);
}

std::vector<InteractionSummaryRecord> ControllerStore::LoadInteractionSummaries(
    const std::string& session_id) const {
  return InteractionRepository(AsSqlite(db_)).LoadInteractionSummaries(session_id);
}

void ControllerStore::UpsertInteractionArchive(const InteractionArchiveRecord& archive) {
  InteractionRepository(AsSqlite(db_)).UpsertInteractionArchive(archive);
}

std::optional<InteractionArchiveRecord> ControllerStore::LoadInteractionArchiveForOwner(
    const std::string& plane_name,
    const std::string& session_id,
    const std::string& owner_kind,
    const std::optional<int>& owner_user_id) const {
  return InteractionRepository(AsSqlite(db_))
      .LoadInteractionArchiveForOwner(plane_name, session_id, owner_kind, owner_user_id);
}

bool ControllerStore::DeleteInteractionSessionForOwner(
    const std::string& plane_name,
    const std::string& session_id,
    const std::string& owner_kind,
    const std::optional<int>& owner_user_id) {
  return InteractionRepository(AsSqlite(db_))
      .DeleteInteractionSessionForOwner(plane_name, session_id, owner_kind, owner_user_id);
}

std::optional<std::string> ControllerStore::LoadControllerSetting(
    const std::string& setting_key) const {
  return ControllerSettingsRepository(AsSqlite(db_)).LoadSetting(setting_key);
}

void ControllerStore::UpsertControllerSetting(
    const std::string& setting_key,
    const std::string& setting_value) {
  ControllerSettingsRepository(AsSqlite(db_)).UpsertSetting(setting_key, setting_value);
}

bool ControllerStore::DeleteControllerSetting(const std::string& setting_key) {
  return ControllerSettingsRepository(AsSqlite(db_)).DeleteSetting(setting_key);
}

bool ControllerStore::DeleteModelLibraryDownloadJob(const std::string& job_id) {
  return ModelLibraryRepository(AsSqlite(db_))
      .DeleteModelLibraryDownloadJob(job_id);
}

bool ControllerStore::UpdatePlaneState(
    const std::string& plane_name,
    const std::string& state) {
  return PlaneRepository(AsSqlite(db_)).UpdatePlaneState(plane_name, state);
}

bool ControllerStore::UpdatePlaneAppliedGeneration(
    const std::string& plane_name,
    int applied_generation) {
  return PlaneRepository(AsSqlite(db_))
      .UpdatePlaneAppliedGeneration(plane_name, applied_generation);
}

bool ControllerStore::UpdatePlaneArtifactsRoot(
    const std::string& plane_name,
    const std::string& artifacts_root) {
  return PlaneRepository(AsSqlite(db_)).UpdatePlaneArtifactsRoot(plane_name, artifacts_root);
}

void ControllerStore::DeletePlane(const std::string& plane_name) {
  PlaneRepository(AsSqlite(db_)).DeletePlane(plane_name);
}

int ControllerStore::SupersedeHostAssignmentsForPlane(
    const std::string& plane_name,
    const std::string& status_message) {
  return AssignmentRepository(AsSqlite(db_))
      .SupersedeHostAssignmentsForPlane(plane_name, status_message);
}

void ControllerStore::UpsertNodeAvailabilityOverride(
    const NodeAvailabilityOverride& availability_override) {
  NodeAvailabilityRepository(AsSqlite(db_))
      .UpsertNodeAvailabilityOverride(availability_override);
}

std::optional<NodeAvailabilityOverride> ControllerStore::LoadNodeAvailabilityOverride(
    const std::string& node_name) const {
  return NodeAvailabilityRepository(AsSqlite(db_))
      .LoadNodeAvailabilityOverride(node_name);
}

std::vector<NodeAvailabilityOverride> ControllerStore::LoadNodeAvailabilityOverrides(
    const std::optional<std::string>& node_name) const {
  return NodeAvailabilityRepository(AsSqlite(db_))
      .LoadNodeAvailabilityOverrides(node_name);
}

void ControllerStore::UpsertDiskRuntimeState(const DiskRuntimeState& runtime_state) {
  DiskRuntimeRepository(AsSqlite(db_)).UpsertDiskRuntimeState(runtime_state);
}

std::optional<DiskRuntimeState> ControllerStore::LoadDiskRuntimeState(
    const std::string& disk_name,
    const std::string& node_name) const {
  return DiskRuntimeRepository(AsSqlite(db_)).LoadDiskRuntimeState(disk_name, node_name);
}

std::vector<DiskRuntimeState> ControllerStore::LoadDiskRuntimeStates(
    const std::optional<std::string>& plane_name,
    const std::optional<std::string>& node_name) const {
  return DiskRuntimeRepository(AsSqlite(db_)).LoadDiskRuntimeStates(plane_name, node_name);
}

void ControllerStore::ReplaceRolloutActions(
    const std::string& plane_name,
    int desired_generation,
    const std::vector<SchedulerRolloutAction>& actions) {
  SchedulerRepository(AsSqlite(db_)).ReplaceRolloutActions(plane_name, desired_generation, actions);
}

std::vector<RolloutActionRecord> ControllerStore::LoadRolloutActions(
    const std::optional<std::string>& plane_name,
    const std::optional<std::string>& target_node_name,
    const std::optional<RolloutActionStatus>& status) const {
  return SchedulerRepository(AsSqlite(db_)).LoadRolloutActions(
      plane_name, target_node_name, status);
}

bool ControllerStore::UpdateRolloutActionStatus(
    int action_id,
    RolloutActionStatus status,
    const std::string& status_message) {
  return SchedulerRepository(AsSqlite(db_))
      .UpdateRolloutActionStatus(action_id, status, status_message);
}

void ControllerStore::UpsertHostObservation(const HostObservation& observation) {
  ObservationRepository(AsSqlite(db_)).UpsertHostObservation(observation);
}

std::optional<HostObservation> ControllerStore::LoadHostObservation(
    const std::string& node_name) const {
  return ObservationRepository(AsSqlite(db_)).LoadHostObservation(node_name);
}

std::vector<HostObservation> ControllerStore::LoadHostObservations(
    const std::optional<std::string>& node_name,
    const std::optional<std::string>& plane_name) const {
  return ObservationRepository(AsSqlite(db_)).LoadHostObservations(node_name, plane_name);
}

void ControllerStore::AppendEvent(const EventRecord& event) {
  EventRepository(AsSqlite(db_)).AppendEvent(event);
}

std::vector<EventRecord> ControllerStore::LoadEvents(
    const std::optional<std::string>& plane_name,
    const std::optional<std::string>& node_name,
    const std::optional<std::string>& worker_name,
    const std::optional<std::string>& category,
    int limit,
    const std::optional<int>& since_id,
    bool ascending) const {
  return EventRepository(AsSqlite(db_)).LoadEvents(
      plane_name,
      node_name,
      worker_name,
      category,
      limit,
      since_id,
      ascending);
}

void ControllerStore::UpsertSchedulerPlaneRuntime(const SchedulerPlaneRuntime& runtime) {
  SchedulerRepository(AsSqlite(db_)).UpsertSchedulerPlaneRuntime(runtime);
}

std::optional<SchedulerPlaneRuntime> ControllerStore::LoadSchedulerPlaneRuntime(
    const std::string& plane_name) const {
  return SchedulerRepository(AsSqlite(db_)).LoadSchedulerPlaneRuntime(plane_name);
}

void ControllerStore::ClearSchedulerPlaneRuntime(const std::string& plane_name) {
  SchedulerRepository(AsSqlite(db_)).ClearSchedulerPlaneRuntime(plane_name);
}

void ControllerStore::UpsertSchedulerWorkerRuntime(const SchedulerWorkerRuntime& runtime) {
  SchedulerRepository(AsSqlite(db_)).UpsertSchedulerWorkerRuntime(runtime);
}

std::optional<SchedulerWorkerRuntime> ControllerStore::LoadSchedulerWorkerRuntime(
    const std::string& worker_name) const {
  return SchedulerRepository(AsSqlite(db_)).LoadSchedulerWorkerRuntime(worker_name);
}

std::vector<SchedulerWorkerRuntime> ControllerStore::LoadSchedulerWorkerRuntimes(
    const std::optional<std::string>& plane_name) const {
  return SchedulerRepository(AsSqlite(db_)).LoadSchedulerWorkerRuntimes(plane_name);
}

void ControllerStore::UpsertSchedulerNodeRuntime(const SchedulerNodeRuntime& runtime) {
  SchedulerRepository(AsSqlite(db_)).UpsertSchedulerNodeRuntime(runtime);
}

std::optional<SchedulerNodeRuntime> ControllerStore::LoadSchedulerNodeRuntime(
    const std::string& node_name) const {
  return SchedulerRepository(AsSqlite(db_)).LoadSchedulerNodeRuntime(node_name);
}

std::vector<SchedulerNodeRuntime> ControllerStore::LoadSchedulerNodeRuntimes(
    const std::optional<std::string>& plane_name) const {
  return SchedulerRepository(AsSqlite(db_)).LoadSchedulerNodeRuntimes(plane_name);
}

void ControllerStore::ReplaceHostAssignments(const std::vector<HostAssignment>& assignments) {
  AssignmentRepository(AsSqlite(db_)).ReplaceHostAssignments(assignments);
}

void ControllerStore::EnqueueHostAssignments(
    const std::vector<HostAssignment>& assignments,
    const std::string& supersede_reason) {
  AssignmentRepository(AsSqlite(db_)).EnqueueHostAssignments(assignments, supersede_reason);
}

std::optional<HostAssignment> ControllerStore::LoadHostAssignment(int assignment_id) const {
  return AssignmentRepository(AsSqlite(db_)).LoadHostAssignment(assignment_id);
}

std::vector<HostAssignment> ControllerStore::LoadHostAssignments(
    const std::optional<std::string>& node_name,
    const std::optional<HostAssignmentStatus>& status,
    const std::optional<std::string>& plane_name) const {
  return AssignmentRepository(AsSqlite(db_)).LoadHostAssignments(node_name, status, plane_name);
}

std::optional<HostAssignment> ControllerStore::ClaimNextHostAssignment(
    const std::string& node_name) {
  return AssignmentRepository(AsSqlite(db_)).ClaimNextHostAssignment(node_name);
}

bool ControllerStore::UpdateHostAssignmentProgress(
    int assignment_id,
    const std::string& progress_json) {
  return AssignmentRepository(AsSqlite(db_)).UpdateHostAssignmentProgress(
      assignment_id, progress_json);
}

bool ControllerStore::TransitionClaimedHostAssignment(
    int assignment_id,
    HostAssignmentStatus status,
    const std::string& status_message) {
  return AssignmentRepository(AsSqlite(db_)).TransitionClaimedHostAssignment(
      assignment_id, status, status_message);
}

bool ControllerStore::RetryFailedHostAssignment(
    int assignment_id,
    const std::string& status_message) {
  return AssignmentRepository(AsSqlite(db_)).RetryFailedHostAssignment(
      assignment_id, status_message);
}

void ControllerStore::UpdateHostAssignmentStatus(
    int assignment_id,
    HostAssignmentStatus status,
    const std::string& status_message) {
  AssignmentRepository(AsSqlite(db_))
      .UpdateHostAssignmentStatus(assignment_id, status, status_message);
}

const std::string& ControllerStore::db_path() const {
  return db_path_;
}

}  // namespace naim
