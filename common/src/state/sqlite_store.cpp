#include "comet/state/auth_repository.h"
#include "comet/state/assignment_repository.h"
#include "comet/state/desired_state_repository.h"
#include "comet/state/desired_state_sqlite_codec.h"
#include "comet/state/disk_runtime_repository.h"
#include "comet/state/event_repository.h"
#include "comet/state/node_availability_repository.h"
#include "comet/state/observation_repository.h"
#include "comet/state/plane_repository.h"
#include "comet/state/registered_host_repository.h"
#include "comet/state/scheduler_repository.h"
#include "comet/state/sqlite_store.h"
#include "comet/state/sqlite_store_schema.h"
#include "comet/state/sqlite_store_support.h"
#include "comet/state/sqlite_statement.h"
#include "comet/state/state_json.h"

#include <array>
#include <filesystem>
#include <map>
#include <nlohmann/json.hpp>
#include <set>
#include <stdexcept>
#include <string>
#include <utility>

#include <sqlite3.h>

namespace comet {

namespace {

using Statement = SqliteStatement;
using sqlite_store_support::AsSqlite;
using sqlite_store_support::AvailabilityOverrideFromStatement;
using sqlite_store_support::EnsureColumn;
using sqlite_store_support::Exec;
using sqlite_store_support::ToColumnText;
using sqlite_store_support::ToOptionalColumnInt;
using nlohmann::json;

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
    status TEXT NOT NULL DEFAULT 'queued',
    model_id TEXT NOT NULL DEFAULT '',
    target_root TEXT NOT NULL DEFAULT '',
    target_subdir TEXT NOT NULL DEFAULT '',
    source_urls_json TEXT NOT NULL DEFAULT '[]',
    target_paths_json TEXT NOT NULL DEFAULT '[]',
    current_item TEXT NOT NULL DEFAULT '',
    bytes_total INTEGER,
    bytes_done INTEGER NOT NULL DEFAULT 0,
    part_count INTEGER NOT NULL DEFAULT 0,
    error_message TEXT NOT NULL DEFAULT '',
    hidden INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
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

std::string SerializeStringArray(const std::vector<std::string>& values) {
  return json(values).dump();
}

std::vector<std::string> ParseStringArray(const std::string& payload) {
  if (payload.empty()) {
    return {};
  }
  try {
    const json parsed = json::parse(payload);
    if (!parsed.is_array()) {
      return {};
    }
    return parsed.get<std::vector<std::string>>();
  } catch (...) {
    return {};
  }
}

ModelLibraryDownloadJobRecord ModelLibraryDownloadJobFromStatement(sqlite3_stmt* statement) {
  ModelLibraryDownloadJobRecord job;
  job.id = ToColumnText(statement, 0);
  job.status = ToColumnText(statement, 1);
  job.model_id = ToColumnText(statement, 2);
  job.target_root = ToColumnText(statement, 3);
  job.target_subdir = ToColumnText(statement, 4);
  job.source_urls = ParseStringArray(ToColumnText(statement, 5));
  job.target_paths = ParseStringArray(ToColumnText(statement, 6));
  job.current_item = ToColumnText(statement, 7);
  if (sqlite3_column_type(statement, 8) != SQLITE_NULL) {
    job.bytes_total =
        static_cast<std::uintmax_t>(sqlite3_column_int64(statement, 8));
  }
  job.bytes_done = static_cast<std::uintmax_t>(sqlite3_column_int64(statement, 9));
  job.part_count = sqlite3_column_int(statement, 10);
  job.error_message = ToColumnText(statement, 11);
  job.hidden = sqlite3_column_int(statement, 12) != 0;
  job.created_at = ToColumnText(statement, 13);
  job.updated_at = ToColumnText(statement, 14);
  return job;
}

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
  Statement statement(
      AsSqlite(db_),
      "INSERT INTO model_library_download_jobs("
      "id, status, model_id, target_root, target_subdir, source_urls_json, "
      "target_paths_json, current_item, bytes_total, bytes_done, part_count, "
      "error_message, hidden, created_at, updated_at) "
      "VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15) "
      "ON CONFLICT(id) DO UPDATE SET "
      "status = excluded.status, "
      "model_id = excluded.model_id, "
      "target_root = excluded.target_root, "
      "target_subdir = excluded.target_subdir, "
      "source_urls_json = excluded.source_urls_json, "
      "target_paths_json = excluded.target_paths_json, "
      "current_item = excluded.current_item, "
      "bytes_total = excluded.bytes_total, "
      "bytes_done = excluded.bytes_done, "
      "part_count = excluded.part_count, "
      "error_message = excluded.error_message, "
      "hidden = excluded.hidden, "
      "created_at = excluded.created_at, "
      "updated_at = excluded.updated_at;");
  statement.BindText(1, job.id);
  statement.BindText(2, job.status);
  statement.BindText(3, job.model_id);
  statement.BindText(4, job.target_root);
  statement.BindText(5, job.target_subdir);
  statement.BindText(6, SerializeStringArray(job.source_urls));
  statement.BindText(7, SerializeStringArray(job.target_paths));
  statement.BindText(8, job.current_item);
  statement.BindOptionalInt64(
      9,
      job.bytes_total.has_value()
          ? std::optional<std::int64_t>(static_cast<std::int64_t>(*job.bytes_total))
          : std::nullopt);
  statement.BindInt64(10, static_cast<std::int64_t>(job.bytes_done));
  statement.BindInt(11, job.part_count);
  statement.BindText(12, job.error_message);
  statement.BindInt(13, job.hidden ? 1 : 0);
  statement.BindText(14, job.created_at);
  statement.BindText(15, job.updated_at);
  statement.StepDone();
}

std::optional<ModelLibraryDownloadJobRecord> ControllerStore::LoadModelLibraryDownloadJob(
    const std::string& job_id) const {
  Statement statement(
      AsSqlite(db_),
      "SELECT id, status, model_id, target_root, target_subdir, source_urls_json, "
      "target_paths_json, current_item, bytes_total, bytes_done, part_count, "
      "error_message, hidden, created_at, updated_at "
      "FROM model_library_download_jobs WHERE id = ?1;");
  statement.BindText(1, job_id);
  if (!statement.StepRow()) {
    return std::nullopt;
  }
  return ModelLibraryDownloadJobFromStatement(statement.raw());
}

std::vector<ModelLibraryDownloadJobRecord> ControllerStore::LoadModelLibraryDownloadJobs(
    const std::optional<std::string>& status) const {
  std::string sql =
      "SELECT id, status, model_id, target_root, target_subdir, source_urls_json, "
      "target_paths_json, current_item, bytes_total, bytes_done, part_count, "
      "error_message, hidden, created_at, updated_at "
      "FROM model_library_download_jobs";
  if (status.has_value()) {
    sql += " WHERE status = ?1";
  }
  sql += " ORDER BY created_at DESC, id DESC;";
  Statement statement(AsSqlite(db_), sql);
  if (status.has_value()) {
    statement.BindText(1, *status);
  }
  std::vector<ModelLibraryDownloadJobRecord> jobs;
  while (statement.StepRow()) {
    jobs.push_back(ModelLibraryDownloadJobFromStatement(statement.raw()));
  }
  return jobs;
}

bool ControllerStore::DeleteModelLibraryDownloadJob(const std::string& job_id) {
  Statement statement(
      AsSqlite(db_),
      "DELETE FROM model_library_download_jobs WHERE id = ?1;");
  statement.BindText(1, job_id);
  statement.StepDone();
  return sqlite3_changes(AsSqlite(db_)) > 0;
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

}  // namespace comet
