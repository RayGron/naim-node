#include "comet/sqlite_store.h"

#include <filesystem>
#include <map>
#include <stdexcept>
#include <string>
#include <utility>

#include <nlohmann/json.hpp>
#include <sqlite3.h>

namespace comet {

namespace {

using nlohmann::json;

constexpr const char* kBootstrapSql = R"SQL(
PRAGMA journal_mode=WAL;
PRAGMA foreign_keys=ON;

CREATE TABLE IF NOT EXISTS planes (
    name TEXT PRIMARY KEY,
    shared_disk_name TEXT NOT NULL,
    control_root TEXT NOT NULL DEFAULT '',
    inference_config_json TEXT NOT NULL DEFAULT '',
    gateway_config_json TEXT NOT NULL DEFAULT '',
    runtime_gpu_nodes_json TEXT NOT NULL DEFAULT '',
    generation INTEGER NOT NULL DEFAULT 1,
    rebalance_iteration INTEGER NOT NULL DEFAULT 0,
    state TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS nodes (
    name TEXT PRIMARY KEY,
    platform TEXT NOT NULL,
    state TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
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
    heartbeat_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
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

void ThrowSqliteError(sqlite3* db, const std::string& action) {
  throw std::runtime_error(action + ": " + sqlite3_errmsg(db));
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

class Statement {
 public:
  Statement(sqlite3* db, const std::string& sql) : db_(db) {
    if (sqlite3_prepare_v2(db_, sql.c_str(), -1, &statement_, nullptr) != SQLITE_OK) {
      ThrowSqliteError(db_, "sqlite prepare failed");
    }
  }

  ~Statement() {
    if (statement_ != nullptr) {
      sqlite3_finalize(statement_);
    }
  }

  Statement(const Statement&) = delete;
  Statement& operator=(const Statement&) = delete;

  void BindText(int index, const std::string& value) {
    if (sqlite3_bind_text(statement_, index, value.c_str(), -1, SQLITE_TRANSIENT) != SQLITE_OK) {
      ThrowSqliteError(db_, "sqlite bind text failed");
    }
  }

  void BindInt(int index, int value) {
    if (sqlite3_bind_int(statement_, index, value) != SQLITE_OK) {
      ThrowSqliteError(db_, "sqlite bind int failed");
    }
  }

  void BindDouble(int index, double value) {
    if (sqlite3_bind_double(statement_, index, value) != SQLITE_OK) {
      ThrowSqliteError(db_, "sqlite bind double failed");
    }
  }

  void BindOptionalInt(int index, const std::optional<int>& value) {
    const int rc =
        value.has_value() ? sqlite3_bind_int(statement_, index, *value)
                          : sqlite3_bind_null(statement_, index);
    if (rc != SQLITE_OK) {
      ThrowSqliteError(db_, "sqlite bind optional int failed");
    }
  }

  void BindOptionalText(int index, const std::optional<std::string>& value) {
    const int rc = value.has_value()
                       ? sqlite3_bind_text(statement_, index, value->c_str(), -1, SQLITE_TRANSIENT)
                       : sqlite3_bind_null(statement_, index);
    if (rc != SQLITE_OK) {
      ThrowSqliteError(db_, "sqlite bind optional text failed");
    }
  }

  bool StepRow() {
    const int rc = sqlite3_step(statement_);
    if (rc == SQLITE_ROW) {
      return true;
    }
    if (rc == SQLITE_DONE) {
      return false;
    }
    ThrowSqliteError(db_, "sqlite step failed");
    return false;
  }

  void StepDone() {
    const int rc = sqlite3_step(statement_);
    if (rc != SQLITE_DONE) {
      ThrowSqliteError(db_, "sqlite step done failed");
    }
  }

  sqlite3_stmt* raw() const {
    return statement_;
  }

 private:
 sqlite3* db_ = nullptr;
  sqlite3_stmt* statement_ = nullptr;
};

bool TableHasColumn(sqlite3* db, const std::string& table_name, const std::string& column_name) {
  Statement statement(db, "PRAGMA table_info(" + table_name + ");");
  while (statement.StepRow()) {
    if (ToColumnText(statement.raw(), 1) == column_name) {
      return true;
    }
  }
  return false;
}

void EnsureColumn(
    sqlite3* db,
    const std::string& table_name,
    const std::string& column_name,
    const std::string& definition_sql) {
  if (TableHasColumn(db, table_name, column_name)) {
    return;
  }
  Exec(db, "ALTER TABLE " + table_name + " ADD COLUMN " + definition_sql + ";");
}

std::string SerializeInferenceSettings(const InferenceRuntimeSettings& settings) {
  return json{
      {"primary_infer_node", settings.primary_infer_node},
      {"net_if", settings.net_if},
      {"models_root", settings.models_root},
      {"gguf_cache_dir", settings.gguf_cache_dir},
      {"infer_log_dir", settings.infer_log_dir},
      {"llama_port", settings.llama_port},
      {"llama_ctx_size", settings.llama_ctx_size},
      {"llama_threads", settings.llama_threads},
      {"llama_gpu_layers", settings.llama_gpu_layers},
      {"inference_healthcheck_retries", settings.inference_healthcheck_retries},
      {"inference_healthcheck_interval_sec", settings.inference_healthcheck_interval_sec},
  }
      .dump();
}

InferenceRuntimeSettings DeserializeInferenceSettings(const std::string& json_text) {
  InferenceRuntimeSettings settings;
  if (json_text.empty()) {
    return settings;
  }

  const json value = json::parse(json_text);
  settings.primary_infer_node =
      value.value("primary_infer_node", settings.primary_infer_node);
  settings.net_if = value.value("net_if", settings.net_if);
  settings.models_root = value.value("models_root", settings.models_root);
  settings.gguf_cache_dir = value.value("gguf_cache_dir", settings.gguf_cache_dir);
  settings.infer_log_dir = value.value("infer_log_dir", settings.infer_log_dir);
  settings.llama_port = value.value("llama_port", settings.llama_port);
  settings.llama_ctx_size = value.value("llama_ctx_size", settings.llama_ctx_size);
  settings.llama_threads = value.value("llama_threads", settings.llama_threads);
  settings.llama_gpu_layers = value.value("llama_gpu_layers", settings.llama_gpu_layers);
  settings.inference_healthcheck_retries = value.value(
      "inference_healthcheck_retries", settings.inference_healthcheck_retries);
  settings.inference_healthcheck_interval_sec = value.value(
      "inference_healthcheck_interval_sec", settings.inference_healthcheck_interval_sec);
  return settings;
}

std::string SerializeGatewaySettings(const GatewaySettings& settings) {
  return json{
      {"listen_host", settings.listen_host},
      {"listen_port", settings.listen_port},
      {"server_name", settings.server_name},
  }
      .dump();
}

std::string SerializeRuntimeGpuNodes(const std::vector<RuntimeGpuNode>& gpu_nodes) {
  json value = json::array();
  for (const auto& gpu_node : gpu_nodes) {
    json gpu_node_json = {
        {"name", gpu_node.name},
        {"node_name", gpu_node.node_name},
        {"gpu_device", gpu_node.gpu_device},
        {"placement_mode", ToString(gpu_node.placement_mode)},
        {"share_mode", ToString(gpu_node.share_mode)},
        {"gpu_fraction", gpu_node.gpu_fraction},
        {"priority", gpu_node.priority},
        {"preemptible", gpu_node.preemptible},
        {"enabled", gpu_node.enabled},
    };
    if (gpu_node.memory_cap_mb.has_value()) {
      gpu_node_json["memory_cap_mb"] = *gpu_node.memory_cap_mb;
    }
    value.push_back(std::move(gpu_node_json));
  }
  return value.dump();
}

std::vector<RuntimeGpuNode> DeserializeRuntimeGpuNodes(const std::string& json_text) {
  std::vector<RuntimeGpuNode> gpu_nodes;
  if (json_text.empty()) {
    return gpu_nodes;
  }

  const json value = json::parse(json_text);
  for (const auto& item : value) {
    gpu_nodes.push_back(
        RuntimeGpuNode{
            item.value("name", std::string{}),
            item.value("node_name", std::string{}),
            item.value("gpu_device", std::string{}),
            ParsePlacementMode(item.value("placement_mode", std::string("manual"))),
            ParseGpuShareMode(item.value("share_mode", std::string("exclusive"))),
            item.value("gpu_fraction", 0.0),
            item.value("priority", 100),
            item.value("preemptible", false),
            item.contains("memory_cap_mb") && !item.at("memory_cap_mb").is_null()
                ? std::optional<int>(item.at("memory_cap_mb").get<int>())
                : std::nullopt,
            item.value("enabled", true),
        });
  }
  return gpu_nodes;
}

GatewaySettings DeserializeGatewaySettings(const std::string& json_text) {
  GatewaySettings settings;
  if (json_text.empty()) {
    return settings;
  }

  const json value = json::parse(json_text);
  settings.listen_host = value.value("listen_host", settings.listen_host);
  settings.listen_port = value.value("listen_port", settings.listen_port);
  settings.server_name = value.value("server_name", settings.server_name);
  return settings;
}

DiskKind ParseDiskKind(const std::string& value) {
  if (value == "plane-shared") {
    return DiskKind::PlaneShared;
  }
  if (value == "infer-private") {
    return DiskKind::InferPrivate;
  }
  if (value == "worker-private") {
    return DiskKind::WorkerPrivate;
  }
  throw std::runtime_error("unknown disk kind '" + value + "'");
}

InstanceRole ParseInstanceRole(const std::string& value) {
  if (value == "infer") {
    return InstanceRole::Infer;
  }
  if (value == "worker") {
    return InstanceRole::Worker;
  }
  throw std::runtime_error("unknown instance role '" + value + "'");
}

sqlite3* AsSqlite(void* db) {
  return static_cast<sqlite3*>(db);
}

HostAssignment AssignmentFromStatement(sqlite3_stmt* statement) {
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
  return assignment;
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

RolloutActionRecord RolloutActionFromStatement(sqlite3_stmt* statement) {
  RolloutActionRecord action;
  action.id = sqlite3_column_int(statement, 0);
  action.desired_generation = sqlite3_column_int(statement, 1);
  action.step = sqlite3_column_int(statement, 2);
  action.worker_name = ToColumnText(statement, 3);
  action.action = ToColumnText(statement, 4);
  action.target_node_name = ToColumnText(statement, 5);
  action.target_gpu_device = ToColumnText(statement, 6);
  action.victim_worker_names = DeserializeStringArray(ToColumnText(statement, 7));
  action.reason = ToColumnText(statement, 8);
  action.status = ParseRolloutActionStatus(ToColumnText(statement, 9));
  action.status_message = ToColumnText(statement, 10);
  return action;
}

HostObservation ObservationFromStatement(sqlite3_stmt* statement) {
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
  observation.heartbeat_at = ToColumnText(statement, 10);
  return observation;
}

NodeAvailabilityOverride AvailabilityOverrideFromStatement(sqlite3_stmt* statement) {
  NodeAvailabilityOverride availability_override;
  availability_override.node_name = ToColumnText(statement, 0);
  availability_override.availability = ParseNodeAvailability(ToColumnText(statement, 1));
  availability_override.status_message = ToColumnText(statement, 2);
  availability_override.updated_at = ToColumnText(statement, 3);
  return availability_override;
}

SchedulerPlaneRuntime SchedulerPlaneRuntimeFromStatement(sqlite3_stmt* statement) {
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

SchedulerWorkerRuntime SchedulerWorkerRuntimeFromStatement(sqlite3_stmt* statement) {
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

SchedulerNodeRuntime SchedulerNodeRuntimeFromStatement(sqlite3_stmt* statement) {
  SchedulerNodeRuntime runtime;
  runtime.node_name = ToColumnText(statement, 0);
  runtime.plane_name = ToColumnText(statement, 1);
  runtime.last_move_at = ToColumnText(statement, 2);
  runtime.last_verified_generation = ToOptionalColumnInt(statement, 3);
  runtime.updated_at = ToColumnText(statement, 4);
  return runtime;
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
  sqlite3* db = AsSqlite(db_);
  Exec(db, kBootstrapSql);
  EnsureColumn(db, "planes", "control_root", "control_root TEXT NOT NULL DEFAULT ''");
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
      "rebalance_iteration",
      "rebalance_iteration INTEGER NOT NULL DEFAULT 0");
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
}

void ControllerStore::ReplaceDesiredState(const DesiredState& state, int generation) {
  ReplaceDesiredState(state, generation, 0);
}

void ControllerStore::ReplaceDesiredState(
    const DesiredState& state,
    int generation,
    int rebalance_iteration) {
  sqlite3* db = AsSqlite(db_);
  Exec(db, "BEGIN IMMEDIATE TRANSACTION;");

  try {
    Exec(
        db,
        "DELETE FROM instance_labels;"
        "DELETE FROM instance_environment;"
        "DELETE FROM instance_dependencies;"
        "DELETE FROM instances;"
        "DELETE FROM virtual_disks;"
        "DELETE FROM node_gpus;"
        "DELETE FROM nodes;"
        "DELETE FROM planes;");

    {
      Statement statement(
          db,
          "INSERT INTO planes("
          "name, shared_disk_name, control_root, inference_config_json, gateway_config_json, "
          "runtime_gpu_nodes_json, generation, rebalance_iteration, state"
          ") VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, 'ready');");
      statement.BindText(1, state.plane_name);
      statement.BindText(2, state.plane_shared_disk_name);
      statement.BindText(
          3,
          state.control_root.empty() ? "/comet/shared/control/" + state.plane_name
                                     : state.control_root);
      statement.BindText(4, SerializeInferenceSettings(state.inference));
      statement.BindText(5, SerializeGatewaySettings(state.gateway));
      statement.BindText(6, SerializeRuntimeGpuNodes(state.runtime_gpu_nodes));
      statement.BindInt(7, generation);
      statement.BindInt(8, rebalance_iteration);
      statement.StepDone();
    }

    for (const auto& node : state.nodes) {
      Statement node_statement(
          db,
          "INSERT INTO nodes(name, platform, state) VALUES(?1, ?2, 'ready');");
      node_statement.BindText(1, node.name);
      node_statement.BindText(2, node.platform);
      node_statement.StepDone();

      for (const auto& gpu_device : node.gpu_devices) {
        Statement gpu_statement(
            db,
            "INSERT INTO node_gpus(node_name, gpu_device, memory_mb) VALUES(?1, ?2, ?3);");
        gpu_statement.BindText(1, node.name);
        gpu_statement.BindText(2, gpu_device);
        const auto memory_it = node.gpu_memory_mb.find(gpu_device);
        gpu_statement.BindInt(3, memory_it == node.gpu_memory_mb.end() ? 0 : memory_it->second);
        gpu_statement.StepDone();
      }
    }

    for (const auto& disk : state.disks) {
      Statement statement(
          db,
          "INSERT INTO virtual_disks("
          "name, plane_name, owner_name, node_name, kind, host_path, container_path, size_gb, "
          "state"
          ") VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, 'ready');");
      statement.BindText(1, disk.name);
      statement.BindText(2, disk.plane_name);
      statement.BindText(3, disk.owner_name);
      statement.BindText(4, disk.node_name);
      statement.BindText(5, ToString(disk.kind));
      statement.BindText(6, disk.host_path);
      statement.BindText(7, disk.container_path);
      statement.BindInt(8, disk.size_gb);
      statement.StepDone();
    }

    for (const auto& instance : state.instances) {
      Statement statement(
          db,
          "INSERT INTO instances("
          "name, plane_name, node_name, role, state, image, command, private_disk_name, "
          "shared_disk_name, gpu_device, placement_mode, share_mode, gpu_fraction, priority, preemptible, "
          "memory_cap_mb, private_disk_size_gb"
          ") VALUES(?1, ?2, ?3, ?4, 'ready', ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16);");
      statement.BindText(1, instance.name);
      statement.BindText(2, instance.plane_name);
      statement.BindText(3, instance.node_name);
      statement.BindText(4, ToString(instance.role));
      statement.BindText(5, instance.image);
      statement.BindText(6, instance.command);
      statement.BindText(7, instance.private_disk_name);
      statement.BindText(8, instance.shared_disk_name);
      statement.BindOptionalText(9, instance.gpu_device);
      statement.BindText(10, ToString(instance.placement_mode));
      statement.BindText(11, ToString(instance.share_mode));
      statement.BindDouble(12, instance.gpu_fraction);
      statement.BindInt(13, instance.priority);
      statement.BindInt(14, instance.preemptible ? 1 : 0);
      statement.BindOptionalInt(15, instance.memory_cap_mb);
      statement.BindInt(16, instance.private_disk_size_gb);
      statement.StepDone();

      for (const auto& dependency : instance.depends_on) {
        Statement dependency_statement(
            db,
            "INSERT INTO instance_dependencies(instance_name, dependency_name) "
            "VALUES(?1, ?2);");
        dependency_statement.BindText(1, instance.name);
        dependency_statement.BindText(2, dependency);
        dependency_statement.StepDone();
      }

      for (const auto& [key, value] : instance.environment) {
        Statement env_statement(
            db,
            "INSERT INTO instance_environment(instance_name, env_key, env_value) "
            "VALUES(?1, ?2, ?3);");
        env_statement.BindText(1, instance.name);
        env_statement.BindText(2, key);
        env_statement.BindText(3, value);
        env_statement.StepDone();
      }

      for (const auto& [key, value] : instance.labels) {
        Statement label_statement(
            db,
            "INSERT INTO instance_labels(instance_name, label_key, label_value) "
            "VALUES(?1, ?2, ?3);");
        label_statement.BindText(1, instance.name);
        label_statement.BindText(2, key);
        label_statement.BindText(3, value);
        label_statement.StepDone();
      }
    }

    Exec(db, "COMMIT;");
  } catch (...) {
    Exec(db, "ROLLBACK;");
    throw;
  }
}

void ControllerStore::ReplaceDesiredState(const DesiredState& state) {
  ReplaceDesiredState(state, 1);
}

std::optional<DesiredState> ControllerStore::LoadDesiredState() const {
  sqlite3* db = AsSqlite(db_);
  DesiredState state;

  {
    Statement statement(
        db,
        "SELECT name, shared_disk_name, control_root, inference_config_json, gateway_config_json, "
        "runtime_gpu_nodes_json "
        "FROM planes ORDER BY created_at DESC, name ASC LIMIT 1;");
    if (!statement.StepRow()) {
      return std::nullopt;
    }
    state.plane_name = ToColumnText(statement.raw(), 0);
    state.plane_shared_disk_name = ToColumnText(statement.raw(), 1);
    state.control_root = ToColumnText(statement.raw(), 2);
    if (state.control_root.empty()) {
      state.control_root = "/comet/shared/control/" + state.plane_name;
    }
    state.inference = DeserializeInferenceSettings(ToColumnText(statement.raw(), 3));
    state.gateway = DeserializeGatewaySettings(ToColumnText(statement.raw(), 4));
    state.runtime_gpu_nodes = DeserializeRuntimeGpuNodes(ToColumnText(statement.raw(), 5));
  }

  {
    std::map<std::string, std::size_t> node_indexes;
    Statement node_statement(
        db,
        "SELECT name, platform FROM nodes ORDER BY name ASC;");
    while (node_statement.StepRow()) {
      NodeInventory node;
      node.name = ToColumnText(node_statement.raw(), 0);
      node.platform = ToColumnText(node_statement.raw(), 1);
      node_indexes[node.name] = state.nodes.size();
      state.nodes.push_back(std::move(node));
    }

    Statement gpu_statement(
        db,
        "SELECT node_name, gpu_device, memory_mb "
        "FROM node_gpus ORDER BY node_name ASC, gpu_device ASC;");
    while (gpu_statement.StepRow()) {
      const std::string node_name = ToColumnText(gpu_statement.raw(), 0);
      const auto node_it = node_indexes.find(node_name);
      if (node_it == node_indexes.end()) {
        throw std::runtime_error("gpu row references unknown node '" + node_name + "'");
      }
      const std::string gpu_device = ToColumnText(gpu_statement.raw(), 1);
      state.nodes[node_it->second].gpu_devices.push_back(gpu_device);
      const int memory_mb = sqlite3_column_int(gpu_statement.raw(), 2);
      if (memory_mb > 0) {
        state.nodes[node_it->second].gpu_memory_mb[gpu_device] = memory_mb;
      }
    }
  }

  {
    Statement statement(
        db,
        "SELECT name, plane_name, owner_name, node_name, kind, host_path, container_path, size_gb "
        "FROM virtual_disks ORDER BY name ASC;");
    while (statement.StepRow()) {
      DiskSpec disk;
      disk.name = ToColumnText(statement.raw(), 0);
      disk.plane_name = ToColumnText(statement.raw(), 1);
      disk.owner_name = ToColumnText(statement.raw(), 2);
      disk.node_name = ToColumnText(statement.raw(), 3);
      disk.kind = ParseDiskKind(ToColumnText(statement.raw(), 4));
      disk.host_path = ToColumnText(statement.raw(), 5);
      disk.container_path = ToColumnText(statement.raw(), 6);
      disk.size_gb = sqlite3_column_int(statement.raw(), 7);
      state.disks.push_back(std::move(disk));
    }
  }

  std::map<std::string, std::size_t> instance_indexes;
  {
    Statement statement(
        db,
        "SELECT name, plane_name, node_name, role, image, command, private_disk_name, "
        "shared_disk_name, gpu_device, placement_mode, share_mode, gpu_fraction, priority, preemptible, "
        "memory_cap_mb, private_disk_size_gb "
        "FROM instances ORDER BY name ASC;");
    while (statement.StepRow()) {
      InstanceSpec instance;
      instance.name = ToColumnText(statement.raw(), 0);
      instance.plane_name = ToColumnText(statement.raw(), 1);
      instance.node_name = ToColumnText(statement.raw(), 2);
      instance.role = ParseInstanceRole(ToColumnText(statement.raw(), 3));
      instance.image = ToColumnText(statement.raw(), 4);
      instance.command = ToColumnText(statement.raw(), 5);
      instance.private_disk_name = ToColumnText(statement.raw(), 6);
      instance.shared_disk_name = ToColumnText(statement.raw(), 7);
      const std::string gpu_device = ToColumnText(statement.raw(), 8);
      if (!gpu_device.empty()) {
        instance.gpu_device = gpu_device;
      }
      instance.placement_mode = ParsePlacementMode(ToColumnText(statement.raw(), 9));
      instance.share_mode = ParseGpuShareMode(ToColumnText(statement.raw(), 10));
      instance.gpu_fraction = sqlite3_column_double(statement.raw(), 11);
      instance.priority = sqlite3_column_int(statement.raw(), 12);
      instance.preemptible = sqlite3_column_int(statement.raw(), 13) != 0;
      instance.memory_cap_mb = ToOptionalColumnInt(statement.raw(), 14);
      instance.private_disk_size_gb = sqlite3_column_int(statement.raw(), 15);
      instance_indexes[instance.name] = state.instances.size();
      state.instances.push_back(std::move(instance));
    }
  }

  {
    Statement statement(
        db,
        "SELECT instance_name, dependency_name "
        "FROM instance_dependencies ORDER BY instance_name ASC, dependency_name ASC;");
    while (statement.StepRow()) {
      const std::string instance_name = ToColumnText(statement.raw(), 0);
      const auto instance_it = instance_indexes.find(instance_name);
      if (instance_it == instance_indexes.end()) {
        throw std::runtime_error(
            "dependency row references unknown instance '" + instance_name + "'");
      }
      state.instances[instance_it->second].depends_on.push_back(ToColumnText(statement.raw(), 1));
    }
  }

  {
    Statement statement(
        db,
        "SELECT instance_name, env_key, env_value "
        "FROM instance_environment ORDER BY instance_name ASC, env_key ASC;");
    while (statement.StepRow()) {
      const std::string instance_name = ToColumnText(statement.raw(), 0);
      const auto instance_it = instance_indexes.find(instance_name);
      if (instance_it == instance_indexes.end()) {
        throw std::runtime_error(
            "environment row references unknown instance '" + instance_name + "'");
      }
      state.instances[instance_it->second].environment[ToColumnText(statement.raw(), 1)] =
          ToColumnText(statement.raw(), 2);
    }
  }

  {
    Statement statement(
        db,
        "SELECT instance_name, label_key, label_value "
        "FROM instance_labels ORDER BY instance_name ASC, label_key ASC;");
    while (statement.StepRow()) {
      const std::string instance_name = ToColumnText(statement.raw(), 0);
      const auto instance_it = instance_indexes.find(instance_name);
      if (instance_it == instance_indexes.end()) {
        throw std::runtime_error(
            "label row references unknown instance '" + instance_name + "'");
      }
      state.instances[instance_it->second].labels[ToColumnText(statement.raw(), 1)] =
          ToColumnText(statement.raw(), 2);
    }
  }

  if (state.inference.primary_infer_node.empty()) {
    for (const auto& instance : state.instances) {
      if (instance.role == InstanceRole::Infer) {
        state.inference.primary_infer_node = instance.node_name;
        break;
      }
    }
  }

  return state;
}

std::optional<int> ControllerStore::LoadDesiredGeneration() const {
  sqlite3* db = AsSqlite(db_);
  Statement statement(
      db,
      "SELECT generation FROM planes ORDER BY created_at DESC, name ASC LIMIT 1;");
  if (!statement.StepRow()) {
    return std::nullopt;
  }
  return sqlite3_column_int(statement.raw(), 0);
}

std::optional<int> ControllerStore::LoadRebalanceIteration() const {
  sqlite3* db = AsSqlite(db_);
  Statement statement(
      db,
      "SELECT rebalance_iteration FROM planes ORDER BY created_at DESC, name ASC LIMIT 1;");
  if (!statement.StepRow()) {
    return std::nullopt;
  }
  return sqlite3_column_int(statement.raw(), 0);
}

void ControllerStore::UpsertNodeAvailabilityOverride(
    const NodeAvailabilityOverride& availability_override) {
  sqlite3* db = AsSqlite(db_);
  Statement statement(
      db,
      "INSERT INTO node_availability_overrides("
      "node_name, availability, status_message, updated_at"
      ") VALUES(?1, ?2, ?3, CURRENT_TIMESTAMP) "
      "ON CONFLICT(node_name) DO UPDATE SET "
      "availability = excluded.availability, "
      "status_message = excluded.status_message, "
      "updated_at = CURRENT_TIMESTAMP;");
  statement.BindText(1, availability_override.node_name);
  statement.BindText(2, ToString(availability_override.availability));
  statement.BindText(3, availability_override.status_message);
  statement.StepDone();
}

std::optional<NodeAvailabilityOverride> ControllerStore::LoadNodeAvailabilityOverride(
    const std::string& node_name) const {
  sqlite3* db = AsSqlite(db_);
  Statement statement(
      db,
      "SELECT node_name, availability, status_message, updated_at "
      "FROM node_availability_overrides WHERE node_name = ?1;");
  statement.BindText(1, node_name);
  if (!statement.StepRow()) {
    return std::nullopt;
  }
  return AvailabilityOverrideFromStatement(statement.raw());
}

std::vector<NodeAvailabilityOverride> ControllerStore::LoadNodeAvailabilityOverrides(
    const std::optional<std::string>& node_name) const {
  sqlite3* db = AsSqlite(db_);
  std::vector<NodeAvailabilityOverride> overrides;

  std::string sql =
      "SELECT node_name, availability, status_message, updated_at "
      "FROM node_availability_overrides";
  if (node_name.has_value()) {
    sql += " WHERE node_name = ?1";
  }
  sql += " ORDER BY node_name ASC;";

  Statement statement(db, sql);
  if (node_name.has_value()) {
    statement.BindText(1, *node_name);
  }
  while (statement.StepRow()) {
    overrides.push_back(AvailabilityOverrideFromStatement(statement.raw()));
  }
  return overrides;
}

void ControllerStore::ReplaceRolloutActions(
    int desired_generation,
    const std::vector<SchedulerRolloutAction>& actions) {
  sqlite3* db = AsSqlite(db_);
  Exec(db, "BEGIN IMMEDIATE TRANSACTION;");
  try {
    {
      Statement delete_statement(
          db,
          "DELETE FROM rollout_actions;");
      delete_statement.StepDone();
    }

    for (const auto& action : actions) {
      Statement insert_statement(
          db,
          "INSERT INTO rollout_actions("
          "desired_generation, step, worker_name, action, target_node_name, "
          "target_gpu_device, victim_worker_names_json, reason, status, status_message, "
          "created_at, updated_at"
          ") VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP);");
      insert_statement.BindInt(1, desired_generation);
      insert_statement.BindInt(2, action.step);
      insert_statement.BindText(3, action.worker_name);
      insert_statement.BindText(4, action.action);
      insert_statement.BindText(5, action.target_node_name);
      insert_statement.BindText(6, action.target_gpu_device);
      insert_statement.BindText(7, SerializeStringArray(action.victim_worker_names));
      insert_statement.BindText(8, action.reason);
      insert_statement.BindText(9, ToString(RolloutActionStatus::Pending));
      insert_statement.BindText(10, "");
      insert_statement.StepDone();
    }

    Exec(db, "COMMIT;");
  } catch (...) {
    Exec(db, "ROLLBACK;");
    throw;
  }
}

std::vector<RolloutActionRecord> ControllerStore::LoadRolloutActions(
    const std::optional<std::string>& target_node_name,
    const std::optional<RolloutActionStatus>& status) const {
  sqlite3* db = AsSqlite(db_);
  std::string sql =
      "SELECT id, desired_generation, step, worker_name, action, target_node_name, "
      "target_gpu_device, victim_worker_names_json, reason, status, status_message "
      "FROM rollout_actions";
  bool has_where = false;
  if (target_node_name.has_value()) {
    sql += " WHERE target_node_name = ?1";
    has_where = true;
  }
  if (status.has_value()) {
    sql += has_where ? " AND " : " WHERE ";
    sql += target_node_name.has_value() ? "status = ?2" : "status = ?1";
  }
  sql += " ORDER BY desired_generation ASC, step ASC, id ASC;";

  Statement statement(db, sql);
  if (target_node_name.has_value()) {
    statement.BindText(1, *target_node_name);
  }
  if (status.has_value()) {
    statement.BindText(target_node_name.has_value() ? 2 : 1, ToString(*status));
  }

  std::vector<RolloutActionRecord> actions;
  while (statement.StepRow()) {
    actions.push_back(RolloutActionFromStatement(statement.raw()));
  }
  return actions;
}

bool ControllerStore::UpdateRolloutActionStatus(
    int action_id,
    RolloutActionStatus status,
    const std::string& status_message) {
  sqlite3* db = AsSqlite(db_);
  Statement statement(
      db,
      "UPDATE rollout_actions "
      "SET status = ?2, status_message = ?3, updated_at = CURRENT_TIMESTAMP "
      "WHERE id = ?1;");
  statement.BindInt(1, action_id);
  statement.BindText(2, ToString(status));
  statement.BindText(3, status_message);
  statement.StepDone();
  return sqlite3_changes(db) == 1;
}

void ControllerStore::UpsertHostObservation(const HostObservation& observation) {
  sqlite3* db = AsSqlite(db_);
  Statement statement(
      db,
      "INSERT INTO host_observations("
      "node_name, plane_name, applied_generation, last_assignment_id, status, "
      "status_message, observed_state_json, runtime_status_json, "
      "instance_runtime_json, gpu_telemetry_json, heartbeat_at, updated_at"
      ") VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP) "
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
  statement.StepDone();
}

std::optional<HostObservation> ControllerStore::LoadHostObservation(
    const std::string& node_name) const {
  sqlite3* db = AsSqlite(db_);
  Statement statement(
      db,
      "SELECT node_name, plane_name, applied_generation, last_assignment_id, status, "
      "status_message, observed_state_json, runtime_status_json, "
      "instance_runtime_json, gpu_telemetry_json, heartbeat_at "
      "FROM host_observations WHERE node_name = ?1;");
  statement.BindText(1, node_name);
  if (!statement.StepRow()) {
    return std::nullopt;
  }
  return ObservationFromStatement(statement.raw());
}

std::vector<HostObservation> ControllerStore::LoadHostObservations(
    const std::optional<std::string>& node_name) const {
  sqlite3* db = AsSqlite(db_);
  std::vector<HostObservation> observations;

  std::string sql =
      "SELECT node_name, plane_name, applied_generation, last_assignment_id, status, "
      "status_message, observed_state_json, runtime_status_json, "
      "instance_runtime_json, gpu_telemetry_json, heartbeat_at "
      "FROM host_observations";
  if (node_name.has_value()) {
    sql += " WHERE node_name = ?1";
  }
  sql += " ORDER BY node_name ASC;";

  Statement statement(db, sql);
  if (node_name.has_value()) {
    statement.BindText(1, *node_name);
  }

  while (statement.StepRow()) {
    observations.push_back(ObservationFromStatement(statement.raw()));
  }

  return observations;
}

void ControllerStore::UpsertSchedulerPlaneRuntime(const SchedulerPlaneRuntime& runtime) {
  sqlite3* db = AsSqlite(db_);
  Statement statement(
      db,
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

std::optional<SchedulerPlaneRuntime> ControllerStore::LoadSchedulerPlaneRuntime(
    const std::string& plane_name) const {
  sqlite3* db = AsSqlite(db_);
  Statement statement(
      db,
      "SELECT plane_name, active_action, active_worker_name, phase, action_generation, "
      "stable_samples, rollback_attempt_count, source_node_name, source_gpu_device, "
      "target_node_name, target_gpu_device, previous_state_json, status_message, "
      "started_at, updated_at "
      "FROM scheduler_plane_runtime WHERE plane_name = ?1;");
  statement.BindText(1, plane_name);
  if (!statement.StepRow()) {
    return std::nullopt;
  }
  return SchedulerPlaneRuntimeFromStatement(statement.raw());
}

void ControllerStore::ClearSchedulerPlaneRuntime(const std::string& plane_name) {
  sqlite3* db = AsSqlite(db_);
  Statement statement(
      db,
      "DELETE FROM scheduler_plane_runtime WHERE plane_name = ?1;");
  statement.BindText(1, plane_name);
  statement.StepDone();
}

void ControllerStore::UpsertSchedulerWorkerRuntime(const SchedulerWorkerRuntime& runtime) {
  sqlite3* db = AsSqlite(db_);
  Statement statement(
      db,
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

std::optional<SchedulerWorkerRuntime> ControllerStore::LoadSchedulerWorkerRuntime(
    const std::string& worker_name) const {
  sqlite3* db = AsSqlite(db_);
  Statement statement(
      db,
      "SELECT worker_name, plane_name, last_move_at, last_eviction_at, "
      "last_verified_generation, last_scheduler_phase, last_status_message, "
      "manual_intervention_required, updated_at "
      "FROM scheduler_worker_runtime WHERE worker_name = ?1;");
  statement.BindText(1, worker_name);
  if (!statement.StepRow()) {
    return std::nullopt;
  }
  return SchedulerWorkerRuntimeFromStatement(statement.raw());
}

std::vector<SchedulerWorkerRuntime> ControllerStore::LoadSchedulerWorkerRuntimes(
    const std::optional<std::string>& plane_name) const {
  sqlite3* db = AsSqlite(db_);
  std::string sql =
      "SELECT worker_name, plane_name, last_move_at, last_eviction_at, "
      "last_verified_generation, last_scheduler_phase, last_status_message, "
      "manual_intervention_required, updated_at "
      "FROM scheduler_worker_runtime";
  if (plane_name.has_value()) {
    sql += " WHERE plane_name = ?1";
  }
  sql += " ORDER BY worker_name ASC;";
  Statement statement(db, sql);
  if (plane_name.has_value()) {
    statement.BindText(1, *plane_name);
  }
  std::vector<SchedulerWorkerRuntime> runtimes;
  while (statement.StepRow()) {
    runtimes.push_back(SchedulerWorkerRuntimeFromStatement(statement.raw()));
  }
  return runtimes;
}

void ControllerStore::UpsertSchedulerNodeRuntime(const SchedulerNodeRuntime& runtime) {
  sqlite3* db = AsSqlite(db_);
  Statement statement(
      db,
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

std::optional<SchedulerNodeRuntime> ControllerStore::LoadSchedulerNodeRuntime(
    const std::string& node_name) const {
  sqlite3* db = AsSqlite(db_);
  Statement statement(
      db,
      "SELECT node_name, plane_name, last_move_at, last_verified_generation, updated_at "
      "FROM scheduler_node_runtime WHERE node_name = ?1;");
  statement.BindText(1, node_name);
  if (!statement.StepRow()) {
    return std::nullopt;
  }
  return SchedulerNodeRuntimeFromStatement(statement.raw());
}

std::vector<SchedulerNodeRuntime> ControllerStore::LoadSchedulerNodeRuntimes(
    const std::optional<std::string>& plane_name) const {
  sqlite3* db = AsSqlite(db_);
  std::string sql =
      "SELECT node_name, plane_name, last_move_at, last_verified_generation, updated_at "
      "FROM scheduler_node_runtime";
  if (plane_name.has_value()) {
    sql += " WHERE plane_name = ?1";
  }
  sql += " ORDER BY node_name ASC;";
  Statement statement(db, sql);
  if (plane_name.has_value()) {
    statement.BindText(1, *plane_name);
  }
  std::vector<SchedulerNodeRuntime> runtimes;
  while (statement.StepRow()) {
    runtimes.push_back(SchedulerNodeRuntimeFromStatement(statement.raw()));
  }
  return runtimes;
}

void ControllerStore::ReplaceHostAssignments(const std::vector<HostAssignment>& assignments) {
  sqlite3* db = AsSqlite(db_);
  Exec(db, "BEGIN IMMEDIATE TRANSACTION;");

  try {
    if (!assignments.empty()) {
      Statement supersede_statement(
          db,
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
      Statement statement(
          db,
          "INSERT INTO host_assignments("
          "node_name, plane_name, desired_generation, attempt_count, max_attempts, "
          "assignment_type, desired_state_json, artifacts_root, status, status_message, "
          "updated_at"
          ") VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, CURRENT_TIMESTAMP);");
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
      statement.StepDone();
    }

    Exec(db, "COMMIT;");
  } catch (...) {
    Exec(db, "ROLLBACK;");
    throw;
  }
}

void ControllerStore::EnqueueHostAssignments(
    const std::vector<HostAssignment>& assignments,
    const std::string& supersede_reason) {
  if (assignments.empty()) {
    return;
  }

  sqlite3* db = AsSqlite(db_);
  Exec(db, "BEGIN IMMEDIATE TRANSACTION;");

  try {
    for (const auto& assignment : assignments) {
      Statement supersede_statement(
          db,
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

      Statement insert_statement(
          db,
          "INSERT INTO host_assignments("
          "node_name, plane_name, desired_generation, attempt_count, max_attempts, "
          "assignment_type, desired_state_json, artifacts_root, status, status_message, "
          "updated_at"
          ") VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, CURRENT_TIMESTAMP);");
      insert_statement.BindText(1, assignment.node_name);
      insert_statement.BindText(2, assignment.plane_name);
      insert_statement.BindInt(3, assignment.desired_generation);
      insert_statement.BindInt(4, assignment.attempt_count);
      insert_statement.BindInt(5, assignment.max_attempts);
      insert_statement.BindText(6, assignment.assignment_type);
      insert_statement.BindText(7, assignment.desired_state_json);
      insert_statement.BindText(8, assignment.artifacts_root);
      insert_statement.BindText(9, ToString(assignment.status));
      insert_statement.BindText(10, assignment.status_message);
      insert_statement.StepDone();
    }

    Exec(db, "COMMIT;");
  } catch (...) {
    Exec(db, "ROLLBACK;");
    throw;
  }
}

std::optional<HostAssignment> ControllerStore::LoadHostAssignment(int assignment_id) const {
  sqlite3* db = AsSqlite(db_);
  Statement statement(
      db,
      "SELECT id, node_name, plane_name, desired_generation, attempt_count, max_attempts, "
      "assignment_type, desired_state_json, artifacts_root, status, status_message "
      "FROM host_assignments WHERE id = ?1;");
  statement.BindInt(1, assignment_id);
  if (!statement.StepRow()) {
    return std::nullopt;
  }
  return AssignmentFromStatement(statement.raw());
}

std::vector<HostAssignment> ControllerStore::LoadHostAssignments(
    const std::optional<std::string>& node_name,
    const std::optional<HostAssignmentStatus>& status) const {
  sqlite3* db = AsSqlite(db_);
  std::vector<HostAssignment> assignments;

  const bool has_node = node_name.has_value();
  const bool has_status = status.has_value();

  std::string sql =
      "SELECT id, node_name, plane_name, desired_generation, attempt_count, max_attempts, "
      "assignment_type, desired_state_json, artifacts_root, status, status_message "
      "FROM host_assignments";
  if (has_node || has_status) {
    sql += " WHERE ";
    if (has_node) {
      sql += "node_name = ?1";
    }
    if (has_node && has_status) {
      sql += " AND ";
    }
    if (has_status) {
      sql += has_node ? "status = ?2" : "status = ?1";
    }
  }
  sql += " ORDER BY id ASC;";

  Statement statement(db, sql);
  if (has_node) {
    statement.BindText(1, *node_name);
  }
  if (has_status) {
    statement.BindText(has_node ? 2 : 1, ToString(*status));
  }

  while (statement.StepRow()) {
    assignments.push_back(AssignmentFromStatement(statement.raw()));
  }

  return assignments;
}

std::optional<HostAssignment> ControllerStore::ClaimNextHostAssignment(
    const std::string& node_name) {
  sqlite3* db = AsSqlite(db_);
  Exec(db, "BEGIN IMMEDIATE TRANSACTION;");

  try {
    std::optional<HostAssignment> assignment;
    {
      Statement statement(
          db,
          "SELECT id, node_name, plane_name, desired_generation, attempt_count, max_attempts, "
          "assignment_type, desired_state_json, artifacts_root, status, status_message "
          "FROM host_assignments "
          "WHERE node_name = ?1 AND status = 'pending' AND attempt_count < max_attempts "
          "ORDER BY id ASC LIMIT 1;");
      statement.BindText(1, node_name);
      if (statement.StepRow()) {
        assignment = AssignmentFromStatement(statement.raw());
      }
    }

    if (!assignment.has_value()) {
      Exec(db, "COMMIT;");
      return std::nullopt;
    }

    {
      Statement statement(
          db,
          "UPDATE host_assignments "
          "SET status = 'claimed', "
          "status_message = '', "
          "attempt_count = attempt_count + 1, "
          "updated_at = CURRENT_TIMESTAMP "
          "WHERE id = ?1 AND status = 'pending';");
      statement.BindInt(1, assignment->id);
      statement.StepDone();
    }

    Exec(db, "COMMIT;");
    assignment->status = HostAssignmentStatus::Claimed;
    assignment->attempt_count += 1;
    return assignment;
  } catch (...) {
    Exec(db, "ROLLBACK;");
    throw;
  }
}

bool ControllerStore::TransitionClaimedHostAssignment(
    int assignment_id,
    HostAssignmentStatus status,
    const std::string& status_message) {
  sqlite3* db = AsSqlite(db_);
  Statement statement(
      db,
      "UPDATE host_assignments "
      "SET status = ?2, status_message = ?3, updated_at = CURRENT_TIMESTAMP "
      "WHERE id = ?1 AND status = 'claimed';");
  statement.BindInt(1, assignment_id);
  statement.BindText(2, ToString(status));
  statement.BindText(3, status_message);
  statement.StepDone();
  return sqlite3_changes(db) > 0;
}

bool ControllerStore::RetryFailedHostAssignment(
    int assignment_id,
    const std::string& status_message) {
  sqlite3* db = AsSqlite(db_);
  Statement statement(
      db,
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
  return sqlite3_changes(db) > 0;
}

void ControllerStore::UpdateHostAssignmentStatus(
    int assignment_id,
    HostAssignmentStatus status,
    const std::string& status_message) {
  sqlite3* db = AsSqlite(db_);
  Statement statement(
      db,
      "UPDATE host_assignments "
      "SET status = ?2, status_message = ?3, updated_at = CURRENT_TIMESTAMP "
      "WHERE id = ?1;");
  statement.BindInt(1, assignment_id);
  statement.BindText(2, ToString(status));
  statement.BindText(3, status_message);
  statement.StepDone();
}

const std::string& ControllerStore::db_path() const {
  return db_path_;
}

std::string ToString(HostAssignmentStatus status) {
  switch (status) {
    case HostAssignmentStatus::Pending:
      return "pending";
    case HostAssignmentStatus::Claimed:
      return "claimed";
    case HostAssignmentStatus::Applied:
      return "applied";
    case HostAssignmentStatus::Failed:
      return "failed";
    case HostAssignmentStatus::Superseded:
      return "superseded";
  }
  return "unknown";
}

HostAssignmentStatus ParseHostAssignmentStatus(const std::string& value) {
  if (value == "pending") {
    return HostAssignmentStatus::Pending;
  }
  if (value == "claimed") {
    return HostAssignmentStatus::Claimed;
  }
  if (value == "applied") {
    return HostAssignmentStatus::Applied;
  }
  if (value == "failed") {
    return HostAssignmentStatus::Failed;
  }
  if (value == "superseded") {
    return HostAssignmentStatus::Superseded;
  }
  throw std::runtime_error("unknown host assignment status '" + value + "'");
}

std::string ToString(HostObservationStatus status) {
  switch (status) {
    case HostObservationStatus::Idle:
      return "idle";
    case HostObservationStatus::Applying:
      return "applying";
    case HostObservationStatus::Applied:
      return "applied";
    case HostObservationStatus::Failed:
      return "failed";
  }
  return "unknown";
}

HostObservationStatus ParseHostObservationStatus(const std::string& value) {
  if (value == "idle") {
    return HostObservationStatus::Idle;
  }
  if (value == "applying") {
    return HostObservationStatus::Applying;
  }
  if (value == "applied") {
    return HostObservationStatus::Applied;
  }
  if (value == "failed") {
    return HostObservationStatus::Failed;
  }
  throw std::runtime_error("unknown host observation status '" + value + "'");
}

std::string ToString(NodeAvailability availability) {
  switch (availability) {
    case NodeAvailability::Active:
      return "active";
    case NodeAvailability::Draining:
      return "draining";
    case NodeAvailability::Unavailable:
      return "unavailable";
  }
  return "unknown";
}

NodeAvailability ParseNodeAvailability(const std::string& value) {
  if (value == "active") {
    return NodeAvailability::Active;
  }
  if (value == "draining") {
    return NodeAvailability::Draining;
  }
  if (value == "unavailable") {
    return NodeAvailability::Unavailable;
  }
  throw std::runtime_error("unknown node availability '" + value + "'");
}

std::string ToString(RolloutActionStatus status) {
  switch (status) {
    case RolloutActionStatus::Pending:
      return "pending";
    case RolloutActionStatus::Acknowledged:
      return "acknowledged";
    case RolloutActionStatus::ReadyToRetry:
      return "ready-to-retry";
  }
  throw std::runtime_error("unknown rollout action status");
}

RolloutActionStatus ParseRolloutActionStatus(const std::string& value) {
  if (value == "pending") {
    return RolloutActionStatus::Pending;
  }
  if (value == "acknowledged") {
    return RolloutActionStatus::Acknowledged;
  }
  if (value == "ready-to-retry") {
    return RolloutActionStatus::ReadyToRetry;
  }
  throw std::runtime_error("unknown rollout action status '" + value + "'");
}

}  // namespace comet
