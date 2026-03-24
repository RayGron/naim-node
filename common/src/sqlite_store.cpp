#include "comet/sqlite_store.h"
#include "comet/state_json.h"

#include <array>
#include <filesystem>
#include <map>
#include <set>
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
    state TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS registered_hosts (
    node_name TEXT PRIMARY KEY,
    advertised_address TEXT NOT NULL DEFAULT '',
    public_key_base64 TEXT NOT NULL DEFAULT '',
    controller_public_key_fingerprint TEXT NOT NULL DEFAULT '',
    transport_mode TEXT NOT NULL DEFAULT 'out',
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

std::string SerializeBootstrapModelSpec(
    const std::optional<BootstrapModelSpec>& bootstrap_model) {
  if (!bootstrap_model.has_value()) {
    return "";
  }
  json value = {
      {"model_id", bootstrap_model->model_id},
  };
  if (bootstrap_model->served_model_name.has_value()) {
    value["served_model_name"] = *bootstrap_model->served_model_name;
  }
  if (bootstrap_model->local_path.has_value()) {
    value["local_path"] = *bootstrap_model->local_path;
  }
  if (bootstrap_model->source_url.has_value()) {
    value["source_url"] = *bootstrap_model->source_url;
  }
  if (!bootstrap_model->source_urls.empty()) {
    value["source_urls"] = bootstrap_model->source_urls;
  }
  if (bootstrap_model->target_filename.has_value()) {
    value["target_filename"] = *bootstrap_model->target_filename;
  }
  if (bootstrap_model->sha256.has_value()) {
    value["sha256"] = *bootstrap_model->sha256;
  }
  return value.dump();
}

std::optional<BootstrapModelSpec> DeserializeBootstrapModelSpec(const std::string& json_text) {
  if (json_text.empty()) {
    return std::nullopt;
  }
  const json value = json::parse(json_text);
  if (!value.is_object()) {
    return std::nullopt;
  }
  BootstrapModelSpec bootstrap_model;
  bootstrap_model.model_id = value.value("model_id", std::string{});
  if (value.contains("served_model_name") && !value.at("served_model_name").is_null()) {
    bootstrap_model.served_model_name = value.at("served_model_name").get<std::string>();
  }
  if (value.contains("local_path") && !value.at("local_path").is_null()) {
    bootstrap_model.local_path = value.at("local_path").get<std::string>();
  }
  if (value.contains("source_url") && !value.at("source_url").is_null()) {
    bootstrap_model.source_url = value.at("source_url").get<std::string>();
  }
  if (value.contains("source_urls") && value.at("source_urls").is_array()) {
    bootstrap_model.source_urls = value.at("source_urls").get<std::vector<std::string>>();
  }
  if (value.contains("target_filename") && !value.at("target_filename").is_null()) {
    bootstrap_model.target_filename = value.at("target_filename").get<std::string>();
  }
  if (value.contains("sha256") && !value.at("sha256").is_null()) {
    bootstrap_model.sha256 = value.at("sha256").get<std::string>();
  }
  return bootstrap_model;
}

std::optional<InteractionSettings> DeserializeInteractionSettings(const std::string& json_text) {
  if (json_text.empty()) {
    return std::nullopt;
  }
  const json value = json::parse(json_text);
  if (!value.is_object()) {
    return std::nullopt;
  }
  InteractionSettings interaction;
  if (value.contains("system_prompt") && !value.at("system_prompt").is_null()) {
    interaction.system_prompt = value.at("system_prompt").get<std::string>();
  }
  interaction.default_response_language =
      value.value("default_response_language", interaction.default_response_language);
  interaction.supported_response_languages =
      value.value("supported_response_languages", std::vector<std::string>{});
  interaction.follow_user_language =
      value.value("follow_user_language", interaction.follow_user_language);
  if (value.contains("completion_policy") && value.at("completion_policy").is_object()) {
    InteractionSettings::CompletionPolicy completion_policy;
    const auto& policy_value = value.at("completion_policy");
    completion_policy.response_mode =
        policy_value.value("response_mode", completion_policy.response_mode);
    completion_policy.max_tokens =
        policy_value.value("max_tokens", completion_policy.max_tokens);
    if (policy_value.contains("target_completion_tokens") &&
        !policy_value.at("target_completion_tokens").is_null()) {
      completion_policy.target_completion_tokens =
          policy_value.at("target_completion_tokens").get<int>();
    }
    completion_policy.max_continuations =
        policy_value.value("max_continuations", completion_policy.max_continuations);
    completion_policy.max_total_completion_tokens = policy_value.value(
        "max_total_completion_tokens",
        completion_policy.max_total_completion_tokens);
    completion_policy.max_elapsed_time_ms =
        policy_value.value("max_elapsed_time_ms", completion_policy.max_elapsed_time_ms);
    if (policy_value.contains("semantic_goal") &&
        !policy_value.at("semantic_goal").is_null()) {
      completion_policy.semantic_goal = policy_value.at("semantic_goal").get<std::string>();
    }
    interaction.completion_policy = std::move(completion_policy);
  }
  if (value.contains("long_completion_policy") && value.at("long_completion_policy").is_object()) {
    InteractionSettings::CompletionPolicy completion_policy;
    const auto& policy_value = value.at("long_completion_policy");
    completion_policy.response_mode =
        policy_value.value("response_mode", completion_policy.response_mode);
    completion_policy.max_tokens =
        policy_value.value("max_tokens", completion_policy.max_tokens);
    if (policy_value.contains("target_completion_tokens") &&
        !policy_value.at("target_completion_tokens").is_null()) {
      completion_policy.target_completion_tokens =
          policy_value.at("target_completion_tokens").get<int>();
    }
    completion_policy.max_continuations =
        policy_value.value("max_continuations", completion_policy.max_continuations);
    completion_policy.max_total_completion_tokens = policy_value.value(
        "max_total_completion_tokens",
        completion_policy.max_total_completion_tokens);
    completion_policy.max_elapsed_time_ms =
        policy_value.value("max_elapsed_time_ms", completion_policy.max_elapsed_time_ms);
    if (policy_value.contains("semantic_goal") &&
        !policy_value.at("semantic_goal").is_null()) {
      completion_policy.semantic_goal = policy_value.at("semantic_goal").get<std::string>();
    }
    interaction.long_completion_policy = std::move(completion_policy);
  }
  return interaction;
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
  assignment.progress_json = ToColumnText(statement, 11);
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
  observation.disk_telemetry_json = ToColumnText(statement, 10);
  observation.network_telemetry_json = ToColumnText(statement, 11);
  observation.cpu_telemetry_json = ToColumnText(statement, 12);
  observation.heartbeat_at = ToColumnText(statement, 13);
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

EventRecord EventFromStatement(sqlite3_stmt* statement) {
  EventRecord event;
  event.id = sqlite3_column_int(statement, 0);
  event.plane_name = ToColumnText(statement, 1);
  event.node_name = ToColumnText(statement, 2);
  event.worker_name = ToColumnText(statement, 3);
  event.assignment_id = ToOptionalColumnInt(statement, 4);
  event.rollout_action_id = ToOptionalColumnInt(statement, 5);
  event.category = ToColumnText(statement, 6);
  event.event_type = ToColumnText(statement, 7);
  event.severity = ToColumnText(statement, 8);
  event.message = ToColumnText(statement, 9);
  event.payload_json = ToColumnText(statement, 10);
  event.created_at = ToColumnText(statement, 11);
  return event;
}

PlaneRecord PlaneFromStatement(sqlite3_stmt* statement) {
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

DiskRuntimeState DiskRuntimeStateFromStatement(sqlite3_stmt* statement) {
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
  {
    Statement plane_statement(
        db,
        "SELECT name FROM planes WHERE desired_state_json = '' ORDER BY name ASC;");
    std::vector<std::string> plane_names;
    while (plane_statement.StepRow()) {
      plane_names.push_back(ToColumnText(plane_statement.raw(), 0));
    }
    for (const auto& plane_name : plane_names) {
      const auto desired_state = LoadDesiredState(plane_name);
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
  EnsureColumn(
      db,
      "rollout_actions",
      "plane_name",
      "plane_name TEXT NOT NULL DEFAULT ''");
  {
    Statement blank_rollout_statement(
        db,
        "SELECT 1 FROM rollout_actions WHERE plane_name = '' LIMIT 1;");
    if (blank_rollout_statement.StepRow()) {
      Statement plane_statement(
          db,
          "SELECT name FROM planes ORDER BY generation DESC, created_at DESC LIMIT 1;");
      if (plane_statement.StepRow()) {
        const std::string plane_name = ToColumnText(plane_statement.raw(), 0);
        if (!plane_name.empty()) {
          Statement backfill_statement(
              db,
              "UPDATE rollout_actions SET plane_name = ?1 WHERE plane_name = '';");
          backfill_statement.BindText(1, plane_name);
          backfill_statement.StepDone();
        }
      }
    }
  }
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
    {
      Statement statement(
          db,
          "DELETE FROM virtual_disks WHERE plane_name = ?1;");
      statement.BindText(1, state.plane_name);
      statement.StepDone();
    }
    {
      Statement statement(
          db,
          "DELETE FROM instances WHERE plane_name = ?1;");
      statement.BindText(1, state.plane_name);
      statement.StepDone();
    }
    {
      Statement statement(
          db,
          "DELETE FROM plane_nodes WHERE plane_name = ?1;");
      statement.BindText(1, state.plane_name);
      statement.StepDone();
    }

    {
      Statement statement(
          db,
          "INSERT INTO planes("
          "name, shared_disk_name, control_root, artifacts_root, plane_mode, bootstrap_model_json, "
          "desired_state_json, inference_config_json, gateway_config_json, runtime_gpu_nodes_json, generation, applied_generation, "
          "rebalance_iteration, state"
          ") VALUES(?1, ?2, ?3, '', ?4, ?5, ?6, ?7, ?8, ?9, ?10, "
          "COALESCE((SELECT applied_generation FROM planes WHERE name = ?1), 0), ?11, 'stopped') "
          "ON CONFLICT(name) DO UPDATE SET "
          "shared_disk_name = excluded.shared_disk_name, "
          "control_root = excluded.control_root, "
          "artifacts_root = planes.artifacts_root, "
          "plane_mode = excluded.plane_mode, "
          "bootstrap_model_json = excluded.bootstrap_model_json, "
          "desired_state_json = excluded.desired_state_json, "
          "inference_config_json = excluded.inference_config_json, "
          "gateway_config_json = excluded.gateway_config_json, "
          "runtime_gpu_nodes_json = excluded.runtime_gpu_nodes_json, "
          "generation = excluded.generation, "
          "rebalance_iteration = excluded.rebalance_iteration, "
          "state = CASE "
          "  WHEN planes.state = 'deleting' THEN planes.state "
          "  WHEN planes.state = 'running' THEN planes.state "
          "  ELSE excluded.state "
          "END;");
      statement.BindText(1, state.plane_name);
      statement.BindText(2, state.plane_shared_disk_name);
      statement.BindText(
          3,
          state.control_root.empty() ? "/comet/shared/control/" + state.plane_name
                                     : state.control_root);
      statement.BindText(4, ToString(state.plane_mode));
      statement.BindText(5, SerializeBootstrapModelSpec(state.bootstrap_model));
      statement.BindText(6, SerializeDesiredStateJson(state));
      statement.BindText(7, SerializeInferenceSettings(state.inference));
      statement.BindText(8, SerializeGatewaySettings(state.gateway));
      statement.BindText(9, SerializeRuntimeGpuNodes(state.runtime_gpu_nodes));
      statement.BindInt(10, generation);
      statement.BindInt(11, rebalance_iteration);
      statement.StepDone();
    }

    for (const auto& node : state.nodes) {
      Statement node_statement(
          db,
          "INSERT INTO nodes(name, platform, state) VALUES(?1, ?2, 'ready') "
          "ON CONFLICT(name) DO UPDATE SET "
          "platform = excluded.platform, "
          "state = excluded.state;");
      node_statement.BindText(1, node.name);
      node_statement.BindText(2, node.platform);
      node_statement.StepDone();

      Statement membership_statement(
          db,
          "INSERT INTO plane_nodes(plane_name, node_name) VALUES(?1, ?2);");
      membership_statement.BindText(1, state.plane_name);
      membership_statement.BindText(2, node.name);
      membership_statement.StepDone();

      {
        Statement clear_gpu_statement(
            db,
            "DELETE FROM node_gpus WHERE node_name = ?1;");
        clear_gpu_statement.BindText(1, node.name);
        clear_gpu_statement.StepDone();
      }

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
  Statement plane_statement(
      db,
      "SELECT name FROM planes ORDER BY created_at DESC, name ASC LIMIT 1;");
  if (!plane_statement.StepRow()) {
    return std::nullopt;
  }
  return LoadDesiredState(ToColumnText(plane_statement.raw(), 0));
}

std::optional<DesiredState> ControllerStore::LoadDesiredState(const std::string& plane_name) const {
  sqlite3* db = AsSqlite(db_);
  {
    Statement statement(
        db,
        "SELECT desired_state_json FROM planes WHERE name = ?1;");
    statement.BindText(1, plane_name);
    if (!statement.StepRow()) {
      return std::nullopt;
    }
    const auto desired_state_json = ToColumnText(statement.raw(), 0);
    if (!desired_state_json.empty()) {
      auto state = DeserializeDesiredStateJson(desired_state_json);
      if (state.control_root.empty()) {
        state.control_root = "/comet/shared/control/" + state.plane_name;
      }
      return state;
    }
  }

  DesiredState state;
  std::set<std::string> plane_node_names;

  {
    Statement statement(
        db,
        "SELECT name, shared_disk_name, control_root, plane_mode, bootstrap_model_json, interaction_settings_json, inference_config_json, "
        "gateway_config_json, runtime_gpu_nodes_json "
        "FROM planes WHERE name = ?1;");
    statement.BindText(1, plane_name);
    if (!statement.StepRow()) {
      return std::nullopt;
    }
    state.plane_name = ToColumnText(statement.raw(), 0);
    state.plane_shared_disk_name = ToColumnText(statement.raw(), 1);
    state.control_root = ToColumnText(statement.raw(), 2);
    state.plane_mode = ParsePlaneMode(ToColumnText(statement.raw(), 3));
    if (state.control_root.empty()) {
      state.control_root = "/comet/shared/control/" + state.plane_name;
    }
    state.bootstrap_model = DeserializeBootstrapModelSpec(ToColumnText(statement.raw(), 4));
    state.interaction = DeserializeInteractionSettings(ToColumnText(statement.raw(), 5));
    state.inference = DeserializeInferenceSettings(ToColumnText(statement.raw(), 6));
    state.gateway = DeserializeGatewaySettings(ToColumnText(statement.raw(), 7));
    state.runtime_gpu_nodes = DeserializeRuntimeGpuNodes(ToColumnText(statement.raw(), 8));
    for (const auto& runtime_node : state.runtime_gpu_nodes) {
      plane_node_names.insert(runtime_node.node_name);
    }
  }

  {
    Statement membership_statement(
        db,
        "SELECT node_name FROM plane_nodes WHERE plane_name = ?1 ORDER BY node_name ASC;");
    membership_statement.BindText(1, plane_name);
    while (membership_statement.StepRow()) {
      plane_node_names.insert(ToColumnText(membership_statement.raw(), 0));
    }
  }

  {
    Statement disk_statement(
        db,
        "SELECT node_name FROM virtual_disks WHERE plane_name = ?1;");
    disk_statement.BindText(1, plane_name);
    while (disk_statement.StepRow()) {
      plane_node_names.insert(ToColumnText(disk_statement.raw(), 0));
    }
    Statement instance_statement(
        db,
        "SELECT node_name FROM instances WHERE plane_name = ?1;");
    instance_statement.BindText(1, plane_name);
    while (instance_statement.StepRow()) {
      plane_node_names.insert(ToColumnText(instance_statement.raw(), 0));
    }
    if (!state.inference.primary_infer_node.empty()) {
      plane_node_names.insert(state.inference.primary_infer_node);
    }
  }

  {
    std::map<std::string, std::size_t> node_indexes;
    if (!plane_node_names.empty()) {
      std::string node_sql = "SELECT name, platform FROM nodes WHERE name IN (";
      for (std::size_t index = 0; index < plane_node_names.size(); ++index) {
        if (index > 0) {
          node_sql += ", ";
        }
        node_sql += "?" + std::to_string(index + 1);
      }
      node_sql += ") ORDER BY name ASC;";
      Statement node_statement(db, node_sql);
      int bind_index = 1;
      for (const auto& node_name : plane_node_names) {
        node_statement.BindText(bind_index++, node_name);
      }
      while (node_statement.StepRow()) {
        NodeInventory node;
        node.name = ToColumnText(node_statement.raw(), 0);
        node.platform = ToColumnText(node_statement.raw(), 1);
        node_indexes[node.name] = state.nodes.size();
        state.nodes.push_back(std::move(node));
      }

      std::string gpu_sql =
          "SELECT node_name, gpu_device, memory_mb "
          "FROM node_gpus WHERE node_name IN (";
      for (std::size_t index = 0; index < plane_node_names.size(); ++index) {
        if (index > 0) {
          gpu_sql += ", ";
        }
        gpu_sql += "?" + std::to_string(index + 1);
      }
      gpu_sql += ") ORDER BY node_name ASC, gpu_device ASC;";
      Statement gpu_statement(db, gpu_sql);
      bind_index = 1;
      for (const auto& node_name : plane_node_names) {
        gpu_statement.BindText(bind_index++, node_name);
      }
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
  }

  {
    Statement statement(
        db,
        "SELECT name, plane_name, owner_name, node_name, kind, host_path, container_path, size_gb "
        "FROM virtual_disks WHERE plane_name = ?1 ORDER BY name ASC;");
    statement.BindText(1, plane_name);
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
        "FROM instances WHERE plane_name = ?1 ORDER BY name ASC;");
    statement.BindText(1, plane_name);
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
        "SELECT d.instance_name, d.dependency_name "
        "FROM instance_dependencies d "
        "JOIN instances i ON i.name = d.instance_name "
        "WHERE i.plane_name = ?1 "
        "ORDER BY d.instance_name ASC, d.dependency_name ASC;");
    statement.BindText(1, plane_name);
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
        "SELECT e.instance_name, e.env_key, e.env_value "
        "FROM instance_environment e "
        "JOIN instances i ON i.name = e.instance_name "
        "WHERE i.plane_name = ?1 "
        "ORDER BY e.instance_name ASC, e.env_key ASC;");
    statement.BindText(1, plane_name);
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
        "SELECT l.instance_name, l.label_key, l.label_value "
        "FROM instance_labels l "
        "JOIN instances i ON i.name = l.instance_name "
        "WHERE i.plane_name = ?1 "
        "ORDER BY l.instance_name ASC, l.label_key ASC;");
    statement.BindText(1, plane_name);
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

std::vector<DesiredState> ControllerStore::LoadDesiredStates() const {
  std::vector<DesiredState> states;
  for (const auto& plane : LoadPlanes()) {
    if (auto state = LoadDesiredState(plane.name); state.has_value()) {
      states.push_back(std::move(*state));
    }
  }
  return states;
}

std::optional<int> ControllerStore::LoadDesiredGeneration() const {
  sqlite3* db = AsSqlite(db_);
  Statement plane_statement(
      db,
      "SELECT name FROM planes ORDER BY created_at DESC, name ASC LIMIT 1;");
  if (!plane_statement.StepRow()) {
    return std::nullopt;
  }
  return LoadDesiredGeneration(ToColumnText(plane_statement.raw(), 0));
}

std::optional<int> ControllerStore::LoadDesiredGeneration(const std::string& plane_name) const {
  sqlite3* db = AsSqlite(db_);
  Statement statement(
      db,
      "SELECT generation FROM planes WHERE name = ?1;");
  statement.BindText(1, plane_name);
  if (!statement.StepRow()) {
    return std::nullopt;
  }
  return sqlite3_column_int(statement.raw(), 0);
}

std::optional<int> ControllerStore::LoadRebalanceIteration() const {
  sqlite3* db = AsSqlite(db_);
  Statement plane_statement(
      db,
      "SELECT name FROM planes ORDER BY created_at DESC, name ASC LIMIT 1;");
  if (!plane_statement.StepRow()) {
    return std::nullopt;
  }
  return LoadRebalanceIteration(ToColumnText(plane_statement.raw(), 0));
}

std::optional<int> ControllerStore::LoadRebalanceIteration(
    const std::string& plane_name) const {
  sqlite3* db = AsSqlite(db_);
  Statement statement(
      db,
      "SELECT rebalance_iteration FROM planes WHERE name = ?1;");
  statement.BindText(1, plane_name);
  if (!statement.StepRow()) {
    return std::nullopt;
  }
  return sqlite3_column_int(statement.raw(), 0);
}

std::vector<PlaneRecord> ControllerStore::LoadPlanes() const {
  sqlite3* db = AsSqlite(db_);
  std::vector<PlaneRecord> planes;
  Statement statement(
      db,
      "SELECT name, shared_disk_name, control_root, artifacts_root, plane_mode, generation, applied_generation, "
      "rebalance_iteration, state, created_at FROM planes ORDER BY name ASC;");
  while (statement.StepRow()) {
    planes.push_back(PlaneFromStatement(statement.raw()));
  }
  return planes;
}

std::optional<PlaneRecord> ControllerStore::LoadPlane(const std::string& plane_name) const {
  sqlite3* db = AsSqlite(db_);
  Statement statement(
      db,
      "SELECT name, shared_disk_name, control_root, artifacts_root, plane_mode, generation, applied_generation, "
      "rebalance_iteration, state, created_at FROM planes WHERE name = ?1;");
  statement.BindText(1, plane_name);
  if (!statement.StepRow()) {
    return std::nullopt;
  }
  return PlaneFromStatement(statement.raw());
}

void ControllerStore::UpsertRegisteredHost(const RegisteredHostRecord& host) {
  sqlite3* db = AsSqlite(db_);
  Statement statement(
      db,
      "INSERT INTO registered_hosts("
      " node_name,"
      " advertised_address,"
      " public_key_base64,"
      " controller_public_key_fingerprint,"
      " transport_mode,"
      " registration_state,"
      " session_state,"
      " session_token,"
      " session_expires_at,"
      " session_host_sequence,"
      " session_controller_sequence,"
      " capabilities_json,"
      " status_message,"
      " last_session_at,"
      " last_heartbeat_at,"
      " updated_at"
      ") VALUES ("
      " ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, CURRENT_TIMESTAMP"
      ")"
      " ON CONFLICT(node_name) DO UPDATE SET"
      " advertised_address = excluded.advertised_address,"
      " public_key_base64 = excluded.public_key_base64,"
      " controller_public_key_fingerprint = excluded.controller_public_key_fingerprint,"
      " transport_mode = excluded.transport_mode,"
      " registration_state = excluded.registration_state,"
      " session_state = excluded.session_state,"
      " session_token = excluded.session_token,"
      " session_expires_at = excluded.session_expires_at,"
      " session_host_sequence = excluded.session_host_sequence,"
      " session_controller_sequence = excluded.session_controller_sequence,"
      " capabilities_json = excluded.capabilities_json,"
      " status_message = excluded.status_message,"
      " last_session_at = excluded.last_session_at,"
      " last_heartbeat_at = excluded.last_heartbeat_at,"
      " updated_at = CURRENT_TIMESTAMP;");
  statement.BindText(1, host.node_name);
  statement.BindText(2, host.advertised_address);
  statement.BindText(3, host.public_key_base64);
  statement.BindText(4, host.controller_public_key_fingerprint);
  statement.BindText(5, host.transport_mode);
  statement.BindText(6, host.registration_state);
  statement.BindText(7, host.session_state);
  statement.BindText(8, host.session_token);
  statement.BindText(9, host.session_expires_at);
  statement.BindInt(10, static_cast<int>(host.session_host_sequence));
  statement.BindInt(11, static_cast<int>(host.session_controller_sequence));
  statement.BindText(12, host.capabilities_json);
  statement.BindText(13, host.status_message);
  statement.BindText(14, host.last_session_at);
  statement.BindText(15, host.last_heartbeat_at);
  statement.StepDone();
}

std::optional<RegisteredHostRecord> ControllerStore::LoadRegisteredHost(
    const std::string& node_name) const {
  sqlite3* db = AsSqlite(db_);
  Statement statement(
      db,
      "SELECT node_name,"
      " advertised_address,"
      " public_key_base64,"
      " controller_public_key_fingerprint,"
      " transport_mode,"
      " registration_state,"
      " session_state,"
      " session_token,"
      " session_expires_at,"
      " session_host_sequence,"
      " session_controller_sequence,"
      " capabilities_json,"
      " status_message,"
      " last_session_at,"
      " last_heartbeat_at,"
      " created_at,"
      " updated_at"
      " FROM registered_hosts WHERE node_name = ?1;");
  statement.BindText(1, node_name);
  if (!statement.StepRow()) {
    return std::nullopt;
  }

  return RegisteredHostRecord{
      ToColumnText(statement.raw(), 0),
      ToColumnText(statement.raw(), 1),
      ToColumnText(statement.raw(), 2),
      ToColumnText(statement.raw(), 3),
      ToColumnText(statement.raw(), 4),
      ToColumnText(statement.raw(), 5),
      ToColumnText(statement.raw(), 6),
      ToColumnText(statement.raw(), 7),
      ToColumnText(statement.raw(), 8),
      sqlite3_column_int64(statement.raw(), 9),
      sqlite3_column_int64(statement.raw(), 10),
      ToColumnText(statement.raw(), 11),
      ToColumnText(statement.raw(), 12),
      ToColumnText(statement.raw(), 13),
      ToColumnText(statement.raw(), 14),
      ToColumnText(statement.raw(), 15),
      ToColumnText(statement.raw(), 16),
  };
}

std::vector<RegisteredHostRecord> ControllerStore::LoadRegisteredHosts(
    const std::optional<std::string>& node_name) const {
  sqlite3* db = AsSqlite(db_);
  std::string sql =
      "SELECT node_name,"
      " advertised_address,"
      " public_key_base64,"
      " controller_public_key_fingerprint,"
      " transport_mode,"
      " registration_state,"
      " session_state,"
      " session_token,"
      " session_expires_at,"
      " session_host_sequence,"
      " session_controller_sequence,"
      " capabilities_json,"
      " status_message,"
      " last_session_at,"
      " last_heartbeat_at,"
      " created_at,"
      " updated_at"
      " FROM registered_hosts";
  if (node_name.has_value()) {
    sql += " WHERE node_name = ?1";
  }
  sql += " ORDER BY node_name ASC;";
  Statement statement(db, sql);
  if (node_name.has_value()) {
    statement.BindText(1, *node_name);
  }

  std::vector<RegisteredHostRecord> hosts;
  while (statement.StepRow()) {
    hosts.push_back(RegisteredHostRecord{
        ToColumnText(statement.raw(), 0),
        ToColumnText(statement.raw(), 1),
        ToColumnText(statement.raw(), 2),
        ToColumnText(statement.raw(), 3),
        ToColumnText(statement.raw(), 4),
        ToColumnText(statement.raw(), 5),
        ToColumnText(statement.raw(), 6),
        ToColumnText(statement.raw(), 7),
        ToColumnText(statement.raw(), 8),
        sqlite3_column_int64(statement.raw(), 9),
        sqlite3_column_int64(statement.raw(), 10),
        ToColumnText(statement.raw(), 11),
        ToColumnText(statement.raw(), 12),
        ToColumnText(statement.raw(), 13),
        ToColumnText(statement.raw(), 14),
        ToColumnText(statement.raw(), 15),
        ToColumnText(statement.raw(), 16),
    });
  }
  return hosts;
}

bool ControllerStore::UpdatePlaneState(
    const std::string& plane_name,
    const std::string& state) {
  sqlite3* db = AsSqlite(db_);
  Statement statement(
      db,
      "UPDATE planes SET state = ?2 WHERE name = ?1;");
  statement.BindText(1, plane_name);
  statement.BindText(2, state);
  statement.StepDone();
  return sqlite3_changes(db) > 0;
}

bool ControllerStore::UpdatePlaneAppliedGeneration(
    const std::string& plane_name,
    int applied_generation) {
  sqlite3* db = AsSqlite(db_);
  Statement statement(
      db,
      "UPDATE planes SET applied_generation = ?2 WHERE name = ?1;");
  statement.BindText(1, plane_name);
  statement.BindInt(2, applied_generation);
  statement.StepDone();
  return sqlite3_changes(db) > 0;
}

bool ControllerStore::UpdatePlaneArtifactsRoot(
    const std::string& plane_name,
    const std::string& artifacts_root) {
  sqlite3* db = AsSqlite(db_);
  Statement statement(
      db,
      "UPDATE planes SET artifacts_root = ?2 WHERE name = ?1;");
  statement.BindText(1, plane_name);
  statement.BindText(2, artifacts_root);
  statement.StepDone();
  return sqlite3_changes(db) > 0;
}

void ControllerStore::DeletePlane(const std::string& plane_name) {
  sqlite3* db = AsSqlite(db_);
  Exec(db, "BEGIN IMMEDIATE TRANSACTION;");
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
      Statement statement(db, sql);
      statement.BindText(1, plane_name);
      statement.StepDone();
    }
    Exec(db, "COMMIT;");
  } catch (...) {
    Exec(db, "ROLLBACK;");
    throw;
  }
}

int ControllerStore::SupersedeHostAssignmentsForPlane(
    const std::string& plane_name,
    const std::string& status_message) {
  sqlite3* db = AsSqlite(db_);
  Statement statement(
      db,
      "UPDATE host_assignments "
      "SET status = 'superseded', status_message = ?2, updated_at = CURRENT_TIMESTAMP "
      "WHERE plane_name = ?1 AND status IN ('pending', 'claimed');");
  statement.BindText(1, plane_name);
  statement.BindText(2, status_message);
  statement.StepDone();
  return sqlite3_changes(db);
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

void ControllerStore::UpsertDiskRuntimeState(const DiskRuntimeState& runtime_state) {
  sqlite3* db = AsSqlite(db_);
  Statement statement(
      db,
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

std::optional<DiskRuntimeState> ControllerStore::LoadDiskRuntimeState(
    const std::string& disk_name,
    const std::string& node_name) const {
  sqlite3* db = AsSqlite(db_);
  Statement statement(
      db,
      "SELECT disk_name, plane_name, node_name, image_path, filesystem_type, loop_device, "
      "mount_point, runtime_state, attached_at, mounted_at, last_verified_at, "
      "status_message, updated_at "
      "FROM disk_runtime_state WHERE disk_name = ?1 AND node_name = ?2;");
  statement.BindText(1, disk_name);
  statement.BindText(2, node_name);
  if (!statement.StepRow()) {
    return std::nullopt;
  }
  return DiskRuntimeStateFromStatement(statement.raw());
}

std::vector<DiskRuntimeState> ControllerStore::LoadDiskRuntimeStates(
    const std::optional<std::string>& plane_name,
    const std::optional<std::string>& node_name) const {
  sqlite3* db = AsSqlite(db_);
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

  Statement statement(db, sql);
  bind_index = 1;
  if (plane_name.has_value()) {
    statement.BindText(bind_index++, *plane_name);
  }
  if (node_name.has_value()) {
    statement.BindText(bind_index++, *node_name);
  }
  while (statement.StepRow()) {
    runtime_states.push_back(DiskRuntimeStateFromStatement(statement.raw()));
  }
  return runtime_states;
}

void ControllerStore::ReplaceRolloutActions(
    const std::string& plane_name,
    int desired_generation,
    const std::vector<SchedulerRolloutAction>& actions) {
  sqlite3* db = AsSqlite(db_);
  Exec(db, "BEGIN IMMEDIATE TRANSACTION;");
  try {
    {
      Statement delete_statement(
          db,
          "DELETE FROM rollout_actions WHERE plane_name = ?1;");
      delete_statement.BindText(1, plane_name);
      delete_statement.StepDone();
    }

    for (const auto& action : actions) {
      Statement insert_statement(
          db,
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

    Exec(db, "COMMIT;");
  } catch (...) {
    Exec(db, "ROLLBACK;");
    throw;
  }
}

std::vector<RolloutActionRecord> ControllerStore::LoadRolloutActions(
    const std::optional<std::string>& plane_name,
    const std::optional<std::string>& target_node_name,
    const std::optional<RolloutActionStatus>& status) const {
  sqlite3* db = AsSqlite(db_);
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

  Statement statement(db, sql);
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

std::optional<HostObservation> ControllerStore::LoadHostObservation(
    const std::string& node_name) const {
  sqlite3* db = AsSqlite(db_);
  Statement statement(
      db,
      "SELECT node_name, plane_name, applied_generation, last_assignment_id, status, "
      "status_message, observed_state_json, runtime_status_json, "
      "instance_runtime_json, gpu_telemetry_json, disk_telemetry_json, network_telemetry_json, cpu_telemetry_json, heartbeat_at "
      "FROM host_observations WHERE node_name = ?1;");
  statement.BindText(1, node_name);
  if (!statement.StepRow()) {
    return std::nullopt;
  }
  return ObservationFromStatement(statement.raw());
}

std::vector<HostObservation> ControllerStore::LoadHostObservations(
    const std::optional<std::string>& node_name,
    const std::optional<std::string>& plane_name) const {
  sqlite3* db = AsSqlite(db_);
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

  Statement statement(db, sql);
  bind_index = 1;
  if (node_name.has_value()) {
    statement.BindText(bind_index++, *node_name);
  }
  if (plane_name.has_value()) {
    statement.BindText(bind_index++, *plane_name);
  }

  while (statement.StepRow()) {
    observations.push_back(ObservationFromStatement(statement.raw()));
  }

  return observations;
}

void ControllerStore::AppendEvent(const EventRecord& event) {
  sqlite3* db = AsSqlite(db_);
  Statement statement(
      db,
      "INSERT INTO event_log("
      "plane_name, node_name, worker_name, assignment_id, rollout_action_id, "
      "category, event_type, severity, message, payload_json"
      ") VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10);");
  statement.BindText(1, event.plane_name);
  statement.BindText(2, event.node_name);
  statement.BindText(3, event.worker_name);
  statement.BindOptionalInt(4, event.assignment_id);
  statement.BindOptionalInt(5, event.rollout_action_id);
  statement.BindText(6, event.category);
  statement.BindText(7, event.event_type);
  statement.BindText(8, event.severity);
  statement.BindText(9, event.message);
  statement.BindText(10, event.payload_json);
  statement.StepDone();
}

std::vector<EventRecord> ControllerStore::LoadEvents(
    const std::optional<std::string>& plane_name,
    const std::optional<std::string>& node_name,
    const std::optional<std::string>& worker_name,
    const std::optional<std::string>& category,
    int limit,
    const std::optional<int>& since_id,
    bool ascending) const {
  sqlite3* db = AsSqlite(db_);
  std::vector<std::string> clauses;
  if (plane_name.has_value()) {
    clauses.push_back("plane_name = ?" + std::to_string(clauses.size() + 1));
  }
  if (node_name.has_value()) {
    clauses.push_back("node_name = ?" + std::to_string(clauses.size() + 1));
  }
  if (worker_name.has_value()) {
    clauses.push_back("worker_name = ?" + std::to_string(clauses.size() + 1));
  }
  if (category.has_value()) {
    clauses.push_back("category = ?" + std::to_string(clauses.size() + 1));
  }
  if (since_id.has_value()) {
    clauses.push_back("id > ?" + std::to_string(clauses.size() + 1));
  }

  std::string sql =
      "SELECT id, plane_name, node_name, worker_name, assignment_id, rollout_action_id, "
      "category, event_type, severity, message, payload_json, created_at "
      "FROM event_log";
  if (!clauses.empty()) {
    sql += " WHERE ";
    for (std::size_t index = 0; index < clauses.size(); ++index) {
      if (index > 0) {
        sql += " AND ";
      }
      sql += clauses[index];
    }
  }
  sql += std::string(" ORDER BY id ") + (ascending ? "ASC" : "DESC") +
         " LIMIT ?" + std::to_string(clauses.size() + 1) + ";";

  Statement statement(db, sql);
  int bind_index = 1;
  if (plane_name.has_value()) {
    statement.BindText(bind_index++, *plane_name);
  }
  if (node_name.has_value()) {
    statement.BindText(bind_index++, *node_name);
  }
  if (worker_name.has_value()) {
    statement.BindText(bind_index++, *worker_name);
  }
  if (category.has_value()) {
    statement.BindText(bind_index++, *category);
  }
  if (since_id.has_value()) {
    statement.BindInt(bind_index++, *since_id);
  }
  statement.BindInt(bind_index, limit > 0 ? limit : 100);

  std::vector<EventRecord> events;
  while (statement.StepRow()) {
    events.push_back(EventFromStatement(statement.raw()));
  }
  return events;
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
          "assignment_type, desired_state_json, artifacts_root, status, status_message, progress_json, "
          "updated_at"
          ") VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, CURRENT_TIMESTAMP);");
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
      insert_statement.BindText(
          11,
          assignment.progress_json.empty() ? "{}" : assignment.progress_json);
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
      "assignment_type, desired_state_json, artifacts_root, status, status_message, progress_json "
      "FROM host_assignments WHERE id = ?1;");
  statement.BindInt(1, assignment_id);
  if (!statement.StepRow()) {
    return std::nullopt;
  }
  return AssignmentFromStatement(statement.raw());
}

std::vector<HostAssignment> ControllerStore::LoadHostAssignments(
    const std::optional<std::string>& node_name,
    const std::optional<HostAssignmentStatus>& status,
    const std::optional<std::string>& plane_name) const {
  sqlite3* db = AsSqlite(db_);
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

  Statement statement(db, sql);
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
          "assignment_type, desired_state_json, artifacts_root, status, status_message, progress_json "
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
          "progress_json = '{\"phase\":\"queued\",\"title\":\"Assignment claimed\",\"detail\":\"Hostd accepted the assignment and is starting work.\",\"percent\":5}', "
          "attempt_count = attempt_count + 1, "
          "updated_at = CURRENT_TIMESTAMP "
          "WHERE id = ?1 AND status = 'pending';");
      statement.BindInt(1, assignment->id);
      statement.StepDone();
    }

    Exec(db, "COMMIT;");
    assignment->status = HostAssignmentStatus::Claimed;
    assignment->attempt_count += 1;
    assignment->progress_json =
        "{\"phase\":\"queued\",\"title\":\"Assignment claimed\",\"detail\":"
        "\"Hostd accepted the assignment and is starting work.\",\"percent\":5}";
    return assignment;
  } catch (...) {
    Exec(db, "ROLLBACK;");
    throw;
  }
}

bool ControllerStore::UpdateHostAssignmentProgress(
    int assignment_id,
    const std::string& progress_json) {
  sqlite3* db = AsSqlite(db_);
  Statement statement(
      db,
      "UPDATE host_assignments "
      "SET progress_json = ?2, updated_at = CURRENT_TIMESTAMP "
      "WHERE id = ?1 AND status = 'claimed';");
  statement.BindInt(1, assignment_id);
  statement.BindText(2, progress_json.empty() ? "{}" : progress_json);
  statement.StepDone();
  return sqlite3_changes(db) > 0;
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
