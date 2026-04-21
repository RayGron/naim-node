#include "naim/state/desired_state_repository.h"

#include <map>
#include <set>
#include <stdexcept>
#include <string>

#include "naim/state/desired_state_sqlite_codec.h"
#include "naim/state/plane_repository.h"
#include "naim/state/sqlite_statement.h"
#include "naim/state/sqlite_store_support.h"
#include "naim/state/state_json.h"

namespace naim {

namespace {

using Statement = SqliteStatement;
using sqlite_store_support::Exec;
using sqlite_store_support::ToColumnText;
using sqlite_store_support::ToOptionalColumnInt;

void EnsureControlRoot(DesiredState* state) {
  if (state != nullptr && state->control_root.empty()) {
    state->control_root = "/naim/shared/control/" + state->plane_name;
  }
}

std::optional<std::string> LoadLatestPlaneName(sqlite3* db) {
  Statement statement(
      db,
      "SELECT name FROM planes ORDER BY created_at DESC, name ASC LIMIT 1;");
  if (!statement.StepRow()) {
    return std::nullopt;
  }
  return ToColumnText(statement.raw(), 0);
}

std::optional<DesiredState> LoadDesiredStateFromJson(
    sqlite3* db,
    const std::string& plane_name) {
  Statement statement(
      db,
      "SELECT desired_state_json FROM planes WHERE name = ?1;");
  statement.BindText(1, plane_name);
  if (!statement.StepRow()) {
    return std::nullopt;
  }
  const auto desired_state_json = ToColumnText(statement.raw(), 0);
  if (desired_state_json.empty()) {
    return std::nullopt;
  }
  auto state = DeserializeDesiredStateJson(desired_state_json);
  EnsureControlRoot(&state);
  return state;
}

DesiredState LoadPlaneBaseState(sqlite3* db, const std::string& plane_name) {
  Statement statement(
      db,
      "SELECT name, shared_disk_name, control_root, plane_mode, bootstrap_model_json, "
      "interaction_settings_json, inference_config_json, gateway_config_json, "
      "runtime_gpu_nodes_json "
      "FROM planes WHERE name = ?1;");
  statement.BindText(1, plane_name);
  if (!statement.StepRow()) {
    throw std::runtime_error("plane row not found for '" + plane_name + "'");
  }

  DesiredState state;
  state.plane_name = ToColumnText(statement.raw(), 0);
  state.plane_shared_disk_name = ToColumnText(statement.raw(), 1);
  state.control_root = ToColumnText(statement.raw(), 2);
  state.plane_mode = ParsePlaneMode(ToColumnText(statement.raw(), 3));
  state.bootstrap_model =
      DesiredStateSqliteCodec::DeserializeBootstrapModelSpec(ToColumnText(statement.raw(), 4));
  state.interaction =
      DesiredStateSqliteCodec::DeserializeInteractionSettings(ToColumnText(statement.raw(), 5));
  state.inference =
      DesiredStateSqliteCodec::DeserializeInferenceSettings(ToColumnText(statement.raw(), 6));
  state.gateway =
      DesiredStateSqliteCodec::DeserializeGatewaySettings(ToColumnText(statement.raw(), 7));
  state.runtime_gpu_nodes =
      DesiredStateSqliteCodec::DeserializeRuntimeGpuNodes(ToColumnText(statement.raw(), 8));
  EnsureControlRoot(&state);
  return state;
}

std::set<std::string> CollectPlaneNodeNames(
    sqlite3* db,
    const std::string& plane_name,
    const DesiredState& state) {
  std::set<std::string> plane_node_names;

  for (const auto& runtime_node : state.runtime_gpu_nodes) {
    plane_node_names.insert(runtime_node.node_name);
  }

  Statement membership_statement(
      db,
      "SELECT node_name FROM plane_nodes WHERE plane_name = ?1 ORDER BY node_name ASC;");
  membership_statement.BindText(1, plane_name);
  while (membership_statement.StepRow()) {
    plane_node_names.insert(ToColumnText(membership_statement.raw(), 0));
  }

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

  return plane_node_names;
}

void LoadPlaneNodes(
    sqlite3* db,
    const std::set<std::string>& plane_node_names,
    DesiredState* state) {
  if (state == nullptr || plane_node_names.empty()) {
    return;
  }

  std::map<std::string, std::size_t> node_indexes;
  std::string node_sql = "SELECT name, platform, execution_mode FROM nodes WHERE name IN (";
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
    node.execution_mode = ParseHostExecutionMode(ToColumnText(node_statement.raw(), 2));
    node_indexes[node.name] = state->nodes.size();
    state->nodes.push_back(std::move(node));
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
    state->nodes[node_it->second].gpu_devices.push_back(gpu_device);
    const int memory_mb = sqlite3_column_int(gpu_statement.raw(), 2);
    if (memory_mb > 0) {
      state->nodes[node_it->second].gpu_memory_mb[gpu_device] = memory_mb;
    }
  }
}

void LoadPlaneDisks(sqlite3* db, const std::string& plane_name, DesiredState* state) {
  if (state == nullptr) {
    return;
  }

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
    disk.kind = DesiredStateSqliteCodec::ParseDiskKind(ToColumnText(statement.raw(), 4));
    disk.host_path = ToColumnText(statement.raw(), 5);
    disk.container_path = ToColumnText(statement.raw(), 6);
    disk.size_gb = sqlite3_column_int(statement.raw(), 7);
    state->disks.push_back(std::move(disk));
  }
}

std::map<std::string, std::size_t> LoadPlaneInstances(
    sqlite3* db,
    const std::string& plane_name,
    DesiredState* state) {
  std::map<std::string, std::size_t> instance_indexes;
  if (state == nullptr) {
    return instance_indexes;
  }

  Statement statement(
      db,
      "SELECT name, plane_name, node_name, role, image, command, private_disk_name, "
      "shared_disk_name, gpu_device, placement_mode, share_mode, gpu_fraction, priority, "
      "preemptible, memory_cap_mb, private_disk_size_gb "
      "FROM instances WHERE plane_name = ?1 ORDER BY name ASC;");
  statement.BindText(1, plane_name);
  while (statement.StepRow()) {
    InstanceSpec instance;
    instance.name = ToColumnText(statement.raw(), 0);
    instance.plane_name = ToColumnText(statement.raw(), 1);
    instance.node_name = ToColumnText(statement.raw(), 2);
    instance.role = DesiredStateSqliteCodec::ParseInstanceRole(ToColumnText(statement.raw(), 3));
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
    instance_indexes[instance.name] = state->instances.size();
    state->instances.push_back(std::move(instance));
  }

  return instance_indexes;
}

void AppendDependencyRows(
    sqlite3* db,
    const std::string& plane_name,
    const std::map<std::string, std::size_t>& instance_indexes,
    DesiredState* state) {
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
    state->instances[instance_it->second].depends_on.push_back(ToColumnText(statement.raw(), 1));
  }
}

void AppendEnvironmentRows(
    sqlite3* db,
    const std::string& plane_name,
    const std::map<std::string, std::size_t>& instance_indexes,
    DesiredState* state) {
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
    state->instances[instance_it->second].environment[ToColumnText(statement.raw(), 1)] =
        ToColumnText(statement.raw(), 2);
  }
}

void AppendLabelRows(
    sqlite3* db,
    const std::string& plane_name,
    const std::map<std::string, std::size_t>& instance_indexes,
    DesiredState* state) {
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
    state->instances[instance_it->second].labels[ToColumnText(statement.raw(), 1)] =
        ToColumnText(statement.raw(), 2);
  }
}

void AppendPublishedPortRows(
    sqlite3* db,
    const std::string& plane_name,
    const std::map<std::string, std::size_t>& instance_indexes,
    DesiredState* state) {
  Statement statement(
      db,
      "SELECT p.instance_name, p.host_ip, p.host_port, p.container_port "
      "FROM instance_published_ports p "
      "JOIN instances i ON i.name = p.instance_name "
      "WHERE i.plane_name = ?1 "
      "ORDER BY p.instance_name ASC, p.host_ip ASC, p.host_port ASC, container_port ASC;");
  statement.BindText(1, plane_name);
  while (statement.StepRow()) {
    const std::string instance_name = ToColumnText(statement.raw(), 0);
    const auto instance_it = instance_indexes.find(instance_name);
    if (instance_it == instance_indexes.end()) {
      throw std::runtime_error(
          "published port row references unknown instance '" + instance_name + "'");
    }
    PublishedPort port;
    port.host_ip = ToColumnText(statement.raw(), 1);
    port.host_port = sqlite3_column_int(statement.raw(), 2);
    port.container_port = sqlite3_column_int(statement.raw(), 3);
    state->instances[instance_it->second].published_ports.push_back(std::move(port));
  }
}

void EnsurePrimaryInferNode(DesiredState* state) {
  if (state == nullptr || !state->inference.primary_infer_node.empty()) {
    return;
  }
  for (const auto& instance : state->instances) {
    if (instance.role == InstanceRole::Infer) {
      state->inference.primary_infer_node = instance.node_name;
      return;
    }
  }
}

}  // namespace

DesiredStateRepository::DesiredStateRepository(sqlite3* db) : db_(db) {}

void DesiredStateRepository::ReplaceDesiredState(
    const DesiredState& state,
    int generation,
    int rebalance_iteration) {
  Exec(db_, "BEGIN IMMEDIATE TRANSACTION;");

  try {
    {
      Statement statement(
          db_,
          "DELETE FROM virtual_disks WHERE plane_name = ?1;");
      statement.BindText(1, state.plane_name);
      statement.StepDone();
    }
    {
      Statement statement(
          db_,
          "DELETE FROM instances WHERE plane_name = ?1;");
      statement.BindText(1, state.plane_name);
      statement.StepDone();
    }
    {
      Statement statement(
          db_,
          "DELETE FROM plane_nodes WHERE plane_name = ?1;");
      statement.BindText(1, state.plane_name);
      statement.StepDone();
    }

    {
      Statement statement(
          db_,
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
          state.control_root.empty() ? "/naim/shared/control/" + state.plane_name
                                     : state.control_root);
      statement.BindText(4, ToString(state.plane_mode));
      statement.BindText(
          5,
          DesiredStateSqliteCodec::SerializeBootstrapModelSpec(state.bootstrap_model));
      statement.BindText(6, SerializeDesiredStateV2Json(state));
      statement.BindText(7, DesiredStateSqliteCodec::SerializeInferenceSettings(state.inference));
      statement.BindText(8, DesiredStateSqliteCodec::SerializeGatewaySettings(state.gateway));
      statement.BindText(
          9,
          DesiredStateSqliteCodec::SerializeRuntimeGpuNodes(state.runtime_gpu_nodes));
      statement.BindInt(10, generation);
      statement.BindInt(11, rebalance_iteration);
      statement.StepDone();
    }

    for (const auto& node : state.nodes) {
      Statement node_statement(
          db_,
          "INSERT INTO nodes(name, platform, execution_mode, state) VALUES(?1, ?2, ?3, 'ready') "
          "ON CONFLICT(name) DO UPDATE SET "
          "platform = excluded.platform, "
          "execution_mode = excluded.execution_mode, "
          "state = excluded.state;");
      node_statement.BindText(1, node.name);
      node_statement.BindText(2, node.platform);
      node_statement.BindText(3, ToString(node.execution_mode));
      node_statement.StepDone();

      Statement membership_statement(
          db_,
          "INSERT INTO plane_nodes(plane_name, node_name) VALUES(?1, ?2);");
      membership_statement.BindText(1, state.plane_name);
      membership_statement.BindText(2, node.name);
      membership_statement.StepDone();

      Statement clear_gpu_statement(
          db_,
          "DELETE FROM node_gpus WHERE node_name = ?1;");
      clear_gpu_statement.BindText(1, node.name);
      clear_gpu_statement.StepDone();

      for (const auto& gpu_device : EffectiveNodeGpuDevices(node)) {
        Statement gpu_statement(
            db_,
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
          db_,
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
          db_,
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
            db_,
            "INSERT INTO instance_dependencies(instance_name, dependency_name) "
            "VALUES(?1, ?2);");
        dependency_statement.BindText(1, instance.name);
        dependency_statement.BindText(2, dependency);
        dependency_statement.StepDone();
      }

      for (const auto& [key, value] : instance.environment) {
        Statement env_statement(
            db_,
            "INSERT INTO instance_environment(instance_name, env_key, env_value) "
            "VALUES(?1, ?2, ?3);");
        env_statement.BindText(1, instance.name);
        env_statement.BindText(2, key);
        env_statement.BindText(3, value);
        env_statement.StepDone();
      }

      for (const auto& [key, value] : instance.labels) {
        Statement label_statement(
            db_,
            "INSERT INTO instance_labels(instance_name, label_key, label_value) "
            "VALUES(?1, ?2, ?3);");
        label_statement.BindText(1, instance.name);
        label_statement.BindText(2, key);
        label_statement.BindText(3, value);
        label_statement.StepDone();
      }

      for (const auto& port : instance.published_ports) {
        Statement port_statement(
            db_,
            "INSERT INTO instance_published_ports(instance_name, host_ip, host_port, "
            "container_port) VALUES(?1, ?2, ?3, ?4);");
        port_statement.BindText(1, instance.name);
        port_statement.BindText(2, port.host_ip);
        port_statement.BindInt(3, port.host_port);
        port_statement.BindInt(4, port.container_port);
        port_statement.StepDone();
      }
    }

    Exec(db_, "COMMIT;");
  } catch (...) {
    Exec(db_, "ROLLBACK;");
    throw;
  }
}

std::optional<DesiredState> DesiredStateRepository::LoadDesiredState() const {
  const auto plane_name = LoadLatestPlaneName(db_);
  if (!plane_name.has_value()) {
    return std::nullopt;
  }
  return LoadDesiredState(*plane_name);
}

std::optional<DesiredState> DesiredStateRepository::LoadDesiredState(
    const std::string& plane_name) const {
  if (auto state = LoadDesiredStateFromJson(db_, plane_name); state.has_value()) {
    return state;
  }

  Statement plane_exists_statement(
      db_,
      "SELECT 1 FROM planes WHERE name = ?1;");
  plane_exists_statement.BindText(1, plane_name);
  if (!plane_exists_statement.StepRow()) {
    return std::nullopt;
  }

  DesiredState state = LoadPlaneBaseState(db_, plane_name);
  const auto plane_node_names = CollectPlaneNodeNames(db_, plane_name, state);
  LoadPlaneNodes(db_, plane_node_names, &state);
  LoadPlaneDisks(db_, plane_name, &state);
  const auto instance_indexes = LoadPlaneInstances(db_, plane_name, &state);
  AppendDependencyRows(db_, plane_name, instance_indexes, &state);
  AppendEnvironmentRows(db_, plane_name, instance_indexes, &state);
  AppendLabelRows(db_, plane_name, instance_indexes, &state);
  AppendPublishedPortRows(db_, plane_name, instance_indexes, &state);
  EnsurePrimaryInferNode(&state);
  return state;
}

std::vector<DesiredState> DesiredStateRepository::LoadDesiredStates() const {
  std::vector<DesiredState> states;
  for (const auto& plane : PlaneRepository(db_).LoadPlanes()) {
    if (auto state = LoadDesiredState(plane.name); state.has_value()) {
      states.push_back(std::move(*state));
    }
  }
  return states;
}

std::optional<int> DesiredStateRepository::LoadDesiredGeneration() const {
  const auto plane_name = LoadLatestPlaneName(db_);
  if (!plane_name.has_value()) {
    return std::nullopt;
  }
  return LoadDesiredGeneration(*plane_name);
}

std::optional<int> DesiredStateRepository::LoadDesiredGeneration(
    const std::string& plane_name) const {
  Statement statement(
      db_,
      "SELECT generation FROM planes WHERE name = ?1;");
  statement.BindText(1, plane_name);
  if (!statement.StepRow()) {
    return std::nullopt;
  }
  return sqlite3_column_int(statement.raw(), 0);
}

std::optional<int> DesiredStateRepository::LoadRebalanceIteration() const {
  const auto plane_name = LoadLatestPlaneName(db_);
  if (!plane_name.has_value()) {
    return std::nullopt;
  }
  return LoadRebalanceIteration(*plane_name);
}

std::optional<int> DesiredStateRepository::LoadRebalanceIteration(
    const std::string& plane_name) const {
  return PlaneRepository(db_).LoadPlaneRebalanceIteration(plane_name);
}

}  // namespace naim
