#include "knowledge/knowledge_vault_service_repository.h"

#include <stdexcept>

#include "naim/state/sqlite_statement.h"
#include "naim/state/sqlite_store.h"

namespace naim::controller {

std::optional<KnowledgeVaultServiceRecord> KnowledgeVaultServiceRepository::LoadService(
    const std::string& db_path,
    const std::string& service_id) const {
  EnsureControllerSchema(db_path);
  sqlite3* db = nullptr;
  if (sqlite3_open(db_path.c_str(), &db) != SQLITE_OK) {
    return std::nullopt;
  }

  KnowledgeVaultServiceRecord record;
  bool found = false;
  {
    naim::SqliteStatement statement(
        db,
        "SELECT service_id, node_name, image, endpoint_host, endpoint_port, desired_state_json, "
        "status, status_message, schema_version, index_epoch, latest_event_sequence "
        "FROM knowledge_vault_services WHERE service_id = ?1;");
    statement.BindText(1, service_id);
    if (statement.StepRow()) {
      found = true;
      record.service_id = ToText(statement.raw(), 0);
      record.node_name = ToText(statement.raw(), 1);
      record.image = ToText(statement.raw(), 2);
      record.endpoint_host = ToText(statement.raw(), 3);
      record.endpoint_port = sqlite3_column_int(statement.raw(), 4);
      record.desired_state_json = ToText(statement.raw(), 5);
      record.status = ToText(statement.raw(), 6);
      record.status_message = ToText(statement.raw(), 7);
      record.schema_version = ToText(statement.raw(), 8);
      record.index_epoch = ToText(statement.raw(), 9);
      record.latest_event_sequence = sqlite3_column_int(statement.raw(), 10);
    }
  }
  sqlite3_close(db);
  if (!found) {
    return std::nullopt;
  }
  return record;
}

std::vector<KnowledgeVaultServiceRecord> KnowledgeVaultServiceRepository::LoadServices(
    const std::string& db_path) const {
  EnsureControllerSchema(db_path);
  sqlite3* db = nullptr;
  if (sqlite3_open(db_path.c_str(), &db) != SQLITE_OK) {
    return {};
  }

  std::vector<KnowledgeVaultServiceRecord> records;
  {
    naim::SqliteStatement statement(
        db,
        "SELECT service_id, node_name, image, endpoint_host, endpoint_port, desired_state_json, "
        "status, status_message, schema_version, index_epoch, latest_event_sequence "
        "FROM knowledge_vault_services ORDER BY service_id;");
    while (statement.StepRow()) {
      KnowledgeVaultServiceRecord record;
      record.service_id = ToText(statement.raw(), 0);
      record.node_name = ToText(statement.raw(), 1);
      record.image = ToText(statement.raw(), 2);
      record.endpoint_host = ToText(statement.raw(), 3);
      record.endpoint_port = sqlite3_column_int(statement.raw(), 4);
      record.desired_state_json = ToText(statement.raw(), 5);
      record.status = ToText(statement.raw(), 6);
      record.status_message = ToText(statement.raw(), 7);
      record.schema_version = ToText(statement.raw(), 8);
      record.index_epoch = ToText(statement.raw(), 9);
      record.latest_event_sequence = sqlite3_column_int(statement.raw(), 10);
      records.push_back(std::move(record));
    }
  }
  sqlite3_close(db);
  return records;
}

void KnowledgeVaultServiceRepository::UpsertService(
    const std::string& db_path,
    const KnowledgeVaultServiceRecord& record) const {
  EnsureControllerSchema(db_path);
  sqlite3* db = nullptr;
  if (sqlite3_open(db_path.c_str(), &db) != SQLITE_OK) {
    throw std::runtime_error("failed to open controller db");
  }
  {
    naim::SqliteStatement statement(
        db,
        "INSERT INTO knowledge_vault_services(service_id, node_name, image, endpoint_host, "
        "endpoint_port, desired_state_json, status, status_message, schema_version, index_epoch, "
        "latest_event_sequence, updated_at) "
        "VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, CURRENT_TIMESTAMP) "
        "ON CONFLICT(service_id) DO UPDATE SET node_name=excluded.node_name, "
        "image=excluded.image, endpoint_host=excluded.endpoint_host, "
        "endpoint_port=excluded.endpoint_port, desired_state_json=excluded.desired_state_json, "
        "status=excluded.status, status_message=excluded.status_message, "
        "schema_version=excluded.schema_version, index_epoch=excluded.index_epoch, "
        "latest_event_sequence=excluded.latest_event_sequence, updated_at=CURRENT_TIMESTAMP;");
    statement.BindText(1, record.service_id);
    statement.BindText(2, record.node_name);
    statement.BindText(3, record.image);
    statement.BindText(4, record.endpoint_host);
    statement.BindInt(5, record.endpoint_port);
    statement.BindText(6, record.desired_state_json);
    statement.BindText(7, record.status);
    statement.BindText(8, record.status_message);
    statement.BindText(9, record.schema_version);
    statement.BindText(10, record.index_epoch);
    statement.BindInt(11, record.latest_event_sequence);
    statement.StepDone();
  }
  sqlite3_close(db);
}

void KnowledgeVaultServiceRepository::UpdateServiceStatus(
    const std::string& db_path,
    const std::string& service_id,
    const nlohmann::json& status) const {
  EnsureControllerSchema(db_path);
  sqlite3* db = nullptr;
  if (sqlite3_open(db_path.c_str(), &db) != SQLITE_OK) {
    return;
  }
  {
    naim::SqliteStatement statement(
        db,
        "UPDATE knowledge_vault_services SET status=?2, schema_version=?3, index_epoch=?4, "
        "latest_event_sequence=?5, updated_at=CURRENT_TIMESTAMP WHERE service_id=?1;");
    statement.BindText(1, service_id);
    statement.BindText(2, status.value("status", std::string("ready")));
    statement.BindText(3, status.value("schema_version", std::string{}));
    statement.BindText(4, status.value("index_epoch", std::string{}));
    statement.BindInt(5, status.value("latest_event_sequence", 0));
    statement.StepDone();
  }
  sqlite3_close(db);
}

std::string KnowledgeVaultServiceRepository::ToText(sqlite3_stmt* statement, int column) {
  const auto* text = sqlite3_column_text(statement, column);
  return text == nullptr ? std::string{} : reinterpret_cast<const char*>(text);
}

void KnowledgeVaultServiceRepository::EnsureControllerSchema(const std::string& db_path) {
  naim::ControllerStore store(db_path);
  store.Initialize();
}

}  // namespace naim::controller
