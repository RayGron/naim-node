#include "naim/state/sqlite_store_support.h"

#include <stdexcept>

#include "naim/state/sqlite_statement.h"

namespace naim::sqlite_store_support {

namespace {

using Statement = SqliteStatement;

}  // namespace

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

void Exec(sqlite3* db, const std::string& sql) {
  char* error_message = nullptr;
  const int rc = sqlite3_exec(db, sql.c_str(), nullptr, nullptr, &error_message);
  if (rc != SQLITE_OK) {
    const std::string message = error_message == nullptr ? "unknown sqlite error" : error_message;
    sqlite3_free(error_message);
    throw std::runtime_error("sqlite exec failed: " + message);
  }
}

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

sqlite3* AsSqlite(void* db) {
  return static_cast<sqlite3*>(db);
}

NodeAvailabilityOverride AvailabilityOverrideFromStatement(sqlite3_stmt* statement) {
  NodeAvailabilityOverride availability_override;
  availability_override.node_name = ToColumnText(statement, 0);
  availability_override.availability = ParseNodeAvailability(ToColumnText(statement, 1));
  availability_override.status_message = ToColumnText(statement, 2);
  availability_override.updated_at = ToColumnText(statement, 3);
  return availability_override;
}

}  // namespace naim::sqlite_store_support
