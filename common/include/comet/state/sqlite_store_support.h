#pragma once

#include <optional>
#include <string>

#include <sqlite3.h>

#include "comet/state/sqlite_store.h"

namespace comet::sqlite_store_support {

std::string ToColumnText(sqlite3_stmt* statement, int column_index);
std::optional<int> ToOptionalColumnInt(sqlite3_stmt* statement, int column_index);
void Exec(sqlite3* db, const std::string& sql);
bool TableHasColumn(sqlite3* db, const std::string& table_name, const std::string& column_name);
void EnsureColumn(
    sqlite3* db,
    const std::string& table_name,
    const std::string& column_name,
    const std::string& definition_sql);
sqlite3* AsSqlite(void* db);
NodeAvailabilityOverride AvailabilityOverrideFromStatement(sqlite3_stmt* statement);

}  // namespace comet::sqlite_store_support
