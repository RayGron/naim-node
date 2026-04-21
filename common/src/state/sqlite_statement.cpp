#include "naim/state/sqlite_statement.h"

#include <stdexcept>

namespace naim {

namespace {

void ThrowSqliteError(sqlite3* db, const std::string& action) {
  throw std::runtime_error(action + ": " + sqlite3_errmsg(db));
}

}  // namespace

SqliteStatement::SqliteStatement(sqlite3* db, const std::string& sql) : db_(db) {
  if (sqlite3_prepare_v2(db_, sql.c_str(), -1, &statement_, nullptr) != SQLITE_OK) {
    ThrowSqliteError(db_, "sqlite prepare failed");
  }
}

SqliteStatement::~SqliteStatement() {
  if (statement_ != nullptr) {
    sqlite3_finalize(statement_);
  }
}

void SqliteStatement::BindText(int index, const std::string& value) {
  if (sqlite3_bind_text(statement_, index, value.c_str(), -1, SQLITE_TRANSIENT) != SQLITE_OK) {
    ThrowSqliteError(db_, "sqlite bind text failed");
  }
}

void SqliteStatement::BindInt(int index, int value) {
  if (sqlite3_bind_int(statement_, index, value) != SQLITE_OK) {
    ThrowSqliteError(db_, "sqlite bind int failed");
  }
}

void SqliteStatement::BindInt64(int index, std::int64_t value) {
  if (sqlite3_bind_int64(statement_, index, value) != SQLITE_OK) {
    ThrowSqliteError(db_, "sqlite bind int64 failed");
  }
}

void SqliteStatement::BindDouble(int index, double value) {
  if (sqlite3_bind_double(statement_, index, value) != SQLITE_OK) {
    ThrowSqliteError(db_, "sqlite bind double failed");
  }
}

void SqliteStatement::BindOptionalInt(int index, const std::optional<int>& value) {
  const int rc =
      value.has_value() ? sqlite3_bind_int(statement_, index, *value)
                        : sqlite3_bind_null(statement_, index);
  if (rc != SQLITE_OK) {
    ThrowSqliteError(db_, "sqlite bind optional int failed");
  }
}

void SqliteStatement::BindOptionalInt64(
    int index,
    const std::optional<std::int64_t>& value) {
  const int rc =
      value.has_value() ? sqlite3_bind_int64(statement_, index, *value)
                        : sqlite3_bind_null(statement_, index);
  if (rc != SQLITE_OK) {
    ThrowSqliteError(db_, "sqlite bind optional int64 failed");
  }
}

void SqliteStatement::BindOptionalText(
    int index,
    const std::optional<std::string>& value) {
  const int rc = value.has_value()
                     ? sqlite3_bind_text(statement_, index, value->c_str(), -1, SQLITE_TRANSIENT)
                     : sqlite3_bind_null(statement_, index);
  if (rc != SQLITE_OK) {
    ThrowSqliteError(db_, "sqlite bind optional text failed");
  }
}

bool SqliteStatement::StepRow() {
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

void SqliteStatement::StepDone() {
  if (sqlite3_step(statement_) != SQLITE_DONE) {
    ThrowSqliteError(db_, "sqlite step done failed");
  }
}

sqlite3_stmt* SqliteStatement::raw() const {
  return statement_;
}

}  // namespace naim
