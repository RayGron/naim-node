#pragma once

#include <cstdint>
#include <optional>
#include <string>

#include <sqlite3.h>

namespace naim {

class SqliteStatement final {
 public:
  SqliteStatement(sqlite3* db, const std::string& sql);
  ~SqliteStatement();

  SqliteStatement(const SqliteStatement&) = delete;
  SqliteStatement& operator=(const SqliteStatement&) = delete;

  void BindText(int index, const std::string& value);
  void BindInt(int index, int value);
  void BindInt64(int index, std::int64_t value);
  void BindDouble(int index, double value);
  void BindOptionalInt(int index, const std::optional<int>& value);
  void BindOptionalInt64(int index, const std::optional<std::int64_t>& value);
  void BindOptionalText(int index, const std::optional<std::string>& value);

  bool StepRow();
  void StepDone();

  sqlite3_stmt* raw() const;

 private:
  sqlite3* db_ = nullptr;
  sqlite3_stmt* statement_ = nullptr;
};

}  // namespace naim
