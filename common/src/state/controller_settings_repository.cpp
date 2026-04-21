#include "naim/state/controller_settings_repository.h"

#include "naim/state/sqlite_statement.h"

namespace naim {

namespace {

using Statement = SqliteStatement;

std::string ToColumnText(sqlite3_stmt* statement, int column_index) {
  const unsigned char* text = sqlite3_column_text(statement, column_index);
  if (text == nullptr) {
    return "";
  }
  return reinterpret_cast<const char*>(text);
}

}  // namespace

ControllerSettingsRepository::ControllerSettingsRepository(sqlite3* db) : db_(db) {}

std::optional<std::string> ControllerSettingsRepository::LoadSetting(
    const std::string& setting_key) const {
  Statement statement(
      db_,
      "SELECT setting_value FROM controller_settings WHERE setting_key = ?1;");
  statement.BindText(1, setting_key);
  if (!statement.StepRow()) {
    return std::nullopt;
  }
  return ToColumnText(statement.raw(), 0);
}

void ControllerSettingsRepository::UpsertSetting(
    const std::string& setting_key,
    const std::string& setting_value) {
  Statement statement(
      db_,
      "INSERT INTO controller_settings(setting_key, setting_value, updated_at) "
      "VALUES(?1, ?2, CURRENT_TIMESTAMP) "
      "ON CONFLICT(setting_key) DO UPDATE SET "
      "setting_value = excluded.setting_value, "
      "updated_at = CURRENT_TIMESTAMP;");
  statement.BindText(1, setting_key);
  statement.BindText(2, setting_value);
  statement.StepDone();
}

bool ControllerSettingsRepository::DeleteSetting(const std::string& setting_key) {
  Statement statement(
      db_,
      "DELETE FROM controller_settings WHERE setting_key = ?1;");
  statement.BindText(1, setting_key);
  statement.StepDone();
  return sqlite3_changes(db_) == 1;
}

}  // namespace naim
