#pragma once

#include <optional>
#include <string>

#include <sqlite3.h>

namespace naim {

class ControllerSettingsRepository final {
 public:
  explicit ControllerSettingsRepository(sqlite3* db);

  std::optional<std::string> LoadSetting(const std::string& setting_key) const;
  void UpsertSetting(const std::string& setting_key, const std::string& setting_value);
  bool DeleteSetting(const std::string& setting_key);

 private:
  sqlite3* db_ = nullptr;
};

}  // namespace naim
