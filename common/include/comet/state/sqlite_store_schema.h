#pragma once

#include <functional>
#include <optional>
#include <string>
#include <string_view>

#include <sqlite3.h>

#include "comet/state/models.h"

namespace comet::sqlite_store_schema {

using LoadDesiredStateFn = std::function<std::optional<DesiredState>(const std::string&)>;

void InitializeSchema(
    sqlite3* db,
    std::string_view bootstrap_sql,
    const LoadDesiredStateFn& load_desired_state);

}  // namespace comet::sqlite_store_schema
