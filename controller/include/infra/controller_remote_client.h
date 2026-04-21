#pragma once

#include <optional>
#include <string>
#include <utility>
#include <vector>

#include <nlohmann/json.hpp>

#include "http/controller_http_transport.h"

std::optional<std::string> ResolveControllerTarget(
    const std::optional<std::string>& explicit_target,
    const std::optional<std::string>& db_arg);

nlohmann::json SendControllerJsonRequest(
    const naim::controller::ControllerEndpointTarget& target,
    const std::string& method,
    const std::string& path,
    const std::vector<std::pair<std::string, std::string>>& params = {});
