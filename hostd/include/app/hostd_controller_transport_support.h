#pragma once

#include <map>
#include <string>

#include <nlohmann/json.hpp>

#include "naim/state/sqlite_store.h"

namespace naim::hostd::controller_transport_support {

std::string Trim(const std::string& value);

nlohmann::json SendControllerJsonRequest(
    const std::string& controller_url,
    const std::string& method,
    const std::string& path,
    const nlohmann::json& payload,
    const std::map<std::string, std::string>& headers = {});

naim::HostAssignment ParseAssignmentPayload(const nlohmann::json& payload);
nlohmann::json BuildHostObservationPayload(const naim::HostObservation& observation);
nlohmann::json BuildDiskRuntimeStatePayload(const naim::DiskRuntimeState& state);
naim::DiskRuntimeState ParseDiskRuntimeStatePayload(const nlohmann::json& payload);

}  // namespace naim::hostd::controller_transport_support
