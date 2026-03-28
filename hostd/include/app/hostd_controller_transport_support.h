#pragma once

#include <map>
#include <string>

#include <nlohmann/json.hpp>

#include "comet/state/sqlite_store.h"

namespace comet::hostd::controller_transport_support {

std::string Trim(const std::string& value);

nlohmann::json SendControllerJsonRequest(
    const std::string& controller_url,
    const std::string& method,
    const std::string& path,
    const nlohmann::json& payload,
    const std::map<std::string, std::string>& headers = {});

comet::HostAssignment ParseAssignmentPayload(const nlohmann::json& payload);
nlohmann::json BuildHostObservationPayload(const comet::HostObservation& observation);
nlohmann::json BuildDiskRuntimeStatePayload(const comet::DiskRuntimeState& state);
comet::DiskRuntimeState ParseDiskRuntimeStatePayload(const nlohmann::json& payload);

}  // namespace comet::hostd::controller_transport_support
