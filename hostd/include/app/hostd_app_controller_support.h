#pragma once

#include <map>
#include <string>

#include <nlohmann/json.hpp>

#include "backend/http_hostd_backend_support.h"

namespace naim::hostd {

class HostdAppControllerSupport final : public IHttpHostdBackendSupport {
 public:
  std::string Trim(const std::string& value) const override;
  nlohmann::json SendControllerJsonRequest(
      const std::string& controller_url,
      const std::string& method,
      const std::string& path,
      const nlohmann::json& payload,
      const std::map<std::string, std::string>& headers = {}) const override;
  naim::HostAssignment ParseAssignmentPayload(const nlohmann::json& payload) const override;
  nlohmann::json BuildHostObservationPayload(
      const naim::HostObservation& observation) const override;
  nlohmann::json BuildDiskRuntimeStatePayload(
      const naim::DiskRuntimeState& state) const override;
  naim::DiskRuntimeState ParseDiskRuntimeStatePayload(
      const nlohmann::json& payload) const override;
};

}  // namespace naim::hostd
