#pragma once

#include "backend/http_hostd_backend_support.h"

namespace comet::hostd {

class DefaultHttpHostdBackendSupport final : public IHttpHostdBackendSupport {
 public:
  nlohmann::json SendControllerJsonRequest(
      const std::string& controller_url,
      const std::string& method,
      const std::string& path,
      const nlohmann::json& payload,
      const std::map<std::string, std::string>& headers = {}) const override;

  comet::HostAssignment ParseAssignmentPayload(const nlohmann::json& payload) const override;
  nlohmann::json BuildHostObservationPayload(
      const comet::HostObservation& observation) const override;
  nlohmann::json BuildDiskRuntimeStatePayload(
      const comet::DiskRuntimeState& state) const override;
  comet::DiskRuntimeState ParseDiskRuntimeStatePayload(
      const nlohmann::json& payload) const override;
  std::string Trim(const std::string& value) const override;
};

}  // namespace comet::hostd
