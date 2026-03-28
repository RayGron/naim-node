#include "backend/default_http_hostd_backend_support.h"

#include "app/hostd_app_support.h"

namespace comet::hostd {

nlohmann::json DefaultHttpHostdBackendSupport::SendControllerJsonRequest(
    const std::string& controller_url,
    const std::string& method,
    const std::string& path,
    const nlohmann::json& payload,
    const std::map<std::string, std::string>& headers) const {
  return appsupport::SendControllerJsonRequest(controller_url, method, path, payload, headers);
}

comet::HostAssignment DefaultHttpHostdBackendSupport::ParseAssignmentPayload(
    const nlohmann::json& payload) const {
  return appsupport::ParseAssignmentPayload(payload);
}

nlohmann::json DefaultHttpHostdBackendSupport::BuildHostObservationPayload(
    const comet::HostObservation& observation) const {
  return appsupport::BuildHostObservationPayload(observation);
}

nlohmann::json DefaultHttpHostdBackendSupport::BuildDiskRuntimeStatePayload(
    const comet::DiskRuntimeState& state) const {
  return appsupport::BuildDiskRuntimeStatePayload(state);
}

comet::DiskRuntimeState DefaultHttpHostdBackendSupport::ParseDiskRuntimeStatePayload(
    const nlohmann::json& payload) const {
  return appsupport::ParseDiskRuntimeStatePayload(payload);
}

std::string DefaultHttpHostdBackendSupport::Trim(const std::string& value) const {
  return appsupport::Trim(value);
}

}  // namespace comet::hostd
