#include "app/hostd_app_controller_support.h"

#include "app/hostd_controller_transport_support.h"

namespace naim::hostd {

std::string HostdAppControllerSupport::Trim(const std::string& value) const {
  return controller_transport_support::Trim(value);
}

nlohmann::json HostdAppControllerSupport::SendControllerJsonRequest(
    const std::string& controller_url,
    const std::string& method,
    const std::string& path,
    const nlohmann::json& payload,
    const std::map<std::string, std::string>& headers) const {
  return controller_transport_support::SendControllerJsonRequest(
      controller_url,
      method,
      path,
      payload,
      headers);
}

naim::HostAssignment HostdAppControllerSupport::ParseAssignmentPayload(
    const nlohmann::json& payload) const {
  return controller_transport_support::ParseAssignmentPayload(payload);
}

nlohmann::json HostdAppControllerSupport::BuildHostObservationPayload(
    const naim::HostObservation& observation) const {
  return controller_transport_support::BuildHostObservationPayload(observation);
}

nlohmann::json HostdAppControllerSupport::BuildDiskRuntimeStatePayload(
    const naim::DiskRuntimeState& state) const {
  return controller_transport_support::BuildDiskRuntimeStatePayload(state);
}

naim::DiskRuntimeState HostdAppControllerSupport::ParseDiskRuntimeStatePayload(
    const nlohmann::json& payload) const {
  return controller_transport_support::ParseDiskRuntimeStatePayload(payload);
}

}  // namespace naim::hostd
