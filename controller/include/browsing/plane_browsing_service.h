#pragma once

#include <optional>
#include <string>

#include <nlohmann/json.hpp>

#include "http/controller_http_transport.h"
#include "http/controller_http_types.h"

namespace naim::controller {

class PlaneBrowsingService final {
 public:
  bool IsEnabled(const DesiredState& desired_state) const;

  std::optional<ControllerEndpointTarget> ResolveTarget(
      const DesiredState& desired_state) const;

  nlohmann::json BuildStatusPayload(
      const DesiredState& desired_state,
      const std::optional<std::string>& plane_state) const;

  std::optional<HttpResponse> ProxyPlaneBrowsingRequest(
      const DesiredState& desired_state,
      const std::string& method,
      const std::string& path_suffix,
      const std::string& body,
      std::string* error_code,
      std::string* error_message) const;
};

}  // namespace naim::controller
