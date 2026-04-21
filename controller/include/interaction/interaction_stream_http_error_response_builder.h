#pragma once

#include <optional>
#include <string>

#include <nlohmann/json.hpp>

#include "http/controller_http_transport.h"
#include "interaction/interaction_service.h"

namespace naim::controller {

class InteractionStreamHttpErrorResponseBuilder final {
 public:
  ::HttpResponse Build(
      int status_code,
      const std::string& request_id,
      const std::string& code,
      const std::string& message,
      bool retryable,
      const std::optional<std::string>& plane_name = std::nullopt,
      const std::optional<PlaneInteractionResolution>& resolution = std::nullopt,
      const nlohmann::json& details = nlohmann::json::object()) const;
};

}  // namespace naim::controller
