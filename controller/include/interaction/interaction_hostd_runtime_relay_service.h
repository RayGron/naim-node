#pragma once

#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "http/controller_http_transport.h"
#include "interaction/interaction_types.h"

namespace naim::controller {

class InteractionHostdRuntimeRelayService final {
 public:
  HttpResponse Send(
      const ControllerEndpointTarget& target,
      const std::string& method,
      const std::string& path,
      const std::string& body,
      const std::vector<std::pair<std::string, std::string>>& headers) const;

 private:
  static std::optional<std::string> FindHeaderValue(
      const std::vector<std::pair<std::string, std::string>>& headers,
      const std::string& name);
  static bool IsAllowedRuntimeRelayPath(
      const std::string& method,
      const std::string& path);
  static bool IsLoopbackRelayTarget(const ControllerEndpointTarget& target);
};

}  // namespace naim::controller
