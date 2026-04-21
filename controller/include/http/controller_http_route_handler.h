#pragma once

#include <optional>
#include <string>

#include "http/controller_http_transport.h"
#include "http/controller_http_types.h"

namespace naim::controller {

class IControllerHttpRouteHandler {
 public:
  virtual ~IControllerHttpRouteHandler() = default;

  virtual std::optional<HttpResponse> TryHandle(
      const std::string& db_path,
      const std::string& default_artifacts_root,
      const HttpRequest& request) const = 0;
};

}  // namespace naim::controller
