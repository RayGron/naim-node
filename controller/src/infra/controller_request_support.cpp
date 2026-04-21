#include "infra/controller_request_support.h"

namespace naim::controller {

nlohmann::json ControllerRequestSupport::ParseJsonRequestBody(
    const HttpRequest& request) const {
  if (request.body.empty()) {
    return nlohmann::json::object();
  }
  return nlohmann::json::parse(request.body);
}

std::string ControllerRequestSupport::ResolveArtifactsRoot(
    const std::optional<std::string>& artifacts_root_arg,
    const std::string& fallback_artifacts_root) const {
  return artifacts_root_arg.value_or(fallback_artifacts_root);
}

}  // namespace naim::controller
