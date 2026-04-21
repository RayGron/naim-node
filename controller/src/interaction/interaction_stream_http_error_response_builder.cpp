#include "interaction/interaction_stream_http_error_response_builder.h"

#include "app/controller_composition_support.h"
#include "interaction/interaction_request_contract_support.h"

namespace naim::controller {

::HttpResponse InteractionStreamHttpErrorResponseBuilder::Build(
    int status_code,
    const std::string& request_id,
    const std::string& code,
    const std::string& message,
    bool retryable,
    const std::optional<std::string>& plane_name,
    const std::optional<PlaneInteractionResolution>& resolution,
    const nlohmann::json& details) const {
  const InteractionContractResponder responder;
  const nlohmann::json payload =
      resolution.has_value()
          ? responder.BuildPlaneErrorPayload(
                *resolution,
                request_id,
                code,
                message,
                retryable,
                details)
          : responder.BuildStandaloneErrorPayload(
                request_id,
                code,
                message,
                retryable,
                plane_name);
  return composition_support::BuildJsonResponse(
      status_code,
      payload,
      InteractionRequestContractSupport{}.BuildInteractionResponseHeaders(
          request_id));
}

}  // namespace naim::controller
