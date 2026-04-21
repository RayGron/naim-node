#pragma once

#include <functional>
#include <optional>
#include <string>

#include "http/controller_http_transport.h"
#include "http/controller_http_types.h"
#include "interaction/interaction_service.h"

class AuthSupportService;

namespace naim::controller {

struct InteractionStreamHttpRequestPreparationResult {
  std::optional<::HttpResponse> error_response;
  std::optional<InteractionStreamSetup> setup;
};

class InteractionStreamHttpRequestPreparationService final {
 public:
  using ResolveRequestContextFn =
      std::function<std::optional<InteractionValidationError>(
          const PlaneInteractionResolution&,
          InteractionRequestContext*)>;

  InteractionStreamHttpRequestPreparationService(
      InteractionStreamRequestResolver stream_request_resolver,
      ResolveRequestContextFn resolve_request_context);

  InteractionStreamHttpRequestPreparationResult Prepare(
      const std::string& db_path,
      const HttpRequest& request,
      const std::string& request_id,
      AuthSupportService& auth_support) const;

 private:
  int ConversationErrorStatusCode(const InteractionValidationError& error) const;
  int RequestContextErrorStatusCode(const InteractionValidationError& error) const;

  InteractionStreamRequestResolver stream_request_resolver_;
  ResolveRequestContextFn resolve_request_context_;
};

}  // namespace naim::controller
