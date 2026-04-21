#include "interaction/interaction_stream_http_request_preparation_service.h"

#include "app/controller_composition_support.h"
#include "auth/auth_support_service.h"
#include "interaction/interaction_completion_policy_support.h"
#include "interaction/interaction_conversation_service.h"
#include "interaction/interaction_request_contract_support.h"
#include "interaction/interaction_stream_http_error_response_builder.h"

namespace naim::controller {

InteractionStreamHttpRequestPreparationService::
    InteractionStreamHttpRequestPreparationService(
        InteractionStreamRequestResolver stream_request_resolver,
        ResolveRequestContextFn resolve_request_context)
    : stream_request_resolver_(std::move(stream_request_resolver)),
      resolve_request_context_(std::move(resolve_request_context)) {}

InteractionStreamHttpRequestPreparationResult
InteractionStreamHttpRequestPreparationService::Prepare(
    const std::string& db_path,
    const HttpRequest& request,
    const std::string& request_id,
    AuthSupportService& auth_support) const {
  const InteractionStreamHttpErrorResponseBuilder error_response_builder;
  const InteractionCompletionPolicySupport completion_policy_support;
  const InteractionRequestContractSupport request_contract_support;
  const auto initial =
      stream_request_resolver_.Resolve(
          db_path,
          request.method,
          request.path,
          request.body,
          request_id);
  if (initial.error_response.has_value()) {
    return InteractionStreamHttpRequestPreparationResult{
        composition_support::BuildJsonResponse(
            initial.error_response->status_code,
            initial.error_response->payload,
            request_contract_support.BuildInteractionResponseHeaders(request_id)),
        std::nullopt,
    };
  }

  InteractionStreamSetup setup = *initial.setup;
  const std::string& plane_name = setup.plane_name;
  InteractionConversationPrincipal principal;

  try {
    naim::ControllerStore store(db_path);
    store.Initialize();
    const auto authenticated =
        setup.resolution.desired_state.protected_plane
            ? auth_support.AuthenticateProtectedPlaneRequest(
                  store, request, plane_name)
            : auth_support.AuthenticateControllerUserSession(
                  store, request, std::nullopt);
    if (setup.resolution.desired_state.protected_plane &&
        !authenticated.has_value()) {
      return InteractionStreamHttpRequestPreparationResult{
          error_response_builder.Build(
              401,
              request_id,
              "unauthorized",
              "protected plane requires an authenticated WebAuthn session or SSH API session",
              false,
              plane_name),
          std::nullopt,
      };
    }
    if (authenticated.has_value()) {
      principal.owner_kind = "user";
      principal.owner_user_id = authenticated->first.id;
      principal.auth_session_kind = authenticated->second.session_kind;
      principal.authenticated = true;
    }

    if (const auto validation_error = InteractionConversationService().PrepareRequest(
            db_path,
            setup.resolution,
            principal,
            &setup.request_context)) {
      return InteractionStreamHttpRequestPreparationResult{
          error_response_builder.Build(
              ConversationErrorStatusCode(*validation_error),
              request_id,
              validation_error->code,
              validation_error->message,
              validation_error->retryable,
              plane_name,
              setup.resolution,
              validation_error->details),
          std::nullopt,
      };
    }
    if (const auto validation_error =
            resolve_request_context_(setup.resolution, &setup.request_context)) {
      return InteractionStreamHttpRequestPreparationResult{
          error_response_builder.Build(
              RequestContextErrorStatusCode(*validation_error),
              request_id,
              validation_error->code,
              validation_error->message,
              validation_error->retryable,
              plane_name,
              setup.resolution,
              validation_error->details),
          std::nullopt,
      };
    }
  } catch (const nlohmann::json::exception& error) {
    return InteractionStreamHttpRequestPreparationResult{
        error_response_builder.Build(
            400,
            request_id,
            "malformed_request",
            error.what(),
            false,
            plane_name),
        std::nullopt,
    };
  } catch (const std::exception& error) {
    return InteractionStreamHttpRequestPreparationResult{
        error_response_builder.Build(
            404,
            request_id,
            "plane_not_found",
            error.what(),
            false,
            plane_name),
        std::nullopt,
    };
  }

  setup.resolved_policy = completion_policy_support.ResolvePolicy(
      setup.resolution.desired_state,
      setup.request_context.payload);
  return InteractionStreamHttpRequestPreparationResult{
      std::nullopt,
      std::move(setup),
  };
}

int InteractionStreamHttpRequestPreparationService::ConversationErrorStatusCode(
    const InteractionValidationError& error) const {
  if (error.code == "session_not_found") {
    return 404;
  }
  if (error.code == "session_delta_invalid") {
    return 422;
  }
  if (error.code == "session_restore_failed") {
    return 500;
  }
  return 409;
}

int InteractionStreamHttpRequestPreparationService::RequestContextErrorStatusCode(
    const InteractionValidationError& error) const {
  if (error.code == "model_mismatch" ||
      error.code == "skills_disabled" ||
      error.code == "skills_not_ready" ||
      error.code == "session_conflict" ||
      error.code == "session_plane_mismatch") {
    return 409;
  }
  return 400;
}

}  // namespace naim::controller
