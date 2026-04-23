#include "interaction/interaction_http_executor_factory.h"

#include "http/controller_http_transport.h"
#include "interaction/interaction_completion_policy_support.h"
#include "interaction/interaction_continuation_payload_builder.h"
#include "interaction/interaction_text_post_processor.h"
#include "interaction/interaction_upstream_event_parser.h"

#include <utility>

using nlohmann::json;

namespace naim::controller {

InteractionHttpExecutorFactory::InteractionHttpExecutorFactory(
    const ::InteractionHttpSupport& support)
    : support_(support) {}

InteractionPlaneResolver InteractionHttpExecutorFactory::MakePlaneResolver() const {
  return InteractionPlaneResolver(
      [&](const naim::DesiredState& desired_state) {
        return support_.FindInferInstanceName(desired_state);
      },
      [&](const naim::HostObservation& observation) {
        return support_.ParseInstanceRuntimeStatuses(observation);
      },
      [&](const naim::HostObservation& observation,
          const std::string& plane_name) {
        return support_.ObservationMatchesPlane(observation, plane_name);
      },
      [&](const naim::DesiredState& desired_state,
          const naim::HostObservation& observation) {
        return support_.BuildPlaneScopedRuntimeStatus(desired_state, observation);
      },
      [&](const std::string& gateway_listen, int fallback_port) {
        return support_.ParseInteractionTarget(gateway_listen, fallback_port);
      },
      [&](const naim::DesiredState& desired_state) {
        return support_.ResolvePlaneLocalInteractionTarget(desired_state);
      },
      [&](naim::ControllerStore& store,
          const naim::DesiredState& desired_state) {
        return support_.CountReadyWorkerMembers(store, desired_state);
      },
      [&](const std::optional<ControllerEndpointTarget>& target,
          const std::string& path) {
        return support_.ProbeControllerTargetOk(target, path);
      },
      [&](const naim::DesiredState& desired_state,
          const std::string& node_name) {
        return support_.DescribeUnsupportedControllerLocalRuntime(
            desired_state, node_name);
      });
}

InteractionSessionExecutor InteractionHttpExecutorFactory::MakeSessionExecutor() const {
  return InteractionSessionExecutor(
      [&](const PlaneInteractionResolution& resolution,
          json payload,
          bool force_stream,
          const ResolvedInteractionPolicy& resolved_policy,
          bool structured_output_json) {
        return support_.BuildInteractionRuntimeRequestBody(
            resolution,
            std::move(payload),
            force_stream,
            resolved_policy,
            structured_output_json);
      },
      [&](const ControllerEndpointTarget& target,
          const std::string& request_id,
          const std::string& body) {
        const ::HttpResponse response = support_.SendControllerHttpRequest(
            target,
            "POST",
            "/v1/chat/completions",
            body,
            {{"Accept", "application/json"},
             {"X-Naim-Request-Id", request_id}});
        return InteractionUpstreamResponse{
            response.status_code,
            response.body,
            response.headers,
        };
      },
      [](const json& payload) {
        return InteractionUpstreamEventParser{}.ExtractInteractionUsage(payload);
      },
      [](const json& payload) {
        return InteractionTextPostProcessor{}.ExtractInteractionText(payload);
      },
      [](const json& payload) {
        return InteractionUpstreamEventParser{}.ExtractInteractionFinishReason(
            payload);
      },
      [](const std::string& input,
         const std::string& marker,
         bool* marker_seen) {
        return InteractionTextPostProcessor{}.RemoveCompletionMarkers(
            input, marker, marker_seen);
      },
      [](const InteractionCompletionPolicy& policy,
         int total_completion_tokens) {
        return InteractionCompletionPolicySupport{}.SessionReachedTargetLength(
            policy, total_completion_tokens);
      },
      [](const InteractionCompletionPolicy& policy,
         const InteractionSegmentSummary& summary) {
        return InteractionCompletionPolicySupport{}.CanCompleteOnNaturalStop(
            policy, summary);
      },
      [](const json& original_payload,
         const std::string& accumulated_text,
         const InteractionCompletionPolicy& policy,
         bool natural_stop_without_marker,
         int total_completion_tokens) {
        return InteractionContinuationPayloadBuilder{}.Build(
            original_payload,
            accumulated_text,
            policy,
            natural_stop_without_marker,
            total_completion_tokens);
      });
}

InteractionStreamSegmentExecutor
InteractionHttpExecutorFactory::MakeStreamSegmentExecutor() const {
  return InteractionStreamSegmentExecutor(
      [&](const PlaneInteractionResolution& resolution,
          json payload,
          bool force_stream,
          const ResolvedInteractionPolicy& resolved_policy,
          bool structured_output_json) {
        return support_.BuildInteractionRuntimeRequestBody(
            resolution,
            std::move(payload),
            force_stream,
            resolved_policy,
            structured_output_json);
      },
      [](const ControllerEndpointTarget& target,
         const std::string& request_id,
         const std::string& body) {
        return ::OpenInteractionStreamRequest(target, request_id, body);
      },
      [&](const ControllerEndpointTarget& target,
          const std::string& request_id,
          const std::string& body) {
        const ::HttpResponse response = support_.SendControllerHttpRequest(
            target,
            "POST",
            "/v1/chat/completions",
            body,
            {{"Accept", "application/json"},
             {"X-Naim-Request-Id", request_id}});
        return InteractionUpstreamResponse{
            response.status_code,
            response.body,
            response.headers,
        };
      },
      [](const std::string& text) {
        return InteractionTextPostProcessor{}.StartsWithReasoningPreamble(text);
      },
      [](std::string text) {
        return InteractionTextPostProcessor{}.SanitizeInteractionText(
            std::move(text));
      },
      [](const json& payload) {
        return InteractionUpstreamEventParser{}.ExtractInteractionUsage(payload);
      },
      [](const json& payload) {
        return InteractionTextPostProcessor{}.ExtractInteractionText(payload);
      },
      [](const json& payload) {
        return InteractionUpstreamEventParser{}.ExtractInteractionFinishReason(
            payload);
      });
}

InteractionProxyExecutor InteractionHttpExecutorFactory::MakeProxyExecutor(
    ResolveRequestContextFn resolve_request_context) const {
  return InteractionProxyExecutor(
      [&](const PlaneInteractionResolution& resolution,
          json payload,
          bool force_stream,
          const ResolvedInteractionPolicy& resolved_policy,
          bool structured_output_json) {
        return support_.BuildInteractionRuntimeRequestBody(
            resolution,
            std::move(payload),
            force_stream,
            resolved_policy,
            structured_output_json);
      },
      [&](const ControllerEndpointTarget& target,
          const std::string& method,
          const std::string& path,
          const std::string& body,
          const std::string& request_id) {
        const ::HttpResponse response = support_.SendControllerHttpRequest(
            target,
            method,
            path,
            body,
            {{"Accept", "application/json"},
             {"X-Naim-Request-Id", request_id}});
        return InteractionUpstreamResponse{
            response.status_code,
            response.body,
            response.headers,
        };
      },
      std::move(resolve_request_context));
}

InteractionStreamRequestResolver
InteractionHttpExecutorFactory::MakeStreamRequestResolver() const {
  return InteractionStreamRequestResolver(
      [&](const std::string& db_path, const std::string& plane_name) {
        return MakePlaneResolver().Resolve(db_path, plane_name);
      },
      [](const PlaneInteractionResolution&, InteractionRequestContext*) {
        return std::optional<InteractionValidationError>{};
      });
}

InteractionStreamHttpRequestPreparationService
InteractionHttpExecutorFactory::MakeStreamRequestPreparationService(
    ResolveRequestContextFn resolve_request_context) const {
  return InteractionStreamHttpRequestPreparationService(
      MakeStreamRequestResolver(),
      std::move(resolve_request_context));
}

InteractionStreamSessionExecutor
InteractionHttpExecutorFactory::MakeStreamSessionExecutor() const {
  return InteractionStreamSessionExecutor(
      [](const InteractionCompletionPolicy& policy,
         int total_completion_tokens) {
        return InteractionCompletionPolicySupport{}.SessionReachedTargetLength(
            policy, total_completion_tokens);
      },
      [](const InteractionCompletionPolicy& policy,
         const InteractionSegmentSummary& summary) {
        return InteractionCompletionPolicySupport{}.CanCompleteOnNaturalStop(
            policy, summary);
      },
      [](const json& original_payload,
         const std::string& accumulated_text,
         const InteractionCompletionPolicy& policy,
         bool natural_stop_without_marker,
         int total_completion_tokens) {
        return InteractionContinuationPayloadBuilder{}.Build(
            original_payload,
            accumulated_text,
            policy,
            natural_stop_without_marker,
            total_completion_tokens);
      });
}

}  // namespace naim::controller
