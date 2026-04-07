#pragma once

#include <functional>
#include <map>
#include <optional>
#include <string>
#include <vector>

#include <nlohmann/json.hpp>

#include "comet/state/models.h"
#include "comet/runtime/runtime_status.h"
#include "comet/state/sqlite_store.h"

namespace comet::controller {

struct ControllerEndpointTarget {
  std::string raw;
  std::string host;
  int port = 0;
  std::string base_path;
};

struct PlaneInteractionResolution {
  std::string db_path;
  comet::DesiredState desired_state;
  std::optional<comet::PlaneRecord> plane_record;
  std::optional<comet::HostObservation> observation;
  std::optional<comet::RuntimeStatus> runtime_status;
  std::optional<ControllerEndpointTarget> target;
  nlohmann::json status_payload;
};

struct InteractionCompletionPolicy {
  std::string response_mode = "normal";
  int max_tokens = 512;
  std::optional<int> target_completion_tokens;
  int max_continuations = 3;
  int max_total_completion_tokens = 1536;
  int max_elapsed_time_ms = 180000;
  bool thinking_enabled = false;
  std::string semantic_goal;
  std::string completion_marker = "[[TASK_COMPLETE]]";
  bool require_completion_marker = false;
};

struct CompletionMarkerFilterState {
  std::string pending;
  bool marker_seen = false;
};

struct InteractionSseFrame {
  std::string event_name = "message";
  std::string data;
};

struct ResolvedInteractionPolicy {
  InteractionCompletionPolicy policy;
  std::string mode = "default";
  bool repository_analysis = false;
  bool long_form = false;
};

struct InteractionSegmentSummary {
  int index = 0;
  int continuation_index = 0;
  std::string text;
  std::string finish_reason = "stop";
  int prompt_tokens = 0;
  int completion_tokens = 0;
  int total_tokens = 0;
  int latency_ms = 0;
  bool marker_seen = false;
};

struct StreamedInteractionSegmentResult {
  InteractionSegmentSummary summary;
  std::string model;
  std::string cleaned_text;
};

struct InteractionSessionResult {
  std::string session_id;
  std::string model;
  std::string content;
  std::vector<InteractionSegmentSummary> segments;
  int total_prompt_tokens = 0;
  int total_completion_tokens = 0;
  int total_tokens = 0;
  int total_latency_ms = 0;
  int continuation_count = 0;
  std::string completion_status = "in_progress";
  std::string stop_reason;
  std::string final_finish_reason = "stop";
  bool marker_seen = false;
};

struct InteractionRequestContext {
  std::string request_id;
  nlohmann::json original_payload = nlohmann::json::object();
  nlohmann::json payload = nlohmann::json::object();
  nlohmann::json client_messages = nlohmann::json::array();
  nlohmann::json delta_messages = nlohmann::json::array();
  bool structured_output_json = false;
  std::string normalized_model;
  std::optional<std::string> requested_session_id;
  std::string conversation_session_id;
  std::string owner_kind = "anonymous";
  std::optional<int> owner_user_id;
  std::string auth_session_kind;
  int expected_session_version = 0;
  nlohmann::json session_context_state = nlohmann::json::object();
  bool session_restored_from_archive = false;
};

inline constexpr const char* kInteractionSessionContextStatePayloadKey =
    "_comet_session_context_state";

struct InteractionValidationError {
  std::string code;
  std::string message;
  bool retryable = false;
  nlohmann::json details = nlohmann::json::object();
};

struct InteractionJsonResponseSpec {
  int status_code = 200;
  nlohmann::json payload = nlohmann::json::object();
};

struct InteractionUpstreamResponse {
  int status_code = 0;
  std::string body;
  std::map<std::string, std::string> headers;
};

struct InteractionProxyResult {
  bool passthrough_upstream = false;
  std::optional<InteractionJsonResponseSpec> json_response;
  InteractionUpstreamResponse upstream;
};

struct InteractionStreamSetup {
  std::string plane_name;
  PlaneInteractionResolution resolution;
  InteractionRequestContext request_context;
  ResolvedInteractionPolicy resolved_policy;
};

struct InteractionStreamResolutionResult {
  std::optional<InteractionJsonResponseSpec> error_response;
  std::optional<InteractionStreamSetup> setup;
};

struct InteractionStreamingUpstreamConnection {
  bool chunked_transfer = false;
  std::string initial_body;
  std::function<std::string()> read_next_chunk;
  std::function<void()> close;
};

InteractionCompletionPolicy NormalizeConfiguredInteractionCompletionPolicy(
    const comet::InteractionSettings::CompletionPolicy& configured_policy);

InteractionCompletionPolicy DefaultChatInteractionCompletionPolicy();

ResolvedInteractionPolicy ResolveInteractionCompletionPolicy(
    const comet::DesiredState& desired_state,
    const nlohmann::json& payload);

std::string GenerateInteractionRequestId();
std::string GenerateInteractionSessionId();
std::string BuildRepositoryAnalysisInstruction();
std::string BuildSemanticCompletionInstruction(
    const InteractionCompletionPolicy& policy);
std::string BuildContinuationPrompt(
    const InteractionCompletionPolicy& policy,
    bool natural_stop_without_marker,
    const std::string& trailing_excerpt = "",
    int remaining_completion_tokens = 0,
    bool hidden_thinking_mode = false,
    bool visible_output_started = false);
bool StartsWithReasoningPreamble(const std::string& text);
std::string SanitizeInteractionText(std::string text);
std::string Utf8SafeSuffix(const std::string& value, std::size_t max_bytes);
bool SessionReachedTargetLength(
    const InteractionCompletionPolicy& policy,
    int total_completion_tokens);
bool CanCompleteOnNaturalStop(
    const InteractionCompletionPolicy& policy,
    const InteractionSegmentSummary& summary);
std::string RemoveCompletionMarkers(
    const std::string& input,
    const std::string& marker,
    bool* marker_seen);
std::string ConsumeCompletionMarkerFilteredChunk(
    CompletionMarkerFilterState& state,
    const std::string& chunk,
    const std::string& marker,
    bool final_flush);
std::string ExtractInteractionText(const nlohmann::json& payload);
std::string ExtractInteractionFinishReason(const nlohmann::json& payload);
nlohmann::json ExtractInteractionUsage(const nlohmann::json& payload);
bool TryConsumeSseFrame(std::string& buffer, InteractionSseFrame* frame);
std::optional<std::string> ParseInteractionStreamPlaneName(
    const std::string& request_method,
    const std::string& request_path);

std::map<std::string, std::string> BuildInteractionResponseHeaders(
    const std::string& request_id);

std::string ResolveInteractionServedModelName(
    const PlaneInteractionResolution& resolution);

std::string ResolveInteractionActiveModelId(
    const PlaneInteractionResolution& resolution);

nlohmann::json BuildInteractionContractMetadata(
    const PlaneInteractionResolution& resolution,
    const std::string& request_id,
    const std::optional<std::string>& session_id = std::nullopt,
    const std::optional<int>& segment_count = std::nullopt,
    const std::optional<int>& continuation_count = std::nullopt);

class InteractionRequestValidator {
 public:
  nlohmann::json ParsePayload(const std::string& body) const;

  std::optional<InteractionValidationError> ValidateAndNormalizeRequest(
      const PlaneInteractionResolution& resolution,
      const nlohmann::json& original_payload,
      InteractionRequestContext* context) const;
};

class InteractionContractResponder {
 public:
  nlohmann::json BuildStandaloneErrorPayload(
      const std::string& request_id,
      const std::string& code,
      const std::string& message,
      bool retryable,
      const std::optional<std::string>& plane_name = std::nullopt,
      const std::optional<std::string>& reason = std::nullopt,
      const std::optional<std::string>& served_model_name = std::nullopt,
      const std::optional<std::string>& active_model_id = std::nullopt,
      const nlohmann::json& details = nlohmann::json::object()) const;

  nlohmann::json BuildPlaneErrorPayload(
      const PlaneInteractionResolution& resolution,
      const std::string& request_id,
      const std::string& code,
      const std::string& message,
      bool retryable,
      const nlohmann::json& details = nlohmann::json::object()) const;
};

class InteractionSessionPresenter {
 public:
  nlohmann::json BuildSessionPayload(const InteractionSessionResult& result) const;

  std::optional<nlohmann::json> ParseStructuredOutputObject(
      const std::string& text) const;

  InteractionJsonResponseSpec BuildResponseSpec(
      const PlaneInteractionResolution& resolution,
      const InteractionRequestContext& request_context,
      const InteractionSessionResult& result) const;
};

class InteractionStreamPresenter {
 public:
  nlohmann::json BuildSessionStartedEvent(
      const std::string& request_id,
      const std::string& session_id,
      const std::string& plane_name,
      const PlaneInteractionResolution& resolution,
      const InteractionRequestContext& request_context) const;

  nlohmann::json BuildSegmentStartedEvent(
      const std::string& request_id,
      const std::string& session_id,
      int segment_index) const;

  nlohmann::json BuildSegmentCompleteEvent(
      const std::string& request_id,
      const std::string& session_id,
      int segment_index,
      const InteractionSegmentSummary& summary) const;

  nlohmann::json BuildContinuationStartedEvent(
      const std::string& request_id,
      const std::string& session_id,
      int continuation_index,
      const InteractionSegmentSummary& summary) const;

  nlohmann::json BuildSessionFailedEvent(
      const std::string& request_id,
      const std::string& session_id,
      const std::string& code,
      const std::string& message,
      bool retryable,
      const InteractionSessionResult& session,
      const std::optional<nlohmann::json>& session_payload = std::nullopt) const;

  nlohmann::json BuildErrorEvent(
      const std::string& request_id,
      const std::string& code,
      const std::string& message,
      bool retryable,
      const std::string& plane_name) const;

  nlohmann::json BuildSessionCompleteEvent(
      const PlaneInteractionResolution& resolution,
      const std::string& request_id,
      const InteractionSessionResult& session,
      const nlohmann::json& session_payload,
      const InteractionRequestContext& request_context) const;

  nlohmann::json BuildCompleteEvent(
      const PlaneInteractionResolution& resolution,
      const std::string& request_id,
      const InteractionRequestContext& request_context,
      const InteractionSessionResult& session,
      const nlohmann::json& session_payload,
      const std::optional<nlohmann::json>& parsed_structured_output) const;
};

class InteractionSessionExecutor {
 public:
  using BuildInteractionUpstreamBodyFn = std::function<std::string(
      const PlaneInteractionResolution&,
      nlohmann::json,
      bool,
      const ResolvedInteractionPolicy&,
      bool)>;
  using SendInteractionRequestFn = std::function<InteractionUpstreamResponse(
      const ControllerEndpointTarget&,
      const std::string&,
      const std::string&)>;
  using ExtractInteractionUsageFn =
      std::function<nlohmann::json(const nlohmann::json&)>;
  using ExtractInteractionTextFn =
      std::function<std::string(const nlohmann::json&)>;
  using ExtractInteractionFinishReasonFn =
      std::function<std::string(const nlohmann::json&)>;
  using RemoveCompletionMarkersFn = std::function<std::string(
      const std::string&,
      const std::string&,
      bool*)>;
  using SessionReachedTargetLengthFn =
      std::function<bool(const InteractionCompletionPolicy&, int)>;
  using CanCompleteOnNaturalStopFn = std::function<bool(
      const InteractionCompletionPolicy&,
      const InteractionSegmentSummary&)>;
  using BuildContinuationPayloadFn = std::function<nlohmann::json(
      const nlohmann::json&,
      const std::string&,
      const InteractionCompletionPolicy&,
      bool,
      int)>;

  InteractionSessionExecutor(
      BuildInteractionUpstreamBodyFn build_interaction_upstream_body,
      SendInteractionRequestFn send_interaction_request,
      ExtractInteractionUsageFn extract_interaction_usage,
      ExtractInteractionTextFn extract_interaction_text,
      ExtractInteractionFinishReasonFn extract_interaction_finish_reason,
      RemoveCompletionMarkersFn remove_completion_markers,
      SessionReachedTargetLengthFn session_reached_target_length,
      CanCompleteOnNaturalStopFn can_complete_on_natural_stop,
      BuildContinuationPayloadFn build_continuation_payload);

  InteractionSessionResult Execute(
      const PlaneInteractionResolution& resolution,
      const InteractionRequestContext& request_context) const;

 private:
  BuildInteractionUpstreamBodyFn build_interaction_upstream_body_;
  SendInteractionRequestFn send_interaction_request_;
  ExtractInteractionUsageFn extract_interaction_usage_;
  ExtractInteractionTextFn extract_interaction_text_;
  ExtractInteractionFinishReasonFn extract_interaction_finish_reason_;
  RemoveCompletionMarkersFn remove_completion_markers_;
  SessionReachedTargetLengthFn session_reached_target_length_;
  CanCompleteOnNaturalStopFn can_complete_on_natural_stop_;
  BuildContinuationPayloadFn build_continuation_payload_;
};

class InteractionProxyExecutor {
 public:
  using BuildInteractionUpstreamBodyFn = std::function<std::string(
      const PlaneInteractionResolution&,
      nlohmann::json,
      bool,
      const ResolvedInteractionPolicy&,
      bool)>;
  using SendProxyRequestFn = std::function<InteractionUpstreamResponse(
      const ControllerEndpointTarget&,
      const std::string&,
      const std::string&,
      const std::string&,
      const std::string&)>;
  using ResolveRequestSkillsFn = std::function<std::optional<InteractionValidationError>(
      const PlaneInteractionResolution&,
      InteractionRequestContext*)>;

  InteractionProxyExecutor(
      BuildInteractionUpstreamBodyFn build_interaction_upstream_body,
      SendProxyRequestFn send_proxy_request,
      ResolveRequestSkillsFn resolve_request_skills);

  InteractionProxyResult Execute(
      const PlaneInteractionResolution& resolution,
      const std::string& request_id,
      const std::string& method,
      const std::string& path,
      const std::string& body) const;

 private:
  BuildInteractionUpstreamBodyFn build_interaction_upstream_body_;
  SendProxyRequestFn send_proxy_request_;
  ResolveRequestSkillsFn resolve_request_skills_;
};

class InteractionStreamRequestResolver {
 public:
  using ResolvePlaneInteractionFn = std::function<PlaneInteractionResolution(
      const std::string&,
      const std::string&)>;
  using ResolveRequestSkillsFn = std::function<std::optional<InteractionValidationError>(
      const PlaneInteractionResolution&,
      InteractionRequestContext*)>;

  InteractionStreamRequestResolver(
      ResolvePlaneInteractionFn resolve_plane_interaction,
      ResolveRequestSkillsFn resolve_request_skills);

  InteractionStreamResolutionResult Resolve(
      const std::string& db_path,
      const std::string& request_method,
      const std::string& request_path,
      const std::string& request_body,
      const std::string& request_id) const;

 private:
  ResolvePlaneInteractionFn resolve_plane_interaction_;
  ResolveRequestSkillsFn resolve_request_skills_;
};

class InteractionStreamSessionExecutor {
 public:
  using SessionReachedTargetLengthFn =
      std::function<bool(const InteractionCompletionPolicy&, int)>;
  using CanCompleteOnNaturalStopFn = std::function<bool(
      const InteractionCompletionPolicy&,
      const InteractionSegmentSummary&)>;
  using BuildContinuationPayloadFn = std::function<nlohmann::json(
      const nlohmann::json&,
      const std::string&,
      const InteractionCompletionPolicy&,
      bool,
      int)>;
  using ExecuteStreamedSegmentFn = std::function<StreamedInteractionSegmentResult(
      const nlohmann::json&,
      int)>;
  using SendEventFn =
      std::function<bool(const std::string&, const nlohmann::json&)>;
  using SendDoneFn = std::function<bool()>;

  InteractionStreamSessionExecutor(
      SessionReachedTargetLengthFn session_reached_target_length,
      CanCompleteOnNaturalStopFn can_complete_on_natural_stop,
      BuildContinuationPayloadFn build_continuation_payload);

  InteractionSessionResult Execute(
      const std::string& request_id,
      const std::string& session_id,
      const std::string& plane_name,
      const PlaneInteractionResolution& resolution,
      const InteractionRequestContext& request_context,
      const ResolvedInteractionPolicy& resolved_policy,
      ExecuteStreamedSegmentFn execute_streamed_segment,
      SendEventFn send_event,
      SendDoneFn send_done) const;

 private:
  SessionReachedTargetLengthFn session_reached_target_length_;
  CanCompleteOnNaturalStopFn can_complete_on_natural_stop_;
  BuildContinuationPayloadFn build_continuation_payload_;
};

class InteractionStreamSegmentExecutor {
 public:
  using BuildInteractionUpstreamBodyFn = std::function<std::string(
      const PlaneInteractionResolution&,
      nlohmann::json,
      bool,
      const ResolvedInteractionPolicy&,
      bool)>;
  using OpenStreamingRequestFn =
      std::function<InteractionStreamingUpstreamConnection(
          const ControllerEndpointTarget&,
          const std::string&,
          const std::string&)>;
  using SendFallbackRequestFn = std::function<InteractionUpstreamResponse(
      const ControllerEndpointTarget&,
      const std::string&,
      const std::string&)>;
  using StartsWithReasoningPreambleFn =
      std::function<bool(const std::string&)>;
  using SanitizeInteractionTextFn =
      std::function<std::string(std::string)>;
  using ExtractInteractionUsageFn =
      std::function<nlohmann::json(const nlohmann::json&)>;
  using ExtractInteractionTextFn =
      std::function<std::string(const nlohmann::json&)>;
  using ExtractInteractionFinishReasonFn =
      std::function<std::string(const nlohmann::json&)>;
  using SendDeltaFn =
      std::function<bool(const std::string&, const std::string&)>;

  InteractionStreamSegmentExecutor(
      BuildInteractionUpstreamBodyFn build_interaction_upstream_body,
      OpenStreamingRequestFn open_streaming_request,
      SendFallbackRequestFn send_fallback_request,
      StartsWithReasoningPreambleFn starts_with_reasoning_preamble,
      SanitizeInteractionTextFn sanitize_interaction_text,
      ExtractInteractionUsageFn extract_interaction_usage,
      ExtractInteractionTextFn extract_interaction_text,
      ExtractInteractionFinishReasonFn extract_interaction_finish_reason);

  StreamedInteractionSegmentResult Execute(
      const PlaneInteractionResolution& resolution,
      const InteractionRequestContext& request_context,
      const ResolvedInteractionPolicy& resolved_policy,
      const std::string& request_id,
      const nlohmann::json& payload,
      int segment_index,
      SendDeltaFn send_delta) const;

 private:
  BuildInteractionUpstreamBodyFn build_interaction_upstream_body_;
  OpenStreamingRequestFn open_streaming_request_;
  SendFallbackRequestFn send_fallback_request_;
  StartsWithReasoningPreambleFn starts_with_reasoning_preamble_;
  SanitizeInteractionTextFn sanitize_interaction_text_;
  ExtractInteractionUsageFn extract_interaction_usage_;
  ExtractInteractionTextFn extract_interaction_text_;
  ExtractInteractionFinishReasonFn extract_interaction_finish_reason_;
};

class InteractionPlaneResolver {
 public:
  using FindInferInstanceNameFn =
      std::function<std::optional<std::string>(const comet::DesiredState&)>;
  using ParseInstanceRuntimeStatusesFn = std::function<
      std::vector<comet::RuntimeProcessStatus>(const comet::HostObservation&)>;
  using ObservationMatchesPlaneFn =
      std::function<bool(const comet::HostObservation&, const std::string&)>;
  using BuildPlaneScopedRuntimeStatusFn = std::function<
      std::optional<comet::RuntimeStatus>(
          const comet::DesiredState&, const comet::HostObservation&)>;
  using ParseInteractionTargetFn =
      std::function<std::optional<ControllerEndpointTarget>(const std::string&, int)>;
  using CountReadyWorkerMembersFn =
      std::function<int(comet::ControllerStore&, const comet::DesiredState&)>;
  using ProbeControllerTargetOkFn =
      std::function<bool(const std::optional<ControllerEndpointTarget>&, const std::string&)>;
  using DescribeUnsupportedControllerLocalRuntimeFn = std::function<
      std::optional<std::string>(const comet::DesiredState&, const std::string&)>;

  InteractionPlaneResolver(
      FindInferInstanceNameFn find_infer_instance_name,
      ParseInstanceRuntimeStatusesFn parse_instance_runtime_statuses,
      ObservationMatchesPlaneFn observation_matches_plane,
      BuildPlaneScopedRuntimeStatusFn build_plane_scoped_runtime_status,
      ParseInteractionTargetFn parse_interaction_target,
      CountReadyWorkerMembersFn count_ready_worker_members,
      ProbeControllerTargetOkFn probe_controller_target_ok,
      DescribeUnsupportedControllerLocalRuntimeFn
          describe_unsupported_controller_local_runtime);

  PlaneInteractionResolution Resolve(
      const std::string& db_path,
      const std::string& plane_name) const;

 private:
  FindInferInstanceNameFn find_infer_instance_name_;
  ParseInstanceRuntimeStatusesFn parse_instance_runtime_statuses_;
  ObservationMatchesPlaneFn observation_matches_plane_;
  BuildPlaneScopedRuntimeStatusFn build_plane_scoped_runtime_status_;
  ParseInteractionTargetFn parse_interaction_target_;
  CountReadyWorkerMembersFn count_ready_worker_members_;
  ProbeControllerTargetOkFn probe_controller_target_ok_;
  DescribeUnsupportedControllerLocalRuntimeFn
      describe_unsupported_controller_local_runtime_;
};

}  // namespace comet::controller
