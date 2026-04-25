#pragma once

#include <functional>
#include <map>
#include <optional>
#include <string>
#include <vector>

#include <nlohmann/json.hpp>

#include "naim/runtime/runtime_status.h"
#include "naim/state/models.h"
#include "naim/state/sqlite_store.h"

namespace naim::controller {

struct ControllerEndpointTarget {
  std::string raw;
  std::string host;
  int port = 0;
  std::string base_path;
};

struct PlaneInteractionResolution {
  std::string db_path;
  naim::DesiredState desired_state;
  std::optional<naim::PlaneRecord> plane_record;
  std::optional<naim::HostObservation> observation;
  std::optional<naim::RuntimeStatus> runtime_status;
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

struct InteractionRuntimeExecutionRequest {
  naim::DesiredState desired_state;
  nlohmann::json status_payload = nlohmann::json::object();
  nlohmann::json payload = nlohmann::json::object();
  ResolvedInteractionPolicy resolved_policy;
  bool structured_output_json = false;
  bool force_stream = false;
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
  bool context_compression_enabled = false;
  std::string context_compression_status = "none";
  int dialog_estimate_before = 0;
  int dialog_estimate_after = 0;
  double context_compression_ratio = 1.0;
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
  bool context_compression_enabled = false;
  std::string context_compression_status = "none";
  int dialog_estimate_before = 0;
  int dialog_estimate_after = 0;
  double context_compression_ratio = 1.0;
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
    "_naim_session_context_state";

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
  std::map<std::string, std::string> headers;
  std::string initial_body;
  std::function<std::string()> read_next_chunk;
  std::function<void()> close;
};

}  // namespace naim::controller
