#include "../include/controller_main_includes.h"

namespace {

using nlohmann::json;
using SocketHandle = comet::platform::SocketHandle;

using ControllerCli = comet::controller::ControllerCli;
using ControllerCommandLine = comet::controller::ControllerCommandLine;
using ControllerEndpointTarget = comet::controller::ControllerEndpointTarget;
using ControllerHttpServer = comet::controller::ControllerHttpServer;
using ControllerHttpRouter = comet::controller::ControllerHttpRouter;
using ControllerHttpServerSupport = comet::controller::ControllerHttpServerSupport;
using ControllerNetworkManager = comet::controller::ControllerNetworkManager;

using HostRegistryService = comet::controller::HostRegistryService;
using InteractionCompletionPolicy = comet::controller::InteractionCompletionPolicy;

using PlaneService = comet::controller::PlaneService;
using PlaneInteractionResolution = comet::controller::PlaneInteractionResolution;

using ResolvedInteractionPolicy = comet::controller::ResolvedInteractionPolicy;
using SchedulerService = comet::controller::SchedulerService;
using WebUiComposeMode = comet::controller::WebUiComposeMode;
using WebUiService = comet::controller::WebUiService;

std::string DefaultDbPath() {
  return (std::filesystem::path("var") / "controller.sqlite").string();
}

std::string DefaultArtifactsRoot() {
  return (std::filesystem::path("var") / "artifacts").string();
}

int DefaultStaleAfterSeconds() {
  return 300;
}

int MinimumSafeDirectRebalanceScore() {
  return 100;
}

int MaximumRebalanceIterationsPerGeneration() {
  return 1;
}

int WorkerMinimumResidencySeconds() {
  return 300;
}

int NodeCooldownAfterMoveSeconds() {
  return 60;
}

int VerificationStableSamplesRequired() {
  return 3;
}

int VerificationTimeoutSeconds() {
  return 45;
}

std::string DefaultUiRoot() {
  return (std::filesystem::path("var") / "ui").string();
}

thread_local const HttpRequest* g_current_http_request = nullptr;

struct ScopedCurrentHttpRequest {
  const HttpRequest* previous = nullptr;

  explicit ScopedCurrentHttpRequest(const HttpRequest& request) : previous(g_current_http_request) {
    g_current_http_request = &request;
  }

  ~ScopedCurrentHttpRequest() {
    g_current_http_request = previous;
  }
};

struct SchedulerRuntimeView;

std::map<std::string, comet::NodeAvailabilityOverride> BuildAvailabilityOverrideMap(
    const std::vector<comet::NodeAvailabilityOverride>& availability_overrides);

comet::NodeAvailability ResolveNodeAvailability(
    const std::map<std::string, comet::NodeAvailabilityOverride>& availability_overrides,
    const std::string& node_name);

std::optional<long long> HeartbeatAgeSeconds(const std::string& heartbeat_at);

std::string HealthFromAge(
    const std::optional<long long>& age_seconds,
    int stale_after_seconds);

std::optional<comet::RuntimeStatus> ParseRuntimeStatus(
    const comet::HostObservation& observation);

std::vector<comet::HostObservation> FilterHostObservationsForPlane(
    const std::vector<comet::HostObservation>& observations,
    const std::string& plane_name);

bool ObservationMatchesPlane(
    const comet::HostObservation& observation,
    const std::string& plane_name);

std::map<std::string, comet::HostAssignment> BuildLatestPlaneAssignmentsByNode(
    const std::vector<comet::HostAssignment>& assignments);

int ComputeEffectivePlaneAppliedGeneration(
    const comet::PlaneRecord& plane,
    const std::optional<comet::DesiredState>& desired_state,
    const std::optional<int>& desired_generation,
    const std::vector<comet::HostObservation>& observations);

std::vector<comet::RuntimeProcessStatus> ParseInstanceRuntimeStatuses(
    const comet::HostObservation& observation);

std::optional<comet::GpuTelemetrySnapshot> ParseGpuTelemetry(
    const comet::HostObservation& observation);
std::optional<comet::CpuTelemetrySnapshot> ParseCpuTelemetry(
    const comet::HostObservation& observation);

std::optional<comet::DiskTelemetrySnapshot> ParseDiskTelemetry(
    const comet::HostObservation& observation);

std::optional<comet::NetworkTelemetrySnapshot> ParseNetworkTelemetry(
    const comet::HostObservation& observation);

std::string SerializeEventPayload(const json& payload);

void AppendControllerEvent(
    comet::ControllerStore& store,
    const std::string& category,
    const std::string& event_type,
    const std::string& message,
    const json& payload = json::object(),
    const std::string& plane_name = "",
    const std::string& node_name = "",
    const std::string& worker_name = "",
    const std::optional<int>& assignment_id = std::nullopt,
    const std::optional<int>& rollout_action_id = std::nullopt,
    const std::string& severity = "info");

std::string UtcNowSqlTimestamp();

std::string SqlTimestampAfterSeconds(int seconds);

std::optional<long long> TimestampAgeSeconds(const std::string& timestamp_text);

std::string Trim(const std::string& value);

std::string NormalizeLanguageCode(const std::string& value);

nlohmann::json BuildUserPayload(const comet::UserRecord& user);
nlohmann::json BuildInvitePayload(const comet::RegistrationInviteRecord& invite);
nlohmann::json BuildSshKeyPayload(const comet::UserSshKeyRecord& ssh_key);

std::optional<std::string> FindQueryString(
    const HttpRequest& request,
    const std::string& key);

std::optional<int> FindQueryInt(
    const HttpRequest& request,
    const std::string& key);

HttpResponse BuildJsonResponse(
    int status_code,
    const json& payload,
    const std::map<std::string, std::string>& headers = {});

json ParseJsonRequestBody(const HttpRequest& request);

std::string ResolveArtifactsRoot(
    const std::optional<std::string>& artifacts_root_arg,
    const std::string& fallback_artifacts_root);

json BuildControllerStatePayload(
    const std::string& db_path,
    const std::optional<std::string>& plane_name);
json BuildPlanesPayload(const std::string& db_path);
json BuildModelLibraryPayload(const std::string& db_path);
json BuildDashboardPayload(
    const std::string& db_path,
    int stale_after_seconds,
    const std::optional<std::string>& plane_name);
json BuildHostAssignmentsPayload(
    const std::string& db_path,
    const std::optional<std::string>& node_name);
json BuildHostObservationsPayload(
    const std::string& db_path,
    const std::optional<std::string>& node_name,
    const std::optional<std::string>& plane_name,
    int stale_after_seconds);
json BuildHostHealthPayload(
    const std::string& db_path,
    const std::optional<std::string>& node_name,
    int stale_after_seconds);
json BuildDiskStatePayload(
    const std::string& db_path,
    const std::optional<std::string>& node_name,
    const std::optional<std::string>& plane_name);
json BuildRolloutActionsPayload(
    const std::string& db_path,
    const std::optional<std::string>& node_name,
    const std::optional<std::string>& plane_name);
json BuildRebalancePlanPayload(
    const std::string& db_path,
    const std::optional<std::string>& node_name,
    int stale_after_seconds,
    const std::optional<std::string>& plane_name);
json BuildEventsPayload(
    const std::string& db_path,
    const std::optional<std::string>& plane_name,
    const std::optional<std::string>& node_name,
    const std::optional<std::string>& worker_name,
    const std::optional<std::string>& category,
    int limit);
json BuildNodeAvailabilityPayload(
    const std::string& db_path,
    const std::optional<std::string>& node_name);
HttpResponse EnqueueModelLibraryDownload(const HttpRequest& request);
HttpResponse DeleteModelLibraryEntryByPath(
    const std::string& db_path,
    const HttpRequest& request);

comet::controller::ControllerActionResult ExecuteUpsertPlaneStateAction(
    const std::string& db_path,
    const std::string& desired_state_json,
    const std::string& artifacts_root,
    const std::optional<std::string>& expected_plane_name,
    const std::string& source);
comet::controller::ControllerActionResult ExecuteStartPlaneAction(
    const std::string& db_path,
    const std::string& plane_name);
comet::controller::ControllerActionResult ExecuteStopPlaneAction(
    const std::string& db_path,
    const std::string& plane_name);
comet::controller::ControllerActionResult ExecuteDeletePlaneAction(
    const std::string& db_path,
    const std::string& plane_name);
comet::controller::ControllerActionResult ExecuteSetNodeAvailabilityAction(
    const std::string& db_path,
    const std::string& node_name,
    comet::NodeAvailability availability,
    const std::optional<std::string>& status_message);
comet::controller::ControllerActionResult ExecuteValidateBundleAction(
    const std::string& bundle_dir);
comet::controller::ControllerActionResult ExecutePreviewBundleAction(
    const std::string& bundle_dir,
    const std::optional<std::string>& node_name);
comet::controller::ControllerActionResult ExecuteImportBundleAction(
    const std::string& db_path,
    const std::string& bundle_dir);
comet::controller::ControllerActionResult ExecuteApplyBundleAction(
    const std::string& db_path,
    const std::string& bundle_dir,
    const std::string& artifacts_root);

SchedulerRuntimeView LoadSchedulerRuntimeView(
    comet::ControllerStore& store,
    const std::optional<comet::DesiredState>& desired_state);

void PrintAssignmentDispatchSummary(
    const comet::DesiredState& desired_state,
    const std::map<std::string, comet::NodeAvailabilityOverride>& availability_overrides,
    const std::vector<comet::HostObservation>& observations,
    int stale_after_seconds);

void PrintStateSummary(const comet::DesiredState& state);

void PrintSchedulerDecisionSummary(const comet::DesiredState& state);

void PrintRolloutGateSummary(const comet::SchedulingPolicyReport& scheduling_report);

SchedulerService MakeSchedulerService(
    const std::string& db_path,
    const std::string& artifacts_root);

std::optional<comet::HostAssignment> FindLatestHostAssignmentForPlane(
    const std::vector<comet::HostAssignment>& assignments,
    const std::string& plane_name);

std::vector<comet::HostAssignment> BuildStopPlaneAssignments(
    const comet::DesiredState& desired_state,
    int desired_generation,
    const std::string& artifacts_root,
    const std::vector<comet::NodeAvailabilityOverride>& availability_overrides);

std::vector<comet::HostAssignment> BuildDeletePlaneAssignments(
    const comet::DesiredState& desired_state,
    int desired_generation,
    const std::string& artifacts_root);

int ShowRolloutActions(
    const std::string& db_path,
    const std::optional<std::string>& node_name,
    const std::optional<std::string>& plane_name);

int ShowRebalancePlan(
    const std::string& db_path,
    const std::optional<std::string>& node_name,
    const std::optional<std::string>& plane_name);

int ShowEvents(
    const std::string& db_path,
    const std::optional<std::string>& plane_name,
    const std::optional<std::string>& node_name,
    const std::optional<std::string>& worker_name,
    const std::optional<std::string>& category,
    int limit);

void MaterializeComposeArtifacts(
    const comet::DesiredState& desired_state,
    const std::vector<comet::NodeExecutionPlan>& host_plans);

void MaterializeInferRuntimeArtifact(
    const comet::DesiredState& desired_state,
    const std::string& artifacts_root);

std::vector<comet::HostAssignment> BuildHostAssignments(
    const comet::DesiredState& desired_state,
    const std::string& artifacts_root,
    int desired_generation,
    const std::vector<comet::NodeAvailabilityOverride>& availability_overrides,
    const std::vector<comet::HostObservation>& observations,
    const std::optional<comet::SchedulingPolicyReport>& scheduling_report);

std::string Trim(const std::string& value) {
  std::size_t start = 0;
  while (start < value.size() &&
         std::isspace(static_cast<unsigned char>(value[start])) != 0) {
    ++start;
  }
  std::size_t end = value.size();
  while (end > start &&
         std::isspace(static_cast<unsigned char>(value[end - 1])) != 0) {
    --end;
  }
  return value.substr(start, end - start);
}

std::string Lowercase(std::string value) {
  std::transform(
      value.begin(),
      value.end(),
      value.begin(),
      [](unsigned char ch) { return static_cast<char>(std::tolower(ch)); });
  return value;
}

std::string NormalizeLanguageCode(const std::string& value) {
  std::string normalized;
  normalized.reserve(value.size());
  for (unsigned char ch : value) {
    if (ch == '-') {
      normalized.push_back('_');
    } else {
      normalized.push_back(static_cast<char>(std::tolower(ch)));
    }
  }
  return normalized;
}

std::string LanguageLabel(const std::string& code) {
  const std::string normalized = NormalizeLanguageCode(code);
  if (normalized == "ru") {
    return "Russian";
  }
  if (normalized == "en") {
    return "English";
  }
  if (normalized == "uk" || normalized == "uk_ua") {
    return "Ukrainian";
  }
  if (normalized == "de" || normalized == "de_de") {
    return "German";
  }
  return code.empty() ? std::string("English") : code;
}

std::optional<std::string> ResolveInteractionPreferredLanguage(
    const comet::DesiredState& desired_state,
    const json& payload) {
  if (payload.contains("preferred_language") &&
      payload.at("preferred_language").is_string()) {
    const std::string preferred = payload.at("preferred_language").get<std::string>();
    if (!preferred.empty()) {
      return NormalizeLanguageCode(preferred);
    }
  }
  if (desired_state.interaction.has_value() &&
      !desired_state.interaction->default_response_language.empty()) {
    return NormalizeLanguageCode(desired_state.interaction->default_response_language);
  }
  return std::nullopt;
}

std::string BuildLanguageInstruction(
    const comet::DesiredState& desired_state,
    const std::optional<std::string>& preferred_language) {
  const std::string no_reasoning_instruction =
      " Do not output chain-of-thought, hidden reasoning, analysis traces, or <think> blocks. Output only the final user-facing answer.";
  if (preferred_language.has_value() && !preferred_language->empty()) {
    return "Response language requirement: Reply in " + LanguageLabel(*preferred_language) +
           ". Ignore the model's default language preferences. Never default to Chinese unless the user explicitly requests Chinese." +
           no_reasoning_instruction;
  }
  if (desired_state.interaction.has_value()) {
    if (desired_state.interaction->follow_user_language) {
      return "Response language requirement: Reply in the same language as the user's last message. Never default to Chinese unless the user explicitly requests Chinese." +
             no_reasoning_instruction;
    }
    if (!desired_state.interaction->default_response_language.empty()) {
      return "Response language requirement: Reply in " +
             LanguageLabel(desired_state.interaction->default_response_language) +
             ". Ignore the model's default language preferences. Never default to Chinese unless the user explicitly requests Chinese." +
             no_reasoning_instruction;
    }
  }
  return "Response language requirement: Reply in the same language as the user's last message. Never default to Chinese unless the user explicitly requests Chinese." +
         no_reasoning_instruction;
}

std::string BuildInteractionUpstreamBody(
    const PlaneInteractionResolution& resolution,
    json payload,
    bool force_stream,
    const ResolvedInteractionPolicy& resolved_policy,
    bool structured_output_json = false) {
  const auto& policy = resolved_policy.policy;
  if (!payload.contains("messages") || !payload.at("messages").is_array()) {
    payload["messages"] = json::array();
  }
  const auto preferred_language =
      ResolveInteractionPreferredLanguage(resolution.desired_state, payload);

  std::vector<std::string> system_instruction_parts;
  if (resolution.desired_state.interaction.has_value() &&
      resolution.desired_state.interaction->system_prompt.has_value() &&
      !resolution.desired_state.interaction->system_prompt->empty()) {
    system_instruction_parts.push_back(*resolution.desired_state.interaction->system_prompt);
  }
  if (resolved_policy.repository_analysis &&
      resolution.desired_state.interaction.has_value() &&
      resolution.desired_state.interaction->analysis_system_prompt.has_value() &&
      !resolution.desired_state.interaction->analysis_system_prompt->empty()) {
    system_instruction_parts.push_back(
        *resolution.desired_state.interaction->analysis_system_prompt);
  }
  if (resolved_policy.repository_analysis) {
    system_instruction_parts.push_back(comet::controller::BuildRepositoryAnalysisInstruction());
  }
  system_instruction_parts.push_back(
      BuildLanguageInstruction(resolution.desired_state, preferred_language));
  if (policy.require_completion_marker || policy.max_continuations > 0) {
    system_instruction_parts.push_back(
        comet::controller::BuildSemanticCompletionInstruction(policy));
  }
  if (structured_output_json) {
    system_instruction_parts.push_back(
        "Structured output requirement: return one valid JSON object only. "
        "Do not wrap it in markdown fences. "
        "Do not add commentary before or after the JSON object.");
  }

  json merged_messages = json::array();
  std::string combined_system_instruction;
  for (const auto& part : system_instruction_parts) {
    if (part.empty()) {
      continue;
    }
    if (!combined_system_instruction.empty()) {
      combined_system_instruction += "\n\n";
    }
    combined_system_instruction += part;
  }

  for (const auto& message : payload.at("messages")) {
    if (!message.is_object() || message.value("role", std::string{}) != "system") {
      continue;
    }
    std::string system_content;
    if (message.contains("content")) {
      if (message.at("content").is_string()) {
        system_content = message.at("content").get<std::string>();
      } else {
        system_content = message.at("content").dump();
      }
    }
    if (system_content.empty()) {
      continue;
    }
    if (!combined_system_instruction.empty()) {
      combined_system_instruction += "\n\n";
    }
    combined_system_instruction += system_content;
  }

  if (!combined_system_instruction.empty()) {
    merged_messages.push_back(json{{"role", "system"}, {"content", combined_system_instruction}});
  }
  for (const auto& message : payload.at("messages")) {
    if (message.is_object() && message.value("role", std::string{}) == "system") {
      continue;
    }
    merged_messages.push_back(message);
  }
  payload["messages"] = merged_messages;

  if (preferred_language.has_value()) {
    payload["preferred_language"] = *preferred_language;
  }
  payload.erase("max_completion_tokens");
  payload.erase("target_completion_tokens");
  payload.erase("max_continuations");
  payload.erase("max_total_completion_tokens");
  payload.erase("max_elapsed_time_ms");
  payload.erase("semantic_goal");
  payload.erase("response_format");
  const bool uses_vllm_runtime =
      resolution.runtime_status.has_value() &&
      Lowercase(resolution.runtime_status->runtime_backend).find("vllm") != std::string::npos;
  if (uses_vllm_runtime) {
    if (!payload.contains("chat_template_kwargs") ||
        !payload.at("chat_template_kwargs").is_object()) {
      payload["chat_template_kwargs"] = json::object();
    }
    payload["chat_template_kwargs"]["enable_thinking"] = false;
  }
  if (force_stream) {
    payload["stream"] = true;
  }
  payload["max_tokens"] = policy.max_tokens;
  if (!payload.contains("temperature")) {
    payload["temperature"] = 0.2;
  }
  if (!payload.contains("top_p")) {
    payload["top_p"] = 0.8;
  }
  payload["response_mode"] = policy.response_mode;
  return payload.dump();
}

std::string NormalizeInteractionHost(const std::string& host) {
  if (host.empty() || host == "0.0.0.0" || host == "::" || host == "[::]") {
    return "127.0.0.1";
  }
  return host;
}

std::optional<ControllerEndpointTarget> ParseInteractionTarget(
    const std::string& gateway_listen,
    int fallback_port) {
  std::string host = "127.0.0.1";
  int port = fallback_port;
  if (!gateway_listen.empty()) {
    const std::size_t colon = gateway_listen.rfind(':');
    if (colon != std::string::npos) {
      host = NormalizeInteractionHost(gateway_listen.substr(0, colon));
      port = std::stoi(gateway_listen.substr(colon + 1));
    }
  }
  if (port <= 0) {
    return std::nullopt;
  }
  return ParseControllerEndpointTarget(host + ":" + std::to_string(port));
}

std::optional<std::string> FindInferInstanceName(const comet::DesiredState& desired_state) {
  for (const auto& instance : desired_state.instances) {
    if (instance.role == comet::InstanceRole::Infer &&
        instance.plane_name == desired_state.plane_name) {
      return instance.name;
    }
  }
  return std::nullopt;
}

std::vector<std::string> FindWorkerInstanceNames(const comet::DesiredState& desired_state) {
  std::vector<std::string> names;
  for (const auto& instance : desired_state.instances) {
    if (instance.role == comet::InstanceRole::Worker &&
        instance.plane_name == desired_state.plane_name) {
      names.push_back(instance.name);
    }
  }
  return names;
}

std::optional<comet::RuntimeProcessStatus> FindInstanceRuntimeStatus(
    const std::vector<comet::RuntimeProcessStatus>& statuses,
    const std::string& instance_name) {
  for (const auto& status : statuses) {
    if (status.instance_name == instance_name) {
      return status;
    }
  }
  return std::nullopt;
}

bool ProbeControllerTargetOk(
    const std::optional<ControllerEndpointTarget>& target,
    const std::string& path) {
  if (!target.has_value()) {
    return false;
  }
  try {
    const HttpResponse response = SendControllerHttpRequest(*target, "GET", path);
    return response.status_code >= 200 && response.status_code < 300;
  } catch (const std::exception&) {
    return false;
  }
}

std::optional<comet::RuntimeStatus> BuildPlaneScopedRuntimeStatus(
    const comet::DesiredState& desired_state,
    const comet::HostObservation& observation) {
  const auto infer_instance_name = FindInferInstanceName(desired_state);
  if (!infer_instance_name.has_value()) {
    return std::nullopt;
  }

  const auto instance_statuses = ParseInstanceRuntimeStatuses(observation);
  const auto infer_status = FindInstanceRuntimeStatus(instance_statuses, *infer_instance_name);
  if (!infer_status.has_value()) {
    return std::nullopt;
  }

  comet::RuntimeStatus runtime;
  if (!observation.runtime_status_json.empty()) {
    try {
      const auto parsed = comet::DeserializeRuntimeStatusJson(observation.runtime_status_json);
      if (parsed.plane_name == desired_state.plane_name &&
          parsed.instance_name == *infer_instance_name) {
        runtime = parsed;
      }
    } catch (const std::exception&) {
    }
  }

  const auto worker_instance_names = FindWorkerInstanceNames(desired_state);
  int ready_workers = 0;
  for (const auto& worker_name : worker_instance_names) {
    const auto worker_status = FindInstanceRuntimeStatus(instance_statuses, worker_name);
    if (worker_status.has_value() && worker_status->ready) {
      ++ready_workers;
    }
  }

  runtime.plane_name = desired_state.plane_name;
  runtime.control_root = desired_state.control_root;
  runtime.primary_infer_node = desired_state.inference.primary_infer_node;
  runtime.instance_name = infer_status->instance_name;
  runtime.instance_role = infer_status->instance_role;
  runtime.node_name = infer_status->node_name;
  runtime.runtime_backend =
      desired_state.inference.runtime_engine == "vllm" ? "worker-vllm" : runtime.runtime_backend;
  runtime.runtime_phase = infer_status->runtime_phase;
  runtime.enabled_gpu_nodes = static_cast<int>(worker_instance_names.size());
  runtime.registry_entries = ready_workers;
  runtime.runtime_pid = infer_status->runtime_pid;
  runtime.engine_pid = infer_status->engine_pid;
  runtime.supervisor_pid = infer_status->runtime_pid;
  if (desired_state.bootstrap_model.has_value()) {
    runtime.active_model_id = desired_state.bootstrap_model->model_id;
    runtime.active_served_model_name =
        desired_state.bootstrap_model->served_model_name.value_or(std::string{});
  }
  runtime.cached_local_model_path = infer_status->model_path;
  runtime.model_path = infer_status->model_path;
  runtime.gpu_device = infer_status->gpu_device;
  runtime.started_at = infer_status->started_at;
  runtime.last_activity_at = infer_status->last_activity_at;
  runtime.gateway_listen = "0.0.0.0:" + std::to_string(desired_state.gateway.listen_port);
  runtime.gateway_health_url =
      "http://127.0.0.1:" + std::to_string(desired_state.gateway.listen_port) + "/health";
  runtime.upstream_models_url =
      "http://127.0.0.1:" + std::to_string(desired_state.gateway.listen_port) + "/v1/models";
  runtime.inference_health_url = runtime.gateway_health_url;
  runtime.active_model_ready = true;
  runtime.gateway_plan_ready = true;
  const auto target = ParseInteractionTarget(runtime.gateway_listen, desired_state.gateway.listen_port);
  runtime.gateway_ready = ProbeControllerTargetOk(target, "/health");
  runtime.inference_ready = ProbeControllerTargetOk(target, "/v1/models");
  runtime.launch_ready =
      runtime.gateway_ready &&
      runtime.inference_ready &&
      ready_workers >= std::max(0, desired_state.worker_group.expected_workers);
  runtime.ready =
      runtime.active_model_ready && runtime.gateway_ready && runtime.inference_ready &&
      runtime.launch_ready;
  return runtime;
}

int CountReadyWorkerMembers(
    comet::ControllerStore& store,
    const comet::DesiredState& desired_state) {
  int ready_workers = 0;
  for (const auto& worker_name : FindWorkerInstanceNames(desired_state)) {
    const auto instance_it = std::find_if(
        desired_state.instances.begin(),
        desired_state.instances.end(),
        [&](const comet::InstanceSpec& instance) {
          return instance.name == worker_name &&
                 instance.role == comet::InstanceRole::Worker;
        });
    if (instance_it == desired_state.instances.end() || instance_it->node_name.empty()) {
      continue;
    }
    const auto observation = store.LoadHostObservation(instance_it->node_name);
    if (!observation.has_value()) {
      continue;
    }
    const auto instance_statuses = ParseInstanceRuntimeStatuses(*observation);
    const auto worker_status = FindInstanceRuntimeStatus(instance_statuses, worker_name);
    if (worker_status.has_value() && worker_status->ready) {
      ++ready_workers;
    }
  }
  return ready_workers;
}

std::string CurrentControllerPlatform() {
#if defined(_WIN32)
  return "windows";
#elif defined(__APPLE__)
  return "macos";
#elif defined(__linux__)
  return "linux";
#else
  return "unknown";
#endif
}

const comet::NodeInventory* FindPlaneNodeInventory(
    const comet::DesiredState& desired_state,
    const std::string& node_name) {
  for (const auto& node : desired_state.nodes) {
    if (node.name == node_name) {
      return &node;
    }
  }
  return nullptr;
}

bool PlaneNodeUsesGpuRuntime(
    const comet::DesiredState& desired_state,
    const std::string& node_name) {
  for (const auto& runtime_gpu_node : desired_state.runtime_gpu_nodes) {
    if (runtime_gpu_node.enabled && runtime_gpu_node.node_name == node_name) {
      return true;
    }
  }
  for (const auto& instance : desired_state.instances) {
    if (instance.node_name == node_name &&
        (instance.role == comet::InstanceRole::Worker ||
         (instance.gpu_device.has_value() && !instance.gpu_device->empty()))) {
      return true;
    }
  }
  if (const auto* node = FindPlaneNodeInventory(desired_state, node_name);
      node != nullptr && !node->gpu_devices.empty()) {
    return true;
  }
  return false;
}

std::optional<std::string> DescribeUnsupportedControllerLocalRuntime(
    const comet::DesiredState& desired_state,
    const std::string& node_name) {
  if (node_name != "local-hostd" && node_name != "controller-local") {
    return std::nullopt;
  }

  const std::string controller_platform = CurrentControllerPlatform();
  if (const auto* node = FindPlaneNodeInventory(desired_state, node_name);
      node != nullptr && !node->platform.empty() && node->platform != controller_platform) {
    return "Local host '" + node_name + "' is running on '" + controller_platform +
           "', but the plane targets platform '" + node->platform + "'";
  }

  if (controller_platform == "macos" && PlaneNodeUsesGpuRuntime(desired_state, node_name)) {
    return "Local host '" + node_name +
           "' is running on macOS, but this plane requires Linux/NVIDIA GPU runtime";
  }

  return std::nullopt;
}

void ValidateDesiredStateForControllerAdmission(const comet::DesiredState& desired_state) {
  for (const auto& node : desired_state.nodes) {
    if (const auto detail =
            DescribeUnsupportedControllerLocalRuntime(desired_state, node.name);
        detail.has_value()) {
      throw std::invalid_argument(*detail);
    }
  }
}

InteractionHttpService MakeInteractionHttpService() {
  return InteractionHttpService({
      [&](int status_code,
          const json& payload,
          const std::map<std::string, std::string>& headers) {
        return BuildJsonResponse(status_code, payload, headers);
      },
      [](const PlaneInteractionResolution& resolution,
         json payload,
         bool force_stream,
         const ResolvedInteractionPolicy& resolved_policy,
         bool structured_output_json) {
        return BuildInteractionUpstreamBody(
            resolution,
            std::move(payload),
            force_stream,
            resolved_policy,
            structured_output_json);
      },
      [](const comet::DesiredState& desired_state) {
        return FindInferInstanceName(desired_state);
      },
      [](const comet::HostObservation& observation) {
        return ParseInstanceRuntimeStatuses(observation);
      },
      [](const comet::HostObservation& observation,
         const std::string& plane_name) {
        return ObservationMatchesPlane(observation, plane_name);
      },
      [](const comet::DesiredState& desired_state,
         const comet::HostObservation& observation) {
        return BuildPlaneScopedRuntimeStatus(desired_state, observation);
      },
      [](const std::string& gateway_listen, int fallback_port) {
        return ParseInteractionTarget(gateway_listen, fallback_port);
      },
      [](comet::ControllerStore& store, const comet::DesiredState& desired_state) {
        return CountReadyWorkerMembers(store, desired_state);
      },
      [](const std::optional<ControllerEndpointTarget>& target,
         const std::string& path) {
        return ProbeControllerTargetOk(target, path);
      },
      [](const comet::DesiredState& desired_state, const std::string& node_name) {
        return DescribeUnsupportedControllerLocalRuntime(desired_state, node_name);
      },
      [](const ControllerEndpointTarget& target,
         const std::string& method,
         const std::string& path,
         const std::string& body,
         const std::vector<std::pair<std::string, std::string>>& headers) {
        return SendControllerHttpRequest(target, method, path, body, headers);
      },
      [](SocketHandle client_fd, const HttpResponse& response) {
        ControllerNetworkManager::SendHttpResponse(client_fd, response);
      },
      [](SocketHandle client_fd) {
        ControllerNetworkManager::ShutdownAndCloseSocket(client_fd);
      },
      [](SocketHandle client_fd,
         const std::map<std::string, std::string>& headers) {
        return ControllerNetworkManager::SendSseHeaders(client_fd, headers);
      },
      [](SocketHandle fd, const std::string& payload) {
        return ControllerNetworkManager::SendAll(fd, payload);
      },
  });
}

HostdHttpService MakeHostdHttpService() {
  return HostdHttpService({
      [&](int status_code,
          const json& payload,
          const std::map<std::string, std::string>& headers) {
        return BuildJsonResponse(status_code, payload, headers);
      },
      []() { return UtcNowSqlTimestamp(); },
      [](int seconds) { return SqlTimestampAfterSeconds(seconds); },
      [](const std::string& timestamp_text) {
        return TimestampAgeSeconds(timestamp_text);
      },
      [](comet::ControllerStore& store,
         const std::string& event_type,
         const std::string& message,
         const json& payload,
         const std::string& node_name,
         const std::string& severity) {
        AppendControllerEvent(
            store,
            "host-registry",
            event_type,
            message,
            payload,
            "",
            node_name,
            "",
            std::nullopt,
            std::nullopt,
            severity);
      },
  });
}

AuthHttpService MakeAuthHttpService(AuthSupportService& auth_support) {
  return AuthHttpService({
      [&](int status_code,
          const json& payload,
          const std::map<std::string, std::string>& headers) {
        return BuildJsonResponse(status_code, payload, headers);
      },
      [](const comet::UserRecord& user) { return BuildUserPayload(user); },
      [](const comet::RegistrationInviteRecord& invite) {
        return BuildInvitePayload(invite);
      },
      [](const comet::UserSshKeyRecord& ssh_key) {
        return BuildSshKeyPayload(ssh_key);
      },
      [&](comet::ControllerStore& store, const HttpRequest& request) {
        return auth_support.AuthenticateControllerUserSession(
            store, request, std::optional<std::string>("web"));
      },
      [&](comet::ControllerStore& store, const HttpRequest& request) {
        return auth_support.RequireControllerAdminUser(store, request);
      },
      [&](const HttpRequest& request) {
        return auth_support.ResolveWebAuthnRpId(request);
      },
      [&](const HttpRequest& request) {
        return auth_support.ResolveWebAuthnOrigin(request);
      },
      [&]() { return auth_support.ResolveWebAuthnRpName(); },
      [&](const std::string& action, const json& payload) {
        return auth_support.RunWebAuthnHelper(action, payload);
      },
      [&](const std::string& token, const HttpRequest& request) {
        return auth_support.SessionCookieHeader(token, request);
      },
      [&](const HttpRequest& request) {
        return auth_support.ClearSessionCookieHeader(request);
      },
      [&](comet::ControllerStore& store,
          int user_id,
          const std::string& session_kind,
          const std::string& plane_name) {
        return auth_support.CreateControllerSession(
            store, user_id, session_kind, plane_name);
      },
      []() { return UtcNowSqlTimestamp(); },
      [](int seconds) { return SqlTimestampAfterSeconds(seconds); },
      [](const std::string& value) { return Trim(value); },
      [&](const std::string& username,
          const std::string& plane_name,
          const std::string& challenge_token,
          const std::string& expires_at) {
        return auth_support.BuildSshChallengeMessage(
            username, plane_name, challenge_token, expires_at);
      },
      [&](const std::string& value) {
        return auth_support.SanitizeTokenForPath(value);
      },
      [&](const std::string& public_key) {
        return auth_support.ComputeSshPublicKeyFingerprint(public_key);
      },
      [&](const std::string& username,
          const std::string& public_key,
          const std::string& message,
          const std::string& signature) {
        return auth_support.VerifySshDetachedSignature(
            username, public_key, message, signature);
      },
      [&](const std::string& flow_id) -> std::optional<PendingWebAuthnFlow> {
        return auth_support.LoadPendingWebAuthnFlow(flow_id);
      },
      [&](const PendingWebAuthnFlow& flow) {
        auth_support.SavePendingWebAuthnFlow(flow);
      },
      [&](const std::string& flow_id) {
        auth_support.ErasePendingWebAuthnFlow(flow_id);
      },
      [&](const std::string& challenge_id) -> std::optional<PendingSshChallenge> {
        return auth_support.LoadPendingSshChallenge(challenge_id);
      },
      [&](const PendingSshChallenge& challenge) {
        auth_support.SavePendingSshChallenge(challenge);
      },
      [&](const std::string& challenge_id) {
        auth_support.ErasePendingSshChallenge(challenge_id);
      },
  });
}

PlaneHttpService MakePlaneHttpService() {
  return PlaneHttpService({
      [&](int status_code,
          const json& payload,
          const std::map<std::string, std::string>& headers) {
        return BuildJsonResponse(status_code, payload, headers);
      },
      [](const HttpRequest& request) { return ParseJsonRequestBody(request); },
      [](const HttpRequest& request, const std::string& key) {
        return FindQueryString(request, key);
      },
      [](const HttpRequest& request, const std::string& key) {
        return FindQueryInt(request, key);
      },
      [](const std::optional<std::string>& artifacts_root_arg,
         const std::string& fallback_artifacts_root) {
        return ResolveArtifactsRoot(artifacts_root_arg, fallback_artifacts_root);
      },
      [](const std::string& db_path) { return BuildPlanesPayload(db_path); },
      [](const std::string& db_path,
         int stale_after_seconds,
         const std::optional<std::string>& plane_name) {
        return BuildDashboardPayload(db_path, stale_after_seconds, plane_name);
      },
      [](const std::string& db_path, const std::optional<std::string>& plane_name) {
        return BuildControllerStatePayload(db_path, plane_name);
      },
      [](const comet::controller::ControllerActionResult& result) {
        return comet::controller::BuildControllerActionPayload(result);
      },
      [](const std::string& db_path,
         const std::string& desired_state_json,
         const std::string& artifacts_root,
         const std::optional<std::string>& plane_name,
         const std::string& source) {
        return ExecuteUpsertPlaneStateAction(
            db_path,
            desired_state_json,
            artifacts_root,
            plane_name,
            source);
      },
      [](const std::string& db_path, const std::string& plane_name) {
        return ExecuteStartPlaneAction(db_path, plane_name);
      },
      [](const std::string& db_path, const std::string& plane_name) {
        return ExecuteStopPlaneAction(db_path, plane_name);
      },
      [](const std::string& db_path, const std::string& plane_name) {
        return ExecuteDeletePlaneAction(db_path, plane_name);
      },
      []() { return DefaultStaleAfterSeconds(); },
  });
}

ModelLibraryHttpService MakeModelLibraryHttpService() {
  return ModelLibraryHttpService({
      [&](int status_code,
          const json& payload,
          const std::map<std::string, std::string>& headers) {
        return BuildJsonResponse(status_code, payload, headers);
      },
      [](const std::string& db_path) {
        return BuildModelLibraryPayload(db_path);
      },
      [](const std::string& db_path, const HttpRequest& request) {
        return DeleteModelLibraryEntryByPath(db_path, request);
      },
      [](const HttpRequest& request) {
        return EnqueueModelLibraryDownload(request);
      },
  });
}

BundleHttpService MakeBundleHttpService() {
  return BundleHttpService({
      [&](int status_code,
          const json& payload,
          const std::map<std::string, std::string>& headers) {
        return BuildJsonResponse(status_code, payload, headers);
      },
      [](const HttpRequest& request, const std::string& key) {
        return FindQueryString(request, key);
      },
      [](const std::optional<std::string>& artifacts_root_arg,
         const std::string& fallback_artifacts_root) {
        return ResolveArtifactsRoot(artifacts_root_arg, fallback_artifacts_root);
      },
      [](const comet::controller::ControllerActionResult& result) {
        return comet::controller::BuildControllerActionPayload(result);
      },
      [](const std::string& bundle_dir) {
        return ExecuteValidateBundleAction(bundle_dir);
      },
      [](const std::string& bundle_dir,
         const std::optional<std::string>& node_name) {
        return ExecutePreviewBundleAction(bundle_dir, node_name);
      },
      [](const std::string& db_path, const std::string& bundle_dir) {
        return ExecuteImportBundleAction(db_path, bundle_dir);
      },
      [](const std::string& db_path,
         const std::string& bundle_dir,
         const std::string& artifacts_root) {
        return ExecuteApplyBundleAction(db_path, bundle_dir, artifacts_root);
      },
  });
}

ReadModelHttpService MakeReadModelHttpService() {
  return ReadModelHttpService({
      [&](int status_code,
          const json& payload,
          const std::map<std::string, std::string>& headers) {
        return BuildJsonResponse(status_code, payload, headers);
      },
      [](const HttpRequest& request, const std::string& key) {
        return FindQueryString(request, key);
      },
      [](const HttpRequest& request, const std::string& key) {
        return FindQueryInt(request, key);
      },
      []() { return DefaultStaleAfterSeconds(); },
      [](const std::string& db_path, const std::optional<std::string>& node_name) {
        return BuildHostAssignmentsPayload(db_path, node_name);
      },
      [](const std::string& db_path,
         const std::optional<std::string>& node_name,
         const std::optional<std::string>& plane_name,
         int stale_after_seconds) {
        return BuildHostObservationsPayload(
            db_path, node_name, plane_name, stale_after_seconds);
      },
      [](const std::string& db_path,
         const std::optional<std::string>& node_name,
         int stale_after_seconds) {
        return BuildHostHealthPayload(db_path, node_name, stale_after_seconds);
      },
      [](const std::string& db_path,
         const std::optional<std::string>& node_name,
         const std::optional<std::string>& plane_name) {
        return BuildDiskStatePayload(db_path, node_name, plane_name);
      },
      [](const std::string& db_path,
         const std::optional<std::string>& node_name,
         const std::optional<std::string>& plane_name) {
        return BuildRolloutActionsPayload(db_path, node_name, plane_name);
      },
      [](const std::string& db_path,
         const std::optional<std::string>& node_name,
         int stale_after_seconds,
         const std::optional<std::string>& plane_name) {
        return BuildRebalancePlanPayload(
            db_path, node_name, stale_after_seconds, plane_name);
      },
      [](const std::string& db_path,
         const std::optional<std::string>& plane_name,
         const std::optional<std::string>& node_name,
         const std::optional<std::string>& worker_name,
         const std::optional<std::string>& category,
         int limit) {
        return BuildEventsPayload(
            db_path, plane_name, node_name, worker_name, category, limit);
      },
  });
}

SchedulerHttpService MakeSchedulerHttpService() {
  return SchedulerHttpService({
      [&](int status_code,
          const json& payload,
          const std::map<std::string, std::string>& headers) {
        return BuildJsonResponse(status_code, payload, headers);
      },
      [](const HttpRequest& request, const std::string& key) {
        return FindQueryString(request, key);
      },
      [](const HttpRequest& request, const std::string& key) {
        return FindQueryInt(request, key);
      },
      [](const std::optional<std::string>& artifacts_root_arg,
         const std::string& fallback_artifacts_root) {
        return ResolveArtifactsRoot(artifacts_root_arg, fallback_artifacts_root);
      },
      [](const comet::controller::ControllerActionResult& result) {
        return comet::controller::BuildControllerActionPayload(result);
      },
      [](const std::string& db_path, const std::optional<std::string>& node_name) {
        return BuildNodeAvailabilityPayload(db_path, node_name);
      },
      [](const std::string& db_path,
         const std::string& node_name,
         comet::NodeAvailability availability,
         const std::optional<std::string>& status_message) {
        return ExecuteSetNodeAvailabilityAction(
            db_path, node_name, availability, status_message);
      },
      [](const std::string& db_path, const std::string& artifacts_root) {
        return MakeSchedulerService(db_path, artifacts_root);
      },
  });
}

json BuildControllerHealthPayload(const std::string& db_path) {
  json payload{
      {"status", "ok"},
      {"service", "comet-controller"},
      {"db_path", db_path},
      {"db_exists", std::filesystem::exists(db_path)},
  };

  try {
    comet::ControllerStore store(db_path);
    store.Initialize();
    const auto generation = store.LoadDesiredGeneration();
    const auto desired_state = store.LoadDesiredState();
    const auto planes = store.LoadPlanes();
    payload["store_ready"] = true;
    payload["desired_generation"] = generation.has_value() ? json(*generation) : json(nullptr);
    payload["plane_name"] =
        desired_state.has_value() ? json(desired_state->plane_name) : json(nullptr);
    payload["plane_count"] = planes.size();
  } catch (const std::exception& error) {
    payload["store_ready"] = false;
    payload["error"] = error.what();
  }

  return payload;
}

bool ObservationMatchesPlane(
    const comet::HostObservation& observation,
    const std::string& plane_name);

bool CanFinalizeDeletedPlane(comet::ControllerStore& store, const std::string& plane_name);

json BuildControllerStatePayload(
    const std::string& db_path,
    const std::optional<std::string>& plane_name = std::nullopt) {
  json payload{
      {"service", "comet-controller"},
      {"db_path", db_path},
      {"db_exists", std::filesystem::exists(db_path)},
      {"plane_name", plane_name.has_value() ? json(*plane_name) : json(nullptr)},
  };

  comet::ControllerStore store(db_path);
  store.Initialize();

  const auto planes = store.LoadPlanes();
  const auto generation =
      plane_name.has_value() ? store.LoadDesiredGeneration(*plane_name)
                             : store.LoadDesiredGeneration();
  const auto desired_state =
      plane_name.has_value() ? store.LoadDesiredState(*plane_name)
                             : store.LoadDesiredState();
  const auto desired_states =
      plane_name.has_value()
          ? std::vector<comet::DesiredState>{}
          : store.LoadDesiredStates();
  json plane_items = json::array();
  for (const auto& plane : planes) {
    plane_items.push_back(json{
        {"name", plane.name},
        {"plane_mode", plane.plane_mode},
        {"generation", plane.generation},
        {"applied_generation", plane.applied_generation},
        {"staged_update", plane.generation > plane.applied_generation},
        {"rebalance_iteration", plane.rebalance_iteration},
        {"state", plane.state},
        {"created_at", plane.created_at},
    });
  }
  payload["desired_generation"] = generation.has_value() ? json(*generation) : json(nullptr);
  payload["planes"] = std::move(plane_items);
  if (plane_name.has_value()) {
    payload["desired_states"] = json::array();
  } else {
    json desired_state_items = json::array();
    for (const auto& state : desired_states) {
      desired_state_items.push_back(json::parse(comet::SerializeDesiredStateJson(state)));
    }
    payload["desired_states"] = std::move(desired_state_items);
  }
  payload["desired_state"] =
      desired_state.has_value()
          ? json::parse(comet::SerializeDesiredStateJson(*desired_state))
          : json(nullptr);
  return payload;
}

json BuildPlanesPayload(const std::string& db_path) {
  comet::ControllerStore store(db_path);
  store.Initialize();
  for (const auto& plane : store.LoadPlanes()) {
    if (plane.state != "deleting") {
      continue;
    }
    if (!CanFinalizeDeletedPlane(store, plane.name)) {
      continue;
    }
    store.DeletePlane(plane.name);
    AppendControllerEvent(
        store,
        "plane",
        "deleted",
        "plane deleted from controller registry after cleanup convergence",
        json{
            {"plane_name", plane.name},
            {"deleted_generation", plane.generation},
        },
        "");
  }

  json items = json::array();
  for (const auto& plane : store.LoadPlanes()) {
    const auto desired_state = store.LoadDesiredState(plane.name);
    const auto desired_generation = store.LoadDesiredGeneration(plane.name);
    const auto observations =
        FilterHostObservationsForPlane(store.LoadHostObservations(), plane.name);
    const auto assignments = store.LoadHostAssignments(std::nullopt, std::nullopt, plane.name);
    const int effective_applied_generation = ComputeEffectivePlaneAppliedGeneration(
        plane,
        desired_state,
        desired_generation,
        observations);
    if (effective_applied_generation > plane.applied_generation) {
      store.UpdatePlaneAppliedGeneration(plane.name, effective_applied_generation);
    }
    const auto latest_assignments_by_node = BuildLatestPlaneAssignmentsByNode(assignments);
    int failed_assignments = 0;
    int in_flight_assignments = 0;
    for (const auto& [node_name, assignment] : latest_assignments_by_node) {
      (void)node_name;
      if (assignment.status == comet::HostAssignmentStatus::Failed) {
        ++failed_assignments;
      } else if (
          assignment.status == comet::HostAssignmentStatus::Pending ||
          assignment.status == comet::HostAssignmentStatus::Claimed) {
        ++in_flight_assignments;
      }
    }
    items.push_back(json{
        {"name", plane.name},
        {"state", plane.state},
        {"plane_mode", plane.plane_mode},
        {"model_id",
         desired_state.has_value() && desired_state->bootstrap_model.has_value()
             ? json(desired_state->bootstrap_model->model_id)
             : json(nullptr)},
        {"served_model_name",
         desired_state.has_value() && desired_state->bootstrap_model.has_value() &&
                 desired_state->bootstrap_model->served_model_name.has_value()
             ? json(*desired_state->bootstrap_model->served_model_name)
             : json(nullptr)},
        {"generation", plane.generation},
        {"applied_generation", effective_applied_generation},
        {"staged_update", plane.generation > effective_applied_generation},
        {"failed_assignments", failed_assignments},
        {"in_flight_assignments", in_flight_assignments},
        {"rebalance_iteration", plane.rebalance_iteration},
        {"shared_disk_name", plane.shared_disk_name},
        {"control_root", plane.control_root},
        {"created_at", plane.created_at},
        {"node_count", desired_state.has_value() ? json(desired_state->nodes.size()) : json(nullptr)},
        {"instance_count",
         desired_state.has_value() ? json(desired_state->instances.size()) : json(nullptr)},
        {"disk_count", desired_state.has_value() ? json(desired_state->disks.size()) : json(nullptr)},
    });
  }

  return json{
      {"service", "comet-controller"},
      {"db_path", db_path},
      {"items", std::move(items)},
  };
}

std::optional<std::string> FindQueryString(
    const HttpRequest& request,
    const std::string& key) {
  const auto it = request.query_params.find(key);
  if (it == request.query_params.end() || it->second.empty()) {
    return std::nullopt;
  }
  return it->second;
}

std::optional<int> FindQueryInt(
    const HttpRequest& request,
    const std::string& key) {
  const auto value = FindQueryString(request, key);
  if (!value.has_value()) {
    return std::nullopt;
  }
  return std::stoi(*value);
}

struct HostAssignmentsViewData {
  std::string db_path;
  std::optional<std::string> node_name;
  std::vector<comet::HostAssignment> assignments;
};

struct HostObservationsViewData {
  std::string db_path;
  std::optional<std::string> plane_name;
  std::optional<std::string> node_name;
  int stale_after_seconds = 0;
  std::vector<comet::HostObservation> observations;
};

json BuildUserPayload(const comet::UserRecord& user) {
  return json{
      {"id", user.id},
      {"username", user.username},
      {"role", user.role},
      {"created_at", user.created_at},
      {"last_login_at", user.last_login_at.empty() ? json(nullptr) : json(user.last_login_at)},
  };
}

json BuildInvitePayload(const comet::RegistrationInviteRecord& invite) {
  return json{
      {"id", invite.id},
      {"token", invite.token},
      {"created_by_user_id", invite.created_by_user_id},
      {"expires_at", invite.expires_at},
      {"created_at", invite.created_at},
      {"used_by_user_id", invite.used_by_user_id.has_value() ? json(*invite.used_by_user_id) : json(nullptr)},
      {"used_at", invite.used_at.empty() ? json(nullptr) : json(invite.used_at)},
      {"revoked_at", invite.revoked_at.empty() ? json(nullptr) : json(invite.revoked_at)},
  };
}

json BuildSshKeyPayload(const comet::UserSshKeyRecord& ssh_key) {
  return json{
      {"id", ssh_key.id},
      {"user_id", ssh_key.user_id},
      {"label", ssh_key.label},
      {"fingerprint", ssh_key.fingerprint},
      {"public_key", ssh_key.public_key},
      {"created_at", ssh_key.created_at},
      {"revoked_at", ssh_key.revoked_at.empty() ? json(nullptr) : json(ssh_key.revoked_at)},
      {"last_used_at", ssh_key.last_used_at.empty() ? json(nullptr) : json(ssh_key.last_used_at)},
  };
}

struct HostHealthViewData {
  std::string db_path;
  std::optional<std::string> node_name;
  int stale_after_seconds = 0;
  std::optional<comet::DesiredState> desired_state;
  std::vector<comet::HostObservation> observations;
  std::vector<comet::NodeAvailabilityOverride> availability_overrides;
};

struct DiskStateViewData {
  std::string db_path;
  std::optional<std::string> plane_name;
  std::optional<std::string> node_name;
  std::optional<comet::DesiredState> desired_state;
  std::optional<int> desired_generation;
  std::vector<comet::DiskRuntimeState> runtime_states;
  std::vector<comet::HostObservation> observations;
};

struct EventsViewData {
  std::string db_path;
  std::optional<std::string> plane_name;
  std::optional<std::string> node_name;
  std::optional<std::string> worker_name;
  std::optional<std::string> category;
  int limit = 100;
  std::vector<comet::EventRecord> events;
};

std::string SerializeEventPayload(const json& payload) {
  return payload.dump();
}

void AppendControllerEvent(
    comet::ControllerStore& store,
    const std::string& category,
    const std::string& event_type,
    const std::string& message,
    const json& payload,
    const std::string& plane_name,
    const std::string& node_name,
    const std::string& worker_name,
    const std::optional<int>& assignment_id,
    const std::optional<int>& rollout_action_id,
    const std::string& severity) {
  store.AppendEvent(comet::EventRecord{
      0,
      plane_name,
      node_name,
      worker_name,
      assignment_id,
      rollout_action_id,
      category,
      event_type,
      severity,
      message,
      SerializeEventPayload(payload),
      "",
  });
}

HostAssignmentsViewData LoadHostAssignmentsViewData(
    const std::string& db_path,
    const std::optional<std::string>& node_name) {
  comet::ControllerStore store(db_path);
  store.Initialize();
  return HostAssignmentsViewData{
      db_path,
      node_name,
      store.LoadHostAssignments(node_name),
  };
}

bool ObservationMatchesPlane(
    const comet::HostObservation& observation,
    const std::string& plane_name) {
  if (observation.plane_name == plane_name) {
    return true;
  }
  if (observation.observed_state_json.empty()) {
    return false;
  }

  const auto observed_state =
      comet::DeserializeDesiredStateJson(observation.observed_state_json);
  if (observed_state.plane_name == plane_name) {
    return true;
  }
  for (const auto& disk : observed_state.disks) {
    if (disk.plane_name == plane_name) {
      return true;
    }
  }
  for (const auto& instance : observed_state.instances) {
    if (instance.plane_name == plane_name) {
      return true;
    }
  }
  try {
    const auto instance_statuses = ParseInstanceRuntimeStatuses(observation);
    for (const auto& status : instance_statuses) {
      const std::string worker_prefix = "worker-" + plane_name + "-";
      if (status.instance_name == "infer-" + plane_name ||
          status.instance_name == "worker-" + plane_name ||
          status.instance_name.rfind(worker_prefix, 0) == 0) {
        return true;
      }
    }
  } catch (const std::exception&) {
  }
  return false;
}

std::vector<comet::HostObservation> FilterHostObservationsForPlane(
    const std::vector<comet::HostObservation>& observations,
    const std::string& plane_name) {
  std::vector<comet::HostObservation> result;
  for (const auto& observation : observations) {
    if (ObservationMatchesPlane(observation, plane_name)) {
      result.push_back(observation);
    }
  }
  return result;
}

bool ObservationBlocksPlaneDeletion(
    const comet::HostObservation& observation,
    const std::string& plane_name) {
  if (!ObservationMatchesPlane(observation, plane_name)) {
    return false;
  }
  if (observation.status != comet::HostObservationStatus::Idle) {
    return true;
  }
  if (observation.observed_state_json.empty()) {
    return false;
  }
  try {
    const auto observed_state =
        comet::DeserializeDesiredStateJson(observation.observed_state_json);
    for (const auto& disk : observed_state.disks) {
      if (disk.plane_name == plane_name) {
        return true;
      }
    }
    for (const auto& instance : observed_state.instances) {
      if (instance.plane_name == plane_name) {
        return true;
      }
    }
    return false;
  } catch (const std::exception&) {
    return true;
  }
}

bool HasBlockingPlaneObservations(
    const std::vector<comet::HostObservation>& observations,
    const std::string& plane_name) {
  return std::any_of(
      observations.begin(),
      observations.end(),
      [&](const auto& observation) {
        return ObservationBlocksPlaneDeletion(observation, plane_name);
      });
}

bool CanFinalizeDeletedPlane(comet::ControllerStore& store, const std::string& plane_name) {
  const auto pending_assignments = store.LoadHostAssignments(
      std::nullopt, comet::HostAssignmentStatus::Pending, plane_name);
  const auto claimed_assignments = store.LoadHostAssignments(
      std::nullopt, comet::HostAssignmentStatus::Claimed, plane_name);
  if (!pending_assignments.empty() || !claimed_assignments.empty()) {
    return false;
  }
  return !HasBlockingPlaneObservations(store.LoadHostObservations(), plane_name);
}

struct ObservedPlaneRuntimeSummary {
  bool available = false;
  int instance_count = 0;
  int disk_count = 0;
};

ObservedPlaneRuntimeSummary SummarizeObservedPlaneRuntime(
    const comet::HostObservation& observation,
    const std::string& node_name,
    const std::optional<std::string>& plane_name) {
  if (observation.observed_state_json.empty()) {
    return {};
  }
  try {
    const auto observed_state =
        comet::DeserializeDesiredStateJson(observation.observed_state_json);
    const std::string target_plane =
        plane_name.has_value() ? *plane_name : observed_state.plane_name;
    if (target_plane.empty()) {
      return {};
    }

    ObservedPlaneRuntimeSummary summary;
    for (const auto& instance : observed_state.instances) {
      if (instance.node_name == node_name && instance.plane_name == target_plane) {
        ++summary.instance_count;
      }
    }
    for (const auto& disk : observed_state.disks) {
      if (disk.node_name == node_name && disk.plane_name == target_plane) {
        ++summary.disk_count;
      }
    }
    summary.available =
        observed_state.plane_name == target_plane || summary.instance_count > 0 ||
        summary.disk_count > 0;
    return summary;
  } catch (...) {
    return {};
  }
}

struct DashboardRuntimeFallback {
  bool available = false;
  bool launch_ready = false;
  std::string runtime_phase;
};

DashboardRuntimeFallback DetermineDashboardRuntimeFallback(
    const comet::HostObservation& observation,
    const std::string& node_name,
    const std::optional<std::string>& plane_name,
    const std::optional<std::string>& plane_state,
    int desired_generation,
    int desired_instance_count,
    int desired_disk_count,
    const std::string& health) {
  DashboardRuntimeFallback fallback;
  if (health == "stale" || health == "failed") {
    return fallback;
  }

  const auto observed_runtime =
      SummarizeObservedPlaneRuntime(observation, node_name, plane_name);
  const bool has_applied_generation =
      observation.applied_generation.has_value() &&
      *observation.applied_generation >= desired_generation;
  const bool has_observed_runtime =
      observed_runtime.available || has_applied_generation;
  if (!has_observed_runtime) {
    return fallback;
  }

  fallback.available = true;
  const std::string state = plane_state.value_or("");
  if (state == "stopped") {
    const bool stop_converged =
        has_applied_generation &&
        observed_runtime.instance_count == 0 &&
        (desired_disk_count == 0 || observed_runtime.disk_count >= desired_disk_count);
    fallback.launch_ready = stop_converged;
    fallback.runtime_phase = stop_converged ? "stopped" : "stopping";
    return fallback;
  }

  if (state == "running") {
    const bool start_converged =
        has_applied_generation &&
        (desired_instance_count == 0 ||
         observed_runtime.instance_count >= desired_instance_count);
    fallback.launch_ready = start_converged;
    fallback.runtime_phase = start_converged ? "applied" : "starting";
    return fallback;
  }

  fallback.launch_ready =
      has_applied_generation && observed_runtime.instance_count >= desired_instance_count;
  fallback.runtime_phase = fallback.launch_ready ? "applied" : "pending";
  return fallback;
}

HostObservationsViewData LoadHostObservationsViewData(
    const std::string& db_path,
    const std::optional<std::string>& node_name,
    const std::optional<std::string>& plane_name,
    int stale_after_seconds) {
  comet::ControllerStore store(db_path);
  store.Initialize();
  return HostObservationsViewData{
      db_path,
      plane_name,
      node_name,
      stale_after_seconds,
      plane_name.has_value()
          ? FilterHostObservationsForPlane(store.LoadHostObservations(node_name), *plane_name)
          : store.LoadHostObservations(node_name),
  };
}

HostHealthViewData LoadHostHealthViewData(
    const std::string& db_path,
    const std::optional<std::string>& node_name,
    int stale_after_seconds) {
  comet::ControllerStore store(db_path);
  store.Initialize();
  return HostHealthViewData{
      db_path,
      node_name,
      stale_after_seconds,
      store.LoadDesiredState(),
      store.LoadHostObservations(node_name),
      store.LoadNodeAvailabilityOverrides(node_name),
  };
}

DiskStateViewData LoadDiskStateViewData(
    const std::string& db_path,
    const std::optional<std::string>& node_name,
    const std::optional<std::string>& plane_name) {
  comet::ControllerStore store(db_path);
  store.Initialize();
  const auto desired_state =
      plane_name.has_value() ? store.LoadDesiredState(*plane_name)
                             : store.LoadDesiredState();
  return DiskStateViewData{
      db_path,
      plane_name,
      node_name,
      desired_state,
      plane_name.has_value() ? store.LoadDesiredGeneration(*plane_name)
                             : store.LoadDesiredGeneration(),
      desired_state.has_value()
          ? store.LoadDiskRuntimeStates(desired_state->plane_name, node_name)
          : std::vector<comet::DiskRuntimeState>{},
      plane_name.has_value()
          ? FilterHostObservationsForPlane(store.LoadHostObservations(node_name), *plane_name)
          : store.LoadHostObservations(node_name),
  };
}

EventsViewData LoadEventsViewData(
    const std::string& db_path,
    const std::optional<std::string>& plane_name,
    const std::optional<std::string>& node_name,
    const std::optional<std::string>& worker_name,
    const std::optional<std::string>& category,
    int limit) {
  comet::ControllerStore store(db_path);
  store.Initialize();
  return EventsViewData{
      db_path,
      plane_name,
      node_name,
      worker_name,
      category,
      limit,
      store.LoadEvents(
          plane_name,
          node_name,
          worker_name,
          category,
          limit),
  };
}

json BuildEventPayloadItem(const comet::EventRecord& event) {
  json payload = json::object();
  if (!event.payload_json.empty()) {
    try {
      payload = json::parse(event.payload_json);
    } catch (...) {
      payload = json{
          {"raw", event.payload_json},
      };
    }
  }
  return json{
      {"id", event.id},
      {"plane_name", event.plane_name.empty() ? json(nullptr) : json(event.plane_name)},
      {"node_name", event.node_name.empty() ? json(nullptr) : json(event.node_name)},
      {"worker_name", event.worker_name.empty() ? json(nullptr) : json(event.worker_name)},
      {"assignment_id", event.assignment_id.has_value() ? json(*event.assignment_id) : json(nullptr)},
      {"rollout_action_id",
       event.rollout_action_id.has_value() ? json(*event.rollout_action_id) : json(nullptr)},
      {"category", event.category},
      {"event_type", event.event_type},
      {"severity", event.severity},
      {"message", event.message},
      {"payload", payload},
      {"created_at", event.created_at},
  };
}

json BuildBootstrapModelPayloadItem(
    const std::optional<comet::BootstrapModelSpec>& bootstrap_model) {
  if (!bootstrap_model.has_value()) {
    return nullptr;
  }
  json item{
      {"model_id", bootstrap_model->model_id},
      {"materialization_mode", bootstrap_model->materialization_mode},
      {"served_model_name",
       bootstrap_model->served_model_name.has_value() ? json(*bootstrap_model->served_model_name)
                                                      : json(nullptr)},
      {"local_path",
       bootstrap_model->local_path.has_value() ? json(*bootstrap_model->local_path)
                                               : json(nullptr)},
      {"source_url",
       bootstrap_model->source_url.has_value() ? json(*bootstrap_model->source_url)
                                               : json(nullptr)},
      {"source_urls", bootstrap_model->source_urls},
      {"target_filename",
       bootstrap_model->target_filename.has_value()
           ? json(*bootstrap_model->target_filename)
           : json(nullptr)},
      {"sha256",
       bootstrap_model->sha256.has_value() ? json(*bootstrap_model->sha256) : json(nullptr)},
  };
  return item;
}

json BuildHostAssignmentsPayload(
    const std::string& db_path,
    const std::optional<std::string>& node_name) {
  const auto view = LoadHostAssignmentsViewData(db_path, node_name);

  json assignments = json::array();
  for (const auto& assignment : view.assignments) {
    const comet::DesiredState desired_node_state =
        comet::DeserializeDesiredStateJson(assignment.desired_state_json);
    assignments.push_back(json{
        {"id", assignment.id},
        {"node_name", assignment.node_name},
        {"plane_name", assignment.plane_name},
        {"desired_generation", assignment.desired_generation},
        {"attempt_count", assignment.attempt_count},
        {"max_attempts", assignment.max_attempts},
        {"assignment_type", assignment.assignment_type},
        {"status", comet::ToString(assignment.status)},
        {"status_message", assignment.status_message},
        {"progress",
         (!assignment.progress_json.empty() && assignment.progress_json != "{}")
             ? json::parse(assignment.progress_json)
             : json(nullptr)},
        {"artifacts_root", assignment.artifacts_root},
        {"instance_count", desired_node_state.instances.size()},
    });
  }

  return json{
      {"service", "comet-controller"},
      {"db_path", view.db_path},
      {"node_name", view.node_name.has_value() ? json(*view.node_name) : json(nullptr)},
      {"assignments", assignments},
  };
}

void ApplyRegisteredHostExecutionModes(
    comet::ControllerStore& store,
    comet::DesiredState* desired_state) {
  if (desired_state == nullptr) {
    return;
  }
  for (auto& node : desired_state->nodes) {
    if (const auto host = store.LoadRegisteredHost(node.name); host.has_value() &&
        !host->execution_mode.empty()) {
      node.execution_mode = comet::ParseHostExecutionMode(host->execution_mode);
    }
  }
}

bool NodeAllowsInstanceRole(
    comet::HostExecutionMode execution_mode,
    comet::InstanceRole role);

struct ControllerPlacementUsage {
  double allocated_fraction = 0.0;
  int allocated_memory_mb = 0;
};

struct ControllerAutoPlacementDecision {
  std::string node_name;
  std::string gpu_device;
  int score = 0;
  bool idle_target = false;
  bool upgrade_to_exclusive = false;
  double allocated_fraction = 0.0;
  int allocated_memory_mb = 0;
  int observed_free_vram_mb = -1;
  int observed_utilization_pct = -1;
  int node_order = 0;
  int gpu_order = 0;
};

std::string EffectiveWorkerSelectionPolicy(const comet::DesiredState& state) {
  if (!state.worker_group.worker_selection_policy.empty()) {
    return state.worker_group.worker_selection_policy;
  }
  if (!state.inference.worker_selection_policy.empty()) {
    return state.inference.worker_selection_policy;
  }
  return "prefer-free-then-share";
}

int ControllerAutoPlacementPolicyRank(
    const std::string& policy,
    const ControllerAutoPlacementDecision& candidate) {
  if (policy == "prefer-free-then-share") {
    return candidate.idle_target ? 0 : 1;
  }
  return candidate.idle_target ? 0 : 1;
}

int ScoreControllerAutoPlacementCandidate(
    const comet::NodeInventory& node,
    const std::string& gpu_device,
    const ControllerPlacementUsage& usage,
    const comet::InferenceRuntimeSettings& inference,
    int observed_free_vram_mb,
    int observed_utilization_pct,
    const std::optional<std::string>& preferred_node_name,
    const std::optional<std::string>& preferred_gpu_device) {
  int score = 0;
  if (usage.allocated_fraction <= 1e-9) {
    score += 60;
  }
  if (node.name == inference.primary_infer_node) {
    score += 10;
  }
  if (preferred_node_name.has_value() && node.name == *preferred_node_name) {
    score += 20;
  }
  if (preferred_gpu_device.has_value() && gpu_device == *preferred_gpu_device) {
    score += 10;
  }
  score += static_cast<int>((1.0 - usage.allocated_fraction) * 20.0);
  if (observed_free_vram_mb >= 0) {
    score += std::max(0, observed_free_vram_mb) / 1024;
  } else {
    const auto memory_it = node.gpu_memory_mb.find(gpu_device);
    if (memory_it != node.gpu_memory_mb.end()) {
      score += std::max(0, memory_it->second - usage.allocated_memory_mb) / 1024;
    }
  }
  if (observed_utilization_pct >= 0) {
    score += std::max(0, 100 - observed_utilization_pct) / 5;
  }
  return score;
}

void ReserveControllerPlacement(
    std::map<std::pair<std::string, std::string>, ControllerPlacementUsage>* placement_usage,
    const comet::InstanceSpec& worker) {
  if (placement_usage == nullptr || !worker.gpu_device.has_value() ||
      worker.gpu_device->empty()) {
    return;
  }
  auto& usage = (*placement_usage)[{worker.node_name, *worker.gpu_device}];
  usage.allocated_fraction += worker.gpu_fraction;
  usage.allocated_memory_mb += worker.memory_cap_mb.value_or(0);
}

const comet::InstanceSpec* FindInferInstance(const comet::DesiredState& desired_state) {
  const auto it = std::find_if(
      desired_state.instances.begin(),
      desired_state.instances.end(),
      [](const comet::InstanceSpec& instance) {
        return instance.role == comet::InstanceRole::Infer;
      });
  return it == desired_state.instances.end() ? nullptr : &*it;
}

void RefreshDerivedWorkerMetadata(comet::DesiredState* desired_state) {
  if (desired_state == nullptr) {
    return;
  }

  desired_state->runtime_gpu_nodes.clear();
  desired_state->worker_group.members.clear();

  int rank = 0;
  for (const auto& instance : desired_state->instances) {
    if (instance.role != comet::InstanceRole::Worker) {
      continue;
    }

    desired_state->runtime_gpu_nodes.push_back(
        comet::RuntimeGpuNode{
            instance.name,
            instance.node_name,
            instance.gpu_device.value_or(""),
            instance.placement_mode,
            instance.share_mode,
            instance.gpu_fraction,
            instance.priority,
            instance.preemptible,
            instance.memory_cap_mb,
            true,
        });

    comet::WorkerGroupMemberSpec member;
    member.name = instance.name;
    member.node_name = instance.node_name;
    member.gpu_device = instance.gpu_device.value_or("");
    member.rank = rank;
    member.gpu_fraction = instance.gpu_fraction;
    member.share_mode = instance.share_mode;
    member.priority = instance.priority;
    member.preemptible = instance.preemptible;
    member.memory_cap_mb = instance.memory_cap_mb;
    member.enabled = true;
    member.leader = rank == 0;
    desired_state->worker_group.members.push_back(std::move(member));
    ++rank;
  }

  if (desired_state->worker_group.expected_workers <= 0) {
    desired_state->worker_group.expected_workers = rank;
  }
  if (desired_state->worker_group.infer_instance_name.empty()) {
    if (const auto* infer = FindInferInstance(*desired_state); infer != nullptr) {
      desired_state->worker_group.infer_instance_name = infer->name;
    }
  }
  if (desired_state->worker_group.rendezvous_host.empty()) {
    desired_state->worker_group.rendezvous_host =
        desired_state->worker_group.infer_instance_name;
  }
}

void ApplyObservedHostGpuInventory(
    comet::ControllerStore& store,
    comet::DesiredState* desired_state) {
  if (desired_state == nullptr) {
    return;
  }

  const auto observations = store.LoadHostObservations();
  std::map<std::string, comet::GpuTelemetrySnapshot> telemetry_by_node;
  for (const auto& observation : observations) {
    if (const auto telemetry = ParseGpuTelemetry(observation); telemetry.has_value()) {
      telemetry_by_node[observation.node_name] = *telemetry;
    }
  }

  for (auto& node : desired_state->nodes) {
    const auto telemetry_it = telemetry_by_node.find(node.name);
    if (telemetry_it == telemetry_by_node.end()) {
      continue;
    }
    for (const auto& device : telemetry_it->second.devices) {
      if (device.gpu_device.empty()) {
        continue;
      }
      if (std::find(node.gpu_devices.begin(), node.gpu_devices.end(), device.gpu_device) ==
          node.gpu_devices.end()) {
        node.gpu_devices.push_back(device.gpu_device);
      }
      if (device.total_vram_mb > 0) {
        node.gpu_memory_mb[device.gpu_device] = device.total_vram_mb;
      }
    }
  }
}

std::optional<ControllerAutoPlacementDecision> SelectControllerAutoPlacement(
    const comet::DesiredState& desired_state,
    const std::map<std::pair<std::string, std::string>, ControllerPlacementUsage>& placement_usage,
    const std::map<std::pair<std::string, std::string>, std::pair<int, int>>& observed_gpu_headroom,
    const comet::InstanceSpec& worker,
    const std::optional<std::string>& requested_node_name,
    const std::optional<std::string>& requested_gpu_device) {
  std::optional<ControllerAutoPlacementDecision> best;
  const std::string selection_policy = EffectiveWorkerSelectionPolicy(desired_state);

  for (std::size_t node_index = 0; node_index < desired_state.nodes.size(); ++node_index) {
    const auto& node = desired_state.nodes[node_index];
    if (!NodeAllowsInstanceRole(node.execution_mode, worker.role)) {
      continue;
    }
    if (requested_node_name.has_value() && node.name != *requested_node_name) {
      continue;
    }
    if (node.gpu_devices.empty()) {
      continue;
    }

    for (std::size_t gpu_index = 0; gpu_index < node.gpu_devices.size(); ++gpu_index) {
      const auto& gpu_device = node.gpu_devices[gpu_index];
      if (requested_gpu_device.has_value() && gpu_device != *requested_gpu_device) {
        continue;
      }

      const auto usage_it = placement_usage.find({node.name, gpu_device});
      const ControllerPlacementUsage usage =
          usage_it == placement_usage.end() ? ControllerPlacementUsage{} : usage_it->second;
      const double free_fraction = 1.0 - usage.allocated_fraction;
      if (free_fraction + 1e-9 < worker.gpu_fraction) {
        continue;
      }

      int observed_free_vram_mb = -1;
      int observed_utilization_pct = -1;
      const auto observed_it = observed_gpu_headroom.find({node.name, gpu_device});
      if (observed_it != observed_gpu_headroom.end()) {
        observed_free_vram_mb = observed_it->second.first;
        observed_utilization_pct = observed_it->second.second;
      }

      if (worker.memory_cap_mb.has_value()) {
        const auto memory_it = node.gpu_memory_mb.find(gpu_device);
        const int capacity_free_mb =
            memory_it == node.gpu_memory_mb.end()
                ? std::numeric_limits<int>::max()
                : std::max(0, memory_it->second - usage.allocated_memory_mb);
        if (*worker.memory_cap_mb > capacity_free_mb) {
          continue;
        }
        if (observed_free_vram_mb >= 0 && *worker.memory_cap_mb > observed_free_vram_mb) {
          continue;
        }
      }

      ControllerAutoPlacementDecision candidate;
      candidate.node_name = node.name;
      candidate.gpu_device = gpu_device;
      candidate.idle_target = usage.allocated_fraction <= 1e-9;
      candidate.upgrade_to_exclusive =
          candidate.idle_target &&
          (worker.share_mode != comet::GpuShareMode::Exclusive ||
           worker.gpu_fraction < 1.0 - 1e-9);
      candidate.allocated_fraction = usage.allocated_fraction;
      candidate.allocated_memory_mb = usage.allocated_memory_mb;
      candidate.observed_free_vram_mb = observed_free_vram_mb;
      candidate.observed_utilization_pct = observed_utilization_pct;
      candidate.node_order = static_cast<int>(node_index);
      candidate.gpu_order = static_cast<int>(gpu_index);
      candidate.score = ScoreControllerAutoPlacementCandidate(
          node,
          gpu_device,
          usage,
          desired_state.inference,
          observed_free_vram_mb,
          observed_utilization_pct,
          requested_node_name,
          requested_gpu_device);

      if (!best.has_value()) {
        best = candidate;
        continue;
      }

      const int candidate_rank = ControllerAutoPlacementPolicyRank(selection_policy, candidate);
      const int best_rank = ControllerAutoPlacementPolicyRank(selection_policy, *best);
      if (candidate_rank < best_rank ||
          (candidate_rank == best_rank &&
           (candidate.allocated_fraction < best->allocated_fraction - 1e-9 ||
            (std::abs(candidate.allocated_fraction - best->allocated_fraction) <= 1e-9 &&
             (candidate.score > best->score ||
              (candidate.score == best->score &&
               (candidate.node_order < best->node_order ||
                (candidate.node_order == best->node_order &&
                 (candidate.gpu_order < best->gpu_order ||
                  (candidate.gpu_order == best->gpu_order &&
                   (candidate.node_name < best->node_name ||
                    (candidate.node_name == best->node_name &&
                     candidate.gpu_device < best->gpu_device)))))))))))) {
        best = candidate;
      }
    }
  }

  return best;
}

void ResolveDesiredStateDynamicPlacements(
    comet::ControllerStore& store,
    comet::DesiredState* desired_state) {
  if (desired_state == nullptr) {
    return;
  }

  ApplyObservedHostGpuInventory(store, desired_state);

  std::map<std::pair<std::string, std::string>, std::pair<int, int>> observed_gpu_headroom;
  for (const auto& observation : store.LoadHostObservations()) {
    if (const auto telemetry = ParseGpuTelemetry(observation); telemetry.has_value()) {
      for (const auto& device : telemetry->devices) {
        if (device.gpu_device.empty()) {
          continue;
        }
        observed_gpu_headroom[{observation.node_name, device.gpu_device}] = {
            device.free_vram_mb,
            device.gpu_utilization_pct,
        };
      }
    }
  }

  std::map<std::pair<std::string, std::string>, ControllerPlacementUsage> placement_usage;
  for (const auto& instance : desired_state->instances) {
    if (instance.role == comet::InstanceRole::Worker) {
      ReserveControllerPlacement(&placement_usage, instance);
    }
  }

  for (auto& instance : desired_state->instances) {
    if (instance.role != comet::InstanceRole::Worker) {
      continue;
    }

    const bool has_gpu_device =
        instance.gpu_device.has_value() && !instance.gpu_device->empty();
    if (instance.placement_mode == comet::PlacementMode::Manual && !has_gpu_device) {
      throw std::runtime_error(
          "worker '" + instance.name +
          "' uses placement_mode=manual but does not specify gpu_device");
    }
    if (has_gpu_device) {
      continue;
    }

    const std::optional<std::string> requested_node_name =
        instance.node_name.empty() ? std::nullopt
                                   : std::optional<std::string>(instance.node_name);
    const auto placement = SelectControllerAutoPlacement(
        *desired_state,
        placement_usage,
        observed_gpu_headroom,
        instance,
        requested_node_name,
        std::nullopt);
    if (!placement.has_value()) {
      if (requested_node_name.has_value()) {
        throw std::runtime_error(
            "worker '" + instance.name + "' could not be assigned to any GPU on node '" +
            *requested_node_name +
            "'; wait for fresh host GPU telemetry or free capacity");
      }
      throw std::runtime_error(
          "worker '" + instance.name +
          "' could not be assigned to any GPU; wait for fresh host GPU telemetry or free capacity");
    }

    instance.node_name = placement->node_name;
    instance.gpu_device = placement->gpu_device;
    if (placement->upgrade_to_exclusive) {
      instance.share_mode = comet::GpuShareMode::Exclusive;
      instance.gpu_fraction = 1.0;
    }
    instance.environment["COMET_NODE_NAME"] = instance.node_name;
    instance.environment["COMET_GPU_DEVICE"] = *instance.gpu_device;
    instance.labels["comet.node"] = instance.node_name;
    instance.labels["comet.placement"] = "auto";
    instance.labels["comet.placement.mode"] = comet::ToString(instance.placement_mode);
    instance.labels["comet.placement.action"] =
        placement->upgrade_to_exclusive ? "upgrade-to-exclusive" : "auto-assign";
    instance.labels["comet.placement.score"] = std::to_string(placement->score);
    if (!requested_node_name.has_value()) {
      instance.labels.erase("comet.requested.node");
    } else {
      instance.labels["comet.requested.node"] = *requested_node_name;
    }
    instance.labels.erase("comet.requested.gpu");

    ReserveControllerPlacement(&placement_usage, instance);
  }

  RefreshDerivedWorkerMetadata(desired_state);
}

bool NodeAllowsInstanceRole(
    comet::HostExecutionMode execution_mode,
    comet::InstanceRole role) {
  switch (execution_mode) {
    case comet::HostExecutionMode::InferOnly:
      return role == comet::InstanceRole::Infer ||
             role == comet::InstanceRole::App;
    case comet::HostExecutionMode::WorkerOnly:
      return role == comet::InstanceRole::Worker;
    case comet::HostExecutionMode::Mixed:
      return true;
  }
  return true;
}

void ValidateDesiredStateExecutionModes(const comet::DesiredState& desired_state) {
  std::map<std::string, comet::HostExecutionMode> node_modes;
  for (const auto& node : desired_state.nodes) {
    node_modes[node.name] = node.execution_mode;
  }
  for (const auto& instance : desired_state.instances) {
    const auto node_it = node_modes.find(instance.node_name);
    if (node_it == node_modes.end()) {
      continue;
    }
    if (!NodeAllowsInstanceRole(node_it->second, instance.role)) {
      throw std::invalid_argument(
          "instance '" + instance.name + "' role '" + comet::ToString(instance.role) +
          "' is not allowed on node '" + instance.node_name + "' execution_mode='" +
          comet::ToString(node_it->second) + "'");
    }
  }
}

json BuildHostObservationsPayload(
    const std::string& db_path,
    const std::optional<std::string>& node_name,
    const std::optional<std::string>& plane_name,
    int stale_after_seconds) {
  const auto view =
      LoadHostObservationsViewData(db_path, node_name, plane_name, stale_after_seconds);

  json observations = json::array();
  for (const auto& observation : view.observations) {
    const auto runtime_status = ParseRuntimeStatus(observation);
    const auto telemetry = ParseGpuTelemetry(observation);
    const auto cpu_telemetry = ParseCpuTelemetry(observation);
    const auto instance_statuses = ParseInstanceRuntimeStatuses(observation);
    const auto disk_telemetry = ParseDiskTelemetry(observation);
    const auto network_telemetry = ParseNetworkTelemetry(observation);

    const auto build_runtime_status_payload =
        [&](const std::optional<comet::RuntimeStatus>& status) -> json {
      if (!status.has_value()) {
        return json{
            {"contract_version", 1},
            {"available", false},
            {"runtime", nullptr},
        };
      }
      return json{
          {"contract_version", 1},
          {"available", true},
          {"runtime", json::parse(comet::SerializeRuntimeStatusJson(*status))},
      };
    };

    const auto build_instance_runtime_payload =
        [&](const std::vector<comet::RuntimeProcessStatus>& statuses) -> json {
      int ready_count = 0;
      int gpu_bound_count = 0;
      int running_count = 0;
      for (const auto& status : statuses) {
        if (status.ready) {
          ++ready_count;
        }
        if (!status.gpu_device.empty()) {
          ++gpu_bound_count;
        }
        if (!status.runtime_phase.empty() && status.runtime_phase != "stopped") {
          ++running_count;
        }
      }
      return json{
          {"contract_version", 1},
          {"available", !statuses.empty()},
          {"summary",
           {
               {"count", statuses.size()},
               {"ready_count", ready_count},
               {"running_count", running_count},
               {"gpu_bound_count", gpu_bound_count},
           }},
          {"items",
           statuses.empty() ? json::array()
                            : json::parse(comet::SerializeRuntimeStatusListJson(statuses))},
      };
    };

    const auto build_gpu_telemetry_payload =
        [&](const std::optional<comet::GpuTelemetrySnapshot>& snapshot) -> json {
      if (!snapshot.has_value()) {
        return json{
            {"contract_version", 1},
            {"available", false},
            {"degraded", true},
            {"source", nullptr},
            {"collected_at", nullptr},
            {"summary",
             {
                 {"device_count", 0},
                 {"owned_process_count", 0},
                 {"unknown_process_count", 0},
                 {"total_vram_mb", 0},
                 {"used_vram_mb", 0},
                 {"free_vram_mb", 0},
             }},
            {"devices", json::array()},
        };
      }

      int owned_process_count = 0;
      int unknown_process_count = 0;
      int total_vram_mb = 0;
      int used_vram_mb = 0;
      int free_vram_mb = 0;
      for (const auto& device : snapshot->devices) {
        total_vram_mb += device.total_vram_mb;
        used_vram_mb += device.used_vram_mb;
        free_vram_mb += device.free_vram_mb;
        for (const auto& process : device.processes) {
          if (process.instance_name == "unknown") {
            ++unknown_process_count;
          } else {
            ++owned_process_count;
          }
        }
      }

      return json{
          {"contract_version", snapshot->contract_version},
          {"available", true},
          {"degraded", snapshot->degraded},
          {"source", snapshot->source.empty() ? json(nullptr) : json(snapshot->source)},
          {"collected_at", snapshot->collected_at.empty() ? json(nullptr) : json(snapshot->collected_at)},
          {"summary",
           {
               {"device_count", snapshot->devices.size()},
               {"owned_process_count", owned_process_count},
               {"unknown_process_count", unknown_process_count},
               {"total_vram_mb", total_vram_mb},
               {"used_vram_mb", used_vram_mb},
               {"free_vram_mb", free_vram_mb},
           }},
          {"devices", json::parse(comet::SerializeGpuTelemetryJson(*snapshot)).at("devices")},
      };
    };

    const auto build_disk_telemetry_payload =
        [&](const std::optional<comet::DiskTelemetrySnapshot>& snapshot) -> json {
      if (!snapshot.has_value()) {
        return json{
            {"contract_version", 1},
            {"available", false},
            {"degraded", true},
            {"source", nullptr},
            {"collected_at", nullptr},
            {"summary",
             {
                 {"disk_count", 0},
                 {"mounted_count", 0},
                 {"healthy_count", 0},
                 {"total_bytes", 0},
                 {"used_bytes", 0},
                 {"free_bytes", 0},
             }},
            {"items", json::array()},
        };
      }

      int mounted_count = 0;
      int healthy_count = 0;
      std::uint64_t total_bytes = 0;
      std::uint64_t used_bytes = 0;
      std::uint64_t free_bytes = 0;
      std::uint64_t read_ios = 0;
      std::uint64_t write_ios = 0;
      std::uint64_t read_bytes = 0;
      std::uint64_t write_bytes = 0;
      std::uint64_t io_time_ms = 0;
      std::uint64_t weighted_io_time_ms = 0;
      int io_in_progress = 0;
      int warning_count = 0;
      int fault_count = 0;
      int read_only_count = 0;
      int perf_counters_count = 0;
      int io_error_counter_count = 0;
      for (const auto& item : snapshot->items) {
        if (item.mounted) {
          ++mounted_count;
        }
        if (item.health == "ok") {
          ++healthy_count;
        }
        total_bytes += item.total_bytes;
        used_bytes += item.used_bytes;
        free_bytes += item.free_bytes;
        read_ios += item.read_ios;
        write_ios += item.write_ios;
        read_bytes += item.read_bytes;
        write_bytes += item.write_bytes;
        io_time_ms += item.io_time_ms;
        weighted_io_time_ms += item.weighted_io_time_ms;
        io_in_progress += item.io_in_progress;
        warning_count += item.warning_count;
        fault_count += item.fault_count;
        if (item.read_only) {
          ++read_only_count;
        }
        if (item.perf_counters_available) {
          ++perf_counters_count;
        }
        if (item.io_error_counters_available) {
          ++io_error_counter_count;
        }
      }

      return json{
          {"contract_version", snapshot->contract_version},
          {"available", true},
          {"degraded", snapshot->degraded},
          {"source", snapshot->source.empty() ? json(nullptr) : json(snapshot->source)},
          {"collected_at", snapshot->collected_at.empty() ? json(nullptr) : json(snapshot->collected_at)},
          {"summary",
           {
               {"disk_count", snapshot->items.size()},
               {"mounted_count", mounted_count},
               {"healthy_count", healthy_count},
               {"total_bytes", total_bytes},
               {"used_bytes", used_bytes},
               {"free_bytes", free_bytes},
               {"read_ios", read_ios},
               {"write_ios", write_ios},
               {"read_bytes", read_bytes},
               {"write_bytes", write_bytes},
               {"io_time_ms", io_time_ms},
               {"weighted_io_time_ms", weighted_io_time_ms},
               {"io_in_progress", io_in_progress},
               {"warning_count", warning_count},
               {"fault_count", fault_count},
               {"read_only_count", read_only_count},
               {"perf_counters_count", perf_counters_count},
               {"io_error_counter_count", io_error_counter_count},
           }},
          {"items", json::parse(comet::SerializeDiskTelemetryJson(*snapshot)).at("items")},
      };
    };

    const auto build_network_telemetry_payload =
        [&](const std::optional<comet::NetworkTelemetrySnapshot>& snapshot) -> json {
      if (!snapshot.has_value()) {
        return json{
            {"contract_version", 1},
            {"available", false},
            {"degraded", true},
            {"source", nullptr},
            {"collected_at", nullptr},
            {"summary",
             {
                 {"interface_count", 0},
                 {"up_count", 0},
                 {"loopback_count", 0},
                 {"rx_bytes", 0},
                 {"tx_bytes", 0},
             }},
            {"interfaces", json::array()},
        };
      }

      int up_count = 0;
      int loopback_count = 0;
      std::uint64_t rx_bytes = 0;
      std::uint64_t tx_bytes = 0;
      for (const auto& interface : snapshot->interfaces) {
        if (interface.link_state == "up" || interface.oper_state == "up") {
          ++up_count;
        }
        if (interface.loopback) {
          ++loopback_count;
        }
        rx_bytes += interface.rx_bytes;
        tx_bytes += interface.tx_bytes;
      }

      return json{
          {"contract_version", snapshot->contract_version},
          {"available", true},
          {"degraded", snapshot->degraded},
          {"source", snapshot->source.empty() ? json(nullptr) : json(snapshot->source)},
          {"collected_at", snapshot->collected_at.empty() ? json(nullptr) : json(snapshot->collected_at)},
          {"summary",
           {
               {"interface_count", snapshot->interfaces.size()},
               {"up_count", up_count},
               {"loopback_count", loopback_count},
               {"rx_bytes", rx_bytes},
               {"tx_bytes", tx_bytes},
           }},
          {"interfaces", json::parse(comet::SerializeNetworkTelemetryJson(*snapshot)).at("interfaces")},
      };
    };

    const auto build_cpu_telemetry_payload =
        [&](const std::optional<comet::CpuTelemetrySnapshot>& snapshot) -> json {
      if (!snapshot.has_value()) {
        return json{
            {"contract_version", 1},
            {"available", false},
            {"degraded", true},
            {"source", nullptr},
            {"collected_at", nullptr},
            {"summary",
             {
                 {"core_count", 0},
                 {"utilization_pct", 0.0},
                 {"loadavg_1m", 0.0},
                 {"loadavg_5m", 0.0},
                 {"loadavg_15m", 0.0},
                 {"total_memory_bytes", 0},
                 {"available_memory_bytes", 0},
                 {"used_memory_bytes", 0},
             }},
            {"snapshot", nullptr},
        };
      }

      return json{
          {"contract_version", snapshot->contract_version},
          {"available", true},
          {"degraded", snapshot->degraded},
          {"source", snapshot->source.empty() ? json(nullptr) : json(snapshot->source)},
          {"collected_at", snapshot->collected_at.empty() ? json(nullptr) : json(snapshot->collected_at)},
          {"summary",
           {
               {"core_count", snapshot->core_count},
               {"utilization_pct", snapshot->utilization_pct},
               {"loadavg_1m", snapshot->loadavg_1m},
               {"loadavg_5m", snapshot->loadavg_5m},
               {"loadavg_15m", snapshot->loadavg_15m},
               {"total_memory_bytes", snapshot->total_memory_bytes},
               {"available_memory_bytes", snapshot->available_memory_bytes},
               {"used_memory_bytes", snapshot->used_memory_bytes},
           }},
          {"snapshot", json::parse(comet::SerializeCpuTelemetryJson(*snapshot))},
      };
    };

    json entry{
        {"node_name", observation.node_name},
        {"plane_name", observation.plane_name.empty() ? json(nullptr) : json(observation.plane_name)},
        {"status", comet::ToString(observation.status)},
        {"status_message", observation.status_message},
        {"heartbeat_at", observation.heartbeat_at},
    };
    if (observation.applied_generation.has_value()) {
      entry["applied_generation"] = *observation.applied_generation;
    } else {
      entry["applied_generation"] = nullptr;
    }
    if (observation.last_assignment_id.has_value()) {
      entry["last_assignment_id"] = *observation.last_assignment_id;
    } else {
      entry["last_assignment_id"] = nullptr;
    }
    const auto age_seconds = HeartbeatAgeSeconds(observation.heartbeat_at);
    entry["age_seconds"] = age_seconds.has_value() ? json(*age_seconds) : json(nullptr);
    entry["health"] = HealthFromAge(age_seconds, view.stale_after_seconds);

    if (!observation.observed_state_json.empty()) {
      entry["observed_state"] = json::parse(observation.observed_state_json);
    } else {
      entry["observed_state"] = nullptr;
    }
    entry["runtime_status"] = build_runtime_status_payload(runtime_status);
    entry["gpu_telemetry"] = build_gpu_telemetry_payload(telemetry);
    entry["disk_telemetry"] = build_disk_telemetry_payload(disk_telemetry);
    entry["network_telemetry"] = build_network_telemetry_payload(network_telemetry);
    entry["cpu_telemetry"] = build_cpu_telemetry_payload(cpu_telemetry);
    entry["instance_runtimes"] = build_instance_runtime_payload(instance_statuses);

    observations.push_back(std::move(entry));
  }

  return json{
      {"service", "comet-controller"},
      {"db_path", view.db_path},
      {"plane_name", view.plane_name.has_value() ? json(*view.plane_name) : json(nullptr)},
      {"node_name", view.node_name.has_value() ? json(*view.node_name) : json(nullptr)},
      {"stale_after_seconds", view.stale_after_seconds},
      {"observations", observations},
  };
}

json BuildHostHealthPayload(
    const std::string& db_path,
    const std::optional<std::string>& node_name,
    int stale_after_seconds) {
  const auto view = LoadHostHealthViewData(db_path, node_name, stale_after_seconds);
  const auto availability_override_map =
      BuildAvailabilityOverrideMap(view.availability_overrides);

  std::map<std::string, comet::HostObservation> observation_by_node;
  for (const auto& observation : view.observations) {
    observation_by_node.emplace(observation.node_name, observation);
  }

  std::vector<std::string> nodes;
  std::set<std::string> seen_nodes;
  if (view.desired_state.has_value()) {
    for (const auto& node : view.desired_state->nodes) {
      if (!view.node_name.has_value() || node.name == *view.node_name) {
        nodes.push_back(node.name);
        seen_nodes.insert(node.name);
      }
    }
  }
  for (const auto& [observed_node_name, observation] : observation_by_node) {
    (void)observation;
    if ((!view.node_name.has_value() || observed_node_name == *view.node_name) &&
        seen_nodes.find(observed_node_name) == seen_nodes.end()) {
      nodes.push_back(observed_node_name);
      seen_nodes.insert(observed_node_name);
    }
  }

  int online_count = 0;
  int stale_count = 0;
  int unknown_count = 0;
  json items = json::array();
  for (const auto& current_node_name : nodes) {
    json item{
        {"node_name", current_node_name},
        {"availability",
         comet::ToString(
             ResolveNodeAvailability(availability_override_map, current_node_name))},
    };
    const auto observation_it = observation_by_node.find(current_node_name);
    if (observation_it == observation_by_node.end()) {
      item["health"] = "unknown";
      item["status"] = nullptr;
      ++unknown_count;
      items.push_back(std::move(item));
      continue;
    }

    const auto age_seconds = HeartbeatAgeSeconds(observation_it->second.heartbeat_at);
    const std::string health = HealthFromAge(age_seconds, view.stale_after_seconds);
    item["health"] = health;
    item["status"] = comet::ToString(observation_it->second.status);
    item["age_seconds"] = age_seconds.has_value() ? json(*age_seconds) : json(nullptr);
    item["heartbeat_at"] = observation_it->second.heartbeat_at;
    item["applied_generation"] =
        observation_it->second.applied_generation.has_value()
            ? json(*observation_it->second.applied_generation)
            : json(nullptr);
    if (const auto runtime_status = ParseRuntimeStatus(observation_it->second);
        runtime_status.has_value()) {
      item["runtime_phase"] = runtime_status->runtime_phase;
      item["runtime_launch_ready"] = runtime_status->launch_ready;
      item["runtime_backend"] = runtime_status->runtime_backend;
    }
    if (const auto telemetry = ParseGpuTelemetry(observation_it->second);
        telemetry.has_value()) {
      item["telemetry_degraded"] = telemetry->degraded;
      item["telemetry_source"] = telemetry->source;
      item["gpu_device_count"] = telemetry->devices.size();
    }
    if (health == "online") {
      ++online_count;
    } else if (health == "stale") {
      ++stale_count;
    } else {
      ++unknown_count;
    }
    items.push_back(std::move(item));
  }

  return json{
      {"service", "comet-controller"},
      {"db_path", view.db_path},
      {"node_name", view.node_name.has_value() ? json(*view.node_name) : json(nullptr)},
      {"stale_after_seconds", view.stale_after_seconds},
      {"summary",
       {
           {"online", online_count},
           {"stale", stale_count},
           {"unknown", unknown_count},
       }},
      {"items", items},
  };
}

json BuildDiskStatePayload(
    const std::string& db_path,
    const std::optional<std::string>& node_name,
    const std::optional<std::string>& plane_name = std::nullopt);

json BuildDashboardPayload(
    const std::string& db_path,
    int stale_after_seconds,
    const std::optional<std::string>& plane_name = std::nullopt);

json BuildNodeAvailabilityPayload(
    const std::string& db_path,
    const std::optional<std::string>& node_name);

json BuildRolloutActionsPayload(
    const std::string& db_path,
    const std::optional<std::string>& node_name,
    const std::optional<std::string>& plane_name = std::nullopt);

json BuildRebalancePlanPayload(
    const std::string& db_path,
    const std::optional<std::string>& node_name,
    int stale_after_seconds,
    const std::optional<std::string>& plane_name = std::nullopt);

json BuildEventsPayload(
    const std::string& db_path,
    const std::optional<std::string>& plane_name,
    const std::optional<std::string>& node_name,
    const std::optional<std::string>& worker_name,
    const std::optional<std::string>& category,
    int limit);

std::string ResolveArtifactsRoot(
    const std::optional<std::string>& artifacts_root_arg,
    const std::string& fallback_artifacts_root = DefaultArtifactsRoot());

int ReconcileRolloutActions(
    const std::string& db_path,
    const std::string& artifacts_root);

int ReconcileRebalanceProposals(
    const std::string& db_path,
    const std::string& artifacts_root);

int SchedulerTick(
    const std::string& db_path,
    const std::string& artifacts_root);

int SetRolloutActionStatus(
    const std::string& db_path,
    int action_id,
    comet::RolloutActionStatus status,
    const std::optional<std::string>& status_message);

int EnqueueRolloutEviction(
    const std::string& db_path,
    int action_id);

int ApplyReadyRolloutAction(
    const std::string& db_path,
    int action_id,
    const std::string& artifacts_root);

int ApplyRebalanceProposal(
    const std::string& db_path,
    const std::string& worker_name,
    const std::string& artifacts_root);

int ValidateBundle(const std::string& bundle_dir);

int PreviewBundle(
    const std::string& bundle_dir,
    const std::optional<std::string>& node_name);

int ImportBundle(const std::string& db_path, const std::string& bundle_dir);

int ApplyBundle(
    const std::string& db_path,
    const std::string& bundle_dir,
    const std::string& artifacts_root);

int SetNodeAvailability(
    const std::string& db_path,
    const std::string& node_name,
    comet::NodeAvailability availability,
    const std::optional<std::string>& status_message);

int RetryHostAssignment(const std::string& db_path, int assignment_id);

HttpResponse BuildJsonResponse(
    int status_code,
    const json& payload,
    const std::map<std::string, std::string>& headers) {
  json enriched = payload;
  if (enriched.is_object()) {
    if (!enriched.contains("api_version")) {
      enriched["api_version"] = "v1";
    }
    if (g_current_http_request != nullptr && !enriched.contains("request")) {
      enriched["request"] = {
          {"path", g_current_http_request->path},
          {"method", g_current_http_request->method},
      };
    }
    if (status_code >= 400) {
      if (!enriched.contains("error") || !enriched.at("error").is_object()) {
        json error{
            {"code", enriched.value("status", "error")},
            {"message",
             enriched.value(
                 "message",
                 ControllerNetworkManager::ReasonPhrase(status_code))},
        };
        if (enriched.contains("details")) {
          error["details"] = enriched["details"];
        }
        enriched["error"] = error;
      } else {
        if (!enriched["error"].contains("code")) {
          enriched["error"]["code"] = enriched.value("status", "error");
        }
        if (!enriched["error"].contains("message")) {
          enriched["error"]["message"] = enriched.value(
              "message",
              ControllerNetworkManager::ReasonPhrase(status_code));
        }
      }
      enriched["status"] = "error";
      enriched.erase("message");
      enriched.erase("details");
      enriched.erase("path");
      enriched.erase("method");
    }
  }
  return HttpResponse{status_code, "application/json", enriched.dump(), headers};
}

json ParseJsonRequestBody(const HttpRequest& request) {
  if (request.body.empty()) {
    return json::object();
  }
  return json::parse(request.body);
}

struct ModelLibraryEntry {
  std::string path;
  std::string name;
  std::string kind;
  std::string format;
  std::string root;
  std::vector<std::string> paths;
  std::uintmax_t size_bytes = 0;
  int part_count = 1;
  std::vector<std::string> referenced_by;
  bool deletable = true;
};

struct ModelLibraryDownloadJob {
  std::string id;
  std::string status = "queued";
  std::string model_id;
  std::string target_root;
  std::string target_subdir;
  std::vector<std::string> source_urls;
  std::vector<std::string> target_paths;
  std::string current_item;
  std::optional<std::uintmax_t> bytes_total;
  std::uintmax_t bytes_done = 0;
  int part_count = 0;
  std::string error_message;
  std::string created_at;
  std::string updated_at;
};

std::mutex g_model_library_jobs_mutex;
std::map<std::string, ModelLibraryDownloadJob> g_model_library_jobs;
std::atomic<std::uint64_t> g_model_library_job_counter{0};

bool EndsWithIgnoreCase(const std::string& value, const std::string& suffix) {
  if (value.size() < suffix.size()) {
    return false;
  }
  const std::size_t offset = value.size() - suffix.size();
  for (std::size_t index = 0; index < suffix.size(); ++index) {
    if (std::tolower(static_cast<unsigned char>(value[offset + index])) !=
        std::tolower(static_cast<unsigned char>(suffix[index]))) {
      return false;
    }
  }
  return true;
}

bool IsAllDigits(const std::string& value) {
  return !value.empty() &&
         std::all_of(
             value.begin(),
             value.end(),
             [](unsigned char ch) { return std::isdigit(ch) != 0; });
}

std::string NormalizePathString(const std::filesystem::path& path) {
  return path.lexically_normal().string();
}

bool IsUsableAbsoluteHostPath(const std::string& value) {
  return !value.empty() && value.front() == '/' && value.rfind("/comet/", 0) != 0;
}

std::string FilenameFromUrlForModelLibrary(const std::string& source_url) {
  std::string trimmed = source_url;
  const auto query_pos = trimmed.find('?');
  if (query_pos != std::string::npos) {
    trimmed = trimmed.substr(0, query_pos);
  }
  const auto fragment_pos = trimmed.find('#');
  if (fragment_pos != std::string::npos) {
    trimmed = trimmed.substr(0, fragment_pos);
  }
  const auto slash_pos = trimmed.find_last_of('/');
  const std::string filename =
      slash_pos == std::string::npos ? trimmed : trimmed.substr(slash_pos + 1);
  if (filename.empty()) {
    throw std::runtime_error("failed to infer filename from URL: " + source_url);
  }
  return filename;
}

std::optional<std::uintmax_t> ProbeContentLengthForModelLibrary(const std::string& source_url) {
  const std::string temp_headers =
      (std::filesystem::temp_directory_path() /
       ("comet-model-head-" + std::to_string(comet::platform::CurrentProcessId()) + "-" +
        std::to_string(g_model_library_job_counter.fetch_add(1)) + ".txt"))
          .string();
  const std::string command = "/usr/bin/curl -fsSLI " + std::string("'") + source_url + "' > '" +
                              temp_headers + "' 2>/dev/null || true";
  std::system(command.c_str());
  std::ifstream input(temp_headers);
  std::filesystem::remove(temp_headers);
  std::string line;
  while (std::getline(input, line)) {
    const std::string trimmed = Trim(line);
    const auto colon = trimmed.find(':');
    if (colon == std::string::npos) {
      continue;
    }
    const std::string key = trimmed.substr(0, colon);
    if (Lowercase(key) != "content-length") {
      continue;
    }
    try {
      return static_cast<std::uintmax_t>(std::stoull(Trim(trimmed.substr(colon + 1))));
    } catch (...) {
      return std::nullopt;
    }
  }
  return std::nullopt;
}

std::optional<std::uintmax_t> FileSizeIfExistsForModelLibrary(const std::filesystem::path& path) {
  std::error_code error;
  if (!std::filesystem::exists(path, error) || error) {
    return std::nullopt;
  }
  if (std::filesystem::is_regular_file(path, error)) {
    const auto size = std::filesystem::file_size(path, error);
    return error ? std::nullopt : std::optional<std::uintmax_t>(size);
  }
  if (!std::filesystem::is_directory(path, error) || error) {
    return std::nullopt;
  }
  std::uintmax_t total = 0;
  for (std::filesystem::recursive_directory_iterator iterator(
           path,
           std::filesystem::directory_options::skip_permission_denied,
           error);
       !error && iterator != std::filesystem::recursive_directory_iterator();
       iterator.increment(error)) {
    if (!iterator->is_regular_file(error) || error) {
      error.clear();
      continue;
    }
    total += iterator->file_size(error);
    if (error) {
      return std::nullopt;
    }
  }
  return error ? std::nullopt : std::optional<std::uintmax_t>(total);
}

bool LooksLikeRecognizedModelDirectoryForLibrary(const std::filesystem::path& path) {
  std::error_code error;
  if (!std::filesystem::exists(path, error) || error ||
      !std::filesystem::is_directory(path, error) || error) {
    return false;
  }
  return std::filesystem::exists(path / "config.json", error) ||
         std::filesystem::exists(path / "params.json", error);
}

bool ParseMultipartGgufFilename(
    const std::string& filename,
    std::string* prefix,
    int* part_index,
    int* part_total) {
  if (!EndsWithIgnoreCase(filename, ".gguf")) {
    return false;
  }
  const std::string stem = filename.substr(0, filename.size() - 5);
  const auto of_pos = stem.rfind("-of-");
  if (of_pos == std::string::npos) {
    return false;
  }
  const auto part_sep = stem.rfind('-', of_pos - 1);
  if (part_sep == std::string::npos) {
    return false;
  }
  const std::string part_index_text = stem.substr(part_sep + 1, of_pos - (part_sep + 1));
  const std::string part_total_text = stem.substr(of_pos + 4);
  if (!IsAllDigits(part_index_text) || !IsAllDigits(part_total_text)) {
    return false;
  }
  if (prefix != nullptr) {
    *prefix = stem.substr(0, part_sep);
  }
  if (part_index != nullptr) {
    *part_index = std::stoi(part_index_text);
  }
  if (part_total != nullptr) {
    *part_total = std::stoi(part_total_text);
  }
  return true;
}

std::vector<std::string> DiscoverModelLibraryRoots(
    const std::vector<comet::DesiredState>& desired_states) {
  std::set<std::string> roots;
  if (const char* env_value = std::getenv("COMET_NODE_MODEL_LIBRARY_ROOTS");
      env_value != nullptr && *env_value != '\0') {
    std::string current;
    for (char ch : std::string(env_value)) {
      if (ch == ':' || ch == ';') {
        const std::string trimmed = Trim(current);
        if (IsUsableAbsoluteHostPath(trimmed)) {
          roots.insert(NormalizePathString(trimmed));
        }
        current.clear();
      } else {
        current.push_back(ch);
      }
    }
    const std::string trimmed = Trim(current);
    if (IsUsableAbsoluteHostPath(trimmed)) {
      roots.insert(NormalizePathString(trimmed));
    }
  }
  for (const auto& desired_state : desired_states) {
    if (!desired_state.bootstrap_model.has_value() ||
        !desired_state.bootstrap_model->local_path.has_value()) {
      continue;
    }
    const std::string& local_path = *desired_state.bootstrap_model->local_path;
    if (!IsUsableAbsoluteHostPath(local_path)) {
      continue;
    }
    std::filesystem::path path(local_path);
    std::error_code error;
    if (std::filesystem::exists(path, error) && !error && std::filesystem::is_regular_file(path, error)) {
      roots.insert(NormalizePathString(path.parent_path()));
      continue;
    }
    if (!error && std::filesystem::exists(path) && std::filesystem::is_directory(path, error) && !error) {
      roots.insert(NormalizePathString(path.parent_path()));
      continue;
    }
    roots.insert(NormalizePathString(path.parent_path()));
  }
  return std::vector<std::string>(roots.begin(), roots.end());
}

std::map<std::string, std::vector<std::string>> BuildModelReferenceMap(
    const std::vector<comet::DesiredState>& desired_states) {
  std::map<std::string, std::vector<std::string>> references;
  for (const auto& desired_state : desired_states) {
    if (!desired_state.bootstrap_model.has_value() ||
        !desired_state.bootstrap_model->local_path.has_value()) {
      continue;
    }
    const std::string& local_path = *desired_state.bootstrap_model->local_path;
    if (!IsUsableAbsoluteHostPath(local_path)) {
      continue;
    }
    references[NormalizePathString(local_path)].push_back(desired_state.plane_name);
  }
  return references;
}

json BuildModelLibraryJobPayload(const ModelLibraryDownloadJob& job) {
  return json{
      {"id", job.id},
      {"status", job.status},
      {"model_id", job.model_id},
      {"target_root", job.target_root},
      {"target_subdir", job.target_subdir},
      {"source_urls", job.source_urls},
      {"target_paths", job.target_paths},
      {"current_item", job.current_item},
      {"bytes_total", job.bytes_total.has_value() ? json(*job.bytes_total) : json(nullptr)},
      {"bytes_done", job.bytes_done},
      {"part_count", job.part_count},
      {"error_message", job.error_message.empty() ? json(nullptr) : json(job.error_message)},
      {"created_at", job.created_at},
      {"updated_at", job.updated_at},
  };
}

void UpdateModelLibraryJob(
    const std::string& job_id,
    const std::function<void(ModelLibraryDownloadJob&)>& update) {
  std::lock_guard<std::mutex> lock(g_model_library_jobs_mutex);
  const auto it = g_model_library_jobs.find(job_id);
  if (it == g_model_library_jobs.end()) {
    return;
  }
  update(it->second);
  it->second.updated_at = UtcNowSqlTimestamp();
}

std::vector<ModelLibraryEntry> ScanModelLibraryEntries(const std::string& db_path) {
  comet::ControllerStore store(db_path);
  const auto desired_states = store.LoadDesiredStates();
  const auto roots = DiscoverModelLibraryRoots(desired_states);
  const auto reference_map = BuildModelReferenceMap(desired_states);

  struct MultipartGroup {
    std::string root;
    std::string name;
    std::vector<std::string> paths;
    std::uintmax_t size_bytes = 0;
    int part_total = 0;
  };

  std::map<std::string, ModelLibraryEntry> entries_by_path;
  std::map<std::string, MultipartGroup> multipart_groups;

  for (const auto& root_text : roots) {
    const std::filesystem::path root(root_text);
    std::error_code error;
    if (!std::filesystem::exists(root, error) || error ||
        !std::filesystem::is_directory(root, error) || error) {
      continue;
    }
    for (std::filesystem::recursive_directory_iterator iterator(
             root,
             std::filesystem::directory_options::skip_permission_denied,
             error);
         !error && iterator != std::filesystem::recursive_directory_iterator();
         iterator.increment(error)) {
      if (error) {
        break;
      }
      const auto depth = iterator.depth();
      if (depth > 4) {
        iterator.disable_recursion_pending();
        continue;
      }
      const std::filesystem::path current_path = iterator->path();
      const std::string current_name = current_path.filename().string();
      if (!current_name.empty() && current_name.front() == '.') {
        if (iterator->is_directory(error)) {
          iterator.disable_recursion_pending();
        }
        error.clear();
        continue;
      }
      if (iterator->is_directory(error)) {
        error.clear();
        if (!LooksLikeRecognizedModelDirectoryForLibrary(current_path)) {
          continue;
        }
        iterator.disable_recursion_pending();
        ModelLibraryEntry entry;
        entry.path = NormalizePathString(current_path);
        entry.name = current_path.filename().string();
        entry.kind = "directory";
        entry.format = "model-directory";
        entry.root = root_text;
        entry.paths = {entry.path};
        entry.size_bytes = FileSizeIfExistsForModelLibrary(current_path).value_or(0);
        const auto reference_it = reference_map.find(entry.path);
        if (reference_it != reference_map.end()) {
          entry.referenced_by = reference_it->second;
          entry.deletable = false;
        }
        entries_by_path[entry.path] = std::move(entry);
        continue;
      }
      if (!iterator->is_regular_file(error)) {
        error.clear();
        continue;
      }
      error.clear();
      if (!EndsWithIgnoreCase(current_name, ".gguf")) {
        continue;
      }
      std::string multipart_prefix;
      int part_index = 0;
      int part_total = 0;
      if (ParseMultipartGgufFilename(current_name, &multipart_prefix, &part_index, &part_total)) {
        const std::string group_key =
            NormalizePathString(current_path.parent_path() / multipart_prefix);
        auto& group = multipart_groups[group_key];
        group.root = root_text;
        group.name = multipart_prefix;
        group.part_total = std::max(group.part_total, part_total);
        group.paths.push_back(NormalizePathString(current_path));
        group.size_bytes += iterator->file_size(error);
        continue;
      }
      ModelLibraryEntry entry;
      entry.path = NormalizePathString(current_path);
      entry.name = current_name;
      entry.kind = "file";
      entry.format = "gguf";
      entry.root = root_text;
      entry.paths = {entry.path};
      entry.size_bytes = iterator->file_size(error);
      const auto reference_it = reference_map.find(entry.path);
      if (reference_it != reference_map.end()) {
        entry.referenced_by = reference_it->second;
        entry.deletable = false;
      }
      entries_by_path[entry.path] = std::move(entry);
    }
  }

  for (auto& [group_key, group] : multipart_groups) {
    std::sort(group.paths.begin(), group.paths.end());
    group.paths.erase(std::unique(group.paths.begin(), group.paths.end()), group.paths.end());
    if (group.paths.empty()) {
      continue;
    }
    ModelLibraryEntry entry;
    entry.path = group.paths.front();
    entry.name = group.name;
    entry.kind = "multipart-gguf";
    entry.format = "gguf";
    entry.root = group.root;
    entry.paths = group.paths;
    entry.size_bytes = group.size_bytes;
    entry.part_count = static_cast<int>(group.paths.size());
    bool referenced = false;
    for (const auto& path : group.paths) {
      const auto reference_it = reference_map.find(path);
      if (reference_it == reference_map.end()) {
        continue;
      }
      referenced = true;
      entry.referenced_by.insert(
          entry.referenced_by.end(),
          reference_it->second.begin(),
          reference_it->second.end());
    }
    if (const auto reference_it = reference_map.find(group_key); reference_it != reference_map.end()) {
      referenced = true;
      entry.referenced_by.insert(
          entry.referenced_by.end(),
          reference_it->second.begin(),
          reference_it->second.end());
    }
    if (referenced) {
      std::sort(entry.referenced_by.begin(), entry.referenced_by.end());
      entry.referenced_by.erase(
          std::unique(entry.referenced_by.begin(), entry.referenced_by.end()),
          entry.referenced_by.end());
      entry.deletable = false;
    }
    entries_by_path[group_key] = std::move(entry);
  }

  std::vector<ModelLibraryEntry> entries;
  entries.reserve(entries_by_path.size());
  for (auto& [_, entry] : entries_by_path) {
    entries.push_back(std::move(entry));
  }
  std::sort(
      entries.begin(),
      entries.end(),
      [](const ModelLibraryEntry& left, const ModelLibraryEntry& right) {
        if (left.root != right.root) {
          return left.root < right.root;
        }
        return left.name < right.name;
      });
  return entries;
}

json BuildModelLibraryPayload(const std::string& db_path) {
  comet::ControllerStore store(db_path);
  const auto desired_states = store.LoadDesiredStates();
  const auto roots = DiscoverModelLibraryRoots(desired_states);
  const auto entries = ScanModelLibraryEntries(db_path);
  json items = json::array();
  for (const auto& entry : entries) {
    items.push_back(json{
        {"path", entry.path},
        {"name", entry.name},
        {"kind", entry.kind},
        {"format", entry.format},
        {"root", entry.root},
        {"paths", entry.paths},
        {"size_bytes", entry.size_bytes},
        {"part_count", entry.part_count},
        {"referenced_by", entry.referenced_by},
        {"deletable", entry.deletable},
    });
  }
  json jobs = json::array();
  {
    std::lock_guard<std::mutex> lock(g_model_library_jobs_mutex);
    for (const auto& [_, job] : g_model_library_jobs) {
      jobs.push_back(BuildModelLibraryJobPayload(job));
    }
  }
  return json{
      {"items", items},
      {"roots", roots},
      {"jobs", jobs},
  };
}

std::string GenerateModelLibraryJobId() {
  return "mdl-" + std::to_string(static_cast<unsigned long long>(std::time(nullptr))) + "-" +
         std::to_string(static_cast<unsigned long long>(g_model_library_job_counter.fetch_add(1)));
}

void DownloadModelLibraryFile(
    const std::string& job_id,
    const std::string& source_url,
    const std::filesystem::path& target_path,
    std::uintmax_t aggregate_prefix,
    const std::optional<std::uintmax_t>& aggregate_total) {
  std::filesystem::create_directories(target_path.parent_path());
  const std::filesystem::path temp_path = target_path.string() + ".part";
  std::filesystem::remove(temp_path);
  auto future = std::async(
      std::launch::async,
      [temp_path_text = temp_path.string(), source_url]() {
        const std::string command = "/usr/bin/curl -fL --silent --show-error --output '" +
                                    temp_path_text + "' '" + source_url + "'";
        return std::system(command.c_str());
      });
  while (future.wait_for(std::chrono::milliseconds(500)) != std::future_status::ready) {
    const auto bytes_done = FileSizeIfExistsForModelLibrary(temp_path).value_or(0);
    UpdateModelLibraryJob(
        job_id,
        [&](ModelLibraryDownloadJob& job) {
          job.bytes_done = aggregate_prefix + bytes_done;
          job.bytes_total = aggregate_total;
          job.current_item = target_path.filename().string();
          job.status = "running";
        });
  }
  const int rc = future.get();
  if (rc != 0) {
    throw std::runtime_error("failed to download model artifact from " + source_url);
  }
  std::filesystem::rename(temp_path, target_path);
  const auto final_size = FileSizeIfExistsForModelLibrary(target_path).value_or(0);
  UpdateModelLibraryJob(
      job_id,
      [&](ModelLibraryDownloadJob& job) {
        job.bytes_done = aggregate_prefix + final_size;
        job.bytes_total = aggregate_total;
        job.current_item = target_path.filename().string();
      });
}

void StartModelLibraryDownloadJob(const std::string& job_id) {
  std::thread([job_id]() {
    try {
      ModelLibraryDownloadJob snapshot;
      {
        std::lock_guard<std::mutex> lock(g_model_library_jobs_mutex);
        snapshot = g_model_library_jobs.at(job_id);
      }
      std::optional<std::uintmax_t> aggregate_total = std::uintmax_t{0};
      for (const auto& source_url : snapshot.source_urls) {
        const auto content_length = ProbeContentLengthForModelLibrary(source_url);
        if (!content_length.has_value()) {
          aggregate_total = std::nullopt;
          break;
        }
        *aggregate_total += *content_length;
      }
      UpdateModelLibraryJob(
          job_id,
          [&](ModelLibraryDownloadJob& job) {
            job.status = "running";
            job.bytes_total = aggregate_total;
          });
      std::uintmax_t aggregate_prefix = 0;
      for (std::size_t index = 0; index < snapshot.source_urls.size(); ++index) {
        const std::filesystem::path target_path(snapshot.target_paths.at(index));
        DownloadModelLibraryFile(
            job_id,
            snapshot.source_urls.at(index),
            target_path,
            aggregate_prefix,
            aggregate_total);
        aggregate_prefix += FileSizeIfExistsForModelLibrary(target_path).value_or(0);
      }
      UpdateModelLibraryJob(
          job_id,
          [&](ModelLibraryDownloadJob& job) {
            job.status = "completed";
            job.bytes_done = aggregate_prefix;
            job.bytes_total = aggregate_total;
            job.current_item.clear();
          });
    } catch (const std::exception& error) {
      UpdateModelLibraryJob(
          job_id,
          [&](ModelLibraryDownloadJob& job) {
            job.status = "failed";
            job.error_message = error.what();
            job.current_item.clear();
          });
    }
  }).detach();
}

HttpResponse EnqueueModelLibraryDownload(const HttpRequest& request) {
  const json body = ParseJsonRequestBody(request);
  const std::string target_root = body.value("target_root", std::string{});
  const std::string target_subdir = body.value("target_subdir", std::string{});
  const std::string model_id = body.value("model_id", std::string{});
  const std::string source_url = body.value("source_url", std::string{});
  std::vector<std::string> source_urls;
  if (body.contains("source_urls") && body.at("source_urls").is_array()) {
    source_urls = body.at("source_urls").get<std::vector<std::string>>();
  } else if (!source_url.empty()) {
    source_urls.push_back(source_url);
  }
  if (target_root.empty() || !IsUsableAbsoluteHostPath(target_root)) {
    return BuildJsonResponse(
        400,
        json{{"status", "bad_request"}, {"message", "target_root must be an absolute host path"}});
  }
  if (source_urls.empty()) {
    return BuildJsonResponse(
        400,
        json{{"status", "bad_request"}, {"message", "source_url or source_urls is required"}});
  }
  std::filesystem::path destination_root(target_root);
  if (!target_subdir.empty()) {
    destination_root /= target_subdir;
  }
  std::vector<std::string> target_paths;
  target_paths.reserve(source_urls.size());
  try {
    for (std::size_t index = 0; index < source_urls.size(); ++index) {
      const auto filename =
          source_urls.size() == 1 && body.contains("target_filename") &&
                  body.at("target_filename").is_string() &&
                  !body.at("target_filename").get<std::string>().empty()
              ? body.at("target_filename").get<std::string>()
              : FilenameFromUrlForModelLibrary(source_urls.at(index));
      target_paths.push_back(NormalizePathString(destination_root / filename));
    }
  } catch (const std::exception& error) {
    return BuildJsonResponse(
        400,
        json{{"status", "bad_request"}, {"message", error.what()}});
  }
  const std::string job_id = GenerateModelLibraryJobId();
  ModelLibraryDownloadJob job;
  job.id = job_id;
  job.model_id = model_id;
  job.target_root = NormalizePathString(std::filesystem::path(target_root));
  job.target_subdir = target_subdir;
  job.source_urls = source_urls;
  job.target_paths = target_paths;
  job.part_count = static_cast<int>(source_urls.size());
  job.created_at = UtcNowSqlTimestamp();
  job.updated_at = job.created_at;
  {
    std::lock_guard<std::mutex> lock(g_model_library_jobs_mutex);
    g_model_library_jobs.emplace(job_id, job);
  }
  StartModelLibraryDownloadJob(job_id);
  return BuildJsonResponse(202, json{{"status", "accepted"}, {"job", BuildModelLibraryJobPayload(job)}});
}

HttpResponse DeleteModelLibraryEntryByPath(
    const std::string& db_path,
    const HttpRequest& request) {
  json body = json::object();
  if (!request.body.empty()) {
    body = ParseJsonRequestBody(request);
  }
  const std::string path = [&]() {
    if (body.contains("path") && body.at("path").is_string()) {
      return body.at("path").get<std::string>();
    }
    const auto query = FindQueryString(request, "path");
    return query.value_or(std::string{});
  }();
  if (path.empty() || !IsUsableAbsoluteHostPath(path)) {
    return BuildJsonResponse(
        400,
        json{{"status", "bad_request"}, {"message", "path must be an absolute host path"}});
  }
  const auto entries = ScanModelLibraryEntries(db_path);
  const auto it = std::find_if(
      entries.begin(),
      entries.end(),
      [&](const ModelLibraryEntry& entry) { return entry.path == NormalizePathString(path); });
  if (it == entries.end()) {
    return BuildJsonResponse(
        404,
        json{{"status", "not_found"}, {"message", "model entry not found"}});
  }
  if (!it->deletable) {
    return BuildJsonResponse(
        409,
        json{{"status", "conflict"},
             {"message", "model is referenced by one or more planes"},
             {"referenced_by", it->referenced_by}});
  }
  std::vector<std::string> deleted_paths;
  std::error_code error;
  if (it->kind == "directory") {
    std::filesystem::remove_all(it->path, error);
    if (error) {
      return BuildJsonResponse(
          500,
          json{{"status", "internal_error"}, {"message", error.message()}});
    }
    deleted_paths.push_back(it->path);
  } else {
    for (const auto& current_path : it->paths) {
      std::filesystem::remove(current_path, error);
      if (error) {
        return BuildJsonResponse(
            500,
            json{{"status", "internal_error"}, {"message", error.message()}});
      }
      deleted_paths.push_back(current_path);
    }
  }
  return BuildJsonResponse(
      200,
      json{{"status", "deleted"}, {"path", it->path}, {"deleted_paths", deleted_paths}});
}

std::string SqlTimestampAfterSeconds(int seconds) {
  const std::time_t future = std::time(nullptr) + seconds;
  std::tm tm{};
  if (!comet::platform::GmTime(&future, &tm)) {
    throw std::runtime_error("failed to format future UTC timestamp");
  }
  std::ostringstream out;
  out << std::put_time(&tm, "%Y-%m-%d %H:%M:%S");
  return out.str();
}

std::optional<std::tm> ParseDisplayTimestamp(const std::string& value) {
  if (value.empty()) {
    return std::nullopt;
  }
  for (const char* format : {"%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%dT%H:%M:%S"}) {
    std::tm tm{};
    std::istringstream input(value);
    input >> std::get_time(&tm, format);
    if (!input.fail()) {
      return tm;
    }
  }
  return std::nullopt;
}

std::string FormatDisplayTimestamp(const std::string& value) {
  const auto parsed = ParseDisplayTimestamp(value);
  if (!parsed.has_value()) {
    return value.empty() ? "(empty)" : value;
  }
  std::ostringstream output;
  output << std::put_time(&*parsed, "%d/%m/%Y %H:%M:%S");
  return output.str();
}

std::string GuessContentType(const std::filesystem::path& file_path) {
  const std::string extension = file_path.extension().string();
  if (extension == ".html") {
    return "text/html; charset=utf-8";
  }
  if (extension == ".js" || extension == ".mjs") {
    return "application/javascript; charset=utf-8";
  }
  if (extension == ".css") {
    return "text/css; charset=utf-8";
  }
  if (extension == ".json") {
    return "application/json; charset=utf-8";
  }
  if (extension == ".svg") {
    return "image/svg+xml";
  }
  if (extension == ".png") {
    return "image/png";
  }
  if (extension == ".jpg" || extension == ".jpeg") {
    return "image/jpeg";
  }
  if (extension == ".ico") {
    return "image/x-icon";
  }
  if (extension == ".txt") {
    return "text/plain; charset=utf-8";
  }
  return "application/octet-stream";
}

std::optional<std::filesystem::path> ResolveUiRequestPath(
    const std::filesystem::path& ui_root,
    const std::string& request_path) {
  if (request_path.empty() || request_path[0] != '/') {
    return std::nullopt;
  }
  if (request_path.rfind("/api/", 0) == 0 || request_path == "/health") {
    return std::nullopt;
  }

  std::filesystem::path relative_path;
  if (request_path == "/") {
    relative_path = "index.html";
  } else {
    relative_path = std::filesystem::path(request_path.substr(1)).lexically_normal();
  }
  if (relative_path.empty()) {
    relative_path = "index.html";
  }
  if (relative_path.is_absolute()) {
    return std::nullopt;
  }
  for (const auto& part : relative_path) {
    if (part == "..") {
      return std::nullopt;
    }
  }

  const auto candidate = ui_root / relative_path;
  if (std::filesystem::is_regular_file(candidate)) {
    return candidate;
  }

  const auto fallback = ui_root / "index.html";
  if (std::filesystem::is_regular_file(fallback)) {
    return fallback;
  }
  return std::nullopt;
}

HttpResponse BuildStaticFileResponse(const std::filesystem::path& file_path) {
  std::ifstream input(file_path, std::ios::binary);
  if (!input.is_open()) {
    throw std::runtime_error("failed to open static asset: " + file_path.string());
  }
  std::ostringstream buffer;
  buffer << input.rdbuf();
  return HttpResponse{200, GuessContentType(file_path), buffer.str(), {}};
}

int EmitRemoteJsonPayload(const json& payload) {
  const int http_status = payload.value("_http_status", 200);
  json sanitized = payload;
  sanitized.erase("_http_status");
  if (http_status >= 400) {
    std::cerr << sanitized.dump(2) << "\n";
    return 1;
  }
  std::cout << sanitized.dump(2) << "\n";
  return 0;
}

int EmitRemoteControllerActionPayload(const json& payload) {
  const int http_status = payload.value("_http_status", 200);
  json sanitized = payload;
  sanitized.erase("_http_status");
  if (http_status >= 400) {
    std::cerr << sanitized.dump(2) << "\n";
    return 1;
  }
  const std::string output = sanitized.value("output", "");
  if (!output.empty()) {
    std::cout << output;
    if (output.back() != '\n') {
      std::cout << "\n";
    }
  } else {
    std::cout << sanitized.dump(2) << "\n";
  }
  return sanitized.value("exit_code", 0);
}

comet::controller::ControllerActionResult ExecuteValidateBundleAction(const std::string& bundle_dir) {
  return comet::controller::RunControllerActionResult(
      "validate-bundle",
      [&]() { return ValidateBundle(bundle_dir); });
}

comet::controller::ControllerActionResult ExecutePreviewBundleAction(
    const std::string& bundle_dir,
    const std::optional<std::string>& node_name) {
  return comet::controller::RunControllerActionResult(
      "preview-bundle",
      [&]() { return PreviewBundle(bundle_dir, node_name); });
}

comet::controller::ControllerActionResult ExecuteImportBundleAction(
    const std::string& db_path,
    const std::string& bundle_dir) {
  return comet::controller::RunControllerActionResult(
      "import-bundle",
      [&]() { return ImportBundle(db_path, bundle_dir); });
}

comet::controller::ControllerActionResult ExecuteApplyBundleAction(
    const std::string& db_path,
    const std::string& bundle_dir,
    const std::string& artifacts_root) {
  return comet::controller::RunControllerActionResult(
      "apply-bundle",
      [&]() { return ApplyBundle(db_path, bundle_dir, artifacts_root); });
}

int ApplyDesiredState(
    const std::string& db_path,
    const comet::DesiredState& desired_state,
    const std::string& artifacts_root,
    const std::string& source_label) {
  comet::ControllerStore store(db_path);
  store.Initialize();
  comet::DesiredState effective_desired_state = desired_state;
  ApplyRegisteredHostExecutionModes(store, &effective_desired_state);
  ResolveDesiredStateDynamicPlacements(store, &effective_desired_state);
  ValidateDesiredStateForControllerAdmission(effective_desired_state);
  ValidateDesiredStateExecutionModes(effective_desired_state);
  const auto current_state = store.LoadDesiredState(effective_desired_state.plane_name);
  comet::RequireSchedulingPolicy(effective_desired_state);
  const auto availability_overrides = store.LoadNodeAvailabilityOverrides();
  const auto observations = store.LoadHostObservations();
  const int desired_generation =
      store.LoadDesiredGeneration(effective_desired_state.plane_name).value_or(0) + 1;
  const comet::ReconcilePlan plan =
      comet::BuildReconcilePlan(current_state, effective_desired_state);
  const comet::SchedulingPolicyReport scheduling_report =
      comet::EvaluateSchedulingPolicy(effective_desired_state);
  const auto host_plans =
      comet::BuildNodeExecutionPlans(current_state, effective_desired_state, artifacts_root);

  std::cout << "apply-plan:\n";
  std::cout << comet::RenderReconcilePlan(plan);
  std::cout << comet::RenderSchedulingPolicyReport(scheduling_report);
  PrintSchedulerDecisionSummary(effective_desired_state);
  PrintRolloutGateSummary(scheduling_report);
  std::cout << comet::RenderNodeExecutionPlans(host_plans);

  MaterializeComposeArtifacts(effective_desired_state, host_plans);
  MaterializeInferRuntimeArtifact(effective_desired_state, artifacts_root);
  store.ReplaceDesiredState(effective_desired_state, desired_generation, 0);
  store.UpdatePlaneArtifactsRoot(effective_desired_state.plane_name, artifacts_root);
  store.ClearSchedulerPlaneRuntime(effective_desired_state.plane_name);
  store.ReplaceRolloutActions(effective_desired_state.plane_name, desired_generation, {});

  const bool existed = current_state.has_value();
  AppendControllerEvent(
      store,
      "plane",
      existed ? "staged-update" : "created",
      existed ? "updated plane desired state; rollout is staged until explicit restart"
             : "created plane desired state in stopped lifecycle state",
      json{
          {"source", source_label},
          {"artifacts_root", artifacts_root},
          {"desired_generation", desired_generation},
          {"applied_generation",
           current_state.has_value()
               ? json(store.LoadPlane(effective_desired_state.plane_name)->applied_generation)
               : json(0)},
          {"worker_count", effective_desired_state.instances.size()},
          {"disk_count", effective_desired_state.disks.size()},
      },
      effective_desired_state.plane_name);
  std::cout << (existed ? "staged update for" : "created") << " plane '" << effective_desired_state.plane_name
            << "' in: " << db_path << "\n";
  std::cout << "desired generation: " << desired_generation << "\n";
  const auto plane = store.LoadPlane(effective_desired_state.plane_name);
  if (plane.has_value()) {
    std::cout << "applied generation: " << plane->applied_generation << "\n";
    std::cout << "plane state: " << plane->state << "\n";
  }
  std::cout << "artifacts written under: " << artifacts_root << "\n";
  std::cout << "runtime rollout is staged; use start-plane to enqueue host assignments\n";
  return 0;
}

comet::controller::ControllerActionResult ExecuteUpsertPlaneStateAction(
    const std::string& db_path,
    const std::string& desired_state_json,
    const std::string& artifacts_root,
    const std::optional<std::string>& expected_plane_name,
    const std::string& source_label) {
  return comet::controller::RunControllerActionResult(
      "upsert-plane-state",
      [&]() {
        const auto desired_state = comet::DeserializeDesiredStateJson(desired_state_json);
        if (expected_plane_name.has_value() &&
            desired_state.plane_name != *expected_plane_name) {
          throw std::runtime_error(
              "plane name mismatch: expected '" + *expected_plane_name + "' but payload contains '" +
              desired_state.plane_name + "'");
        }
        return ApplyDesiredState(db_path, desired_state, artifacts_root, source_label);
      });
}

comet::controller::ControllerActionResult ExecuteSetNodeAvailabilityAction(
    const std::string& db_path,
    const std::string& node_name,
    comet::NodeAvailability availability,
    const std::optional<std::string>& status_message) {
  return comet::controller::RunControllerActionResult(
      "set-node-availability",
      [&]() { return SetNodeAvailability(db_path, node_name, availability, status_message); });
}

comet::controller::ControllerActionResult ExecuteRetryHostAssignmentAction(
    const std::string& db_path,
    int assignment_id) {
  return comet::controller::RunControllerActionResult(
      "retry-host-assignment",
      [&]() { return RetryHostAssignment(db_path, assignment_id); });
}

PlaneService MakePlaneService(const std::string& db_path) {
  return PlaneService(
      db_path,
      [](const std::string& value) { return FormatDisplayTimestamp(value); },
      [](const comet::DesiredState& state) { PrintStateSummary(state); },
      [](comet::ControllerStore& store, comet::DesiredState* desired_state) {
        ApplyRegisteredHostExecutionModes(store, desired_state);
        ResolveDesiredStateDynamicPlacements(store, desired_state);
        ValidateDesiredStateForControllerAdmission(*desired_state);
        ValidateDesiredStateExecutionModes(*desired_state);
      },
      [](comet::ControllerStore& store,
         const std::string& category,
         const std::string& event_type,
         const std::string& message,
         const json& payload,
         const std::string& plane_name) {
        AppendControllerEvent(store, category, event_type, message, payload, plane_name);
      },
      [](comet::ControllerStore& store, const std::string& plane_name) {
        return CanFinalizeDeletedPlane(store, plane_name);
      },
      [](const std::vector<comet::HostAssignment>& assignments, const std::string& plane_name) {
        return FindLatestHostAssignmentForPlane(assignments, plane_name);
      },
      [](const comet::DesiredState& desired_state,
         const std::string& artifacts_root,
         int desired_generation,
         const std::vector<comet::NodeAvailabilityOverride>& availability_overrides,
         const std::vector<comet::HostObservation>& observations,
         const comet::SchedulingPolicyReport& scheduling_report) {
        return BuildHostAssignments(
            desired_state,
            artifacts_root,
            desired_generation,
            availability_overrides,
            observations,
            scheduling_report);
      },
      [](const comet::DesiredState& desired_state,
         int desired_generation,
         const std::string& artifacts_root,
         const std::vector<comet::NodeAvailabilityOverride>& availability_overrides) {
        return BuildStopPlaneAssignments(
            desired_state,
            desired_generation,
            artifacts_root,
            availability_overrides);
      },
      [](const comet::DesiredState& desired_state,
         int desired_generation,
         const std::string& artifacts_root) {
        return BuildDeletePlaneAssignments(
            desired_state,
            desired_generation,
            artifacts_root);
      },
      []() { return DefaultArtifactsRoot(); });
}

SchedulerService MakeSchedulerService(
    const std::string& db_path,
    const std::string& artifacts_root) {
  return SchedulerService(
      [&](const std::optional<std::string>& node_name,
          const std::optional<std::string>& plane_name) {
        return ShowRolloutActions(db_path, node_name, plane_name);
      },
      [&](const std::optional<std::string>& node_name,
          const std::optional<std::string>& plane_name) {
        return ShowRebalancePlan(db_path, node_name, plane_name);
      },
      [&](const std::optional<std::string>& plane_name,
          const std::optional<std::string>& node_name,
          const std::optional<std::string>& worker_name,
          const std::optional<std::string>& category,
          int limit) {
        return ShowEvents(db_path, plane_name, node_name, worker_name, category, limit);
      },
      [&](const std::string& worker_name) {
        return comet::controller::RunControllerActionResult(
            "apply-rebalance-proposal",
            [&]() { return ApplyRebalanceProposal(db_path, worker_name, artifacts_root); });
      },
      [&]() {
        return comet::controller::RunControllerActionResult(
            "reconcile-rebalance-proposals",
            [&]() { return ReconcileRebalanceProposals(db_path, artifacts_root); });
      },
      [&]() {
        return comet::controller::RunControllerActionResult(
            "scheduler-tick",
            [&]() { return SchedulerTick(db_path, artifacts_root); });
      },
      [&](int action_id,
          const std::string& requested_status,
          const std::optional<std::string>& message) {
        return comet::controller::RunControllerActionResult(
            "set-rollout-action-status",
            [&]() {
              return SetRolloutActionStatus(
                  db_path,
                  action_id,
                  comet::ParseRolloutActionStatus(requested_status),
                  message);
            });
      },
      [&](int action_id) {
        return comet::controller::RunControllerActionResult(
            "enqueue-rollout-eviction",
            [&]() { return EnqueueRolloutEviction(db_path, action_id); });
      },
      [&]() {
        return comet::controller::RunControllerActionResult(
            "reconcile-rollout-actions",
            [&]() { return ReconcileRolloutActions(db_path, artifacts_root); });
      },
      [&](int action_id) {
        return comet::controller::RunControllerActionResult(
            "apply-ready-rollout-action",
            [&]() { return ApplyReadyRolloutAction(db_path, action_id, artifacts_root); });
      });
}

comet::controller::ControllerActionResult ExecuteStartPlaneAction(
    const std::string& db_path,
    const std::string& plane_name) {
  PlaneService plane_service = MakePlaneService(db_path);
  return comet::controller::RunControllerActionResult(
      "start-plane",
      [&]() { return plane_service.StartPlane(plane_name); });
}

HostRegistryService MakeHostRegistryService(const std::string& db_path) {
  return HostRegistryService(
      db_path,
      [](comet::ControllerStore& store,
         const std::string& event_type,
         const std::string& message,
         const json& payload,
         const std::string& node_name,
         const std::string& severity) {
        AppendControllerEvent(
            store,
            "host-registry",
            event_type,
            message,
            payload,
            "",
            node_name,
            "",
            std::nullopt,
            std::nullopt,
            severity);
      });
}

comet::controller::ControllerActionResult ExecuteStopPlaneAction(
    const std::string& db_path,
    const std::string& plane_name) {
  PlaneService plane_service = MakePlaneService(db_path);
  return comet::controller::RunControllerActionResult(
      "stop-plane",
      [&]() { return plane_service.StopPlane(plane_name); });
}

comet::controller::ControllerActionResult ExecuteDeletePlaneAction(
    const std::string& db_path,
    const std::string& plane_name) {
  PlaneService plane_service = MakePlaneService(db_path);
  return comet::controller::RunControllerActionResult(
      "delete-plane",
      [&]() { return plane_service.DeletePlane(plane_name); });
}

comet::controller::ControllerActionResult ExecuteRevokeHostdAction(
    const std::string& db_path,
    const std::string& node_name,
    const std::optional<std::string>& status_message) {
  const auto service = MakeHostRegistryService(db_path);
  return comet::controller::RunControllerActionResult(
      "revoke-hostd",
      [&]() { return service.RevokeHost(node_name, status_message); });
}

comet::controller::ControllerActionResult ExecuteRotateHostdKeyAction(
    const std::string& db_path,
    const std::string& node_name,
    const std::string& public_key_base64,
    const std::optional<std::string>& status_message) {
  const auto service = MakeHostRegistryService(db_path);
  return comet::controller::RunControllerActionResult(
      "rotate-hostd-key",
      [&]() { return service.RotateHostKey(node_name, public_key_base64, status_message); });
}

int ExecuteRemoteControllerCommand(
    const ControllerEndpointTarget& target,
    const std::string& command,
    const ControllerCommandLine& cli) {
  const auto plane_name = cli.plane();
  const auto node_name = cli.node();
  const auto stale_after = cli.stale_after();
  const auto bundle_dir = cli.bundle();
  const auto artifacts_root = cli.artifacts_root();
  const auto action_id = cli.id();
  const auto worker_name = cli.worker();
  const auto limit = cli.limit();
  const auto category = cli.category();
  const auto message = cli.message();
  const auto status = cli.status();
  const auto availability = cli.availability();

  if (command == "list-planes") {
    std::cout << SendControllerJsonRequest(target, "GET", "/api/v1/planes").dump(2) << "\n";
    return 0;
  }
  if (command == "show-plane") {
    if (!plane_name.has_value()) {
      throw std::runtime_error("missing required --plane for remote show-plane");
    }
    std::cout << SendControllerJsonRequest(target, "GET", "/api/v1/planes/" + ControllerHttpServerSupport::UrlEncode(*plane_name))
                     .dump(2)
              << "\n";
    return 0;
  }
  if (command == "start-plane") {
    if (!plane_name.has_value()) {
      throw std::runtime_error("missing required --plane for remote start-plane");
    }
    return EmitRemoteControllerActionPayload(
        SendControllerJsonRequest(
            target,
            "POST",
            "/api/v1/planes/" + ControllerHttpServerSupport::UrlEncode(*plane_name) + "/start"));
  }
  if (command == "stop-plane") {
    if (!plane_name.has_value()) {
      throw std::runtime_error("missing required --plane for remote stop-plane");
    }
    return EmitRemoteControllerActionPayload(
        SendControllerJsonRequest(
            target,
            "POST",
            "/api/v1/planes/" + ControllerHttpServerSupport::UrlEncode(*plane_name) + "/stop"));
  }
  if (command == "delete-plane") {
    if (!plane_name.has_value()) {
      throw std::runtime_error("missing required --plane for remote delete-plane");
    }
    return EmitRemoteControllerActionPayload(
        SendControllerJsonRequest(
            target,
            "DELETE",
            "/api/v1/planes/" + ControllerHttpServerSupport::UrlEncode(*plane_name)));
  }
  if (command == "show-state") {
    return EmitRemoteJsonPayload(
        SendControllerJsonRequest(target, "GET", "/api/v1/state"));
  }
  if (command == "show-hostd-hosts") {
    return EmitRemoteJsonPayload(
        SendControllerJsonRequest(
            target,
            "GET",
            "/api/v1/hostd/hosts",
            {{"node", node_name.value_or("")}}));
  }
  if (command == "revoke-hostd") {
    if (!node_name.has_value()) {
      std::cerr << "error: --node is required\n";
      return 1;
    }
    return EmitRemoteControllerActionPayload(
        SendControllerJsonRequest(
            target,
            "POST",
            "/api/v1/hostd/hosts/" + ControllerHttpServerSupport::UrlEncode(*node_name) + "/revoke",
            {{"message", message.value_or("")}}));
  }
  if (command == "rotate-hostd-key") {
    if (!node_name.has_value()) {
      std::cerr << "error: --node is required\n";
      return 1;
    }
    const auto public_key = cli.public_key_base64();
    if (!public_key.has_value()) {
      std::cerr << "error: --public-key is required\n";
      return 1;
    }
    return EmitRemoteControllerActionPayload(
        SendControllerJsonRequest(
            target,
            "POST",
            "/api/v1/hostd/hosts/" + ControllerHttpServerSupport::UrlEncode(*node_name) + "/rotate-key",
            json{
                {"public_key_base64", *public_key},
                {"message", message.value_or("")},
            }));
  }
  if (command == "show-host-assignments") {
    return EmitRemoteJsonPayload(
        SendControllerJsonRequest(
            target,
            "GET",
            "/api/v1/host-assignments",
            {{"node", node_name.value_or("")}}));
  }
  if (command == "show-host-observations") {
    return EmitRemoteJsonPayload(
        SendControllerJsonRequest(
            target,
            "GET",
            "/api/v1/host-observations",
            {{"plane", plane_name.value_or("")},
             {"node", node_name.value_or("")},
             {"stale_after", stale_after.has_value() ? std::to_string(*stale_after) : ""}}));
  }
  if (command == "show-host-health") {
    return EmitRemoteJsonPayload(
        SendControllerJsonRequest(
            target,
            "GET",
            "/api/v1/host-health",
            {{"node", node_name.value_or("")},
             {"stale_after", stale_after.has_value() ? std::to_string(*stale_after) : ""}}));
  }
  if (command == "show-disk-state") {
    return EmitRemoteJsonPayload(
        SendControllerJsonRequest(
            target,
            "GET",
            "/api/v1/disk-state",
            {{"node", node_name.value_or("")},
             {"plane", plane_name.value_or("")}}));
  }
  if (command == "show-rollout-actions") {
    return EmitRemoteJsonPayload(
        SendControllerJsonRequest(
            target,
            "GET",
            "/api/v1/rollout-actions",
            {{"node", node_name.value_or("")},
             {"plane", plane_name.value_or("")}}));
  }
  if (command == "show-rebalance-plan") {
    return EmitRemoteJsonPayload(
        SendControllerJsonRequest(
            target,
            "GET",
            "/api/v1/rebalance-plan",
            {{"node", node_name.value_or("")},
             {"plane", plane_name.value_or("")},
             {"stale_after", stale_after.has_value() ? std::to_string(*stale_after) : ""}}));
  }
  if (command == "show-events") {
    return EmitRemoteJsonPayload(
        SendControllerJsonRequest(
            target,
            "GET",
            "/api/v1/events",
            {{"plane", plane_name.value_or("")},
             {"node", node_name.value_or("")},
             {"worker", worker_name.value_or("")},
             {"category", category.value_or("")},
             {"limit", limit.has_value() ? std::to_string(*limit) : ""}}));
  }
  if (command == "show-node-availability") {
    return EmitRemoteJsonPayload(
        SendControllerJsonRequest(
            target,
            "GET",
            "/api/v1/node-availability",
            {{"node", node_name.value_or("")}}));
  }
  if (command == "validate-bundle") {
    if (!bundle_dir.has_value()) {
      std::cerr << "error: --bundle is required\n";
      return 1;
    }
    return EmitRemoteControllerActionPayload(
        SendControllerJsonRequest(
            target,
            "POST",
            "/api/v1/bundles/validate",
            {{"bundle", *bundle_dir}}));
  }
  if (command == "preview-bundle") {
    if (!bundle_dir.has_value()) {
      std::cerr << "error: --bundle is required\n";
      return 1;
    }
    return EmitRemoteControllerActionPayload(
        SendControllerJsonRequest(
            target,
            "POST",
            "/api/v1/bundles/preview",
            {{"bundle", *bundle_dir}, {"node", node_name.value_or("")}}));
  }
  if (command == "import-bundle") {
    if (!bundle_dir.has_value()) {
      std::cerr << "error: --bundle is required\n";
      return 1;
    }
    return EmitRemoteControllerActionPayload(
        SendControllerJsonRequest(
            target,
            "POST",
            "/api/v1/bundles/import",
            {{"bundle", *bundle_dir}}));
  }
  if (command == "apply-bundle") {
    if (!bundle_dir.has_value()) {
      std::cerr << "error: --bundle is required\n";
      return 1;
    }
    return EmitRemoteControllerActionPayload(
        SendControllerJsonRequest(
            target,
            "POST",
            "/api/v1/bundles/apply",
            {{"bundle", *bundle_dir},
             {"artifacts_root", artifacts_root.value_or("")}}));
  }
  if (command == "scheduler-tick") {
    return EmitRemoteControllerActionPayload(
        SendControllerJsonRequest(
            target,
            "POST",
            "/api/v1/scheduler-tick",
            {{"artifacts_root", artifacts_root.value_or("")}}));
  }
  if (command == "reconcile-rebalance-proposals") {
    return EmitRemoteControllerActionPayload(
        SendControllerJsonRequest(
            target,
            "POST",
            "/api/v1/reconcile-rebalance-proposals",
            {{"artifacts_root", artifacts_root.value_or("")}}));
  }
  if (command == "reconcile-rollout-actions") {
    return EmitRemoteControllerActionPayload(
        SendControllerJsonRequest(
            target,
            "POST",
            "/api/v1/reconcile-rollout-actions",
            {{"artifacts_root", artifacts_root.value_or("")}}));
  }
  if (command == "apply-rebalance-proposal") {
    if (!worker_name.has_value()) {
      std::cerr << "error: --worker is required\n";
      return 1;
    }
    return EmitRemoteControllerActionPayload(
        SendControllerJsonRequest(
            target,
            "POST",
            "/api/v1/apply-rebalance-proposal",
            {{"worker", *worker_name},
             {"artifacts_root", artifacts_root.value_or("")}}));
  }
  if (command == "set-rollout-action-status") {
    if (!action_id.has_value()) {
      std::cerr << "error: --id is required\n";
      return 1;
    }
    if (!status.has_value()) {
      std::cerr << "error: --status is required\n";
      return 1;
    }
    return EmitRemoteControllerActionPayload(
        SendControllerJsonRequest(
            target,
            "POST",
            "/api/v1/set-rollout-action-status",
            {{"id", std::to_string(*action_id)},
             {"status", *status},
             {"message", message.value_or("")}}));
  }
  if (command == "enqueue-rollout-eviction") {
    if (!action_id.has_value()) {
      std::cerr << "error: --id is required\n";
      return 1;
    }
    return EmitRemoteControllerActionPayload(
        SendControllerJsonRequest(
            target,
            "POST",
            "/api/v1/enqueue-rollout-eviction",
            {{"id", std::to_string(*action_id)}}));
  }
  if (command == "apply-ready-rollout-action") {
    if (!action_id.has_value()) {
      std::cerr << "error: --id is required\n";
      return 1;
    }
    return EmitRemoteControllerActionPayload(
        SendControllerJsonRequest(
            target,
            "POST",
            "/api/v1/apply-ready-rollout-action",
            {{"id", std::to_string(*action_id)},
             {"artifacts_root", artifacts_root.value_or("")}}));
  }
  if (command == "set-node-availability") {
    if (!node_name.has_value()) {
      std::cerr << "error: --node is required\n";
      return 1;
    }
    if (!availability.has_value()) {
      std::cerr << "error: --availability is required\n";
      return 1;
    }
    return EmitRemoteControllerActionPayload(
        SendControllerJsonRequest(
            target,
            "POST",
            "/api/v1/node-availability",
            {{"node", *node_name},
             {"availability", *availability},
             {"message", message.value_or("")}}));
  }
  if (command == "retry-host-assignment") {
    if (!action_id.has_value()) {
      std::cerr << "error: --id is required\n";
      return 1;
    }
    return EmitRemoteControllerActionPayload(
        SendControllerJsonRequest(
            target,
            "POST",
            "/api/v1/retry-host-assignment",
            {{"id", std::to_string(*action_id)}}));
  }

  std::cerr << "error: command '" << command
            << "' is not available through --controller yet\n";
  return 1;
}

std::string ResolveDbPath(const std::optional<std::string>& db_arg) {
  return db_arg.value_or(DefaultDbPath());
}

std::string ResolveArtifactsRoot(
    const std::optional<std::string>& artifacts_root_arg,
    const std::string& fallback_artifacts_root) {
  return artifacts_root_arg.value_or(fallback_artifacts_root);
}

std::optional<comet::HostAssignment> FindLatestHostAssignmentForNode(
    const std::vector<comet::HostAssignment>& assignments,
    const std::string& node_name);

std::optional<comet::HostAssignment> FindLatestHostAssignmentForPlane(
    const std::vector<comet::HostAssignment>& assignments,
    const std::string& plane_name);

std::map<std::string, comet::NodeAvailabilityOverride> BuildAvailabilityOverrideMap(
    const std::vector<comet::NodeAvailabilityOverride>& availability_overrides);

comet::NodeAvailability ResolveNodeAvailability(
    const std::map<std::string, comet::NodeAvailabilityOverride>& availability_overrides,
    const std::string& node_name);

bool IsNodeSchedulable(comet::NodeAvailability availability);

void PrintAssignmentDispatchSummary(
    const comet::DesiredState& desired_state,
    const std::map<std::string, comet::NodeAvailabilityOverride>& availability_overrides,
    const std::vector<comet::HostObservation>& observations,
    int stale_after_seconds);

std::optional<long long> HeartbeatAgeSeconds(const std::string& heartbeat_at);

std::optional<long long> TimestampAgeSeconds(const std::string& timestamp_text);

std::string HealthFromAge(
    const std::optional<long long>& age_seconds,
    int stale_after_seconds);

std::optional<comet::RuntimeStatus> ParseRuntimeStatus(
    const comet::HostObservation& observation);

std::vector<comet::RuntimeProcessStatus> ParseInstanceRuntimeStatuses(
    const comet::HostObservation& observation);

std::optional<comet::GpuTelemetrySnapshot> ParseGpuTelemetry(
    const comet::HostObservation& observation);
std::optional<comet::CpuTelemetrySnapshot> ParseCpuTelemetry(
    const comet::HostObservation& observation);

std::optional<std::string> ObservedSchedulingGateReason(
    const std::vector<comet::HostObservation>& observations,
    const std::string& node_name,
    int stale_after_seconds);

int ReconcileRolloutActions(
    const std::string& db_path,
    const std::string& artifacts_root);

int ReconcileRebalanceProposals(
    const std::string& db_path,
    const std::string& artifacts_root);

int SchedulerTick(
    const std::string& db_path,
    const std::string& artifacts_root);

std::map<std::string, std::vector<comet::SchedulerRolloutAction>> BuildRolloutActionsByTargetNode(
    const comet::SchedulingPolicyReport& scheduling_report);

void PrintRolloutGateSummary(const comet::SchedulingPolicyReport& scheduling_report);

void PrintStateSummary(const comet::DesiredState& state) {
  std::cout << "plane: " << state.plane_name << "\n";
  std::cout << "control_root: " << state.control_root << "\n";
  std::cout << "inference:\n";
  std::cout << "  primary_infer_node=" << state.inference.primary_infer_node
            << " net_if=" << state.inference.net_if
            << " llama_port=" << state.inference.llama_port << "\n";
  std::cout << "gateway:\n";
  std::cout << "  listen=" << state.gateway.listen_host << ":" << state.gateway.listen_port
            << " server_name=" << state.gateway.server_name << "\n";
  std::cout << "nodes:\n";
  for (const auto& node : state.nodes) {
    std::cout << "  - " << node.name << " (" << node.platform << "), gpus=";
    for (std::size_t index = 0; index < node.gpu_devices.size(); ++index) {
      if (index > 0) {
        std::cout << ",";
      }
      const auto it = node.gpu_memory_mb.find(node.gpu_devices[index]);
      std::cout << node.gpu_devices[index];
      if (it != node.gpu_memory_mb.end()) {
        std::cout << "(" << it->second << "MB)";
      }
    }
    std::cout << "\n";
  }

  std::cout << "disks:\n";
  for (const auto& disk : state.disks) {
    std::cout << "  - " << disk.name
              << " kind=" << comet::ToString(disk.kind)
              << " node=" << disk.node_name
              << " host_path=" << disk.host_path
              << " container_path=" << disk.container_path
              << " size_gb=" << disk.size_gb
              << "\n";
  }

  std::cout << "instances:\n";
  for (const auto& instance : state.instances) {
    std::cout << "  - " << instance.name
              << " role=" << comet::ToString(instance.role)
              << " node=" << instance.node_name;
    if (instance.gpu_device.has_value()) {
      std::cout << " gpu=" << *instance.gpu_device
                << " fraction=" << instance.gpu_fraction
                << " placement_mode=" << comet::ToString(instance.placement_mode)
                << " share_mode=" << comet::ToString(instance.share_mode)
                << " priority=" << instance.priority
                << " preemptible=" << (instance.preemptible ? "true" : "false");
      if (instance.memory_cap_mb.has_value()) {
        std::cout << " memory_cap_mb=" << *instance.memory_cap_mb;
      }
      const auto placement_it = instance.labels.find("comet.placement");
      if (placement_it != instance.labels.end()) {
        std::cout << " placement=" << placement_it->second;
      }
      const auto action_it = instance.labels.find("comet.placement.action");
      if (action_it != instance.labels.end()) {
        std::cout << " placement_action=" << action_it->second;
      }
      const auto score_it = instance.labels.find("comet.placement.score");
      if (score_it != instance.labels.end()) {
        std::cout << " placement_score=" << score_it->second;
      }
      const auto decision_it = instance.labels.find("comet.placement.decision");
      if (decision_it != instance.labels.end()) {
        std::cout << " placement_decision=" << decision_it->second;
      }
      const auto next_action_it = instance.labels.find("comet.placement.next_action");
      if (next_action_it != instance.labels.end()) {
        std::cout << " next_action=" << next_action_it->second;
      }
      const auto next_target_it = instance.labels.find("comet.placement.next_target");
      if (next_target_it != instance.labels.end()) {
        std::cout << " next_target=" << next_target_it->second;
      }
      const auto victims_it = instance.labels.find("comet.preemption.victims");
      if (victims_it != instance.labels.end()) {
        std::cout << " preemption_victims=" << victims_it->second;
      }
      const auto defer_reason_it = instance.labels.find("comet.placement.defer_reason");
      if (defer_reason_it != instance.labels.end()) {
        std::cout << " defer_reason=" << defer_reason_it->second;
      }
    }
    std::cout << "\n";
  }
}

void PrintDiskRuntimeStates(const std::vector<comet::DiskRuntimeState>& runtime_states) {
  std::cout << "disk-runtime-state:\n";
  if (runtime_states.empty()) {
    std::cout << "  (empty)\n";
    return;
  }

  for (const auto& runtime_state : runtime_states) {
    std::cout << "  - disk=" << runtime_state.disk_name
              << " node=" << runtime_state.node_name
              << " state="
              << (runtime_state.runtime_state.empty() ? "(empty)" : runtime_state.runtime_state);
    if (!runtime_state.mount_point.empty()) {
      std::cout << " mount_point=" << runtime_state.mount_point;
    }
    if (!runtime_state.filesystem_type.empty()) {
      std::cout << " filesystem=" << runtime_state.filesystem_type;
    }
    if (!runtime_state.image_path.empty()) {
      std::cout << " image=" << runtime_state.image_path;
    }
    if (!runtime_state.loop_device.empty()) {
      std::cout << " loop_device=" << runtime_state.loop_device;
    }
    if (!runtime_state.last_verified_at.empty()) {
      std::cout << " last_verified_at=" << runtime_state.last_verified_at;
    }
    std::cout << "\n";
    if (!runtime_state.status_message.empty()) {
      std::cout << "    message=" << runtime_state.status_message << "\n";
    }
  }
}

void PrintDetailedDiskState(
    const comet::DesiredState& state,
    const std::vector<comet::DiskRuntimeState>& runtime_states,
    const std::vector<comet::HostObservation>& observations,
    const std::optional<std::string>& node_name = std::nullopt) {
  std::map<std::string, comet::DiskRuntimeState> runtime_by_key;
  for (const auto& runtime_state : runtime_states) {
    runtime_by_key.emplace(runtime_state.disk_name + "@" + runtime_state.node_name, runtime_state);
  }
  std::map<std::string, comet::DiskTelemetryRecord> telemetry_by_key;
  for (const auto& observation : observations) {
    const auto disk_telemetry = ParseDiskTelemetry(observation);
    if (!disk_telemetry.has_value()) {
      continue;
    }
    for (const auto& item : disk_telemetry->items) {
      telemetry_by_key[item.disk_name + "@" + item.node_name] = item;
    }
  }

  std::cout << "disk-state:\n";
  bool printed = false;
  for (const auto& disk : state.disks) {
    if (node_name.has_value() && disk.node_name != *node_name) {
      continue;
    }
    printed = true;
    const std::string key = disk.name + "@" + disk.node_name;
    const auto runtime_it = runtime_by_key.find(key);
    std::cout << "  - disk=" << disk.name
              << " kind=" << comet::ToString(disk.kind)
              << " node=" << disk.node_name
              << " size_gb=" << disk.size_gb
              << " desired_host_path=" << disk.host_path
              << " desired_container_path=" << disk.container_path;
    if (runtime_it == runtime_by_key.end()) {
      std::cout << " realized_state=missing-runtime-state\n";
      continue;
    }

    const auto& runtime_state = runtime_it->second;
    std::cout << " realized_state="
              << (runtime_state.runtime_state.empty() ? "(empty)" : runtime_state.runtime_state);
    if (!runtime_state.mount_point.empty()) {
      std::cout << " mount_point=" << runtime_state.mount_point;
    }
    if (!runtime_state.filesystem_type.empty()) {
      std::cout << " filesystem=" << runtime_state.filesystem_type;
    }
    if (!runtime_state.image_path.empty()) {
      std::cout << " image=" << runtime_state.image_path;
    }
    if (!runtime_state.loop_device.empty()) {
      std::cout << " loop_device=" << runtime_state.loop_device;
    }
    if (!runtime_state.last_verified_at.empty()) {
      std::cout << " last_verified_at=" << runtime_state.last_verified_at;
    }
    const auto telemetry_it = telemetry_by_key.find(key);
    if (telemetry_it != telemetry_by_key.end()) {
      std::cout << " usage_bytes=" << telemetry_it->second.used_bytes
                << "/" << telemetry_it->second.total_bytes
                << " free_bytes=" << telemetry_it->second.free_bytes
                << " read_bytes=" << telemetry_it->second.read_bytes
                << " write_bytes=" << telemetry_it->second.write_bytes
                << " read_ios=" << telemetry_it->second.read_ios
                << " write_ios=" << telemetry_it->second.write_ios
                << " io_time_ms=" << telemetry_it->second.io_time_ms
                << " fault_count=" << telemetry_it->second.fault_count
                << " warning_count=" << telemetry_it->second.warning_count
                << " perf_counters=" << (telemetry_it->second.perf_counters_available ? "yes" : "no")
                << " io_error_counters="
                << (telemetry_it->second.io_error_counters_available ? "yes" : "no")
                << " mount_health="
                << (telemetry_it->second.health.empty() ? "(empty)" : telemetry_it->second.health);
    }
    std::cout << "\n";
    if (!runtime_state.status_message.empty()) {
      std::cout << "    message=" << runtime_state.status_message << "\n";
    }
  }

  for (const auto& runtime_state : runtime_states) {
    if (node_name.has_value() && runtime_state.node_name != *node_name) {
      continue;
    }
    const std::string key = runtime_state.disk_name + "@" + runtime_state.node_name;
    bool found_in_desired = false;
    for (const auto& disk : state.disks) {
      if (disk.name + "@" + disk.node_name == key) {
        found_in_desired = true;
        break;
      }
    }
    if (found_in_desired) {
      continue;
    }
    printed = true;
    std::cout << "  - disk=" << runtime_state.disk_name
              << " node=" << runtime_state.node_name
              << " realized_state="
              << (runtime_state.runtime_state.empty() ? "(empty)" : runtime_state.runtime_state)
              << " desired_state=(orphan-runtime-state)";
    if (!runtime_state.mount_point.empty()) {
      std::cout << " mount_point=" << runtime_state.mount_point;
    }
    if (!runtime_state.image_path.empty()) {
      std::cout << " image=" << runtime_state.image_path;
    }
    if (!runtime_state.loop_device.empty()) {
      std::cout << " loop_device=" << runtime_state.loop_device;
    }
    const auto telemetry_it = telemetry_by_key.find(key);
    if (telemetry_it != telemetry_by_key.end()) {
      std::cout << " usage_bytes=" << telemetry_it->second.used_bytes
                << "/" << telemetry_it->second.total_bytes
                << " free_bytes=" << telemetry_it->second.free_bytes
                << " read_bytes=" << telemetry_it->second.read_bytes
                << " write_bytes=" << telemetry_it->second.write_bytes
                << " read_ios=" << telemetry_it->second.read_ios
                << " write_ios=" << telemetry_it->second.write_ios
                << " io_time_ms=" << telemetry_it->second.io_time_ms
                << " fault_count=" << telemetry_it->second.fault_count
                << " warning_count=" << telemetry_it->second.warning_count
                << " perf_counters=" << (telemetry_it->second.perf_counters_available ? "yes" : "no")
                << " io_error_counters="
                << (telemetry_it->second.io_error_counters_available ? "yes" : "no")
                << " mount_health="
                << (telemetry_it->second.health.empty() ? "(empty)" : telemetry_it->second.health);
    }
    std::cout << "\n";
    if (!runtime_state.status_message.empty()) {
      std::cout << "    message=" << runtime_state.status_message << "\n";
    }
  }

  if (!printed) {
    std::cout << "  (empty)\n";
  }
}

void PrintSchedulerDecisionSummary(const comet::DesiredState& state) {
  bool has_decisions = false;
  for (const auto& instance : state.instances) {
    if (instance.role != comet::InstanceRole::Worker) {
      continue;
    }
    if (instance.labels.find("comet.placement.decision") == instance.labels.end()) {
      continue;
    }
    if (!has_decisions) {
      std::cout << "scheduler-decisions:\n";
      has_decisions = true;
    }

    std::cout << "  - worker=" << instance.name;
    const auto decision_it = instance.labels.find("comet.placement.decision");
    if (decision_it != instance.labels.end()) {
      std::cout << " decision=" << decision_it->second;
    }
    const auto next_action_it = instance.labels.find("comet.placement.next_action");
    if (next_action_it != instance.labels.end()) {
      std::cout << " next_action=" << next_action_it->second;
    }
    const auto next_target_it = instance.labels.find("comet.placement.next_target");
    if (next_target_it != instance.labels.end()) {
      std::cout << " next_target=" << next_target_it->second;
    }
    const auto victims_it = instance.labels.find("comet.preemption.victims");
    if (victims_it != instance.labels.end()) {
      std::cout << " victims=" << victims_it->second;
    }
    const auto defer_reason_it = instance.labels.find("comet.placement.defer_reason");
    if (defer_reason_it != instance.labels.end()) {
      std::cout << " defer_reason=" << defer_reason_it->second;
    }
    std::cout << "\n";
  }
}

std::map<std::string, std::vector<comet::SchedulerRolloutAction>> BuildRolloutActionsByTargetNode(
    const comet::SchedulingPolicyReport& scheduling_report) {
  std::map<std::string, std::vector<comet::SchedulerRolloutAction>> result;
  for (const auto& action : scheduling_report.rollout_actions) {
    result[action.target_node_name].push_back(action);
  }
  return result;
}

void PrintRolloutGateSummary(const comet::SchedulingPolicyReport& scheduling_report) {
  if (scheduling_report.rollout_actions.empty()) {
    return;
  }

  std::set<std::string> worker_names;
  std::set<std::string> node_names;
  for (const auto& action : scheduling_report.rollout_actions) {
    if (!action.worker_name.empty()) {
      worker_names.insert(action.worker_name);
    }
    if (!action.target_node_name.empty()) {
      node_names.insert(action.target_node_name);
    }
  }

  std::cout << "rollout-gates:\n";
  std::cout << "  gated_workers=" << worker_names.size()
            << " gated_nodes=" << node_names.size()
            << " deferred_actions=" << scheduling_report.rollout_actions.size() << "\n";
}

void PrintPersistedRolloutActions(
    const std::vector<comet::RolloutActionRecord>& actions) {
  std::cout << "rollout-actions:\n";
  if (actions.empty()) {
    std::cout << "  (empty)\n";
    return;
  }

  for (const auto& action : actions) {
    std::cout << "  - id=" << action.id
              << " generation=" << action.desired_generation
              << " step=" << action.step
              << " worker=" << action.worker_name
              << " action=" << action.action
              << " target=" << action.target_node_name << ":" << action.target_gpu_device
              << " status=" << comet::ToString(action.status);
    if (!action.victim_worker_names.empty()) {
      std::cout << " victims=";
      for (std::size_t index = 0; index < action.victim_worker_names.size(); ++index) {
        if (index > 0) {
          std::cout << ",";
        }
        std::cout << action.victim_worker_names[index];
      }
    }
    if (!action.reason.empty()) {
      std::cout << " reason=" << action.reason;
    }
    std::cout << "\n";
    if (!action.status_message.empty()) {
      std::cout << "    message=" << action.status_message << "\n";
    }
  }
}

std::optional<comet::RolloutActionRecord> FindRolloutActionById(
    const std::vector<comet::RolloutActionRecord>& actions,
    int action_id) {
  for (const auto& action : actions) {
    if (action.id == action_id) {
      return action;
    }
  }
  return std::nullopt;
}

void RemoveWorkerFromDesiredState(
    comet::DesiredState* state,
    const std::string& worker_name) {
  if (state == nullptr) {
    return;
  }

  state->instances.erase(
      std::remove_if(
          state->instances.begin(),
          state->instances.end(),
          [&](const comet::InstanceSpec& instance) { return instance.name == worker_name; }),
      state->instances.end());
  state->runtime_gpu_nodes.erase(
      std::remove_if(
          state->runtime_gpu_nodes.begin(),
          state->runtime_gpu_nodes.end(),
          [&](const comet::RuntimeGpuNode& gpu_node) { return gpu_node.name == worker_name; }),
      state->runtime_gpu_nodes.end());
  state->disks.erase(
      std::remove_if(
          state->disks.begin(),
          state->disks.end(),
          [&](const comet::DiskSpec& disk) {
            return disk.kind == comet::DiskKind::WorkerPrivate &&
                   disk.owner_name == worker_name;
          }),
      state->disks.end());
  for (auto& instance : state->instances) {
    instance.depends_on.erase(
        std::remove(instance.depends_on.begin(), instance.depends_on.end(), worker_name),
        instance.depends_on.end());
  }
}

void MaterializeRetryPlacementAction(
    comet::DesiredState* state,
    const comet::RolloutActionRecord& action,
    const std::vector<std::string>& victim_worker_names) {
  if (state == nullptr) {
    return;
  }

  for (const auto& victim_worker_name : victim_worker_names) {
    RemoveWorkerFromDesiredState(state, victim_worker_name);
  }

  auto instance_it = std::find_if(
      state->instances.begin(),
      state->instances.end(),
      [&](const comet::InstanceSpec& instance) {
        return instance.role == comet::InstanceRole::Worker &&
               instance.name == action.worker_name;
      });
  if (instance_it == state->instances.end()) {
    throw std::runtime_error(
        "worker '" + action.worker_name + "' not found in desired state");
  }

  instance_it->node_name = action.target_node_name;
  instance_it->gpu_device = action.target_gpu_device;
  instance_it->share_mode = comet::GpuShareMode::Exclusive;
  instance_it->gpu_fraction = 1.0;
  instance_it->labels["comet.node"] = action.target_node_name;
  instance_it->labels["comet.placement"] = "auto";
  instance_it->labels["comet.placement.action"] = "materialized-retry-placement";
  instance_it->labels["comet.placement.decision"] = "applied";
  instance_it->labels.erase("comet.placement.next_action");
  instance_it->labels.erase("comet.placement.next_target");
  instance_it->labels.erase("comet.placement.defer_reason");
  instance_it->labels.erase("comet.preemption.victims");

  auto runtime_gpu_it = std::find_if(
      state->runtime_gpu_nodes.begin(),
      state->runtime_gpu_nodes.end(),
      [&](const comet::RuntimeGpuNode& gpu_node) {
        return gpu_node.name == action.worker_name;
      });
  if (runtime_gpu_it != state->runtime_gpu_nodes.end()) {
    runtime_gpu_it->node_name = action.target_node_name;
    runtime_gpu_it->gpu_device = action.target_gpu_device;
    runtime_gpu_it->share_mode = comet::GpuShareMode::Exclusive;
    runtime_gpu_it->gpu_fraction = 1.0;
  }

  auto disk_it = std::find_if(
      state->disks.begin(),
      state->disks.end(),
      [&](const comet::DiskSpec& disk) {
        return disk.kind == comet::DiskKind::WorkerPrivate &&
               disk.owner_name == action.worker_name;
      });
  if (disk_it != state->disks.end()) {
    disk_it->node_name = action.target_node_name;
  }
}

std::string RolloutActionTag(int action_id) {
  return "rollout_action_id=" + std::to_string(action_id);
}

bool AssignmentReferencesRolloutAction(
    const comet::HostAssignment& assignment,
    int action_id) {
  return assignment.status_message.find(RolloutActionTag(action_id)) != std::string::npos;
}

std::vector<comet::HostAssignment> BuildEvictionAssignmentsForAction(
    const comet::DesiredState& desired_state,
    int desired_generation,
    const comet::RolloutActionRecord& action,
    const std::vector<comet::HostAssignment>& existing_assignments) {
  if (action.action != "evict-best-effort") {
    throw std::runtime_error(
        "rollout action id=" + std::to_string(action.id) +
        " is not an evict-best-effort action");
  }
  if (action.victim_worker_names.empty()) {
    throw std::runtime_error(
        "rollout action id=" + std::to_string(action.id) +
        " has no victim workers to evict");
  }

  std::map<std::string, std::vector<std::string>> victim_workers_by_node;
  for (const auto& victim_worker_name : action.victim_worker_names) {
    bool found = false;
    for (const auto& instance : desired_state.instances) {
      if (instance.role == comet::InstanceRole::Worker &&
          instance.name == victim_worker_name) {
        victim_workers_by_node[instance.node_name].push_back(victim_worker_name);
        found = true;
        break;
      }
    }
    if (!found) {
      throw std::runtime_error(
          "victim worker '" + victim_worker_name +
          "' not found in desired state for rollout action id=" +
          std::to_string(action.id));
    }
  }

  comet::DesiredState eviction_state = desired_state;
  int required_memory_cap_mb = 0;
  for (const auto& instance : desired_state.instances) {
    if (instance.role == comet::InstanceRole::Worker &&
        instance.name == action.worker_name) {
      required_memory_cap_mb = instance.memory_cap_mb.value_or(0);
      break;
    }
  }
  for (const auto& victim_worker_name : action.victim_worker_names) {
    RemoveWorkerFromDesiredState(&eviction_state, victim_worker_name);
  }

  const auto plane_assignment =
      FindLatestHostAssignmentForPlane(existing_assignments, desired_state.plane_name);
  std::vector<comet::HostAssignment> assignments;
  for (const auto& [node_name, victim_workers] : victim_workers_by_node) {
    comet::HostAssignment assignment;
    assignment.node_name = node_name;
    assignment.plane_name = desired_state.plane_name;
    assignment.desired_generation = desired_generation;
    assignment.assignment_type = "evict-workers";
    assignment.desired_state_json =
        comet::SerializeDesiredStateJson(
            comet::SliceDesiredStateForNode(eviction_state, node_name));
    const auto latest_assignment =
        FindLatestHostAssignmentForNode(existing_assignments, node_name);
    assignment.artifacts_root = latest_assignment.has_value()
                                    ? latest_assignment->artifacts_root
                                    : (plane_assignment.has_value()
                                           ? plane_assignment->artifacts_root
                                           : DefaultArtifactsRoot());
    assignment.status = comet::HostAssignmentStatus::Pending;
    std::ostringstream message;
    message << RolloutActionTag(action.id)
            << " evict workers for rollout worker=" << action.worker_name
            << " target_gpu=" << action.target_gpu_device
            << " required_memory_cap_mb=" << required_memory_cap_mb
            << " victims=";
    for (std::size_t index = 0; index < victim_workers.size(); ++index) {
      if (index > 0) {
        message << ",";
      }
      message << victim_workers[index];
    }
    assignment.status_message = message.str();
    assignments.push_back(std::move(assignment));
  }
  return assignments;
}

std::optional<comet::RolloutActionRecord> FindPriorRolloutActionForWorker(
    const std::vector<comet::RolloutActionRecord>& actions,
    const comet::RolloutActionRecord& action,
    const std::string& requested_action_name) {
  std::optional<comet::RolloutActionRecord> result;
  for (const auto& candidate_action : actions) {
    if (candidate_action.desired_generation != action.desired_generation ||
        candidate_action.worker_name != action.worker_name ||
        candidate_action.step >= action.step ||
        candidate_action.action != requested_action_name) {
      continue;
    }
    result = candidate_action;
  }
  return result;
}

bool AreRolloutEvictionAssignmentsApplied(
    const std::vector<comet::HostAssignment>& assignments,
    int action_id) {
  bool found = false;
  for (const auto& assignment : assignments) {
    if (assignment.assignment_type != "evict-workers" ||
        !AssignmentReferencesRolloutAction(assignment, action_id)) {
      continue;
    }
    found = true;
    if (assignment.status != comet::HostAssignmentStatus::Applied) {
      return false;
    }
  }
  return found;
}

enum class SchedulerRolloutPhase {
  Planned,
  EvictionEnqueued,
  EvictionApplied,
  RetryReady,
  RetryMaterialized,
  HostFailed,
  HostStale,
  RuntimeFailed,
  RolloutApplied,
};

struct RolloutLifecycleEntry {
  std::string worker_name;
  int desired_generation = 0;
  SchedulerRolloutPhase phase = SchedulerRolloutPhase::Planned;
  std::optional<int> action_id;
  std::string target_node_name;
  std::string target_gpu_device;
  std::vector<std::string> victim_worker_names;
  std::string detail;
};

struct RebalancePlanEntry {
  std::string worker_name;
  comet::PlacementMode placement_mode = comet::PlacementMode::Manual;
  std::string current_node_name;
  std::string current_gpu_device;
  std::string target_node_name;
  std::string target_gpu_device;
  std::string rebalance_class;
  std::string decision;
  std::string state;
  std::string action;
  int score = 0;
  bool preemption_required = false;
  std::vector<std::string> victim_worker_names;
  std::string gate_reason;
};

struct RebalancePolicySummary {
  int actionable_count = 0;
  int safe_direct_count = 0;
  int rollout_class_count = 0;
  int gated_count = 0;
  int blocked_active_rollout_count = 0;
  int assignment_busy_count = 0;
  int observation_gated_count = 0;
  int stable_hold_count = 0;
  int below_threshold_count = 0;
  int propose_count = 0;
  int hold_count = 0;
  int defer_count = 0;
  int no_candidate_count = 0;
  std::vector<std::string> actionable_workers;
  std::vector<std::string> safe_direct_workers;
  std::vector<std::string> rollout_class_workers;
  std::vector<std::string> gated_workers;
  std::vector<std::string> blocked_active_rollout_workers;
  std::vector<std::string> assignment_busy_workers;
  std::vector<std::string> observation_gated_workers;
  std::vector<std::string> stable_hold_workers;
  std::vector<std::string> below_threshold_workers;
  std::vector<std::string> proposed_workers;
  std::vector<std::string> held_workers;
  std::vector<std::string> deferred_workers;
  std::vector<std::string> no_candidate_workers;
};

struct RebalanceControllerGateSummary {
  bool cluster_ready = true;
  int active_rollout_count = 0;
  int blocking_assignment_count = 0;
  int unconverged_node_count = 0;
  std::vector<std::string> active_rollout_workers;
  std::vector<std::string> blocking_assignment_nodes;
  std::vector<std::string> unconverged_nodes;
};

struct RebalanceIterationBudgetSummary {
  int current_iteration = 0;
  int max_iterations = 0;
  bool exhausted = false;
};

struct RebalanceLoopStatusSummary {
  std::string state;
  std::string reason;
};

struct SchedulerRuntimeView {
  std::optional<comet::SchedulerPlaneRuntime> plane_runtime;
  std::map<std::string, comet::SchedulerWorkerRuntime> worker_runtime_by_name;
  std::map<std::string, comet::SchedulerNodeRuntime> node_runtime_by_name;
};

void MaterializeRebalancePlanEntry(
    comet::DesiredState* state,
    const RebalancePlanEntry& entry) {
  if (state == nullptr) {
    return;
  }

  auto instance_it = std::find_if(
      state->instances.begin(),
      state->instances.end(),
      [&](const comet::InstanceSpec& instance) {
        return instance.role == comet::InstanceRole::Worker &&
               instance.name == entry.worker_name;
      });
  if (instance_it == state->instances.end()) {
    throw std::runtime_error(
        "worker '" + entry.worker_name + "' not found in desired state");
  }

  instance_it->node_name = entry.target_node_name;
  instance_it->gpu_device = entry.target_gpu_device;
  instance_it->environment["COMET_NODE_NAME"] = entry.target_node_name;
  if (!entry.target_gpu_device.empty()) {
    instance_it->environment["COMET_GPU_DEVICE"] = entry.target_gpu_device;
  } else {
    instance_it->environment.erase("COMET_GPU_DEVICE");
  }
  if (entry.action == "upgrade-to-exclusive") {
    instance_it->share_mode = comet::GpuShareMode::Exclusive;
    instance_it->gpu_fraction = 1.0;
  }
  instance_it->labels["comet.node"] = entry.target_node_name;
  instance_it->labels["comet.placement"] = "auto";
  instance_it->labels["comet.placement.action"] = "materialized-rebalance-" + entry.action;
  instance_it->labels["comet.placement.score"] = std::to_string(entry.score);
  instance_it->labels["comet.placement.decision"] = "applied";
  instance_it->labels.erase("comet.placement.next_action");
  instance_it->labels.erase("comet.placement.next_target");
  instance_it->labels.erase("comet.placement.defer_reason");
  instance_it->labels.erase("comet.preemption.victims");

  auto runtime_gpu_it = std::find_if(
      state->runtime_gpu_nodes.begin(),
      state->runtime_gpu_nodes.end(),
      [&](const comet::RuntimeGpuNode& gpu_node) {
        return gpu_node.name == entry.worker_name;
      });
  if (runtime_gpu_it != state->runtime_gpu_nodes.end()) {
    runtime_gpu_it->node_name = entry.target_node_name;
    runtime_gpu_it->gpu_device = entry.target_gpu_device;
    runtime_gpu_it->share_mode = instance_it->share_mode;
    runtime_gpu_it->gpu_fraction = instance_it->gpu_fraction;
  }

  auto disk_it = std::find_if(
      state->disks.begin(),
      state->disks.end(),
      [&](const comet::DiskSpec& disk) {
        return disk.kind == comet::DiskKind::WorkerPrivate &&
               disk.owner_name == entry.worker_name;
      });
  if (disk_it != state->disks.end()) {
    disk_it->node_name = entry.target_node_name;
  }
}

std::string ToString(SchedulerRolloutPhase phase) {
  switch (phase) {
    case SchedulerRolloutPhase::Planned:
      return "planned";
    case SchedulerRolloutPhase::EvictionEnqueued:
      return "eviction-enqueued";
    case SchedulerRolloutPhase::EvictionApplied:
      return "eviction-applied";
    case SchedulerRolloutPhase::RetryReady:
      return "retry-ready";
    case SchedulerRolloutPhase::RetryMaterialized:
      return "retry-materialized";
    case SchedulerRolloutPhase::HostFailed:
      return "host-failed";
    case SchedulerRolloutPhase::HostStale:
      return "host-stale";
    case SchedulerRolloutPhase::RuntimeFailed:
      return "runtime-failed";
    case SchedulerRolloutPhase::RolloutApplied:
      return "rollout-applied";
  }
  return "unknown";
}

bool HasRolloutEvictionAssignments(
    const std::vector<comet::HostAssignment>& assignments,
    int action_id) {
  for (const auto& assignment : assignments) {
    if (assignment.assignment_type == "evict-workers" &&
        AssignmentReferencesRolloutAction(assignment, action_id)) {
      return true;
    }
  }
  return false;
}

std::optional<comet::HostAssignment> FindLatestHostAssignmentForNodeGeneration(
    const std::vector<comet::HostAssignment>& assignments,
    const std::string& node_name,
    int desired_generation) {
  std::optional<comet::HostAssignment> result;
  for (const auto& assignment : assignments) {
    if (assignment.node_name != node_name ||
        assignment.desired_generation != desired_generation) {
      continue;
    }
    result = assignment;
  }
  return result;
}

std::optional<comet::HostObservation> FindHostObservationForNode(
    const std::vector<comet::HostObservation>& observations,
    const std::string& node_name) {
  for (const auto& observation : observations) {
    if (observation.node_name == node_name) {
      return observation;
    }
  }
  return std::nullopt;
}

std::map<std::string, comet::HostAssignment> BuildLatestPlaneAssignmentsByNode(
    const std::vector<comet::HostAssignment>& assignments) {
  std::map<std::string, comet::HostAssignment> latest_by_node;
  for (const auto& assignment : assignments) {
    auto it = latest_by_node.find(assignment.node_name);
    if (it == latest_by_node.end() || assignment.id >= it->second.id) {
      latest_by_node[assignment.node_name] = assignment;
    }
  }
  return latest_by_node;
}

int ComputeEffectivePlaneAppliedGeneration(
    const comet::PlaneRecord& plane,
    const std::optional<comet::DesiredState>& desired_state,
    const std::optional<int>& desired_generation,
    const std::vector<comet::HostObservation>& observations) {
  if (!desired_state.has_value() || !desired_generation.has_value()) {
    return plane.applied_generation;
  }
  if (*desired_generation <= plane.applied_generation) {
    return plane.applied_generation;
  }
  for (const auto& node : desired_state->nodes) {
    const auto observation = FindHostObservationForNode(observations, node.name);
    if (!observation.has_value()) {
      return plane.applied_generation;
    }
    if (!observation->applied_generation.has_value() ||
        *observation->applied_generation < *desired_generation ||
        observation->status == comet::HostObservationStatus::Failed) {
      return plane.applied_generation;
    }
  }
  return *desired_generation;
}

std::vector<RolloutLifecycleEntry> BuildRolloutLifecycleEntries(
    const comet::DesiredState& desired_state,
    int desired_generation,
    const std::vector<comet::RolloutActionRecord>& rollout_actions,
    const std::vector<comet::HostAssignment>& assignments,
    const std::vector<comet::HostObservation>& observations) {
  std::map<std::string, std::vector<comet::RolloutActionRecord>> actions_by_worker;
  for (const auto& action : rollout_actions) {
    if (action.desired_generation == desired_generation) {
      actions_by_worker[action.worker_name].push_back(action);
    }
  }

  std::vector<RolloutLifecycleEntry> entries;
  for (auto& [worker_name, actions] : actions_by_worker) {
    std::sort(
        actions.begin(),
        actions.end(),
        [](const comet::RolloutActionRecord& left, const comet::RolloutActionRecord& right) {
          if (left.step != right.step) {
            return left.step < right.step;
          }
          return left.id < right.id;
        });

    const comet::RolloutActionRecord* evict_action = nullptr;
    const comet::RolloutActionRecord* retry_action = nullptr;
    for (const auto& action : actions) {
      if (action.action == "evict-best-effort" && evict_action == nullptr) {
        evict_action = &action;
      } else if (action.action == "retry-placement" && retry_action == nullptr) {
        retry_action = &action;
      }
    }
    if (evict_action == nullptr && retry_action == nullptr) {
      continue;
    }

    RolloutLifecycleEntry entry;
    entry.worker_name = worker_name;
    entry.desired_generation = desired_generation;
    const auto* target_action = retry_action != nullptr ? retry_action : evict_action;
    entry.target_node_name = target_action->target_node_name;
    entry.target_gpu_device = target_action->target_gpu_device;
    if (evict_action != nullptr) {
      entry.victim_worker_names = evict_action->victim_worker_names;
    }

    if (evict_action != nullptr) {
      entry.action_id = evict_action->id;
      if (evict_action->status == comet::RolloutActionStatus::Pending) {
        entry.phase = SchedulerRolloutPhase::Planned;
        entry.detail = "awaiting eviction enqueue";
      } else if (evict_action->status == comet::RolloutActionStatus::Acknowledged) {
        if (AreRolloutEvictionAssignmentsApplied(assignments, evict_action->id)) {
          entry.phase = SchedulerRolloutPhase::EvictionApplied;
          entry.detail = "eviction assignments applied";
        } else if (HasRolloutEvictionAssignments(assignments, evict_action->id)) {
          entry.phase = SchedulerRolloutPhase::EvictionEnqueued;
          entry.detail = "eviction assignments enqueued";
        } else {
          entry.phase = SchedulerRolloutPhase::EvictionEnqueued;
          entry.detail = evict_action->status_message.empty()
                             ? "eviction acknowledged"
                             : evict_action->status_message;
        }
      } else if (evict_action->status == comet::RolloutActionStatus::ReadyToRetry) {
        entry.phase = SchedulerRolloutPhase::EvictionApplied;
        entry.detail = "eviction completed";
      }
    }

    if (retry_action != nullptr &&
        retry_action->status == comet::RolloutActionStatus::ReadyToRetry) {
      entry.phase = SchedulerRolloutPhase::RetryReady;
      entry.action_id = retry_action->id;
      entry.detail = "retry placement can be materialized";
    }

    entries.push_back(std::move(entry));
  }

  for (const auto& instance : desired_state.instances) {
    if (instance.role != comet::InstanceRole::Worker) {
      continue;
    }
    const auto placement_action_it = instance.labels.find("comet.placement.action");
    const auto placement_decision_it = instance.labels.find("comet.placement.decision");
    if (placement_action_it == instance.labels.end() ||
        placement_decision_it == instance.labels.end() ||
        placement_action_it->second != "materialized-retry-placement" ||
        placement_decision_it->second != "applied") {
      continue;
    }
    if (actions_by_worker.find(instance.name) != actions_by_worker.end()) {
      continue;
    }

    RolloutLifecycleEntry entry;
    entry.worker_name = instance.name;
    entry.desired_generation = desired_generation;
    entry.phase = SchedulerRolloutPhase::RetryMaterialized;
    entry.target_node_name = instance.node_name;
    entry.target_gpu_device = instance.gpu_device.value_or("");

    const auto target_assignment =
        FindLatestHostAssignmentForNodeGeneration(
            assignments,
            instance.node_name,
            desired_generation);
    const auto target_observation =
        FindHostObservationForNode(observations, instance.node_name);
    if (target_observation.has_value() &&
        target_observation->status == comet::HostObservationStatus::Failed) {
      entry.phase = SchedulerRolloutPhase::HostFailed;
      entry.detail = "target node observation failed";
    } else if (target_observation.has_value() &&
               HealthFromAge(
                   HeartbeatAgeSeconds(target_observation->heartbeat_at),
                   DefaultStaleAfterSeconds()) == "stale") {
      entry.phase = SchedulerRolloutPhase::HostStale;
      entry.detail = "target node observation stale";
    } else if (target_observation.has_value() &&
               ParseRuntimeStatus(*target_observation).has_value() &&
               ParseRuntimeStatus(*target_observation)->runtime_phase == "failed") {
      entry.phase = SchedulerRolloutPhase::RuntimeFailed;
      entry.detail = "target runtime reported failed phase";
    } else if (target_observation.has_value() &&
               target_observation->status == comet::HostObservationStatus::Applied &&
               target_observation->applied_generation.has_value() &&
               *target_observation->applied_generation >= desired_generation) {
      entry.phase = SchedulerRolloutPhase::RolloutApplied;
      entry.detail = "target node observed desired generation applied";
    } else if (target_assignment.has_value()) {
      entry.detail =
          "target node assignment status=" + comet::ToString(target_assignment->status);
    } else {
      entry.detail = "materialized in desired state";
    }

    entries.push_back(std::move(entry));
  }

  std::sort(
      entries.begin(),
      entries.end(),
      [](const RolloutLifecycleEntry& left, const RolloutLifecycleEntry& right) {
        return left.worker_name < right.worker_name;
      });
  return entries;
}

void PrintRolloutLifecycleEntries(const std::vector<RolloutLifecycleEntry>& entries) {
  std::cout << "rollout-lifecycle:\n";
  if (entries.empty()) {
    std::cout << "  (empty)\n";
    return;
  }

  for (const auto& entry : entries) {
    std::cout << "  - worker=" << entry.worker_name
              << " generation=" << entry.desired_generation
              << " phase=" << ToString(entry.phase);
    if (entry.action_id.has_value()) {
      std::cout << " action_id=" << *entry.action_id;
    }
    if (!entry.target_node_name.empty() || !entry.target_gpu_device.empty()) {
      std::cout << " target=" << entry.target_node_name << ":" << entry.target_gpu_device;
    }
    if (!entry.victim_worker_names.empty()) {
      std::cout << " victims=";
      for (std::size_t index = 0; index < entry.victim_worker_names.size(); ++index) {
        if (index > 0) {
          std::cout << ",";
        }
        std::cout << entry.victim_worker_names[index];
      }
    }
    if (!entry.detail.empty()) {
      std::cout << " detail=" << entry.detail;
    }
    std::cout << "\n";
  }
}

std::optional<RolloutLifecycleEntry> FindRolloutLifecycleEntry(
    const std::vector<RolloutLifecycleEntry>& entries,
    const std::string& worker_name) {
  for (const auto& entry : entries) {
    if (entry.worker_name == worker_name) {
      return entry;
    }
  }
  return std::nullopt;
}

bool RolloutPhaseBlocksRebalance(SchedulerRolloutPhase phase) {
  return phase != SchedulerRolloutPhase::RolloutApplied;
}

bool HostAssignmentBlocksRebalance(const comet::HostAssignment& assignment) {
  return assignment.status == comet::HostAssignmentStatus::Pending ||
         assignment.status == comet::HostAssignmentStatus::Claimed;
}

bool NodeHasBlockingHostAssignment(
    const std::vector<comet::HostAssignment>& assignments,
    const std::string& node_name) {
  for (const auto& assignment : assignments) {
    if (assignment.node_name == node_name &&
        HostAssignmentBlocksRebalance(assignment)) {
      return true;
    }
  }
  return false;
}

RebalanceControllerGateSummary BuildRebalanceControllerGateSummary(
    const comet::DesiredState& desired_state,
    int desired_generation,
    const std::vector<comet::NodeAvailabilityOverride>& availability_overrides,
    const std::vector<RolloutLifecycleEntry>& rollout_lifecycle_entries,
    const std::vector<comet::HostAssignment>& assignments,
    const SchedulerRuntimeView& scheduler_runtime,
    const std::vector<comet::HostObservation>& observations,
    int stale_after_seconds) {
  RebalanceControllerGateSummary summary;
  std::set<std::string> active_rollout_workers;
  for (const auto& entry : rollout_lifecycle_entries) {
    if (RolloutPhaseBlocksRebalance(entry.phase)) {
      active_rollout_workers.insert(entry.worker_name);
    }
  }
  if (scheduler_runtime.plane_runtime.has_value() &&
      !scheduler_runtime.plane_runtime->active_action.empty() &&
      !scheduler_runtime.plane_runtime->active_worker_name.empty()) {
    active_rollout_workers.insert(scheduler_runtime.plane_runtime->active_worker_name);
  }

  std::set<std::string> blocking_assignment_nodes;
  for (const auto& assignment : assignments) {
    if (HostAssignmentBlocksRebalance(assignment)) {
      blocking_assignment_nodes.insert(assignment.node_name);
    }
  }

  summary.active_rollout_workers.assign(
      active_rollout_workers.begin(), active_rollout_workers.end());
  summary.blocking_assignment_nodes.assign(
      blocking_assignment_nodes.begin(), blocking_assignment_nodes.end());
  summary.active_rollout_count =
      static_cast<int>(summary.active_rollout_workers.size());
  summary.blocking_assignment_count =
      static_cast<int>(summary.blocking_assignment_nodes.size());

  const auto availability_override_map =
      BuildAvailabilityOverrideMap(availability_overrides);
  std::set<std::string> unconverged_nodes;
  for (const auto& node : desired_state.nodes) {
    if (!IsNodeSchedulable(
            ResolveNodeAvailability(availability_override_map, node.name))) {
      continue;
    }
    const auto observation = FindHostObservationForNode(observations, node.name);
    if (!observation.has_value()) {
      unconverged_nodes.insert(node.name);
      continue;
    }
    if (observation->status == comet::HostObservationStatus::Failed) {
      unconverged_nodes.insert(node.name);
      continue;
    }
    const auto age_seconds = HeartbeatAgeSeconds(observation->heartbeat_at);
    if (HealthFromAge(age_seconds, stale_after_seconds) != "online") {
      unconverged_nodes.insert(node.name);
      continue;
    }
    if (!observation->applied_generation.has_value() ||
        *observation->applied_generation != desired_generation) {
      unconverged_nodes.insert(node.name);
      continue;
    }
  }

  summary.unconverged_nodes.assign(
      unconverged_nodes.begin(), unconverged_nodes.end());
  summary.unconverged_node_count =
      static_cast<int>(summary.unconverged_nodes.size());
  summary.cluster_ready =
      summary.active_rollout_count == 0 &&
      summary.blocking_assignment_count == 0 &&
      summary.unconverged_node_count == 0;
  return summary;
}

const comet::InstanceSpec* FindWorkerInstance(
    const comet::DesiredState& state,
    const std::string& worker_name) {
  for (const auto& instance : state.instances) {
    if (instance.role == comet::InstanceRole::Worker && instance.name == worker_name) {
      return &instance;
    }
  }
  return nullptr;
}

constexpr int ComputePressureUtilizationThresholdPct() {
  return 85;
}

constexpr int ObservedMoveVramReserveMb() {
  return 1024;
}

std::optional<comet::GpuDeviceTelemetry> FindObservedGpuDeviceTelemetry(
    const std::vector<comet::HostObservation>& observations,
    const std::string& node_name,
    const std::string& gpu_device) {
  const auto observation = FindHostObservationForNode(observations, node_name);
  if (!observation.has_value()) {
    return std::nullopt;
  }
  const auto telemetry = ParseGpuTelemetry(*observation);
  if (!telemetry.has_value()) {
    return std::nullopt;
  }
  for (const auto& device : telemetry->devices) {
    if (device.gpu_device == gpu_device) {
      return device;
    }
  }
  return std::nullopt;
}

bool ObservedGpuDeviceHasForeignProcess(
    const std::vector<comet::HostObservation>& observations,
    const std::string& node_name,
    const std::string& gpu_device,
    const std::string& worker_name) {
  const auto device = FindObservedGpuDeviceTelemetry(observations, node_name, gpu_device);
  if (!device.has_value()) {
    return false;
  }
  for (const auto& process : device->processes) {
    if (process.instance_name != worker_name && process.instance_name != "unknown") {
      return true;
    }
  }
  return false;
}

std::optional<std::string> ObservedGpuPlacementGateReason(
    const std::vector<comet::HostObservation>& observations,
    const comet::InstanceSpec& worker,
    const std::string& target_node_name,
    const std::string& target_gpu_device,
    bool moving_to_different_gpu) {
  const auto device = FindObservedGpuDeviceTelemetry(observations, target_node_name, target_gpu_device);
  if (!device.has_value()) {
    return std::nullopt;
  }

  if (worker.memory_cap_mb.has_value() &&
      device->free_vram_mb < (*worker.memory_cap_mb + ObservedMoveVramReserveMb())) {
    return std::string("observed-insufficient-vram");
  }

  if (moving_to_different_gpu &&
      device->gpu_utilization_pct >= ComputePressureUtilizationThresholdPct() &&
      ObservedGpuDeviceHasForeignProcess(observations, target_node_name, target_gpu_device, worker.name)) {
    return std::string("compute-pressure");
  }

  return std::nullopt;
}

std::vector<RebalancePlanEntry> BuildRebalancePlanEntries(
    const comet::DesiredState& state,
    const comet::SchedulingPolicyReport& scheduling_report,
    const std::vector<comet::NodeAvailabilityOverride>& availability_overrides,
    const std::vector<RolloutLifecycleEntry>& rollout_lifecycle_entries,
    const std::vector<comet::HostAssignment>& assignments,
    const SchedulerRuntimeView& scheduler_runtime,
    const std::vector<comet::HostObservation>& observations,
    int stale_after_seconds,
    const std::optional<std::string>& node_name_filter = std::nullopt) {
  std::vector<RebalancePlanEntry> entries;
  for (const auto& recommendation : scheduling_report.placement_recommendations) {
    const auto* worker = FindWorkerInstance(state, recommendation.worker_name);
    if (worker == nullptr) {
      continue;
    }
    if (worker->placement_mode == comet::PlacementMode::Manual) {
      continue;
    }
    if (node_name_filter.has_value() && worker->node_name != *node_name_filter) {
      bool candidate_matches = false;
      for (const auto& candidate : recommendation.candidates) {
        if (candidate.node_name == *node_name_filter) {
          candidate_matches = true;
          break;
        }
      }
      if (!candidate_matches) {
        continue;
      }
    }

    RebalancePlanEntry entry;
    entry.worker_name = recommendation.worker_name;
    entry.placement_mode = worker->placement_mode;
    entry.current_node_name = recommendation.current_node_name;
    entry.current_gpu_device = recommendation.current_gpu_device;
    const auto availability_override_map =
        BuildAvailabilityOverrideMap(availability_overrides);
    const auto source_availability =
        ResolveNodeAvailability(availability_override_map, recommendation.current_node_name);
    const bool source_requires_exit = source_availability != comet::NodeAvailability::Active;

    const auto worker_runtime_it =
        scheduler_runtime.worker_runtime_by_name.find(recommendation.worker_name);
    if (worker_runtime_it != scheduler_runtime.worker_runtime_by_name.end() &&
        worker_runtime_it->second.manual_intervention_required) {
      entry.rebalance_class = "gated";
      entry.decision = "hold";
      entry.state = "manual-intervention-required";
      entry.gate_reason = "manual-intervention-required";
      entries.push_back(std::move(entry));
      continue;
    }

    if (scheduler_runtime.plane_runtime.has_value() &&
        !scheduler_runtime.plane_runtime->active_action.empty() &&
        scheduler_runtime.plane_runtime->active_worker_name == recommendation.worker_name) {
      entry.rebalance_class = "gated";
      entry.decision = "hold";
      entry.state = scheduler_runtime.plane_runtime->phase.empty()
                        ? "active-scheduler-action"
                        : scheduler_runtime.plane_runtime->phase;
      entry.target_node_name = scheduler_runtime.plane_runtime->target_node_name;
      entry.target_gpu_device = scheduler_runtime.plane_runtime->target_gpu_device;
      entry.gate_reason = scheduler_runtime.plane_runtime->active_action;
      entries.push_back(std::move(entry));
      continue;
    }

    if (scheduler_runtime.plane_runtime.has_value() &&
        !scheduler_runtime.plane_runtime->active_action.empty() &&
        scheduler_runtime.plane_runtime->active_worker_name != recommendation.worker_name) {
      entry.rebalance_class = "gated";
      entry.decision = "hold";
      entry.state = "active-scheduler-action";
      entry.gate_reason = scheduler_runtime.plane_runtime->active_action;
      entries.push_back(std::move(entry));
      continue;
    }

    const auto rollout_lifecycle =
        FindRolloutLifecycleEntry(rollout_lifecycle_entries, recommendation.worker_name);
    if (rollout_lifecycle.has_value() &&
        RolloutPhaseBlocksRebalance(rollout_lifecycle->phase)) {
      entry.rebalance_class = "rollout-class";
      entry.decision = "hold";
      entry.state = "active-rollout";
      entry.target_node_name = rollout_lifecycle->target_node_name;
      entry.target_gpu_device = rollout_lifecycle->target_gpu_device;
      entry.gate_reason = ToString(rollout_lifecycle->phase);
      entries.push_back(std::move(entry));
      continue;
    }

    const comet::PlacementCandidate* selected_candidate = nullptr;
    if (source_requires_exit) {
      for (const auto& candidate : recommendation.candidates) {
        if (candidate.action == "insufficient-memory" ||
            candidate.action == "insufficient-fraction") {
          continue;
        }
        const auto target_availability =
            ResolveNodeAvailability(availability_override_map, candidate.node_name);
        if (candidate.node_name != recommendation.current_node_name &&
            IsNodeSchedulable(target_availability)) {
          selected_candidate = &candidate;
          break;
        }
      }
    }
    if (selected_candidate == nullptr) {
      for (const auto& candidate : recommendation.candidates) {
        if (candidate.action == "insufficient-memory" ||
            candidate.action == "insufficient-fraction") {
          continue;
        }
        selected_candidate = &candidate;
        break;
      }
    }
    if (selected_candidate == nullptr && !recommendation.candidates.empty()) {
      selected_candidate = &recommendation.candidates.front();
    }
    if (selected_candidate == nullptr) {
      entry.rebalance_class = source_requires_exit ? "gated" : "no-candidate";
      entry.decision = "hold";
      entry.state = source_requires_exit ? "draining-source" : "no-candidate";
      entry.gate_reason =
          source_requires_exit ? "no-active-drain-target" : std::string{};
      entries.push_back(std::move(entry));
      continue;
    }

    entry.target_node_name = selected_candidate->node_name;
    entry.target_gpu_device = selected_candidate->gpu_device;
    entry.action = selected_candidate->action;
    entry.score = selected_candidate->score;
    entry.preemption_required = selected_candidate->preemption_required;
    entry.victim_worker_names = selected_candidate->preemption_victims;
    const auto target_availability =
        ResolveNodeAvailability(availability_override_map, selected_candidate->node_name);

    if (worker_runtime_it != scheduler_runtime.worker_runtime_by_name.end()) {
      const auto last_move_age = TimestampAgeSeconds(worker_runtime_it->second.last_move_at);
      if (last_move_age.has_value() &&
          *last_move_age < WorkerMinimumResidencySeconds()) {
        entry.rebalance_class = "stable";
        entry.decision = "hold";
        entry.state = "min-residency";
        entry.gate_reason =
            "min-residency(" + std::to_string(*last_move_age) + "<" +
            std::to_string(WorkerMinimumResidencySeconds()) + ")";
        entries.push_back(std::move(entry));
        continue;
      }
    }

    auto source_node_runtime_it =
        scheduler_runtime.node_runtime_by_name.find(recommendation.current_node_name);
    auto target_node_runtime_it =
        scheduler_runtime.node_runtime_by_name.find(selected_candidate->node_name);
    const auto source_move_age =
        source_node_runtime_it == scheduler_runtime.node_runtime_by_name.end()
            ? std::optional<long long>{}
            : TimestampAgeSeconds(source_node_runtime_it->second.last_move_at);
    const auto target_move_age =
        target_node_runtime_it == scheduler_runtime.node_runtime_by_name.end()
            ? std::optional<long long>{}
            : TimestampAgeSeconds(target_node_runtime_it->second.last_move_at);
    if ((source_move_age.has_value() && *source_move_age < NodeCooldownAfterMoveSeconds()) ||
        (target_move_age.has_value() && *target_move_age < NodeCooldownAfterMoveSeconds())) {
      entry.rebalance_class = "gated";
      entry.decision = "hold";
      entry.state = "cooldown";
      if (source_move_age.has_value() && target_move_age.has_value() &&
          *source_move_age < NodeCooldownAfterMoveSeconds() &&
          *target_move_age < NodeCooldownAfterMoveSeconds()) {
        entry.gate_reason = "cooldown-source-and-target";
      } else if (source_move_age.has_value() && *source_move_age < NodeCooldownAfterMoveSeconds()) {
        entry.gate_reason = "cooldown-source";
      } else {
        entry.gate_reason = "cooldown-target";
      }
      entries.push_back(std::move(entry));
      continue;
    }

    if (source_requires_exit &&
        selected_candidate->node_name == recommendation.current_node_name) {
      entry.rebalance_class = "gated";
      entry.decision = "hold";
      entry.state = "draining-source";
      entry.gate_reason = "no-active-drain-target";
      entries.push_back(std::move(entry));
      continue;
    }

    if (!IsNodeSchedulable(target_availability)) {
      entry.rebalance_class = "gated";
      entry.decision = "hold";
      entry.state = "gated-target";
      entry.gate_reason =
          target_availability == comet::NodeAvailability::Draining
              ? "draining-target"
              : "unavailable-target";
      entries.push_back(std::move(entry));
      continue;
    }

    const bool source_assignment_busy =
        NodeHasBlockingHostAssignment(assignments, recommendation.current_node_name);
    const bool target_assignment_busy =
        NodeHasBlockingHostAssignment(assignments, selected_candidate->node_name);
    if (source_assignment_busy || target_assignment_busy) {
      entry.rebalance_class = "gated";
      entry.decision = "hold";
      entry.state = "assignment-in-flight";
      if (source_assignment_busy && target_assignment_busy) {
        entry.gate_reason = "source-and-target-node-busy";
      } else if (source_assignment_busy) {
        entry.gate_reason = "source-node-busy";
      } else {
        entry.gate_reason = "target-node-busy";
      }
      entries.push_back(std::move(entry));
      continue;
    }

    const auto gate_reason =
        ObservedSchedulingGateReason(
            observations, selected_candidate->node_name, stale_after_seconds);
    if (gate_reason.has_value()) {
      entry.rebalance_class = "gated";
      entry.decision = "hold";
      entry.state = "gated-target";
      entry.gate_reason = *gate_reason;
    } else if (const auto gpu_gate_reason =
                   ObservedGpuPlacementGateReason(
                       observations,
                       *worker,
                       selected_candidate->node_name,
                       selected_candidate->gpu_device,
                       selected_candidate->node_name != recommendation.current_node_name ||
                           selected_candidate->gpu_device != recommendation.current_gpu_device);
               gpu_gate_reason.has_value()) {
      entry.rebalance_class = "gated";
      entry.decision = "hold";
      entry.state = "gated-target";
      entry.gate_reason = *gpu_gate_reason;
    } else if (selected_candidate->preemption_required) {
      entry.rebalance_class = "rollout-class";
      entry.decision = "defer";
      entry.state = source_requires_exit ? "drain-preemption" : "deferred-preemption";
    } else if (selected_candidate->score < MinimumSafeDirectRebalanceScore()) {
      entry.rebalance_class = "stable";
      entry.decision = "hold";
      entry.state = "below-threshold";
      entry.gate_reason =
          "score-below-threshold(" + std::to_string(selected_candidate->score) + "<" +
          std::to_string(MinimumSafeDirectRebalanceScore()) + ")";
    } else if (selected_candidate->same_node &&
               selected_candidate->action == "upgrade-to-exclusive") {
      entry.rebalance_class = "safe-direct";
      entry.decision = "propose";
      entry.state = "ready-in-place-upgrade";
    } else if (selected_candidate->same_node) {
      entry.rebalance_class = "stable";
      entry.decision = "hold";
      entry.state = "stay";
    } else {
      entry.rebalance_class = "safe-direct";
      entry.decision = "propose";
      entry.state = source_requires_exit ? "ready-drain-move" : "ready-move";
    }

    entries.push_back(std::move(entry));
  }

  std::sort(
      entries.begin(),
      entries.end(),
      [](const RebalancePlanEntry& left, const RebalancePlanEntry& right) {
        if (left.worker_name != right.worker_name) {
          return left.worker_name < right.worker_name;
        }
        return left.target_node_name < right.target_node_name;
      });
  return entries;
}

void PrintRebalancePlanEntries(const std::vector<RebalancePlanEntry>& entries) {
  std::cout << "rebalance-plan:\n";
  if (entries.empty()) {
    std::cout << "  (empty)\n";
    return;
  }
  for (const auto& entry : entries) {
    std::cout << "  - worker=" << entry.worker_name
              << " placement_mode=" << comet::ToString(entry.placement_mode)
              << " current=" << entry.current_node_name << ":" << entry.current_gpu_device
              << " class=" << (entry.rebalance_class.empty() ? "(empty)" : entry.rebalance_class)
              << " decision=" << entry.decision
              << " state=" << entry.state;
    if (!entry.target_node_name.empty() || !entry.target_gpu_device.empty()) {
      std::cout << " target=" << entry.target_node_name << ":" << entry.target_gpu_device;
    }
    if (!entry.action.empty()) {
      std::cout << " action=" << entry.action;
    }
    std::cout << " score=" << entry.score
              << " preemption_required=" << (entry.preemption_required ? "yes" : "no");
    if (!entry.victim_worker_names.empty()) {
      std::cout << " victims=";
      for (std::size_t index = 0; index < entry.victim_worker_names.size(); ++index) {
        if (index > 0) {
          std::cout << ",";
        }
        std::cout << entry.victim_worker_names[index];
      }
    }
    if (!entry.gate_reason.empty()) {
      std::cout << " gate_reason=" << entry.gate_reason;
    }
    std::cout << "\n";
  }
}

RebalancePolicySummary BuildRebalancePolicySummary(
    const std::vector<RebalancePlanEntry>& entries) {
  RebalancePolicySummary summary;
  for (const auto& entry : entries) {
    if (entry.state == "no-candidate") {
      ++summary.gated_count;
      ++summary.no_candidate_count;
      summary.gated_workers.push_back(entry.worker_name);
      summary.no_candidate_workers.push_back(entry.worker_name);
      continue;
    }
    if (entry.decision == "propose") {
      ++summary.actionable_count;
      ++summary.safe_direct_count;
      ++summary.propose_count;
      summary.actionable_workers.push_back(entry.worker_name);
      summary.safe_direct_workers.push_back(entry.worker_name);
      summary.proposed_workers.push_back(entry.worker_name);
      continue;
    }
    if (entry.decision == "defer") {
      ++summary.rollout_class_count;
      ++summary.defer_count;
      summary.rollout_class_workers.push_back(entry.worker_name);
      summary.deferred_workers.push_back(entry.worker_name);
      continue;
    }
    if (entry.state == "active-rollout") {
      ++summary.rollout_class_count;
      ++summary.blocked_active_rollout_count;
      summary.rollout_class_workers.push_back(entry.worker_name);
      summary.blocked_active_rollout_workers.push_back(entry.worker_name);
    } else if (entry.state == "assignment-in-flight" ||
               entry.state == "gated-target" ||
               entry.state == "draining-source" ||
               entry.state == "manual-intervention-required" ||
               entry.state == "active-scheduler-action" ||
               entry.state == "cooldown" ||
               entry.state == "min-residency") {
      ++summary.gated_count;
      summary.gated_workers.push_back(entry.worker_name);
      if (entry.state == "assignment-in-flight") {
        ++summary.assignment_busy_count;
        summary.assignment_busy_workers.push_back(entry.worker_name);
      } else if (entry.state == "gated-target") {
        ++summary.observation_gated_count;
        summary.observation_gated_workers.push_back(entry.worker_name);
      }
    } else {
      if (entry.state == "below-threshold") {
        ++summary.below_threshold_count;
        summary.below_threshold_workers.push_back(entry.worker_name);
      }
      ++summary.stable_hold_count;
      summary.stable_hold_workers.push_back(entry.worker_name);
    }
    ++summary.hold_count;
    summary.held_workers.push_back(entry.worker_name);
  }
  return summary;
}

void PrintWorkerListLine(
    const std::string& label,
    const std::vector<std::string>& workers) {
  if (workers.empty()) {
    return;
  }
  std::cout << "  " << label << "=";
  for (std::size_t index = 0; index < workers.size(); ++index) {
    if (index > 0) {
      std::cout << ",";
    }
    std::cout << workers[index];
  }
  std::cout << "\n";
}

void PrintRebalancePolicySummary(const RebalancePolicySummary& summary) {
  std::cout << "rebalance-policy:\n";
  std::cout << "  actionable=" << summary.actionable_count
            << " safe_direct=" << summary.safe_direct_count
            << " rollout_class=" << summary.rollout_class_count
            << " gated=" << summary.gated_count
            << " blocked_active_rollouts=" << summary.blocked_active_rollout_count
            << " assignment_busy=" << summary.assignment_busy_count
            << " observation_gated=" << summary.observation_gated_count
            << " stable_holds=" << summary.stable_hold_count
            << " below_threshold=" << summary.below_threshold_count
            << " deferred=" << summary.defer_count
            << " no_candidate=" << summary.no_candidate_count << "\n";
  std::cout << "  propose=" << summary.propose_count
            << " hold=" << summary.hold_count
            << " defer=" << summary.defer_count
            << " no_candidate=" << summary.no_candidate_count << "\n";
  PrintWorkerListLine("actionable_workers", summary.actionable_workers);
  PrintWorkerListLine("safe_direct_workers", summary.safe_direct_workers);
  PrintWorkerListLine("rollout_class_workers", summary.rollout_class_workers);
  PrintWorkerListLine("gated_workers", summary.gated_workers);
  PrintWorkerListLine(
      "blocked_active_rollout_workers", summary.blocked_active_rollout_workers);
  PrintWorkerListLine("assignment_busy_workers", summary.assignment_busy_workers);
  PrintWorkerListLine("observation_gated_workers", summary.observation_gated_workers);
  PrintWorkerListLine("stable_hold_workers", summary.stable_hold_workers);
  PrintWorkerListLine("below_threshold_workers", summary.below_threshold_workers);
  PrintWorkerListLine("proposed_workers", summary.proposed_workers);
  PrintWorkerListLine("held_workers", summary.held_workers);
  PrintWorkerListLine("deferred_workers", summary.deferred_workers);
  PrintWorkerListLine("no_candidate_workers", summary.no_candidate_workers);
}

void PrintRebalanceControllerGateSummary(
    const RebalanceControllerGateSummary& summary) {
  std::cout << "rebalance-controller-gate:\n";
  std::cout << "  cluster_ready=" << (summary.cluster_ready ? "yes" : "no")
            << " active_rollouts=" << summary.active_rollout_count
            << " blocking_assignment_nodes=" << summary.blocking_assignment_count
            << " unconverged_nodes=" << summary.unconverged_node_count << "\n";
  PrintWorkerListLine("active_rollout_workers", summary.active_rollout_workers);
  PrintWorkerListLine("blocking_assignment_nodes", summary.blocking_assignment_nodes);
  PrintWorkerListLine("unconverged_nodes", summary.unconverged_nodes);
}

RebalanceIterationBudgetSummary BuildRebalanceIterationBudgetSummary(int current_iteration) {
  RebalanceIterationBudgetSummary summary;
  summary.current_iteration = current_iteration;
  summary.max_iterations = MaximumRebalanceIterationsPerGeneration();
  summary.exhausted = summary.current_iteration >= summary.max_iterations;
  return summary;
}

void PrintRebalanceIterationBudgetSummary(
    const RebalanceIterationBudgetSummary& summary) {
  std::cout << "rebalance-iteration-budget:\n";
  std::cout << "  iteration=" << summary.current_iteration << "/" << summary.max_iterations
            << " exhausted=" << (summary.exhausted ? "yes" : "no") << "\n";
}

RebalanceLoopStatusSummary BuildRebalanceLoopStatusSummary(
    const RebalanceControllerGateSummary& controller_gate_summary,
    const RebalanceIterationBudgetSummary& iteration_budget_summary,
    const RebalancePolicySummary& policy_summary) {
  RebalanceLoopStatusSummary summary;
  if (!controller_gate_summary.cluster_ready) {
    summary.state = "waiting-for-convergence";
    if (controller_gate_summary.unconverged_node_count > 0) {
      summary.reason =
          "unconverged-nodes=" + std::to_string(controller_gate_summary.unconverged_node_count);
    } else if (controller_gate_summary.blocking_assignment_count > 0) {
      summary.reason =
          "blocking-assignments=" + std::to_string(controller_gate_summary.blocking_assignment_count);
    } else {
      summary.reason =
          "active-rollouts=" + std::to_string(controller_gate_summary.active_rollout_count);
    }
    return summary;
  }
  if (iteration_budget_summary.exhausted && policy_summary.actionable_count > 0) {
    summary.state = "complete";
    summary.reason =
        "rebalance-iteration-limit=" + std::to_string(iteration_budget_summary.current_iteration) +
        "/" + std::to_string(iteration_budget_summary.max_iterations);
    return summary;
  }
  if (policy_summary.rollout_class_count > 0) {
    summary.state = "waiting-for-rollout";
    summary.reason = "rollout-class-workers=" + std::to_string(policy_summary.rollout_class_count);
    return summary;
  }
  if (policy_summary.actionable_count > 0) {
    summary.state = "actionable";
    summary.reason = "safe-direct-workers=" + std::to_string(policy_summary.actionable_count);
    return summary;
  }
  summary.state = "complete";
  if (policy_summary.below_threshold_count > 0) {
    summary.reason =
        "remaining-moves-below-threshold=" + std::to_string(policy_summary.below_threshold_count);
  } else if (policy_summary.no_candidate_count > 0) {
    summary.reason =
        "no-candidate-workers=" + std::to_string(policy_summary.no_candidate_count);
  } else {
    summary.reason = "no-actionable-rebalance";
  }
  return summary;
}

void PrintRebalanceLoopStatusSummary(const RebalanceLoopStatusSummary& summary) {
  std::cout << "rebalance-loop-status:\n";
  std::cout << "  state=" << summary.state;
  if (!summary.reason.empty()) {
    std::cout << " reason=" << summary.reason;
  }
  std::cout << "\n";
}

struct RolloutActionsViewData {
  std::string db_path;
  std::optional<std::string> plane_name;
  std::optional<std::string> node_name;
  std::optional<comet::DesiredState> desired_state;
  std::optional<int> desired_generation;
  std::vector<comet::RolloutActionRecord> actions;
  std::optional<SchedulerRuntimeView> scheduler_runtime;
  std::vector<RolloutLifecycleEntry> lifecycle;
  std::size_t gated_worker_count = 0;
  std::size_t gated_node_count = 0;
};

struct RebalancePlanViewData {
  std::string db_path;
  std::optional<std::string> plane_name;
  std::optional<std::string> node_name;
  int stale_after_seconds = 0;
  std::optional<comet::DesiredState> desired_state;
  int desired_generation = 0;
  std::vector<RebalancePlanEntry> rebalance_entries;
  RebalanceControllerGateSummary controller_gate_summary;
  RebalanceIterationBudgetSummary iteration_budget_summary;
  RebalancePolicySummary policy_summary;
  RebalanceLoopStatusSummary loop_status;
  SchedulerRuntimeView scheduler_runtime;
};

struct StateAggregateViewData {
  std::string db_path;
  int stale_after_seconds = 0;
  std::vector<comet::PlaneRecord> planes;
  std::vector<comet::DesiredState> desired_states;
  std::optional<comet::DesiredState> desired_state;
  std::optional<int> desired_generation;
  std::vector<comet::DiskRuntimeState> disk_runtime_states;
  comet::SchedulingPolicyReport scheduling_report;
  std::vector<comet::HostObservation> observations;
  std::vector<comet::HostAssignment> assignments;
  std::vector<comet::NodeAvailabilityOverride> availability_overrides;
  SchedulerRuntimeView scheduler_runtime;
  std::vector<RolloutLifecycleEntry> rollout_lifecycle;
  std::vector<RebalancePlanEntry> rebalance_entries;
  RebalanceControllerGateSummary controller_gate_summary;
  RebalanceIterationBudgetSummary iteration_budget_summary;
  RebalancePolicySummary rebalance_policy_summary;
  RebalanceLoopStatusSummary loop_status;
};

RolloutActionsViewData LoadRolloutActionsViewData(
    const std::string& db_path,
    const std::optional<std::string>& node_name,
    const std::optional<std::string>& plane_name) {
  comet::ControllerStore store(db_path);
  store.Initialize();

  RolloutActionsViewData view;
  view.db_path = db_path;
  view.plane_name = plane_name;
  view.node_name = node_name;
  view.desired_state =
      plane_name.has_value() ? store.LoadDesiredState(*plane_name)
                             : store.LoadDesiredState();
  view.desired_generation =
      plane_name.has_value() ? store.LoadDesiredGeneration(*plane_name)
                             : store.LoadDesiredGeneration();
  view.actions =
      view.desired_state.has_value()
          ? store.LoadRolloutActions(view.desired_state->plane_name, node_name)
          : store.LoadRolloutActions(plane_name, node_name);

  std::set<std::string> worker_names;
  std::set<std::string> node_names;
  for (const auto& action : view.actions) {
    worker_names.insert(action.worker_name);
    node_names.insert(action.target_node_name);
  }
  view.gated_worker_count = worker_names.size();
  view.gated_node_count = node_names.size();

  if (view.desired_state.has_value()) {
    view.scheduler_runtime = LoadSchedulerRuntimeView(store, view.desired_state);
    if (view.desired_generation.has_value()) {
      const auto plane_assignments =
          store.LoadHostAssignments(std::nullopt, std::nullopt, view.desired_state->plane_name);
      const auto plane_observations =
          FilterHostObservationsForPlane(
              store.LoadHostObservations(),
              view.desired_state->plane_name);
      view.lifecycle = BuildRolloutLifecycleEntries(
          *view.desired_state,
          *view.desired_generation,
          view.actions,
          plane_assignments,
          plane_observations);
    }
  }
  return view;
}

RebalancePlanViewData LoadRebalancePlanViewData(
    const std::string& db_path,
    const std::optional<std::string>& node_name,
    int stale_after_seconds,
    const std::optional<std::string>& plane_name) {
  comet::ControllerStore store(db_path);
  store.Initialize();

  RebalancePlanViewData view;
  view.db_path = db_path;
  view.plane_name = plane_name;
  view.node_name = node_name;
  view.stale_after_seconds = stale_after_seconds;
  view.desired_state =
      plane_name.has_value() ? store.LoadDesiredState(*plane_name)
                             : store.LoadDesiredState();
  if (!view.desired_state.has_value()) {
    return view;
  }

  view.desired_generation =
      (plane_name.has_value() ? store.LoadDesiredGeneration(*plane_name)
                              : store.LoadDesiredGeneration())
          .value_or(0);
  const auto observations =
      FilterHostObservationsForPlane(
          store.LoadHostObservations(),
          view.desired_state->plane_name);
  const auto assignments =
      store.LoadHostAssignments(std::nullopt, std::nullopt, view.desired_state->plane_name);
  const auto availability_overrides = store.LoadNodeAvailabilityOverrides();
  const auto scheduling_report = comet::EvaluateSchedulingPolicy(*view.desired_state);
  view.scheduler_runtime = LoadSchedulerRuntimeView(store, view.desired_state);
  const auto rollout_actions = store.LoadRolloutActions(view.desired_state->plane_name);
  const auto rollout_lifecycle = BuildRolloutLifecycleEntries(
      *view.desired_state,
      view.desired_generation,
      rollout_actions,
      assignments,
      observations);
  view.rebalance_entries = BuildRebalancePlanEntries(
      *view.desired_state,
      scheduling_report,
      availability_overrides,
      rollout_lifecycle,
      assignments,
      view.scheduler_runtime,
      observations,
      stale_after_seconds,
      node_name);
  view.controller_gate_summary = BuildRebalanceControllerGateSummary(
      *view.desired_state,
      view.desired_generation,
      availability_overrides,
      rollout_lifecycle,
      assignments,
      view.scheduler_runtime,
      observations,
      stale_after_seconds);
  view.iteration_budget_summary =
      BuildRebalanceIterationBudgetSummary(store.LoadRebalanceIteration().value_or(0));
  view.policy_summary = BuildRebalancePolicySummary(view.rebalance_entries);
  view.loop_status = BuildRebalanceLoopStatusSummary(
      view.controller_gate_summary,
      view.iteration_budget_summary,
      view.policy_summary);
  return view;
}

StateAggregateViewData LoadStateAggregateViewData(
    const std::string& db_path,
    int stale_after_seconds,
    const std::optional<std::string>& plane_name = std::nullopt) {
  comet::ControllerStore store(db_path);
  store.Initialize();

  StateAggregateViewData view;
  view.db_path = db_path;
  view.stale_after_seconds = stale_after_seconds;
  view.planes = store.LoadPlanes();
  view.desired_states = store.LoadDesiredStates();
  view.desired_state =
      plane_name.has_value() ? store.LoadDesiredState(*plane_name) : store.LoadDesiredState();
  view.desired_generation =
      plane_name.has_value() ? store.LoadDesiredGeneration(*plane_name)
                             : store.LoadDesiredGeneration();
  if (!view.desired_state.has_value()) {
    return view;
  }

  view.disk_runtime_states = store.LoadDiskRuntimeStates(view.desired_state->plane_name);
  view.scheduling_report = comet::EvaluateSchedulingPolicy(*view.desired_state);
  view.observations =
      plane_name.has_value()
          ? FilterHostObservationsForPlane(store.LoadHostObservations(), *plane_name)
                             : store.LoadHostObservations();
  view.assignments =
      plane_name.has_value()
          ? store.LoadHostAssignments(std::nullopt, std::nullopt, *plane_name)
          : store.LoadHostAssignments();
  view.availability_overrides = store.LoadNodeAvailabilityOverrides();
  view.scheduler_runtime = LoadSchedulerRuntimeView(store, view.desired_state);
  const auto plane_rollout_actions = store.LoadRolloutActions(view.desired_state->plane_name);
  view.rollout_lifecycle =
      view.desired_generation.has_value()
          ? BuildRolloutLifecycleEntries(
                *view.desired_state,
                *view.desired_generation,
                plane_rollout_actions,
                view.assignments,
                view.observations)
          : std::vector<RolloutLifecycleEntry>{};
  view.rebalance_entries =
      BuildRebalancePlanEntries(
          *view.desired_state,
          view.scheduling_report,
          view.availability_overrides,
          view.rollout_lifecycle,
          view.assignments,
          view.scheduler_runtime,
          view.observations,
          stale_after_seconds);
  view.controller_gate_summary =
      BuildRebalanceControllerGateSummary(
          *view.desired_state,
          view.desired_generation.value_or(0),
          view.availability_overrides,
          view.rollout_lifecycle,
          view.assignments,
          view.scheduler_runtime,
          view.observations,
          stale_after_seconds);
  view.iteration_budget_summary =
      BuildRebalanceIterationBudgetSummary(store.LoadRebalanceIteration().value_or(0));
  view.rebalance_policy_summary =
      BuildRebalancePolicySummary(view.rebalance_entries);
  view.loop_status =
      BuildRebalanceLoopStatusSummary(
          view.controller_gate_summary,
          view.iteration_budget_summary,
          view.rebalance_policy_summary);
  return view;
}

json BuildDiskStatePayload(
    const std::string& db_path,
    const std::optional<std::string>& node_name,
    const std::optional<std::string>& plane_name) {
  const auto view = LoadDiskStateViewData(db_path, node_name, plane_name);
  comet::ControllerStore store(db_path);
  store.Initialize();
  const auto state = view.desired_state;

  json payload{
      {"service", "comet-controller"},
      {"db_path", db_path},
      {"plane_name", plane_name.has_value() ? json(*plane_name) : json(nullptr)},
      {"node_name", node_name.has_value() ? json(*node_name) : json(nullptr)},
  };
  if (!state.has_value()) {
    payload["desired_state"] = nullptr;
    payload["items"] = json::array();
    return payload;
  }

  const auto runtime_states = view.runtime_states;
  const auto observations = view.observations;
  payload["plane_name"] = state->plane_name;
  payload["desired_generation"] =
      view.desired_generation.has_value() ? json(*view.desired_generation) : json(nullptr);

  std::map<std::string, comet::DiskRuntimeState> runtime_by_key;
  for (const auto& runtime_state : runtime_states) {
    runtime_by_key.emplace(runtime_state.disk_name + "@" + runtime_state.node_name, runtime_state);
  }
  std::map<std::string, comet::DiskTelemetryRecord> telemetry_by_key;
  for (const auto& observation : observations) {
    const auto disk_telemetry = ParseDiskTelemetry(observation);
    if (!disk_telemetry.has_value()) {
      continue;
    }
    for (const auto& item : disk_telemetry->items) {
      telemetry_by_key[item.disk_name + "@" + item.node_name] = item;
    }
  }

  json items = json::array();
  for (const auto& disk : state->disks) {
    if (node_name.has_value() && disk.node_name != *node_name) {
      continue;
    }
    json item{
        {"disk_name", disk.name},
        {"kind", comet::ToString(disk.kind)},
        {"plane_name", disk.plane_name},
        {"owner_name", disk.owner_name},
        {"node_name", disk.node_name},
        {"size_gb", disk.size_gb},
        {"desired_host_path", disk.host_path},
        {"desired_container_path", disk.container_path},
    };
    const std::string key = disk.name + "@" + disk.node_name;
    const auto runtime_it = runtime_by_key.find(key);
    if (runtime_it == runtime_by_key.end()) {
      item["realized_state"] = "missing-runtime-state";
    } else {
      const auto& runtime_state = runtime_it->second;
      item["realized_state"] =
          runtime_state.runtime_state.empty() ? json("(empty)") : json(runtime_state.runtime_state);
      item["mount_point"] = runtime_state.mount_point.empty() ? json(nullptr) : json(runtime_state.mount_point);
      item["filesystem_type"] =
          runtime_state.filesystem_type.empty() ? json(nullptr) : json(runtime_state.filesystem_type);
      item["image_path"] = runtime_state.image_path.empty() ? json(nullptr) : json(runtime_state.image_path);
      item["loop_device"] = runtime_state.loop_device.empty() ? json(nullptr) : json(runtime_state.loop_device);
      item["last_verified_at"] =
          runtime_state.last_verified_at.empty() ? json(nullptr) : json(runtime_state.last_verified_at);
      item["status_message"] =
          runtime_state.status_message.empty() ? json(nullptr) : json(runtime_state.status_message);
    }
    const auto telemetry_it = telemetry_by_key.find(key);
    if (telemetry_it != telemetry_by_key.end()) {
      const auto& telemetry = telemetry_it->second;
      item["usage_bytes"] = {
          {"total_bytes", telemetry.total_bytes},
          {"used_bytes", telemetry.used_bytes},
          {"free_bytes", telemetry.free_bytes},
      };
      item["io_bytes"] = {
          {"read_bytes", telemetry.read_bytes},
          {"write_bytes", telemetry.write_bytes},
      };
      item["io_ops"] = {
          {"read_ios", telemetry.read_ios},
          {"write_ios", telemetry.write_ios},
      };
      item["io_time_ms"] = telemetry.io_time_ms;
      item["weighted_io_time_ms"] = telemetry.weighted_io_time_ms;
      item["io_error_count"] = telemetry.io_error_count;
      item["io_in_progress"] = telemetry.io_in_progress;
      item["warning_count"] = telemetry.warning_count;
      item["fault_count"] = telemetry.fault_count;
      item["read_only"] = telemetry.read_only;
      item["perf_counters_available"] = telemetry.perf_counters_available;
      item["io_error_counters_available"] = telemetry.io_error_counters_available;
      item["mount_source"] =
          telemetry.mount_source.empty() ? json(nullptr) : json(telemetry.mount_source);
      item["fault_reasons"] = telemetry.fault_reasons;
      item["mount_health"] = telemetry.health.empty() ? json(nullptr) : json(telemetry.health);
      item["mounted"] = telemetry.mounted;
      item["telemetry_status_message"] =
          telemetry.status_message.empty() ? json(nullptr) : json(telemetry.status_message);
    }
    items.push_back(std::move(item));
  }

  for (const auto& runtime_state : runtime_states) {
    if (node_name.has_value() && runtime_state.node_name != *node_name) {
      continue;
    }
    const std::string key = runtime_state.disk_name + "@" + runtime_state.node_name;
    bool found_in_desired = false;
    for (const auto& disk : state->disks) {
      if (disk.name + "@" + disk.node_name == key) {
        found_in_desired = true;
        break;
      }
    }
    if (found_in_desired) {
      continue;
    }
    items.push_back(json{
        {"disk_name", runtime_state.disk_name},
        {"plane_name", runtime_state.plane_name},
        {"node_name", runtime_state.node_name},
        {"realized_state",
         runtime_state.runtime_state.empty() ? json("(empty)") : json(runtime_state.runtime_state)},
        {"desired_state", "orphan-runtime-state"},
        {"mount_point", runtime_state.mount_point.empty() ? json(nullptr) : json(runtime_state.mount_point)},
        {"image_path", runtime_state.image_path.empty() ? json(nullptr) : json(runtime_state.image_path)},
        {"loop_device", runtime_state.loop_device.empty() ? json(nullptr) : json(runtime_state.loop_device)},
        {"status_message",
         runtime_state.status_message.empty() ? json(nullptr) : json(runtime_state.status_message)},
    });
  }

  payload["desired_state"] = "present";
  payload["items"] = std::move(items);
  return payload;
}

json BuildDashboardPayload(
    const std::string& db_path,
    int stale_after_seconds,
    const std::optional<std::string>& plane_name) {
  const auto view = LoadStateAggregateViewData(db_path, stale_after_seconds, plane_name);
  comet::ControllerStore store(db_path);
  store.Initialize();
  const auto recent_events =
      store.LoadEvents(plane_name, std::nullopt, std::nullopt, std::nullopt, 10);
  const auto rollout_actions =
      plane_name.has_value() ? store.LoadRolloutActions(*plane_name)
                             : store.LoadRolloutActions();

  json payload{
      {"service", "comet-controller"},
      {"db_path", db_path},
      {"stale_after_seconds", stale_after_seconds},
      {"plane_name", plane_name.has_value() ? json(*plane_name) : json(nullptr)},
      {"desired_generation", view.desired_generation.has_value() ? json(*view.desired_generation) : json(nullptr)},
  };
  if (!view.desired_state.has_value()) {
    payload["plane"] = nullptr;
    payload["planes"] = json::array();
    payload["nodes"] = json::array();
    payload["runtime"] = {
        {"observed_nodes", 0},
        {"ready_nodes", 0},
        {"not_ready_nodes", 0},
        {"degraded_gpu_telemetry_nodes", 0},
    };
    payload["assignments"] = {
        {"total", 0},
        {"pending", 0},
        {"claimed", 0},
        {"applied", 0},
        {"failed", 0},
        {"by_node", json::array()},
    };
    payload["rollout"] = {
        {"total_actions", 0},
        {"pending", 0},
        {"acknowledged", 0},
        {"ready_to_retry", 0},
        {"workers", json::array()},
    };
    payload["alerts"] = {
        {"critical", 0},
        {"warning", 0},
        {"booting", 0},
        {"total", 0},
        {"items", json::array()},
    };
    payload["recent_events"] = json::array();
    return payload;
  }

  std::map<std::string, comet::HostObservation> observation_by_node;
  for (const auto& observation : view.observations) {
    observation_by_node.emplace(observation.node_name, observation);
  }
  int effective_plane_applied_generation = 0;
  std::optional<comet::PlaneRecord> dashboard_plane_record;
  for (const auto& plane : view.planes) {
    if (plane.name != view.desired_state->plane_name) {
      continue;
    }
    dashboard_plane_record = plane;
    effective_plane_applied_generation = ComputeEffectivePlaneAppliedGeneration(
        plane,
        view.desired_state,
        view.desired_generation,
        view.observations);
    if (effective_plane_applied_generation > plane.applied_generation) {
      store.UpdatePlaneAppliedGeneration(plane.name, effective_plane_applied_generation);
    }
    break;
  }
  const auto availability_override_map =
      BuildAvailabilityOverrideMap(view.availability_overrides);

  payload["plane"] = {
      {"plane_name", view.desired_state->plane_name},
      {"plane_mode", comet::ToString(view.desired_state->plane_mode)},
      {"state",
       [&]() -> json {
         return dashboard_plane_record.has_value() ? json(dashboard_plane_record->state)
                                                   : json(nullptr);
       }()},
      {"desired_generation", view.desired_generation.has_value() ? json(*view.desired_generation) : json(nullptr)},
      {"applied_generation", dashboard_plane_record.has_value() ? json(effective_plane_applied_generation)
                                                                 : json(nullptr)},
      {"staged_update",
       dashboard_plane_record.has_value()
           ? dashboard_plane_record->generation > effective_plane_applied_generation
           : false},
      {"node_count", view.desired_state->nodes.size()},
      {"instance_count", view.desired_state->instances.size()},
      {"disk_count", view.desired_state->disks.size()},
      {"shared_disk_name", view.desired_state->plane_shared_disk_name},
      {"control_root", view.desired_state->control_root},
      {"bootstrap_model", BuildBootstrapModelPayloadItem(view.desired_state->bootstrap_model)},
  };
  json plane_items = json::array();
  for (const auto& plane : view.planes) {
    const auto desired_state_it =
        std::find_if(
            view.desired_states.begin(),
            view.desired_states.end(),
            [&](const comet::DesiredState& candidate) {
              return candidate.plane_name == plane.name;
            });
    const int plane_applied_generation =
        plane.name == view.desired_state->plane_name ? effective_plane_applied_generation
                                                     : plane.applied_generation;
    plane_items.push_back(json{
        {"plane_name", plane.name},
        {"plane_mode",
         desired_state_it != view.desired_states.end()
             ? json(comet::ToString(desired_state_it->plane_mode))
             : json(plane.plane_mode)},
        {"state", plane.state},
        {"generation", plane.generation},
        {"applied_generation", plane_applied_generation},
        {"staged_update", plane.generation > plane_applied_generation},
        {"rebalance_iteration", plane.rebalance_iteration},
        {"node_count",
         desired_state_it != view.desired_states.end() ? json(desired_state_it->nodes.size())
                                                       : json(nullptr)},
        {"instance_count",
         desired_state_it != view.desired_states.end() ? json(desired_state_it->instances.size())
                                                       : json(nullptr)},
        {"disk_count",
         desired_state_it != view.desired_states.end() ? json(desired_state_it->disks.size())
                                                       : json(nullptr)},
    });
  }
  payload["planes"] = std::move(plane_items);

  json nodes = json::array();
  int observed_nodes = 0;
  int ready_nodes = 0;
  int not_ready_nodes = 0;
  int degraded_gpu_nodes = 0;
  const std::optional<std::string> selected_plane_state =
      plane_name.has_value()
          ? [&]() -> std::optional<std::string> {
              for (const auto& plane : view.planes) {
                if (plane.name == *plane_name) {
                  return plane.state;
                }
              }
              return std::nullopt;
            }()
          : std::nullopt;
  std::map<std::string, comet::NodeInventory> dashboard_nodes;
  if (plane_name.has_value()) {
    for (const auto& node : view.desired_state->nodes) {
      dashboard_nodes.emplace(node.name, node);
    }
  } else {
    for (const auto& state : view.desired_states) {
      for (const auto& node : state.nodes) {
        dashboard_nodes.emplace(node.name, node);
      }
    }
  }
  for (const auto& [dashboard_node_name, node] : dashboard_nodes) {
    (void)dashboard_node_name;
    int desired_instance_count = 0;
    int desired_disk_count = 0;
    int desired_plane_count = 0;
    std::set<std::string> node_planes;
    if (plane_name.has_value()) {
      for (const auto& instance : view.desired_state->instances) {
        if (instance.node_name == node.name) {
          ++desired_instance_count;
          node_planes.insert(view.desired_state->plane_name);
        }
      }
      for (const auto& disk : view.desired_state->disks) {
        if (disk.node_name == node.name) {
          ++desired_disk_count;
          node_planes.insert(view.desired_state->plane_name);
        }
      }
    } else {
      for (const auto& state : view.desired_states) {
        for (const auto& instance : state.instances) {
          if (instance.node_name == node.name) {
            ++desired_instance_count;
            node_planes.insert(state.plane_name);
          }
        }
        for (const auto& disk : state.disks) {
          if (disk.node_name == node.name) {
            ++desired_disk_count;
            node_planes.insert(state.plane_name);
          }
        }
      }
    }
    desired_plane_count = static_cast<int>(node_planes.size());

    json item{
        {"node_name", node.name},
        {"availability", comet::ToString(ResolveNodeAvailability(availability_override_map, node.name))},
        {"plane_count", desired_plane_count},
        {"planes", json(node_planes)},
        {"desired_instance_count", desired_instance_count},
        {"desired_disk_count", desired_disk_count},
        {"gpu_count", node.gpu_devices.size()},
    };
    const auto observation_it = observation_by_node.find(node.name);
    if (observation_it == observation_by_node.end()) {
      item["health"] = "unknown";
      item["status"] = nullptr;
      item["runtime_launch_ready"] = nullptr;
      item["runtime_phase"] = nullptr;
      nodes.push_back(std::move(item));
      continue;
    }

    ++observed_nodes;
    const auto age_seconds = HeartbeatAgeSeconds(observation_it->second.heartbeat_at);
    item["health"] = HealthFromAge(age_seconds, stale_after_seconds);
    item["status"] = comet::ToString(observation_it->second.status);
    item["heartbeat_at"] = observation_it->second.heartbeat_at;
    item["applied_generation"] =
        observation_it->second.applied_generation.has_value()
            ? json(*observation_it->second.applied_generation)
            : json(nullptr);
    if (const auto runtime_status = ParseRuntimeStatus(observation_it->second);
        runtime_status.has_value()) {
      item["runtime_launch_ready"] = runtime_status->launch_ready;
      item["runtime_phase"] =
          runtime_status->runtime_phase.empty() ? json(nullptr) : json(runtime_status->runtime_phase);
      item["runtime_backend"] =
          runtime_status->runtime_backend.empty() ? json(nullptr) : json(runtime_status->runtime_backend);
      if (runtime_status->launch_ready) {
        ++ready_nodes;
      } else {
        ++not_ready_nodes;
      }
    } else {
      const auto fallback = DetermineDashboardRuntimeFallback(
          observation_it->second,
          node.name,
          plane_name,
          selected_plane_state,
          view.desired_generation.value_or(0),
          desired_instance_count,
          desired_disk_count,
          item.value("health", std::string("unknown")));
      if (fallback.available) {
        item["runtime_launch_ready"] = fallback.launch_ready;
        item["runtime_phase"] =
            fallback.runtime_phase.empty() ? json(nullptr) : json(fallback.runtime_phase);
        if (fallback.launch_ready) {
          ++ready_nodes;
        } else {
          ++not_ready_nodes;
        }
      } else {
        item["runtime_launch_ready"] = nullptr;
        item["runtime_phase"] = nullptr;
        ++not_ready_nodes;
      }
    }
    if (const auto gpu_telemetry = ParseGpuTelemetry(observation_it->second);
        gpu_telemetry.has_value()) {
      item["telemetry_degraded"] = gpu_telemetry->degraded;
      if (gpu_telemetry->degraded) {
        ++degraded_gpu_nodes;
      }
    }
    nodes.push_back(std::move(item));
  }
  payload["nodes"] = std::move(nodes);
  payload["runtime"] = {
      {"observed_nodes", observed_nodes},
      {"ready_nodes", ready_nodes},
      {"not_ready_nodes", not_ready_nodes},
      {"degraded_gpu_telemetry_nodes", degraded_gpu_nodes},
  };

  int pending_assignments = 0;
  int claimed_assignments = 0;
  int applied_assignments = 0;
  int failed_assignments = 0;
  const auto latest_assignments_by_node = BuildLatestPlaneAssignmentsByNode(view.assignments);
  json latest_progress = nullptr;
  int latest_progress_assignment_id = -1;
  for (const auto& [node_name, assignment] : latest_assignments_by_node) {
    (void)node_name;
    switch (assignment.status) {
      case comet::HostAssignmentStatus::Pending:
        ++pending_assignments;
        break;
      case comet::HostAssignmentStatus::Claimed:
        ++claimed_assignments;
        break;
      case comet::HostAssignmentStatus::Applied:
        ++applied_assignments;
        break;
      case comet::HostAssignmentStatus::Failed:
        ++failed_assignments;
        break;
      default:
        break;
    }
    if ((assignment.status == comet::HostAssignmentStatus::Pending ||
         assignment.status == comet::HostAssignmentStatus::Claimed) &&
        !assignment.progress_json.empty() &&
        assignment.progress_json != "{}" &&
        assignment.id > latest_progress_assignment_id) {
      latest_progress = json::parse(assignment.progress_json);
      latest_progress_assignment_id = assignment.id;
    }
  }
  json assignment_nodes = json::array();
  for (const auto& [node_name, assignment] : latest_assignments_by_node) {
    (void)node_name;
    assignment_nodes.push_back(json{
        {"node_name", assignment.node_name},
        {"latest_assignment_id", assignment.id},
        {"latest_status", comet::ToString(assignment.status)},
        {"latest_progress",
         (!assignment.progress_json.empty() && assignment.progress_json != "{}")
             ? json::parse(assignment.progress_json)
             : json(nullptr)},
        {"pending", assignment.status == comet::HostAssignmentStatus::Pending ? 1 : 0},
        {"claimed", assignment.status == comet::HostAssignmentStatus::Claimed ? 1 : 0},
        {"failed", assignment.status == comet::HostAssignmentStatus::Failed ? 1 : 0},
    });
  }
  payload["assignments"] = {
      {"total", latest_assignments_by_node.size()},
      {"pending", pending_assignments},
      {"claimed", claimed_assignments},
      {"applied", applied_assignments},
      {"failed", failed_assignments},
      {"latest_progress", latest_progress},
      {"by_node", std::move(assignment_nodes)},
  };

  int pending_rollout = 0;
  int acknowledged_rollout = 0;
  int ready_rollout = 0;
  std::set<std::string> rollout_workers;
  for (const auto& action : rollout_actions) {
    rollout_workers.insert(action.worker_name);
    if (action.status == comet::RolloutActionStatus::Pending) {
      ++pending_rollout;
    } else if (action.status == comet::RolloutActionStatus::Acknowledged) {
      ++acknowledged_rollout;
    } else if (action.status == comet::RolloutActionStatus::ReadyToRetry) {
      ++ready_rollout;
    }
  }
  payload["rollout"] = {
      {"total_actions", rollout_actions.size()},
      {"pending", pending_rollout},
      {"acknowledged", acknowledged_rollout},
      {"ready_to_retry", ready_rollout},
      {"workers", json(rollout_workers)},
      {"loop_status", view.loop_status.state},
      {"loop_reason", view.loop_status.reason},
  };

  int critical_alerts = 0;
  int warning_alerts = 0;
  int booting_alerts = 0;
  json alert_items = json::array();
  const auto push_alert =
      [&](const std::string& severity,
          const std::string& kind,
          const std::string& title,
          const std::string& detail,
          const std::optional<std::string>& node_name = std::nullopt,
          const std::optional<std::string>& worker_name = std::nullopt,
          const std::optional<int>& assignment_id = std::nullopt,
          const std::optional<int>& event_id = std::nullopt) {
        if (severity == "critical") {
          ++critical_alerts;
        } else if (severity == "warning") {
          ++warning_alerts;
        } else if (severity == "booting") {
          ++booting_alerts;
        }
        json item{
            {"severity", severity},
            {"kind", kind},
            {"title", title},
            {"detail", detail},
            {"node_name", node_name.has_value() ? json(*node_name) : json(nullptr)},
            {"worker_name", worker_name.has_value() ? json(*worker_name) : json(nullptr)},
            {"assignment_id", assignment_id.has_value() ? json(*assignment_id) : json(nullptr)},
            {"event_id", event_id.has_value() ? json(*event_id) : json(nullptr)},
        };
        alert_items.push_back(std::move(item));
      };

  for (const auto& [node_name, assignment] : latest_assignments_by_node) {
    (void)node_name;
    if (assignment.status == comet::HostAssignmentStatus::Failed) {
      push_alert(
          "critical",
          "failed-assignment",
          "Assignment failed",
          "Host assignment failed and requires retry or operator action.",
          assignment.node_name,
          std::nullopt,
          assignment.id);
    } else if (
        assignment.status == comet::HostAssignmentStatus::Pending ||
        assignment.status == comet::HostAssignmentStatus::Claimed) {
      push_alert(
          "booting",
          "assignment-in-flight",
          "Assignment in progress",
          "Host assignment is still pending or claimed.",
          assignment.node_name,
          std::nullopt,
          assignment.id);
    }
  }

  for (const auto& [dashboard_node_name, item] : dashboard_nodes) {
    (void)dashboard_node_name;
    const std::string node_name = item.name;
    const auto observation_it = observation_by_node.find(item.name);
    if (observation_it == observation_by_node.end()) {
      push_alert(
          "warning",
          "missing-observation",
          "Node has no observation",
          "Controller does not have a recent observation for this node.",
          node_name);
      continue;
    }

    const auto age_seconds = HeartbeatAgeSeconds(observation_it->second.heartbeat_at);
    const std::string health = HealthFromAge(age_seconds, stale_after_seconds);
    if (health == "failed" || health == "stale") {
      push_alert(
          "critical",
          "node-health",
          "Node heartbeat is stale",
          "Observed state for this node is stale or failed.",
          node_name);
    }

    const auto availability =
        ResolveNodeAvailability(availability_override_map, node_name);
    if (availability != comet::NodeAvailability::Active) {
      push_alert(
          "warning",
          "node-availability",
          "Node is not active",
          "Node availability override is blocking normal scheduling.",
          node_name);
    }

    if (const auto runtime_status = ParseRuntimeStatus(observation_it->second);
        runtime_status.has_value()) {
      if (!runtime_status->launch_ready) {
        push_alert(
            "booting",
            "runtime-not-ready",
            "Runtime still starting",
            "Node runtime is not launch-ready yet.",
            node_name);
      }
    } else {
      const auto fallback = DetermineDashboardRuntimeFallback(
          observation_it->second,
          node_name,
          plane_name,
          selected_plane_state,
          view.desired_generation.value_or(0),
          std::count_if(
              view.desired_state->instances.begin(),
              view.desired_state->instances.end(),
              [&](const auto& instance) { return instance.node_name == node_name; }),
          std::count_if(
              view.desired_state->disks.begin(),
              view.desired_state->disks.end(),
              [&](const auto& disk) { return disk.node_name == node_name; }),
          health);
      if (!fallback.available) {
        push_alert(
            "booting",
            "runtime-missing",
            "Runtime status missing",
            "No runtime status has been reported yet for this node.",
            node_name);
      } else if (!fallback.launch_ready) {
        push_alert(
            "booting",
            "runtime-transition",
            "Runtime transition in progress",
            "Observed runtime state is converging even though low-level runtime status is not available yet.",
            item.name);
      }
    }

    if (const auto gpu_telemetry = ParseGpuTelemetry(observation_it->second);
        gpu_telemetry.has_value() && gpu_telemetry->degraded) {
      push_alert(
          "warning",
          "gpu-telemetry-degraded",
          "GPU telemetry degraded",
          "GPU telemetry is running in degraded mode on this node.",
          item.name);
    }
  }

  for (const auto& action : rollout_actions) {
    push_alert(
        "warning",
        "rollout-action",
        "Deferred rollout requires follow-up",
        action.action + " for worker " + action.worker_name,
        action.target_node_name.empty() ? std::nullopt
                                        : std::optional<std::string>(action.target_node_name),
        action.worker_name);
  }

  json recent_items = json::array();
  int surfaced_event_alerts = 0;
  for (const auto& event : recent_events) {
    if ((event.severity == "error" || event.severity == "warning") &&
        surfaced_event_alerts < 5) {
      push_alert(
          event.severity == "error" ? "critical" : "warning",
          "event-log",
          event.category + "." + event.event_type,
          event.message,
          event.node_name.empty() ? std::nullopt
                                  : std::optional<std::string>(event.node_name),
          event.worker_name.empty() ? std::nullopt
                                    : std::optional<std::string>(event.worker_name),
          std::nullopt,
          event.id);
      ++surfaced_event_alerts;
    }
    recent_items.push_back(BuildEventPayloadItem(event));
  }
  payload["alerts"] = {
      {"critical", critical_alerts},
      {"warning", warning_alerts},
      {"booting", booting_alerts},
      {"total", critical_alerts + warning_alerts + booting_alerts},
      {"items", std::move(alert_items)},
  };
  payload["recent_events"] = std::move(recent_items);
  return payload;
}

json BuildNodeAvailabilityPayload(
    const std::string& db_path,
    const std::optional<std::string>& node_name) {
  comet::ControllerStore store(db_path);
  store.Initialize();
  const auto overrides = store.LoadNodeAvailabilityOverrides(node_name);

  json items = json::array();
  for (const auto& override_record : overrides) {
    items.push_back(json{
        {"node_name", override_record.node_name},
        {"availability", comet::ToString(override_record.availability)},
        {"status_message",
         override_record.status_message.empty() ? json(nullptr) : json(override_record.status_message)},
        {"updated_at",
         override_record.updated_at.empty() ? json(nullptr) : json(override_record.updated_at)},
    });
  }

  return json{
      {"service", "comet-controller"},
      {"db_path", db_path},
      {"node_name", node_name.has_value() ? json(*node_name) : json(nullptr)},
      {"items", items},
  };
}

json BuildRolloutActionsPayload(
    const std::string& db_path,
    const std::optional<std::string>& node_name,
    const std::optional<std::string>& plane_name) {
  const auto view = LoadRolloutActionsViewData(db_path, node_name, plane_name);

  json payload{
      {"service", "comet-controller"},
      {"db_path", view.db_path},
      {"plane_name", view.plane_name.has_value() ? json(*view.plane_name) : json(nullptr)},
      {"node_name", view.node_name.has_value() ? json(*view.node_name) : json(nullptr)},
  };
  payload["desired_generation"] =
      view.desired_generation.has_value() ? json(*view.desired_generation) : json(nullptr);

  json action_items = json::array();
  for (const auto& action : view.actions) {
    action_items.push_back(json{
        {"id", action.id},
        {"desired_generation", action.desired_generation},
        {"step", action.step},
        {"worker_name", action.worker_name},
        {"action", action.action},
        {"target_node_name", action.target_node_name},
        {"target_gpu_device", action.target_gpu_device},
        {"victim_worker_names", action.victim_worker_names},
        {"reason", action.reason},
        {"status", comet::ToString(action.status)},
        {"status_message", action.status_message},
    });
  }
  payload["rollout_gates"] = json{
      {"gated_workers", view.gated_worker_count},
      {"gated_nodes", view.gated_node_count},
      {"deferred_actions", view.actions.size()},
  };
  payload["actions"] = std::move(action_items);

  if (view.desired_state.has_value() && view.desired_generation.has_value()) {
    json lifecycle_items = json::array();
    for (const auto& entry : view.lifecycle) {
      lifecycle_items.push_back(json{
          {"worker_name", entry.worker_name},
          {"desired_generation", entry.desired_generation},
          {"phase", ToString(entry.phase)},
          {"action_id", entry.action_id.has_value() ? json(*entry.action_id) : json(nullptr)},
          {"target_node_name", entry.target_node_name.empty() ? json(nullptr) : json(entry.target_node_name)},
          {"target_gpu_device", entry.target_gpu_device.empty() ? json(nullptr) : json(entry.target_gpu_device)},
          {"victim_worker_names", entry.victim_worker_names},
          {"detail", entry.detail.empty() ? json(nullptr) : json(entry.detail)},
      });
    }
    payload["rollout_lifecycle"] = std::move(lifecycle_items);
  } else {
    payload["rollout_lifecycle"] = json::array();
  }

  return payload;
}

json BuildEventsPayload(
    const std::string& db_path,
    const std::optional<std::string>& plane_name,
    const std::optional<std::string>& node_name,
    const std::optional<std::string>& worker_name,
    const std::optional<std::string>& category,
    int limit) {
  const auto view =
      LoadEventsViewData(db_path, plane_name, node_name, worker_name, category, limit);
  json items = json::array();
  for (const auto& event : view.events) {
    items.push_back(BuildEventPayloadItem(event));
  }

  return json{
      {"service", "comet-controller"},
      {"db_path", view.db_path},
      {"plane_name", view.plane_name.has_value() ? json(*view.plane_name) : json(nullptr)},
      {"node_name", view.node_name.has_value() ? json(*view.node_name) : json(nullptr)},
      {"worker_name", view.worker_name.has_value() ? json(*view.worker_name) : json(nullptr)},
      {"category", view.category.has_value() ? json(*view.category) : json(nullptr)},
      {"limit", view.limit},
      {"events", std::move(items)},
  };
}

json BuildRebalancePlanPayload(
    const std::string& db_path,
    const std::optional<std::string>& node_name,
    int stale_after_seconds,
    const std::optional<std::string>& plane_name) {
  const auto view =
      LoadRebalancePlanViewData(db_path, node_name, stale_after_seconds, plane_name);

  json payload{
      {"service", "comet-controller"},
      {"db_path", view.db_path},
      {"plane_name", view.plane_name.has_value() ? json(*view.plane_name) : json(nullptr)},
      {"node_name", view.node_name.has_value() ? json(*view.node_name) : json(nullptr)},
      {"stale_after_seconds", view.stale_after_seconds},
  };
  if (!view.desired_state.has_value()) {
    payload["desired_state"] = nullptr;
    payload["rebalance_plan"] = json::array();
    return payload;
  }

  json plan_items = json::array();
  for (const auto& entry : view.rebalance_entries) {
    json item;
    item["worker_name"] = entry.worker_name;
    item["placement_mode"] = comet::ToString(entry.placement_mode);
    item["current_node_name"] = entry.current_node_name;
    item["current_gpu_device"] = entry.current_gpu_device;
    item["target_node_name"] =
        entry.target_node_name.empty() ? json(nullptr) : json(entry.target_node_name);
    item["target_gpu_device"] =
        entry.target_gpu_device.empty() ? json(nullptr) : json(entry.target_gpu_device);
    item["rebalance_class"] = entry.rebalance_class;
    item["decision"] = entry.decision;
    item["state"] = entry.state;
    item["action"] = entry.action.empty() ? json(nullptr) : json(entry.action);
    item["score"] = entry.score;
    item["preemption_required"] = entry.preemption_required;
    item["victim_worker_names"] = entry.victim_worker_names;
    item["gate_reason"] = entry.gate_reason.empty() ? json(nullptr) : json(entry.gate_reason);
    plan_items.push_back(std::move(item));
  }

  json worker_runtime_items = json::array();
  for (const auto& [worker_name, runtime] : view.scheduler_runtime.worker_runtime_by_name) {
    json item;
    item["worker_name"] = worker_name;
    item["last_move_at"] = runtime.last_move_at.empty() ? json(nullptr) : json(runtime.last_move_at);
    item["last_eviction_at"] =
        runtime.last_eviction_at.empty() ? json(nullptr) : json(runtime.last_eviction_at);
    item["last_verified_generation"] =
        runtime.last_verified_generation.has_value() ? json(*runtime.last_verified_generation)
                                                     : json(nullptr);
    item["last_scheduler_phase"] =
        runtime.last_scheduler_phase.empty() ? json(nullptr) : json(runtime.last_scheduler_phase);
    item["last_status_message"] =
        runtime.last_status_message.empty() ? json(nullptr) : json(runtime.last_status_message);
    item["manual_intervention_required"] = runtime.manual_intervention_required;
    worker_runtime_items.push_back(std::move(item));
  }
  json node_runtime_items = json::array();
  for (const auto& [runtime_node_name, runtime] : view.scheduler_runtime.node_runtime_by_name) {
    json item;
    item["node_name"] = runtime_node_name;
    item["last_move_at"] = runtime.last_move_at.empty() ? json(nullptr) : json(runtime.last_move_at);
    item["last_verified_generation"] =
        runtime.last_verified_generation.has_value() ? json(*runtime.last_verified_generation)
                                                     : json(nullptr);
    node_runtime_items.push_back(std::move(item));
  }

  payload["plane_name"] = view.desired_state->plane_name;
  payload["desired_generation"] = view.desired_generation;
  payload["rebalance_plan"] = std::move(plan_items);
  payload["controller_gate"] = json{
      {"cluster_ready", view.controller_gate_summary.cluster_ready},
      {"active_rollouts", view.controller_gate_summary.active_rollout_count},
      {"blocking_assignment_nodes", view.controller_gate_summary.blocking_assignment_count},
      {"unconverged_nodes", view.controller_gate_summary.unconverged_node_count},
      {"active_rollout_workers", view.controller_gate_summary.active_rollout_workers},
      {"blocking_assignment_node_names", view.controller_gate_summary.blocking_assignment_nodes},
      {"unconverged_node_names", view.controller_gate_summary.unconverged_nodes},
  };
  payload["iteration_budget"] = json{
      {"current_iteration", view.iteration_budget_summary.current_iteration},
      {"max_iterations", view.iteration_budget_summary.max_iterations},
      {"exhausted", view.iteration_budget_summary.exhausted},
  };
  payload["loop_status"] = json{
      {"state", view.loop_status.state},
      {"reason", view.loop_status.reason},
  };
  payload["policy_summary"] = json{
      {"actionable", view.policy_summary.actionable_count},
      {"safe_direct", view.policy_summary.safe_direct_count},
      {"rollout_class", view.policy_summary.rollout_class_count},
      {"gated", view.policy_summary.gated_count},
      {"blocked_active_rollouts", view.policy_summary.blocked_active_rollout_count},
      {"assignment_busy", view.policy_summary.assignment_busy_count},
      {"observation_gated", view.policy_summary.observation_gated_count},
      {"stable_holds", view.policy_summary.stable_hold_count},
      {"below_threshold", view.policy_summary.below_threshold_count},
      {"deferred", view.policy_summary.defer_count},
      {"no_candidate", view.policy_summary.no_candidate_count},
      {"actionable_workers", view.policy_summary.actionable_workers},
      {"safe_direct_workers", view.policy_summary.safe_direct_workers},
      {"rollout_class_workers", view.policy_summary.rollout_class_workers},
      {"gated_workers", view.policy_summary.gated_workers},
      {"stable_hold_workers", view.policy_summary.stable_hold_workers},
  };
  json scheduler_runtime_json;
  if (view.scheduler_runtime.plane_runtime.has_value()) {
    json plane_runtime_json;
    plane_runtime_json["active_action"] = view.scheduler_runtime.plane_runtime->active_action;
    plane_runtime_json["active_worker_name"] = view.scheduler_runtime.plane_runtime->active_worker_name;
    plane_runtime_json["phase"] = view.scheduler_runtime.plane_runtime->phase;
    plane_runtime_json["action_generation"] = view.scheduler_runtime.plane_runtime->action_generation;
    plane_runtime_json["stable_samples"] = view.scheduler_runtime.plane_runtime->stable_samples;
    plane_runtime_json["rollback_attempt_count"] =
        view.scheduler_runtime.plane_runtime->rollback_attempt_count;
    plane_runtime_json["source_node_name"] = view.scheduler_runtime.plane_runtime->source_node_name;
    plane_runtime_json["source_gpu_device"] = view.scheduler_runtime.plane_runtime->source_gpu_device;
    plane_runtime_json["target_node_name"] = view.scheduler_runtime.plane_runtime->target_node_name;
    plane_runtime_json["target_gpu_device"] = view.scheduler_runtime.plane_runtime->target_gpu_device;
    plane_runtime_json["status_message"] = view.scheduler_runtime.plane_runtime->status_message;
    plane_runtime_json["started_at"] = view.scheduler_runtime.plane_runtime->started_at;
    scheduler_runtime_json["plane_runtime"] = std::move(plane_runtime_json);
  } else {
    scheduler_runtime_json["plane_runtime"] = nullptr;
  }
  scheduler_runtime_json["worker_runtime"] = std::move(worker_runtime_items);
  scheduler_runtime_json["node_runtime"] = std::move(node_runtime_items);
  payload["scheduler_runtime"] = std::move(scheduler_runtime_json);
  return payload;
}

void ShowDemoPlan() {
  PrintStateSummary(comet::BuildDemoState());
}

void PrintPreviewSummary(const comet::DesiredState& state) {
  std::cout << "preview:\n";
  std::cout << "  plane=" << state.plane_name << "\n";
  std::cout << "  nodes=" << state.nodes.size() << "\n";
  std::cout << "  disks=" << state.disks.size() << "\n";
  std::cout << "  instances=" << state.instances.size() << "\n";

  const auto node_plans = comet::BuildNodeComposePlans(state);
  for (const auto& plan : node_plans) {
    std::cout << "  node " << plan.node_name
              << ": services=" << plan.services.size()
              << " disks=" << plan.disks.size() << "\n";
  }
}

int RenderComposeForState(
    const comet::DesiredState& state,
    const std::optional<std::string>& node_name) {
  if (node_name.has_value()) {
    const auto plan = comet::FindNodeComposePlan(state, *node_name);
    if (!plan.has_value()) {
      std::cerr << "error: node '" << *node_name << "' not found in state\n";
      return 1;
    }
    std::cout << comet::RenderComposeYaml(*plan);
    return 0;
  }

  const auto plans = comet::BuildNodeComposePlans(state);
  for (std::size_t index = 0; index < plans.size(); ++index) {
    if (index > 0) {
      std::cout << "\n---\n";
    }
    std::cout << comet::RenderComposeYaml(plans[index]);
  }
  return 0;
}

int RenderDemoCompose(const std::optional<std::string>& node_name) {
  return RenderComposeForState(comet::BuildDemoState(), node_name);
}

int ValidateBundle(const std::string& bundle_dir) {
  const comet::DesiredState state = comet::ImportPlaneBundle(bundle_dir);
  comet::RequireSchedulingPolicy(state);
  const comet::SchedulingPolicyReport scheduling_report =
      comet::EvaluateSchedulingPolicy(state);
  std::cout << "bundle validation: OK\n";
  PrintPreviewSummary(state);
  std::cout << comet::RenderSchedulingPolicyReport(scheduling_report);
  PrintSchedulerDecisionSummary(state);
  PrintRolloutGateSummary(scheduling_report);
  return 0;
}

int PreviewBundle(const std::string& bundle_dir, const std::optional<std::string>& node_name) {
  const comet::DesiredState state = comet::ImportPlaneBundle(bundle_dir);
  const comet::SchedulingPolicyReport scheduling_report =
      comet::EvaluateSchedulingPolicy(state);
  PrintPreviewSummary(state);
  std::cout << comet::RenderSchedulingPolicyReport(scheduling_report);
  PrintSchedulerDecisionSummary(state);
  PrintRolloutGateSummary(scheduling_report);
  std::cout << "\ncompose-preview:\n";
  return RenderComposeForState(state, node_name);
}

int PlanBundle(const std::string& db_path, const std::string& bundle_dir) {
  comet::ControllerStore store(db_path);
  store.Initialize();
  const auto current_state = store.LoadDesiredState();
  const comet::DesiredState desired_state = comet::ImportPlaneBundle(bundle_dir);
  const auto availability_overrides = store.LoadNodeAvailabilityOverrides();
  const auto observations = store.LoadHostObservations();
  const comet::ReconcilePlan plan =
      comet::BuildReconcilePlan(current_state, desired_state);
  const comet::SchedulingPolicyReport scheduling_report =
      comet::EvaluateSchedulingPolicy(desired_state);

  std::cout << "bundle-plan:\n";
  std::cout << "  db=" << db_path << "\n";
  std::cout << "  bundle=" << bundle_dir << "\n";
  std::cout << comet::RenderReconcilePlan(plan);
  std::cout << comet::RenderSchedulingPolicyReport(scheduling_report);
  PrintSchedulerDecisionSummary(desired_state);
  PrintRolloutGateSummary(scheduling_report);
  PrintAssignmentDispatchSummary(
      desired_state,
      BuildAvailabilityOverrideMap(availability_overrides),
      observations,
      DefaultStaleAfterSeconds());
  return 0;
}

std::vector<comet::NodeExecutionPlan> FilterNodeExecutionPlans(
    const std::vector<comet::NodeExecutionPlan>& plans,
    const std::optional<std::string>& node_name) {
  if (!node_name.has_value()) {
    return plans;
  }

  std::vector<comet::NodeExecutionPlan> filtered;
  for (const auto& plan : plans) {
    if (plan.node_name == *node_name) {
      filtered.push_back(plan);
    }
  }

  if (filtered.empty()) {
    throw std::runtime_error("node '" + *node_name + "' not found in execution plan");
  }

  return filtered;
}

std::map<std::string, comet::NodeComposePlan> BuildComposePlanMap(const comet::DesiredState& state) {
  std::map<std::string, comet::NodeComposePlan> result;
  for (const auto& plan : comet::BuildNodeComposePlans(state)) {
    result.emplace(plan.node_name, plan);
  }
  return result;
}

std::map<std::string, comet::NodeAvailabilityOverride> BuildAvailabilityOverrideMap(
    const std::vector<comet::NodeAvailabilityOverride>& availability_overrides) {
  std::map<std::string, comet::NodeAvailabilityOverride> result;
  for (const auto& availability_override : availability_overrides) {
    result.emplace(availability_override.node_name, availability_override);
  }
  return result;
}

comet::NodeAvailability ResolveNodeAvailability(
    const std::map<std::string, comet::NodeAvailabilityOverride>& availability_overrides,
    const std::string& node_name) {
  const auto it = availability_overrides.find(node_name);
  if (it == availability_overrides.end()) {
    return comet::NodeAvailability::Active;
  }
  return it->second.availability;
}

bool IsNodeSchedulable(comet::NodeAvailability availability) {
  return availability == comet::NodeAvailability::Active;
}

void PrintNodeAvailabilityOverrides(
    const std::vector<comet::NodeAvailabilityOverride>& availability_overrides) {
  std::cout << "node-availability:\n";
  if (availability_overrides.empty()) {
    std::cout << "  (empty)\n";
    return;
  }

  for (const auto& availability_override : availability_overrides) {
    std::cout << "  - node=" << availability_override.node_name
              << " availability=" << comet::ToString(availability_override.availability)
              << " updated_at=" << FormatDisplayTimestamp(availability_override.updated_at)
              << "\n";
    if (!availability_override.status_message.empty()) {
      std::cout << "    message=" << availability_override.status_message << "\n";
    }
  }
}

std::optional<std::string> ObservedSchedulingGateReason(
    const std::vector<comet::HostObservation>& observations,
    const std::string& node_name,
    int stale_after_seconds) {
  const auto observation = FindHostObservationForNode(observations, node_name);
  if (!observation.has_value()) {
    return std::nullopt;
  }
  if (observation->status == comet::HostObservationStatus::Failed) {
    return std::string("failed");
  }
  const auto age_seconds = HeartbeatAgeSeconds(observation->heartbeat_at);
  if (HealthFromAge(age_seconds, stale_after_seconds) == "stale") {
    return std::string("stale");
  }
  const auto runtime_status = ParseRuntimeStatus(*observation);
  if (runtime_status.has_value() && runtime_status->runtime_phase == "failed") {
    return std::string("runtime-failed");
  }
  const auto gpu_telemetry = ParseGpuTelemetry(*observation);
  if (gpu_telemetry.has_value() && gpu_telemetry->degraded) {
    return std::string("telemetry-degraded");
  }
  return std::nullopt;
}

void PrintAssignmentDispatchSummary(
    const comet::DesiredState& desired_state,
    const std::map<std::string, comet::NodeAvailabilityOverride>& availability_overrides,
    const std::vector<comet::HostObservation>& observations,
    int stale_after_seconds) {
  std::size_t schedulable_nodes = 0;
  std::vector<std::string> skipped_nodes;
  for (const auto& node : desired_state.nodes) {
    const auto availability = ResolveNodeAvailability(availability_overrides, node.name);
    if (!IsNodeSchedulable(availability)) {
      skipped_nodes.push_back(
          node.name + "(" + comet::ToString(availability) + ")");
      continue;
    }
    const auto observed_gate_reason =
        ObservedSchedulingGateReason(observations, node.name, stale_after_seconds);
    if (observed_gate_reason.has_value()) {
      skipped_nodes.push_back(node.name + "(" + *observed_gate_reason + ")");
      continue;
    }
    ++schedulable_nodes;
  }

  std::cout << "assignment-dispatch:\n";
  std::cout << "  schedulable_nodes=" << schedulable_nodes << "/" << desired_state.nodes.size()
            << "\n";
  if (!skipped_nodes.empty()) {
    std::cout << "  skipped_nodes=";
    for (std::size_t index = 0; index < skipped_nodes.size(); ++index) {
      if (index > 0) {
        std::cout << ",";
      }
      std::cout << skipped_nodes[index];
    }
    std::cout << "\n";
  }
}

void WriteTextFile(const std::string& path, const std::string& contents) {
  const std::filesystem::path file_path(path);
  if (file_path.has_parent_path()) {
    std::filesystem::create_directories(file_path.parent_path());
  }

  std::ofstream out(path, std::ios::binary | std::ios::trunc);
  if (!out.is_open()) {
    throw std::runtime_error("failed to open file for write: " + path);
  }
  out << contents;
  if (!out.good()) {
    throw std::runtime_error("failed to write file: " + path);
  }
}

void RemoveFileIfExists(const std::string& path) {
  std::error_code error;
  std::filesystem::remove(path, error);
  if (error) {
    throw std::runtime_error("failed to remove file '" + path + "': " + error.message());
  }
}

void MaterializeComposeArtifacts(
    const comet::DesiredState& desired_state,
    const std::vector<comet::NodeExecutionPlan>& host_plans) {
  const auto desired_compose_plans = BuildComposePlanMap(desired_state);

  for (const auto& host_plan : host_plans) {
    for (const auto& operation : host_plan.operations) {
      if (operation.kind == comet::HostOperationKind::WriteComposeFile) {
        const auto compose_it = desired_compose_plans.find(host_plan.node_name);
        if (compose_it == desired_compose_plans.end()) {
          throw std::runtime_error(
              "missing compose plan for node '" + host_plan.node_name + "'");
        }
        WriteTextFile(operation.target, comet::RenderComposeYaml(compose_it->second));
      }

      if (operation.kind == comet::HostOperationKind::RemoveComposeFile) {
        RemoveFileIfExists(operation.target);
      }
    }
  }
}

std::string InferRuntimeArtifactPath(
    const std::string& artifacts_root,
    const std::string& plane_name) {
  return (
      std::filesystem::path(artifacts_root) / plane_name / "infer-runtime.json").string();
}

void MaterializeInferRuntimeArtifact(
    const comet::DesiredState& desired_state,
    const std::string& artifacts_root) {
  WriteTextFile(
      InferRuntimeArtifactPath(artifacts_root, desired_state.plane_name),
      comet::RenderInferRuntimeConfigJson(desired_state));
}

std::vector<comet::HostAssignment> BuildHostAssignments(
    const comet::DesiredState& desired_state,
    const std::string& artifacts_root,
    int desired_generation,
    const std::vector<comet::NodeAvailabilityOverride>& availability_overrides,
    const std::vector<comet::HostObservation>& observations,
    const std::optional<comet::SchedulingPolicyReport>& scheduling_report = std::nullopt) {
  std::vector<comet::HostAssignment> assignments;
  assignments.reserve(desired_state.nodes.size());
  const auto availability_override_map =
      BuildAvailabilityOverrideMap(availability_overrides);
  const auto rollout_actions_by_target_node =
      scheduling_report.has_value()
          ? BuildRolloutActionsByTargetNode(*scheduling_report)
          : std::map<std::string, std::vector<comet::SchedulerRolloutAction>>{};

  for (const auto& node : desired_state.nodes) {
    if (!IsNodeSchedulable(
            ResolveNodeAvailability(availability_override_map, node.name))) {
      continue;
    }
    if (ObservedSchedulingGateReason(
            observations, node.name, DefaultStaleAfterSeconds()).has_value()) {
      continue;
    }
    comet::HostAssignment assignment;
    assignment.node_name = node.name;
    assignment.plane_name = desired_state.plane_name;
    assignment.desired_generation = desired_generation;
    assignment.assignment_type = "apply-node-state";
    assignment.desired_state_json =
        comet::SerializeDesiredStateJson(
            comet::SliceDesiredStateForNode(desired_state, node.name));
    assignment.artifacts_root = artifacts_root;
    assignment.status = comet::HostAssignmentStatus::Pending;
    const auto rollout_it = rollout_actions_by_target_node.find(node.name);
    if (rollout_it != rollout_actions_by_target_node.end() &&
        !rollout_it->second.empty()) {
      std::set<std::string> gated_workers;
      for (const auto& action : rollout_it->second) {
        if (!action.worker_name.empty()) {
          gated_workers.insert(action.worker_name);
        }
      }
      std::ostringstream message;
      message << "scheduler rollout actions pending on target node " << node.name << " for workers ";
      bool first = true;
      for (const auto& worker_name : gated_workers) {
        if (!first) {
          message << ",";
        }
        first = false;
        message << worker_name;
      }
      assignment.status_message = message.str();
    }
    assignments.push_back(std::move(assignment));
  }

  return assignments;
}

std::optional<comet::HostAssignment> FindLatestHostAssignmentForNode(
    const std::vector<comet::HostAssignment>& assignments,
    const std::string& node_name) {
  std::optional<comet::HostAssignment> result;
  for (const auto& assignment : assignments) {
    if (assignment.node_name != node_name) {
      continue;
    }
    result = assignment;
  }
  return result;
}

std::optional<comet::HostAssignment> FindLatestHostAssignmentForNodePlane(
    const std::vector<comet::HostAssignment>& assignments,
    const std::string& node_name,
    const std::string& plane_name) {
  std::optional<comet::HostAssignment> result;
  for (const auto& assignment : assignments) {
    if (assignment.node_name != node_name || assignment.plane_name != plane_name) {
      continue;
    }
    result = assignment;
  }
  return result;
}

std::optional<comet::HostAssignment> FindLatestHostAssignmentForPlane(
    const std::vector<comet::HostAssignment>& assignments,
    const std::string& plane_name) {
  std::optional<comet::HostAssignment> result;
  for (const auto& assignment : assignments) {
    if (assignment.plane_name != plane_name) {
      continue;
    }
    result = assignment;
  }
  return result;
}

bool AssignmentRepresentsDrainedNode(const comet::HostAssignment& assignment) {
  return assignment.assignment_type == "drain-node-state" &&
         (assignment.status == comet::HostAssignmentStatus::Pending ||
          assignment.status == comet::HostAssignmentStatus::Claimed ||
          assignment.status == comet::HostAssignmentStatus::Applied);
}

bool ObservedNodeStateNeedsResync(
    const comet::DesiredState& desired_state,
    const std::string& node_name,
    const comet::HostObservation& observation) {
  if (observation.observed_state_json.empty()) {
    return true;
  }

  const comet::DesiredState observed_node_state =
      comet::DeserializeDesiredStateJson(observation.observed_state_json);
  const comet::DesiredState desired_node_state =
      comet::SliceDesiredStateForNode(desired_state, node_name);

  if (desired_node_state.disks.empty() && desired_node_state.instances.empty()) {
    return false;
  }

  std::size_t observed_disk_count = 0;
  std::size_t observed_instance_count = 0;
  for (const auto& disk : observed_node_state.disks) {
    if (disk.node_name == node_name && disk.plane_name == desired_state.plane_name) {
      ++observed_disk_count;
    }
  }
  for (const auto& instance : observed_node_state.instances) {
    if (instance.node_name == node_name && instance.plane_name == desired_state.plane_name) {
      ++observed_instance_count;
    }
  }

  if (!desired_node_state.disks.empty() && observed_disk_count == 0) {
    return true;
  }
  if (!desired_node_state.instances.empty() && observed_instance_count == 0) {
    return true;
  }
  return false;
}

std::optional<comet::HostAssignment> BuildResyncAssignmentForNode(
    const comet::DesiredState& desired_state,
    int desired_generation,
    const std::string& node_name,
    const std::vector<comet::HostAssignment>& existing_assignments,
    const std::optional<comet::HostObservation>& observation) {
  bool node_exists = false;
  for (const auto& node : desired_state.nodes) {
    if (node.name == node_name) {
      node_exists = true;
      break;
    }
  }
  if (!node_exists) {
    return std::nullopt;
  }

  const auto latest_assignment = FindLatestHostAssignmentForNodePlane(
      existing_assignments,
      node_name,
      desired_state.plane_name);
  const bool latest_assignment_is_drain =
      latest_assignment.has_value() && latest_assignment->desired_generation == desired_generation &&
      AssignmentRepresentsDrainedNode(*latest_assignment);

  if (observation.has_value() &&
      observation->applied_generation.has_value() &&
      *observation->applied_generation == desired_generation &&
      observation->status != comet::HostObservationStatus::Failed &&
      !latest_assignment_is_drain &&
      !ObservedNodeStateNeedsResync(desired_state, node_name, *observation)) {
    return std::nullopt;
  }

  if (latest_assignment.has_value() &&
      latest_assignment->desired_generation == desired_generation &&
      latest_assignment->assignment_type == "apply-node-state" &&
      (latest_assignment->status == comet::HostAssignmentStatus::Pending ||
       latest_assignment->status == comet::HostAssignmentStatus::Claimed ||
       latest_assignment->status == comet::HostAssignmentStatus::Applied)) {
    return std::nullopt;
  }

  comet::HostAssignment assignment;
  assignment.node_name = node_name;
  assignment.plane_name = desired_state.plane_name;
  assignment.desired_generation = desired_generation;
  assignment.assignment_type = "apply-node-state";
  assignment.desired_state_json =
      comet::SerializeDesiredStateJson(
          comet::SliceDesiredStateForNode(desired_state, node_name));
  const auto plane_assignment =
      FindLatestHostAssignmentForPlane(existing_assignments, desired_state.plane_name);
  assignment.artifacts_root = latest_assignment.has_value()
                                  ? latest_assignment->artifacts_root
                                  : (plane_assignment.has_value()
                                         ? plane_assignment->artifacts_root
                                         : DefaultArtifactsRoot());
  assignment.status = comet::HostAssignmentStatus::Pending;
  assignment.status_message = "resync after node returned to active";
  return assignment;
}

std::optional<comet::NodeInventory> FindNodeInventory(
    const comet::DesiredState& desired_state,
    const std::string& node_name) {
  for (const auto& node : desired_state.nodes) {
    if (node.name == node_name) {
      return node;
    }
  }
  return std::nullopt;
}

std::optional<comet::HostAssignment> BuildDrainAssignmentForNode(
    const comet::DesiredState& desired_state,
    int desired_generation,
    const std::string& node_name,
    const std::vector<comet::HostAssignment>& existing_assignments) {
  const auto node = FindNodeInventory(desired_state, node_name);
  if (!node.has_value()) {
    return std::nullopt;
  }

  comet::DesiredState drain_state;
  drain_state.plane_name = desired_state.plane_name;
  drain_state.plane_shared_disk_name = desired_state.plane_shared_disk_name;
  drain_state.control_root = desired_state.control_root;
  drain_state.inference = desired_state.inference;
  drain_state.gateway = desired_state.gateway;
  drain_state.runtime_gpu_nodes = desired_state.runtime_gpu_nodes;
  drain_state.nodes.push_back(*node);

  const auto latest_assignment = FindLatestHostAssignmentForNodePlane(
      existing_assignments,
      node_name,
      desired_state.plane_name);
  const auto plane_assignment =
      FindLatestHostAssignmentForPlane(existing_assignments, desired_state.plane_name);

  comet::HostAssignment assignment;
  assignment.node_name = node_name;
  assignment.plane_name = desired_state.plane_name;
  assignment.desired_generation = desired_generation;
  assignment.assignment_type = "drain-node-state";
  assignment.desired_state_json = comet::SerializeDesiredStateJson(drain_state);
  assignment.artifacts_root = latest_assignment.has_value()
                                  ? latest_assignment->artifacts_root
                                  : (plane_assignment.has_value()
                                         ? plane_assignment->artifacts_root
                                         : DefaultArtifactsRoot());
  assignment.status = comet::HostAssignmentStatus::Pending;
  assignment.status_message = "drain after node availability changed";
  return assignment;
}

comet::DesiredState BuildStoppedPlaneNodeState(
    const comet::DesiredState& desired_state,
    const std::string& node_name) {
  comet::DesiredState stopped_state = comet::SliceDesiredStateForNode(desired_state, node_name);
  stopped_state.instances.clear();
  return stopped_state;
}

comet::DesiredState BuildDeletedPlaneNodeState(
    const comet::DesiredState& desired_state,
    const std::string& node_name) {
  comet::DesiredState deleted_state = comet::SliceDesiredStateForNode(desired_state, node_name);
  deleted_state.instances.clear();
  deleted_state.disks.clear();
  deleted_state.plane_shared_disk_name.clear();
  deleted_state.control_root.clear();
  return deleted_state;
}

std::vector<comet::HostAssignment> BuildStopPlaneAssignments(
    const comet::DesiredState& desired_state,
    int desired_generation,
    const std::string& artifacts_root,
    const std::vector<comet::NodeAvailabilityOverride>& availability_overrides) {
  std::vector<comet::HostAssignment> assignments;
  assignments.reserve(desired_state.nodes.size());
  const auto availability_override_map =
      BuildAvailabilityOverrideMap(availability_overrides);
  for (const auto& node : desired_state.nodes) {
    if (!IsNodeSchedulable(
            ResolveNodeAvailability(availability_override_map, node.name))) {
      continue;
    }
    comet::HostAssignment assignment;
    assignment.node_name = node.name;
    assignment.plane_name = desired_state.plane_name;
    assignment.desired_generation = desired_generation;
    assignment.assignment_type = "stop-plane-state";
    assignment.desired_state_json =
        comet::SerializeDesiredStateJson(
            BuildStoppedPlaneNodeState(desired_state, node.name));
    assignment.artifacts_root = artifacts_root;
    assignment.status = comet::HostAssignmentStatus::Pending;
    assignment.status_message = "plane stop lifecycle transition";
    assignments.push_back(std::move(assignment));
  }
  return assignments;
}

std::vector<comet::HostAssignment> BuildDeletePlaneAssignments(
    const comet::DesiredState& desired_state,
    int desired_generation,
    const std::string& artifacts_root) {
  std::vector<comet::HostAssignment> assignments;
  assignments.reserve(desired_state.nodes.size());
  for (const auto& node : desired_state.nodes) {
    comet::HostAssignment assignment;
    assignment.node_name = node.name;
    assignment.plane_name = desired_state.plane_name;
    assignment.desired_generation = desired_generation;
    assignment.assignment_type = "delete-plane-state";
    assignment.desired_state_json =
        comet::SerializeDesiredStateJson(
            BuildDeletedPlaneNodeState(desired_state, node.name));
    assignment.artifacts_root = artifacts_root;
    assignment.status = comet::HostAssignmentStatus::Pending;
    assignment.status_message = "plane delete lifecycle transition";
    assignments.push_back(std::move(assignment));
  }
  return assignments;
}

void PrintHostAssignments(const std::vector<comet::HostAssignment>& assignments) {
  std::cout << "host-assignments:\n";
  if (assignments.empty()) {
    std::cout << "  (empty)\n";
    return;
  }

  for (const auto& assignment : assignments) {
    const comet::DesiredState desired_node_state =
        comet::DeserializeDesiredStateJson(assignment.desired_state_json);
    std::cout << "  - id=" << assignment.id
              << " node=" << assignment.node_name
              << " plane=" << assignment.plane_name
              << " generation=" << assignment.desired_generation
              << " attempts=" << assignment.attempt_count << "/" << assignment.max_attempts
              << " type=" << assignment.assignment_type
              << " status=" << comet::ToString(assignment.status)
              << " instances=" << desired_node_state.instances.size()
              << " artifacts_root=" << assignment.artifacts_root << "\n";
    if (!assignment.status_message.empty()) {
      std::cout << "    message=" << assignment.status_message << "\n";
    }
  }
}

std::time_t ToUtcTime(std::tm* timestamp) {
#if defined(_WIN32)
  return _mkgmtime(timestamp);
#else
  return timegm(timestamp);
#endif
}

std::optional<long long> HeartbeatAgeSeconds(const std::string& heartbeat_at) {
  if (heartbeat_at.empty()) {
    return std::nullopt;
  }

  std::tm heartbeat_tm{};
  std::istringstream input(heartbeat_at);
  input >> std::get_time(&heartbeat_tm, "%Y-%m-%d %H:%M:%S");
  if (input.fail()) {
    return std::nullopt;
  }

  const std::time_t heartbeat_time = ToUtcTime(&heartbeat_tm);
  if (heartbeat_time < 0) {
    return std::nullopt;
  }

  const std::time_t now = std::time(nullptr);
  return static_cast<long long>(now - heartbeat_time);
}

std::optional<long long> TimestampAgeSeconds(const std::string& timestamp_text) {
  return HeartbeatAgeSeconds(timestamp_text);
}

SchedulerRuntimeView LoadSchedulerRuntimeView(
    comet::ControllerStore& store,
    const std::optional<comet::DesiredState>& desired_state) {
  SchedulerRuntimeView view;
  if (!desired_state.has_value()) {
    return view;
  }
  view.plane_runtime = store.LoadSchedulerPlaneRuntime(desired_state->plane_name);
  for (const auto& runtime : store.LoadSchedulerWorkerRuntimes(desired_state->plane_name)) {
    view.worker_runtime_by_name.emplace(runtime.worker_name, runtime);
  }
  for (const auto& runtime : store.LoadSchedulerNodeRuntimes(desired_state->plane_name)) {
    view.node_runtime_by_name.emplace(runtime.node_name, runtime);
  }
  return view;
}

void PrintSchedulerRuntimeView(const SchedulerRuntimeView& view) {
  std::cout << "scheduler-runtime:\n";
  if (!view.plane_runtime.has_value()) {
    std::cout << "  plane_action=(none)\n";
  } else {
    const auto& runtime = *view.plane_runtime;
    std::cout << "  plane_action="
              << (runtime.active_action.empty() ? "(none)" : runtime.active_action)
              << " phase=" << (runtime.phase.empty() ? "(empty)" : runtime.phase)
              << " worker="
              << (runtime.active_worker_name.empty() ? "(empty)" : runtime.active_worker_name)
              << " generation=" << runtime.action_generation
              << " stable_samples=" << runtime.stable_samples << "/"
              << VerificationStableSamplesRequired()
              << " rollback_attempts=" << runtime.rollback_attempt_count << "\n";
    if (!runtime.status_message.empty()) {
      std::cout << "  status_message=" << runtime.status_message << "\n";
    }
  }
  if (!view.worker_runtime_by_name.empty()) {
    std::cout << "  worker_runtime:\n";
    for (const auto& [worker_name, runtime] : view.worker_runtime_by_name) {
      std::cout << "    - worker=" << worker_name
                << " last_move_at="
                << (runtime.last_move_at.empty() ? "(empty)" : runtime.last_move_at)
                << " last_eviction_at="
                << (runtime.last_eviction_at.empty() ? "(empty)" : runtime.last_eviction_at);
      if (runtime.last_verified_generation.has_value()) {
        std::cout << " last_verified_generation=" << *runtime.last_verified_generation;
      }
      if (!runtime.last_scheduler_phase.empty()) {
        std::cout << " last_phase=" << runtime.last_scheduler_phase;
      }
      std::cout << " manual_intervention_required="
                << (runtime.manual_intervention_required ? "yes" : "no") << "\n";
      if (!runtime.last_status_message.empty()) {
        std::cout << "      status_message=" << runtime.last_status_message << "\n";
      }
    }
  }
  if (!view.node_runtime_by_name.empty()) {
    std::cout << "  node_runtime:\n";
    for (const auto& [node_name, runtime] : view.node_runtime_by_name) {
      std::cout << "    - node=" << node_name
                << " last_move_at="
                << (runtime.last_move_at.empty() ? "(empty)" : runtime.last_move_at);
      if (runtime.last_verified_generation.has_value()) {
        std::cout << " last_verified_generation=" << *runtime.last_verified_generation;
      }
      std::cout << "\n";
    }
  }
}

const comet::RuntimeProcessStatus* FindInstanceRuntimeStatus(
    const std::vector<comet::RuntimeProcessStatus>& statuses,
    const std::string& instance_name,
    const std::string& gpu_device) {
  for (const auto& status : statuses) {
    if (status.instance_name == instance_name &&
        status.gpu_device == gpu_device) {
      return &status;
    }
  }
  return nullptr;
}

bool TelemetryShowsOwnedProcess(
    const std::optional<comet::GpuTelemetrySnapshot>& telemetry,
    const std::string& gpu_device,
    const std::string& instance_name) {
  if (!telemetry.has_value()) {
    return false;
  }
  for (const auto& device : telemetry->devices) {
    if (device.gpu_device != gpu_device) {
      continue;
    }
    for (const auto& process : device.processes) {
      if (process.instance_name == instance_name) {
        return true;
      }
    }
  }
  return false;
}

struct SchedulerVerificationResult {
  bool converged = false;
  bool stable = false;
  bool timed_out = false;
  int next_stable_samples = 0;
  std::string detail;
};

SchedulerVerificationResult EvaluateSchedulerActionVerification(
    const comet::SchedulerPlaneRuntime& plane_runtime,
    const std::vector<comet::HostObservation>& observations) {
  SchedulerVerificationResult result;
  const bool rollback_mode = plane_runtime.phase == "rollback-applied" ||
                             plane_runtime.phase == "rollback-planned";
  const std::string expected_node =
      rollback_mode ? plane_runtime.source_node_name : plane_runtime.target_node_name;
  const std::string expected_gpu =
      rollback_mode ? plane_runtime.source_gpu_device : plane_runtime.target_gpu_device;
  const std::string cleared_node =
      rollback_mode ? plane_runtime.target_node_name : plane_runtime.source_node_name;
  const std::string cleared_gpu =
      rollback_mode ? plane_runtime.target_gpu_device : plane_runtime.source_gpu_device;

  const auto target_observation = FindHostObservationForNode(observations, expected_node);
  const auto source_observation = FindHostObservationForNode(observations, cleared_node);
  if (!target_observation.has_value()) {
    result.detail = "missing-target-observation";
  } else {
    const auto target_runtimes = ParseInstanceRuntimeStatuses(*target_observation);
    const auto target_runtime =
        FindInstanceRuntimeStatus(target_runtimes, plane_runtime.active_worker_name, expected_gpu);
    const auto target_telemetry = ParseGpuTelemetry(*target_observation);
    const bool target_generation_applied =
        target_observation->applied_generation.has_value() &&
        *target_observation->applied_generation >= plane_runtime.action_generation;
    const bool target_runtime_ready =
        target_runtime != nullptr &&
        target_runtime->ready &&
        (target_runtime->runtime_phase == "running" ||
         target_runtime->runtime_phase == "ready" ||
         target_runtime->runtime_phase == "loaded");
    const bool target_gpu_owned =
        TelemetryShowsOwnedProcess(
            target_telemetry, expected_gpu, plane_runtime.active_worker_name);

    bool source_cleared = true;
    if (source_observation.has_value()) {
      const auto source_runtimes = ParseInstanceRuntimeStatuses(*source_observation);
      const auto source_runtime =
          FindInstanceRuntimeStatus(
              source_runtimes,
              plane_runtime.active_worker_name,
              cleared_gpu);
      const auto source_telemetry = ParseGpuTelemetry(*source_observation);
      source_cleared =
          source_runtime == nullptr &&
          !TelemetryShowsOwnedProcess(
              source_telemetry, cleared_gpu, plane_runtime.active_worker_name);
    }

    result.converged =
        target_generation_applied && target_runtime_ready && target_gpu_owned && source_cleared;
    if (result.converged) {
      result.next_stable_samples = plane_runtime.stable_samples + 1;
      result.stable = result.next_stable_samples >= VerificationStableSamplesRequired();
      result.detail = "verified-sample";
    } else {
      result.next_stable_samples = 0;
      std::ostringstream detail;
      detail << "target_generation_applied=" << (target_generation_applied ? "yes" : "no")
             << " target_runtime_ready=" << (target_runtime_ready ? "yes" : "no")
             << " target_gpu_owned=" << (target_gpu_owned ? "yes" : "no")
             << " source_cleared=" << (source_cleared ? "yes" : "no");
      result.detail = detail.str();
    }
  }

  const auto action_age = TimestampAgeSeconds(plane_runtime.started_at);
  result.timed_out =
      action_age.has_value() && *action_age >= VerificationTimeoutSeconds();
  return result;
}

std::string UtcNowSqlTimestamp() {
  const std::time_t now = std::time(nullptr);
  std::tm tm{};
  if (!comet::platform::GmTime(&now, &tm)) {
    throw std::runtime_error("failed to format current UTC timestamp");
  }
  std::ostringstream out;
  out << std::put_time(&tm, "%Y-%m-%d %H:%M:%S");
  return out.str();
}

void MarkWorkerMoveVerified(
    comet::ControllerStore* store,
    const comet::SchedulerPlaneRuntime& plane_runtime) {
  if (store == nullptr) {
    return;
  }
  const std::string now = UtcNowSqlTimestamp();
  comet::SchedulerWorkerRuntime worker_runtime;
  if (const auto current = store->LoadSchedulerWorkerRuntime(plane_runtime.active_worker_name);
      current.has_value()) {
    worker_runtime = *current;
  }
  worker_runtime.plane_name = plane_runtime.plane_name;
  worker_runtime.worker_name = plane_runtime.active_worker_name;
  worker_runtime.last_move_at = now;
  worker_runtime.last_verified_generation = plane_runtime.action_generation;
  worker_runtime.last_scheduler_phase = "verified";
  worker_runtime.last_status_message =
      plane_runtime.phase == "rollback-applied"
          ? "rollback verification succeeded"
          : "move verification succeeded";
  worker_runtime.manual_intervention_required = false;
  store->UpsertSchedulerWorkerRuntime(worker_runtime);

  for (const auto& node_name : {plane_runtime.source_node_name, plane_runtime.target_node_name}) {
    if (node_name.empty()) {
      continue;
    }
    comet::SchedulerNodeRuntime node_runtime;
    if (const auto current = store->LoadSchedulerNodeRuntime(node_name); current.has_value()) {
      node_runtime = *current;
    }
    node_runtime.plane_name = plane_runtime.plane_name;
    node_runtime.node_name = node_name;
    node_runtime.last_move_at = now;
    node_runtime.last_verified_generation = plane_runtime.action_generation;
    store->UpsertSchedulerNodeRuntime(node_runtime);
  }
}

void MarkWorkersEvicted(
    comet::ControllerStore* store,
    const std::string& plane_name,
    const std::vector<std::string>& worker_names) {
  if (store == nullptr) {
    return;
  }
  const std::string now = UtcNowSqlTimestamp();
  for (const auto& worker_name : worker_names) {
    if (worker_name.empty()) {
      continue;
    }
    comet::SchedulerWorkerRuntime runtime;
    if (const auto current = store->LoadSchedulerWorkerRuntime(worker_name); current.has_value()) {
      runtime = *current;
    }
    runtime.plane_name = plane_name;
    runtime.worker_name = worker_name;
    runtime.last_eviction_at = now;
    runtime.last_scheduler_phase = "evicted";
    runtime.last_status_message = "eviction verified";
    store->UpsertSchedulerWorkerRuntime(runtime);
  }
}

std::string HealthFromAge(
    const std::optional<long long>& age_seconds,
    int stale_after_seconds) {
  if (!age_seconds.has_value()) {
    return "unknown";
  }
  return *age_seconds > stale_after_seconds ? "stale" : "online";
}

std::optional<comet::RuntimeStatus> ParseRuntimeStatus(
    const comet::HostObservation& observation) {
  if (observation.runtime_status_json.empty()) {
    return std::nullopt;
  }
  return comet::DeserializeRuntimeStatusJson(observation.runtime_status_json);
}

std::vector<comet::RuntimeProcessStatus> ParseInstanceRuntimeStatuses(
    const comet::HostObservation& observation) {
  if (observation.instance_runtime_json.empty()) {
    return {};
  }
  return comet::DeserializeRuntimeStatusListJson(observation.instance_runtime_json);
}

std::optional<comet::GpuTelemetrySnapshot> ParseGpuTelemetry(
    const comet::HostObservation& observation) {
  if (observation.gpu_telemetry_json.empty()) {
    return std::nullopt;
  }
  return comet::DeserializeGpuTelemetryJson(observation.gpu_telemetry_json);
}

std::optional<comet::DiskTelemetrySnapshot> ParseDiskTelemetry(
    const comet::HostObservation& observation) {
  if (observation.disk_telemetry_json.empty()) {
    return std::nullopt;
  }
  return comet::DeserializeDiskTelemetryJson(observation.disk_telemetry_json);
}

std::optional<comet::NetworkTelemetrySnapshot> ParseNetworkTelemetry(
    const comet::HostObservation& observation) {
  if (observation.network_telemetry_json.empty()) {
    return std::nullopt;
  }
  return comet::DeserializeNetworkTelemetryJson(observation.network_telemetry_json);
}

std::optional<comet::CpuTelemetrySnapshot> ParseCpuTelemetry(
    const comet::HostObservation& observation) {
  if (observation.cpu_telemetry_json.empty()) {
    return std::nullopt;
  }
  return comet::DeserializeCpuTelemetryJson(observation.cpu_telemetry_json);
}

void PrintHostObservations(
    const std::vector<comet::HostObservation>& observations,
    int stale_after_seconds) {
  std::cout << "host-observations:\n";
  if (observations.empty()) {
    std::cout << "  (empty)\n";
    return;
  }

  for (const auto& observation : observations) {
    std::size_t disk_count = 0;
    std::size_t instance_count = 0;
    if (!observation.observed_state_json.empty()) {
      const comet::DesiredState observed_state =
          comet::DeserializeDesiredStateJson(observation.observed_state_json);
      disk_count = observed_state.disks.size();
      instance_count = observed_state.instances.size();
    }
    const auto runtime_status = ParseRuntimeStatus(observation);
    const auto instance_statuses = ParseInstanceRuntimeStatuses(observation);
    const auto gpu_telemetry = ParseGpuTelemetry(observation);
    const auto disk_telemetry = ParseDiskTelemetry(observation);
    const auto network_telemetry = ParseNetworkTelemetry(observation);
    const auto cpu_telemetry = ParseCpuTelemetry(observation);
    const auto age_seconds = HeartbeatAgeSeconds(observation.heartbeat_at);

    std::cout << "  - node=" << observation.node_name
              << " plane=" << (observation.plane_name.empty() ? "(none)" : observation.plane_name)
              << " status=" << comet::ToString(observation.status);
    if (observation.applied_generation.has_value()) {
      std::cout << " applied_generation=" << *observation.applied_generation;
    }
    if (observation.last_assignment_id.has_value()) {
      std::cout << " last_assignment_id=" << *observation.last_assignment_id;
    }
    std::cout << " disks=" << disk_count
              << " instances=" << instance_count
              << " heartbeat_at=" << FormatDisplayTimestamp(observation.heartbeat_at);
    if (age_seconds.has_value()) {
      std::cout << " age_seconds=" << *age_seconds
                << " health=" << HealthFromAge(age_seconds, stale_after_seconds);
    }
    if (runtime_status.has_value()) {
      std::cout << " runtime_backend="
                << (runtime_status->runtime_backend.empty()
                        ? "(empty)"
                        : runtime_status->runtime_backend)
                << " runtime_phase="
                << (runtime_status->runtime_phase.empty()
                        ? "(empty)"
                        : runtime_status->runtime_phase)
                << " runtime_launch_ready="
                << (runtime_status->launch_ready ? "yes" : "no")
                << " runtime_model="
                << (runtime_status->active_model_id.empty()
                        ? "(empty)"
                        : runtime_status->active_model_id)
                << " gateway="
                << (runtime_status->gateway_listen.empty()
                        ? "(empty)"
                        : runtime_status->gateway_listen);
    }
    if (gpu_telemetry.has_value()) {
      std::cout << " telemetry_source="
                << (gpu_telemetry->source.empty() ? "(empty)" : gpu_telemetry->source)
                << " telemetry_degraded=" << (gpu_telemetry->degraded ? "yes" : "no")
                << " gpu_devices=" << gpu_telemetry->devices.size();
    }
    if (disk_telemetry.has_value()) {
      std::cout << " disk_telemetry_source="
                << (disk_telemetry->source.empty() ? "(empty)" : disk_telemetry->source)
                << " disk_count=" << disk_telemetry->items.size();
      std::uint64_t total_read_bytes = 0;
      std::uint64_t total_write_bytes = 0;
      int total_fault_count = 0;
      int total_warning_count = 0;
      for (const auto& disk : disk_telemetry->items) {
        total_read_bytes += disk.read_bytes;
        total_write_bytes += disk.write_bytes;
        total_fault_count += disk.fault_count;
        total_warning_count += disk.warning_count;
      }
      std::cout << " disk_read_bytes=" << total_read_bytes
                << " disk_write_bytes=" << total_write_bytes
                << " disk_faults=" << total_fault_count
                << " disk_warnings=" << total_warning_count;
    }
    if (network_telemetry.has_value()) {
      std::cout << " network_telemetry_source="
                << (network_telemetry->source.empty() ? "(empty)" : network_telemetry->source)
                << " net_ifaces=" << network_telemetry->interfaces.size();
    }
    if (cpu_telemetry.has_value()) {
      std::cout << " cpu_telemetry_source="
                << (cpu_telemetry->source.empty() ? "(empty)" : cpu_telemetry->source)
                << " cpu_utilization_pct=" << static_cast<int>(cpu_telemetry->utilization_pct)
                << " cpu_cores=" << cpu_telemetry->core_count;
    }
    if (!instance_statuses.empty()) {
      std::cout << " instance_runtimes=" << instance_statuses.size();
    }
    std::cout << "\n";
    if (!observation.status_message.empty()) {
      std::cout << "    message=" << observation.status_message << "\n";
    }
    if (runtime_status.has_value()) {
      std::cout << "    runtime aliases=";
      if (runtime_status->aliases.empty()) {
        std::cout << "(empty)";
      } else {
        for (std::size_t index = 0; index < runtime_status->aliases.size(); ++index) {
          if (index > 0) {
            std::cout << ",";
          }
          std::cout << runtime_status->aliases[index];
        }
      }
      std::cout << " runtime_profile="
                << (runtime_status->active_runtime_profile.empty()
                        ? "(empty)"
                        : runtime_status->active_runtime_profile)
                << " inference_ready=" << (runtime_status->inference_ready ? "yes" : "no")
                << " gateway_ready=" << (runtime_status->gateway_ready ? "yes" : "no")
                << "\n";
    }
    if (gpu_telemetry.has_value()) {
      for (const auto& device : gpu_telemetry->devices) {
        std::cout << "    gpu device=" << device.gpu_device
                  << " used_vram_mb=" << device.used_vram_mb
                  << "/" << device.total_vram_mb
                  << " free_vram_mb=" << device.free_vram_mb
                  << " util_pct=" << device.gpu_utilization_pct;
        if (!device.processes.empty()) {
          std::cout << " processes=";
          for (std::size_t index = 0; index < device.processes.size(); ++index) {
            if (index > 0) {
              std::cout << ",";
            }
            std::cout << device.processes[index].instance_name
                      << ":" << device.processes[index].pid
                      << ":" << device.processes[index].used_vram_mb << "MB";
          }
        }
        std::cout << "\n";
      }
    }
    if (disk_telemetry.has_value()) {
      for (const auto& disk : disk_telemetry->items) {
        std::cout << "    disk name=" << disk.disk_name
                  << " phase=" << (disk.runtime_state.empty() ? "(empty)" : disk.runtime_state)
                  << " mounted=" << (disk.mounted ? "yes" : "no")
                  << " health=" << (disk.health.empty() ? "(empty)" : disk.health)
                  << " used_bytes=" << disk.used_bytes
                  << " free_bytes=" << disk.free_bytes
                  << " read_bytes=" << disk.read_bytes
                  << " write_bytes=" << disk.write_bytes
                  << " read_ios=" << disk.read_ios
                  << " write_ios=" << disk.write_ios
                  << " io_time_ms=" << disk.io_time_ms
                  << " io_in_progress=" << disk.io_in_progress
                  << " fault_count=" << disk.fault_count
                  << " warning_count=" << disk.warning_count
                  << " perf_counters=" << (disk.perf_counters_available ? "yes" : "no")
                  << " io_error_counters=" << (disk.io_error_counters_available ? "yes" : "no")
                  << " read_only=" << (disk.read_only ? "yes" : "no");
        if (!disk.mount_point.empty()) {
          std::cout << " mount_point=" << disk.mount_point;
        }
        if (!disk.mount_source.empty()) {
          std::cout << " mount_source=" << disk.mount_source;
        }
        if (!disk.filesystem_type.empty()) {
          std::cout << " filesystem=" << disk.filesystem_type;
        }
        if (!disk.fault_reasons.empty()) {
          std::cout << " faults=";
          for (std::size_t index = 0; index < disk.fault_reasons.size(); ++index) {
            if (index > 0) {
              std::cout << ",";
            }
            std::cout << disk.fault_reasons[index];
          }
        }
        std::cout << "\n";
      }
    }
    if (network_telemetry.has_value()) {
      for (const auto& interface : network_telemetry->interfaces) {
        std::cout << "    net iface=" << interface.interface_name
                  << " oper_state=" << (interface.oper_state.empty() ? "(empty)" : interface.oper_state)
                  << " link_state=" << (interface.link_state.empty() ? "(empty)" : interface.link_state)
                  << " rx_bytes=" << interface.rx_bytes
                  << " tx_bytes=" << interface.tx_bytes
                  << " loopback=" << (interface.loopback ? "yes" : "no")
                  << "\n";
      }
    }
    if (cpu_telemetry.has_value()) {
      std::cout << "    cpu loadavg="
                << cpu_telemetry->loadavg_1m << ","
                << cpu_telemetry->loadavg_5m << ","
                << cpu_telemetry->loadavg_15m
                << " mem_used_bytes=" << cpu_telemetry->used_memory_bytes
                << " mem_total_bytes=" << cpu_telemetry->total_memory_bytes
                << " degraded=" << (cpu_telemetry->degraded ? "yes" : "no")
                << "\n";
    }
    if (!instance_statuses.empty()) {
      for (const auto& instance_status : instance_statuses) {
        std::cout << "    instance name=" << instance_status.instance_name
                  << " role=" << instance_status.instance_role
                  << " phase=" << instance_status.runtime_phase
                  << " ready=" << (instance_status.ready ? "yes" : "no")
                  << " pid=" << instance_status.runtime_pid
                  << " gpu=" << (instance_status.gpu_device.empty() ? "(empty)" : instance_status.gpu_device);
        if (!instance_status.model_path.empty()) {
          std::cout << " model_path=" << instance_status.model_path;
        }
        std::cout << "\n";
      }
    }
  }
}

void PrintHostHealth(
    const std::optional<comet::DesiredState>& desired_state,
    const std::vector<comet::HostObservation>& observations,
    const std::vector<comet::NodeAvailabilityOverride>& availability_overrides,
    const std::optional<std::string>& node_name,
    int stale_after_seconds) {
  std::map<std::string, comet::HostObservation> observation_by_node;
  for (const auto& observation : observations) {
    observation_by_node.emplace(observation.node_name, observation);
  }
  const auto availability_override_map =
      BuildAvailabilityOverrideMap(availability_overrides);

  std::vector<std::string> nodes;
  std::set<std::string> seen_nodes;
  if (desired_state.has_value()) {
    for (const auto& node : desired_state->nodes) {
      if (!node_name.has_value() || node.name == *node_name) {
        nodes.push_back(node.name);
        seen_nodes.insert(node.name);
      }
    }
  }
  for (const auto& [observed_node_name, observation] : observation_by_node) {
    (void)observation;
    if ((!node_name.has_value() || observed_node_name == *node_name) &&
        seen_nodes.find(observed_node_name) == seen_nodes.end()) {
      nodes.push_back(observed_node_name);
      seen_nodes.insert(observed_node_name);
    }
  }

  std::cout << "host-health:\n";
  if (nodes.empty()) {
    std::cout << "  (empty)\n";
    return;
  }

  int online_count = 0;
  int stale_count = 0;
  int unknown_count = 0;

  for (const auto& current_node_name : nodes) {
    const auto observation_it = observation_by_node.find(current_node_name);
    if (observation_it == observation_by_node.end()) {
      std::cout << "  - node=" << current_node_name
                << " availability="
                << comet::ToString(
                       ResolveNodeAvailability(availability_override_map, current_node_name))
                << " health=unknown status=(none)\n";
      ++unknown_count;
      continue;
    }

    const auto age_seconds = HeartbeatAgeSeconds(observation_it->second.heartbeat_at);
    const std::string health = HealthFromAge(age_seconds, stale_after_seconds);
    const auto runtime_status = ParseRuntimeStatus(observation_it->second);
    const auto gpu_telemetry = ParseGpuTelemetry(observation_it->second);
    if (health == "online") {
      ++online_count;
    } else if (health == "stale") {
      ++stale_count;
    } else {
      ++unknown_count;
    }

    std::cout << "  - node=" << current_node_name
              << " availability="
              << comet::ToString(
                     ResolveNodeAvailability(availability_override_map, current_node_name))
              << " health=" << health
              << " status=" << comet::ToString(observation_it->second.status);
    if (observation_it->second.applied_generation.has_value()) {
      std::cout << " applied_generation=" << *observation_it->second.applied_generation;
    }
    if (age_seconds.has_value()) {
      std::cout << " age_seconds=" << *age_seconds;
    }
    if (observation_it->second.last_assignment_id.has_value()) {
      std::cout << " last_assignment_id=" << *observation_it->second.last_assignment_id;
    }
    if (runtime_status.has_value()) {
      std::cout << " runtime_backend="
                << (runtime_status->runtime_backend.empty()
                        ? "(empty)"
                        : runtime_status->runtime_backend)
                << " runtime_phase="
                << (runtime_status->runtime_phase.empty()
                        ? "(empty)"
                        : runtime_status->runtime_phase)
                << " runtime_launch_ready="
                << (runtime_status->launch_ready ? "yes" : "no")
                << " runtime_model="
                << (runtime_status->active_model_id.empty()
                        ? "(empty)"
                        : runtime_status->active_model_id);
    }
    if (gpu_telemetry.has_value()) {
      std::cout << " telemetry="
                << (gpu_telemetry->degraded ? "degraded" : "ok")
                << ":" << (gpu_telemetry->source.empty() ? "unknown" : gpu_telemetry->source);
    }
    std::cout << "\n";
    if (!observation_it->second.status_message.empty()) {
      std::cout << "    message=" << observation_it->second.status_message << "\n";
    }
  }

  std::cout << "summary: online=" << online_count
            << " stale=" << stale_count
            << " unknown=" << unknown_count << "\n";
}

int PlanHostOps(
    const std::string& db_path,
    const std::string& bundle_dir,
    const std::string& artifacts_root,
    const std::optional<std::string>& node_name) {
  comet::ControllerStore store(db_path);
  store.Initialize();
  const auto current_state = store.LoadDesiredState();
  const comet::DesiredState desired_state = comet::ImportPlaneBundle(bundle_dir);
  const auto host_plans =
      FilterNodeExecutionPlans(
          comet::BuildNodeExecutionPlans(current_state, desired_state, artifacts_root),
          node_name);

  std::cout << "host-op-plan:\n";
  std::cout << "  db=" << db_path << "\n";
  std::cout << "  bundle=" << bundle_dir << "\n";
  std::cout << "  artifacts_root=" << artifacts_root << "\n";
  std::cout << comet::RenderNodeExecutionPlans(host_plans);
  return 0;
}

int InitDb(const std::string& db_path) {
  comet::ControllerStore store(db_path);
  store.Initialize();
  std::cout << "initialized db: " << db_path << "\n";
  return 0;
}

int SeedDemo(const std::string& db_path) {
  comet::ControllerStore store(db_path);
  store.Initialize();
  const comet::DesiredState desired_state = comet::BuildDemoState();
  comet::RequireSchedulingPolicy(desired_state);
  const comet::SchedulingPolicyReport scheduling_report =
      comet::EvaluateSchedulingPolicy(desired_state);
  const auto availability_overrides = store.LoadNodeAvailabilityOverrides();
  const auto observations = store.LoadHostObservations();
  const int desired_generation =
      store.LoadDesiredGeneration(desired_state.plane_name).value_or(0) + 1;
  store.ReplaceDesiredState(desired_state, desired_generation, 0);
  store.ClearSchedulerPlaneRuntime(desired_state.plane_name);
  store.ReplaceRolloutActions(
      desired_state.plane_name, desired_generation, scheduling_report.rollout_actions);
  store.ReplaceHostAssignments(
      BuildHostAssignments(
          desired_state,
          DefaultArtifactsRoot(),
          desired_generation,
          availability_overrides,
          observations,
          scheduling_report));
  std::cout << "seeded demo state into: " << db_path << "\n";
  std::cout << "desired generation: " << desired_generation << "\n";
  PrintSchedulerDecisionSummary(desired_state);
  PrintRolloutGateSummary(scheduling_report);
  PrintAssignmentDispatchSummary(
      desired_state,
      BuildAvailabilityOverrideMap(availability_overrides),
      observations,
      DefaultStaleAfterSeconds());
  return 0;
}

int ImportBundle(const std::string& db_path, const std::string& bundle_dir) {
  comet::ControllerStore store(db_path);
  store.Initialize();
  const comet::DesiredState desired_state = comet::ImportPlaneBundle(bundle_dir);
  comet::RequireSchedulingPolicy(desired_state);
  const int desired_generation =
      store.LoadDesiredGeneration(desired_state.plane_name).value_or(0) + 1;
  store.ReplaceDesiredState(desired_state, desired_generation, 0);
  store.UpdatePlaneArtifactsRoot(desired_state.plane_name, DefaultArtifactsRoot());
  store.ClearSchedulerPlaneRuntime(desired_state.plane_name);
  store.ReplaceRolloutActions(desired_state.plane_name, desired_generation, {});
  AppendControllerEvent(
      store,
      "bundle",
      "imported",
      "imported bundle into desired state; rollout is staged until explicit start",
      json{
          {"bundle_dir", bundle_dir},
          {"desired_generation", desired_generation},
          {"worker_count", desired_state.instances.size()},
          {"disk_count", desired_state.disks.size()},
      },
      desired_state.plane_name);
  std::cout << "imported bundle '" << bundle_dir << "' into: " << db_path << "\n";
  std::cout << "desired generation: " << desired_generation << "\n";
  std::cout << "runtime rollout is staged; use start-plane to enqueue host assignments\n";
  return 0;
}

int ApplyBundle(
    const std::string& db_path,
    const std::string& bundle_dir,
    const std::string& artifacts_root) {
  comet::ControllerStore store(db_path);
  store.Initialize();
  const comet::DesiredState desired_state = comet::ImportPlaneBundle(bundle_dir);
  const auto current_state = store.LoadDesiredState(desired_state.plane_name);
  comet::RequireSchedulingPolicy(desired_state);
  const int desired_generation =
      store.LoadDesiredGeneration(desired_state.plane_name).value_or(0) + 1;
  const comet::ReconcilePlan plan =
      comet::BuildReconcilePlan(current_state, desired_state);
  const comet::SchedulingPolicyReport scheduling_report =
      comet::EvaluateSchedulingPolicy(desired_state);
  const auto host_plans =
      comet::BuildNodeExecutionPlans(current_state, desired_state, artifacts_root);

  std::cout << "apply-plan:\n";
  std::cout << comet::RenderReconcilePlan(plan);
  std::cout << comet::RenderSchedulingPolicyReport(scheduling_report);
  PrintSchedulerDecisionSummary(desired_state);
  PrintRolloutGateSummary(scheduling_report);
  std::cout << comet::RenderNodeExecutionPlans(host_plans);

  MaterializeComposeArtifacts(desired_state, host_plans);
  MaterializeInferRuntimeArtifact(desired_state, artifacts_root);
  store.ReplaceDesiredState(desired_state, desired_generation, 0);
  store.UpdatePlaneArtifactsRoot(desired_state.plane_name, artifacts_root);
  store.ClearSchedulerPlaneRuntime(desired_state.plane_name);
  store.ReplaceRolloutActions(desired_state.plane_name, desired_generation, {});
  AppendControllerEvent(
      store,
      "bundle",
      "applied",
      "applied bundle into desired state; rollout is staged until explicit start",
      json{
          {"bundle_dir", bundle_dir},
          {"artifacts_root", artifacts_root},
          {"desired_generation", desired_generation},
          {"worker_count", desired_state.instances.size()},
          {"disk_count", desired_state.disks.size()},
      },
      desired_state.plane_name);
  std::cout << "applied bundle '" << bundle_dir << "' into: " << db_path << "\n";
  std::cout << "desired generation: " << desired_generation << "\n";
  std::cout << "artifacts written under: " << artifacts_root << "\n";
  std::cout << "runtime rollout is staged; use start-plane to enqueue host assignments\n";
  return 0;
}

int ShowHostAssignments(
    const std::string& db_path,
    const std::optional<std::string>& node_name) {
  const auto view = LoadHostAssignmentsViewData(db_path, node_name);
  std::cout << "db: " << view.db_path << "\n";
  PrintHostAssignments(view.assignments);
  return 0;
}

int ShowHostObservations(
    const std::string& db_path,
    const std::optional<std::string>& plane_name,
    const std::optional<std::string>& node_name,
    int stale_after_seconds) {
  const auto view =
      LoadHostObservationsViewData(db_path, node_name, plane_name, stale_after_seconds);
  std::cout << "db: " << view.db_path << "\n";
  if (view.plane_name.has_value()) {
    std::cout << "plane: " << *view.plane_name << "\n";
  }
  std::cout << "stale_after_seconds: " << view.stale_after_seconds << "\n";
  PrintHostObservations(view.observations, view.stale_after_seconds);
  return 0;
}

int ShowNodeAvailability(
    const std::string& db_path,
    const std::optional<std::string>& node_name) {
  comet::ControllerStore store(db_path);
  store.Initialize();

  std::cout << "db: " << db_path << "\n";
  PrintNodeAvailabilityOverrides(store.LoadNodeAvailabilityOverrides(node_name));
  return 0;
}

int ShowHostHealth(
    const std::string& db_path,
    const std::optional<std::string>& node_name,
    int stale_after_seconds) {
  const auto view = LoadHostHealthViewData(db_path, node_name, stale_after_seconds);
  std::cout << "db: " << view.db_path << "\n";
  std::cout << "stale_after_seconds: " << view.stale_after_seconds << "\n";
  PrintHostHealth(
      view.desired_state,
      view.observations,
      view.availability_overrides,
      view.node_name,
      view.stale_after_seconds);
  return 0;
}

int ShowRolloutActions(
    const std::string& db_path,
    const std::optional<std::string>& node_name,
    const std::optional<std::string>& plane_name) {
  const auto view = LoadRolloutActionsViewData(db_path, node_name, plane_name);

  std::cout << "db: " << view.db_path << "\n";
  if (view.desired_generation.has_value()) {
    std::cout << "desired generation: " << *view.desired_generation << "\n";
  }
  if (!view.actions.empty()) {
    std::cout << "rollout-gates:\n";
    std::cout << "  gated_workers=" << view.gated_worker_count
              << " gated_nodes=" << view.gated_node_count
              << " deferred_actions=" << view.actions.size() << "\n";
  }
  PrintPersistedRolloutActions(view.actions);
  if (view.scheduler_runtime.has_value()) {
    PrintSchedulerRuntimeView(*view.scheduler_runtime);
  }
  if (!view.lifecycle.empty()) {
    PrintRolloutLifecycleEntries(view.lifecycle);
  }
  return 0;
}

int ShowRebalancePlan(
    const std::string& db_path,
    const std::optional<std::string>& node_name,
    const std::optional<std::string>& plane_name) {
  const auto view = LoadRebalancePlanViewData(
      db_path,
      node_name,
      DefaultStaleAfterSeconds(),
      plane_name);
  if (!view.desired_state.has_value()) {
    std::cout << "rebalance-plan:\n  (empty)\n";
    return 0;
  }

  std::cout << "db: " << view.db_path << "\n";
  std::cout << "desired generation: " << view.desired_generation << "\n";
  PrintRebalanceControllerGateSummary(view.controller_gate_summary);
  PrintRebalanceIterationBudgetSummary(view.iteration_budget_summary);
  PrintRebalanceLoopStatusSummary(view.loop_status);
  PrintRebalancePlanEntries(view.rebalance_entries);
  PrintRebalancePolicySummary(view.policy_summary);
  PrintSchedulerRuntimeView(view.scheduler_runtime);
  return 0;
}

void PrintEvents(const std::vector<comet::EventRecord>& events) {
  std::cout << "events:\n";
  if (events.empty()) {
    std::cout << "  (empty)\n";
    return;
  }
  for (const auto& event : events) {
    std::cout << "  - id=" << event.id
              << " category=" << event.category
              << " type=" << event.event_type
              << " severity=" << event.severity;
    if (!event.plane_name.empty()) {
      std::cout << " plane=" << event.plane_name;
    }
    if (!event.node_name.empty()) {
      std::cout << " node=" << event.node_name;
    }
    if (!event.worker_name.empty()) {
      std::cout << " worker=" << event.worker_name;
    }
    if (event.assignment_id.has_value()) {
      std::cout << " assignment_id=" << *event.assignment_id;
    }
    if (event.rollout_action_id.has_value()) {
      std::cout << " rollout_action_id=" << *event.rollout_action_id;
    }
    std::cout << " at=" << FormatDisplayTimestamp(event.created_at)
              << " message="
              << (event.message.empty() ? "(empty)" : event.message)
              << "\n";
  }
}

int ShowEvents(
    const std::string& db_path,
    const std::optional<std::string>& plane_name,
    const std::optional<std::string>& node_name,
    const std::optional<std::string>& worker_name,
    const std::optional<std::string>& category,
    int limit) {
  const auto view =
      LoadEventsViewData(db_path, plane_name, node_name, worker_name, category, limit);
  std::cout << "db: " << view.db_path << "\n";
  std::cout << "limit: " << view.limit << "\n";
  if (view.plane_name.has_value()) {
    std::cout << "plane: " << *view.plane_name << "\n";
  }
  if (view.node_name.has_value()) {
    std::cout << "node: " << *view.node_name << "\n";
  }
  if (view.worker_name.has_value()) {
    std::cout << "worker: " << *view.worker_name << "\n";
  }
  if (view.category.has_value()) {
    std::cout << "category: " << *view.category << "\n";
  }
  PrintEvents(view.events);
  return 0;
}

int SetRolloutActionStatus(
    const std::string& db_path,
    int action_id,
    comet::RolloutActionStatus status,
    const std::optional<std::string>& status_message) {
  comet::ControllerStore store(db_path);
  store.Initialize();
  if (!store.UpdateRolloutActionStatus(action_id, status, status_message.value_or(""))) {
    throw std::runtime_error(
        "rollout action id=" + std::to_string(action_id) + " not found");
  }
  const auto updated_action = FindRolloutActionById(store.LoadRolloutActions(), action_id);
  if (updated_action.has_value()) {
    AppendControllerEvent(
        store,
        "rollout-action",
        "status-updated",
        "updated rollout action status",
        json{
            {"status", comet::ToString(status)},
            {"status_message", status_message.value_or("")},
            {"action", updated_action->action},
            {"step", updated_action->step},
        },
        updated_action->plane_name,
        updated_action->target_node_name,
        updated_action->worker_name,
        std::nullopt,
        action_id);
  }
  std::cout << "updated rollout action id=" << action_id
            << " status=" << comet::ToString(status) << "\n";
  if (updated_action.has_value()) {
    PrintPersistedRolloutActions(store.LoadRolloutActions(updated_action->plane_name));
  } else {
    PrintPersistedRolloutActions(store.LoadRolloutActions());
  }
  return 0;
}

int EnqueueRolloutEviction(
    const std::string& db_path,
    int action_id) {
  comet::ControllerStore store(db_path);
  store.Initialize();

  const auto desired_state = store.LoadDesiredState();
  const auto desired_generation = store.LoadDesiredGeneration();
  if (!desired_state.has_value() || !desired_generation.has_value()) {
    throw std::runtime_error("no desired state found in controller db");
  }

  const auto rollout_actions = store.LoadRolloutActions(desired_state->plane_name);
  const auto action = FindRolloutActionById(rollout_actions, action_id);
  if (!action.has_value()) {
    throw std::runtime_error(
        "rollout action id=" + std::to_string(action_id) + " not found");
  }
  if (action->desired_generation != *desired_generation) {
    throw std::runtime_error(
        "rollout action id=" + std::to_string(action_id) +
        " does not belong to current desired generation " +
        std::to_string(*desired_generation));
  }
  if (action->action != "evict-best-effort") {
    throw std::runtime_error(
        "rollout action id=" + std::to_string(action_id) +
        " is not an evict-best-effort action");
  }
  if (action->status != comet::RolloutActionStatus::Pending &&
      action->status != comet::RolloutActionStatus::Acknowledged) {
    throw std::runtime_error(
        "rollout action id=" + std::to_string(action_id) +
        " cannot enqueue eviction from status=" +
        comet::ToString(action->status));
  }

  const auto existing_assignments = store.LoadHostAssignments();
  const auto eviction_assignments = BuildEvictionAssignmentsForAction(
      *desired_state,
      *desired_generation,
      *action,
      existing_assignments);
  store.EnqueueHostAssignments(
      eviction_assignments,
      "superseded by rollout eviction action id=" + std::to_string(action_id));

  std::set<std::string> node_names;
  for (const auto& assignment : eviction_assignments) {
    node_names.insert(assignment.node_name);
  }
  std::ostringstream message;
  message << "eviction assignments enqueued on nodes ";
  bool first = true;
  for (const auto& node_name : node_names) {
    if (!first) {
      message << ",";
    }
    first = false;
    message << node_name;
  }
  store.UpdateRolloutActionStatus(
      action_id,
      comet::RolloutActionStatus::Acknowledged,
      message.str());
  AppendControllerEvent(
      store,
      "rollout-action",
      "eviction-enqueued",
      message.str(),
      json{
          {"victims", action->victim_worker_names},
          {"target_node", action->target_node_name},
          {"target_gpu", action->target_gpu_device},
      },
      desired_state->plane_name,
      action->target_node_name,
      action->worker_name,
      std::nullopt,
      action_id);

  std::cout << "enqueued rollout eviction action id=" << action_id << "\n";
  PrintPersistedRolloutActions(store.LoadRolloutActions(desired_state->plane_name));
  for (const auto& node_name : node_names) {
    PrintHostAssignments(store.LoadHostAssignments(node_name));
  }
  return 0;
}

int ApplyReadyRolloutAction(
    const std::string& db_path,
    int action_id,
    const std::string& artifacts_root) {
  comet::ControllerStore store(db_path);
  store.Initialize();

  const auto desired_state = store.LoadDesiredState();
  const auto desired_generation = store.LoadDesiredGeneration();
  if (!desired_state.has_value() || !desired_generation.has_value()) {
    throw std::runtime_error("no desired state found in controller db");
  }

  const auto rollout_actions = store.LoadRolloutActions(desired_state->plane_name);
  const auto action = FindRolloutActionById(rollout_actions, action_id);
  if (!action.has_value()) {
    throw std::runtime_error(
        "rollout action id=" + std::to_string(action_id) + " not found");
  }
  if (action->status != comet::RolloutActionStatus::ReadyToRetry) {
    throw std::runtime_error(
        "rollout action id=" + std::to_string(action_id) +
        " is not ready-to-retry; current status=" +
        comet::ToString(action->status));
  }
  if (action->action != "retry-placement") {
    throw std::runtime_error(
        "rollout action id=" + std::to_string(action_id) +
        " is not a retry-placement action");
  }

  std::vector<std::string> victim_worker_names;
  for (const auto& candidate_action : rollout_actions) {
    if (candidate_action.desired_generation != action->desired_generation ||
        candidate_action.worker_name != action->worker_name ||
        candidate_action.step >= action->step) {
      continue;
    }
    if (candidate_action.status != comet::RolloutActionStatus::ReadyToRetry) {
      throw std::runtime_error(
          "prior rollout step id=" + std::to_string(candidate_action.id) +
          " is not ready-to-retry");
    }
    if (candidate_action.action == "evict-best-effort") {
      victim_worker_names = candidate_action.victim_worker_names;
    }
  }

  comet::DesiredState updated_state = *desired_state;
  MaterializeRetryPlacementAction(&updated_state, *action, victim_worker_names);
  comet::RequireSchedulingPolicy(updated_state);
  const comet::SchedulingPolicyReport scheduling_report =
      comet::EvaluateSchedulingPolicy(updated_state);
  const int next_generation = *desired_generation + 1;
  const auto availability_overrides = store.LoadNodeAvailabilityOverrides();
  const auto observations = store.LoadHostObservations();

  store.ReplaceDesiredState(updated_state, next_generation, 0);
  store.ClearSchedulerPlaneRuntime(updated_state.plane_name);
  store.ReplaceRolloutActions(
      updated_state.plane_name, next_generation, scheduling_report.rollout_actions);
  store.ReplaceHostAssignments(
      BuildHostAssignments(
          updated_state,
          artifacts_root,
          next_generation,
          availability_overrides,
          observations,
          scheduling_report));
  AppendControllerEvent(
      store,
      "rollout-action",
      "retry-placement-applied",
      "materialized ready rollout action",
      json{
          {"desired_generation", next_generation},
          {"target_node", action->target_node_name},
          {"target_gpu", action->target_gpu_device},
          {"victims", victim_worker_names},
      },
      updated_state.plane_name,
      action->target_node_name,
      action->worker_name,
      std::nullopt,
      action_id);

  std::cout << "applied ready rollout action id=" << action_id << "\n";
  std::cout << "desired generation: " << next_generation << "\n";
  PrintStateSummary(updated_state);
  std::cout << comet::RenderSchedulingPolicyReport(scheduling_report);
  PrintSchedulerDecisionSummary(updated_state);
  PrintRolloutGateSummary(scheduling_report);
  return 0;
}

int ApplyRebalanceProposal(
    const std::string& db_path,
    const std::string& worker_name,
    const std::string& artifacts_root) {
  comet::ControllerStore store(db_path);
  store.Initialize();

  const auto desired_state = store.LoadDesiredState();
  const auto desired_generation = store.LoadDesiredGeneration();
  const auto rebalance_iteration = store.LoadRebalanceIteration();
  if (!desired_state.has_value() || !desired_generation.has_value()) {
    throw std::runtime_error("no desired state found in controller db");
  }

  const auto assignments = store.LoadHostAssignments();
  const auto observations = store.LoadHostObservations();
  const auto scheduling_report = comet::EvaluateSchedulingPolicy(*desired_state);
  const auto scheduler_runtime = LoadSchedulerRuntimeView(store, desired_state);
  const auto rollout_lifecycle =
      BuildRolloutLifecycleEntries(
          *desired_state,
          *desired_generation,
          store.LoadRolloutActions(desired_state->plane_name),
          assignments,
          observations);
  const auto rebalance_entries =
      BuildRebalancePlanEntries(
          *desired_state,
          scheduling_report,
          store.LoadNodeAvailabilityOverrides(),
          rollout_lifecycle,
          assignments,
          scheduler_runtime,
          observations,
          DefaultStaleAfterSeconds());

  const auto rebalance_it = std::find_if(
      rebalance_entries.begin(),
      rebalance_entries.end(),
      [&](const RebalancePlanEntry& entry) { return entry.worker_name == worker_name; });
  if (rebalance_it == rebalance_entries.end()) {
    throw std::runtime_error(
        "no rebalance plan entry found for worker '" + worker_name + "'");
  }
  if (rebalance_it->decision != "propose") {
    throw std::runtime_error(
        "worker '" + worker_name + "' is not actionable for rebalance; current decision=" +
        rebalance_it->decision + " state=" + rebalance_it->state);
  }
  const auto iteration_budget_summary =
      BuildRebalanceIterationBudgetSummary(rebalance_iteration.value_or(0));
  if (iteration_budget_summary.exhausted) {
    throw std::runtime_error(
        "rebalance iteration budget exhausted (" +
        std::to_string(iteration_budget_summary.current_iteration) + "/" +
        std::to_string(iteration_budget_summary.max_iterations) +
        "); apply a fresh bundle or rollout generation before materializing another direct rebalance");
  }

  comet::DesiredState updated_state = *desired_state;
  MaterializeRebalancePlanEntry(&updated_state, *rebalance_it);
  comet::RequireSchedulingPolicy(updated_state);
  const comet::SchedulingPolicyReport updated_scheduling_report =
      comet::EvaluateSchedulingPolicy(updated_state);
  const int next_generation = *desired_generation + 1;
  const auto availability_overrides = store.LoadNodeAvailabilityOverrides();
  const auto host_plans =
      comet::BuildNodeExecutionPlans(desired_state, updated_state, artifacts_root);

  MaterializeComposeArtifacts(updated_state, host_plans);
  MaterializeInferRuntimeArtifact(updated_state, artifacts_root);
  store.ReplaceDesiredState(
      updated_state,
      next_generation,
      rebalance_iteration.value_or(0) + 1);
  store.ReplaceRolloutActions(
      updated_state.plane_name,
      next_generation,
      updated_scheduling_report.rollout_actions);
  store.ReplaceHostAssignments(
      BuildHostAssignments(
          updated_state,
          artifacts_root,
          next_generation,
          availability_overrides,
          observations,
          updated_scheduling_report));
  comet::SchedulerPlaneRuntime plane_runtime;
  plane_runtime.plane_name = updated_state.plane_name;
  plane_runtime.active_action = "rebalance";
  plane_runtime.active_worker_name = rebalance_it->worker_name;
  plane_runtime.phase = "verifying-move";
  plane_runtime.action_generation = next_generation;
  plane_runtime.stable_samples = 0;
  plane_runtime.rollback_attempt_count = 0;
  plane_runtime.source_node_name = rebalance_it->current_node_name;
  plane_runtime.source_gpu_device = rebalance_it->current_gpu_device;
  plane_runtime.target_node_name = rebalance_it->target_node_name;
  plane_runtime.target_gpu_device = rebalance_it->target_gpu_device;
  plane_runtime.previous_state_json = comet::SerializeDesiredStateJson(*desired_state);
  plane_runtime.status_message = "awaiting post-move verification";
  store.UpsertSchedulerPlaneRuntime(plane_runtime);
  AppendControllerEvent(
      store,
      "scheduler",
      "rebalance-materialized",
      "materialized safe-direct rebalance proposal",
      json{
          {"desired_generation", next_generation},
          {"source_node", rebalance_it->current_node_name},
          {"source_gpu", rebalance_it->current_gpu_device},
          {"target_node", rebalance_it->target_node_name},
          {"target_gpu", rebalance_it->target_gpu_device},
          {"action", rebalance_it->action},
          {"score", rebalance_it->score},
      },
      updated_state.plane_name,
      rebalance_it->target_node_name,
      rebalance_it->worker_name);

  std::cout << "applied rebalance proposal for worker '" << worker_name << "'\n";
  std::cout << "desired generation: " << next_generation << "\n";
  std::cout << "target=" << rebalance_it->target_node_name << ":"
            << rebalance_it->target_gpu_device << "\n";
  PrintStateSummary(updated_state);
  std::cout << comet::RenderSchedulingPolicyReport(updated_scheduling_report);
  PrintSchedulerDecisionSummary(updated_state);
  PrintRolloutGateSummary(updated_scheduling_report);
  PrintAssignmentDispatchSummary(
      updated_state,
      BuildAvailabilityOverrideMap(availability_overrides),
      observations,
      DefaultStaleAfterSeconds());
  return 0;
}

int ReconcileRebalanceProposals(
    const std::string& db_path,
    const std::string& artifacts_root) {
  comet::ControllerStore store(db_path);
  store.Initialize();

  const auto desired_state = store.LoadDesiredState();
  const auto desired_generation = store.LoadDesiredGeneration();
  const auto rebalance_iteration = store.LoadRebalanceIteration();
  if (!desired_state.has_value() || !desired_generation.has_value()) {
    throw std::runtime_error("no desired state found in controller db");
  }

  const auto assignments = store.LoadHostAssignments();
  const auto observations = store.LoadHostObservations();
  const auto scheduling_report = comet::EvaluateSchedulingPolicy(*desired_state);
  const auto scheduler_runtime = LoadSchedulerRuntimeView(store, desired_state);
  const auto rollout_lifecycle =
      BuildRolloutLifecycleEntries(
          *desired_state,
          *desired_generation,
          store.LoadRolloutActions(desired_state->plane_name),
          assignments,
          observations);
  auto rebalance_entries =
      BuildRebalancePlanEntries(
          *desired_state,
          scheduling_report,
          store.LoadNodeAvailabilityOverrides(),
          rollout_lifecycle,
          assignments,
          scheduler_runtime,
          observations,
          DefaultStaleAfterSeconds());
  const auto controller_gate_summary =
      BuildRebalanceControllerGateSummary(
          *desired_state,
          *desired_generation,
          store.LoadNodeAvailabilityOverrides(),
          rollout_lifecycle,
          assignments,
          scheduler_runtime,
          observations,
          DefaultStaleAfterSeconds());
  const auto iteration_budget_summary =
      BuildRebalanceIterationBudgetSummary(rebalance_iteration.value_or(0));
  const auto rebalance_policy_summary =
      BuildRebalancePolicySummary(rebalance_entries);
  PrintRebalanceControllerGateSummary(controller_gate_summary);
  PrintRebalanceIterationBudgetSummary(iteration_budget_summary);
  PrintRebalanceLoopStatusSummary(
      BuildRebalanceLoopStatusSummary(
          controller_gate_summary,
          iteration_budget_summary,
          rebalance_policy_summary));

  if (!controller_gate_summary.cluster_ready) {
    std::cout << "rebalance proposals: blocked by controller gate\n";
    return 0;
  }

  rebalance_entries.erase(
      std::remove_if(
          rebalance_entries.begin(),
          rebalance_entries.end(),
          [](const RebalancePlanEntry& entry) {
            return entry.decision != "propose" || entry.rebalance_class != "safe-direct";
          }),
      rebalance_entries.end());

  if (rebalance_entries.empty()) {
    std::cout << "rebalance proposals: none actionable\n";
    return 0;
  }
  if (iteration_budget_summary.exhausted) {
    std::cout << "rebalance proposals: blocked by iteration budget\n";
    return 0;
  }

  std::sort(
      rebalance_entries.begin(),
      rebalance_entries.end(),
      [](const RebalancePlanEntry& left, const RebalancePlanEntry& right) {
        if (left.score != right.score) {
          return left.score > right.score;
        }
        return left.worker_name < right.worker_name;
      });

  std::cout << "selected rebalance proposal: worker=" << rebalance_entries.front().worker_name
            << " target=" << rebalance_entries.front().target_node_name << ":"
            << rebalance_entries.front().target_gpu_device
            << " score=" << rebalance_entries.front().score << "\n";
  return ApplyRebalanceProposal(
      db_path, rebalance_entries.front().worker_name, artifacts_root);
}

int AdvanceActiveSchedulerAction(
    const std::string& db_path,
    const std::string& artifacts_root) {
  comet::ControllerStore store(db_path);
  store.Initialize();

  const auto desired_state = store.LoadDesiredState();
  const auto desired_generation = store.LoadDesiredGeneration();
  if (!desired_state.has_value() || !desired_generation.has_value()) {
    std::cout << "scheduler active-action: no desired state\n";
    return 0;
  }

  const auto plane_runtime = store.LoadSchedulerPlaneRuntime(desired_state->plane_name);
  if (!plane_runtime.has_value() || plane_runtime->active_action.empty()) {
    std::cout << "scheduler active-action: none\n";
    return 0;
  }

  if (plane_runtime->phase == "rollback-planned") {
    if (plane_runtime->previous_state_json.empty()) {
      throw std::runtime_error(
          "rollback-planned action has no previous desired state payload");
    }
    const comet::DesiredState rollback_state =
        comet::DeserializeDesiredStateJson(plane_runtime->previous_state_json);
    comet::RequireSchedulingPolicy(rollback_state);
    const auto availability_overrides = store.LoadNodeAvailabilityOverrides();
    const auto observations = store.LoadHostObservations();
    const auto rollback_report = comet::EvaluateSchedulingPolicy(rollback_state);
    const int rollback_generation = *desired_generation + 1;
    store.ReplaceDesiredState(rollback_state, rollback_generation, 0);
    store.ReplaceRolloutActions(
        rollback_state.plane_name,
        rollback_generation,
        rollback_report.rollout_actions);
    store.ReplaceHostAssignments(
        BuildHostAssignments(
            rollback_state,
            artifacts_root,
            rollback_generation,
            availability_overrides,
            observations,
            rollback_report));
    comet::SchedulerPlaneRuntime updated_runtime = *plane_runtime;
    updated_runtime.phase = "rollback-applied";
    updated_runtime.action_generation = rollback_generation;
    updated_runtime.stable_samples = 0;
    updated_runtime.rollback_attempt_count = 1;
    updated_runtime.started_at = UtcNowSqlTimestamp();
    updated_runtime.status_message = "rollback materialized after verification timeout";
    store.UpsertSchedulerPlaneRuntime(updated_runtime);
    AppendControllerEvent(
        store,
        "scheduler",
        "rollback-applied",
        updated_runtime.status_message,
        json{
            {"worker", updated_runtime.active_worker_name},
            {"generation", rollback_generation},
            {"phase", updated_runtime.phase},
        },
        rollback_state.plane_name,
        updated_runtime.target_node_name,
        updated_runtime.active_worker_name);
    std::cout << "scheduler active-action: rollback-applied worker="
              << updated_runtime.active_worker_name
              << " generation=" << rollback_generation << "\n";
    return 0;
  }

  const auto observations = store.LoadHostObservations();
  const auto verification = EvaluateSchedulerActionVerification(*plane_runtime, observations);
  comet::SchedulerPlaneRuntime updated_runtime = *plane_runtime;
  updated_runtime.stable_samples = verification.next_stable_samples;
  updated_runtime.status_message = verification.detail;

  if (verification.stable) {
    MarkWorkerMoveVerified(&store, updated_runtime);
    AppendControllerEvent(
        store,
        "scheduler",
        "move-verified",
        verification.detail,
        json{
            {"worker", updated_runtime.active_worker_name},
            {"generation", updated_runtime.action_generation},
            {"phase", updated_runtime.phase},
            {"stable_samples", updated_runtime.stable_samples},
        },
        updated_runtime.plane_name,
        updated_runtime.target_node_name,
        updated_runtime.active_worker_name);
    store.ClearSchedulerPlaneRuntime(updated_runtime.plane_name);
    std::cout << "scheduler active-action: verified worker="
              << updated_runtime.active_worker_name
              << " phase=" << updated_runtime.phase << "\n";
    return 0;
  }

  if (!verification.timed_out) {
    store.UpsertSchedulerPlaneRuntime(updated_runtime);
    std::cout << "scheduler active-action: waiting worker="
              << updated_runtime.active_worker_name
              << " phase=" << updated_runtime.phase
              << " stable_samples=" << updated_runtime.stable_samples << "/"
              << VerificationStableSamplesRequired()
              << " detail=" << verification.detail << "\n";
    return 0;
  }

  if (updated_runtime.rollback_attempt_count == 0 &&
      !updated_runtime.previous_state_json.empty()) {
    comet::SchedulerWorkerRuntime worker_runtime;
    if (const auto current = store.LoadSchedulerWorkerRuntime(updated_runtime.active_worker_name);
        current.has_value()) {
      worker_runtime = *current;
    }
    worker_runtime.plane_name = updated_runtime.plane_name;
    worker_runtime.worker_name = updated_runtime.active_worker_name;
    worker_runtime.last_scheduler_phase = "failed-verification";
    worker_runtime.last_status_message = verification.detail;
    worker_runtime.manual_intervention_required = false;
    store.UpsertSchedulerWorkerRuntime(worker_runtime);
    updated_runtime.phase = "rollback-planned";
    updated_runtime.stable_samples = 0;
    updated_runtime.started_at = UtcNowSqlTimestamp();
    updated_runtime.status_message = "verification timed out; rollback planned";
    store.UpsertSchedulerPlaneRuntime(updated_runtime);
    AppendControllerEvent(
        store,
        "scheduler",
        "rollback-planned",
        verification.detail,
        json{
            {"worker", updated_runtime.active_worker_name},
            {"generation", updated_runtime.action_generation},
            {"phase", updated_runtime.phase},
        },
        updated_runtime.plane_name,
        updated_runtime.target_node_name,
        updated_runtime.active_worker_name,
        std::nullopt,
        std::nullopt,
        "warning");
    std::cout << "scheduler active-action: rollback-planned worker="
              << updated_runtime.active_worker_name
              << " generation=" << updated_runtime.action_generation << "\n";
    return 0;
  }

  comet::SchedulerWorkerRuntime worker_runtime;
  if (const auto current = store.LoadSchedulerWorkerRuntime(updated_runtime.active_worker_name);
      current.has_value()) {
    worker_runtime = *current;
  }
  worker_runtime.plane_name = updated_runtime.plane_name;
  worker_runtime.worker_name = updated_runtime.active_worker_name;
  worker_runtime.last_scheduler_phase = "manual-intervention-required";
  worker_runtime.last_status_message = verification.detail;
  worker_runtime.manual_intervention_required = true;
  store.UpsertSchedulerWorkerRuntime(worker_runtime);
  AppendControllerEvent(
      store,
      "scheduler",
      "manual-intervention-required",
      verification.detail,
      json{
          {"worker", updated_runtime.active_worker_name},
          {"generation", updated_runtime.action_generation},
          {"phase", updated_runtime.phase},
      },
      updated_runtime.plane_name,
      updated_runtime.target_node_name,
      updated_runtime.active_worker_name,
      std::nullopt,
      std::nullopt,
      "error");
  store.ClearSchedulerPlaneRuntime(updated_runtime.plane_name);
  std::cout << "scheduler active-action: manual-intervention-required worker="
            << updated_runtime.active_worker_name
            << " detail=" << verification.detail << "\n";
  return 0;
}

int SchedulerTick(
    const std::string& db_path,
    const std::string& artifacts_root) {
  comet::ControllerStore store(db_path);
  store.Initialize();

  const auto desired_state = store.LoadDesiredState();
  const auto desired_generation = store.LoadDesiredGeneration();
  if (!desired_state.has_value() || !desired_generation.has_value()) {
    std::cout << "scheduler-tick: no desired state\n";
    return 0;
  }

  if (const auto plane_runtime = store.LoadSchedulerPlaneRuntime(desired_state->plane_name);
      plane_runtime.has_value() && !plane_runtime->active_action.empty()) {
    std::cout << "scheduler-tick: step=active-scheduler-action\n";
    return AdvanceActiveSchedulerAction(db_path, artifacts_root);
  }

  const auto rollout_actions = store.LoadRolloutActions(desired_state->plane_name);
  bool has_active_rollout = false;
  for (const auto& action : rollout_actions) {
    if (action.desired_generation == *desired_generation &&
        action.status != comet::RolloutActionStatus::ReadyToRetry) {
      has_active_rollout = true;
      break;
    }
  }
  if (!rollout_actions.empty()) {
    std::cout << "scheduler-tick: step=rollout-reconcile\n";
    return ReconcileRolloutActions(db_path, artifacts_root);
  }

  std::cout << "scheduler-tick: step=rebalance-reconcile\n";
  if (has_active_rollout) {
    std::cout << "scheduler-tick: rollout still active\n";
    return 0;
  }
  return ReconcileRebalanceProposals(db_path, artifacts_root);
}

int ReconcileRolloutActions(
    const std::string& db_path,
    const std::string& artifacts_root) {
  comet::ControllerStore store(db_path);
  store.Initialize();

  const auto desired_state = store.LoadDesiredState();
  const auto desired_generation = store.LoadDesiredGeneration();
  if (!desired_state.has_value() || !desired_generation.has_value()) {
    throw std::runtime_error("no desired state found in controller db");
  }

  const auto all_rollout_actions = store.LoadRolloutActions(desired_state->plane_name);
  std::vector<comet::RolloutActionRecord> rollout_actions;
  for (const auto& action : all_rollout_actions) {
    if (action.desired_generation == *desired_generation) {
      rollout_actions.push_back(action);
    }
  }

  std::cout << "db: " << db_path << "\n";
  std::cout << "desired generation: " << *desired_generation << "\n";
  if (rollout_actions.empty()) {
    std::cout << "rollout reconcile: no rollout actions for current generation\n";
    return 0;
  }

  bool changed = false;
  for (const auto& action : rollout_actions) {
    if (action.action == "evict-best-effort") {
      if (action.status == comet::RolloutActionStatus::Pending) {
        const auto existing_assignments = store.LoadHostAssignments();
        const auto eviction_assignments = BuildEvictionAssignmentsForAction(
            *desired_state,
            *desired_generation,
            action,
            existing_assignments);
        store.EnqueueHostAssignments(
            eviction_assignments,
            "superseded by rollout eviction action id=" + std::to_string(action.id));
        store.UpdateRolloutActionStatus(
            action.id,
            comet::RolloutActionStatus::Acknowledged,
            "controller-managed eviction assignments enqueued");
        std::cout << "rollout reconcile: enqueued eviction action id=" << action.id << "\n";
        changed = true;
        continue;
      }

      if (action.status == comet::RolloutActionStatus::Acknowledged &&
          AreRolloutEvictionAssignmentsApplied(store.LoadHostAssignments(), action.id)) {
        store.UpdateRolloutActionStatus(
            action.id,
            comet::RolloutActionStatus::ReadyToRetry,
            "eviction assignments applied");
        MarkWorkersEvicted(&store, desired_state->plane_name, action.victim_worker_names);
        std::cout << "rollout reconcile: eviction action id=" << action.id
                  << " is ready-to-retry\n";
        changed = true;
      }
      continue;
    }

    if (action.action != "retry-placement") {
      continue;
    }

    auto current_action = FindRolloutActionById(
        store.LoadRolloutActions(desired_state->plane_name), action.id);
    if (!current_action.has_value()) {
      continue;
    }

    const auto prior_evict_action = FindPriorRolloutActionForWorker(
        store.LoadRolloutActions(desired_state->plane_name),
        *current_action,
        "evict-best-effort");
    if (current_action->status == comet::RolloutActionStatus::Pending &&
        prior_evict_action.has_value() &&
        prior_evict_action->status == comet::RolloutActionStatus::ReadyToRetry) {
      store.UpdateRolloutActionStatus(
          current_action->id,
          comet::RolloutActionStatus::ReadyToRetry,
          "preceding eviction completed");
      std::cout << "rollout reconcile: retry action id=" << current_action->id
                << " is ready-to-retry\n";
      changed = true;
      current_action = FindRolloutActionById(
          store.LoadRolloutActions(desired_state->plane_name), action.id);
    }

    if (current_action.has_value() &&
        current_action->status == comet::RolloutActionStatus::ReadyToRetry) {
      std::cout << "rollout reconcile: materializing retry action id="
                << current_action->id << "\n";
      return ApplyReadyRolloutAction(db_path, current_action->id, artifacts_root);
    }
  }

  if (!changed) {
    std::cout << "rollout reconcile: no state changes\n";
  }
  PrintPersistedRolloutActions(store.LoadRolloutActions(desired_state->plane_name));
  if (const auto state = store.LoadDesiredState(); state.has_value()) {
    if (const auto generation = store.LoadDesiredGeneration(); generation.has_value()) {
      PrintRolloutLifecycleEntries(
          BuildRolloutLifecycleEntries(
              *state,
              *generation,
              store.LoadRolloutActions(state->plane_name),
              store.LoadHostAssignments(),
              store.LoadHostObservations()));
    }
  }
  return 0;
}

int SetNodeAvailability(
    const std::string& db_path,
    const std::string& node_name,
    comet::NodeAvailability availability,
    const std::optional<std::string>& status_message) {
  comet::ControllerStore store(db_path);
  store.Initialize();
  const auto previous_override = store.LoadNodeAvailabilityOverride(node_name);
  const auto previous_availability =
      previous_override.has_value() ? previous_override->availability
                                    : comet::NodeAvailability::Active;

  comet::NodeAvailabilityOverride availability_override;
  availability_override.node_name = node_name;
  availability_override.availability = availability;
  availability_override.status_message = status_message.value_or("");
  store.UpsertNodeAvailabilityOverride(availability_override);
  AppendControllerEvent(
      store,
      "node-availability",
      "updated",
      "updated node availability override",
      json{
          {"availability", comet::ToString(availability)},
          {"previous_availability", comet::ToString(previous_availability)},
          {"status_message", status_message.value_or("")},
      },
      "",
      node_name);

  std::cout << "updated node availability for " << node_name << "\n";
  PrintNodeAvailabilityOverrides(store.LoadNodeAvailabilityOverrides(node_name));

  const auto desired_states = store.LoadDesiredStates();
  if (!desired_states.empty()) {
    const auto existing_assignments = store.LoadHostAssignments();
    const auto node_observation = store.LoadHostObservation(node_name);
    if (previous_availability == comet::NodeAvailability::Active &&
        availability != comet::NodeAvailability::Active) {
      std::vector<comet::HostAssignment> drain_assignments;
      for (const auto& desired_state : desired_states) {
        const auto plane = store.LoadPlane(desired_state.plane_name);
        if (!plane.has_value() || plane->state == "stopped") {
          continue;
        }
        const auto desired_generation = store.LoadDesiredGeneration(desired_state.plane_name);
        if (!desired_generation.has_value()) {
          continue;
        }
        const auto drain_assignment = BuildDrainAssignmentForNode(
            desired_state,
            *desired_generation,
            node_name,
            existing_assignments);
        if (drain_assignment.has_value()) {
          drain_assignments.push_back(*drain_assignment);
        }
      }
      if (!drain_assignments.empty()) {
        store.EnqueueHostAssignments(
            drain_assignments,
            "superseded by node drain for availability transition");
        std::cout << "queued drain assignment for " << node_name
                  << " planes=" << drain_assignments.size() << "\n";
        PrintHostAssignments(store.LoadHostAssignments(node_name));
      }
    }

    if (previous_availability != comet::NodeAvailability::Active &&
        availability == comet::NodeAvailability::Active) {
      std::vector<comet::HostAssignment> resync_assignments;
      for (const auto& desired_state : desired_states) {
        const auto plane = store.LoadPlane(desired_state.plane_name);
        if (!plane.has_value() || plane->state == "stopped") {
          continue;
        }
        const auto desired_generation = store.LoadDesiredGeneration(desired_state.plane_name);
        if (!desired_generation.has_value()) {
          continue;
        }
        const auto resync_assignment = BuildResyncAssignmentForNode(
            desired_state,
            *desired_generation,
            node_name,
            existing_assignments,
            node_observation);
        if (resync_assignment.has_value()) {
          resync_assignments.push_back(*resync_assignment);
        }
      }
      if (!resync_assignments.empty()) {
        store.EnqueueHostAssignments(
            resync_assignments,
            "superseded by node reactivation for availability transition");
        std::cout << "queued resync assignment for " << node_name
                  << " planes=" << resync_assignments.size() << "\n";
        PrintHostAssignments(store.LoadHostAssignments(node_name));
      } else {
        std::cout << "no resync assignment needed for " << node_name << "\n";
      }
    }
  }
  return 0;
}

int RetryHostAssignment(const std::string& db_path, int assignment_id) {
  comet::ControllerStore store(db_path);
  store.Initialize();

  const auto assignment = store.LoadHostAssignment(assignment_id);
  if (!assignment.has_value()) {
    throw std::runtime_error(
        "host assignment id=" + std::to_string(assignment_id) + " not found");
  }
  if (assignment->status != comet::HostAssignmentStatus::Failed) {
    throw std::runtime_error(
        "host assignment id=" + std::to_string(assignment_id) +
        " is not failed; current status=" + comet::ToString(assignment->status));
  }

  const auto latest_generation = store.LoadDesiredGeneration();
  if (latest_generation.has_value() &&
      assignment->desired_generation != *latest_generation) {
    throw std::runtime_error(
        "host assignment id=" + std::to_string(assignment_id) +
        " belongs to stale desired generation " +
        std::to_string(assignment->desired_generation) +
        "; latest generation is " + std::to_string(*latest_generation));
  }

  if (!store.RetryFailedHostAssignment(
          assignment_id,
          "requeued by operator for desired generation " +
              std::to_string(assignment->desired_generation))) {
    throw std::runtime_error(
        "failed to requeue host assignment id=" + std::to_string(assignment_id));
  }
  AppendControllerEvent(
      store,
      "host-assignment",
      "retried",
      "requeued failed host assignment",
      json{
          {"desired_generation", assignment->desired_generation},
          {"assignment_type", assignment->assignment_type},
          {"attempt_count", assignment->attempt_count},
      },
      assignment->plane_name,
      assignment->node_name,
      "",
      assignment_id);

  const auto updated_assignment = store.LoadHostAssignment(assignment_id);
  std::cout << "requeued host assignment id=" << assignment_id << "\n";
  if (updated_assignment.has_value()) {
    PrintHostAssignments({*updated_assignment});
  }
  return 0;
}

int ShowState(const std::string& db_path) {
  const auto view = LoadStateAggregateViewData(db_path, DefaultStaleAfterSeconds());
  if (!view.desired_state.has_value()) {
    std::cout << "state: empty\n";
    return 0;
  }

  std::cout << "db: " << view.db_path << "\n";
  if (view.desired_generation.has_value()) {
    std::cout << "desired generation: " << *view.desired_generation << "\n";
  }
  PrintStateSummary(*view.desired_state);
  PrintDiskRuntimeStates(view.disk_runtime_states);
  PrintDetailedDiskState(*view.desired_state, view.disk_runtime_states, view.observations);
  std::cout << comet::RenderSchedulingPolicyReport(view.scheduling_report);
  PrintSchedulerDecisionSummary(*view.desired_state);
  PrintRolloutGateSummary(view.scheduling_report);
  PrintRebalanceControllerGateSummary(view.controller_gate_summary);
  PrintRebalanceIterationBudgetSummary(view.iteration_budget_summary);
  PrintRebalanceLoopStatusSummary(view.loop_status);
  PrintRebalancePlanEntries(view.rebalance_entries);
  PrintRebalancePolicySummary(view.rebalance_policy_summary);
  PrintSchedulerRuntimeView(view.scheduler_runtime);
  if (view.desired_generation.has_value()) {
    PrintRolloutLifecycleEntries(view.rollout_lifecycle);
  }
  std::cout << "\n";
  PrintNodeAvailabilityOverrides(view.availability_overrides);
  std::cout << "\n";
  PrintHostObservations(view.observations, view.stale_after_seconds);
  std::cout << "\n";
  PrintHostHealth(
      view.desired_state,
      view.observations,
      view.availability_overrides,
      std::nullopt,
      view.stale_after_seconds);
  return 0;
}

int ShowDiskState(
    const std::string& db_path,
    const std::optional<std::string>& node_name,
    const std::optional<std::string>& plane_name) {
  const auto view = LoadDiskStateViewData(db_path, node_name, plane_name);
  if (!view.desired_state.has_value()) {
    std::cout << "disk-state:\n";
    std::cout << "  (empty)\n";
    return 0;
  }

  std::cout << "db: " << view.db_path << "\n";
  if (view.plane_name.has_value()) {
    std::cout << "plane_filter: " << *view.plane_name << "\n";
  }
  if (view.node_name.has_value()) {
    std::cout << "node_filter: " << *view.node_name << "\n";
  }
  PrintDetailedDiskState(*view.desired_state, view.runtime_states, view.observations, view.node_name);
  return 0;
}

int RenderCompose(const std::string& db_path, const std::optional<std::string>& node_name) {
  comet::ControllerStore store(db_path);
  store.Initialize();
  const auto state = store.LoadDesiredState();
  if (!state.has_value()) {
    std::cerr << "error: no desired state found in db '" << db_path << "'\n";
    return 1;
  }
  return RenderComposeForState(*state, node_name);
}

int RenderInferRuntime(const std::string& db_path) {
  comet::ControllerStore store(db_path);
  store.Initialize();
  const auto state = store.LoadDesiredState();
  if (!state.has_value()) {
    std::cerr << "error: no desired state found in db '" << db_path << "'\n";
    return 1;
  }
  std::cout << comet::RenderInferRuntimeConfigJson(*state) << "\n";
  return 0;
}

}  // namespace

class ControllerApp final {
 public:
  ControllerApp(int argc, char** argv) : cli_(argc, argv) {}

  int Run() {
    if (!cli_.HasCommand()) {
      cli_.PrintUsage(std::cout);
      return 1;
    }

    const std::string& command = cli_.command();
    if (command == "show-demo-plan") {
      ShowDemoPlan();
      return 0;
    }

    if (command == "render-demo-compose") {
      return RenderDemoCompose(cli_.node());
    }

    try {
      const auto db_arg = cli_.db();
      const auto controller_target = ResolveControllerTarget(cli_.controller(), db_arg);
      if (controller_target.has_value()) {
        return ExecuteRemoteControllerCommand(
            ParseControllerEndpointTarget(*controller_target),
            command,
            cli_);
      }

      const std::string db_path = ResolveDbPath(db_arg);
      AuthSupportService auth_support;
      HostRegistryService host_registry_service = MakeHostRegistryService(db_path);
      PlaneService plane_service = MakePlaneService(db_path);
      SchedulerService scheduler_service = MakeSchedulerService(
          db_path,
          ResolveArtifactsRoot(cli_.artifacts_root()));
      WebUiService web_ui_service(
          db_path,
          [](comet::ControllerStore& store,
             const std::string& event_type,
             const std::string& message,
             const json& payload) {
            AppendControllerEvent(store, "web-ui", event_type, message, payload);
          });
      ControllerCli controller_cli(
          cli_,
          host_registry_service,
          plane_service,
          scheduler_service,
          web_ui_service,
          [&]() { return InitDb(db_path); },
          [&]() { return SeedDemo(db_path); },
          [&]() { return ShowState(db_path); },
          [&](const std::optional<std::string>& node_name) {
            return ShowHostAssignments(db_path, node_name);
          },
          [&](const std::optional<std::string>& plane_name,
              const std::optional<std::string>& node_name,
              int stale_after_seconds) {
            return ShowHostObservations(db_path, plane_name, node_name, stale_after_seconds);
          },
          [&](const std::optional<std::string>& node_name, int stale_after_seconds) {
            return ShowHostHealth(db_path, node_name, stale_after_seconds);
          },
          [&](const std::optional<std::string>& node_name,
              const std::optional<std::string>& plane_name) {
            return ShowDiskState(db_path, node_name, plane_name);
          },
          [&](const std::string& bundle_dir) {
            return comet::controller::EmitControllerActionResult(ExecuteValidateBundleAction(bundle_dir));
          },
          [&](const std::string& bundle_dir, const std::optional<std::string>& node_name) {
            return comet::controller::EmitControllerActionResult(
                ExecutePreviewBundleAction(bundle_dir, node_name));
          },
          [&](const std::string& bundle_dir) { return PlanBundle(db_path, bundle_dir); },
          [&](const std::string& bundle_dir, const std::string& artifacts_root) {
            return comet::controller::EmitControllerActionResult(
                ExecuteApplyBundleAction(db_path, bundle_dir, artifacts_root));
          },
          [&](const std::string& state_path, const std::string& artifacts_root) {
            const auto desired_state = comet::LoadDesiredStateJson(state_path);
            if (!desired_state.has_value()) {
              throw std::runtime_error(
                  "failed to load desired state file '" + state_path + "'");
            }
            return ApplyDesiredState(
                db_path,
                *desired_state,
                artifacts_root,
                "state-file:" + state_path);
          },
          [&](const std::string& bundle_dir,
              const std::string& artifacts_root,
              const std::optional<std::string>& node_name) {
            return PlanHostOps(db_path, bundle_dir, artifacts_root, node_name);
          },
          [&](const std::optional<std::string>& node_name) {
            return ShowNodeAvailability(db_path, node_name);
          },
          [&](const std::string& node_name,
              const std::string& availability,
              const std::optional<std::string>& message) {
            return comet::controller::EmitControllerActionResult(
                ExecuteSetNodeAvailabilityAction(
                    db_path,
                    node_name,
                    comet::ParseNodeAvailability(availability),
                    message));
          },
          [&](int assignment_id) {
            return comet::controller::EmitControllerActionResult(
                ExecuteRetryHostAssignmentAction(db_path, assignment_id));
          },
          [&](const std::string& bundle_dir) {
            return comet::controller::EmitControllerActionResult(
                ExecuteImportBundleAction(db_path, bundle_dir));
          },
          [&](const std::optional<std::string>& node_name) {
            return RenderCompose(db_path, node_name);
          },
          [&]() { return RenderInferRuntime(db_path); },
          [&](const std::string& listen_host,
              int listen_port,
              const std::optional<std::string>& requested_ui_root,
              const std::string& artifacts_root) {
            std::optional<std::filesystem::path> ui_root;
            if (requested_ui_root.has_value()) {
              ui_root = std::filesystem::path(*requested_ui_root);
            } else {
              const std::filesystem::path default_ui_root = DefaultUiRoot();
              if (std::filesystem::exists(default_ui_root)) {
                ui_root = default_ui_root;
              }
            }
            auto interaction_http_service = MakeInteractionHttpService();
            auto auth_http_service = MakeAuthHttpService(auth_support);
            auto hostd_http_service = MakeHostdHttpService();
            auto bundle_http_service = MakeBundleHttpService();
            auto model_library_http_service = MakeModelLibraryHttpService();
            auto plane_http_service = MakePlaneHttpService();
            auto read_model_http_service = MakeReadModelHttpService();
            auto scheduler_http_service = MakeSchedulerHttpService();
            ControllerHttpRouter router(
                db_path,
                artifacts_root,
                ui_root,
                auth_support,
                {
                    &auth_http_service,
                    &hostd_http_service,
                    &bundle_http_service,
                    &model_library_http_service,
                    &plane_http_service,
                    &read_model_http_service,
                    &scheduler_http_service,
                    &interaction_http_service,
                },
                {
                    [&](int status_code,
                        const json& payload,
                        const std::map<std::string, std::string>& headers) {
                      return BuildJsonResponse(status_code, payload, headers);
                    },
                    [&](const std::string& health_db_path) {
                      return BuildControllerHealthPayload(health_db_path);
                    },
                    [&](const std::filesystem::path& root,
                        const std::string& request_path) {
                      return ResolveUiRequestPath(root, request_path);
                    },
                    [&](const std::filesystem::path& file_path) {
                      return BuildStaticFileResponse(file_path);
                    },
                    [&](const std::string& action_db_path, int assignment_id) {
                      return ExecuteRetryHostAssignmentAction(
                          action_db_path,
                          assignment_id);
                    },
                });
            ControllerHttpServer server({
                [&](const HttpRequest& request) {
                  const ScopedCurrentHttpRequest scoped_request(request);
                  return router.HandleRequest(request);
                },
                [&](SocketHandle client_fd,
                   const std::string& interaction_db_path,
                   const HttpRequest& request) {
                  interaction_http_service.StreamPlaneInteractionSse(
                      client_fd,
                      interaction_db_path,
                      request);
                },
                [](const std::string& method, const std::string& path) {
                  return comet::controller::ParseInteractionStreamPlaneName(
                      method,
                      path);
                },
                [](const comet::EventRecord& event) {
                  return BuildEventPayloadItem(event);
                },
            });
            return server.Serve({
                db_path,
                artifacts_root,
                listen_host,
                listen_port,
                ui_root,
                "/health,/api/v1/health,/api/v1/bundles/validate,/api/v1/bundles/preview,/api/v1/bundles/import,/api/v1/bundles/apply,/api/v1/model-library,/api/v1/model-library/download,/api/v1/planes,/api/v1/planes/<plane>,/api/v1/planes/<plane>/dashboard,/api/v1/planes/<plane>/start,/api/v1/planes/<plane>/stop,/api/v1/planes/<plane>[DELETE],/api/v1/planes/<plane>/interaction/status,/api/v1/planes/<plane>/interaction/models,/api/v1/planes/<plane>/interaction/chat/completions,/api/v1/planes/<plane>/interaction/chat/completions/stream,/api/v1/state,/api/v1/dashboard,/api/v1/host-assignments,/api/v1/host-observations,/api/v1/host-health,/api/v1/disk-state,/api/v1/rollout-actions,/api/v1/rebalance-plan,/api/v1/events,/api/v1/events/stream,/api/v1/scheduler-tick,/api/v1/reconcile-rebalance-proposals,/api/v1/reconcile-rollout-actions,/api/v1/apply-rebalance-proposal,/api/v1/set-rollout-action-status,/api/v1/enqueue-rollout-eviction,/api/v1/apply-ready-rollout-action,/api/v1/node-availability,/api/v1/retry-host-assignment,/api/v1/hostd/hosts,/api/v1/hostd/hosts/<node>/revoke,/api/v1/hostd/hosts/<node>/rotate-key",
            });
          },
          [&](const std::string& node_name, const std::optional<std::string>& status_message) {
            return comet::controller::EmitControllerActionResult(
                ExecuteRevokeHostdAction(db_path, node_name, status_message));
          },
          [&](const std::string& node_name,
              const std::string& public_key_base64,
              const std::optional<std::string>& status_message) {
            return comet::controller::EmitControllerActionResult(
                ExecuteRotateHostdKeyAction(
                    db_path,
                    node_name,
                    public_key_base64,
                    status_message));
          });

      if (const auto result = controller_cli.TryRun(); result.has_value()) {
        return *result;
      }

    } catch (const std::exception& error) {
      std::cerr << "error: " << error.what() << "\n";
      return 1;
    }

    cli_.PrintUsage(std::cout);
    return 1;
  }

 private:
  ControllerCommandLine cli_;
};

int main(int argc, char** argv) {
  ControllerApp app(argc, argv);
  return app.Run();
}
