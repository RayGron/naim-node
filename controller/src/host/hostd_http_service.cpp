#include "host/hostd_http_service.h"

#include <algorithm>
#include <cstdint>
#include <filesystem>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include "infra/controller_action.h"

#include "naim/security/crypto_utils.h"
#include "naim/state/models.h"
#include "naim/state/sqlite_store.h"

using nlohmann::json;

namespace {

constexpr std::uintmax_t kMaxModelArtifactChunkBytes = 4ULL * 1024ULL * 1024ULL;

bool StartsWithPathPrefix(const std::string& path, const std::string& prefix) {
  return path.rfind(prefix, 0) == 0;
}

struct HostInventorySummary {
  std::string storage_root;
  int gpu_count = 0;
  std::uint64_t total_memory_bytes = 0;
  std::uint64_t storage_total_bytes = 0;
  std::uint64_t storage_free_bytes = 0;
  bool has_storage_capacity = false;
};

std::optional<std::string> FindQueryStringValue(
    const HttpRequest& request,
    const std::string& key) {
  const auto it = request.query_params.find(key);
  if (it == request.query_params.end() || it->second.empty()) {
    return std::nullopt;
  }
  return it->second;
}

json ParseJsonBody(const HttpRequest& request) {
  if (request.body.empty()) {
    return json::object();
  }
  return json::parse(request.body);
}

std::string BuildHostRequestAad(
    const std::string& message_type,
    const std::string& node_name,
    std::int64_t sequence_number) {
  return "request\n" + message_type + "\n" + node_name + "\n" +
         std::to_string(sequence_number);
}

std::string BuildHostResponseAad(
    const std::string& message_type,
    const std::string& node_name,
    std::int64_t sequence_number) {
  return "response\n" + message_type + "\n" + node_name + "\n" +
         std::to_string(sequence_number);
}

json BuildAssignmentPayloadItem(const naim::HostAssignment& assignment) {
  json progress = nullptr;
  if (!assignment.progress_json.empty() && assignment.progress_json != "{}") {
    progress = json::parse(assignment.progress_json);
  }
  return json{
      {"id", assignment.id},
      {"node_name", assignment.node_name},
      {"plane_name", assignment.plane_name},
      {"desired_generation", assignment.desired_generation},
      {"attempt_count", assignment.attempt_count},
      {"max_attempts", assignment.max_attempts},
      {"assignment_type", assignment.assignment_type},
      {"desired_state_json", assignment.desired_state_json},
      {"artifacts_root", assignment.artifacts_root},
      {"status", naim::ToString(assignment.status)},
      {"status_message", assignment.status_message},
      {"progress", progress},
  };
}

naim::HostObservation ParseHostObservationPayload(const json& payload) {
  naim::HostObservation observation;
  observation.node_name = payload.value("node_name", std::string{});
  observation.plane_name = payload.value("plane_name", std::string{});
  if (payload.contains("applied_generation") &&
      !payload.at("applied_generation").is_null()) {
    observation.applied_generation = payload.at("applied_generation").get<int>();
  }
  if (payload.contains("last_assignment_id") &&
      !payload.at("last_assignment_id").is_null()) {
    observation.last_assignment_id = payload.at("last_assignment_id").get<int>();
  }
  observation.status = naim::ParseHostObservationStatus(
      payload.value("status", std::string("idle")));
  observation.status_message = payload.value("status_message", std::string{});
  observation.observed_state_json =
      payload.value("observed_state_json", std::string{});
  observation.runtime_status_json =
      payload.value("runtime_status_json", std::string{});
  observation.instance_runtime_json =
      payload.value("instance_runtime_json", std::string{});
  observation.gpu_telemetry_json =
      payload.value("gpu_telemetry_json", std::string{});
  observation.disk_telemetry_json =
      payload.value("disk_telemetry_json", std::string{});
  observation.network_telemetry_json =
      payload.value("network_telemetry_json", std::string{});
  observation.cpu_telemetry_json =
      payload.value("cpu_telemetry_json", std::string{});
  observation.heartbeat_at = payload.value("heartbeat_at", std::string{});
  return observation;
}

json ParseCapabilitiesJson(const std::string& capabilities_json) {
  if (capabilities_json.empty()) {
    return json::object();
  }
  const json parsed = json::parse(capabilities_json, nullptr, false);
  return parsed.is_discarded() ? json::object() : parsed;
}

bool IsUsableAbsoluteHostPath(const std::string& value) {
  return !value.empty() && value.front() == '/' &&
         value.rfind("/naim/", 0) != 0;
}

std::string NormalizePathString(const std::filesystem::path& path) {
  return path.lexically_normal().string();
}

bool PathBelongsToRoot(
    const std::filesystem::path& path,
    const std::filesystem::path& root) {
  const auto normalized_path = path.lexically_normal();
  const auto normalized_root = root.lexically_normal();
  const auto relative = normalized_path.lexically_relative(normalized_root);
  if (relative.empty()) {
    return true;
  }
  const auto relative_text = relative.string();
  return relative_text != ".." && relative_text.rfind("../", 0) != 0 &&
         relative_text.rfind("..\\", 0) != 0;
}

bool HostAllowsModelArtifactSource(const naim::RegisteredHostRecord& host) {
  return host.registration_state == "registered" &&
         host.session_state == "connected" &&
         (host.storage_role_enabled || host.derived_role == "storage" ||
          host.derived_role == "worker");
}

std::string HostStorageRoot(const naim::RegisteredHostRecord& host) {
  const json capabilities = ParseCapabilitiesJson(host.capabilities_json);
  if (capabilities.contains("storage_root") && capabilities["storage_root"].is_string()) {
    return capabilities["storage_root"].get<std::string>();
  }
  return {};
}

bool HostCanServeModelArtifactPath(
    const naim::RegisteredHostRecord& host,
    const std::string& source_path) {
  if (!HostAllowsModelArtifactSource(host) || !IsUsableAbsoluteHostPath(source_path)) {
    return false;
  }
  const std::string storage_root = HostStorageRoot(host);
  return IsUsableAbsoluteHostPath(storage_root) &&
         PathBelongsToRoot(source_path, storage_root);
}

std::optional<std::string> ResolveModelArtifactSourceNode(
    naim::ControllerStore& store,
    const std::string& requested_source_node_name,
    const std::string& source_path) {
  if (!IsUsableAbsoluteHostPath(source_path)) {
    return std::nullopt;
  }
  const std::string normalized_source =
      NormalizePathString(std::filesystem::path(source_path));
  if (!requested_source_node_name.empty()) {
    const auto host = store.LoadRegisteredHost(requested_source_node_name);
    if (host.has_value() && HostCanServeModelArtifactPath(*host, normalized_source)) {
      return host->node_name;
    }
    return std::nullopt;
  }

  for (const auto& job : store.LoadModelLibraryDownloadJobs("completed")) {
    if (job.node_name.empty()) {
      continue;
    }
    std::vector<std::string> paths = job.retained_output_paths;
    paths.insert(paths.end(), job.target_paths.begin(), job.target_paths.end());
    for (const auto& path : paths) {
      if (NormalizePathString(std::filesystem::path(path)) == normalized_source) {
        const auto host = store.LoadRegisteredHost(job.node_name);
        if (host.has_value() && HostCanServeModelArtifactPath(*host, normalized_source)) {
          return host->node_name;
        }
      }
    }
  }

  for (const auto& host : store.LoadRegisteredHosts()) {
    if (HostCanServeModelArtifactPath(host, normalized_source)) {
      return host.node_name;
    }
  }
  return std::nullopt;
}

std::vector<std::string> ParseRequestedSourcePaths(const json& body) {
  std::vector<std::string> source_paths;
  if (body.contains("source_paths") && body.at("source_paths").is_array()) {
    for (const auto& item : body.at("source_paths")) {
      if (!item.is_string()) {
        continue;
      }
      const std::string path = NormalizePathString(std::filesystem::path(item.get<std::string>()));
      if (!path.empty()) {
        source_paths.push_back(path);
      }
    }
  }
  if (source_paths.empty()) {
    const std::string source_path =
        NormalizePathString(std::filesystem::path(body.value("source_path", std::string{})));
    if (!source_path.empty()) {
      source_paths.push_back(source_path);
    }
  }
  std::sort(source_paths.begin(), source_paths.end());
  source_paths.erase(std::unique(source_paths.begin(), source_paths.end()), source_paths.end());
  return source_paths;
}

HostInventorySummary BuildInventorySummary(
    const naim::RegisteredHostRecord& host,
    const naim::HostObservation& observation) {
  HostInventorySummary summary;
  const json capabilities = ParseCapabilitiesJson(host.capabilities_json);
  if (capabilities.contains("storage_root") && capabilities["storage_root"].is_string()) {
    summary.storage_root = capabilities["storage_root"].get<std::string>();
  }
  if (!observation.gpu_telemetry_json.empty()) {
    summary.gpu_count = static_cast<int>(
        naim::DeserializeGpuTelemetryJson(observation.gpu_telemetry_json).devices.size());
  }
  if (!observation.cpu_telemetry_json.empty()) {
    summary.total_memory_bytes =
        naim::DeserializeCpuTelemetryJson(observation.cpu_telemetry_json).total_memory_bytes;
  }
  if (!observation.disk_telemetry_json.empty()) {
    const auto disks = naim::DeserializeDiskTelemetryJson(observation.disk_telemetry_json);
    for (const auto& item : disks.items) {
      if (item.disk_name == "storage-root" ||
          (!summary.storage_root.empty() && item.mount_point == summary.storage_root)) {
        summary.storage_total_bytes = item.total_bytes;
        summary.storage_free_bytes = item.free_bytes;
        summary.has_storage_capacity = item.total_bytes > 0;
        if (summary.storage_root.empty()) {
          summary.storage_root = item.mount_point;
        }
        break;
      }
    }
  }
  return summary;
}

constexpr std::uint64_t kStorageRoleMinDiskBytes =
    100ULL * 1024ULL * 1024ULL * 1024ULL;
constexpr std::uint64_t kWorkerMinRamBytes =
    32ULL * 1024ULL * 1024ULL * 1024ULL;

std::pair<std::string, std::string> DeriveRole(const HostInventorySummary& summary) {
  if (summary.gpu_count == 0 &&
      summary.storage_total_bytes > kStorageRoleMinDiskBytes) {
    return {"storage", "eligible: no gpu, disk > 100 GB"};
  }
  if (summary.gpu_count >= 1 &&
      summary.total_memory_bytes >= kWorkerMinRamBytes &&
      summary.storage_total_bytes > kStorageRoleMinDiskBytes) {
    return {"worker", "eligible: gpu >= 1, ram >= 32 GB, disk > 100 GB"};
  }
  if (summary.storage_total_bytes <= kStorageRoleMinDiskBytes) {
    return {"ineligible", "disk <= 100 GB"};
  }
  if (summary.gpu_count >= 1 && summary.total_memory_bytes < kWorkerMinRamBytes) {
    return {"ineligible", "gpu present but ram < 32 GB"};
  }
  return {"ineligible", "inventory does not match storage or worker role"};
}

json MergeCapabilities(
    const std::string& capabilities_json,
    const HostInventorySummary& summary) {
  json capabilities = ParseCapabilitiesJson(capabilities_json);
  if (!summary.storage_root.empty()) {
    capabilities["storage_root"] = summary.storage_root;
  }
  if (summary.total_memory_bytes > 0) {
    capabilities["total_memory_bytes"] = summary.total_memory_bytes;
  }
  capabilities["gpu_count"] = summary.gpu_count;
  if (summary.has_storage_capacity) {
    capabilities["storage_total_bytes"] = summary.storage_total_bytes;
    capabilities["storage_free_bytes"] = summary.storage_free_bytes;
  }
  return capabilities;
}

json BuildDiskRuntimeStatePayloadItem(const naim::DiskRuntimeState& state) {
  return json{
      {"disk_name", state.disk_name},
      {"plane_name", state.plane_name},
      {"node_name", state.node_name},
      {"image_path", state.image_path},
      {"filesystem_type", state.filesystem_type},
      {"loop_device", state.loop_device},
      {"mount_point", state.mount_point},
      {"runtime_state", state.runtime_state},
      {"attached_at", state.attached_at},
      {"mounted_at", state.mounted_at},
      {"last_verified_at", state.last_verified_at},
      {"status_message", state.status_message},
      {"updated_at", state.updated_at},
  };
}

naim::DiskRuntimeState ParseDiskRuntimeStatePayload(const json& payload) {
  naim::DiskRuntimeState state;
  state.disk_name = payload.value("disk_name", std::string{});
  state.plane_name = payload.value("plane_name", std::string{});
  state.node_name = payload.value("node_name", std::string{});
  state.image_path = payload.value("image_path", std::string{});
  state.filesystem_type = payload.value("filesystem_type", std::string{});
  state.loop_device = payload.value("loop_device", std::string{});
  state.mount_point = payload.value("mount_point", std::string{});
  state.runtime_state = payload.value("runtime_state", std::string{});
  state.attached_at = payload.value("attached_at", std::string{});
  state.mounted_at = payload.value("mounted_at", std::string{});
  state.last_verified_at = payload.value("last_verified_at", std::string{});
  state.status_message = payload.value("status_message", std::string{});
  return state;
}

std::map<std::string, naim::HostAssignment> BuildLatestPlaneAssignmentsByNode(
    const std::vector<naim::HostAssignment>& assignments) {
  std::map<std::string, naim::HostAssignment> latest;
  for (const auto& assignment : assignments) {
    auto it = latest.find(assignment.node_name);
    if (it == latest.end() || it->second.id < assignment.id) {
      latest[assignment.node_name] = assignment;
    }
  }
  return latest;
}

}  // namespace

HostdHttpService::HostdHttpService(HostdHttpSupport support)
    : support_(std::move(support)) {}

std::optional<HttpResponse> HostdHttpService::HandleRequest(
    const std::string& db_path,
    const HttpRequest& request) const {
  if (!StartsWithPathPrefix(request.path, "/api/v1/hostd/")) {
    return std::nullopt;
  }
  if (request.path == "/api/v1/hostd/register") {
    return HandleRegister(db_path, request);
  }
  if (request.path == "/api/v1/hostd/hosts") {
    return HandleHosts(db_path, request);
  }
  if (StartsWithPathPrefix(request.path, "/api/v1/hostd/hosts/")) {
    return HandleHostPath(db_path, request);
  }
  if (request.path == "/api/v1/hostd/session/open") {
    return HandleSessionOpen(db_path, request);
  }
  if (request.path == "/api/v1/hostd/session/heartbeat") {
    return HandleSessionHeartbeat(db_path, request);
  }
  if (request.path == "/api/v1/hostd/assignments/next") {
    return HandleNextAssignment(db_path, request);
  }
  if (StartsWithPathPrefix(request.path, "/api/v1/hostd/assignments/")) {
    return HandleAssignmentAction(db_path, request);
  }
  if (request.path == "/api/v1/hostd/model-artifacts/chunks/request") {
    return HandleModelArtifactChunkRequest(db_path, request);
  }
  if (request.path == "/api/v1/hostd/model-artifacts/chunks/poll") {
    return HandleModelArtifactChunkPoll(db_path, request);
  }
  if (request.path == "/api/v1/hostd/model-artifacts/manifest/request") {
    return HandleModelArtifactManifestRequest(db_path, request);
  }
  if (request.path == "/api/v1/hostd/model-artifacts/manifest/poll") {
    return HandleModelArtifactManifestPoll(db_path, request);
  }
  if (request.path == "/api/v1/hostd/observations") {
    return HandleObservations(db_path, request);
  }
  if (request.path == "/api/v1/hostd/events") {
    return HandleEvents(db_path, request);
  }
  if (request.path == "/api/v1/hostd/disk-runtime-state") {
    return HandleDiskRuntimeState(db_path, request);
  }
  if (request.path == "/api/v1/hostd/disk-runtime-state/load") {
    return HandleDiskRuntimeStateLoad(db_path, request);
  }
  return support_.build_json_response(404, json{{"status", "not_found"}}, {});
}

namespace {

class HostdRequestContext {
 public:
  HostdRequestContext(
      const HostdHttpSupport& support,
      const std::string& db_path)
      : support_(support), db_path_(db_path), store_(db_path) {
    store_.Initialize();
  }

  naim::ControllerStore& store() { return store_; }

  HttpResponse Json(
      int status_code,
      const json& payload,
      const std::map<std::string, std::string>& headers = {}) const {
    return support_.build_json_response(status_code, payload, headers);
  }

  std::optional<naim::RegisteredHostRecord> Authenticate(
      const HttpRequest& request,
      const std::optional<std::string>& expected_node_name = std::nullopt) {
    const auto token_it = request.headers.find("x-naim-host-session");
    if (token_it == request.headers.end() || token_it->second.empty()) {
      return std::nullopt;
    }
    const auto node_name_it = request.headers.find("x-naim-host-node");
    if (node_name_it == request.headers.end() || node_name_it->second.empty()) {
      return std::nullopt;
    }
    const auto host = store_.LoadRegisteredHost(node_name_it->second);
    if (!host.has_value()) {
      return std::nullopt;
    }
    if (expected_node_name.has_value() && *expected_node_name != host->node_name) {
      return std::nullopt;
    }
    if (host->registration_state != "registered") {
      return std::nullopt;
    }
    if (!host->session_expires_at.empty()) {
      const auto expires_age =
          support_.timestamp_age_seconds(host->session_expires_at);
      if (expires_age.has_value() && *expires_age >= 0) {
        return std::nullopt;
      }
    }
    if (host->session_token.empty() || host->session_token != token_it->second) {
      return std::nullopt;
    }
    return host;
  }

  json ParseEncryptedBody(
      const HttpRequest& request,
      naim::RegisteredHostRecord* host,
      const std::string& message_type) {
    const json body = ParseJsonBody(request);
    if (!body.value("encrypted", false)) {
      return body;
    }
    const std::int64_t sequence_number =
        body.value("sequence_number", static_cast<std::int64_t>(0));
    if (sequence_number <= host->session_host_sequence) {
      throw std::runtime_error("stale or replayed host session request");
    }
    const naim::EncryptedEnvelope envelope{
        body.value("nonce", std::string{}),
        body.value("ciphertext", std::string{}),
    };
    const std::string decrypted = naim::DecryptEnvelopeBase64(
        envelope,
        host->session_token,
        BuildHostRequestAad(message_type, host->node_name, sequence_number));
    host->session_host_sequence = sequence_number;
    host->session_expires_at = support_.sql_timestamp_after_seconds(600);
    store_.UpsertRegisteredHost(*host);
    if (decrypted.empty()) {
      return json::object();
    }
    return json::parse(decrypted);
  }

  HttpResponse EncryptedResponse(
      naim::RegisteredHostRecord* host,
      const std::string& message_type,
      const json& payload) {
    if (const auto latest = store_.LoadRegisteredHost(host->node_name);
        latest.has_value()) {
      *host = *latest;
    }
    host->session_controller_sequence += 1;
    store_.UpsertRegisteredHost(*host);
    const naim::EncryptedEnvelope envelope = naim::EncryptEnvelopeBase64(
        payload.dump(),
        host->session_token,
        BuildHostResponseAad(
            message_type, host->node_name, host->session_controller_sequence));
    return Json(
        200,
        json{
            {"encrypted", true},
            {"sequence_number", host->session_controller_sequence},
            {"nonce", envelope.nonce_base64},
            {"ciphertext", envelope.ciphertext_base64},
        });
  }

  naim::controller::HostRegistryService MakeHostRegistryService() const {
    return naim::controller::HostRegistryService(
        db_path_, support_.host_registry_event_sink());
  }

  void EmitHostRegistryEvent(
      const std::string& event_type,
      const std::string& message,
      const json& payload,
      const std::string& node_name,
      const std::string& severity) {
    support_.host_registry_event_sink()(
        store_, event_type, message, payload, node_name, severity);
  }

  const HostdHttpSupport& support() const { return support_; }

 private:
  const HostdHttpSupport& support_;
  std::string db_path_;
  naim::ControllerStore store_;
};

}  // namespace

HttpResponse HostdHttpService::HandleRegister(
    const std::string& db_path,
    const HttpRequest& request) const {
  if (request.method != "POST") {
    return support_.build_json_response(405, json{{"status", "method_not_allowed"}}, {});
  }
  try {
    const json body = ParseJsonBody(request);
    const std::string node_name = body.value("node_name", std::string{});
    if (node_name.empty()) {
      return support_.build_json_response(
          400,
          json{{"status", "bad_request"},
               {"message", "missing required field 'node_name'"}},
          {});
    }
    HostdRequestContext context(support_, db_path);
    auto current = context.store().LoadRegisteredHost(node_name);
    if (!current.has_value()) {
      return context.Json(
          404,
          json{{"status", "not_found"},
               {"message", "host node is not provisioned"}});
    }
    naim::RegisteredHostRecord host = *current;
    const std::string onboarding_key = body.value("onboarding_key", std::string{});
    if (host.onboarding_key_hash.empty()) {
      return context.Json(
          409,
          json{{"status", "conflict"},
               {"message", "host node does not accept onboarding registration"}});
    }
    if (onboarding_key.empty() ||
        naim::ComputeSha256Hex(onboarding_key) != host.onboarding_key_hash) {
      return context.Json(
          403,
          json{{"status", "forbidden"},
               {"message", "invalid onboarding key"}});
    }
    host.node_name = node_name;
    host.advertised_address =
        body.value("advertised_address", host.advertised_address);
    host.public_key_base64 =
        body.value("public_key_base64", host.public_key_base64);
    host.controller_public_key_fingerprint = body.value(
        "controller_public_key_fingerprint",
        host.controller_public_key_fingerprint);
    host.transport_mode = body.value(
        "transport_mode",
        host.transport_mode.empty() ? "out" : host.transport_mode);
    host.execution_mode = body.value(
        "execution_mode",
        host.execution_mode.empty() ? std::string("mixed") : host.execution_mode);
    naim::ParseHostExecutionMode(host.execution_mode);
    host.registration_state = body.value(
        "registration_state",
        std::string("registered"));
    host.onboarding_key_hash.clear();
    host.onboarding_state = "completed";
    host.session_state = body.value(
        "session_state",
        host.session_state.empty() ? "disconnected" : host.session_state);
    host.session_token.clear();
    host.session_expires_at.clear();
    host.capabilities_json = body.value("capabilities_json", std::string("{}"));
    host.status_message = body.value(
        "status_message", std::string("registered via host-agent API"));
    context.store().UpsertRegisteredHost(host);
    context.EmitHostRegistryEvent(
        "registered",
        "registered host node",
        json{{"transport_mode", host.transport_mode},
             {"execution_mode", host.execution_mode}},
        node_name,
        "info");
    return context.Json(
        200,
        json{{"service", "naim-controller"},
             {"node_name", node_name},
             {"registration_state", host.registration_state},
             {"controller_public_key_fingerprint",
              host.controller_public_key_fingerprint.empty()
                  ? json(nullptr)
                  : json(host.controller_public_key_fingerprint)}});
  } catch (const std::exception& error) {
    return support_.build_json_response(
        500,
        json{{"status", "internal_error"},
             {"message", error.what()},
             {"path", request.path}},
        {});
  }
}

HttpResponse HostdHttpService::HandleHosts(
    const std::string& db_path,
    const HttpRequest& request) const {
  try {
    HostdRequestContext context(support_, db_path);
    if (request.method == "POST") {
      const json body = ParseJsonBody(request);
      const std::string node_name = body.value("node_name", std::string{});
      if (node_name.empty()) {
        return context.Json(
            400,
            json{{"status", "bad_request"},
                 {"message", "missing required field 'node_name'"}});
      }
      if (context.store().LoadRegisteredHost(node_name).has_value()) {
        return context.Json(
            409,
            json{{"status", "conflict"},
                 {"message", "host node already exists"}});
      }
      const std::string onboarding_key = naim::RandomTokenBase64(24);
      naim::RegisteredHostRecord host;
      host.node_name = node_name;
      host.transport_mode = "out";
      host.execution_mode = "mixed";
      host.registration_state = "provisioned";
      host.onboarding_key_hash = naim::ComputeSha256Hex(onboarding_key);
      host.onboarding_state = "pending";
      host.derived_role = "ineligible";
      host.role_reason = "awaiting first inventory scan";
      host.session_state = "disconnected";
      host.status_message = "node provisioned; awaiting naim-node onboarding";
      context.store().UpsertRegisteredHost(host);
      context.EmitHostRegistryEvent(
          "provisioned",
          "provisioned host node for onboarding",
          json::object(),
          node_name,
          "info");
      return context.Json(
          200,
          json{{"service", "naim-controller"},
               {"node_name", node_name},
               {"onboarding_key", onboarding_key},
               {"onboarding_state", host.onboarding_state}});
    }
    if (request.method != "GET") {
      return support_.build_json_response(
          405, json{{"status", "method_not_allowed"}}, {});
    }
    return context.Json(
        200,
        context.MakeHostRegistryService().BuildPayload(
            FindQueryStringValue(request, "node")));
  } catch (const std::exception& error) {
    return support_.build_json_response(
        500,
        json{{"status", "internal_error"},
             {"message", error.what()},
             {"path", request.path}},
        {});
  }
}

HttpResponse HostdHttpService::HandleHostPath(
    const std::string& db_path,
    const HttpRequest& request) const {
  const std::string remainder =
      request.path.substr(std::string("/api/v1/hostd/hosts/").size());
  if (remainder.empty()) {
    return support_.build_json_response(404, json{{"status", "not_found"}}, {});
  }
  const auto revoke_pos = remainder.find("/revoke");
  if (revoke_pos != std::string::npos &&
      revoke_pos + std::string("/revoke").size() == remainder.size()) {
    if (request.method != "POST") {
      return support_.build_json_response(
          405, json{{"status", "method_not_allowed"}}, {});
    }
    const std::string node_name = remainder.substr(0, revoke_pos);
    try {
      const json body = ParseJsonBody(request);
      const std::optional<std::string> message =
          body.contains("message") && body["message"].is_string()
              ? std::make_optional(body["message"].get<std::string>())
              : std::nullopt;
      const auto service =
          naim::controller::HostRegistryService(db_path, support_.host_registry_event_sink());
      return support_.build_json_response(
          200,
          naim::controller::BuildControllerActionPayload(
              naim::controller::RunControllerActionResult(
                  "revoke-hostd",
                  [&]() { return service.RevokeHost(node_name, message); })),
          {});
    } catch (const std::exception& error) {
      return support_.build_json_response(
          500,
          json{{"status", "internal_error"},
               {"message", error.what()},
               {"path", request.path}},
          {});
    }
  }
  const auto rotate_pos = remainder.find("/rotate-key");
  if (rotate_pos != std::string::npos &&
      rotate_pos + std::string("/rotate-key").size() == remainder.size()) {
    if (request.method != "POST") {
      return support_.build_json_response(
          405, json{{"status", "method_not_allowed"}}, {});
    }
    const std::string node_name = remainder.substr(0, rotate_pos);
    try {
      const json body = ParseJsonBody(request);
      const std::string public_key_base64 =
          body.value("public_key_base64", std::string{});
      if (public_key_base64.empty()) {
        return support_.build_json_response(
            400,
            json{{"status", "bad_request"},
                 {"message", "missing required field 'public_key_base64'"}},
            {});
      }
      const std::optional<std::string> message =
          body.contains("message") && body["message"].is_string()
              ? std::make_optional(body["message"].get<std::string>())
              : std::nullopt;
      const auto service =
          naim::controller::HostRegistryService(db_path, support_.host_registry_event_sink());
      return support_.build_json_response(
          200,
          naim::controller::BuildControllerActionPayload(
              naim::controller::RunControllerActionResult(
                  "rotate-hostd-key",
                  [&]() {
                    return service.RotateHostKey(
                        node_name, public_key_base64, message);
                  })),
          {});
    } catch (const std::exception& error) {
      return support_.build_json_response(
          500,
          json{{"status", "internal_error"},
               {"message", error.what()},
               {"path", request.path}},
          {});
    }
  }
  const auto reset_onboarding_pos = remainder.find("/reset-onboarding");
  if (reset_onboarding_pos != std::string::npos &&
      reset_onboarding_pos + std::string("/reset-onboarding").size() == remainder.size()) {
    if (request.method != "POST") {
      return support_.build_json_response(
          405, json{{"status", "method_not_allowed"}}, {});
    }
    const std::string node_name = remainder.substr(0, reset_onboarding_pos);
    try {
      const json body = ParseJsonBody(request);
      const std::optional<std::string> message =
          body.contains("message") && body["message"].is_string()
              ? std::make_optional(body["message"].get<std::string>())
              : std::nullopt;
      const auto service =
          naim::controller::HostRegistryService(db_path, support_.host_registry_event_sink());
      return support_.build_json_response(
          200,
          service.ResetHostOnboardingPayload(node_name, message),
          {});
    } catch (const std::exception& error) {
      return support_.build_json_response(
          409,
          json{{"status", "conflict"},
               {"message", error.what()},
               {"path", request.path}},
          {});
    }
  }
  const auto storage_role_pos = remainder.find("/storage-role");
  if (storage_role_pos != std::string::npos &&
      storage_role_pos + std::string("/storage-role").size() == remainder.size()) {
    if (request.method != "POST") {
      return support_.build_json_response(
          405, json{{"status", "method_not_allowed"}}, {});
    }
    const std::string node_name = remainder.substr(0, storage_role_pos);
    try {
      const json body = ParseJsonBody(request);
      std::optional<bool> enabled;
      if (body.contains("enabled") && body["enabled"].is_boolean()) {
        enabled = body["enabled"].get<bool>();
      } else if (const auto query_enabled = FindQueryStringValue(request, "enabled");
                 query_enabled.has_value()) {
        if (*query_enabled == "true" || *query_enabled == "1" ||
            *query_enabled == "enabled") {
          enabled = true;
        } else if (*query_enabled == "false" || *query_enabled == "0" ||
                   *query_enabled == "disabled") {
          enabled = false;
        }
      }
      if (!enabled.has_value()) {
        return support_.build_json_response(
            400,
            json{{"status", "bad_request"},
                 {"message", "missing required boolean field or query parameter 'enabled'"}},
            {});
      }
      std::optional<std::string> message;
      if (body.contains("message") && body["message"].is_string()) {
        message = body["message"].get<std::string>();
      } else {
        message = FindQueryStringValue(request, "message");
      }
      const auto service =
          naim::controller::HostRegistryService(db_path, support_.host_registry_event_sink());
      return support_.build_json_response(
          200,
          service.SetHostStorageRolePayload(
              node_name,
              *enabled,
              message),
          {});
    } catch (const std::exception& error) {
      return support_.build_json_response(
          409,
          json{{"status", "conflict"},
               {"message", error.what()},
               {"path", request.path}},
          {});
    }
  }
  return support_.build_json_response(404, json{{"status", "not_found"}}, {});
}

HttpResponse HostdHttpService::HandleSessionOpen(
    const std::string& db_path,
    const HttpRequest& request) const {
  if (request.method != "POST") {
    return support_.build_json_response(405, json{{"status", "method_not_allowed"}}, {});
  }
  try {
    const json body = ParseJsonBody(request);
    const std::string node_name = body.value("node_name", std::string{});
    if (node_name.empty()) {
      return support_.build_json_response(
          400,
          json{{"status", "bad_request"},
               {"message", "missing required field 'node_name'"}},
          {});
    }
    HostdRequestContext context(support_, db_path);
    auto current = context.store().LoadRegisteredHost(node_name);
    if (!current.has_value()) {
      return context.Json(
          404,
          json{{"status", "not_found"},
               {"message", "host node is not registered"}});
    }
    const std::string timestamp = body.value("timestamp", std::string{});
    const std::string nonce = body.value("nonce", std::string{});
    const std::string signature = body.value("signature", std::string{});
    if (timestamp.empty() || nonce.empty() || signature.empty()) {
      return context.Json(
          400,
          json{{"status", "bad_request"},
               {"message", "missing required session handshake fields"}});
    }
    if (current->public_key_base64.empty()) {
      return context.Json(
          403,
          json{{"status", "forbidden"},
               {"message", "registered host is missing public key"}});
    }
    const std::string signed_message =
        "hostd-session-open\n" + node_name + "\n" + timestamp + "\n" + nonce;
    if (!naim::VerifyDetachedBase64(
            signed_message, signature, current->public_key_base64)) {
      return context.Json(
          403,
          json{{"status", "forbidden"},
               {"message", "invalid host session signature"}});
    }
    current->session_state = "connected";
    current->last_session_at = support_.utc_now_sql_timestamp();
    current->session_token = naim::RandomTokenBase64(32);
    current->session_expires_at = support_.sql_timestamp_after_seconds(600);
    current->session_host_sequence = 0;
    current->session_controller_sequence = 0;
    current->status_message =
        body.value("status_message", std::string("session opened"));
    context.store().UpsertRegisteredHost(*current);
    context.EmitHostRegistryEvent(
        "session-opened",
        "opened host-agent session",
        json::object(),
        node_name,
        "info");
    return context.Json(
        200,
        json{
            {"service", "naim-controller"},
            {"node_name", node_name},
            {"session_state", current->session_state},
            {"last_session_at", current->last_session_at},
            {"session_token", current->session_token},
            {"controller_public_key_fingerprint",
             current->controller_public_key_fingerprint},
            {"controller_sequence", current->session_controller_sequence},
        });
  } catch (const std::exception& error) {
    return support_.build_json_response(
        500,
        json{{"status", "internal_error"},
             {"message", error.what()},
             {"path", request.path}},
        {});
  }
}

HttpResponse HostdHttpService::HandleSessionHeartbeat(
    const std::string& db_path,
    const HttpRequest& request) const {
  if (request.method != "POST") {
    return support_.build_json_response(405, json{{"status", "method_not_allowed"}}, {});
  }
  try {
    HostdRequestContext context(support_, db_path);
    const auto authenticated = context.Authenticate(request);
    if (!authenticated.has_value()) {
      return context.Json(
          403,
          json{{"status", "forbidden"},
               {"message", "invalid or missing host session"}});
    }
    auto current = *authenticated;
    const json decrypted =
        context.ParseEncryptedBody(request, &current, "session/heartbeat");
    const std::string node_name =
        decrypted.value("node_name", current.node_name);
    if (node_name.empty() || node_name != current.node_name) {
      return context.Json(
          403,
          json{{"status", "forbidden"},
               {"message", "node mismatch for host heartbeat"}});
    }
    current.session_state =
        decrypted.value("session_state", std::string("connected"));
    current.last_heartbeat_at = support_.utc_now_sql_timestamp();
    current.last_session_at = current.last_heartbeat_at;
    current.status_message =
        decrypted.value("status_message", std::string("heartbeat"));
    context.store().UpsertRegisteredHost(current);
    return context.EncryptedResponse(
        &current,
        "session/heartbeat",
        json{
            {"service", "naim-controller"},
            {"node_name", node_name},
            {"session_state", current.session_state},
            {"last_heartbeat_at", current.last_heartbeat_at},
        });
  } catch (const std::exception& error) {
    return support_.build_json_response(
        500,
        json{{"status", "internal_error"},
             {"message", error.what()},
             {"path", request.path}},
        {});
  }
}

HttpResponse HostdHttpService::HandleNextAssignment(
    const std::string& db_path,
    const HttpRequest& request) const {
  if (request.method != "GET" && request.method != "POST") {
    return support_.build_json_response(405, json{{"status", "method_not_allowed"}}, {});
  }
  try {
    HostdRequestContext context(support_, db_path);
    std::optional<std::string> node_name = FindQueryStringValue(request, "node");
    std::optional<naim::RegisteredHostRecord> authenticated;
    bool encrypted_request = false;
    if (request.method == "POST") {
      authenticated = context.Authenticate(request);
      if (!authenticated.has_value()) {
        return context.Json(
            403,
            json{{"status", "forbidden"},
                 {"message", "invalid or missing host session"}});
      }
      auto host = *authenticated;
      const json decrypted =
          context.ParseEncryptedBody(request, &host, "assignments/next");
      node_name = decrypted.contains("node_name")
                      ? std::optional<std::string>(
                            decrypted.value("node_name", std::string{}))
                      : node_name;
      authenticated = host;
      encrypted_request = true;
    }
    if (!node_name.has_value() || node_name->empty()) {
      return context.Json(
          400,
          json{{"status", "bad_request"},
               {"message", "missing required query parameter 'node'"}});
    }
    if (!authenticated.has_value()) {
      authenticated = context.Authenticate(request, *node_name);
      if (!authenticated.has_value()) {
        return context.Json(
            403,
            json{{"status", "forbidden"},
                 {"message", "invalid or missing host session"}});
      }
    }
    const auto assignment = context.store().ClaimNextHostAssignment(*node_name);
    const json payload{
        {"service", "naim-controller"},
        {"node_name", *node_name},
        {"assignment",
         assignment.has_value() ? BuildAssignmentPayloadItem(*assignment)
                                : json(nullptr)},
    };
    if (encrypted_request) {
      auto host = *authenticated;
      return context.EncryptedResponse(&host, "assignments/next", payload);
    }
    return context.Json(200, payload);
  } catch (const std::exception& error) {
    return support_.build_json_response(
        500,
        json{{"status", "internal_error"},
             {"message", error.what()},
             {"path", request.path}},
        {});
  }
}

HttpResponse HostdHttpService::HandleAssignmentAction(
    const std::string& db_path,
    const HttpRequest& request) const {
  const std::string remainder =
      request.path.substr(std::string("/api/v1/hostd/assignments/").size());
  const auto slash = remainder.find('/');
  if (slash == std::string::npos) {
    return support_.build_json_response(404, json{{"status", "not_found"}}, {});
  }
  const int assignment_id = std::stoi(remainder.substr(0, slash));
  const std::string action = remainder.substr(slash + 1);
  if (request.method != "POST") {
    return support_.build_json_response(405, json{{"status", "method_not_allowed"}}, {});
  }
  try {
    HostdRequestContext context(support_, db_path);
    const auto assignment = context.store().LoadHostAssignment(assignment_id);
    if (!assignment.has_value()) {
      return context.Json(
          404,
          json{{"status", "not_found"}, {"message", "assignment not found"}});
    }
    const auto authenticated =
        context.Authenticate(request, assignment->node_name);
    if (!authenticated.has_value()) {
      return context.Json(
          403,
          json{{"status", "forbidden"},
               {"message", "invalid or missing host session"}});
    }
    auto host = *authenticated;
    const json body = context.ParseEncryptedBody(
        request,
        &host,
        "assignments/" + std::to_string(assignment_id) + "/" + action);
    const std::string status_message =
        body.value("status_message", std::string{});
    if (action == "progress") {
      const bool updated =
          context.store().UpdateHostAssignmentProgress(assignment_id, body.dump());
      if (assignment->assignment_type == "model-library-download") {
        const json assignment_payload =
            json::parse(assignment->desired_state_json, nullptr, false);
        const std::string job_id =
            assignment_payload.is_object()
                ? assignment_payload.value("job_id", std::string{})
                : std::string{};
        if (!job_id.empty()) {
          if (auto job = context.store().LoadModelLibraryDownloadJob(job_id);
              job.has_value()) {
            job->status = "running";
            job->phase = body.value("phase", std::string("running"));
            job->current_item = body.value("detail", std::string{});
            if (body.contains("bytes_done") && body["bytes_done"].is_number_integer()) {
              job->bytes_done = body["bytes_done"].get<std::int64_t>();
            }
            if (body.contains("bytes_total") && body["bytes_total"].is_number_integer()) {
              job->bytes_total = body["bytes_total"].get<std::int64_t>();
            }
            context.store().UpsertModelLibraryDownloadJob(*job);
          }
        }
      }
      return context.EncryptedResponse(
          &host,
          "assignments/" + std::to_string(assignment_id) + "/progress",
          json{{"service", "naim-controller"},
               {"updated", updated},
               {"assignment_id", assignment_id}});
    }
    if (action == "applied") {
      const bool updated = context.store().TransitionClaimedHostAssignment(
          assignment_id, naim::HostAssignmentStatus::Applied, status_message);
      if (updated && assignment->assignment_type == "model-library-download") {
        const json assignment_payload =
            json::parse(assignment->desired_state_json, nullptr, false);
        const std::string job_id =
            assignment_payload.is_object()
                ? assignment_payload.value("job_id", std::string{})
                : std::string{};
        if (!job_id.empty()) {
          if (auto job = context.store().LoadModelLibraryDownloadJob(job_id);
              job.has_value()) {
            job->status = "completed";
            job->phase = "completed";
            job->current_item.clear();
            job->error_message.clear();
            context.store().UpsertModelLibraryDownloadJob(*job);
          }
        }
      }
      if (updated && assignment->assignment_type == "apply-node-state") {
        const auto plane_assignments = context.store().LoadHostAssignments(
            std::nullopt, std::nullopt, assignment->plane_name);
        const auto latest_assignments_by_node =
            BuildLatestPlaneAssignmentsByNode(plane_assignments);
        const bool converged_generation = std::all_of(
            latest_assignments_by_node.begin(),
            latest_assignments_by_node.end(),
            [&](const auto& entry) {
              const auto& candidate = entry.second;
              if (candidate.assignment_type != "apply-node-state" ||
                  candidate.desired_generation != assignment->desired_generation) {
                return true;
              }
              return candidate.status == naim::HostAssignmentStatus::Applied ||
                     candidate.status == naim::HostAssignmentStatus::Superseded;
            });
        if (converged_generation) {
          context.store().UpdatePlaneAppliedGeneration(
              assignment->plane_name, assignment->desired_generation);
        }
      }
      return context.EncryptedResponse(
          &host,
          "assignments/" + std::to_string(assignment_id) + "/applied",
          json{{"service", "naim-controller"},
               {"updated", updated},
               {"assignment_id", assignment_id}});
    }
    if (action == "failed") {
      const bool retry = body.value("retry", false);
      const bool updated = retry
                               ? context.store().TransitionClaimedHostAssignment(
                                     assignment_id,
                                     naim::HostAssignmentStatus::Pending,
                                     status_message)
                               : context.store().TransitionClaimedHostAssignment(
                                     assignment_id,
                                     naim::HostAssignmentStatus::Failed,
                                     status_message);
      if (updated && assignment->assignment_type == "model-library-download" && !retry) {
        const json assignment_payload =
            json::parse(assignment->desired_state_json, nullptr, false);
        const std::string job_id =
            assignment_payload.is_object()
                ? assignment_payload.value("job_id", std::string{})
                : std::string{};
        if (!job_id.empty()) {
          if (auto job = context.store().LoadModelLibraryDownloadJob(job_id);
              job.has_value()) {
            job->status = "failed";
            job->phase = "failed";
            job->error_message = status_message;
            context.store().UpsertModelLibraryDownloadJob(*job);
          }
        }
      }
      return context.EncryptedResponse(
          &host,
          "assignments/" + std::to_string(assignment_id) + "/failed",
          json{{"service", "naim-controller"},
               {"updated", updated},
               {"assignment_id", assignment_id},
               {"retry", retry}});
    }
    return context.Json(404, json{{"status", "not_found"}});
  } catch (const std::exception& error) {
    return support_.build_json_response(
        500,
        json{{"status", "internal_error"},
             {"message", error.what()},
             {"path", request.path}},
	        {});
	  }
	}

HttpResponse HostdHttpService::HandleModelArtifactChunkRequest(
    const std::string& db_path,
    const HttpRequest& request) const {
  if (request.method != "POST") {
    return support_.build_json_response(405, json{{"status", "method_not_allowed"}}, {});
  }
  try {
    HostdRequestContext context(support_, db_path);
    const auto authenticated = context.Authenticate(request);
    if (!authenticated.has_value()) {
      return context.Json(
          403,
          json{{"status", "forbidden"},
               {"message", "invalid or missing host session"}});
    }
    auto host = *authenticated;
    const json body =
        context.ParseEncryptedBody(request, &host, "model-artifacts/chunks/request");
    const std::string requester_node_name =
        body.value("requester_node_name", host.node_name);
    if (requester_node_name != host.node_name) {
      return context.Json(
          403,
          json{{"status", "forbidden"},
               {"message", "requester_node_name does not match host session"}});
    }
    const std::string source_path = NormalizePathString(
        std::filesystem::path(body.value("source_path", std::string{})));
    const std::string requested_source_node_name =
        body.value("source_node_name", std::string{});
    const auto source_node_name = ResolveModelArtifactSourceNode(
        context.store(),
        requested_source_node_name,
        source_path);
    if (!source_node_name.has_value()) {
      return context.EncryptedResponse(
          &host,
          "model-artifacts/chunks/request",
          json{{"service", "naim-controller"},
               {"status", "not_found"},
               {"message", "model artifact source node not found or not eligible"}});
    }

    const std::uintmax_t offset =
        body.contains("offset") && body.at("offset").is_number_unsigned()
            ? body.at("offset").get<std::uintmax_t>()
            : std::uintmax_t{0};
    std::uintmax_t max_bytes =
        body.contains("max_bytes") && body.at("max_bytes").is_number_unsigned()
            ? body.at("max_bytes").get<std::uintmax_t>()
            : kMaxModelArtifactChunkBytes;
    if (max_bytes == 0 || max_bytes > kMaxModelArtifactChunkBytes) {
      max_bytes = kMaxModelArtifactChunkBytes;
    }

    const std::string transfer_id = naim::RandomTokenBase64(18);
    const std::string plane_name = "model-transfer:" + transfer_id;
    naim::HostAssignment assignment;
    assignment.node_name = *source_node_name;
    assignment.plane_name = plane_name;
    assignment.desired_generation = 0;
    assignment.max_attempts = 3;
    assignment.assignment_type = "model-artifact-read-chunk";
    assignment.desired_state_json =
        json{{"transfer_id", transfer_id},
             {"requester_node_name", requester_node_name},
             {"source_node_name", *source_node_name},
             {"source_path", source_path},
             {"offset", offset},
             {"max_bytes", max_bytes}}
            .dump();
    assignment.artifacts_root = source_path;
    assignment.status_message = "queued model artifact chunk read";
    assignment.progress_json =
        json{{"phase", "queued"},
             {"title", "Model artifact chunk queued"},
             {"detail", "Waiting for storage node to read the model artifact chunk."},
             {"percent", 0}}
            .dump();
    context.store().EnqueueHostAssignments({assignment}, "");

    int assignment_id = 0;
    for (const auto& candidate :
         context.store().LoadHostAssignments(
             std::make_optional<std::string>(*source_node_name),
             std::nullopt,
             std::make_optional<std::string>(plane_name))) {
      const json desired =
          json::parse(candidate.desired_state_json, nullptr, false);
      if (desired.is_object() && desired.value("transfer_id", std::string{}) == transfer_id) {
        assignment_id = candidate.id;
        break;
      }
    }
    if (assignment_id <= 0) {
      return context.EncryptedResponse(
          &host,
          "model-artifacts/chunks/request",
          json{{"service", "naim-controller"},
               {"status", "internal_error"},
               {"message", "failed to resolve queued model artifact assignment"}});
    }
    return context.EncryptedResponse(
        &host,
        "model-artifacts/chunks/request",
        json{{"service", "naim-controller"},
             {"status", "queued"},
             {"assignment_id", assignment_id},
             {"transfer_id", transfer_id},
             {"source_node_name", *source_node_name},
             {"source_path", source_path},
             {"offset", offset},
             {"max_bytes", max_bytes}});
  } catch (const std::exception& error) {
    return support_.build_json_response(
        500,
        json{{"status", "internal_error"},
             {"message", error.what()},
             {"path", request.path}},
        {});
  }
}

HttpResponse HostdHttpService::HandleModelArtifactChunkPoll(
    const std::string& db_path,
    const HttpRequest& request) const {
  if (request.method != "POST") {
    return support_.build_json_response(405, json{{"status", "method_not_allowed"}}, {});
  }
  try {
    HostdRequestContext context(support_, db_path);
    const auto authenticated = context.Authenticate(request);
    if (!authenticated.has_value()) {
      return context.Json(
          403,
          json{{"status", "forbidden"},
               {"message", "invalid or missing host session"}});
    }
    auto host = *authenticated;
    const json body =
        context.ParseEncryptedBody(request, &host, "model-artifacts/chunks/poll");
    const std::string requester_node_name =
        body.value("requester_node_name", host.node_name);
    if (requester_node_name != host.node_name) {
      return context.Json(
          403,
          json{{"status", "forbidden"},
               {"message", "requester_node_name does not match host session"}});
    }
    const int assignment_id = body.value("assignment_id", 0);
    const auto assignment = context.store().LoadHostAssignment(assignment_id);
    if (!assignment.has_value() ||
        assignment->assignment_type != "model-artifact-read-chunk") {
      return context.EncryptedResponse(
          &host,
          "model-artifacts/chunks/poll",
          json{{"service", "naim-controller"},
               {"status", "not_found"},
               {"message", "model artifact chunk assignment not found"}});
    }
    const json desired =
        json::parse(assignment->desired_state_json, nullptr, false);
    if (!desired.is_object() ||
        desired.value("requester_node_name", std::string{}) != requester_node_name) {
      return context.Json(
          403,
          json{{"status", "forbidden"},
               {"message", "model artifact chunk assignment belongs to another requester"}});
    }
    json progress = json::object();
    if (!assignment->progress_json.empty()) {
      progress = json::parse(assignment->progress_json, nullptr, false);
      if (!progress.is_object()) {
        progress = json::object();
      }
    }
    return context.EncryptedResponse(
        &host,
        "model-artifacts/chunks/poll",
        json{{"service", "naim-controller"},
             {"status", naim::ToString(assignment->status)},
             {"assignment_id", assignment->id},
             {"source_node_name", assignment->node_name},
             {"status_message", assignment->status_message},
             {"progress", progress}});
  } catch (const std::exception& error) {
    return support_.build_json_response(
        500,
        json{{"status", "internal_error"},
             {"message", error.what()},
             {"path", request.path}},
        {});
  }
}

HttpResponse HostdHttpService::HandleModelArtifactManifestRequest(
    const std::string& db_path,
    const HttpRequest& request) const {
  if (request.method != "POST") {
    return support_.build_json_response(405, json{{"status", "method_not_allowed"}}, {});
  }
  try {
    HostdRequestContext context(support_, db_path);
    const auto authenticated = context.Authenticate(request);
    if (!authenticated.has_value()) {
      return context.Json(
          403,
          json{{"status", "forbidden"},
               {"message", "invalid or missing host session"}});
    }
    auto host = *authenticated;
    const json body =
        context.ParseEncryptedBody(request, &host, "model-artifacts/manifest/request");
    const std::string requester_node_name =
        body.value("requester_node_name", host.node_name);
    if (requester_node_name != host.node_name) {
      return context.Json(
          403,
          json{{"status", "forbidden"},
               {"message", "requester_node_name does not match host session"}});
    }

    const std::vector<std::string> source_paths = ParseRequestedSourcePaths(body);
    if (source_paths.empty()) {
      return context.EncryptedResponse(
          &host,
          "model-artifacts/manifest/request",
          json{{"service", "naim-controller"},
               {"status", "bad_request"},
               {"message", "model artifact manifest request is missing source_paths"}});
    }

    const std::string requested_source_node_name =
        body.value("source_node_name", std::string{});
    const auto source_node_name = ResolveModelArtifactSourceNode(
        context.store(),
        requested_source_node_name,
        source_paths.front());
    if (!source_node_name.has_value()) {
      return context.EncryptedResponse(
          &host,
          "model-artifacts/manifest/request",
          json{{"service", "naim-controller"},
               {"status", "not_found"},
               {"message", "model artifact source node not found or not eligible"}});
    }
    const auto source_host = context.store().LoadRegisteredHost(*source_node_name);
    const bool all_paths_allowed =
        source_host.has_value() &&
        std::all_of(
            source_paths.begin(),
            source_paths.end(),
            [&](const std::string& source_path) {
              return HostCanServeModelArtifactPath(*source_host, source_path);
            });
    if (!all_paths_allowed) {
      return context.EncryptedResponse(
          &host,
          "model-artifacts/manifest/request",
          json{{"service", "naim-controller"},
               {"status", "not_found"},
               {"message", "one or more model artifact source paths are not eligible"}});
    }

    const std::string transfer_id = naim::RandomTokenBase64(18);
    const std::string plane_name = "model-manifest:" + transfer_id;
    naim::HostAssignment assignment;
    assignment.node_name = *source_node_name;
    assignment.plane_name = plane_name;
    assignment.desired_generation = 0;
    assignment.max_attempts = 3;
    assignment.assignment_type = "model-artifact-build-manifest";
    assignment.desired_state_json =
        json{{"transfer_id", transfer_id},
             {"requester_node_name", requester_node_name},
             {"source_node_name", *source_node_name},
             {"source_paths", source_paths}}
            .dump();
    assignment.artifacts_root = source_paths.front();
    assignment.status_message = "queued model artifact manifest build";
    assignment.progress_json =
        json{{"phase", "queued"},
             {"title", "Model artifact manifest queued"},
             {"detail", "Waiting for storage node to build the model artifact manifest."},
             {"percent", 0}}
            .dump();
    context.store().EnqueueHostAssignments({assignment}, "");

    int assignment_id = 0;
    for (const auto& candidate :
         context.store().LoadHostAssignments(
             std::make_optional<std::string>(*source_node_name),
             std::nullopt,
             std::make_optional<std::string>(plane_name))) {
      const json desired =
          json::parse(candidate.desired_state_json, nullptr, false);
      if (desired.is_object() && desired.value("transfer_id", std::string{}) == transfer_id) {
        assignment_id = candidate.id;
        break;
      }
    }
    if (assignment_id <= 0) {
      return context.EncryptedResponse(
          &host,
          "model-artifacts/manifest/request",
          json{{"service", "naim-controller"},
               {"status", "internal_error"},
               {"message", "failed to resolve queued model artifact manifest assignment"}});
    }
    return context.EncryptedResponse(
        &host,
        "model-artifacts/manifest/request",
        json{{"service", "naim-controller"},
             {"status", "queued"},
             {"assignment_id", assignment_id},
             {"transfer_id", transfer_id},
             {"source_node_name", *source_node_name},
             {"source_paths", source_paths}});
  } catch (const std::exception& error) {
    return support_.build_json_response(
        500,
        json{{"status", "internal_error"},
             {"message", error.what()},
             {"path", request.path}},
        {});
  }
}

HttpResponse HostdHttpService::HandleModelArtifactManifestPoll(
    const std::string& db_path,
    const HttpRequest& request) const {
  if (request.method != "POST") {
    return support_.build_json_response(405, json{{"status", "method_not_allowed"}}, {});
  }
  try {
    HostdRequestContext context(support_, db_path);
    const auto authenticated = context.Authenticate(request);
    if (!authenticated.has_value()) {
      return context.Json(
          403,
          json{{"status", "forbidden"},
               {"message", "invalid or missing host session"}});
    }
    auto host = *authenticated;
    const json body =
        context.ParseEncryptedBody(request, &host, "model-artifacts/manifest/poll");
    const std::string requester_node_name =
        body.value("requester_node_name", host.node_name);
    if (requester_node_name != host.node_name) {
      return context.Json(
          403,
          json{{"status", "forbidden"},
               {"message", "requester_node_name does not match host session"}});
    }
    const int assignment_id = body.value("assignment_id", 0);
    const auto assignment = context.store().LoadHostAssignment(assignment_id);
    if (!assignment.has_value() ||
        assignment->assignment_type != "model-artifact-build-manifest") {
      return context.EncryptedResponse(
          &host,
          "model-artifacts/manifest/poll",
          json{{"service", "naim-controller"},
               {"status", "not_found"},
               {"message", "model artifact manifest assignment not found"}});
    }
    const json desired =
        json::parse(assignment->desired_state_json, nullptr, false);
    if (!desired.is_object() ||
        desired.value("requester_node_name", std::string{}) != requester_node_name) {
      return context.Json(
          403,
          json{{"status", "forbidden"},
               {"message", "model artifact manifest assignment belongs to another requester"}});
    }
    json progress = json::object();
    if (!assignment->progress_json.empty()) {
      progress = json::parse(assignment->progress_json, nullptr, false);
      if (!progress.is_object()) {
        progress = json::object();
      }
    }
    return context.EncryptedResponse(
        &host,
        "model-artifacts/manifest/poll",
        json{{"service", "naim-controller"},
             {"status", naim::ToString(assignment->status)},
             {"assignment_id", assignment->id},
             {"source_node_name", assignment->node_name},
             {"status_message", assignment->status_message},
             {"progress", progress}});
  } catch (const std::exception& error) {
    return support_.build_json_response(
        500,
        json{{"status", "internal_error"},
             {"message", error.what()},
             {"path", request.path}},
        {});
  }
}

HttpResponse HostdHttpService::HandleObservations(
    const std::string& db_path,
    const HttpRequest& request) const {
  if (request.method != "POST") {
    return support_.build_json_response(405, json{{"status", "method_not_allowed"}}, {});
  }
  try {
    HostdRequestContext context(support_, db_path);
    const auto authenticated = context.Authenticate(request);
    if (!authenticated.has_value()) {
      return context.Json(
          403,
          json{{"status", "forbidden"},
               {"message", "invalid or missing host session"}});
    }
    auto host = *authenticated;
    const json body =
        context.ParseEncryptedBody(request, &host, "observations/upsert");
    const auto observation = ParseHostObservationPayload(body);
    if (observation.node_name.empty()) {
      return context.Json(
          400,
          json{{"status", "bad_request"},
               {"message", "missing required field 'node_name'"}});
    }
    if (host.node_name != observation.node_name) {
      return context.Json(
          403,
          json{{"status", "forbidden"},
               {"message", "node mismatch for host observation"}});
    }
    context.store().UpsertHostObservation(observation);
    if (auto current = context.store().LoadRegisteredHost(observation.node_name);
        current.has_value()) {
      const auto inventory = BuildInventorySummary(*current, observation);
      const auto [derived_role, role_reason] = DeriveRole(inventory);
      current->derived_role = derived_role;
      current->role_reason = role_reason;
      current->last_inventory_scan_at = support_.utc_now_sql_timestamp();
      current->capabilities_json = MergeCapabilities(
          current->capabilities_json,
          inventory)
                                      .dump();
      context.store().UpsertRegisteredHost(*current);
    }
    return context.EncryptedResponse(
        &host,
        "observations/upsert",
        json{{"service", "naim-controller"},
             {"node_name", observation.node_name},
             {"updated", true}});
  } catch (const std::exception& error) {
    return support_.build_json_response(
        500,
        json{{"status", "internal_error"},
             {"message", error.what()},
             {"path", request.path}},
        {});
  }
}

HttpResponse HostdHttpService::HandleEvents(
    const std::string& db_path,
    const HttpRequest& request) const {
  if (request.method != "POST") {
    return support_.build_json_response(405, json{{"status", "method_not_allowed"}}, {});
  }
  try {
    HostdRequestContext context(support_, db_path);
    const auto authenticated = context.Authenticate(request);
    if (!authenticated.has_value()) {
      return context.Json(
          403,
          json{{"status", "forbidden"},
               {"message", "invalid or missing host session"}});
    }
    auto host = *authenticated;
    const json body = context.ParseEncryptedBody(request, &host, "events/append");
    const std::string node_name = body.value("node_name", std::string{});
    if (!node_name.empty() && node_name != host.node_name) {
      return context.Json(
          403,
          json{{"status", "forbidden"},
               {"message", "node mismatch for event append"}});
    }
    context.store().AppendEvent(naim::EventRecord{
        0,
        body.value("plane_name", std::string{}),
        body.value("node_name", std::string{}),
        body.value("worker_name", std::string{}),
        body.contains("assignment_id") && !body.at("assignment_id").is_null()
            ? std::optional<int>(body.at("assignment_id").get<int>())
            : std::nullopt,
        body.contains("rollout_action_id") &&
                !body.at("rollout_action_id").is_null()
            ? std::optional<int>(body.at("rollout_action_id").get<int>())
            : std::nullopt,
        body.value("category", std::string{}),
        body.value("event_type", std::string{}),
        body.value("severity", std::string("info")),
        body.value("message", std::string{}),
        body.value("payload_json", std::string("{}")),
        "",
    });
    return context.EncryptedResponse(
        &host,
        "events/append",
        json{{"service", "naim-controller"}, {"appended", true}});
  } catch (const std::exception& error) {
    return support_.build_json_response(
        500,
        json{{"status", "internal_error"},
             {"message", error.what()},
             {"path", request.path}},
        {});
  }
}

HttpResponse HostdHttpService::HandleDiskRuntimeState(
    const std::string& db_path,
    const HttpRequest& request) const {
  try {
    HostdRequestContext context(support_, db_path);
    if (request.method == "GET") {
      const auto disk_name = FindQueryStringValue(request, "disk_name");
      const auto node_name = FindQueryStringValue(request, "node");
      if (!disk_name.has_value() || !node_name.has_value()) {
        return context.Json(
            400,
            json{{"status", "bad_request"},
                 {"message",
                  "missing required query parameters 'disk_name' and 'node'"}});
      }
      const auto authenticated = context.Authenticate(request, *node_name);
      if (!authenticated.has_value()) {
        return context.Json(
            403,
            json{{"status", "forbidden"},
                 {"message", "invalid or missing host session"}});
      }
      const auto runtime_state =
          context.store().LoadDiskRuntimeState(*disk_name, *node_name);
      return context.Json(
          200,
          json{{"service", "naim-controller"},
               {"runtime_state",
                runtime_state.has_value()
                    ? BuildDiskRuntimeStatePayloadItem(*runtime_state)
                    : json(nullptr)}});
    }
    if (request.method == "POST") {
      const auto authenticated = context.Authenticate(request);
      if (!authenticated.has_value()) {
        return context.Json(
            403,
            json{{"status", "forbidden"},
                 {"message", "invalid or missing host session"}});
      }
      auto host = *authenticated;
      const json body = context.ParseEncryptedBody(
          request, &host, "disk-runtime-state/upsert");
      const auto runtime_state = ParseDiskRuntimeStatePayload(body);
      if (runtime_state.disk_name.empty() || runtime_state.node_name.empty()) {
        return context.Json(
            400,
            json{{"status", "bad_request"},
                 {"message",
                  "missing required fields 'disk_name' and 'node_name'"}});
      }
      if (host.node_name != runtime_state.node_name) {
        return context.Json(
            403,
            json{{"status", "forbidden"},
                 {"message", "node mismatch for disk runtime state"}});
      }
      context.store().UpsertDiskRuntimeState(runtime_state);
      return context.EncryptedResponse(
          &host,
          "disk-runtime-state/upsert",
          json{{"service", "naim-controller"},
               {"updated", true},
               {"disk_name", runtime_state.disk_name}});
    }
    return context.Json(405, json{{"status", "method_not_allowed"}});
  } catch (const std::exception& error) {
    return support_.build_json_response(
        500,
        json{{"status", "internal_error"},
             {"message", error.what()},
             {"path", request.path}},
        {});
  }
}

HttpResponse HostdHttpService::HandleDiskRuntimeStateLoad(
    const std::string& db_path,
    const HttpRequest& request) const {
  if (request.method != "POST") {
    return support_.build_json_response(405, json{{"status", "method_not_allowed"}}, {});
  }
  try {
    HostdRequestContext context(support_, db_path);
    const auto authenticated = context.Authenticate(request);
    if (!authenticated.has_value()) {
      return context.Json(
          403,
          json{{"status", "forbidden"},
               {"message", "invalid or missing host session"}});
    }
    auto host = *authenticated;
    const json body =
        context.ParseEncryptedBody(request, &host, "disk-runtime-state/load");
    const std::string disk_name = body.value("disk_name", std::string{});
    const std::string node_name = body.value("node_name", std::string{});
    if (disk_name.empty() || node_name.empty()) {
      return context.Json(
          400,
          json{{"status", "bad_request"},
               {"message", "missing disk_name or node_name"}});
    }
    if (host.node_name != node_name) {
      return context.Json(
          403,
          json{{"status", "forbidden"},
               {"message", "node mismatch for disk runtime load"}});
    }
    const auto runtime_state =
        context.store().LoadDiskRuntimeState(disk_name, node_name);
    return context.EncryptedResponse(
        &host,
        "disk-runtime-state/load",
        json{
            {"service", "naim-controller"},
            {"runtime_state",
             runtime_state.has_value()
                 ? BuildDiskRuntimeStatePayloadItem(*runtime_state)
                 : json(nullptr)},
        });
  } catch (const std::exception& error) {
    return support_.build_json_response(
        500,
        json{{"status", "internal_error"},
             {"message", error.what()},
             {"path", request.path}},
        {});
  }
}
