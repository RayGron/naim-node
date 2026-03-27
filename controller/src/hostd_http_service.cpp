#include "../include/hostd_http_service.h"

#include <algorithm>
#include <cstdint>
#include <stdexcept>
#include <string>
#include <utility>

#include "../include/controller_action.h"

#include "comet/crypto_utils.h"
#include "comet/models.h"
#include "comet/sqlite_store.h"

using nlohmann::json;

namespace {

bool StartsWithPathPrefix(const std::string& path, const std::string& prefix) {
  return path.rfind(prefix, 0) == 0;
}

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

json BuildAssignmentPayloadItem(const comet::HostAssignment& assignment) {
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
      {"status", comet::ToString(assignment.status)},
      {"status_message", assignment.status_message},
      {"progress", progress},
  };
}

comet::HostObservation ParseHostObservationPayload(const json& payload) {
  comet::HostObservation observation;
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
  observation.status = comet::ParseHostObservationStatus(
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

json BuildDiskRuntimeStatePayloadItem(const comet::DiskRuntimeState& state) {
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

comet::DiskRuntimeState ParseDiskRuntimeStatePayload(const json& payload) {
  comet::DiskRuntimeState state;
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

std::map<std::string, comet::HostAssignment> BuildLatestPlaneAssignmentsByNode(
    const std::vector<comet::HostAssignment>& assignments) {
  std::map<std::string, comet::HostAssignment> latest;
  for (const auto& assignment : assignments) {
    auto it = latest.find(assignment.node_name);
    if (it == latest.end() || it->second.id < assignment.id) {
      latest[assignment.node_name] = assignment;
    }
  }
  return latest;
}

}  // namespace

HostdHttpService::HostdHttpService(Deps deps) : deps_(std::move(deps)) {}

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
  return deps_.build_json_response(404, json{{"status", "not_found"}}, {});
}

namespace {

class HostdRequestContext {
 public:
  HostdRequestContext(
      const HostdHttpService::Deps& deps,
      const std::string& db_path)
      : deps_(deps), db_path_(db_path), store_(db_path) {
    store_.Initialize();
  }

  comet::ControllerStore& store() { return store_; }

  HttpResponse Json(
      int status_code,
      const json& payload,
      const std::map<std::string, std::string>& headers = {}) const {
    return deps_.build_json_response(status_code, payload, headers);
  }

  std::optional<comet::RegisteredHostRecord> Authenticate(
      const HttpRequest& request,
      const std::optional<std::string>& expected_node_name = std::nullopt) {
    const auto token_it = request.headers.find("x-comet-host-session");
    if (token_it == request.headers.end() || token_it->second.empty()) {
      return std::nullopt;
    }
    const auto node_name_it = request.headers.find("x-comet-host-node");
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
          deps_.timestamp_age_seconds(host->session_expires_at);
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
      comet::RegisteredHostRecord* host,
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
    const comet::EncryptedEnvelope envelope{
        body.value("nonce", std::string{}),
        body.value("ciphertext", std::string{}),
    };
    const std::string decrypted = comet::DecryptEnvelopeBase64(
        envelope,
        host->session_token,
        BuildHostRequestAad(message_type, host->node_name, sequence_number));
    host->session_host_sequence = sequence_number;
    host->session_expires_at = deps_.sql_timestamp_after_seconds(600);
    store_.UpsertRegisteredHost(*host);
    if (decrypted.empty()) {
      return json::object();
    }
    return json::parse(decrypted);
  }

  HttpResponse EncryptedResponse(
      comet::RegisteredHostRecord* host,
      const std::string& message_type,
      const json& payload) {
    host->session_controller_sequence += 1;
    store_.UpsertRegisteredHost(*host);
    const comet::EncryptedEnvelope envelope = comet::EncryptEnvelopeBase64(
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

  comet::controller::HostRegistryService MakeHostRegistryService() const {
    return comet::controller::HostRegistryService(
        db_path_, deps_.host_registry_event_sink);
  }

  void EmitHostRegistryEvent(
      const std::string& event_type,
      const std::string& message,
      const json& payload,
      const std::string& node_name,
      const std::string& severity) {
    deps_.host_registry_event_sink(
        store_, event_type, message, payload, node_name, severity);
  }

  const HostdHttpService::Deps& deps() const { return deps_; }

 private:
  const HostdHttpService::Deps& deps_;
  std::string db_path_;
  comet::ControllerStore store_;
};

}  // namespace

HttpResponse HostdHttpService::HandleRegister(
    const std::string& db_path,
    const HttpRequest& request) const {
  if (request.method != "POST") {
    return deps_.build_json_response(405, json{{"status", "method_not_allowed"}}, {});
  }
  try {
    const json body = ParseJsonBody(request);
    const std::string node_name = body.value("node_name", std::string{});
    if (node_name.empty()) {
      return deps_.build_json_response(
          400,
          json{{"status", "bad_request"},
               {"message", "missing required field 'node_name'"}},
          {});
    }
    HostdRequestContext context(deps_, db_path);
    comet::RegisteredHostRecord host;
    if (const auto current = context.store().LoadRegisteredHost(node_name);
        current.has_value()) {
      host = *current;
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
    comet::ParseHostExecutionMode(host.execution_mode);
    host.registration_state = body.value(
        "registration_state",
        host.registration_state.empty() ? "registered" : host.registration_state);
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
        json{{"service", "comet-controller"},
             {"node_name", node_name},
             {"registration_state", host.registration_state}});
  } catch (const std::exception& error) {
    return deps_.build_json_response(
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
  if (request.method != "GET") {
    return deps_.build_json_response(405, json{{"status", "method_not_allowed"}}, {});
  }
  try {
    HostdRequestContext context(deps_, db_path);
    return context.Json(
        200,
        context.MakeHostRegistryService().BuildPayload(
            FindQueryStringValue(request, "node")));
  } catch (const std::exception& error) {
    return deps_.build_json_response(
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
    return deps_.build_json_response(404, json{{"status", "not_found"}}, {});
  }
  const auto revoke_pos = remainder.find("/revoke");
  if (revoke_pos != std::string::npos &&
      revoke_pos + std::string("/revoke").size() == remainder.size()) {
    if (request.method != "POST") {
      return deps_.build_json_response(
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
          comet::controller::HostRegistryService(db_path, deps_.host_registry_event_sink);
      return deps_.build_json_response(
          200,
          comet::controller::BuildControllerActionPayload(
              comet::controller::RunControllerActionResult(
                  "revoke-hostd",
                  [&]() { return service.RevokeHost(node_name, message); })),
          {});
    } catch (const std::exception& error) {
      return deps_.build_json_response(
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
      return deps_.build_json_response(
          405, json{{"status", "method_not_allowed"}}, {});
    }
    const std::string node_name = remainder.substr(0, rotate_pos);
    try {
      const json body = ParseJsonBody(request);
      const std::string public_key_base64 =
          body.value("public_key_base64", std::string{});
      if (public_key_base64.empty()) {
        return deps_.build_json_response(
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
          comet::controller::HostRegistryService(db_path, deps_.host_registry_event_sink);
      return deps_.build_json_response(
          200,
          comet::controller::BuildControllerActionPayload(
              comet::controller::RunControllerActionResult(
                  "rotate-hostd-key",
                  [&]() {
                    return service.RotateHostKey(
                        node_name, public_key_base64, message);
                  })),
          {});
    } catch (const std::exception& error) {
      return deps_.build_json_response(
          500,
          json{{"status", "internal_error"},
               {"message", error.what()},
               {"path", request.path}},
          {});
    }
  }
  return deps_.build_json_response(404, json{{"status", "not_found"}}, {});
}

HttpResponse HostdHttpService::HandleSessionOpen(
    const std::string& db_path,
    const HttpRequest& request) const {
  if (request.method != "POST") {
    return deps_.build_json_response(405, json{{"status", "method_not_allowed"}}, {});
  }
  try {
    const json body = ParseJsonBody(request);
    const std::string node_name = body.value("node_name", std::string{});
    if (node_name.empty()) {
      return deps_.build_json_response(
          400,
          json{{"status", "bad_request"},
               {"message", "missing required field 'node_name'"}},
          {});
    }
    HostdRequestContext context(deps_, db_path);
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
    if (!comet::VerifyDetachedBase64(
            signed_message, signature, current->public_key_base64)) {
      return context.Json(
          403,
          json{{"status", "forbidden"},
               {"message", "invalid host session signature"}});
    }
    current->session_state = "connected";
    current->last_session_at = deps_.utc_now_sql_timestamp();
    current->session_token = comet::RandomTokenBase64(32);
    current->session_expires_at = deps_.sql_timestamp_after_seconds(600);
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
            {"service", "comet-controller"},
            {"node_name", node_name},
            {"session_state", current->session_state},
            {"last_session_at", current->last_session_at},
            {"session_token", current->session_token},
            {"controller_public_key_fingerprint",
             current->controller_public_key_fingerprint},
            {"controller_sequence", current->session_controller_sequence},
        });
  } catch (const std::exception& error) {
    return deps_.build_json_response(
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
    return deps_.build_json_response(405, json{{"status", "method_not_allowed"}}, {});
  }
  try {
    HostdRequestContext context(deps_, db_path);
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
    current.last_heartbeat_at = deps_.utc_now_sql_timestamp();
    current.last_session_at = current.last_heartbeat_at;
    current.status_message =
        decrypted.value("status_message", std::string("heartbeat"));
    context.store().UpsertRegisteredHost(current);
    return context.EncryptedResponse(
        &current,
        "session/heartbeat",
        json{
            {"service", "comet-controller"},
            {"node_name", node_name},
            {"session_state", current.session_state},
            {"last_heartbeat_at", current.last_heartbeat_at},
        });
  } catch (const std::exception& error) {
    return deps_.build_json_response(
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
    return deps_.build_json_response(405, json{{"status", "method_not_allowed"}}, {});
  }
  try {
    HostdRequestContext context(deps_, db_path);
    std::optional<std::string> node_name = FindQueryStringValue(request, "node");
    std::optional<comet::RegisteredHostRecord> authenticated;
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
        {"service", "comet-controller"},
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
    return deps_.build_json_response(
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
    return deps_.build_json_response(404, json{{"status", "not_found"}}, {});
  }
  const int assignment_id = std::stoi(remainder.substr(0, slash));
  const std::string action = remainder.substr(slash + 1);
  if (request.method != "POST") {
    return deps_.build_json_response(405, json{{"status", "method_not_allowed"}}, {});
  }
  try {
    HostdRequestContext context(deps_, db_path);
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
      return context.EncryptedResponse(
          &host,
          "assignments/" + std::to_string(assignment_id) + "/progress",
          json{{"service", "comet-controller"},
               {"updated", updated},
               {"assignment_id", assignment_id}});
    }
    if (action == "applied") {
      const bool updated = context.store().TransitionClaimedHostAssignment(
          assignment_id, comet::HostAssignmentStatus::Applied, status_message);
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
              return candidate.status == comet::HostAssignmentStatus::Applied ||
                     candidate.status == comet::HostAssignmentStatus::Superseded;
            });
        if (converged_generation) {
          context.store().UpdatePlaneAppliedGeneration(
              assignment->plane_name, assignment->desired_generation);
        }
      }
      return context.EncryptedResponse(
          &host,
          "assignments/" + std::to_string(assignment_id) + "/applied",
          json{{"service", "comet-controller"},
               {"updated", updated},
               {"assignment_id", assignment_id}});
    }
    if (action == "failed") {
      const bool retry = body.value("retry", false);
      const bool updated = retry
                               ? context.store().TransitionClaimedHostAssignment(
                                     assignment_id,
                                     comet::HostAssignmentStatus::Pending,
                                     status_message)
                               : context.store().TransitionClaimedHostAssignment(
                                     assignment_id,
                                     comet::HostAssignmentStatus::Failed,
                                     status_message);
      return context.EncryptedResponse(
          &host,
          "assignments/" + std::to_string(assignment_id) + "/failed",
          json{{"service", "comet-controller"},
               {"updated", updated},
               {"assignment_id", assignment_id},
               {"retry", retry}});
    }
    return context.Json(404, json{{"status", "not_found"}});
  } catch (const std::exception& error) {
    return deps_.build_json_response(
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
    return deps_.build_json_response(405, json{{"status", "method_not_allowed"}}, {});
  }
  try {
    HostdRequestContext context(deps_, db_path);
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
    return context.EncryptedResponse(
        &host,
        "observations/upsert",
        json{{"service", "comet-controller"},
             {"node_name", observation.node_name},
             {"updated", true}});
  } catch (const std::exception& error) {
    return deps_.build_json_response(
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
    return deps_.build_json_response(405, json{{"status", "method_not_allowed"}}, {});
  }
  try {
    HostdRequestContext context(deps_, db_path);
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
    context.store().AppendEvent(comet::EventRecord{
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
        json{{"service", "comet-controller"}, {"appended", true}});
  } catch (const std::exception& error) {
    return deps_.build_json_response(
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
    HostdRequestContext context(deps_, db_path);
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
          json{{"service", "comet-controller"},
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
          json{{"service", "comet-controller"},
               {"updated", true},
               {"disk_name", runtime_state.disk_name}});
    }
    return context.Json(405, json{{"status", "method_not_allowed"}});
  } catch (const std::exception& error) {
    return deps_.build_json_response(
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
    return deps_.build_json_response(405, json{{"status", "method_not_allowed"}}, {});
  }
  try {
    HostdRequestContext context(deps_, db_path);
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
            {"service", "comet-controller"},
            {"runtime_state",
             runtime_state.has_value()
                 ? BuildDiskRuntimeStatePayloadItem(*runtime_state)
                 : json(nullptr)},
        });
  } catch (const std::exception& error) {
    return deps_.build_json_response(
        500,
        json{{"status", "internal_error"},
             {"message", error.what()},
             {"path", request.path}},
        {});
  }
}
