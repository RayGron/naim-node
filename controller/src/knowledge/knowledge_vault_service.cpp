#include "knowledge/knowledge_vault_service.h"

#include <chrono>
#include <cstdlib>
#include <sstream>
#include <stdexcept>
#include <thread>

#include "http/controller_http_server_support.h"
#include "knowledge/knowledge_vault_service_repository.h"
#include "model/model_library_node_placement.h"
#include "naim/state/sqlite_store.h"

namespace naim::controller {

nlohmann::json KnowledgeVaultService::BuildStatus(const std::string& db_path) const {
  const KnowledgeVaultServiceRepository repository;
  const auto record = repository.LoadService(db_path, "kv_default");
  if (!record.has_value()) {
    return nlohmann::json{
        {"service_id", "kv_default"},
        {"status", "stopped"},
        {"managed_by", "naim-controller"},
        {"runtime_entity", "separate-container"},
    };
  }

  nlohmann::json status{
      {"service_id", record->service_id},
      {"status", record->status},
      {"status_message", record->status_message},
      {"node_name", record->node_name},
      {"image", record->image},
      {"endpoint", "hostd-relay://" + record->node_name + ":" + std::to_string(record->endpoint_port)},
      {"schema_version", record->schema_version.empty() ? nlohmann::json(nullptr) : nlohmann::json(record->schema_version)},
      {"index_epoch", record->index_epoch.empty() ? nlohmann::json(nullptr) : nlohmann::json(record->index_epoch)},
      {"latest_event_sequence", record->latest_event_sequence},
      {"managed_by", "naim-controller"},
      {"runtime_entity", "separate-container"},
  };

  try {
    const auto live = SendViaHostdProxy(
        db_path,
        *record,
        "GET",
        "/v1/status",
        "",
        {});
    if (live.status_code >= 200 && live.status_code < 300) {
      const auto payload = nlohmann::json::parse(live.body, nullptr, false);
      if (!payload.is_discarded() && payload.is_object()) {
        repository.UpdateServiceStatus(db_path, record->service_id, payload);
        status["runtime"] = payload;
        status["status"] = payload.value("status", std::string("ready"));
      }
    }
  } catch (const std::exception& error) {
    status["runtime_error"] = error.what();
  }
  return status;
}

HttpResponse KnowledgeVaultService::ApplyService(
    const std::string& db_path,
    const HttpRequest& request) const {
  const auto body = ParseJsonBody(request);
  KnowledgeVaultServiceRecord record;
  record.service_id = body.value("service_id", std::string("kv_default"));
  record.node_name = SelectStorageNode(db_path, body);
  const std::string storage_root = LoadStorageRoot(db_path, record.node_name);
  record.image = body.value("image", DefaultKnowledgeImage());
  record.endpoint_host = "127.0.0.1";
  record.endpoint_port = body.value("port", 18200);
  record.status = "starting";
  record.desired_state_json = nlohmann::json{
      {"service_id", record.service_id},
      {"node_name", record.node_name},
      {"image", record.image},
      {"endpoint_host", record.endpoint_host},
      {"endpoint_port", record.endpoint_port},
      {"storage_root", storage_root},
  }.dump();
  KnowledgeVaultServiceRepository{}.UpsertService(db_path, record);

  naim::HostAssignment assignment;
  assignment.node_name = record.node_name;
  assignment.plane_name = "knowledge-vault:" + record.service_id;
  assignment.desired_generation = 0;
  assignment.max_attempts = 3;
  assignment.assignment_type = "knowledge-vault-apply";
  assignment.desired_state_json = record.desired_state_json;
  assignment.artifacts_root = "";
  assignment.status = naim::HostAssignmentStatus::Pending;
  assignment.status_message = "apply knowledge vault service";

  naim::ControllerStore store(db_path);
  store.Initialize();
  store.EnqueueHostAssignments({assignment}, "superseded by knowledge vault apply");
  return BuildJsonResponse(
      202,
      nlohmann::json{
          {"service_id", record.service_id},
          {"status", "queued"},
          {"node_name", record.node_name},
          {"image", record.image},
      });
}

HttpResponse KnowledgeVaultService::StopService(
    const std::string& db_path,
    const HttpRequest& request) const {
  const auto body = ParseJsonBody(request);
  const std::string service_id = body.value("service_id", std::string("kv_default"));
  const auto record = KnowledgeVaultServiceRepository{}.LoadService(db_path, service_id);
  if (!record.has_value()) {
    return BuildJsonResponse(
        404,
        nlohmann::json{{"status", "not_found"}, {"message", "knowledge vault service not found"}});
  }

  naim::HostAssignment assignment;
  assignment.node_name = record->node_name;
  assignment.plane_name = "knowledge-vault:" + record->service_id;
  assignment.desired_generation = 0;
  assignment.max_attempts = 3;
  assignment.assignment_type = "knowledge-vault-stop";
  assignment.desired_state_json = record->desired_state_json;
  assignment.artifacts_root = "";
  assignment.status = naim::HostAssignmentStatus::Pending;
  assignment.status_message = "stop knowledge vault service";

  naim::ControllerStore store(db_path);
  store.Initialize();
  store.EnqueueHostAssignments({assignment}, "superseded by knowledge vault stop");
  return BuildJsonResponse(
      202,
      nlohmann::json{{"service_id", service_id}, {"status", "queued"}, {"node_name", record->node_name}});
}

HttpResponse KnowledgeVaultService::ProxyServiceRequest(
    const std::string& db_path,
    const HttpRequest& request,
    const std::string& upstream_path) const {
  const auto record = KnowledgeVaultServiceRepository{}.LoadService(db_path, "kv_default");
  if (!record.has_value()) {
    return BuildJsonResponse(
        404,
        nlohmann::json{{"status", "not_found"}, {"message", "knowledge vault service not configured"}});
  }
  std::map<std::string, std::string> headers;
  const std::string content_type = HeaderValue(request, "Content-Type");
  if (!content_type.empty()) {
    headers["Content-Type"] = content_type;
  }
  std::string path = upstream_path;
  bool first_query_param = true;
  for (const auto& [key, value] : request.query_params) {
    path += first_query_param ? "?" : "&";
    first_query_param = false;
    path += ControllerHttpServerSupport::UrlEncode(key);
    path += "=";
    path += ControllerHttpServerSupport::UrlEncode(value);
  }
  return SendViaHostdProxy(
      db_path,
      *record,
      request.method,
      path,
      request.body,
      headers);
}

HttpResponse KnowledgeVaultService::BuildJsonResponse(
    int status_code,
    const nlohmann::json& payload) {
  HttpResponse response;
  response.status_code = status_code;
  response.content_type = "application/json";
  response.body = payload.dump();
  response.headers["Cache-Control"] = "no-store";
  return response;
}

nlohmann::json KnowledgeVaultService::ParseJsonBody(const HttpRequest& request) {
  if (request.body.empty()) {
    return nlohmann::json::object();
  }
  const auto parsed = nlohmann::json::parse(request.body, nullptr, false);
  if (parsed.is_discarded()) {
    throw std::runtime_error("request body is not valid JSON");
  }
  return parsed;
}

std::string KnowledgeVaultService::DefaultKnowledgeImage() {
  const char* image = std::getenv("NAIM_KNOWLEDGE_IMAGE");
  return image != nullptr && *image != '\0'
             ? std::string(image)
             : std::string("chainzano.com/naim/knowledge-runtime:dev");
}

std::string KnowledgeVaultService::HeaderValue(
    const HttpRequest& request,
    const std::string& key) {
  const auto it = request.headers.find(key);
  return it == request.headers.end() ? std::string{} : it->second;
}

std::string KnowledgeVaultService::NewRequestId() {
  const auto now = std::chrono::system_clock::now().time_since_epoch().count();
  return "kvp_" + std::to_string(now);
}

std::string KnowledgeVaultService::SelectStorageNode(
    const std::string& db_path,
    const nlohmann::json& request) const {
  const std::string requested = request.value("node_name", std::string{});
  naim::ControllerStore store(db_path);
  store.Initialize();
  std::string first_eligible;
  for (const auto& host : store.LoadRegisteredHosts()) {
    const auto summary = ModelLibraryNodePlacement::BuildSummary(host);
    const bool eligible =
        summary.session_state == "connected" &&
        ModelLibraryNodePlacement::AllowsModelPlacementRole(
            summary.derived_role,
            summary.storage_role_enabled,
            false);
    if (!eligible) {
      continue;
    }
    if (first_eligible.empty()) {
      first_eligible = summary.node_name;
    }
    if (!requested.empty() && requested == summary.node_name) {
      return requested;
    }
  }
  if (!requested.empty()) {
    throw std::runtime_error("requested knowledge storage node is not connected or storage-capable");
  }
  if (first_eligible.empty()) {
    throw std::runtime_error("no connected storage-capable node is available");
  }
  return first_eligible;
}

std::string KnowledgeVaultService::LoadStorageRoot(
    const std::string& db_path,
    const std::string& node_name) const {
  naim::ControllerStore store(db_path);
  store.Initialize();
  for (const auto& host : store.LoadRegisteredHosts()) {
    const auto summary = ModelLibraryNodePlacement::BuildSummary(host);
    if (summary.node_name == node_name) {
      return summary.storage_root;
    }
  }
  return {};
}

HttpResponse KnowledgeVaultService::SendViaHostdProxy(
    const std::string& db_path,
    const KnowledgeVaultServiceRecord& record,
    const std::string& method,
    const std::string& upstream_path,
    const std::string& body,
    const std::map<std::string, std::string>& headers) const {
  const std::string relay_id = NewRequestId();
  nlohmann::json header_pairs = nlohmann::json::array();
  for (const auto& [key, value] : headers) {
    header_pairs.push_back(nlohmann::json::array({key, value}));
  }
  const nlohmann::json payload{
      {"relay_id", relay_id},
      {"service_id", record.service_id},
      {"target_host", record.endpoint_host},
      {"target_port", record.endpoint_port},
      {"method", method},
      {"path", upstream_path},
      {"body", body},
      {"headers", header_pairs},
  };
  naim::HostAssignment assignment;
  assignment.node_name = record.node_name;
  assignment.plane_name = "knowledge-proxy:" + relay_id;
  assignment.desired_generation = 0;
  assignment.max_attempts = 1;
  assignment.assignment_type = "knowledge-vault-http-proxy";
  assignment.desired_state_json = payload.dump();
  assignment.artifacts_root = "";
  assignment.status = naim::HostAssignmentStatus::Pending;
  assignment.status_message = "knowledge vault proxy request";

  naim::ControllerStore store(db_path);
  store.Initialize();
  store.EnqueueHostAssignments({assignment});

  const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(60);
  while (std::chrono::steady_clock::now() < deadline) {
    const auto assignments =
        store.LoadHostAssignments(record.node_name, std::nullopt, "knowledge-proxy:" + relay_id);
    if (!assignments.empty()) {
      const auto& current = assignments.back();
      if (current.status == naim::HostAssignmentStatus::Applied) {
        const auto progress = nlohmann::json::parse(current.progress_json, nullptr, false);
        if (!progress.is_discarded() && progress.value("phase", std::string{}) == "response-ready") {
          HttpResponse response;
          response.status_code = progress.value("status_code", 502);
          response.content_type = progress.value("content_type", std::string("application/json"));
          response.body = progress.value("body", std::string{});
          return response;
        }
      }
      if (current.status == naim::HostAssignmentStatus::Failed) {
        return BuildJsonResponse(
            502,
            nlohmann::json{{"status", "proxy_failed"}, {"message", current.status_message}});
      }
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(250));
  }
  return BuildJsonResponse(
      504,
      nlohmann::json{{"status", "proxy_timeout"}, {"message", "knowledge vault proxy timed out"}});
}

}  // namespace naim::controller
