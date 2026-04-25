#include "knowledge/knowledge_vault_service.h"

#include <cstdlib>
#include <sstream>
#include <stdexcept>
#include <vector>

#include "http/controller_http_server_support.h"
#include "knowledge/knowledge_vault_service_repository.h"
#include "model/model_library_node_placement.h"
#include "naim/state/sqlite_store.h"

namespace naim::controller {
namespace {

bool IsSelectedStorageNode(const ModelLibraryNodeSummary& summary) {
  return summary.registration_state == "registered" &&
         summary.session_state == "connected" &&
         !summary.storage_root.empty() &&
         (summary.derived_role == "storage" || summary.storage_role_enabled);
}

std::string KnowledgeVaultPlaneName(const std::string& service_id) {
  return "knowledge-vault:" + service_id;
}

nlohmann::json SerializeApplyAssignment(const naim::HostAssignment& assignment) {
  return nlohmann::json{
      {"id", assignment.id},
      {"status", naim::ToString(assignment.status)},
      {"attempt_count", assignment.attempt_count},
      {"max_attempts", assignment.max_attempts},
      {"status_message", assignment.status_message},
  };
}

std::optional<naim::HostAssignment> LoadLatestApplyAssignment(
    const std::string& db_path,
    const KnowledgeVaultServiceRecord& record) {
  naim::ControllerStore store(db_path);
  store.Initialize();
  const auto assignments = store.LoadHostAssignments(
      record.node_name,
      std::nullopt,
      KnowledgeVaultPlaneName(record.service_id));
  for (auto it = assignments.rbegin(); it != assignments.rend(); ++it) {
    if (it->assignment_type == "knowledge-vault-apply") {
      return *it;
    }
  }
  return std::nullopt;
}

bool RuntimeProbeShouldWaitForApply(
    const std::string& db_path,
    const KnowledgeVaultServiceRecord& record,
    nlohmann::json& status) {
  const auto apply_assignment = LoadLatestApplyAssignment(db_path, record);
  if (!apply_assignment.has_value() ||
      apply_assignment->status == naim::HostAssignmentStatus::Applied) {
    return false;
  }

  status["apply_assignment"] = SerializeApplyAssignment(*apply_assignment);
  if (apply_assignment->status == naim::HostAssignmentStatus::Failed) {
    status["status"] = "failed";
    status["runtime_error"] = apply_assignment->status_message;
  }
  return true;
}

bool CachedStatusIsUsable(const KnowledgeVaultServiceRecord& record) {
  return record.status == "ready" && !record.schema_version.empty();
}

std::string NormalizeEndpointHost(std::string value) {
  if (value.rfind("http://", 0) == 0) {
    value = value.substr(7);
  }
  const auto slash = value.find('/');
  if (slash != std::string::npos) {
    value = value.substr(0, slash);
  }
  const auto colon = value.rfind(':');
  if (colon != std::string::npos) {
    value = value.substr(0, colon);
  }
  return value;
}

}  // namespace

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
      {"endpoint", "http://" + record->endpoint_host + ":" + std::to_string(record->endpoint_port)},
      {"transport", "direct-http"},
      {"schema_version", record->schema_version.empty() ? nlohmann::json(nullptr) : nlohmann::json(record->schema_version)},
      {"index_epoch", record->index_epoch.empty() ? nlohmann::json(nullptr) : nlohmann::json(record->index_epoch)},
      {"latest_event_sequence", record->latest_event_sequence},
      {"managed_by", "naim-controller"},
      {"runtime_entity", "separate-container"},
  };

  if (RuntimeProbeShouldWaitForApply(db_path, *record, status)) {
    return status;
  }
  if (CachedStatusIsUsable(*record)) {
    return status;
  }

  try {
    const auto live = SendDirectRuntime(
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
  record.endpoint_host = body.value(
      "endpoint_host",
      LoadNodeEndpointHost(db_path, record.node_name));
  record.endpoint_host = NormalizeEndpointHost(record.endpoint_host);
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
  assignment.plane_name = KnowledgeVaultPlaneName(record.service_id);
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
  assignment.plane_name = KnowledgeVaultPlaneName(record->service_id);
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
  return SendDirectRuntime(
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
             : std::string("chainzano.com/naim/knowledge-runtime:latest");
}

std::string KnowledgeVaultService::HeaderValue(
    const HttpRequest& request,
    const std::string& key) {
  const auto it = request.headers.find(key);
  return it == request.headers.end() ? std::string{} : it->second;
}

std::string KnowledgeVaultService::SelectStorageNode(
    const std::string& db_path,
    const nlohmann::json& request) const {
  const std::string requested = request.value("node_name", std::string{});
  naim::ControllerStore store(db_path);
  store.Initialize();
  std::string first_auto_storage;
  std::string first_manual_storage;
  for (const auto& host : store.LoadRegisteredHosts()) {
    const auto summary = ModelLibraryNodePlacement::BuildSummary(host);
    if (!IsSelectedStorageNode(summary)) {
      continue;
    }
    if (summary.derived_role == "storage" && first_auto_storage.empty()) {
      first_auto_storage = summary.node_name;
    }
    if (summary.storage_role_enabled && first_manual_storage.empty()) {
      first_manual_storage = summary.node_name;
    }
    if (!requested.empty() && requested == summary.node_name) {
      return requested;
    }
  }
  if (!requested.empty()) {
    throw std::runtime_error(
        "requested knowledge storage node is not connected or selected for storage");
  }
  if (!first_auto_storage.empty()) {
    return first_auto_storage;
  }
  if (!first_manual_storage.empty()) {
    return first_manual_storage;
  }
  throw std::runtime_error("no connected storage node is selected for Knowledge Vault");
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

std::string KnowledgeVaultService::LoadNodeEndpointHost(
    const std::string& db_path,
    const std::string& node_name) const {
  naim::ControllerStore store(db_path);
  store.Initialize();
  for (const auto& host : store.LoadRegisteredHosts()) {
    if (host.node_name == node_name) {
      if (!host.advertised_address.empty()) {
        return NormalizeEndpointHost(host.advertised_address);
      }
      return NormalizeEndpointHost(host.node_name);
    }
  }
  throw std::runtime_error("knowledge vault node endpoint host is not available");
}

HttpResponse KnowledgeVaultService::SendDirectRuntime(
    const KnowledgeVaultServiceRecord& record,
    const std::string& method,
    const std::string& upstream_path,
    const std::string& body,
    const std::map<std::string, std::string>& headers) const {
  std::vector<std::pair<std::string, std::string>> header_pairs;
  for (const auto& [key, value] : headers) {
    header_pairs.emplace_back(key, value);
  }
  return SendControllerHttpRequest(
      ParseControllerEndpointTarget(
          record.endpoint_host + ":" + std::to_string(record.endpoint_port)),
      method,
      upstream_path,
      body,
      header_pairs);
}

}  // namespace naim::controller
