#include "knowledge/knowledge_vault_http_service.h"

#include <string>
#include <utility>

namespace naim::controller {
namespace {

bool IsPlaneScopedKnowledgeRoute(const std::string& path) {
  constexpr const char* kPrefix = "/api/v1/knowledge-vault/";
  if (path.rfind(kPrefix, 0) != 0) {
    return false;
  }
  const std::string suffix = path.substr(std::string(kPrefix).size());
  return suffix == "search" ||
         suffix == "context" ||
         suffix == "query-route" ||
         suffix == "source-ingest" ||
         suffix == "graph-neighborhood";
}

nlohmann::json SelectedKnowledgeIds(const naim::DesiredState& desired_state) {
  nlohmann::json selected = nlohmann::json::array();
  if (!desired_state.knowledge.has_value()) {
    return selected;
  }
  for (const auto& knowledge_id : desired_state.knowledge->selected_knowledge_ids) {
    if (!knowledge_id.empty()) {
      selected.push_back(knowledge_id);
    }
  }
  return selected;
}

bool MissingOrEmptyArray(const nlohmann::json& value, const char* key) {
  if (!value.contains(key)) {
    return true;
  }
  return value.at(key).is_array() && value.at(key).empty();
}

}  // namespace

KnowledgeVaultHttpService::KnowledgeVaultHttpService(KnowledgeVaultService service)
    : service_(std::move(service)) {}

std::optional<HttpResponse> KnowledgeVaultHttpService::HandleRequest(
    const std::string& db_path,
    const HttpRequest& request) const {
  constexpr const char* kPrefix = "/api/v1/knowledge-vault";
  if (!StartsWith(request.path, kPrefix)) {
    return std::nullopt;
  }
  try {
    if (request.path == std::string(kPrefix) + "/status" && request.method == "GET") {
      return KnowledgeVaultService::BuildJsonResponse(200, service_.BuildStatus(db_path));
    }
    if (request.path == std::string(kPrefix) + "/apply" && request.method == "POST") {
      return service_.ApplyService(db_path, request);
    }
    if (request.path == std::string(kPrefix) + "/stop" && request.method == "POST") {
      return service_.StopService(db_path, request);
    }
    const std::string suffix = StripPrefix(request.path, kPrefix);
    if (suffix.rfind("/blocks", 0) == 0 ||
        suffix.rfind("/heads", 0) == 0 ||
        suffix.rfind("/relations", 0) == 0 ||
        suffix.rfind("/search", 0) == 0 ||
        suffix.rfind("/context", 0) == 0 ||
        suffix.rfind("/query-route", 0) == 0 ||
        suffix.rfind("/source-ingest", 0) == 0 ||
        suffix.rfind("/capsules", 0) == 0 ||
        suffix.rfind("/overlays", 0) == 0 ||
        suffix.rfind("/replica-merges", 0) == 0 ||
        suffix.rfind("/reviews", 0) == 0 ||
        suffix.rfind("/repair", 0) == 0 ||
        suffix.rfind("/jobs", 0) == 0 ||
        suffix.rfind("/markdown-export", 0) == 0 ||
        suffix.rfind("/markdown-import", 0) == 0 ||
        suffix.rfind("/graph-neighborhood", 0) == 0 ||
        suffix.rfind("/catalog", 0) == 0) {
      return service_.ProxyServiceRequest(db_path, request, "/v1" + suffix);
    }
    return KnowledgeVaultService::BuildJsonResponse(
        404,
        nlohmann::json{{"status", "not_found"}, {"message", "knowledge vault route not found"}});
  } catch (const std::exception& error) {
    return KnowledgeVaultService::BuildJsonResponse(
        500,
        nlohmann::json{{"status", "internal_error"}, {"message", error.what()}});
  }
}

std::optional<HttpResponse> KnowledgeVaultHttpService::HandlePlaneRequest(
    const std::string& db_path,
    const HttpRequest& request,
    const naim::DesiredState& desired_state,
    const std::string& plane_name) const {
  return HandleRequest(
      db_path,
      BuildPlaneScopedRequest(request, desired_state, plane_name));
}

HttpRequest KnowledgeVaultHttpService::BuildPlaneScopedRequest(
    const HttpRequest& request,
    const naim::DesiredState& desired_state,
    const std::string& plane_name) {
  HttpRequest rewritten = request;
  constexpr const char* kPlanePrefix = "/api/v1/planes/";
  if (request.path.rfind(kPlanePrefix, 0) != 0) {
    return rewritten;
  }
  const std::string remainder = request.path.substr(std::string(kPlanePrefix).size());
  const auto separator = remainder.find('/');
  if (separator == std::string::npos) {
    return rewritten;
  }
  const std::string suffix = remainder.substr(separator);
  rewritten.path = "/api/v1" + suffix;

  if (request.method != "POST" || !IsPlaneScopedKnowledgeRoute(rewritten.path)) {
    return rewritten;
  }

  nlohmann::json body = nlohmann::json::object();
  if (!request.body.empty()) {
    body = nlohmann::json::parse(request.body, nullptr, false);
    if (body.is_discarded() || !body.is_object()) {
      return rewritten;
    }
  }

  body["plane_id"] = plane_name;
  const auto selected_ids = SelectedKnowledgeIds(desired_state);
  if (rewritten.path == "/api/v1/knowledge-vault/context" &&
      !selected_ids.empty() &&
      MissingOrEmptyArray(body, "selected_knowledge_ids")) {
    body["selected_knowledge_ids"] = selected_ids;
  }
  if (rewritten.path == "/api/v1/knowledge-vault/graph-neighborhood" &&
      !selected_ids.empty() &&
      MissingOrEmptyArray(body, "knowledge_ids")) {
    body["knowledge_ids"] = selected_ids;
  }
  rewritten.body = body.dump();
  rewritten.headers["Content-Type"] = "application/json";
  return rewritten;
}

bool KnowledgeVaultHttpService::StartsWith(
    const std::string& value,
    const std::string& prefix) {
  return value.rfind(prefix, 0) == 0;
}

std::string KnowledgeVaultHttpService::StripPrefix(
    const std::string& value,
    const std::string& prefix) {
  return value.substr(prefix.size());
}

}  // namespace naim::controller
