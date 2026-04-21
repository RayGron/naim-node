#include "knowledge/knowledge_vault_http_service.h"

#include <string>
#include <utility>

namespace naim::controller {

namespace {

bool StartsWith(const std::string& value, const std::string& prefix) {
  return value.rfind(prefix, 0) == 0;
}

std::string StripPrefix(const std::string& value, const std::string& prefix) {
  return value.substr(prefix.size());
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
        suffix.rfind("/capsules", 0) == 0 ||
        suffix.rfind("/overlays", 0) == 0 ||
        suffix.rfind("/replica-merges", 0) == 0) {
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

}  // namespace naim::controller
