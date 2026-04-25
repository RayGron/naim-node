#pragma once

#include <map>
#include <optional>
#include <string>

#include <nlohmann/json.hpp>

#include "http/controller_http_transport.h"
#include "http/controller_http_types.h"

namespace naim::controller {

struct KnowledgeVaultServiceRecord {
  std::string service_id = "kv_default";
  std::string node_name;
  std::string image;
  std::string endpoint_host = "127.0.0.1";
  int endpoint_port = 18200;
  std::string desired_state_json = "{}";
  std::string status = "stopped";
  std::string status_message;
  std::string schema_version;
  std::string index_epoch;
  int latest_event_sequence = 0;
};

class KnowledgeVaultService final {
 public:
  nlohmann::json BuildStatus(const std::string& db_path) const;
  HttpResponse ApplyService(const std::string& db_path, const HttpRequest& request) const;
  HttpResponse StopService(const std::string& db_path, const HttpRequest& request) const;
  HttpResponse ProxyServiceRequest(
      const std::string& db_path,
      const HttpRequest& request,
      const std::string& upstream_path) const;
  static HttpResponse BuildJsonResponse(int status_code, const nlohmann::json& payload);

 private:
  static nlohmann::json ParseJsonBody(const HttpRequest& request);
  static std::string DefaultKnowledgeImage();
  static std::string HeaderValue(const HttpRequest& request, const std::string& key);
  std::string SelectStorageNode(const std::string& db_path, const nlohmann::json& request) const;
  std::string LoadStorageRoot(
      const std::string& db_path,
      const std::string& node_name) const;
  std::string LoadNodeEndpointHost(
      const std::string& db_path,
      const std::string& node_name) const;
  HttpResponse SendDirectRuntime(
      const KnowledgeVaultServiceRecord& record,
      const std::string& method,
      const std::string& upstream_path,
      const std::string& body,
      const std::map<std::string, std::string>& headers) const;
};

}  // namespace naim::controller
