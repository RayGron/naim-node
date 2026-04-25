#pragma once

#include <optional>
#include <string>
#include <vector>

#include <nlohmann/json.hpp>

#include "http/controller_http_transport.h"
#include "http/controller_http_types.h"

namespace naim::controller {

struct ProtocolRegistryItem {
  std::string protocol_id;
  std::string owner;
  std::string transport;
  std::string auth;
  std::string latency_class;
  std::string retry_semantics;
  std::string ordering;
  std::string timeout;
  std::string fallback;
  std::string slo;
  std::string status;
  nlohmann::json capabilities = nlohmann::json::object();
};

class ProtocolRegistryService final {
 public:
  std::optional<HttpResponse> HandleRequest(const HttpRequest& request) const;

  nlohmann::json BuildPayload() const;
  nlohmann::json BuildItemPayload(const std::string& protocol_id) const;
  std::vector<ProtocolRegistryItem> Items() const;

 private:
  static HttpResponse JsonResponse(int status_code, const nlohmann::json& payload);
  static nlohmann::json ItemToJson(const ProtocolRegistryItem& item);
};

}  // namespace naim::controller
