#pragma once

#include <map>
#include <string>

#include <nlohmann/json.hpp>

namespace naim::hostd {

enum class HostdRuntimeProxyPolicy {
  Runtime,
  KnowledgeVault,
};

struct HostdRuntimeHttpResponse {
  int status_code = 502;
  std::string content_type = "application/json";
  std::string body;
  std::map<std::string, std::string> headers;
};

class HostdRuntimeHttpProxy final {
 public:
  std::map<std::string, std::string> ParseProxyHeaders(
      const nlohmann::json& headers_json) const;
  HostdRuntimeHttpResponse Send(
      const std::string& host,
      int port,
      const std::string& method,
      const std::string& path,
      const std::string& body,
      const std::map<std::string, std::string>& headers,
      HostdRuntimeProxyPolicy policy) const;

 private:
  static std::string TrimAscii(std::string value);
  static std::string LowercaseAscii(std::string value);
  static bool IsLoopbackRuntimeHost(const std::string& host);
  static bool IsAllowedRuntimeProxyPath(
      const std::string& method,
      const std::string& path);
  static bool IsAllowedProxyPath(
      HostdRuntimeProxyPolicy policy,
      const std::string& method,
      const std::string& path);
  static std::string PolicyLabel(HostdRuntimeProxyPolicy policy);
  static HostdRuntimeHttpResponse ParseHttpResponse(const std::string& response_text);
};

}  // namespace naim::hostd
