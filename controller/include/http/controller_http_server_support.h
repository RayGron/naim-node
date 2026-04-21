#pragma once

#include <map>
#include <optional>
#include <string>
#include <string_view>

#include "http/controller_http_types.h"

namespace naim::controller {

class ControllerHttpServerSupport {
 public:
  static std::map<std::string, std::string> ParseQueryParams(
      const std::string& query_text);
  static std::string UrlEncode(std::string_view value);
  static HttpRequest ParseHttpRequest(const std::string& request_text);
  static std::size_t ExpectedRequestBytes(const std::string& request_text);
  static std::optional<std::string> FindHeaderString(
      const HttpRequest& request,
      const std::string& key);
  static bool StartsWithPath(
      const std::string& value,
      const std::string& prefix);

 private:
  static std::string TrimCopy(const std::string& value);
  static std::string LowercaseCopy(const std::string& value);
};

}  // namespace naim::controller
