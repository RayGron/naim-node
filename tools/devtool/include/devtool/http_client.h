#pragma once

#include <map>
#include <string>

namespace naim::devtool {

struct HttpRequest {
  std::string method;
  std::string url;
  std::map<std::string, std::string> headers;
  std::string body;
  int timeout_seconds = 60;
};

struct HttpResponse {
  int status_code = 0;
  std::map<std::string, std::string> headers;
  std::string body;
};

HttpResponse PerformHttpRequest(const HttpRequest& request);

}  // namespace naim::devtool
