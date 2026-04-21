#pragma once

#include <map>
#include <string>

namespace naim::infer {

struct SimpleResponse {
  int status_code = 200;
  std::string content_type = "application/json";
  std::string body;
};

struct HttpRequest {
  std::string method = "GET";
  std::string path = "/";
  std::map<std::string, std::string> headers;
  std::string body;
};

struct UpstreamTarget {
  std::string host;
  int port = 80;
};

}  // namespace naim::infer
