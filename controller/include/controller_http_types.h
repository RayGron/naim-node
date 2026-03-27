#pragma once

#include <map>
#include <string>

struct HttpRequest {
  std::string method = "GET";
  std::string path = "/";
  std::map<std::string, std::string> headers;
  std::map<std::string, std::string> query_params;
  std::string body;
};
