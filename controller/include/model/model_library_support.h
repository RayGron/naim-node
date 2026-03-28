#pragma once

#include <map>
#include <optional>
#include <string>

#include <nlohmann/json.hpp>

#include "http/controller_http_transport.h"
#include "http/controller_http_types.h"
#include "infra/controller_request_support.h"

class ModelLibrarySupport final {
 public:
  explicit ModelLibrarySupport(
      const comet::controller::ControllerRequestSupport& request_support);

  HttpResponse build_json_response(
      int status_code,
      const nlohmann::json& payload,
      const std::map<std::string, std::string>& headers) const;
  nlohmann::json parse_json_request_body(const HttpRequest& request) const;
  std::optional<std::string> find_query_string(
      const HttpRequest& request,
      const std::string& key) const;
  std::string utc_now_sql_timestamp() const;

 private:
  const comet::controller::ControllerRequestSupport& request_support_;
};
