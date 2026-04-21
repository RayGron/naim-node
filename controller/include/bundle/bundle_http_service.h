#pragma once

#include <optional>
#include <string>

#include <nlohmann/json.hpp>

#include "app/controller_service_interfaces.h"
#include "infra/controller_action.h"
#include "infra/controller_request_support.h"
#include "http/controller_http_transport.h"
#include "http/controller_http_types.h"

class BundleHttpService {
 public:
  BundleHttpService(
      const naim::controller::IBundleCliService& bundle_cli_service,
      naim::controller::ControllerRequestSupport request_support = {});

  std::optional<HttpResponse> HandleRequest(
      const std::string& db_path,
      const std::string& default_artifacts_root,
      const HttpRequest& request) const;

 private:
  HttpResponse BuildJsonResponse(
      int status_code,
      const nlohmann::json& payload,
      const std::map<std::string, std::string>& headers = {}) const;

  std::optional<std::string> FindQueryString(
      const HttpRequest& request,
      const std::string& key) const;

  const naim::controller::IBundleCliService& bundle_cli_service_;
  naim::controller::ControllerRequestSupport request_support_;
};
