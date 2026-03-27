#pragma once

#include <functional>
#include <optional>
#include <string>

#include <nlohmann/json.hpp>

#include "controller_action.h"
#include "controller_http_transport.h"
#include "controller_http_types.h"

class BundleHttpService {
 public:
  using BuildJsonResponseFn = std::function<HttpResponse(
      int,
      const nlohmann::json&,
      const std::map<std::string, std::string>&)>;
  using FindQueryStringFn = std::function<std::optional<std::string>(
      const HttpRequest&,
      const std::string&)>;
  using ResolveArtifactsRootFn = std::function<std::string(
      const std::optional<std::string>&,
      const std::string&)>;
  using BuildControllerActionPayloadFn =
      std::function<nlohmann::json(const comet::controller::ControllerActionResult&)>;
  using ValidateBundleActionFn =
      std::function<comet::controller::ControllerActionResult(const std::string&)>;
  using PreviewBundleActionFn = std::function<comet::controller::ControllerActionResult(
      const std::string&,
      const std::optional<std::string>&)>;
  using ImportBundleActionFn = std::function<comet::controller::ControllerActionResult(
      const std::string&,
      const std::string&)>;
  using ApplyBundleActionFn = std::function<comet::controller::ControllerActionResult(
      const std::string&,
      const std::string&,
      const std::string&)>;

  struct Deps {
    BuildJsonResponseFn build_json_response;
    FindQueryStringFn find_query_string;
    ResolveArtifactsRootFn resolve_artifacts_root;
    BuildControllerActionPayloadFn build_controller_action_payload;
    ValidateBundleActionFn validate_bundle_action;
    PreviewBundleActionFn preview_bundle_action;
    ImportBundleActionFn import_bundle_action;
    ApplyBundleActionFn apply_bundle_action;
  };

  explicit BundleHttpService(Deps deps);

  std::optional<HttpResponse> HandleRequest(
      const std::string& db_path,
      const std::string& default_artifacts_root,
      const HttpRequest& request) const;

 private:
  Deps deps_;
};
