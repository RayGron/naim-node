#include "bundle/bundle_http_service.h"

#include <utility>

using nlohmann::json;

BundleHttpService::BundleHttpService(
    const naim::controller::IBundleCliService& bundle_cli_service,
    naim::controller::ControllerRequestSupport request_support)
    : bundle_cli_service_(bundle_cli_service),
      request_support_(std::move(request_support)) {}

HttpResponse BundleHttpService::BuildJsonResponse(
    int status_code,
    const json& payload,
    const std::map<std::string, std::string>& headers) const {
  return HttpResponse{status_code, "application/json", payload.dump(), headers};
}

std::optional<std::string> BundleHttpService::FindQueryString(
    const HttpRequest& request,
    const std::string& key) const {
  const auto it = request.query_params.find(key);
  if (it == request.query_params.end() || it->second.empty()) {
    return std::nullopt;
  }
  return it->second;
}

std::optional<HttpResponse> BundleHttpService::HandleRequest(
    const std::string& db_path,
    const std::string& default_artifacts_root,
    const HttpRequest& request) const {
  if (request.path == "/api/v1/bundles/validate") {
    if (request.method != "POST") {
      return BuildJsonResponse(
          405, json{{"status", "method_not_allowed"}}, {});
    }
    const auto bundle_dir = FindQueryString(request, "bundle");
    if (!bundle_dir.has_value()) {
      return BuildJsonResponse(
          400,
          json{{"status", "bad_request"},
               {"message", "missing required query parameter 'bundle'"}},
          {});
    }
    try {
      return BuildJsonResponse(
          200,
          naim::controller::BuildControllerActionPayload(
              bundle_cli_service_.ExecuteValidateBundleAction(*bundle_dir)),
          {});
    } catch (const std::exception& error) {
      return BuildJsonResponse(
          500,
          json{{"status", "internal_error"},
               {"message", error.what()},
               {"path", request.path}},
          {});
    }
  }

  if (request.path == "/api/v1/bundles/preview") {
    if (request.method != "POST") {
      return BuildJsonResponse(
          405, json{{"status", "method_not_allowed"}}, {});
    }
    const auto bundle_dir = FindQueryString(request, "bundle");
    if (!bundle_dir.has_value()) {
      return BuildJsonResponse(
          400,
          json{{"status", "bad_request"},
               {"message", "missing required query parameter 'bundle'"}},
          {});
    }
    try {
      return BuildJsonResponse(
          200,
          naim::controller::BuildControllerActionPayload(
              bundle_cli_service_.ExecutePreviewBundleAction(
                  *bundle_dir,
                  FindQueryString(request, "node"))),
          {});
    } catch (const std::exception& error) {
      return BuildJsonResponse(
          500,
          json{{"status", "internal_error"},
               {"message", error.what()},
               {"path", request.path}},
          {});
    }
  }

  if (request.path == "/api/v1/bundles/import") {
    if (request.method != "POST") {
      return BuildJsonResponse(
          405, json{{"status", "method_not_allowed"}}, {});
    }
    const auto bundle_dir = FindQueryString(request, "bundle");
    if (!bundle_dir.has_value()) {
      return BuildJsonResponse(
          400,
          json{{"status", "bad_request"},
               {"message", "missing required query parameter 'bundle'"}},
          {});
    }
    try {
      return BuildJsonResponse(
          200,
          naim::controller::BuildControllerActionPayload(
              bundle_cli_service_.ExecuteImportBundleAction(db_path, *bundle_dir)),
          {});
    } catch (const std::exception& error) {
      return BuildJsonResponse(
          500,
          json{{"status", "internal_error"},
               {"message", error.what()},
               {"path", request.path}},
          {});
    }
  }

  if (request.path == "/api/v1/bundles/apply") {
    if (request.method != "POST") {
      return BuildJsonResponse(
          405, json{{"status", "method_not_allowed"}}, {});
    }
    const auto bundle_dir = FindQueryString(request, "bundle");
    if (!bundle_dir.has_value()) {
      return BuildJsonResponse(
          400,
          json{{"status", "bad_request"},
               {"message", "missing required query parameter 'bundle'"}},
          {});
    }
    try {
      return BuildJsonResponse(
          200,
          naim::controller::BuildControllerActionPayload(
              bundle_cli_service_.ExecuteApplyBundleAction(
                  db_path,
                  *bundle_dir,
                  request_support_.ResolveArtifactsRoot(
                      FindQueryString(request, "artifacts_root"),
                      default_artifacts_root))),
          {});
    } catch (const std::exception& error) {
      return BuildJsonResponse(
          500,
          json{{"status", "internal_error"},
               {"message", error.what()},
               {"path", request.path}},
          {});
    }
  }

  return std::nullopt;
}
