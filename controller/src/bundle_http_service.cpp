#include "../include/bundle_http_service.h"

#include <stdexcept>
#include <utility>

using nlohmann::json;

BundleHttpService::BundleHttpService(Deps deps) : deps_(std::move(deps)) {}

std::optional<HttpResponse> BundleHttpService::HandleRequest(
    const std::string& db_path,
    const std::string& default_artifacts_root,
    const HttpRequest& request) const {
  if (request.path == "/api/v1/bundles/validate") {
    if (request.method != "POST") {
      return deps_.build_json_response(
          405, json{{"status", "method_not_allowed"}}, {});
    }
    const auto bundle_dir = deps_.find_query_string(request, "bundle");
    if (!bundle_dir.has_value()) {
      return deps_.build_json_response(
          400,
          json{{"status", "bad_request"},
               {"message", "missing required query parameter 'bundle'"}},
          {});
    }
    try {
      return deps_.build_json_response(
          200,
          deps_.build_controller_action_payload(
              deps_.validate_bundle_action(*bundle_dir)),
          {});
    } catch (const std::exception& error) {
      return deps_.build_json_response(
          500,
          json{{"status", "internal_error"},
               {"message", error.what()},
               {"path", request.path}},
          {});
    }
  }

  if (request.path == "/api/v1/bundles/preview") {
    if (request.method != "POST") {
      return deps_.build_json_response(
          405, json{{"status", "method_not_allowed"}}, {});
    }
    const auto bundle_dir = deps_.find_query_string(request, "bundle");
    if (!bundle_dir.has_value()) {
      return deps_.build_json_response(
          400,
          json{{"status", "bad_request"},
               {"message", "missing required query parameter 'bundle'"}},
          {});
    }
    try {
      return deps_.build_json_response(
          200,
          deps_.build_controller_action_payload(
              deps_.preview_bundle_action(
                  *bundle_dir,
                  deps_.find_query_string(request, "node"))),
          {});
    } catch (const std::exception& error) {
      return deps_.build_json_response(
          500,
          json{{"status", "internal_error"},
               {"message", error.what()},
               {"path", request.path}},
          {});
    }
  }

  if (request.path == "/api/v1/bundles/import") {
    if (request.method != "POST") {
      return deps_.build_json_response(
          405, json{{"status", "method_not_allowed"}}, {});
    }
    const auto bundle_dir = deps_.find_query_string(request, "bundle");
    if (!bundle_dir.has_value()) {
      return deps_.build_json_response(
          400,
          json{{"status", "bad_request"},
               {"message", "missing required query parameter 'bundle'"}},
          {});
    }
    try {
      return deps_.build_json_response(
          200,
          deps_.build_controller_action_payload(
              deps_.import_bundle_action(db_path, *bundle_dir)),
          {});
    } catch (const std::exception& error) {
      return deps_.build_json_response(
          500,
          json{{"status", "internal_error"},
               {"message", error.what()},
               {"path", request.path}},
          {});
    }
  }

  if (request.path == "/api/v1/bundles/apply") {
    if (request.method != "POST") {
      return deps_.build_json_response(
          405, json{{"status", "method_not_allowed"}}, {});
    }
    const auto bundle_dir = deps_.find_query_string(request, "bundle");
    if (!bundle_dir.has_value()) {
      return deps_.build_json_response(
          400,
          json{{"status", "bad_request"},
               {"message", "missing required query parameter 'bundle'"}},
          {});
    }
    try {
      return deps_.build_json_response(
          200,
          deps_.build_controller_action_payload(
              deps_.apply_bundle_action(
                  db_path,
                  *bundle_dir,
                  deps_.resolve_artifacts_root(
                      deps_.find_query_string(request, "artifacts_root"),
                      default_artifacts_root))),
          {});
    } catch (const std::exception& error) {
      return deps_.build_json_response(
          500,
          json{{"status", "internal_error"},
               {"message", error.what()},
               {"path", request.path}},
          {});
    }
  }

  return std::nullopt;
}
