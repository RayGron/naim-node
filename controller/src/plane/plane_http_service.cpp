#include "plane/plane_http_service.h"

#include <stdexcept>
#include <string>
#include <utility>

using nlohmann::json;

namespace {

bool StartsWithPathPrefix(const std::string& path, const std::string& prefix) {
  return path.rfind(prefix, 0) == 0;
}

}  // namespace

PlaneHttpService::PlaneHttpService(PlaneHttpSupport support) : support_(std::move(support)) {}

std::optional<HttpResponse> PlaneHttpService::HandleRequest(
    const std::string& db_path,
    const std::string& default_artifacts_root,
    const HttpRequest& request) const {
  if (request.path == "/api/v1/planes") {
    return HandlePlanesCollection(db_path, default_artifacts_root, request);
  }
  if (request.path == "/api/v1/state") {
    return HandleControllerState(db_path, request);
  }
  if (request.path == "/api/v1/dashboard") {
    return HandleDashboard(db_path, request);
  }
  if (StartsWithPathPrefix(request.path, "/api/v1/planes/")) {
    if (request.path.find("/interaction/") != std::string::npos) {
      return std::nullopt;
    }
    return HandlePlanePath(db_path, default_artifacts_root, request);
  }
  return std::nullopt;
}

HttpResponse PlaneHttpService::HandlePlanesCollection(
    const std::string& db_path,
    const std::string& default_artifacts_root,
    const HttpRequest& request) const {
  if (request.method == "GET") {
    try {
      if (support_.plane_registry_service() == nullptr) {
        throw std::runtime_error("plane registry service is not configured");
      }
      return support_.build_json_response(
          200,
          support_.plane_registry_service()->BuildPlanesPayload(db_path),
          {});
    } catch (const std::exception& error) {
      return support_.build_json_response(
          500,
          json{{"status", "internal_error"}, {"message", error.what()}, {"path", request.path}},
          {});
    }
  }
  if (request.method == "POST") {
    try {
      const json body = support_.parse_json_request_body(request);
      const json desired_state_payload =
          body.contains("desired_state") ? body.at("desired_state") : body;
      if (!desired_state_payload.is_object()) {
        return support_.build_json_response(
            400,
            json{{"status", "bad_request"},
                 {"message", "request body must contain desired_state object"}},
            {});
      }
      const std::string artifacts_root = support_.resolve_artifacts_root(
          body.contains("artifacts_root") && body["artifacts_root"].is_string()
              ? std::optional<std::string>(body["artifacts_root"].get<std::string>())
              : support_.find_query_string(request, "artifacts_root"),
          default_artifacts_root);
      return support_.build_json_response(
          200,
          support_.build_controller_action_payload(support_.upsert_plane_state_action(
              db_path,
              desired_state_payload.dump(2),
              artifacts_root,
              std::nullopt,
              "api")),
          {});
    } catch (const std::invalid_argument& error) {
      return support_.build_json_response(
          400,
          json{{"status", "bad_request"}, {"message", error.what()}, {"path", request.path}},
          {});
    } catch (const std::exception& error) {
      return support_.build_json_response(
          500,
          json{{"status", "internal_error"}, {"message", error.what()}, {"path", request.path}},
          {});
    }
  }
  return support_.build_json_response(405, json{{"status", "method_not_allowed"}}, {});
}

HttpResponse PlaneHttpService::HandlePlanePath(
    const std::string& db_path,
    const std::string& default_artifacts_root,
    const HttpRequest& request) const {
  const std::string remainder = request.path.substr(std::string("/api/v1/planes/").size());
  if (remainder.empty()) {
    return support_.build_json_response(404, json{{"status", "not_found"}}, {});
  }

  const auto start_pos = remainder.find("/start");
  if (start_pos != std::string::npos &&
      start_pos + std::string("/start").size() == remainder.size()) {
    if (request.method != "POST") {
      return support_.build_json_response(405, json{{"status", "method_not_allowed"}}, {});
    }
    const std::string plane_name = remainder.substr(0, start_pos);
    try {
      return support_.build_json_response(
          200,
          support_.build_controller_action_payload(
              support_.start_plane_action(db_path, plane_name)),
          {});
    } catch (const std::invalid_argument& error) {
      return support_.build_json_response(
          400,
          json{{"status", "bad_request"}, {"message", error.what()}, {"path", request.path}},
          {});
    } catch (const std::exception& error) {
      return support_.build_json_response(
          500,
          json{{"status", "internal_error"}, {"message", error.what()}, {"path", request.path}},
          {});
    }
  }

  const auto stop_pos = remainder.find("/stop");
  if (stop_pos != std::string::npos &&
      stop_pos + std::string("/stop").size() == remainder.size()) {
    if (request.method != "POST") {
      return support_.build_json_response(405, json{{"status", "method_not_allowed"}}, {});
    }
    const std::string plane_name = remainder.substr(0, stop_pos);
    try {
      return support_.build_json_response(
          200,
          support_.build_controller_action_payload(
              support_.stop_plane_action(db_path, plane_name)),
          {});
    } catch (const std::exception& error) {
      return support_.build_json_response(
          500,
          json{{"status", "internal_error"}, {"message", error.what()}, {"path", request.path}},
          {});
    }
  }

  const auto dashboard_pos = remainder.find("/dashboard");
  if (dashboard_pos != std::string::npos &&
      dashboard_pos + std::string("/dashboard").size() == remainder.size()) {
    if (request.method != "GET") {
      return support_.build_json_response(405, json{{"status", "method_not_allowed"}}, {});
    }
    const std::string plane_name = remainder.substr(0, dashboard_pos);
    try {
      if (support_.dashboard_service() == nullptr) {
        throw std::runtime_error("dashboard service is not configured");
      }
      return support_.build_json_response(
          200,
          support_.dashboard_service()->BuildPayload(
              db_path,
              support_.find_query_int(request, "stale_after")
                  .value_or(support_.default_stale_after_seconds()),
              plane_name),
          {});
    } catch (const std::exception& error) {
      return support_.build_json_response(
          500,
          json{{"status", "internal_error"}, {"message", error.what()}, {"path", request.path}},
          {});
    }
  }

  if (request.method == "DELETE" && remainder.find('/') == std::string::npos) {
    try {
      return support_.build_json_response(
          200,
          support_.build_controller_action_payload(
              support_.delete_plane_action(db_path, remainder)),
          {});
    } catch (const std::exception& error) {
      return support_.build_json_response(
          500,
          json{{"status", "internal_error"}, {"message", error.what()}, {"path", request.path}},
          {});
    }
  }

  if (request.method == "PUT" && remainder.find('/') == std::string::npos) {
    try {
      const json body = support_.parse_json_request_body(request);
      const json desired_state_payload =
          body.contains("desired_state") ? body.at("desired_state") : body;
      if (!desired_state_payload.is_object()) {
        return support_.build_json_response(
            400,
            json{{"status", "bad_request"},
                 {"message", "request body must contain desired_state object"}},
            {});
      }
      const std::string artifacts_root = support_.resolve_artifacts_root(
          body.contains("artifacts_root") && body["artifacts_root"].is_string()
              ? std::optional<std::string>(body["artifacts_root"].get<std::string>())
              : support_.find_query_string(request, "artifacts_root"),
          default_artifacts_root);
      return support_.build_json_response(
          200,
          support_.build_controller_action_payload(support_.upsert_plane_state_action(
              db_path,
              desired_state_payload.dump(2),
              artifacts_root,
              remainder,
              "api")),
          {});
    } catch (const std::invalid_argument& error) {
      return support_.build_json_response(
          400,
          json{{"status", "bad_request"}, {"message", error.what()}, {"path", request.path}},
          {});
    } catch (const std::exception& error) {
      return support_.build_json_response(
          500,
          json{{"status", "internal_error"}, {"message", error.what()}, {"path", request.path}},
          {});
    }
  }

  if (request.method == "GET" && remainder.find('/') == std::string::npos) {
    try {
      if (support_.controller_state_service() == nullptr) {
        throw std::runtime_error("controller state service is not configured");
      }
      return support_.build_json_response(
          200,
          support_.controller_state_service()->BuildPayload(db_path, remainder),
          {});
    } catch (const std::exception& error) {
      return support_.build_json_response(
          500,
          json{{"status", "internal_error"}, {"message", error.what()}, {"path", request.path}},
          {});
    }
  }

  if (remainder.find('/') != std::string::npos) {
    return support_.build_json_response(404, json{{"status", "not_found"}}, {});
  }
  return support_.build_json_response(405, json{{"status", "method_not_allowed"}}, {});
}

HttpResponse PlaneHttpService::HandleControllerState(
    const std::string& db_path,
    const HttpRequest& request) const {
  if (request.method != "GET") {
    return support_.build_json_response(405, json{{"status", "method_not_allowed"}}, {});
  }
  try {
    if (support_.controller_state_service() == nullptr) {
      throw std::runtime_error("controller state service is not configured");
    }
    return support_.build_json_response(
        200,
        support_.controller_state_service()->BuildPayload(db_path, std::nullopt),
        {});
  } catch (const std::exception& error) {
    return support_.build_json_response(
        500,
        json{{"status", "internal_error"}, {"message", error.what()}, {"path", request.path}},
        {});
  }
}

HttpResponse PlaneHttpService::HandleDashboard(
    const std::string& db_path,
    const HttpRequest& request) const {
  if (request.method != "GET") {
    return support_.build_json_response(405, json{{"status", "method_not_allowed"}}, {});
  }
  try {
    if (support_.dashboard_service() == nullptr) {
      throw std::runtime_error("dashboard service is not configured");
    }
    return support_.build_json_response(
        200,
        support_.dashboard_service()->BuildPayload(
            db_path,
            support_.find_query_int(request, "stale_after")
                .value_or(support_.default_stale_after_seconds()),
            support_.find_query_string(request, "plane")),
        {});
  } catch (const std::exception& error) {
    return support_.build_json_response(
        500,
        json{{"status", "internal_error"}, {"message", error.what()}, {"path", request.path}},
        {});
  }
}
