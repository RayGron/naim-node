#include "plane/plane_http_service.h"

#include <stdexcept>
#include <string>
#include <utility>

#include "browsing/plane_browsing_service.h"
#include "comet/state/sqlite_store.h"
#include "skills/plane_skill_catalog_service.h"
#include "skills/plane_skills_service.h"

using nlohmann::json;

namespace {

bool StartsWithPathPrefix(const std::string& path, const std::string& prefix) {
  return path.rfind(prefix, 0) == 0;
}

}  // namespace

PlaneHttpService::PlaneHttpService(
    PlaneHttpSupport support,
    comet::controller::PlaneSkillCatalogService plane_skill_catalog_service)
    : support_(std::move(support)),
      plane_skill_catalog_service_(std::move(plane_skill_catalog_service)) {}

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
      const auto parsed_request = request_parser_.ParseUpsertRequestBody(body);
      const std::string artifacts_root = support_.resolve_artifacts_root(
          body.contains("artifacts_root") && body["artifacts_root"].is_string()
              ? std::optional<std::string>(body["artifacts_root"].get<std::string>())
              : support_.find_query_string(request, "artifacts_root"),
          default_artifacts_root);
      return support_.build_json_response(
          200,
          support_.build_controller_action_payload(support_.upsert_plane_state_action(
              db_path,
              parsed_request.desired_state_payload.dump(2),
              artifacts_root,
              std::nullopt,
              parsed_request.source_label)),
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

  const auto skills_pos = remainder.find("/skills");
  if (skills_pos != std::string::npos) {
    const bool skills_suffix_valid =
        skills_pos + std::string("/skills").size() == remainder.size() ||
        remainder.at(skills_pos + std::string("/skills").size()) == '/';
    if (skills_suffix_valid) {
      const std::string plane_name = remainder.substr(0, skills_pos);
      const std::string path_suffix =
          remainder.substr(skills_pos + std::string("/skills").size());
      try {
        if (path_suffix == "/resolve-context") {
          if (request.method != "POST") {
            return support_.build_json_response(
                405, json{{"status", "method_not_allowed"}}, {});
          }
          comet::ControllerStore store(db_path);
          store.Initialize();
          const auto desired_state = store.LoadDesiredState(plane_name);
          if (!desired_state.has_value()) {
            return support_.build_json_response(
                404,
                json{{"status", "not_found"},
                     {"message", "plane '" + plane_name + "' not found"},
                     {"path", request.path}},
                {});
          }
          comet::controller::PlaneInteractionResolution resolution;
          resolution.db_path = db_path;
          resolution.desired_state = *desired_state;
          return support_.build_json_response(
              200,
              comet::controller::PlaneSkillsService().BuildContextResolutionPayload(
                  db_path,
                  resolution,
                  support_.parse_json_request_body(request)),
              {});
        }
        if (path_suffix.empty() || path_suffix == "/") {
          if (request.method == "GET") {
            return support_.build_json_response(
                200,
                plane_skill_catalog_service_.BuildListPayload(db_path, plane_name),
                {});
          }
          if (request.method == "POST") {
            return support_.build_json_response(
                200,
                plane_skill_catalog_service_.CreateSkill(
                    db_path,
                    plane_name,
                    support_.parse_json_request_body(request),
                    default_artifacts_root),
                {});
          }
        } else {
          const std::string skill_id =
              path_suffix.front() == '/' ? path_suffix.substr(1) : path_suffix;
          if (!skill_id.empty() && skill_id.find('/') == std::string::npos) {
            if (request.method == "GET") {
              return support_.build_json_response(
                  200,
                  plane_skill_catalog_service_.BuildSkillPayload(db_path, plane_name, skill_id),
                  {});
            }
            if (request.method == "PUT") {
              return support_.build_json_response(
                  200,
                  plane_skill_catalog_service_.UpdateSkill(
                      db_path,
                      plane_name,
                      skill_id,
                      support_.parse_json_request_body(request),
                      false,
                      default_artifacts_root),
                  {});
            }
            if (request.method == "PATCH") {
              return support_.build_json_response(
                  200,
                  plane_skill_catalog_service_.UpdateSkill(
                      db_path,
                      plane_name,
                      skill_id,
                      support_.parse_json_request_body(request),
                      true,
                      default_artifacts_root),
                  {});
            }
            if (request.method == "DELETE") {
              return support_.build_json_response(
                  200,
                  plane_skill_catalog_service_.DeleteSkill(
                      db_path,
                      plane_name,
                      skill_id,
                      default_artifacts_root),
                  {});
            }
          }
        }
        return support_.build_json_response(405, json{{"status", "method_not_allowed"}}, {});
      } catch (const std::invalid_argument& error) {
        return support_.build_json_response(
            400,
            json{{"status", "bad_request"}, {"message", error.what()}, {"path", request.path}},
            {});
      } catch (const std::runtime_error& error) {
        const std::string message = error.what();
        const int status =
            message.find("not found") != std::string::npos ? 404 : 409;
        return support_.build_json_response(
            status,
            json{{"status", status == 404 ? "not_found" : "conflict"},
                 {"message", message},
                 {"path", request.path}},
            {});
      } catch (const std::exception& error) {
        return support_.build_json_response(
            500,
            json{{"status", "internal_error"}, {"message", error.what()}, {"path", request.path}},
            {});
      }
    }
  }

  const auto webgateway_pos = remainder.find("/webgateway");
  if (webgateway_pos != std::string::npos) {
    const bool webgateway_suffix_valid =
        webgateway_pos + std::string("/webgateway").size() == remainder.size() ||
        remainder.at(webgateway_pos + std::string("/webgateway").size()) == '/';
    if (webgateway_suffix_valid) {
      const std::string plane_name = remainder.substr(0, webgateway_pos);
      const std::string path_suffix =
          remainder.substr(webgateway_pos + std::string("/webgateway").size());
      try {
        comet::ControllerStore store(db_path);
        store.Initialize();
        const auto desired_state = store.LoadDesiredState(plane_name);
        if (!desired_state.has_value()) {
          return support_.build_json_response(
              404,
              json{{"status", "not_found"},
                   {"message", "plane '" + plane_name + "' not found"},
                   {"path", request.path}},
              {});
        }
        const auto plane = store.LoadPlane(plane_name);
        const std::optional<std::string> plane_state =
            plane.has_value() ? std::optional<std::string>(plane->state) : std::nullopt;
        const comet::controller::PlaneBrowsingService browsing_service;
        if (path_suffix.empty() || path_suffix == "/" || path_suffix == "/status") {
          if (request.method != "GET") {
            return support_.build_json_response(
                405, json{{"status", "method_not_allowed"}}, {});
          }
          return support_.build_json_response(
              200,
              browsing_service.BuildStatusPayload(*desired_state, plane_state),
              {});
        }

        std::string error_code;
        std::string error_message;
        const auto proxied = browsing_service.ProxyPlaneBrowsingRequest(
            *desired_state,
            request.method,
            path_suffix,
            request.body,
            &error_code,
            &error_message);
        if (!proxied.has_value()) {
          const int status_code = error_code == "webgateway_disabled" ? 409 : 503;
          return support_.build_json_response(
              status_code,
              json{{"status", "error"},
                   {"message", error_message},
                   {"error", {{"code", error_code}, {"message", error_message}}},
                   {"path", request.path}},
              {});
        }
        return *proxied;
      } catch (const std::exception& error) {
        return support_.build_json_response(
            500,
            json{{"status", "internal_error"}, {"message", error.what()}, {"path", request.path}},
            {});
      }
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
      const auto parsed_request = request_parser_.ParseUpsertRequestBody(body);
      const std::string artifacts_root = support_.resolve_artifacts_root(
          body.contains("artifacts_root") && body["artifacts_root"].is_string()
              ? std::optional<std::string>(body["artifacts_root"].get<std::string>())
              : support_.find_query_string(request, "artifacts_root"),
          default_artifacts_root);
      return support_.build_json_response(
          200,
          support_.build_controller_action_payload(support_.upsert_plane_state_action(
              db_path,
              parsed_request.desired_state_payload.dump(2),
              artifacts_root,
              remainder,
              parsed_request.source_label)),
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
