#include "../include/read_model_http_service.h"

#include <stdexcept>
#include <utility>

using nlohmann::json;

ReadModelHttpService::ReadModelHttpService(Deps deps) : deps_(std::move(deps)) {}

std::optional<HttpResponse> ReadModelHttpService::HandleRequest(
    const std::string& db_path,
    const HttpRequest& request) const {
  if (request.path == "/api/v1/host-assignments") {
    if (request.method != "GET") {
      return deps_.build_json_response(
          405, json{{"status", "method_not_allowed"}}, {});
    }
    try {
      return deps_.build_json_response(
          200,
          deps_.build_host_assignments_payload(
              db_path,
              deps_.find_query_string(request, "node")),
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

  if (request.path == "/api/v1/host-observations") {
    if (request.method != "GET") {
      return deps_.build_json_response(
          405, json{{"status", "method_not_allowed"}}, {});
    }
    try {
      return deps_.build_json_response(
          200,
          deps_.build_host_observations_payload(
              db_path,
              deps_.find_query_string(request, "node"),
              deps_.find_query_string(request, "plane"),
              deps_.find_query_int(request, "stale_after")
                  .value_or(deps_.default_stale_after_seconds())),
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

  if (request.path == "/api/v1/host-health") {
    if (request.method != "GET") {
      return deps_.build_json_response(
          405, json{{"status", "method_not_allowed"}}, {});
    }
    try {
      return deps_.build_json_response(
          200,
          deps_.build_host_health_payload(
              db_path,
              deps_.find_query_string(request, "node"),
              deps_.find_query_int(request, "stale_after")
                  .value_or(deps_.default_stale_after_seconds())),
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

  if (request.path == "/api/v1/disk-state") {
    if (request.method != "GET") {
      return deps_.build_json_response(
          405, json{{"status", "method_not_allowed"}}, {});
    }
    try {
      return deps_.build_json_response(
          200,
          deps_.build_disk_state_payload(
              db_path,
              deps_.find_query_string(request, "node"),
              deps_.find_query_string(request, "plane")),
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

  if (request.path == "/api/v1/rollout-actions") {
    if (request.method != "GET") {
      return deps_.build_json_response(
          405, json{{"status", "method_not_allowed"}}, {});
    }
    try {
      return deps_.build_json_response(
          200,
          deps_.build_rollout_actions_payload(
              db_path,
              deps_.find_query_string(request, "node"),
              deps_.find_query_string(request, "plane")),
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

  if (request.path == "/api/v1/rebalance-plan") {
    if (request.method != "GET") {
      return deps_.build_json_response(
          405, json{{"status", "method_not_allowed"}}, {});
    }
    try {
      return deps_.build_json_response(
          200,
          deps_.build_rebalance_plan_payload(
              db_path,
              deps_.find_query_string(request, "node"),
              deps_.find_query_int(request, "stale_after")
                  .value_or(deps_.default_stale_after_seconds()),
              deps_.find_query_string(request, "plane")),
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

  if (request.path == "/api/v1/events") {
    if (request.method != "GET") {
      return deps_.build_json_response(
          405, json{{"status", "method_not_allowed"}}, {});
    }
    try {
      return deps_.build_json_response(
          200,
          deps_.build_events_payload(
              db_path,
              deps_.find_query_string(request, "plane"),
              deps_.find_query_string(request, "node"),
              deps_.find_query_string(request, "worker"),
              deps_.find_query_string(request, "category"),
              deps_.find_query_int(request, "limit").value_or(100)),
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
