#include "read_model/read_model_http_service.h"

#include <stdexcept>
#include <utility>

using nlohmann::json;

ReadModelHttpService::ReadModelHttpService(ReadModelHttpSupport support)
    : support_(std::move(support)) {}

std::optional<HttpResponse> ReadModelHttpService::HandleRequest(
    const std::string& db_path,
    const HttpRequest& request) const {
  if (request.path == "/api/v1/host-assignments") {
    if (request.method != "GET") {
      return support_.build_json_response(
          405, json{{"status", "method_not_allowed"}}, {});
    }
    try {
      if (support_.read_model_service() == nullptr) {
        throw std::runtime_error("read model service is not configured");
      }
      return support_.build_json_response(
          200,
          support_.read_model_service()->BuildHostAssignmentsPayload(
              db_path,
              support_.find_query_string(request, "node")),
          {});
    } catch (const std::exception& error) {
      return support_.build_json_response(
          500,
          json{{"status", "internal_error"},
               {"message", error.what()},
               {"path", request.path}},
          {});
    }
  }

  if (request.path == "/api/v1/host-observations") {
    if (request.method != "GET") {
      return support_.build_json_response(
          405, json{{"status", "method_not_allowed"}}, {});
    }
    try {
      if (support_.read_model_service() == nullptr) {
        throw std::runtime_error("read model service is not configured");
      }
      return support_.build_json_response(
          200,
          support_.read_model_service()->BuildHostObservationsPayload(
              db_path,
              support_.find_query_string(request, "node"),
              support_.find_query_string(request, "plane"),
              support_.find_query_int(request, "stale_after")
                  .value_or(support_.default_stale_after_seconds())),
          {});
    } catch (const std::exception& error) {
      return support_.build_json_response(
          500,
          json{{"status", "internal_error"},
               {"message", error.what()},
               {"path", request.path}},
          {});
    }
  }

  if (request.path == "/api/v1/host-health") {
    if (request.method != "GET") {
      return support_.build_json_response(
          405, json{{"status", "method_not_allowed"}}, {});
    }
    try {
      if (support_.read_model_service() == nullptr) {
        throw std::runtime_error("read model service is not configured");
      }
      return support_.build_json_response(
          200,
          support_.read_model_service()->BuildHostHealthPayload(
              db_path,
              support_.find_query_string(request, "node"),
              support_.find_query_int(request, "stale_after")
                  .value_or(support_.default_stale_after_seconds())),
          {});
    } catch (const std::exception& error) {
      return support_.build_json_response(
          500,
          json{{"status", "internal_error"},
               {"message", error.what()},
               {"path", request.path}},
          {});
    }
  }

  if (request.path == "/api/v1/disk-state") {
    if (request.method != "GET") {
      return support_.build_json_response(
          405, json{{"status", "method_not_allowed"}}, {});
    }
    try {
      if (support_.read_model_service() == nullptr) {
        throw std::runtime_error("read model service is not configured");
      }
      return support_.build_json_response(
          200,
          support_.read_model_service()->BuildDiskStatePayload(
              db_path,
              support_.find_query_string(request, "node"),
              support_.find_query_string(request, "plane")),
          {});
    } catch (const std::exception& error) {
      return support_.build_json_response(
          500,
          json{{"status", "internal_error"},
               {"message", error.what()},
               {"path", request.path}},
          {});
    }
  }

  if (request.path == "/api/v1/rollout-actions") {
    if (request.method != "GET") {
      return support_.build_json_response(
          405, json{{"status", "method_not_allowed"}}, {});
    }
    try {
      if (support_.scheduler_view_service() == nullptr) {
        throw std::runtime_error("scheduler view service is not configured");
      }
      return support_.build_json_response(
          200,
          support_.scheduler_view_service()->BuildRolloutActionsPayload(
              support_.load_rollout_actions_view_data(
                  db_path,
                  support_.find_query_string(request, "node"),
                  support_.find_query_string(request, "plane"))),
          {});
    } catch (const std::exception& error) {
      return support_.build_json_response(
          500,
          json{{"status", "internal_error"},
               {"message", error.what()},
               {"path", request.path}},
          {});
    }
  }

  if (request.path == "/api/v1/rebalance-plan") {
    if (request.method != "GET") {
      return support_.build_json_response(
          405, json{{"status", "method_not_allowed"}}, {});
    }
    try {
      if (support_.scheduler_view_service() == nullptr) {
        throw std::runtime_error("scheduler view service is not configured");
      }
      return support_.build_json_response(
          200,
          support_.scheduler_view_service()->BuildRebalancePlanPayload(
              support_.load_rebalance_plan_view_data(
                  db_path,
                  support_.find_query_string(request, "node"),
                  support_.find_query_int(request, "stale_after")
                      .value_or(support_.default_stale_after_seconds()),
                  support_.find_query_string(request, "plane"))),
          {});
    } catch (const std::exception& error) {
      return support_.build_json_response(
          500,
          json{{"status", "internal_error"},
               {"message", error.what()},
               {"path", request.path}},
          {});
    }
  }

  if (request.path == "/api/v1/events") {
    if (request.method != "GET") {
      return support_.build_json_response(
          405, json{{"status", "method_not_allowed"}}, {});
    }
    try {
      if (support_.read_model_service() == nullptr) {
        throw std::runtime_error("read model service is not configured");
      }
      return support_.build_json_response(
          200,
          support_.read_model_service()->BuildEventsPayload(
              db_path,
              support_.find_query_string(request, "plane"),
              support_.find_query_string(request, "node"),
              support_.find_query_string(request, "worker"),
              support_.find_query_string(request, "category"),
              support_.find_query_int(request, "limit").value_or(100)),
          {});
    } catch (const std::exception& error) {
      return support_.build_json_response(
          500,
          json{{"status", "internal_error"},
               {"message", error.what()},
               {"path", request.path}},
          {});
    }
  }

  return std::nullopt;
}
