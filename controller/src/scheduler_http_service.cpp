#include "../include/scheduler_http_service.h"

#include <stdexcept>
#include <utility>

using nlohmann::json;

SchedulerHttpService::SchedulerHttpService(Deps deps) : deps_(std::move(deps)) {}

std::optional<HttpResponse> SchedulerHttpService::HandleRequest(
    const std::string& db_path,
    const std::string& default_artifacts_root,
    const HttpRequest& request) const {
  if (request.path == "/api/v1/node-availability") {
    if (request.method == "GET") {
      try {
        return deps_.build_json_response(
            200,
            deps_.build_node_availability_payload(
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
    if (request.method != "POST") {
      return deps_.build_json_response(
          405, json{{"status", "method_not_allowed"}}, {});
    }
    const auto node_name = deps_.find_query_string(request, "node");
    const auto availability = deps_.find_query_string(request, "availability");
    if (!node_name.has_value()) {
      return deps_.build_json_response(
          400,
          json{{"status", "bad_request"},
               {"message", "missing required query parameter 'node'"}},
          {});
    }
    if (!availability.has_value()) {
      return deps_.build_json_response(
          400,
          json{{"status", "bad_request"},
               {"message", "missing required query parameter 'availability'"}},
          {});
    }
    try {
      return deps_.build_json_response(
          200,
          deps_.build_controller_action_payload(
              deps_.set_node_availability_action(
                  db_path,
                  *node_name,
                  comet::ParseNodeAvailability(*availability),
                  deps_.find_query_string(request, "message"))),
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

  if (request.path == "/api/v1/scheduler-tick") {
    if (request.method != "POST") {
      return deps_.build_json_response(
          405, json{{"status", "method_not_allowed"}}, {});
    }
    try {
      auto scheduler_service = deps_.make_scheduler_service(
          db_path,
          deps_.resolve_artifacts_root(
              deps_.find_query_string(request, "artifacts_root"),
              default_artifacts_root));
      return deps_.build_json_response(
          200,
          deps_.build_controller_action_payload(
              scheduler_service.ExecuteSchedulerTick()),
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

  if (request.path == "/api/v1/reconcile-rebalance-proposals") {
    if (request.method != "POST") {
      return deps_.build_json_response(
          405, json{{"status", "method_not_allowed"}}, {});
    }
    try {
      auto scheduler_service = deps_.make_scheduler_service(
          db_path,
          deps_.resolve_artifacts_root(
              deps_.find_query_string(request, "artifacts_root"),
              default_artifacts_root));
      return deps_.build_json_response(
          200,
          deps_.build_controller_action_payload(
              scheduler_service.ExecuteReconcileRebalanceProposals()),
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

  if (request.path == "/api/v1/reconcile-rollout-actions") {
    if (request.method != "POST") {
      return deps_.build_json_response(
          405, json{{"status", "method_not_allowed"}}, {});
    }
    try {
      auto scheduler_service = deps_.make_scheduler_service(
          db_path,
          deps_.resolve_artifacts_root(
              deps_.find_query_string(request, "artifacts_root"),
              default_artifacts_root));
      return deps_.build_json_response(
          200,
          deps_.build_controller_action_payload(
              scheduler_service.ExecuteReconcileRolloutActions()),
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

  if (request.path == "/api/v1/apply-rebalance-proposal") {
    if (request.method != "POST") {
      return deps_.build_json_response(
          405, json{{"status", "method_not_allowed"}}, {});
    }
    const auto worker_name = deps_.find_query_string(request, "worker");
    if (!worker_name.has_value()) {
      return deps_.build_json_response(
          400,
          json{{"status", "bad_request"},
               {"message", "missing required query parameter 'worker'"}},
          {});
    }
    try {
      auto scheduler_service = deps_.make_scheduler_service(
          db_path,
          deps_.resolve_artifacts_root(
              deps_.find_query_string(request, "artifacts_root"),
              default_artifacts_root));
      return deps_.build_json_response(
          200,
          deps_.build_controller_action_payload(
              scheduler_service.ExecuteApplyRebalanceProposal(*worker_name)),
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

  if (request.path == "/api/v1/set-rollout-action-status") {
    if (request.method != "POST") {
      return deps_.build_json_response(
          405, json{{"status", "method_not_allowed"}}, {});
    }
    const auto action_id = deps_.find_query_int(request, "id");
    const auto status = deps_.find_query_string(request, "status");
    if (!action_id.has_value()) {
      return deps_.build_json_response(
          400,
          json{{"status", "bad_request"},
               {"message", "missing required query parameter 'id'"}},
          {});
    }
    if (!status.has_value()) {
      return deps_.build_json_response(
          400,
          json{{"status", "bad_request"},
               {"message", "missing required query parameter 'status'"}},
          {});
    }
    try {
      auto scheduler_service = deps_.make_scheduler_service(
          db_path,
          deps_.resolve_artifacts_root(
              deps_.find_query_string(request, "artifacts_root"),
              default_artifacts_root));
      return deps_.build_json_response(
          200,
          deps_.build_controller_action_payload(
              scheduler_service.ExecuteSetRolloutActionStatus(
                  *action_id,
                  *status,
                  deps_.find_query_string(request, "message"))),
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

  if (request.path == "/api/v1/enqueue-rollout-eviction") {
    if (request.method != "POST") {
      return deps_.build_json_response(
          405, json{{"status", "method_not_allowed"}}, {});
    }
    const auto action_id = deps_.find_query_int(request, "id");
    if (!action_id.has_value()) {
      return deps_.build_json_response(
          400,
          json{{"status", "bad_request"},
               {"message", "missing required query parameter 'id'"}},
          {});
    }
    try {
      auto scheduler_service = deps_.make_scheduler_service(
          db_path,
          deps_.resolve_artifacts_root(
              deps_.find_query_string(request, "artifacts_root"),
              default_artifacts_root));
      return deps_.build_json_response(
          200,
          deps_.build_controller_action_payload(
              scheduler_service.ExecuteEnqueueRolloutEviction(*action_id)),
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

  if (request.path == "/api/v1/apply-ready-rollout-action") {
    if (request.method != "POST") {
      return deps_.build_json_response(
          405, json{{"status", "method_not_allowed"}}, {});
    }
    const auto action_id = deps_.find_query_int(request, "id");
    if (!action_id.has_value()) {
      return deps_.build_json_response(
          400,
          json{{"status", "bad_request"},
               {"message", "missing required query parameter 'id'"}},
          {});
    }
    try {
      auto scheduler_service = deps_.make_scheduler_service(
          db_path,
          deps_.resolve_artifacts_root(
              deps_.find_query_string(request, "artifacts_root"),
              default_artifacts_root));
      return deps_.build_json_response(
          200,
          deps_.build_controller_action_payload(
              scheduler_service.ExecuteApplyReadyRolloutAction(*action_id)),
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
