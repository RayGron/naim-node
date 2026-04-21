#include "scheduler/scheduler_http_service.h"

#include <stdexcept>
#include <utility>

using nlohmann::json;

SchedulerHttpService::SchedulerHttpService(
    const naim::controller::ControllerRequestSupport& controller_request_support,
    const naim::controller::ReadModelService& read_model_service,
    const naim::controller::AssignmentOrchestrationService& assignment_orchestration_service,
    const ISchedulerServiceFactory& scheduler_service_factory)
    : controller_request_support_(controller_request_support),
      read_model_service_(read_model_service),
      assignment_orchestration_service_(assignment_orchestration_service),
      scheduler_service_factory_(scheduler_service_factory) {}

HttpResponse SchedulerHttpService::BuildJsonResponse(
    int status_code,
    const json& payload,
    const std::map<std::string, std::string>& headers) const {
  return HttpResponse{
      status_code,
      "application/json",
      payload.dump(),
      headers,
  };
}

std::optional<std::string> SchedulerHttpService::FindQueryString(
    const HttpRequest& request,
    const std::string& key) const {
  const auto it = request.query_params.find(key);
  if (it == request.query_params.end() || it->second.empty()) {
    return std::nullopt;
  }
  return it->second;
}

std::optional<int> SchedulerHttpService::FindQueryInt(
    const HttpRequest& request,
    const std::string& key) const {
  const auto value = FindQueryString(request, key);
  if (!value.has_value()) {
    return std::nullopt;
  }
  return std::stoi(*value);
}

std::optional<HttpResponse> SchedulerHttpService::HandleRequest(
    const std::string& db_path,
    const std::string& default_artifacts_root,
    const HttpRequest& request) const {
  if (request.path == "/api/v1/node-availability") {
    if (request.method == "GET") {
      try {
        return BuildJsonResponse(
            200,
            read_model_service_.BuildNodeAvailabilityPayload(
                db_path,
                FindQueryString(request, "node")),
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
    if (request.method != "POST") {
      return BuildJsonResponse(
          405, json{{"status", "method_not_allowed"}}, {});
    }
    const auto node_name = FindQueryString(request, "node");
    const auto availability = FindQueryString(request, "availability");
    if (!node_name.has_value()) {
      return BuildJsonResponse(
          400,
          json{{"status", "bad_request"},
               {"message", "missing required query parameter 'node'"}},
          {});
    }
    if (!availability.has_value()) {
      return BuildJsonResponse(
          400,
          json{{"status", "bad_request"},
               {"message", "missing required query parameter 'availability'"}},
          {});
    }
    try {
      return BuildJsonResponse(
          200,
          naim::controller::BuildControllerActionPayload(
              assignment_orchestration_service_.ExecuteSetNodeAvailabilityAction(
                  db_path,
                  *node_name,
                  naim::ParseNodeAvailability(*availability),
                  FindQueryString(request, "message"))),
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

  if (request.path == "/api/v1/scheduler-tick") {
    if (request.method != "POST") {
      return BuildJsonResponse(
          405, json{{"status", "method_not_allowed"}}, {});
    }
    try {
      auto scheduler_service = scheduler_service_factory_.CreateSchedulerService(
          db_path,
          controller_request_support_.ResolveArtifactsRoot(
              FindQueryString(request, "artifacts_root"),
              default_artifacts_root));
      return BuildJsonResponse(
          200,
          naim::controller::BuildControllerActionPayload(
              scheduler_service.ExecuteSchedulerTick()),
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

  if (request.path == "/api/v1/reconcile-rebalance-proposals") {
    if (request.method != "POST") {
      return BuildJsonResponse(
          405, json{{"status", "method_not_allowed"}}, {});
    }
    try {
      auto scheduler_service = scheduler_service_factory_.CreateSchedulerService(
          db_path,
          controller_request_support_.ResolveArtifactsRoot(
              FindQueryString(request, "artifacts_root"),
              default_artifacts_root));
      return BuildJsonResponse(
          200,
          naim::controller::BuildControllerActionPayload(
              scheduler_service.ExecuteReconcileRebalanceProposals()),
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

  if (request.path == "/api/v1/reconcile-rollout-actions") {
    if (request.method != "POST") {
      return BuildJsonResponse(
          405, json{{"status", "method_not_allowed"}}, {});
    }
    try {
      auto scheduler_service = scheduler_service_factory_.CreateSchedulerService(
          db_path,
          controller_request_support_.ResolveArtifactsRoot(
              FindQueryString(request, "artifacts_root"),
              default_artifacts_root));
      return BuildJsonResponse(
          200,
          naim::controller::BuildControllerActionPayload(
              scheduler_service.ExecuteReconcileRolloutActions()),
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

  if (request.path == "/api/v1/apply-rebalance-proposal") {
    if (request.method != "POST") {
      return BuildJsonResponse(
          405, json{{"status", "method_not_allowed"}}, {});
    }
    const auto worker_name = FindQueryString(request, "worker");
    if (!worker_name.has_value()) {
      return BuildJsonResponse(
          400,
          json{{"status", "bad_request"},
               {"message", "missing required query parameter 'worker'"}},
          {});
    }
    try {
      auto scheduler_service = scheduler_service_factory_.CreateSchedulerService(
          db_path,
          controller_request_support_.ResolveArtifactsRoot(
              FindQueryString(request, "artifacts_root"),
              default_artifacts_root));
      return BuildJsonResponse(
          200,
          naim::controller::BuildControllerActionPayload(
              scheduler_service.ExecuteApplyRebalanceProposal(*worker_name)),
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

  if (request.path == "/api/v1/set-rollout-action-status") {
    if (request.method != "POST") {
      return BuildJsonResponse(
          405, json{{"status", "method_not_allowed"}}, {});
    }
    const auto action_id = FindQueryInt(request, "id");
    const auto status = FindQueryString(request, "status");
    if (!action_id.has_value()) {
      return BuildJsonResponse(
          400,
          json{{"status", "bad_request"},
               {"message", "missing required query parameter 'id'"}},
          {});
    }
    if (!status.has_value()) {
      return BuildJsonResponse(
          400,
          json{{"status", "bad_request"},
               {"message", "missing required query parameter 'status'"}},
          {});
    }
    try {
      auto scheduler_service = scheduler_service_factory_.CreateSchedulerService(
          db_path,
          controller_request_support_.ResolveArtifactsRoot(
              FindQueryString(request, "artifacts_root"),
              default_artifacts_root));
      return BuildJsonResponse(
          200,
          naim::controller::BuildControllerActionPayload(
              scheduler_service.ExecuteSetRolloutActionStatus(
                  *action_id,
                  *status,
                  FindQueryString(request, "message"))),
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

  if (request.path == "/api/v1/enqueue-rollout-eviction") {
    if (request.method != "POST") {
      return BuildJsonResponse(
          405, json{{"status", "method_not_allowed"}}, {});
    }
    const auto action_id = FindQueryInt(request, "id");
    if (!action_id.has_value()) {
      return BuildJsonResponse(
          400,
          json{{"status", "bad_request"},
               {"message", "missing required query parameter 'id'"}},
          {});
    }
    try {
      auto scheduler_service = scheduler_service_factory_.CreateSchedulerService(
          db_path,
          controller_request_support_.ResolveArtifactsRoot(
              FindQueryString(request, "artifacts_root"),
              default_artifacts_root));
      return BuildJsonResponse(
          200,
          naim::controller::BuildControllerActionPayload(
              scheduler_service.ExecuteEnqueueRolloutEviction(*action_id)),
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

  if (request.path == "/api/v1/apply-ready-rollout-action") {
    if (request.method != "POST") {
      return BuildJsonResponse(
          405, json{{"status", "method_not_allowed"}}, {});
    }
    const auto action_id = FindQueryInt(request, "id");
    if (!action_id.has_value()) {
      return BuildJsonResponse(
          400,
          json{{"status", "bad_request"},
               {"message", "missing required query parameter 'id'"}},
          {});
    }
    try {
      auto scheduler_service = scheduler_service_factory_.CreateSchedulerService(
          db_path,
          controller_request_support_.ResolveArtifactsRoot(
              FindQueryString(request, "artifacts_root"),
              default_artifacts_root));
      return BuildJsonResponse(
          200,
          naim::controller::BuildControllerActionPayload(
              scheduler_service.ExecuteApplyReadyRolloutAction(*action_id)),
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
