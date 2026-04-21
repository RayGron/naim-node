#pragma once

#include <optional>
#include <string>

#include <nlohmann/json.hpp>

#include "infra/controller_request_support.h"
#include "infra/controller_action.h"
#include "http/controller_http_transport.h"
#include "http/controller_http_types.h"
#include "read_model/read_model_service.h"
#include "scheduler/assignment_orchestration_service.h"
#include "scheduler/scheduler_service.h"

#include "naim/state/models.h"

class ISchedulerServiceFactory {
 public:
  virtual ~ISchedulerServiceFactory() = default;
  virtual naim::controller::SchedulerService CreateSchedulerService(
      const std::string& db_path,
      const std::string& artifacts_root) const = 0;
};

class SchedulerHttpService {
 public:
  SchedulerHttpService(
      const naim::controller::ControllerRequestSupport& controller_request_support,
      const naim::controller::ReadModelService& read_model_service,
      const naim::controller::AssignmentOrchestrationService& assignment_orchestration_service,
      const ISchedulerServiceFactory& scheduler_service_factory);

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

  std::optional<int> FindQueryInt(
      const HttpRequest& request,
      const std::string& key) const;

  const naim::controller::ControllerRequestSupport& controller_request_support_;
  const naim::controller::ReadModelService& read_model_service_;
  const naim::controller::AssignmentOrchestrationService& assignment_orchestration_service_;
  const ISchedulerServiceFactory& scheduler_service_factory_;
};
