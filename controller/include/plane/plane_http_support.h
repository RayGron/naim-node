#pragma once

#include <map>
#include <optional>
#include <string>

#include <nlohmann/json.hpp>

#include "http/controller_http_transport.h"
#include "http/controller_http_types.h"
#include "infra/controller_action.h"
#include "infra/controller_request_support.h"
#include "plane/controller_state_service.h"
#include "plane/dashboard_service.h"
#include "plane/plane_mutation_service.h"
#include "plane/plane_registry_service.h"

class PlaneHttpSupport final {
 public:
  PlaneHttpSupport(
      const comet::controller::ControllerRequestSupport& request_support,
      const comet::controller::PlaneMutationService& plane_mutation_service,
      const comet::controller::PlaneRegistryService& plane_registry_service,
      const comet::controller::ControllerStateService& controller_state_service,
      const comet::controller::DashboardService& dashboard_service,
      int stale_after_seconds);

  HttpResponse build_json_response(
      int status_code,
      const nlohmann::json& payload,
      const std::map<std::string, std::string>& headers) const;
  nlohmann::json parse_json_request_body(const HttpRequest& request) const;
  std::optional<std::string> find_query_string(
      const HttpRequest& request,
      const std::string& key) const;
  std::optional<int> find_query_int(
      const HttpRequest& request,
      const std::string& key) const;
  std::string resolve_artifacts_root(
      const std::optional<std::string>& artifacts_root_arg,
      const std::string& fallback_artifacts_root) const;
  nlohmann::json build_controller_action_payload(
      const comet::controller::ControllerActionResult& result) const;
  comet::controller::ControllerActionResult upsert_plane_state_action(
      const std::string& db_path,
      const std::string& desired_state_json,
      const std::string& artifacts_root,
      const std::optional<std::string>& plane_name,
      const std::string& source) const;
  comet::controller::ControllerActionResult start_plane_action(
      const std::string& db_path,
      const std::string& plane_name) const;
  comet::controller::ControllerActionResult stop_plane_action(
      const std::string& db_path,
      const std::string& plane_name) const;
  comet::controller::ControllerActionResult delete_plane_action(
      const std::string& db_path,
      const std::string& plane_name) const;
  int default_stale_after_seconds() const;
  const comet::controller::PlaneRegistryService* plane_registry_service() const;
  const comet::controller::ControllerStateService* controller_state_service() const;
  const comet::controller::DashboardService* dashboard_service() const;

 private:
  const comet::controller::ControllerRequestSupport& request_support_;
  const comet::controller::PlaneMutationService& plane_mutation_service_;
  const comet::controller::PlaneRegistryService& plane_registry_service_;
  const comet::controller::ControllerStateService& controller_state_service_;
  const comet::controller::DashboardService& dashboard_service_;
  int stale_after_seconds_;
};
