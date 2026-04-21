#pragma once

#include <optional>
#include <string>

#include <nlohmann/json.hpp>

#include "infra/controller_action.h"
#include "http/controller_http_transport.h"
#include "http/controller_http_types.h"
#include "plane/controller_state_service.h"
#include "plane/dashboard_service.h"
#include "plane/plane_desired_state_request_parser.h"
#include "plane/plane_http_support.h"
#include "plane/plane_registry_service.h"
#include "skills/plane_skill_catalog_service.h"

class PlaneHttpService {
 public:
  PlaneHttpService(
      PlaneHttpSupport support,
      naim::controller::PlaneSkillCatalogService plane_skill_catalog_service);

  std::optional<HttpResponse> HandleRequest(
      const std::string& db_path,
      const std::string& default_artifacts_root,
      const HttpRequest& request) const;

 private:
  HttpResponse HandlePlanesCollection(
      const std::string& db_path,
      const std::string& default_artifacts_root,
      const HttpRequest& request) const;
  HttpResponse HandlePlanePath(
      const std::string& db_path,
      const std::string& default_artifacts_root,
      const HttpRequest& request) const;
  HttpResponse HandleControllerState(
      const std::string& db_path,
      const HttpRequest& request) const;
  HttpResponse HandleDashboard(
      const std::string& db_path,
      const HttpRequest& request) const;

  PlaneHttpSupport support_;
  naim::controller::PlaneSkillCatalogService plane_skill_catalog_service_;
  PlaneDesiredStateRequestParser request_parser_;
};
