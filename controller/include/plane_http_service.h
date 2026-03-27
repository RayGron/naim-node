#pragma once

#include <functional>
#include <optional>
#include <string>

#include <nlohmann/json.hpp>

#include "controller_action.h"
#include "controller_http_transport.h"
#include "controller_http_types.h"

class PlaneHttpService {
 public:
  using BuildJsonResponseFn = std::function<HttpResponse(
      int,
      const nlohmann::json&,
      const std::map<std::string, std::string>&)>;
  using ParseJsonRequestBodyFn =
      std::function<nlohmann::json(const HttpRequest&)>;
  using FindQueryStringFn = std::function<std::optional<std::string>(
      const HttpRequest&,
      const std::string&)>;
  using FindQueryIntFn =
      std::function<std::optional<int>(const HttpRequest&, const std::string&)>;
  using ResolveArtifactsRootFn = std::function<std::string(
      const std::optional<std::string>&,
      const std::string&)>;
  using BuildPlanesPayloadFn =
      std::function<nlohmann::json(const std::string&)>;
  using BuildDashboardPayloadFn = std::function<nlohmann::json(
      const std::string&,
      int,
      const std::optional<std::string>&)>;
  using BuildControllerStatePayloadFn = std::function<nlohmann::json(
      const std::string&,
      const std::optional<std::string>&)>;
  using BuildControllerActionPayloadFn =
      std::function<nlohmann::json(const comet::controller::ControllerActionResult&)>;
  using UpsertPlaneStateActionFn = std::function<comet::controller::ControllerActionResult(
      const std::string&,
      const std::string&,
      const std::string&,
      const std::optional<std::string>&,
      const std::string&)>;
  using PlaneActionFn = std::function<comet::controller::ControllerActionResult(
      const std::string&,
      const std::string&)>;
  using DefaultStaleAfterSecondsFn = std::function<int()>;

  struct Deps {
    BuildJsonResponseFn build_json_response;
    ParseJsonRequestBodyFn parse_json_request_body;
    FindQueryStringFn find_query_string;
    FindQueryIntFn find_query_int;
    ResolveArtifactsRootFn resolve_artifacts_root;
    BuildPlanesPayloadFn build_planes_payload;
    BuildDashboardPayloadFn build_dashboard_payload;
    BuildControllerStatePayloadFn build_controller_state_payload;
    BuildControllerActionPayloadFn build_controller_action_payload;
    UpsertPlaneStateActionFn upsert_plane_state_action;
    PlaneActionFn start_plane_action;
    PlaneActionFn stop_plane_action;
    PlaneActionFn delete_plane_action;
    DefaultStaleAfterSecondsFn default_stale_after_seconds;
  };

  explicit PlaneHttpService(Deps deps);

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

  Deps deps_;
};
