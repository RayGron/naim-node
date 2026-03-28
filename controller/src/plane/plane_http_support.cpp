#include "plane/plane_http_support.h"

#include "app/controller_composition_support.h"

PlaneHttpSupport::PlaneHttpSupport(
    const comet::controller::ControllerRequestSupport& request_support,
    const comet::controller::PlaneMutationService& plane_mutation_service,
    const comet::controller::PlaneRegistryService& plane_registry_service,
    const comet::controller::ControllerStateService& controller_state_service,
    const comet::controller::DashboardService& dashboard_service,
    int stale_after_seconds)
    : request_support_(request_support),
      plane_mutation_service_(plane_mutation_service),
      plane_registry_service_(plane_registry_service),
      controller_state_service_(controller_state_service),
      dashboard_service_(dashboard_service),
      stale_after_seconds_(stale_after_seconds) {}

HttpResponse PlaneHttpSupport::build_json_response(
    int status_code,
    const nlohmann::json& payload,
    const std::map<std::string, std::string>& headers) const {
  return comet::controller::composition_support::BuildJsonResponse(
      status_code,
      payload,
      headers);
}

nlohmann::json PlaneHttpSupport::parse_json_request_body(const HttpRequest& request) const {
  return request_support_.ParseJsonRequestBody(request);
}

std::optional<std::string> PlaneHttpSupport::find_query_string(
    const HttpRequest& request,
    const std::string& key) const {
  return comet::controller::composition_support::FindQueryString(request, key);
}

std::optional<int> PlaneHttpSupport::find_query_int(
    const HttpRequest& request,
    const std::string& key) const {
  return comet::controller::composition_support::FindQueryInt(request, key);
}

std::string PlaneHttpSupport::resolve_artifacts_root(
    const std::optional<std::string>& artifacts_root_arg,
    const std::string& fallback_artifacts_root) const {
  return request_support_.ResolveArtifactsRoot(artifacts_root_arg, fallback_artifacts_root);
}

nlohmann::json PlaneHttpSupport::build_controller_action_payload(
    const comet::controller::ControllerActionResult& result) const {
  return BuildControllerActionPayload(result);
}

comet::controller::ControllerActionResult PlaneHttpSupport::upsert_plane_state_action(
    const std::string& db_path,
    const std::string& desired_state_json,
    const std::string& artifacts_root,
    const std::optional<std::string>& plane_name,
    const std::string& source) const {
  return plane_mutation_service_.ExecuteUpsertPlaneStateAction(
      db_path,
      desired_state_json,
      artifacts_root,
      plane_name,
      source);
}

comet::controller::ControllerActionResult PlaneHttpSupport::start_plane_action(
    const std::string& db_path,
    const std::string& plane_name) const {
  return plane_mutation_service_.ExecuteStartPlaneAction(db_path, plane_name);
}

comet::controller::ControllerActionResult PlaneHttpSupport::stop_plane_action(
    const std::string& db_path,
    const std::string& plane_name) const {
  return plane_mutation_service_.ExecuteStopPlaneAction(db_path, plane_name);
}

comet::controller::ControllerActionResult PlaneHttpSupport::delete_plane_action(
    const std::string& db_path,
    const std::string& plane_name) const {
  return plane_mutation_service_.ExecuteDeletePlaneAction(db_path, plane_name);
}

int PlaneHttpSupport::default_stale_after_seconds() const {
  return stale_after_seconds_;
}

const comet::controller::PlaneRegistryService* PlaneHttpSupport::plane_registry_service() const {
  return &plane_registry_service_;
}

const comet::controller::ControllerStateService* PlaneHttpSupport::controller_state_service()
    const {
  return &controller_state_service_;
}

const comet::controller::DashboardService* PlaneHttpSupport::dashboard_service() const {
  return &dashboard_service_;
}
