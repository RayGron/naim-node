#include "read_model/read_model_http_support.h"

#include "app/controller_composition_support.h"

ReadModelHttpSupport::ReadModelHttpSupport(
    const naim::controller::ReadModelService& read_model_service,
    const SchedulerViewService& scheduler_view_service,
    const naim::controller::StateAggregateLoader& state_aggregate_loader,
    int stale_after_seconds)
    : read_model_service_(read_model_service),
      scheduler_view_service_(scheduler_view_service),
      state_aggregate_loader_(state_aggregate_loader),
      stale_after_seconds_(stale_after_seconds) {}

HttpResponse ReadModelHttpSupport::build_json_response(
    int status_code,
    const nlohmann::json& payload,
    const std::map<std::string, std::string>& headers) const {
  return naim::controller::composition_support::BuildJsonResponse(
      status_code,
      payload,
      headers);
}

std::optional<std::string> ReadModelHttpSupport::find_query_string(
    const HttpRequest& request,
    const std::string& key) const {
  return naim::controller::composition_support::FindQueryString(request, key);
}

std::optional<int> ReadModelHttpSupport::find_query_int(
    const HttpRequest& request,
    const std::string& key) const {
  return naim::controller::composition_support::FindQueryInt(request, key);
}

int ReadModelHttpSupport::default_stale_after_seconds() const {
  return stale_after_seconds_;
}

const naim::controller::ReadModelService* ReadModelHttpSupport::read_model_service() const {
  return &read_model_service_;
}

const SchedulerViewService* ReadModelHttpSupport::scheduler_view_service() const {
  return &scheduler_view_service_;
}

RolloutActionsViewData ReadModelHttpSupport::load_rollout_actions_view_data(
    const std::string& db_path,
    const std::optional<std::string>& node_name,
    const std::optional<std::string>& plane_name) const {
  return state_aggregate_loader_.LoadRolloutActionsViewData(db_path, node_name, plane_name);
}

RebalancePlanViewData ReadModelHttpSupport::load_rebalance_plan_view_data(
    const std::string& db_path,
    const std::optional<std::string>& node_name,
    int view_stale_after_seconds,
    const std::optional<std::string>& plane_name) const {
  return state_aggregate_loader_.LoadRebalancePlanViewData(
      db_path,
      node_name,
      view_stale_after_seconds,
      plane_name);
}
