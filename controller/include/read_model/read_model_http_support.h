#pragma once

#include <map>
#include <optional>
#include <string>

#include <nlohmann/json.hpp>

#include "http/controller_http_transport.h"
#include "http/controller_http_types.h"
#include "read_model/read_model_service.h"
#include "read_model/state_aggregate_loader.h"
#include "scheduler/scheduler_view_service.h"

class ReadModelHttpSupport final {
 public:
  ReadModelHttpSupport(
      const naim::controller::ReadModelService& read_model_service,
      const SchedulerViewService& scheduler_view_service,
      const naim::controller::StateAggregateLoader& state_aggregate_loader,
      int stale_after_seconds);

  HttpResponse build_json_response(
      int status_code,
      const nlohmann::json& payload,
      const std::map<std::string, std::string>& headers) const;
  std::optional<std::string> find_query_string(
      const HttpRequest& request,
      const std::string& key) const;
  std::optional<int> find_query_int(
      const HttpRequest& request,
      const std::string& key) const;
  int default_stale_after_seconds() const;
  const naim::controller::ReadModelService* read_model_service() const;
  const SchedulerViewService* scheduler_view_service() const;
  RolloutActionsViewData load_rollout_actions_view_data(
      const std::string& db_path,
      const std::optional<std::string>& node_name,
      const std::optional<std::string>& plane_name) const;
  RebalancePlanViewData load_rebalance_plan_view_data(
      const std::string& db_path,
      const std::optional<std::string>& node_name,
      int view_stale_after_seconds,
      const std::optional<std::string>& plane_name) const;

 private:
  const naim::controller::ReadModelService& read_model_service_;
  const SchedulerViewService& scheduler_view_service_;
  const naim::controller::StateAggregateLoader& state_aggregate_loader_;
  int stale_after_seconds_;
};
