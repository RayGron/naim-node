#pragma once

#include <functional>
#include <optional>
#include <string>

#include <nlohmann/json.hpp>

#include "controller_http_transport.h"
#include "controller_http_types.h"

class ReadModelHttpService {
 public:
  using BuildJsonResponseFn = std::function<HttpResponse(
      int,
      const nlohmann::json&,
      const std::map<std::string, std::string>&)>;
  using FindQueryStringFn = std::function<std::optional<std::string>(
      const HttpRequest&,
      const std::string&)>;
  using FindQueryIntFn =
      std::function<std::optional<int>(const HttpRequest&, const std::string&)>;
  using DefaultStaleAfterSecondsFn = std::function<int()>;
  using BuildHostAssignmentsPayloadFn =
      std::function<nlohmann::json(const std::string&, const std::optional<std::string>&)>;
  using BuildHostObservationsPayloadFn = std::function<nlohmann::json(
      const std::string&,
      const std::optional<std::string>&,
      const std::optional<std::string>&,
      int)>;
  using BuildHostHealthPayloadFn = std::function<nlohmann::json(
      const std::string&,
      const std::optional<std::string>&,
      int)>;
  using BuildDiskStatePayloadFn = std::function<nlohmann::json(
      const std::string&,
      const std::optional<std::string>&,
      const std::optional<std::string>&)>;
  using BuildRolloutActionsPayloadFn = std::function<nlohmann::json(
      const std::string&,
      const std::optional<std::string>&,
      const std::optional<std::string>&)>;
  using BuildRebalancePlanPayloadFn = std::function<nlohmann::json(
      const std::string&,
      const std::optional<std::string>&,
      int,
      const std::optional<std::string>&)>;
  using BuildEventsPayloadFn = std::function<nlohmann::json(
      const std::string&,
      const std::optional<std::string>&,
      const std::optional<std::string>&,
      const std::optional<std::string>&,
      const std::optional<std::string>&,
      int)>;

  struct Deps {
    BuildJsonResponseFn build_json_response;
    FindQueryStringFn find_query_string;
    FindQueryIntFn find_query_int;
    DefaultStaleAfterSecondsFn default_stale_after_seconds;
    BuildHostAssignmentsPayloadFn build_host_assignments_payload;
    BuildHostObservationsPayloadFn build_host_observations_payload;
    BuildHostHealthPayloadFn build_host_health_payload;
    BuildDiskStatePayloadFn build_disk_state_payload;
    BuildRolloutActionsPayloadFn build_rollout_actions_payload;
    BuildRebalancePlanPayloadFn build_rebalance_plan_payload;
    BuildEventsPayloadFn build_events_payload;
  };

  explicit ReadModelHttpService(Deps deps);

  std::optional<HttpResponse> HandleRequest(
      const std::string& db_path,
      const HttpRequest& request) const;

 private:
  Deps deps_;
};
