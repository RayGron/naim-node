#pragma once

#include <map>
#include <optional>
#include <string>
#include <vector>

#include <nlohmann/json.hpp>

#include "infra/controller_runtime_support_service.h"
#include "observation/plane_observation_matcher.h"
#include "naim/runtime/runtime_status.h"
#include "naim/state/sqlite_store.h"

namespace naim::controller {

class ReadModelService {
 public:
  ReadModelService();
  explicit ReadModelService(ControllerRuntimeSupportService runtime_support_service);

  nlohmann::json BuildEventPayloadItem(
      const naim::EventRecord& event) const;

  nlohmann::json BuildHostAssignmentsPayload(
      const std::string& db_path,
      const std::optional<std::string>& node_name) const;

  nlohmann::json BuildHostObservationsPayload(
      const std::string& db_path,
      const std::optional<std::string>& node_name,
      const std::optional<std::string>& plane_name,
      int stale_after_seconds) const;

  nlohmann::json BuildHostHealthPayload(
      const std::string& db_path,
      const std::optional<std::string>& node_name,
      int stale_after_seconds) const;

  nlohmann::json BuildDiskStatePayload(
      const std::string& db_path,
      const std::optional<std::string>& node_name,
      const std::optional<std::string>& plane_name) const;

  nlohmann::json BuildEventsPayload(
      const std::string& db_path,
      const std::optional<std::string>& plane_name,
      const std::optional<std::string>& node_name,
      const std::optional<std::string>& worker_name,
      const std::optional<std::string>& category,
      int limit) const;

  nlohmann::json BuildNodeAvailabilityPayload(
      const std::string& db_path,
      const std::optional<std::string>& node_name) const;

 private:
  ControllerRuntimeSupportService runtime_support_service_;
  PlaneObservationMatcher plane_observation_matcher_;
};

}  // namespace naim::controller
