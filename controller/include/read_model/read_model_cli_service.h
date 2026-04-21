#pragma once

#include <optional>
#include <string>
#include <vector>

#include "app/controller_service_interfaces.h"
#include "infra/controller_print_service.h"
#include "observation/plane_observation_matcher.h"
#include "scheduler/scheduler_view_service.h"
#include "read_model/state_aggregate_loader.h"

#include "naim/state/models.h"
#include "naim/state/sqlite_store.h"

namespace naim::controller {

class ReadModelCliService : public IReadModelCliService {
 public:
  ReadModelCliService(
      const ControllerPrintService& controller_print_service,
      const StateAggregateLoader& state_aggregate_loader,
      const SchedulerViewService& scheduler_view_service,
      int default_stale_after_seconds = 300,
      int verification_stable_samples_required = 3);

  int ShowHostAssignments(
      const std::string& db_path,
      const std::optional<std::string>& node_name) const override;

  int ShowHostObservations(
      const std::string& db_path,
      const std::optional<std::string>& plane_name,
      const std::optional<std::string>& node_name,
      int stale_after_seconds) const override;

  int ShowNodeAvailability(
      const std::string& db_path,
      const std::optional<std::string>& node_name) const override;

  int ShowHostHealth(
      const std::string& db_path,
      const std::optional<std::string>& node_name,
      int stale_after_seconds) const override;

  int ShowEvents(
      const std::string& db_path,
      const std::optional<std::string>& plane_name,
      const std::optional<std::string>& node_name,
      const std::optional<std::string>& worker_name,
      const std::optional<std::string>& category,
      int limit) const override;

  int ShowState(const std::string& db_path) const override;

  int ShowDiskState(
      const std::string& db_path,
      const std::optional<std::string>& node_name,
      const std::optional<std::string>& plane_name) const override;

 private:
  const ControllerPrintService& controller_print_service_;
  const StateAggregateLoader& state_aggregate_loader_;
  const SchedulerViewService& scheduler_view_service_;
  PlaneObservationMatcher plane_observation_matcher_;
  int default_stale_after_seconds_ = 300;
  int verification_stable_samples_required_ = 3;
};

}  // namespace naim::controller
