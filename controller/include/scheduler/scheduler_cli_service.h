#pragma once

#include <optional>
#include <string>

#include "infra/controller_print_service.h"
#include "scheduler/scheduler_view_service.h"
#include "read_model/state_aggregate_loader.h"

namespace naim::controller {

class SchedulerCliService {
 public:
  SchedulerCliService(
      const StateAggregateLoader& state_aggregate_loader,
      const SchedulerViewService& scheduler_view_service,
      const ControllerPrintService& controller_print_service,
      int default_stale_after_seconds = 300,
      int verification_stable_samples_required = 3);

  int ShowRolloutActions(
      const std::string& db_path,
      const std::optional<std::string>& node_name,
      const std::optional<std::string>& plane_name) const;

  int ShowRebalancePlan(
      const std::string& db_path,
      const std::optional<std::string>& node_name,
      const std::optional<std::string>& plane_name) const;

 private:
  const StateAggregateLoader& state_aggregate_loader_;
  const SchedulerViewService& scheduler_view_service_;
  const ControllerPrintService& controller_print_service_;
  int default_stale_after_seconds_ = 300;
  int verification_stable_samples_required_ = 3;
};

}  // namespace naim::controller
