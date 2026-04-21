#pragma once

#include <optional>
#include <string>
#include <vector>

#include "app/controller_service_interfaces.h"
#include "infra/controller_action.h"
#include "scheduler/scheduler_view_service.h"

#include "naim/state/models.h"

namespace naim::controller {

class ControllerEventService;
class ControllerPrintService;
class ControllerRuntimeSupportService;
class PlaneRealizationService;
class ReadModelCliService;
class SchedulerExecutionSupport;
class StateAggregateLoader;

struct SchedulerVerificationResult {
  bool converged = false;
  bool stable = false;
  bool timed_out = false;
  int next_stable_samples = 0;
  std::string detail;
};

class SchedulerService : public ISchedulerService {
 public:
  SchedulerService(
      std::string db_path,
      std::string artifacts_root,
      int default_stale_after_seconds,
      int verification_stable_samples_required,
      const StateAggregateLoader& state_aggregate_loader,
      const SchedulerViewService& scheduler_view_service,
      const ReadModelCliService& read_model_cli_service,
      const ControllerPrintService& controller_print_service,
      const ControllerRuntimeSupportService& runtime_support_service,
      const SchedulerExecutionSupport& scheduler_execution_support,
      const PlaneRealizationService& plane_realization_service,
      const ControllerEventService& controller_event_service);

  int ShowRolloutActions(
      const std::optional<std::string>& node_name,
      const std::optional<std::string>& plane_name) const override;
  int ShowRebalancePlan(
      const std::optional<std::string>& node_name,
      const std::optional<std::string>& plane_name) const override;
  int ShowEvents(
      const std::optional<std::string>& plane_name,
      const std::optional<std::string>& node_name,
      const std::optional<std::string>& worker_name,
      const std::optional<std::string>& category,
      int limit) const override;
  ControllerActionResult ExecuteApplyRebalanceProposal(
      const std::string& worker_name) const;
  ControllerActionResult ExecuteReconcileRebalanceProposals() const;
  ControllerActionResult ExecuteSchedulerTick() const;
  ControllerActionResult ExecuteSetRolloutActionStatus(
      int action_id,
      const std::string& requested_status,
      const std::optional<std::string>& message) const;
  ControllerActionResult ExecuteEnqueueRolloutEviction(int action_id) const;
  ControllerActionResult ExecuteReconcileRolloutActions() const;
  ControllerActionResult ExecuteApplyReadyRolloutAction(int action_id) const;
  int ApplyRebalanceProposal(const std::string& worker_name) const override;
  int ReconcileRebalanceProposals() const override;
  int SchedulerTick() const override;
  int SetRolloutActionStatus(
      int action_id,
      const std::string& requested_status,
      const std::optional<std::string>& message) const override;
  int EnqueueRolloutEviction(int action_id) const override;
  int ReconcileRolloutActions() const override;
  int ApplyReadyRolloutAction(int action_id) const override;

 private:
  std::optional<naim::RolloutActionRecord> FindRolloutActionById(
      const std::vector<naim::RolloutActionRecord>& actions,
      int action_id) const;

  int AdvanceActiveSchedulerAction() const;
  int SetRolloutActionStatus(
      int action_id,
      naim::RolloutActionStatus status,
      const std::optional<std::string>& status_message) const;

  std::string db_path_;
  std::string artifacts_root_;
  int default_stale_after_seconds_ = 300;
  int verification_stable_samples_required_ = 3;
  const StateAggregateLoader& state_aggregate_loader_;
  const SchedulerViewService& scheduler_view_service_;
  const ReadModelCliService& read_model_cli_service_;
  const ControllerPrintService& controller_print_service_;
  const ControllerRuntimeSupportService& runtime_support_service_;
  const SchedulerExecutionSupport& scheduler_execution_support_;
  const PlaneRealizationService& plane_realization_service_;
  const ControllerEventService& controller_event_service_;
};

}  // namespace naim::controller
