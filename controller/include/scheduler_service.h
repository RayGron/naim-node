#pragma once

#include <functional>
#include <optional>
#include <string>

#include "controller_action.h"

namespace comet::controller {

class SchedulerService {
 public:
  using PlaneNodeQueryAction = std::function<int(
      const std::optional<std::string>&,
      const std::optional<std::string>&)>;
  using EventsQueryAction = std::function<int(
      const std::optional<std::string>&,
      const std::optional<std::string>&,
      const std::optional<std::string>&,
      const std::optional<std::string>&,
      int)>;
  using ActionResultWorker = std::function<ControllerActionResult(const std::string&)>;
  using ActionResultNullary = std::function<ControllerActionResult()>;
  using ActionResultStatus = std::function<ControllerActionResult(
      int,
      const std::string&,
      const std::optional<std::string>&)>;
  using ActionResultId = std::function<ControllerActionResult(int)>;

  SchedulerService(
      PlaneNodeQueryAction show_rollout_actions_action,
      PlaneNodeQueryAction show_rebalance_plan_action,
      EventsQueryAction show_events_action,
      ActionResultWorker apply_rebalance_proposal_action,
      ActionResultNullary reconcile_rebalance_proposals_action,
      ActionResultNullary scheduler_tick_action,
      ActionResultStatus set_rollout_action_status_action,
      ActionResultId enqueue_rollout_eviction_action,
      ActionResultNullary reconcile_rollout_actions_action,
      ActionResultId apply_ready_rollout_action_action);

  int ShowRolloutActions(
      const std::optional<std::string>& node_name,
      const std::optional<std::string>& plane_name) const;
  int ShowRebalancePlan(
      const std::optional<std::string>& node_name,
      const std::optional<std::string>& plane_name) const;
  int ShowEvents(
      const std::optional<std::string>& plane_name,
      const std::optional<std::string>& node_name,
      const std::optional<std::string>& worker_name,
      const std::optional<std::string>& category,
      int limit) const;
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
  int ApplyRebalanceProposal(const std::string& worker_name) const;
  int ReconcileRebalanceProposals() const;
  int SchedulerTick() const;
  int SetRolloutActionStatus(
      int action_id,
      const std::string& requested_status,
      const std::optional<std::string>& message) const;
  int EnqueueRolloutEviction(int action_id) const;
  int ReconcileRolloutActions() const;
  int ApplyReadyRolloutAction(int action_id) const;

 private:
  PlaneNodeQueryAction show_rollout_actions_action_;
  PlaneNodeQueryAction show_rebalance_plan_action_;
  EventsQueryAction show_events_action_;
  ActionResultWorker apply_rebalance_proposal_action_;
  ActionResultNullary reconcile_rebalance_proposals_action_;
  ActionResultNullary scheduler_tick_action_;
  ActionResultStatus set_rollout_action_status_action_;
  ActionResultId enqueue_rollout_eviction_action_;
  ActionResultNullary reconcile_rollout_actions_action_;
  ActionResultId apply_ready_rollout_action_action_;
};

}  // namespace comet::controller
