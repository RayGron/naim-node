#include "../include/scheduler_service.h"

#include <utility>

namespace comet::controller {

SchedulerService::SchedulerService(
    PlaneNodeQueryAction show_rollout_actions_action,
    PlaneNodeQueryAction show_rebalance_plan_action,
    EventsQueryAction show_events_action,
    ActionResultWorker apply_rebalance_proposal_action,
    ActionResultNullary reconcile_rebalance_proposals_action,
    ActionResultNullary scheduler_tick_action,
    ActionResultStatus set_rollout_action_status_action,
    ActionResultId enqueue_rollout_eviction_action,
    ActionResultNullary reconcile_rollout_actions_action,
    ActionResultId apply_ready_rollout_action_action)
    : show_rollout_actions_action_(std::move(show_rollout_actions_action)),
      show_rebalance_plan_action_(std::move(show_rebalance_plan_action)),
      show_events_action_(std::move(show_events_action)),
      apply_rebalance_proposal_action_(std::move(apply_rebalance_proposal_action)),
      reconcile_rebalance_proposals_action_(
          std::move(reconcile_rebalance_proposals_action)),
      scheduler_tick_action_(std::move(scheduler_tick_action)),
      set_rollout_action_status_action_(std::move(set_rollout_action_status_action)),
      enqueue_rollout_eviction_action_(std::move(enqueue_rollout_eviction_action)),
      reconcile_rollout_actions_action_(std::move(reconcile_rollout_actions_action)),
      apply_ready_rollout_action_action_(std::move(apply_ready_rollout_action_action)) {}

int SchedulerService::ShowRolloutActions(
    const std::optional<std::string>& node_name,
    const std::optional<std::string>& plane_name) const {
  return show_rollout_actions_action_(node_name, plane_name);
}

int SchedulerService::ShowRebalancePlan(
    const std::optional<std::string>& node_name,
    const std::optional<std::string>& plane_name) const {
  return show_rebalance_plan_action_(node_name, plane_name);
}

int SchedulerService::ShowEvents(
    const std::optional<std::string>& plane_name,
    const std::optional<std::string>& node_name,
    const std::optional<std::string>& worker_name,
    const std::optional<std::string>& category,
    int limit) const {
  return show_events_action_(plane_name, node_name, worker_name, category, limit);
}

ControllerActionResult SchedulerService::ExecuteApplyRebalanceProposal(
    const std::string& worker_name) const {
  return apply_rebalance_proposal_action_(worker_name);
}

ControllerActionResult SchedulerService::ExecuteReconcileRebalanceProposals() const {
  return reconcile_rebalance_proposals_action_();
}

ControllerActionResult SchedulerService::ExecuteSchedulerTick() const {
  return scheduler_tick_action_();
}

ControllerActionResult SchedulerService::ExecuteSetRolloutActionStatus(
    int action_id,
    const std::string& requested_status,
    const std::optional<std::string>& message) const {
  return set_rollout_action_status_action_(action_id, requested_status, message);
}

ControllerActionResult SchedulerService::ExecuteEnqueueRolloutEviction(int action_id) const {
  return enqueue_rollout_eviction_action_(action_id);
}

ControllerActionResult SchedulerService::ExecuteReconcileRolloutActions() const {
  return reconcile_rollout_actions_action_();
}

ControllerActionResult SchedulerService::ExecuteApplyReadyRolloutAction(int action_id) const {
  return apply_ready_rollout_action_action_(action_id);
}

int SchedulerService::ApplyRebalanceProposal(const std::string& worker_name) const {
  return EmitControllerActionResult(ExecuteApplyRebalanceProposal(worker_name));
}

int SchedulerService::ReconcileRebalanceProposals() const {
  return EmitControllerActionResult(ExecuteReconcileRebalanceProposals());
}

int SchedulerService::SchedulerTick() const {
  return EmitControllerActionResult(ExecuteSchedulerTick());
}

int SchedulerService::SetRolloutActionStatus(
    int action_id,
    const std::string& requested_status,
    const std::optional<std::string>& message) const {
  return EmitControllerActionResult(
      ExecuteSetRolloutActionStatus(action_id, requested_status, message));
}

int SchedulerService::EnqueueRolloutEviction(int action_id) const {
  return EmitControllerActionResult(ExecuteEnqueueRolloutEviction(action_id));
}

int SchedulerService::ReconcileRolloutActions() const {
  return EmitControllerActionResult(ExecuteReconcileRolloutActions());
}

int SchedulerService::ApplyReadyRolloutAction(int action_id) const {
  return EmitControllerActionResult(ExecuteApplyReadyRolloutAction(action_id));
}

}  // namespace comet::controller
