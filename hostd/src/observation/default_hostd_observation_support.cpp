#include "observation/default_hostd_observation_support.h"

#include "app/hostd_app_support.h"

namespace comet::hostd {

void DefaultHostdObservationSupport::ShowLocalState(
    const std::string& node_name,
    const std::string& state_root) const {
  appsupport::ShowLocalState(node_name, state_root);
}

void DefaultHostdObservationSupport::ShowRuntimeStatus(
    const std::string& node_name,
    const std::string& state_root) const {
  appsupport::ShowRuntimeStatus(node_name, state_root);
}

comet::HostObservation DefaultHostdObservationSupport::BuildObservedStateSnapshot(
    const std::string& node_name,
    const std::string& state_root,
    comet::HostObservationStatus status,
    const std::string& status_message,
    const std::optional<int>& assignment_id) const {
  return appsupport::BuildObservedStateSnapshot(
      node_name,
      state_root,
      status,
      status_message,
      assignment_id);
}

void DefaultHostdObservationSupport::AppendHostdEvent(
    HostdBackend& backend,
    const std::string& category,
    const std::string& event_type,
    const std::string& message,
    const nlohmann::json& payload,
    const std::string& plane_name,
    const std::string& node_name,
    const std::string& worker_name,
    const std::optional<int>& assignment_id,
    const std::optional<int>& rollout_action_id,
    const std::string& severity) const {
  appsupport::AppendHostdEvent(
      backend,
      category,
      event_type,
      message,
      payload,
      plane_name,
      node_name,
      worker_name,
      assignment_id,
      rollout_action_id,
      severity);
}

}  // namespace comet::hostd
