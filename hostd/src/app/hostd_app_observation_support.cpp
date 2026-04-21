#include "app/hostd_app_observation_support.h"

namespace naim::hostd {

HostdAppObservationSupport::HostdAppObservationSupport()
    : path_support_(),
      display_support_(path_support_),
      reporting_support_() {}

void HostdAppObservationSupport::ShowLocalState(
    const std::string& node_name,
    const std::string& state_root) const {
  display_support_.ShowLocalState(node_name, state_root);
}

void HostdAppObservationSupport::ShowRuntimeStatus(
    const std::string& node_name,
    const std::string& state_root) const {
  display_support_.ShowRuntimeStatus(node_name, state_root);
}

naim::HostObservation HostdAppObservationSupport::BuildObservedStateSnapshot(
    const std::string& node_name,
    const std::string& storage_root,
    const std::string& state_root,
    naim::HostObservationStatus status,
    const std::string& status_message,
    const std::optional<int>& assignment_id) const {
  return reporting_support_.BuildObservedStateSnapshot(
      node_name,
      storage_root,
      state_root,
      status,
      status_message,
      assignment_id);
}

void HostdAppObservationSupport::AppendHostdEvent(
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
  reporting_support_.AppendHostdEvent(
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

}  // namespace naim::hostd
