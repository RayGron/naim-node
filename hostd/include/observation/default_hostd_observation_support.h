#pragma once

#include "observation/hostd_observation_service.h"

namespace comet::hostd {

class DefaultHostdObservationSupport final : public IHostdObservationSupport {
 public:
  void ShowLocalState(const std::string& node_name, const std::string& state_root) const override;
  void ShowRuntimeStatus(const std::string& node_name, const std::string& state_root) const override;
  comet::HostObservation BuildObservedStateSnapshot(
      const std::string& node_name,
      const std::string& state_root,
      comet::HostObservationStatus status,
      const std::string& status_message,
      const std::optional<int>& assignment_id = std::nullopt) const override;
  void AppendHostdEvent(
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
      const std::string& severity) const override;
};

}  // namespace comet::hostd
