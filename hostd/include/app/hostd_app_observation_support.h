#pragma once

#include <optional>
#include <string>

#include <nlohmann/json.hpp>

#include "app/hostd_desired_state_display_support.h"
#include "app/hostd_desired_state_path_support.h"
#include "app/hostd_reporting_support.h"
#include "backend/hostd_backend.h"
#include "observation/hostd_observation_service.h"

namespace naim::hostd {

class HostdAppObservationSupport final : public IHostdObservationSupport {
 public:
  HostdAppObservationSupport();

  void ShowLocalState(const std::string& node_name, const std::string& state_root) const override;
  void ShowRuntimeStatus(const std::string& node_name, const std::string& state_root) const override;
  naim::HostObservation BuildObservedStateSnapshot(
      const std::string& node_name,
      const std::string& storage_root,
      const std::string& state_root,
      naim::HostObservationStatus status,
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

 private:
  HostdDesiredStatePathSupport path_support_;
  HostdDesiredStateDisplaySupport display_support_;
  HostdReportingSupport reporting_support_;
};

}  // namespace naim::hostd
