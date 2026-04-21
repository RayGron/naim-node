#pragma once

#include <optional>
#include <string>

#include "backend/hostd_backend_factory.h"
#include "naim/state/models.h"

namespace naim::hostd {

class IHostdObservationSupport {
 public:
  virtual ~IHostdObservationSupport() = default;

  virtual void ShowLocalState(const std::string& node_name, const std::string& state_root) const = 0;
  virtual void ShowRuntimeStatus(const std::string& node_name, const std::string& state_root) const = 0;
  virtual naim::HostObservation BuildObservedStateSnapshot(
      const std::string& node_name,
      const std::string& storage_root,
      const std::string& state_root,
      naim::HostObservationStatus status,
      const std::string& status_message,
      const std::optional<int>& assignment_id = std::nullopt) const = 0;
  virtual void AppendHostdEvent(
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
      const std::string& severity) const = 0;
};

class HostdObservationService {
 public:
  HostdObservationService(
      const IHostdBackendFactory& backend_factory,
      const IHostdObservationSupport& support);

  void ShowLocalState(const std::string& node_name, const std::string& state_root) const;
  void ShowRuntimeStatus(const std::string& node_name, const std::string& state_root) const;
  void ReportObservedState(
      HostdBackend& backend,
      const naim::HostObservation& observation,
      const std::string& source_label) const;
  void ReportLocalObservedState(
      const std::optional<std::string>& db_path,
      const std::optional<std::string>& controller_url,
      const std::optional<std::string>& host_private_key_path,
      const std::optional<std::string>& controller_fingerprint,
      const std::optional<std::string>& onboarding_key,
      const std::string& node_name,
      const std::string& storage_root,
      const std::string& state_root) const;

 private:
  const IHostdBackendFactory& backend_factory_;
  const IHostdObservationSupport& support_;
};

}  // namespace naim::hostd
