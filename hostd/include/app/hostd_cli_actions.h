#pragma once

#include "cli/hostd_cli.h"
#include "observation/hostd_observation_service.h"
#include "state_apply/hostd_assignment_service.h"

namespace comet::hostd {

class HostdCliActions final : public IHostdCliActions {
 public:
  HostdCliActions(
      const HostdAssignmentService& assignment_service,
      const HostdObservationService& observation_service);

  void ShowDemoOps(
      const std::string& node_name,
      const std::string& storage_root,
      const std::optional<std::string>& runtime_root) override;
  void ShowStateOps(
      const std::string& db_path,
      const std::string& node_name,
      const std::string& artifacts_root,
      const std::string& storage_root,
      const std::optional<std::string>& runtime_root,
      const std::string& state_root) override;
  void ShowLocalState(const std::string& node_name, const std::string& state_root) override;
  void ShowRuntimeStatus(const std::string& node_name, const std::string& state_root) override;
  void ReportLocalObservedState(
      const std::optional<std::string>& db_path,
      const std::optional<std::string>& controller_url,
      const std::optional<std::string>& host_private_key_path,
      const std::optional<std::string>& controller_fingerprint,
      const std::string& node_name,
      const std::string& state_root) override;
  void ApplyStateOps(
      const std::string& db_path,
      const std::string& node_name,
      const std::string& artifacts_root,
      const std::string& storage_root,
      const std::optional<std::string>& runtime_root,
      const std::string& state_root,
      ComposeMode compose_mode) override;
  void ApplyNextAssignment(
      const std::optional<std::string>& db_path,
      const std::optional<std::string>& controller_url,
      const std::optional<std::string>& host_private_key_path,
      const std::optional<std::string>& controller_fingerprint,
      const std::string& node_name,
      const std::string& storage_root,
      const std::optional<std::string>& runtime_root,
      const std::string& state_root,
      ComposeMode compose_mode) override;

 private:
  const HostdAssignmentService& assignment_service_;
  const HostdObservationService& observation_service_;
};

}  // namespace comet::hostd
