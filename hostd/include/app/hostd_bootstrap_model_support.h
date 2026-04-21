#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <vector>

#include "app/hostd_bootstrap_active_model_support.h"
#include "app/hostd_bootstrap_model_artifact_support.h"
#include "app/hostd_bootstrap_transfer_support.h"
#include "app/hostd_command_support.h"
#include "app/hostd_file_support.h"
#include "app/hostd_reporting_support.h"
#include "backend/hostd_backend.h"
#include "naim/state/models.h"

namespace naim::hostd {

class HostdBootstrapModelSupport final {
 public:
  HostdBootstrapModelSupport(
      const HostdBootstrapModelArtifactSupport& artifact_support,
      const HostdBootstrapActiveModelSupport& active_model_support,
      const HostdBootstrapTransferSupport& transfer_support,
      const HostdCommandSupport& command_support,
      const HostdFileSupport& file_support,
      const HostdReportingSupport& reporting_support);

  void BootstrapPlaneModelIfNeeded(
      const naim::DesiredState& state,
      const std::string& node_name,
      HostdBackend* backend,
      const std::optional<int>& assignment_id) const;

 private:
  void PublishAssignmentProgress(
      HostdBackend* backend,
      const std::optional<int>& assignment_id,
      const std::string& phase,
      const std::string& title,
      const std::string& detail,
      int percent,
      const std::string& plane_name,
      const std::string& node_name,
      const std::optional<std::uintmax_t>& bytes_done = std::nullopt,
      const std::optional<std::uintmax_t>& bytes_total = std::nullopt) const;
  bool TryUseReferenceBootstrapModel(
      const naim::DesiredState& state,
      const std::string& node_name,
      const naim::BootstrapModelSpec& bootstrap_model,
      HostdBackend* backend,
      const std::optional<int>& assignment_id) const;
  bool TryAcquireControllerRelayedBootstrapModel(
      const naim::DesiredState& state,
      const std::string& node_name,
      const naim::BootstrapModelSpec& bootstrap_model,
      const std::string& target_path,
      bool write_active_model,
      HostdBackend* backend,
      const std::optional<int>& assignment_id) const;
  bool TryPrepareWorkerBootstrapModel(
      const naim::DesiredState& state,
      const std::string& node_name,
      const naim::BootstrapModelSpec& bootstrap_model,
      HostdBackend* backend,
      const std::optional<int>& assignment_id) const;
  bool TryWriteBackPreparedModel(
      const naim::DesiredState& state,
      const std::string& node_name,
      const naim::BootstrapModelSpec& bootstrap_model,
      const std::string& prepared_path,
      HostdBackend* backend,
      const std::optional<int>& assignment_id) const;
  bool TryUseSharedBootstrapFromOtherNode(
      const naim::DesiredState& state,
      const std::string& node_name,
      const std::vector<HostdBootstrapModelArtifact>& artifacts,
      const std::string& target_path,
      HostdBackend* backend,
      const std::optional<int>& assignment_id) const;
  std::optional<std::uintmax_t> ExpectedArtifactSize(
      const HostdBootstrapModelArtifact& artifact) const;
  bool IsArtifactAlreadyPresent(const HostdBootstrapModelArtifact& artifact) const;
  std::optional<std::uintmax_t> ComputeAggregateExpectedSize(
      const std::vector<HostdBootstrapModelArtifact>& artifacts,
      bool& already_present) const;
  void AcquireArtifactsIfNeeded(
      const naim::DesiredState& state,
      const std::string& node_name,
      const std::vector<HostdBootstrapModelArtifact>& artifacts,
      const std::optional<std::uintmax_t>& aggregate_total,
      bool already_present,
      HostdBackend* backend,
      const std::optional<int>& assignment_id) const;
  void VerifyBootstrapChecksumIfNeeded(
      const naim::DesiredState& state,
      const std::string& node_name,
      const naim::BootstrapModelSpec& bootstrap_model,
      const std::vector<HostdBootstrapModelArtifact>& artifacts,
      const std::string& target_path,
      bool already_present,
      HostdBackend* backend,
      const std::optional<int>& assignment_id) const;
  static bool HasBootstrapSource(const naim::BootstrapModelSpec& bootstrap_model);

  const HostdBootstrapModelArtifactSupport& artifact_support_;
  const HostdBootstrapActiveModelSupport& active_model_support_;
  const HostdBootstrapTransferSupport& transfer_support_;
  const HostdCommandSupport& command_support_;
  const HostdFileSupport& file_support_;
  const HostdReportingSupport& reporting_support_;
};

}  // namespace naim::hostd
