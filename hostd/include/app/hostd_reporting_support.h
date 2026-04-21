#pragma once

#include <cstdint>
#include <map>
#include <optional>
#include <string>
#include <vector>

#include <nlohmann/json.hpp>

#include "backend/hostd_backend.h"
#include "app/hostd_desired_state_path_support.h"
#include "app/hostd_local_runtime_state_support.h"
#include "app/hostd_local_state_path_support.h"
#include "app/hostd_local_state_repository.h"
#include "app/hostd_observed_state_snapshot_builder.h"
#include "app/hostd_runtime_telemetry_support.h"
#include "app/hostd_system_telemetry_collector.h"
#include "naim/state/sqlite_store.h"

namespace naim::hostd {

class HostdReportingSupport final {
 public:
  HostdReportingSupport();

  nlohmann::json BuildAssignmentProgressPayload(
      const std::string& phase,
      const std::string& title,
      const std::string& detail,
      int percent,
      const std::string& plane_name,
      const std::string& node_name,
      const std::optional<std::uintmax_t>& bytes_done = std::nullopt,
      const std::optional<std::uintmax_t>& bytes_total = std::nullopt) const;
  void PublishAssignmentProgress(
      HostdBackend* backend,
      const std::optional<int>& assignment_id,
      const nlohmann::json& progress) const;
  void AppendHostdEvent(
      HostdBackend& backend,
      const std::string& category,
      const std::string& event_type,
      const std::string& message,
      const nlohmann::json& payload = nlohmann::json::object(),
      const std::string& plane_name = "",
      const std::string& node_name = "",
      const std::string& worker_name = "",
      const std::optional<int>& assignment_id = std::nullopt,
      const std::optional<int>& rollout_action_id = std::nullopt,
      const std::string& severity = "info") const;
  std::vector<std::string> ParseTaggedCsv(
      const std::string& tagged_message,
      const std::string& tag) const;
  std::map<std::string, int> CaptureServiceHostPids(
      const std::vector<std::string>& service_names) const;
  bool VerifyEvictionAssignment(
      const naim::DesiredState& desired_state,
      const std::string& node_name,
      const std::string& state_root,
      const std::string& tagged_message,
      const std::map<std::string, int>& expected_victim_host_pids) const;
  naim::HostObservation BuildObservedStateSnapshot(
      const std::string& node_name,
      const std::string& storage_root,
      const std::string& state_root,
      naim::HostObservationStatus status,
      const std::string& status_message,
      const std::optional<int>& assignment_id = std::nullopt) const;

 private:
  static std::string SerializeEventPayload(const nlohmann::json& payload);

  HostdSystemTelemetryCollector system_telemetry_collector_;
  HostdRuntimeTelemetrySupport runtime_telemetry_support_;
  HostdDesiredStatePathSupport observed_state_snapshot_builder_desired_state_path_support_;
  HostdLocalStatePathSupport observed_state_snapshot_builder_local_state_path_support_;
  HostdLocalStateRepository observed_state_snapshot_builder_local_state_repository_;
  HostdLocalRuntimeStateSupport observed_state_snapshot_builder_local_runtime_state_support_;
  HostdObservedStateSnapshotBuilder observed_state_snapshot_builder_;
};

}  // namespace naim::hostd
