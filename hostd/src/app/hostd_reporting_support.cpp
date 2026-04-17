#include "app/hostd_reporting_support.h"

#include <algorithm>

namespace naim::hostd {

HostdReportingSupport::HostdReportingSupport()
    : system_telemetry_collector_(),
      runtime_telemetry_support_(),
      observed_state_snapshot_builder_desired_state_path_support_(),
      observed_state_snapshot_builder_local_state_path_support_(),
      observed_state_snapshot_builder_local_state_repository_(
          observed_state_snapshot_builder_local_state_path_support_),
      observed_state_snapshot_builder_local_runtime_state_support_(
          observed_state_snapshot_builder_desired_state_path_support_,
          observed_state_snapshot_builder_local_state_repository_,
          runtime_telemetry_support_),
      observed_state_snapshot_builder_(
          observed_state_snapshot_builder_local_state_repository_,
          observed_state_snapshot_builder_local_runtime_state_support_,
          runtime_telemetry_support_,
          system_telemetry_collector_) {}

nlohmann::json HostdReportingSupport::BuildAssignmentProgressPayload(
    const std::string& phase,
    const std::string& title,
    const std::string& detail,
    int percent,
    const std::string& plane_name,
    const std::string& node_name,
    const std::optional<std::uintmax_t>& bytes_done,
    const std::optional<std::uintmax_t>& bytes_total) const {
  nlohmann::json payload{
      {"phase", phase},
      {"title", title},
      {"detail", detail},
      {"percent", std::max(0, std::min(100, percent))},
      {"plane_name", plane_name},
      {"node_name", node_name},
  };
  if (bytes_done.has_value()) {
    payload["bytes_done"] = *bytes_done;
  }
  if (bytes_total.has_value()) {
    payload["bytes_total"] = *bytes_total;
  }
  return payload;
}

void HostdReportingSupport::PublishAssignmentProgress(
    HostdBackend* backend,
    const std::optional<int>& assignment_id,
    const nlohmann::json& progress) const {
  if (backend == nullptr || !assignment_id.has_value()) {
    return;
  }
  try {
    backend->UpdateHostAssignmentProgress(*assignment_id, progress);
  } catch (...) {
    // Progress is telemetry. A transient controller/session failure must not abort
    // long-running downloads or leave the transfer future waiting for curl forever.
  }
}

void HostdReportingSupport::AppendHostdEvent(
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
  backend.AppendEvent(naim::EventRecord{
      0,
      plane_name,
      node_name,
      worker_name,
      assignment_id,
      rollout_action_id,
      category,
      event_type,
      severity,
      message,
      SerializeEventPayload(payload),
      "",
  });
}

std::vector<std::string> HostdReportingSupport::ParseTaggedCsv(
    const std::string& tagged_message,
    const std::string& tag) const {
  return runtime_telemetry_support_.ParseTaggedCsv(tagged_message, tag);
}

std::map<std::string, int> HostdReportingSupport::CaptureServiceHostPids(
    const std::vector<std::string>& service_names) const {
  return runtime_telemetry_support_.CaptureServiceHostPids(service_names);
}

bool HostdReportingSupport::VerifyEvictionAssignment(
    const naim::DesiredState& desired_state,
    const std::string& node_name,
    const std::string& state_root,
    const std::string& tagged_message,
    const std::map<std::string, int>& expected_victim_host_pids) const {
  return runtime_telemetry_support_.VerifyEvictionAssignment(
      desired_state,
      node_name,
      state_root,
      tagged_message,
      expected_victim_host_pids);
}

naim::HostObservation HostdReportingSupport::BuildObservedStateSnapshot(
    const std::string& node_name,
    const std::string& storage_root,
    const std::string& state_root,
    naim::HostObservationStatus status,
    const std::string& status_message,
    const std::optional<int>& assignment_id) const {
  return observed_state_snapshot_builder_.BuildObservedStateSnapshot(
      node_name,
      storage_root,
      state_root,
      status,
      status_message,
      assignment_id);
}

std::string HostdReportingSupport::SerializeEventPayload(
    const nlohmann::json& payload) {
  return payload.dump();
}

}  // namespace naim::hostd
