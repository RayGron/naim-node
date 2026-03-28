#pragma once

#include <map>
#include <optional>
#include <string>
#include <vector>

#include <nlohmann/json.hpp>

#include "backend/hostd_backend.h"
#include "cli/hostd_command_line.h"
#include "comet/state/models.h"

namespace comet::hostd::appsupport {

nlohmann::json SendControllerJsonRequest(
    const std::string& controller_url,
    const std::string& method,
    const std::string& path,
    const nlohmann::json& payload,
    const std::map<std::string, std::string>& headers = {});

comet::HostAssignment ParseAssignmentPayload(const nlohmann::json& payload);
nlohmann::json BuildHostObservationPayload(const comet::HostObservation& observation);
nlohmann::json BuildDiskRuntimeStatePayload(const comet::DiskRuntimeState& state);
comet::DiskRuntimeState ParseDiskRuntimeStatePayload(const nlohmann::json& payload);
std::string Trim(const std::string& value);

void ShowLocalState(const std::string& node_name, const std::string& state_root);
void ShowRuntimeStatus(const std::string& node_name, const std::string& state_root);
comet::HostObservation BuildObservedStateSnapshot(
    const std::string& node_name,
    const std::string& state_root,
    comet::HostObservationStatus status,
    const std::string& status_message,
    const std::optional<int>& assignment_id = std::nullopt);
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
    const std::string& severity);

comet::DesiredState RebaseStateForRuntimeRoot(
    comet::DesiredState state,
    const std::string& storage_root,
    const std::optional<std::string>& runtime_root);
nlohmann::json BuildAssignmentProgressPayload(
    const std::string& phase,
    const std::string& phase_label,
    const std::string& message,
    int progress_percent,
    const std::string& plane_name,
    const std::string& node_name);
void PublishAssignmentProgress(
    HostdBackend* backend,
    const std::optional<int>& assignment_id,
    const nlohmann::json& progress);
std::vector<std::string> ParseTaggedCsv(
    const std::string& tagged_message,
    const std::string& tag);
std::map<std::string, int> CaptureServiceHostPids(
    const std::vector<std::string>& service_names);
bool VerifyEvictionAssignment(
    const comet::DesiredState& desired_state,
    const std::string& node_name,
    const std::string& state_root,
    const std::string& tagged_message,
    const std::map<std::string, int>& expected_victim_host_pids);
void ApplyDesiredNodeState(
    const comet::DesiredState& desired_node_state,
    const std::string& artifacts_root,
    const std::string& storage_root,
    const std::optional<std::string>& runtime_root,
    const std::string& state_root,
    ComposeMode compose_mode,
    const std::string& source_label,
    const std::optional<int>& desired_generation,
    const std::optional<int>& assignment_id,
    HostdBackend* backend);
void ShowDemoOps(
    const std::string& node_name,
    const std::string& storage_root,
    const std::optional<std::string>& runtime_root);
void ShowStateOps(
    const std::string& db_path,
    const std::string& node_name,
    const std::string& artifacts_root,
    const std::string& storage_root,
    const std::optional<std::string>& runtime_root,
    const std::string& state_root);

}  // namespace comet::hostd::appsupport
