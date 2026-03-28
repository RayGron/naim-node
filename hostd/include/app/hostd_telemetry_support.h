#pragma once

#include <map>
#include <optional>
#include <string>
#include <vector>

#include "comet/runtime/runtime_status.h"
#include "comet/state/models.h"
#include "comet/state/sqlite_store.h"

namespace comet::hostd::telemetry_support {

std::vector<comet::RuntimeProcessStatus> LoadLocalInstanceRuntimeStatuses(
    const std::string& state_root,
    const std::string& node_name,
    const std::optional<std::string>& plane_name = std::nullopt);

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

comet::HostObservation BuildObservedStateSnapshot(
    const std::string& node_name,
    const std::string& state_root,
    comet::HostObservationStatus status,
    const std::string& status_message,
    const std::optional<int>& assignment_id = std::nullopt);

}  // namespace comet::hostd::telemetry_support
