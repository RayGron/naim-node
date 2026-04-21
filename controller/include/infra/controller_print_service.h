#pragma once

#include <optional>
#include <string>
#include <vector>

#include "infra/controller_runtime_support_service.h"
#include "naim/state/models.h"
#include "naim/runtime/runtime_status.h"
#include "naim/state/sqlite_store.h"

namespace naim::controller {

class ControllerPrintService {
 public:
  ControllerPrintService();
  explicit ControllerPrintService(ControllerRuntimeSupportService runtime_support_service);

  void PrintStateSummary(const naim::DesiredState& state) const;
  void PrintDiskRuntimeStates(const std::vector<naim::DiskRuntimeState>& runtime_states) const;
  void PrintDetailedDiskState(
      const naim::DesiredState& state,
      const std::vector<naim::DiskRuntimeState>& runtime_states,
      const std::vector<naim::HostObservation>& observations,
      const std::optional<std::string>& node_name) const;
  void PrintSchedulerDecisionSummary(const naim::DesiredState& state) const;
  void PrintRolloutGateSummary(const naim::SchedulingPolicyReport& scheduling_report) const;
  void PrintPersistedRolloutActions(
      const std::vector<naim::RolloutActionRecord>& actions) const;
  void PrintNodeAvailabilityOverrides(
      const std::vector<naim::NodeAvailabilityOverride>& availability_overrides) const;
  void PrintAssignmentDispatchSummary(
      const naim::DesiredState& desired_state,
      const std::map<std::string, naim::NodeAvailabilityOverride>& availability_overrides,
      const std::vector<naim::HostObservation>& observations,
      int stale_after_seconds) const;
  void PrintHostAssignments(
      const std::vector<naim::HostAssignment>& assignments) const;
  void PrintHostObservations(
      const std::vector<naim::HostObservation>& observations,
      int stale_after_seconds) const;
  void PrintHostHealth(
      const std::optional<naim::DesiredState>& desired_state,
      const std::vector<naim::HostObservation>& observations,
      const std::vector<naim::NodeAvailabilityOverride>& availability_overrides,
      const std::optional<std::string>& node_name,
      int stale_after_seconds) const;
  void PrintEvents(const std::vector<naim::EventRecord>& events) const;

 private:
  bool IsNodeSchedulable(naim::NodeAvailability availability) const;
  std::optional<std::string> ObservedSchedulingGateReason(
      const std::vector<naim::HostObservation>& observations,
      const std::string& node_name,
      int stale_after_seconds) const;
  std::optional<std::tm> ParseDisplayTimestamp(const std::string& value) const;
  std::string FormatDisplayTimestamp(const std::string& value) const;

  ControllerRuntimeSupportService runtime_support_service_;
};

}  // namespace naim::controller
