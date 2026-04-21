#pragma once

#include <optional>
#include <set>
#include <string>
#include <vector>

#include "naim/runtime/runtime_status.h"
#include "naim/state/sqlite_store.h"

namespace naim::controller {

class PlaneObservationMatcher {
 public:
  bool MatchesPlaneInstanceName(
      const std::string& instance_name,
      const std::string& plane_name,
      const std::set<std::string>& plane_instance_names = {}) const;

  std::set<std::string> CollectPlaneInstanceNames(
      const naim::DesiredState& observed_state,
      const std::string& plane_name) const;

  naim::DesiredState FilterObservedStateForPlane(
      const naim::DesiredState& observed_state,
      const std::string& plane_name) const;

  std::optional<naim::DesiredState> ParseObservedStateForPlane(
      const naim::HostObservation& observation,
      const std::optional<std::string>& plane_name) const;

  bool ObservationMatchesPlane(
      const naim::HostObservation& observation,
      const std::string& plane_name) const;

  std::vector<naim::HostObservation> FilterHostObservationsForPlane(
      const std::vector<naim::HostObservation>& observations,
      const std::string& plane_name) const;

  std::optional<naim::RuntimeStatus> FilterRuntimeStatusForPlane(
      const std::optional<naim::RuntimeStatus>& runtime_status,
      const std::optional<std::string>& plane_name,
      const std::set<std::string>& plane_instance_names) const;

  std::vector<naim::RuntimeProcessStatus> FilterInstanceRuntimeStatusesForPlane(
      const std::vector<naim::RuntimeProcessStatus>& statuses,
      const std::optional<std::string>& plane_name,
      const std::set<std::string>& plane_instance_names) const;

  std::optional<naim::GpuTelemetrySnapshot> FilterGpuTelemetryForPlane(
      const std::optional<naim::GpuTelemetrySnapshot>& snapshot,
      const std::optional<std::string>& plane_name,
      const std::set<std::string>& plane_instance_names) const;

  std::optional<naim::DiskTelemetrySnapshot> FilterDiskTelemetryForPlane(
      const std::optional<naim::DiskTelemetrySnapshot>& snapshot,
      const std::optional<std::string>& plane_name) const;

 private:
  bool HasPlaneEntities(const naim::DesiredState& observed_state) const;

  const std::vector<std::string>& RuntimeInstancePrefixes() const;
};

}  // namespace naim::controller
