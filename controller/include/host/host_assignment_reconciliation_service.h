#pragma once

#include <map>
#include <optional>
#include <string>
#include <vector>

#include "naim/state/sqlite_store.h"

namespace naim::controller {

class HostAssignmentReconciliationService {
 public:
  struct Result {
    int applied = 0;
    int superseded = 0;

    int Total() const {
      return applied + superseded;
    }
  };

  Result Reconcile(
      naim::ControllerStore& store,
      const std::optional<std::string>& plane_name = std::nullopt) const;

 private:
  Result ReconcilePlane(
      naim::ControllerStore& store,
      const std::string& plane_name,
      const std::vector<naim::HostAssignment>& claimed_assignments,
      const std::vector<naim::HostObservation>& observations) const;
  std::vector<naim::HostAssignment> LoadClaimedApplyAssignments(
      naim::ControllerStore& store,
      const std::optional<std::string>& plane_name) const;
  std::vector<std::string> BuildPlaneNames(
      const std::vector<naim::HostAssignment>& claimed_assignments) const;
  std::map<std::string, naim::HostAssignment> BuildLatestAssignmentsByNode(
      const std::vector<naim::HostAssignment>& assignments) const;
  std::optional<naim::HostObservation> FindObservationForNode(
      const std::vector<naim::HostObservation>& observations,
      const std::string& node_name) const;
  bool ShouldSupersedeClaimedAssignment(
      const naim::HostAssignment& assignment,
      const std::map<std::string, naim::HostAssignment>& latest_assignments_by_node) const;
  bool ShouldMarkClaimedAssignmentApplied(
      const naim::HostAssignment& assignment,
      const std::optional<naim::PlaneRecord>& plane,
      const std::optional<naim::HostObservation>& observation) const;
};

}  // namespace naim::controller
