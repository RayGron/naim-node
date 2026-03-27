#pragma once

#include <functional>
#include <optional>
#include <string>
#include <vector>

#include <nlohmann/json.hpp>

#include "comet/models.h"
#include "comet/scheduling_policy.h"
#include "comet/sqlite_store.h"

namespace comet::controller {

using PlaneTimestampFormatter = std::function<std::string(const std::string&)>;
using PlaneStateSummaryPrinter = std::function<void(const comet::DesiredState&)>;
using PlaneStatePreparer =
    std::function<void(comet::ControllerStore&, comet::DesiredState*)>;
using PlaneEventAppender = std::function<void(
    comet::ControllerStore&,
    const std::string&,
    const std::string&,
    const std::string&,
    const nlohmann::json&,
    const std::string&)>;
using PlaneDeleteFinalizer =
    std::function<bool(comet::ControllerStore&, const std::string&)>;
using PlaneHostAssignmentFinder = std::function<std::optional<comet::HostAssignment>(
    const std::vector<comet::HostAssignment>&,
    const std::string&)>;
using PlaneStartAssignmentBuilder = std::function<std::vector<comet::HostAssignment>(
    const comet::DesiredState&,
    const std::string&,
    int,
    const std::vector<comet::NodeAvailabilityOverride>&,
    const std::vector<comet::HostObservation>&,
    const comet::SchedulingPolicyReport&)>;
using PlaneStopAssignmentBuilder = std::function<std::vector<comet::HostAssignment>(
    const comet::DesiredState&,
    int,
    const std::string&,
    const std::vector<comet::NodeAvailabilityOverride>&)>;
using PlaneDeleteAssignmentBuilder = std::function<std::vector<comet::HostAssignment>(
    const comet::DesiredState&,
    int,
    const std::string&)>;
using DefaultArtifactsRootProvider = std::function<std::string()>;

class PlaneService {
 public:
  PlaneService(
      std::string db_path,
      PlaneTimestampFormatter timestamp_formatter,
      PlaneStateSummaryPrinter state_summary_printer,
      PlaneStatePreparer state_preparer,
      PlaneEventAppender event_appender,
      PlaneDeleteFinalizer can_finalize_deleted_plane,
      PlaneHostAssignmentFinder find_latest_host_assignment,
      PlaneStartAssignmentBuilder build_start_assignments,
      PlaneStopAssignmentBuilder build_stop_assignments,
      PlaneDeleteAssignmentBuilder build_delete_assignments,
      DefaultArtifactsRootProvider default_artifacts_root_provider);

  int ListPlanes() const;
  int ShowPlane(const std::string& plane_name) const;
  int StartPlane(const std::string& plane_name) const;
  int StopPlane(const std::string& plane_name) const;
  int DeletePlane(const std::string& plane_name) const;

 private:
  std::string db_path_;
  PlaneTimestampFormatter timestamp_formatter_;
  PlaneStateSummaryPrinter state_summary_printer_;
  PlaneStatePreparer state_preparer_;
  PlaneEventAppender event_appender_;
  PlaneDeleteFinalizer can_finalize_deleted_plane_;
  PlaneHostAssignmentFinder find_latest_host_assignment_;
  PlaneStartAssignmentBuilder build_start_assignments_;
  PlaneStopAssignmentBuilder build_stop_assignments_;
  PlaneDeleteAssignmentBuilder build_delete_assignments_;
  DefaultArtifactsRootProvider default_artifacts_root_provider_;
};

}  // namespace comet::controller
