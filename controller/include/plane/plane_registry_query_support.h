#pragma once

#include <map>
#include <optional>
#include <string>
#include <vector>

#include "infra/controller_runtime_support_service.h"
#include "observation/plane_observation_matcher.h"

namespace naim::controller {

class PlaneRegistryQuerySupport {
 public:
  virtual ~PlaneRegistryQuerySupport() = default;

  virtual std::vector<naim::HostObservation> FilterHostObservationsForPlane(
      const std::vector<naim::HostObservation>& observations,
      const std::string& plane_name) const = 0;
  virtual int ComputeEffectiveAppliedGeneration(
      const naim::PlaneRecord& plane,
      const std::optional<naim::DesiredState>& desired_state,
      const std::optional<int>& desired_generation,
      const std::vector<naim::HostObservation>& observations) const = 0;
  virtual std::map<std::string, naim::HostAssignment> BuildLatestAssignmentsByNode(
      const std::vector<naim::HostAssignment>& assignments) const = 0;
};

class ControllerPlaneRegistryQuerySupport final
    : public PlaneRegistryQuerySupport {
 public:
  explicit ControllerPlaneRegistryQuerySupport(
      const ControllerRuntimeSupportService& runtime_support_service);

  std::vector<naim::HostObservation> FilterHostObservationsForPlane(
      const std::vector<naim::HostObservation>& observations,
      const std::string& plane_name) const override;
  int ComputeEffectiveAppliedGeneration(
      const naim::PlaneRecord& plane,
      const std::optional<naim::DesiredState>& desired_state,
      const std::optional<int>& desired_generation,
      const std::vector<naim::HostObservation>& observations) const override;
  std::map<std::string, naim::HostAssignment> BuildLatestAssignmentsByNode(
      const std::vector<naim::HostAssignment>& assignments) const override;

 private:
  const ControllerRuntimeSupportService& runtime_support_service_;
  PlaneObservationMatcher plane_observation_matcher_;
};

}  // namespace naim::controller
