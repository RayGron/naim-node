#pragma once

#include <optional>
#include <string>
#include <vector>

#include "infra/controller_runtime_support_service.h"
#include "plane/plane_realization_service.h"

namespace naim::controller {

class SchedulerAssignmentQuerySupport {
 public:
  virtual ~SchedulerAssignmentQuerySupport() = default;

  virtual std::optional<naim::HostAssignment> FindLatestHostAssignmentForNode(
      const std::vector<naim::HostAssignment>& assignments,
      const std::string& node_name) const = 0;
  virtual std::optional<naim::HostAssignment> FindLatestHostAssignmentForPlane(
      const std::vector<naim::HostAssignment>& assignments,
      const std::string& plane_name) const = 0;
  virtual std::string DefaultArtifactsRoot() const = 0;
};

class SchedulerVerificationSupport {
 public:
  virtual ~SchedulerVerificationSupport() = default;

  virtual std::optional<naim::HostObservation> FindHostObservationForNode(
      const std::vector<naim::HostObservation>& observations,
      const std::string& node_name) const = 0;
  virtual std::vector<naim::RuntimeProcessStatus> ParseInstanceRuntimeStatuses(
      const naim::HostObservation& observation) const = 0;
  virtual std::optional<naim::GpuTelemetrySnapshot> ParseGpuTelemetry(
      const naim::HostObservation& observation) const = 0;
  virtual std::optional<long long> TimestampAgeSeconds(
      const std::string& timestamp_text) const = 0;
  virtual std::string UtcNowSqlTimestamp() const = 0;
};

class ControllerSchedulerAssignmentQuerySupport final
    : public SchedulerAssignmentQuerySupport {
 public:
  ControllerSchedulerAssignmentQuerySupport(
      const PlaneRealizationService& plane_realization_service,
      std::string default_artifacts_root);

  std::optional<naim::HostAssignment> FindLatestHostAssignmentForNode(
      const std::vector<naim::HostAssignment>& assignments,
      const std::string& node_name) const override;
  std::optional<naim::HostAssignment> FindLatestHostAssignmentForPlane(
      const std::vector<naim::HostAssignment>& assignments,
      const std::string& plane_name) const override;
  std::string DefaultArtifactsRoot() const override;

 private:
  const PlaneRealizationService& plane_realization_service_;
  std::string default_artifacts_root_;
};

class ControllerSchedulerVerificationSupport final
    : public SchedulerVerificationSupport {
 public:
  explicit ControllerSchedulerVerificationSupport(
      const ControllerRuntimeSupportService& runtime_support_service);

  std::optional<naim::HostObservation> FindHostObservationForNode(
      const std::vector<naim::HostObservation>& observations,
      const std::string& node_name) const override;
  std::vector<naim::RuntimeProcessStatus> ParseInstanceRuntimeStatuses(
      const naim::HostObservation& observation) const override;
  std::optional<naim::GpuTelemetrySnapshot> ParseGpuTelemetry(
      const naim::HostObservation& observation) const override;
  std::optional<long long> TimestampAgeSeconds(
      const std::string& timestamp_text) const override;
  std::string UtcNowSqlTimestamp() const override;

 private:
  const ControllerRuntimeSupportService& runtime_support_service_;
};

}  // namespace naim::controller
