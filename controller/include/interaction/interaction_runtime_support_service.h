#pragma once

#include <optional>
#include <string>
#include <vector>

#include "http/controller_http_transport.h"
#include "interaction/interaction_replica_group_summary_builder.h"

#include "naim/state/models.h"
#include "naim/runtime/runtime_status.h"
#include "naim/state/sqlite_store.h"

namespace naim::controller {

class InteractionRuntimeSupportService {
 public:
  std::optional<ControllerEndpointTarget> ParseInteractionTarget(
      const std::string& gateway_listen,
      int fallback_port) const;

  std::optional<ControllerEndpointTarget> ResolvePlaneLocalInteractionTarget(
      const naim::DesiredState& desired_state) const;

  std::optional<std::string> FindInferInstanceName(
      const naim::DesiredState& desired_state) const;

  std::optional<std::string> FindInteractionInstanceName(
      const naim::DesiredState& desired_state) const;

  std::vector<std::string> FindWorkerInstanceNames(
      const naim::DesiredState& desired_state) const;

  std::optional<naim::RuntimeProcessStatus> FindInstanceRuntimeStatus(
      const std::vector<naim::RuntimeProcessStatus>& statuses,
      const std::string& instance_name) const;

  bool ProbeControllerTargetOk(
      const std::optional<ControllerEndpointTarget>& target,
      const std::string& path) const;

  std::optional<naim::RuntimeStatus> BuildPlaneScopedRuntimeStatus(
      const naim::DesiredState& desired_state,
      const naim::HostObservation& observation,
      const std::function<std::vector<naim::RuntimeProcessStatus>(
          const naim::HostObservation&)>& parse_instance_runtime_statuses) const;

  int CountReadyWorkerMembers(
      naim::ControllerStore& store,
      const naim::DesiredState& desired_state,
      const std::function<std::vector<naim::RuntimeProcessStatus>(
          const naim::HostObservation&)>& parse_instance_runtime_statuses) const;

 private:
  std::string NormalizeInteractionHost(const std::string& host) const;

  InteractionReplicaGroupSummaryBuilder interaction_replica_group_summary_builder_;
};

}  // namespace naim::controller
