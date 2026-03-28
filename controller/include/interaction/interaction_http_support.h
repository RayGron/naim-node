#pragma once

#include <map>
#include <optional>
#include <string>
#include <vector>

#include <nlohmann/json.hpp>

#include "app/controller_language_support.h"
#include "app/controller_time_support.h"
#include "http/controller_http_transport.h"
#include "http/controller_http_types.h"
#include "interaction/interaction_runtime_support_service.h"
#include "interaction/interaction_service.h"
#include "infra/controller_network_manager.h"
#include "infra/controller_runtime_support_service.h"
#include "plane/desired_state_policy_service.h"

#include "comet/core/platform_compat.h"
#include "comet/runtime/runtime_status.h"
#include "comet/state/models.h"
#include "comet/state/sqlite_store.h"

class InteractionHttpSupport final {
 public:
  InteractionHttpSupport(
      const comet::controller::ControllerRuntimeSupportService& runtime_support_service,
      const comet::controller::DesiredStatePolicyService& desired_state_policy_service,
      const comet::controller::InteractionRuntimeSupportService& interaction_runtime_support_service);

  HttpResponse BuildJsonResponse(
      int status_code,
      const nlohmann::json& payload,
      const std::map<std::string, std::string>& headers) const;
  std::string BuildInteractionUpstreamBody(
      const comet::controller::PlaneInteractionResolution& resolution,
      nlohmann::json payload,
      bool force_stream,
      const comet::controller::ResolvedInteractionPolicy& resolved_policy,
      bool structured_output_json) const;
  std::optional<std::string> FindInferInstanceName(const comet::DesiredState& desired_state) const;
  std::vector<comet::RuntimeProcessStatus> ParseInstanceRuntimeStatuses(
      const comet::HostObservation& observation) const;
  bool ObservationMatchesPlane(
      const comet::HostObservation& observation,
      const std::string& plane_name) const;
  std::optional<comet::RuntimeStatus> BuildPlaneScopedRuntimeStatus(
      const comet::DesiredState& desired_state,
      const comet::HostObservation& observation) const;
  std::optional<comet::controller::ControllerEndpointTarget> ParseInteractionTarget(
      const std::string& gateway_listen,
      int fallback_port) const;
  int CountReadyWorkerMembers(
      comet::ControllerStore& store,
      const comet::DesiredState& desired_state) const;
  bool ProbeControllerTargetOk(
      const std::optional<comet::controller::ControllerEndpointTarget>& target,
      const std::string& path) const;
  std::optional<std::string> DescribeUnsupportedControllerLocalRuntime(
      const comet::DesiredState& desired_state,
      const std::string& node_name) const;
  HttpResponse SendControllerHttpRequest(
      const comet::controller::ControllerEndpointTarget& target,
      const std::string& method,
      const std::string& path,
      const std::string& body,
      const std::vector<std::pair<std::string, std::string>>& headers) const;
  void SendHttpResponse(
      comet::platform::SocketHandle client_fd,
      const HttpResponse& response) const;
  void ShutdownAndCloseSocket(comet::platform::SocketHandle client_fd) const;
  bool SendSseHeaders(
      comet::platform::SocketHandle client_fd,
      const std::map<std::string, std::string>& headers) const;
  bool SendAll(comet::platform::SocketHandle fd, const std::string& payload) const;

 private:
  const comet::controller::ControllerRuntimeSupportService& runtime_support_service_;
  const comet::controller::DesiredStatePolicyService& desired_state_policy_service_;
  const comet::controller::InteractionRuntimeSupportService&
      interaction_runtime_support_service_;
};
