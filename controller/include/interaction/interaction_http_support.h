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
#include "interaction/interaction_hostd_runtime_relay_service.h"
#include "interaction/interaction_runtime_support_service.h"
#include "interaction/interaction_types.h"
#include "infra/controller_network_manager.h"
#include "infra/controller_runtime_support_service.h"
#include "observation/plane_observation_matcher.h"
#include "plane/desired_state_policy_service.h"

#include "naim/core/platform_compat.h"
#include "naim/runtime/runtime_status.h"
#include "naim/state/models.h"
#include "naim/state/sqlite_store.h"

class InteractionHttpSupport final {
 public:
  InteractionHttpSupport(
      const naim::controller::ControllerRuntimeSupportService& runtime_support_service,
      const naim::controller::DesiredStatePolicyService& desired_state_policy_service,
      const naim::controller::InteractionRuntimeSupportService& interaction_runtime_support_service);

  HttpResponse BuildJsonResponse(
      int status_code,
      const nlohmann::json& payload,
      const std::map<std::string, std::string>& headers) const;
  std::string BuildInteractionUpstreamBody(
      const naim::controller::PlaneInteractionResolution& resolution,
      nlohmann::json payload,
      bool force_stream,
      const naim::controller::ResolvedInteractionPolicy& resolved_policy,
      bool structured_output_json) const;
  std::string BuildInteractionRuntimeRequestBody(
      const naim::controller::PlaneInteractionResolution& resolution,
      nlohmann::json payload,
      bool force_stream,
      const naim::controller::ResolvedInteractionPolicy& resolved_policy,
      bool structured_output_json) const;
  std::optional<std::string> FindInferInstanceName(const naim::DesiredState& desired_state) const;
  std::vector<naim::RuntimeProcessStatus> ParseInstanceRuntimeStatuses(
      const naim::HostObservation& observation) const;
  bool ObservationMatchesPlane(
      const naim::HostObservation& observation,
      const std::string& plane_name) const;
  std::optional<naim::RuntimeStatus> BuildPlaneScopedRuntimeStatus(
      const naim::DesiredState& desired_state,
      const naim::HostObservation& observation) const;
  std::optional<naim::controller::ControllerEndpointTarget> ParseInteractionTarget(
      const std::string& gateway_listen,
      int fallback_port) const;
  std::optional<naim::controller::ControllerEndpointTarget> ResolvePlaneLocalInteractionTarget(
      const naim::DesiredState& desired_state) const;
  int CountReadyWorkerMembers(
      naim::ControllerStore& store,
      const naim::DesiredState& desired_state) const;
  bool ProbeControllerTargetOk(
      const std::optional<naim::controller::ControllerEndpointTarget>& target,
      const std::string& path) const;
  std::optional<std::string> DescribeUnsupportedControllerLocalRuntime(
      const naim::DesiredState& desired_state,
      const std::string& node_name) const;
  HttpResponse SendControllerHttpRequest(
      const naim::controller::ControllerEndpointTarget& target,
      const std::string& method,
      const std::string& path,
      const std::string& body,
      const std::vector<std::pair<std::string, std::string>>& headers) const;
  void SendHttpResponse(
      naim::platform::SocketHandle client_fd,
      const HttpResponse& response) const;
  void ShutdownAndCloseSocket(naim::platform::SocketHandle client_fd) const;
  bool SendSseHeaders(
      naim::platform::SocketHandle client_fd,
      const std::map<std::string, std::string>& headers) const;
  bool SendAll(naim::platform::SocketHandle fd, const std::string& payload) const;

 private:
  const naim::controller::ControllerRuntimeSupportService& runtime_support_service_;
  const naim::controller::DesiredStatePolicyService& desired_state_policy_service_;
  const naim::controller::InteractionRuntimeSupportService&
      interaction_runtime_support_service_;
  naim::controller::InteractionHostdRuntimeRelayService hostd_runtime_relay_service_;
  naim::controller::PlaneObservationMatcher plane_observation_matcher_;
};
