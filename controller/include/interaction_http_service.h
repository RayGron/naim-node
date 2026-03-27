#pragma once

#include <functional>
#include <map>
#include <optional>
#include <string>

#include <nlohmann/json.hpp>

#include "controller_http_transport.h"
#include "controller_http_types.h"
#include "interaction_service.h"
#include "comet/platform_compat.h"

class InteractionHttpService {
 public:
  using BuildJsonResponseFn = std::function<HttpResponse(
      int,
      const nlohmann::json&,
      const std::map<std::string, std::string>&)>;
  using BuildInteractionUpstreamBodyFn = std::function<std::string(
      const comet::controller::PlaneInteractionResolution&,
      nlohmann::json,
      bool,
      const comet::controller::ResolvedInteractionPolicy&,
      bool)>;
  using FindInferInstanceNameFn = std::function<std::optional<std::string>(
      const comet::DesiredState&)>;
  using ParseInstanceRuntimeStatusesFn = std::function<
      std::vector<comet::RuntimeProcessStatus>(const comet::HostObservation&)>;
  using ObservationMatchesPlaneFn = std::function<bool(
      const comet::HostObservation&,
      const std::string&)>;
  using BuildPlaneScopedRuntimeStatusFn = std::function<
      std::optional<comet::RuntimeStatus>(
          const comet::DesiredState&,
          const comet::HostObservation&)>;
  using ParseInteractionTargetFn = std::function<
      std::optional<comet::controller::ControllerEndpointTarget>(
          const std::string&,
          int)>;
  using CountReadyWorkerMembersFn = std::function<int(
      comet::ControllerStore&,
      const comet::DesiredState&)>;
  using ProbeControllerTargetOkFn = std::function<bool(
      const std::optional<comet::controller::ControllerEndpointTarget>&,
      const std::string&)>;
  using DescribeUnsupportedControllerLocalRuntimeFn = std::function<
      std::optional<std::string>(
          const comet::DesiredState&,
          const std::string&)>;
  using SendControllerHttpRequestFn = std::function<HttpResponse(
      const comet::controller::ControllerEndpointTarget&,
      const std::string&,
      const std::string&,
      const std::string&,
      const std::vector<std::pair<std::string, std::string>>&)>;
  using SendHttpResponseFn = std::function<void(
      comet::platform::SocketHandle,
      const HttpResponse&)>;
  using ShutdownAndCloseSocketFn =
      std::function<void(comet::platform::SocketHandle)>;
  using SendSseHeadersFn = std::function<bool(
      comet::platform::SocketHandle,
      const std::map<std::string, std::string>&)>;
  using SendAllFn = std::function<bool(
      comet::platform::SocketHandle,
      const std::string&)>;

  struct Deps {
    BuildJsonResponseFn build_json_response;
    BuildInteractionUpstreamBodyFn build_interaction_upstream_body;
    FindInferInstanceNameFn find_infer_instance_name;
    ParseInstanceRuntimeStatusesFn parse_instance_runtime_statuses;
    ObservationMatchesPlaneFn observation_matches_plane;
    BuildPlaneScopedRuntimeStatusFn build_plane_scoped_runtime_status;
    ParseInteractionTargetFn parse_interaction_target;
    CountReadyWorkerMembersFn count_ready_worker_members;
    ProbeControllerTargetOkFn probe_controller_target_ok;
    DescribeUnsupportedControllerLocalRuntimeFn
        describe_unsupported_controller_local_runtime;
    SendControllerHttpRequestFn send_controller_http_request;
    SendHttpResponseFn send_http_response;
    ShutdownAndCloseSocketFn shutdown_and_close_socket;
    SendSseHeadersFn send_sse_headers;
    SendAllFn send_all;
  };

  explicit InteractionHttpService(Deps deps);

  comet::controller::PlaneInteractionResolution ResolvePlane(
      const std::string& db_path,
      const std::string& plane_name) const;

  comet::controller::InteractionSessionResult ExecuteSession(
      const comet::controller::PlaneInteractionResolution& resolution,
      const comet::controller::InteractionRequestContext& request_context) const;

  HttpResponse BuildSessionResponse(
      const comet::controller::PlaneInteractionResolution& resolution,
      const comet::controller::InteractionRequestContext& request_context,
      const comet::controller::InteractionSessionResult& result) const;

  HttpResponse ProxyJson(
      const comet::controller::PlaneInteractionResolution& resolution,
      const std::string& request_id,
      const std::string& method,
      const std::string& path,
      const std::string& body = "") const;

  void StreamPlaneInteractionSse(
      comet::platform::SocketHandle client_fd,
      const std::string& db_path,
      const HttpRequest& request) const;

 private:
  static nlohmann::json BuildContinuationPayload(
      const nlohmann::json& original_payload,
      const std::string& accumulated_text,
      const comet::controller::InteractionCompletionPolicy& policy,
      bool natural_stop_without_marker,
      int total_completion_tokens);

  bool SendInteractionSseEvent(
      comet::platform::SocketHandle client_fd,
      const std::string& event_name,
      const nlohmann::json& payload) const;

  bool SendInteractionSseDone(comet::platform::SocketHandle client_fd) const;

  comet::controller::InteractionPlaneResolver MakePlaneResolver() const;
  comet::controller::InteractionSessionExecutor MakeSessionExecutor() const;
  comet::controller::InteractionStreamSegmentExecutor
  MakeStreamSegmentExecutor() const;
  comet::controller::InteractionProxyExecutor MakeProxyExecutor() const;
  comet::controller::InteractionStreamRequestResolver
  MakeStreamRequestResolver() const;
  comet::controller::InteractionStreamSessionExecutor
  MakeStreamSessionExecutor() const;

  Deps deps_;
};
