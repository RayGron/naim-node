#pragma once

#include <map>
#include <optional>
#include <string>

#include <nlohmann/json.hpp>

#include "http/controller_http_transport.h"
#include "http/controller_http_types.h"
#include "interaction/interaction_http_support.h"
#include "interaction/interaction_service.h"
#include "comet/core/platform_compat.h"

class InteractionHttpService {
 public:
  explicit InteractionHttpService(InteractionHttpSupport support);

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

  InteractionHttpSupport support_;
};
