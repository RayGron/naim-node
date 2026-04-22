#pragma once

#include <map>
#include <optional>
#include <string>

#include <nlohmann/json.hpp>

#include "http/controller_http_transport.h"
#include "http/controller_http_types.h"
#include "interaction/interaction_http_support.h"
#include "interaction/interaction_types.h"
#include "naim/core/platform_compat.h"

class AuthSupportService;

class InteractionHttpService {
 public:
  explicit InteractionHttpService(InteractionHttpSupport support);

  naim::controller::PlaneInteractionResolution ResolvePlane(
      const std::string& db_path,
      const std::string& plane_name) const;

  naim::controller::InteractionSessionResult ExecuteSession(
      const naim::controller::PlaneInteractionResolution& resolution,
      const naim::controller::InteractionRequestContext& request_context) const;

  std::optional<naim::controller::InteractionValidationError> ResolveRequestSkills(
      const naim::controller::PlaneInteractionResolution& resolution,
      naim::controller::InteractionRequestContext* request_context) const;

  std::optional<naim::controller::InteractionValidationError> ResolveRequestBrowsing(
      const naim::controller::PlaneInteractionResolution& resolution,
      naim::controller::InteractionRequestContext* request_context) const;

  std::optional<naim::controller::InteractionValidationError> ResolveRequestKnowledge(
      const naim::controller::PlaneInteractionResolution& resolution,
      naim::controller::InteractionRequestContext* request_context) const;

  std::optional<naim::controller::InteractionValidationError> ResolveRequestContext(
      const naim::controller::PlaneInteractionResolution& resolution,
      naim::controller::InteractionRequestContext* request_context) const;

  HttpResponse BuildSessionResponse(
      const naim::controller::PlaneInteractionResolution& resolution,
      const naim::controller::InteractionRequestContext& request_context,
      const naim::controller::InteractionSessionResult& result) const;

  HttpResponse ProxyJson(
      const naim::controller::PlaneInteractionResolution& resolution,
      const std::string& request_id,
      const std::string& method,
      const std::string& path,
      const std::string& body = "") const;

 void StreamPlaneInteractionSse(
      naim::platform::SocketHandle client_fd,
      const std::string& db_path,
      const HttpRequest& request,
      AuthSupportService& auth_support) const;

 private:
  InteractionHttpSupport support_;
};
