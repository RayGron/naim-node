#pragma once

#include <optional>
#include <string>

#include <nlohmann/json.hpp>

#include "interaction/interaction_service.h"

namespace comet::controller {

struct InteractionConversationPrincipal {
  std::string owner_kind = "anonymous";
  std::optional<int> owner_user_id;
  std::string auth_session_kind;
  bool authenticated = false;
};

class InteractionConversationService final {
 public:
  std::optional<InteractionValidationError> PrepareRequest(
      const std::string& db_path,
      const PlaneInteractionResolution& resolution,
      const InteractionConversationPrincipal& principal,
      InteractionRequestContext* context) const;

  std::optional<InteractionValidationError> PersistResponse(
      const std::string& db_path,
      const PlaneInteractionResolution& resolution,
      InteractionRequestContext* context,
      const InteractionSessionResult& result) const;

  nlohmann::json BuildSessionsListPayload(
      const std::string& db_path,
      const std::string& plane_name,
      int user_id) const;

  std::optional<nlohmann::json> BuildSessionDetailPayload(
      const std::string& db_path,
      const std::string& plane_name,
      int user_id,
      const std::string& session_id) const;

  bool DeleteSession(
      const std::string& db_path,
      const std::string& plane_name,
      int user_id,
      const std::string& session_id) const;
};

}  // namespace comet::controller
