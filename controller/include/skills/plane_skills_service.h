#pragma once

#include <optional>
#include <string>

#include <nlohmann/json.hpp>

#include "http/controller_http_transport.h"
#include "http/controller_http_types.h"
#include "interaction/interaction_service.h"

namespace comet::controller {

class PlaneSkillsService final {
 public:
  static constexpr const char* kSystemInstructionPayloadKey =
      "_comet_skills_system_instruction";
  static constexpr const char* kAppliedSkillsPayloadKey = "_comet_applied_skills";
  static constexpr const char* kSkillsSessionIdPayloadKey = "_comet_skills_session_id";

  bool IsEnabled(const DesiredState& desired_state) const;

  std::optional<ControllerEndpointTarget> ResolveTarget(
      const DesiredState& desired_state) const;

  std::optional<InteractionValidationError> ResolveInteractionSkills(
      const PlaneInteractionResolution& resolution,
      InteractionRequestContext* context) const;

  std::optional<HttpResponse> ProxyPlaneSkillsRequest(
      const PlaneInteractionResolution& resolution,
      const std::string& method,
      const std::string& path_suffix,
      const std::string& body,
      std::string* error_code,
      std::string* error_message) const;

 private:
  static std::optional<std::string> ParseSessionId(const nlohmann::json& payload);
  static std::optional<std::vector<std::string>> ParseSkillIds(const nlohmann::json& payload);
  static std::string BuildSystemInstruction(const nlohmann::json& skills);
};

}  // namespace comet::controller

