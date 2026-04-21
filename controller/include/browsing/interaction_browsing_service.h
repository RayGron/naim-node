#pragma once

#include <optional>
#include <string>

#include <nlohmann/json.hpp>

#include "interaction/interaction_service.h"

namespace naim::controller {

class InteractionBrowsingService final {
 public:
  static constexpr const char* kSystemInstructionPayloadKey =
      "_naim_browsing_system_instruction";
  static constexpr const char* kSummaryPayloadKey =
      "_naim_browsing_summary";
  static constexpr const char* kWebGatewayContextPayloadKey =
      "_naim_webgateway_context";
  static constexpr const char* kWebGatewayPolicyPayloadKey =
      "_naim_webgateway_policy";
  static constexpr const char* kWebGatewayReviewPayloadKey =
      "_naim_webgateway_review";

  std::optional<InteractionValidationError> ResolveInteractionBrowsing(
      const PlaneInteractionResolution& resolution,
      InteractionRequestContext* context) const;

  void ReviewInteractionResponse(
      const PlaneInteractionResolution& resolution,
      const InteractionRequestContext& request_context,
      InteractionSessionResult* result) const;

 private:
  std::string ReadPersistedBrowsingMode(
      const InteractionRequestContext& context) const;

  std::string LastUserMessageContent(
      const InteractionRequestContext& context) const;

  bool LatestMessageRequestsLookup(
      const InteractionRequestContext& context) const;

  std::optional<nlohmann::json> BuildLocalEnabledIdleContext(
      const InteractionRequestContext& context) const;

  void PersistBrowsingMode(
      const nlohmann::json& webgateway_context,
      InteractionRequestContext* context) const;
};

}  // namespace naim::controller
