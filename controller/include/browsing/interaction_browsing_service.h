#pragma once

#include <optional>
#include <string>

#include <nlohmann/json.hpp>

#include "interaction/interaction_service.h"

namespace comet::controller {

class InteractionBrowsingService final {
 public:
  static constexpr const char* kSystemInstructionPayloadKey =
      "_comet_browsing_system_instruction";
  static constexpr const char* kSummaryPayloadKey =
      "_comet_browsing_summary";
  static constexpr const char* kWebGatewayContextPayloadKey =
      "_comet_webgateway_context";
  static constexpr const char* kWebGatewayPolicyPayloadKey =
      "_comet_webgateway_policy";
  static constexpr const char* kWebGatewayReviewPayloadKey =
      "_comet_webgateway_review";

  std::optional<InteractionValidationError> ResolveInteractionBrowsing(
      const PlaneInteractionResolution& resolution,
      InteractionRequestContext* context) const;

  void ReviewInteractionResponse(
      const PlaneInteractionResolution& resolution,
      const InteractionRequestContext& request_context,
      InteractionSessionResult* result) const;
};

}  // namespace comet::controller
