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

  std::optional<InteractionValidationError> ResolveInteractionBrowsing(
      const PlaneInteractionResolution& resolution,
      InteractionRequestContext* context) const;
};

}  // namespace comet::controller
