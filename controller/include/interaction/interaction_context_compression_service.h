#pragma once

#include "interaction/interaction_types.h"

namespace naim::controller {

class InteractionContextCompressionService final {
 public:
  void Apply(
      const PlaneInteractionResolution& resolution,
      InteractionRequestContext* request_context) const;
};

}  // namespace naim::controller
