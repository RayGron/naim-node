#pragma once

#include <string>

#include "interaction/interaction_types.h"

namespace naim::controller {

class InteractionRuntimeRequestCodec final {
 public:
  std::string Serialize(const InteractionRuntimeExecutionRequest& request) const;
  InteractionRuntimeExecutionRequest Deserialize(const std::string& json_text) const;
};

}  // namespace naim::controller
