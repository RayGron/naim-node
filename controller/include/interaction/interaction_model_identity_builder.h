#pragma once

#include <string_view>

#include "naim/runtime/model_adapter.h"
#include "interaction/interaction_service.h"

namespace naim::controller {

class InteractionModelIdentityBuilder final {
 public:
  naim::runtime::ModelIdentity BuildRuntimePreferred(
      const PlaneInteractionResolution& resolution) const;

  naim::runtime::ModelIdentity BuildStatusPreferred(
      const PlaneInteractionResolution& resolution) const;

 private:
  std::string ReadJsonStringOrEmpty(
      const nlohmann::json& payload,
      std::string_view key) const;
};

}  // namespace naim::controller
