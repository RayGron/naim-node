#pragma once

#include <optional>
#include <string>

#include "interaction/interaction_types.h"
#include "naim/state/sqlite_store.h"

namespace naim::controller {

class InteractionTargetRelayPolicy final {
 public:
  void EnableHostdRuntimeRelayForRemoteLoopback(
      naim::ControllerStore& store,
      const std::string& db_path,
      const std::string& node_name,
      const std::string& plane_name,
      std::optional<ControllerEndpointTarget>* target) const;

 private:
  static bool IsLoopbackInteractionTarget(const ControllerEndpointTarget& target);
};

}  // namespace naim::controller
