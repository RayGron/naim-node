#include "interaction/interaction_target_relay_policy.h"

namespace naim::controller {

void InteractionTargetRelayPolicy::EnableHostdRuntimeRelayForRemoteLoopback(
    naim::ControllerStore& store,
    const std::string& db_path,
    const std::string& node_name,
    const std::string& plane_name,
    std::optional<ControllerEndpointTarget>* target) const {
  if (target == nullptr || !target->has_value() ||
      !IsLoopbackInteractionTarget(**target) || node_name.empty()) {
    return;
  }
  const auto host = store.LoadRegisteredHost(node_name);
  if (!host.has_value() ||
      host->registration_state != "registered" ||
      host->transport_mode != "out") {
    return;
  }
  (*target)->use_hostd_runtime_relay = true;
  (*target)->relay_db_path = db_path;
  (*target)->relay_node_name = node_name;
  (*target)->relay_plane_name = plane_name;
}

bool InteractionTargetRelayPolicy::IsLoopbackInteractionTarget(
    const ControllerEndpointTarget& target) {
  return target.host == "127.0.0.1" || target.host == "localhost" ||
         target.host == "::1";
}

}  // namespace naim::controller
