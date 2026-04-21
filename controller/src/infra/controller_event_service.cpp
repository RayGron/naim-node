#include "infra/controller_event_service.h"

namespace naim::controller {

std::string ControllerEventService::SerializePayload(
    const nlohmann::json& payload) const {
  return payload.dump();
}

void ControllerEventService::AppendEvent(
    naim::ControllerStore& store,
    const std::string& category,
    const std::string& event_type,
    const std::string& message,
    const nlohmann::json& payload,
    const std::string& plane_name,
    const std::string& node_name,
    const std::string& worker_name,
    const std::optional<int>& assignment_id,
    const std::optional<int>& rollout_action_id,
    const std::string& severity) const {
  store.AppendEvent(naim::EventRecord{
      0,
      plane_name,
      node_name,
      worker_name,
      assignment_id,
      rollout_action_id,
      category,
      event_type,
      severity,
      message,
      SerializePayload(payload),
      "",
  });
}

}  // namespace naim::controller
