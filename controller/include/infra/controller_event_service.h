#pragma once

#include <optional>
#include <string>

#include <nlohmann/json.hpp>

#include "naim/state/sqlite_store.h"

namespace naim::controller {

class ControllerEventService {
 public:
  void AppendEvent(
      naim::ControllerStore& store,
      const std::string& category,
      const std::string& event_type,
      const std::string& message,
      const nlohmann::json& payload = nlohmann::json::object(),
      const std::string& plane_name = "",
      const std::string& node_name = "",
      const std::string& worker_name = "",
      const std::optional<int>& assignment_id = std::nullopt,
      const std::optional<int>& rollout_action_id = std::nullopt,
      const std::string& severity = "info") const;

 private:
  std::string SerializePayload(const nlohmann::json& payload) const;
};

}  // namespace naim::controller
