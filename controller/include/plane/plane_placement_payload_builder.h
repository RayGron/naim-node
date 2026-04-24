#pragma once

#include <optional>
#include <set>
#include <vector>

#include <nlohmann/json.hpp>

#include "naim/state/models.h"

namespace naim::controller {

class PlanePlacementPayloadBuilder final {
 public:
  explicit PlanePlacementPayloadBuilder(const naim::DesiredState& desired_state);

  nlohmann::json Build() const;

 private:
  std::optional<std::string> ResolveExternalAppHostAuthMode() const;
  std::optional<std::string> FindFirstInstanceNodeName(naim::InstanceRole role) const;
  std::vector<const naim::InstanceSpec*> FindInstances(naim::InstanceRole role) const;
  std::set<std::string> FindInstanceNodeNames(naim::InstanceRole role) const;

  const naim::DesiredState& desired_state_;
};

}  // namespace naim::controller
