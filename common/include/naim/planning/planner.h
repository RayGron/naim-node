#pragma once

#include <optional>
#include <string>
#include <vector>

#include "naim/state/models.h"

namespace naim {

std::vector<NodeComposePlan> BuildNodeComposePlans(const DesiredState& state);
std::optional<NodeComposePlan> FindNodeComposePlan(
    const DesiredState& state,
    const std::string& node_name);

}  // namespace naim
