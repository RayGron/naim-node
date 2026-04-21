#pragma once

#include <optional>
#include <string>

#include "naim/state/models.h"

namespace naim {

DesiredState SliceDesiredStateForNode(
    const DesiredState& state,
    const std::string& node_name);

DesiredState ResolvePlacementTargetAliases(DesiredState state);

std::string SerializeDesiredStateJson(const DesiredState& state);
std::string SerializeDesiredStateV2Json(const DesiredState& state);
DesiredState DeserializeDesiredStateJson(const std::string& json_text);

std::optional<DesiredState> LoadDesiredStateJson(const std::string& path);
void SaveDesiredStateJson(const DesiredState& state, const std::string& path);

}  // namespace naim
