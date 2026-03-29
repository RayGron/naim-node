#pragma once

#include <optional>
#include <string>

#include "comet/state/models.h"

namespace comet {

DesiredState SliceDesiredStateForNode(
    const DesiredState& state,
    const std::string& node_name);

DesiredState ResolvePlacementTargetAliases(DesiredState state);

std::string SerializeDesiredStateJson(const DesiredState& state);
std::string SerializeDesiredStateV2Json(const DesiredState& state);
DesiredState DeserializeDesiredStateJson(const std::string& json_text);

std::optional<DesiredState> LoadDesiredStateJson(const std::string& path);
void SaveDesiredStateJson(const DesiredState& state, const std::string& path);

}  // namespace comet
