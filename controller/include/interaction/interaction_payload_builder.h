#pragma once

#include <string>

#include <nlohmann/json.hpp>

#include "interaction/interaction_types.h"

namespace naim::controller {

std::string BuildInteractionUpstreamBodyPayload(
    const PlaneInteractionResolution& resolution,
    nlohmann::json payload,
    bool force_stream,
    const ResolvedInteractionPolicy& resolved_policy,
    bool structured_output_json = false);

}  // namespace naim::controller
