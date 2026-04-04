#pragma once

#include <string>

#include <nlohmann/json.hpp>

#include "interaction/interaction_service.h"

namespace comet::controller {

std::string BuildInteractionUpstreamBodyPayload(
    const PlaneInteractionResolution& resolution,
    nlohmann::json payload,
    bool force_stream,
    const ResolvedInteractionPolicy& resolved_policy,
    bool structured_output_json = false);

}  // namespace comet::controller
