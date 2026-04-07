#pragma once

#include <filesystem>
#include <optional>
#include <string>

#include "comet/state/models.h"

namespace comet::hostd::appsupport {

std::optional<std::filesystem::path> DetectCometRepoRoot();

std::optional<std::filesystem::path> ResolvePlaneOwnedPath(
    const comet::DesiredState& state,
    const std::string& relative_path,
    const std::string& artifacts_root);

}  // namespace comet::hostd::appsupport
