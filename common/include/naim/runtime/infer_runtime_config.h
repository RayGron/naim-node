#pragma once

#include <string>

#include "naim/state/models.h"

namespace naim {

std::string InferRuntimeConfigRelativePath(const std::string& infer_instance_name);
std::string InferRuntimeConfigControlPath(
    const std::string& control_root,
    const std::string& infer_instance_name);
std::string InferRuntimeStatusRelativePath(const std::string& infer_instance_name);
std::string InferRuntimeStatusControlPath(
    const std::string& control_root,
    const std::string& infer_instance_name);
std::string RenderInferRuntimeConfigJson(const DesiredState& state);
std::string RenderInferRuntimeConfigJsonForInstance(
    const DesiredState& state,
    const std::string& infer_instance_name);

}  // namespace naim
