#pragma once

#include "app/infer_command_line.h"
#include "runtime/infer_control_support.h"
#include "runtime/infer_runtime_types.h"

namespace comet::infer::model_cache_support {

void PreloadModel(
    const RuntimeConfig& config,
    const InferCommandLineOptions& args,
    bool apply);

int CacheStatus(
    const RuntimeConfig& config,
    const InferCommandLineOptions& args);

void SwitchModel(
    const RuntimeConfig& config,
    const control_support::RuntimeProfile& profile,
    const InferCommandLineOptions& args,
    bool apply);

}  // namespace comet::infer::model_cache_support
