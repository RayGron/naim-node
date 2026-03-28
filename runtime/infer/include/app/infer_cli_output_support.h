#pragma once

#include <nlohmann/json.hpp>

#include "runtime/infer_control_support.h"
#include "runtime/infer_runtime_types.h"

namespace comet::infer::cli_output_support {

void PrintConfigSummary(const RuntimeConfig& config);
void PrintPrepareRuntime(const RuntimeConfig& config, bool apply);
void PrintListProfiles(const nlohmann::json& profiles_json);
void BootstrapRuntime(
    const RuntimeConfig& config,
    const control_support::RuntimeProfile& profile,
    bool apply);
int PrintRuntimeAssetsStatus(const RuntimeConfig& config);
void PrintLaunchPlan(const RuntimeConfig& config);

}  // namespace comet::infer::cli_output_support
