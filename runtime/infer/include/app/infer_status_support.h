#pragma once

#include <string>

#include "runtime/infer_runtime_types.h"

namespace comet::infer::status_support {

void PrintGatewayPlan(const RuntimeConfig& config, bool apply);
int PrintGatewayStatus(const RuntimeConfig& config);
int PrintStatus(const RuntimeConfig& config, const std::string& backend, bool apply);
void StopRuntime(const RuntimeConfig& config, bool apply, const std::string& backend);
int RunDoctor(const RuntimeConfig& config, const std::string& checks);

}  // namespace comet::infer::status_support
