#pragma once

#include <functional>
#include <string>

#include <nlohmann/json.hpp>

namespace comet::controller {

struct ControllerActionResult {
  std::string action_name;
  int exit_code = 0;
  std::string output;
};

ControllerActionResult RunControllerActionResult(
    const std::string& action_name,
    const std::function<int()>& action);

nlohmann::json BuildControllerActionPayload(const ControllerActionResult& result);

int EmitControllerActionResult(const ControllerActionResult& result);

}  // namespace comet::controller
