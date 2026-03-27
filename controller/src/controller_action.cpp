#include "../include/controller_action.h"

#include <iostream>
#include <sstream>

namespace comet::controller {

ControllerActionResult RunControllerActionResult(
    const std::string& action_name,
    const std::function<int()>& action) {
  std::ostringstream captured_stdout;
  auto* const original_stdout = std::cout.rdbuf(captured_stdout.rdbuf());
  try {
    const int exit_code = action();
    std::cout.rdbuf(original_stdout);
    return ControllerActionResult{action_name, exit_code, captured_stdout.str()};
  } catch (...) {
    std::cout.rdbuf(original_stdout);
    throw;
  }
}

nlohmann::json BuildControllerActionPayload(const ControllerActionResult& result) {
  return nlohmann::json{
      {"status", result.exit_code == 0 ? "ok" : "failed"},
      {"action", result.action_name},
      {"exit_code", result.exit_code},
      {"output", result.output},
  };
}

int EmitControllerActionResult(const ControllerActionResult& result) {
  std::cout << result.output;
  return result.exit_code;
}

}  // namespace comet::controller
