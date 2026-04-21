#pragma once

#include <optional>
#include <string>

#include "app/controller_command_line.h"

namespace naim::controller {

class ControllerApp final {
 public:
  ControllerApp(int argc, char** argv);

  ControllerApp(const ControllerApp&) = delete;
  ControllerApp& operator=(const ControllerApp&) = delete;

  int Run();

 private:
  static std::string ResolveDbPath(const std::optional<std::string>& db_arg);

  ControllerCommandLine cli_;
};

}  // namespace naim::controller
