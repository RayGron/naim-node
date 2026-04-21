#pragma once

#include <string>
#include <vector>

namespace naim::launcher {

class ProcessRunner {
 public:
  bool CommandExists(const std::string& command) const;
  int RunShellCommand(const std::string& command) const;
  std::string CaptureShellOutput(const std::string& command) const;
  int RunCommand(const std::vector<std::string>& args) const;
  int SpawnCommand(const std::vector<std::string>& args) const;
};

}  // namespace naim::launcher
