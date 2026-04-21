#pragma once

#include <string>

namespace naim::hostd {

class HostdCommandSupport final {
 public:
  std::string Trim(const std::string& value) const;
  std::string RunCommandCapture(const std::string& command) const;
  std::string ShellQuote(const std::string& value) const;
  bool RunCommandOk(const std::string& command) const;
  std::string ResolvedDockerCommand() const;
  std::string ResolvedDockerComposeCommand() const;
};

}  // namespace naim::hostd
