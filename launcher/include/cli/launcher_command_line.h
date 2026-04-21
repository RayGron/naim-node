#pragma once

#include <optional>
#include <ostream>
#include <string>
#include <vector>

namespace naim::launcher {

class LauncherCommandLine {
 public:
  static LauncherCommandLine FromArgv(int argc, char** argv);

  explicit LauncherCommandLine(std::vector<std::string> args);

  bool HasCommand() const;
  const std::string& command() const;
  const std::vector<std::string>& args() const;
  std::vector<std::string> Tail(std::size_t offset) const;

  void PrintUsage(std::ostream& out) const;

  std::optional<std::string> FindFlagValue(const std::string& flag) const;
  bool HasFlag(const std::string& flag) const;

  static int ParseIntValue(const std::optional<std::string>& value, int fallback);

 private:
  std::vector<std::string> args_;
};

}  // namespace naim::launcher
