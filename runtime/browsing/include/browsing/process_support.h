#pragma once

#include <filesystem>
#include <map>
#include <optional>
#include <string>
#include <vector>

namespace naim::browsing {

struct CommandRequest {
  std::vector<std::string> args;
  std::map<std::string, std::string> environment;
  std::optional<std::filesystem::path> working_directory;
  bool clear_environment = false;
  bool merge_stderr_into_stdout = true;
};

struct CommandResult {
  int exit_code = 0;
  std::string output;
};

CommandResult RunCommandCapture(const CommandRequest& request);

}  // namespace naim::browsing
