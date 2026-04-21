#pragma once

#include <filesystem>
#include <string>

#include "cli/launcher_command_line.h"

namespace naim::launcher {

class HostdRegistrationService {
 public:
  void Connect(const LauncherCommandLine& command_line) const;

 private:
  std::string ReadPublicKeyBase64Argument(const std::string& value) const;
  std::string ReadTextFile(const std::filesystem::path& path) const;
  std::string Trim(const std::string& value) const;
};

}  // namespace naim::launcher
