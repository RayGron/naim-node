#pragma once

#include <filesystem>
#include <optional>
#include <string>

#include "platform/process_runner.h"

namespace naim::launcher {

class LauncherDoctorService {
 public:
  explicit LauncherDoctorService(const ProcessRunner& process_runner);

  void Run(const std::filesystem::path& self_path, const std::optional<std::string>& role) const;

 private:
  const ProcessRunner& process_runner_;
};

}  // namespace naim::launcher
