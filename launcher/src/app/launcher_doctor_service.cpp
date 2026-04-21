#include "app/launcher_doctor_service.h"

#include <filesystem>
#include <iostream>
#include <set>

namespace naim::launcher {

namespace fs = std::filesystem;

LauncherDoctorService::LauncherDoctorService(const ProcessRunner& process_runner)
    : process_runner_(process_runner) {}

void LauncherDoctorService::Run(
    const fs::path& self_path,
    const std::optional<std::string>& role) const {
  const std::set<std::string> required_commands = {"docker"};
  const fs::path controller_binary = self_path.parent_path() / "naim-controller";
  const fs::path hostd_binary = self_path.parent_path() / "naim-hostd";
  std::cout << "doctor\n";
  std::cout << "binary=" << self_path << "\n";
  if (!role.has_value() || *role == "controller") {
    std::cout << "controller_binary=" << (fs::exists(controller_binary) ? "yes" : "no") << "\n";
  }
  if (!role.has_value() || *role == "hostd") {
    std::cout << "hostd_binary=" << (fs::exists(hostd_binary) ? "yes" : "no") << "\n";
  }
  for (const std::string& command : required_commands) {
    std::cout << command << "=" << (process_runner_.CommandExists(command) ? "yes" : "no")
              << "\n";
  }
  std::cout << "systemctl=" << (process_runner_.CommandExists("systemctl") ? "yes" : "no")
            << "\n";
  std::cout << "systemd_analyze="
            << (process_runner_.CommandExists("systemd-analyze") ? "yes" : "no") << "\n";
}

}  // namespace naim::launcher
