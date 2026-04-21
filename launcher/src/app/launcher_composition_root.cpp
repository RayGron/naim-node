#include "app/launcher_composition_root.h"

#include <cstdlib>
#include <iostream>
#include <stdexcept>

#include "naim/core/platform_compat.h"

namespace naim::launcher {

LauncherCompositionRoot::LauncherCompositionRoot()
    : signal_manager_(std::make_unique<SignalManager>()),
      path_resolver_(std::make_unique<LauncherPathResolver>()),
      process_runner_(std::make_unique<ProcessRunner>()),
      install_layout_resolver_(std::make_unique<InstallLayoutResolver>()),
      config_loader_(std::make_unique<GeneratedConfigLoader>(*install_layout_resolver_)),
      install_service_(
          std::make_unique<LauncherInstallService>(*install_layout_resolver_, *process_runner_)),
      run_service_(std::make_unique<LauncherRunService>(
          *install_layout_resolver_,
          *config_loader_,
          *process_runner_,
          *install_service_)),
      doctor_service_(std::make_unique<LauncherDoctorService>(*process_runner_)),
      hostd_registration_service_(std::make_unique<HostdRegistrationService>()) {}

int LauncherCompositionRoot::Run(
    const LauncherCommandLine& command_line,
    const char* argv0) const {
  signal_manager_->RegisterHandlers();

  if (!command_line.HasCommand()) {
    command_line.PrintUsage(std::cout);
    return 1;
  }

  const std::filesystem::path self_path = path_resolver_->ResolveSelfPath(argv0);
  const std::vector<std::string>& args = command_line.args();
  const std::string& command = command_line.command();

  try {
    if (command == "version") {
      std::cout << "naim-node 0.1.0\n";
      return 0;
    }

    if (command == "doctor") {
      doctor_service_->Run(
          self_path,
          args.size() > 1 ? std::optional<std::string>(args[1]) : std::nullopt);
      return 0;
    }

    if (command == "connect-hostd") {
      hostd_registration_service_->Connect(command_line);
      return 0;
    }

    if (command == "install") {
      if (args.size() < 2) {
        throw std::runtime_error("install requires role");
      }
      const LauncherCommandLine install_command_line(command_line.Tail(2));
      if (args[1] == "controller") {
        install_service_->InstallController(self_path, install_command_line);
        return 0;
      }
      if (args[1] == "hostd") {
        install_service_->InstallHostd(self_path, install_command_line);
        return 0;
      }
      throw std::runtime_error("unknown install role '" + args[1] + "'");
    }

    if (command == "service") {
      if (args.size() < 3) {
        throw std::runtime_error("service requires action and role");
      }
      install_service_->ServiceCommand(
          args[1],
          args[2],
          LauncherCommandLine(command_line.Tail(3)));
      return 0;
    }

    if (command == "run") {
      if (args.size() < 2) {
        throw std::runtime_error("run requires role");
      }
      if (args[1] == "controller") {
        const std::filesystem::path controller_binary =
            path_resolver_->ResolveSiblingBinary(self_path, "naim-controller");
        return run_service_->RunController(
            *signal_manager_,
            self_path,
            controller_binary,
            command_line);
      }
      if (args[1] == "hostd") {
        const std::filesystem::path hostd_binary =
            path_resolver_->ResolveSiblingBinary(self_path, "naim-hostd");
        return run_service_->RunHostd(
            *signal_manager_,
            hostd_binary,
            self_path,
            command_line);
      }
      throw std::runtime_error("unknown run role '" + args[1] + "'");
    }
  } catch (const std::exception& error) {
    std::cerr << "error: " << error.what() << "\n";
    return 1;
  }

  command_line.PrintUsage(std::cout);
  return 1;
}

bool LauncherCompositionRoot::RunningInManagedServiceMode() const {
  const char* value = std::getenv("NAIM_SERVICE_MODE");
  return value != nullptr && std::string(value) == "1";
}

bool LauncherCompositionRoot::SystemdAvailable() const {
#if defined(_WIN32)
  return false;
#else
  if (!process_runner_->CommandExists("systemctl")) {
    return false;
  }
  if (naim::platform::HasElevatedPrivileges()) {
    return process_runner_->RunShellCommand("systemctl is-system-running >/dev/null 2>&1") == 0;
  }
  return process_runner_->RunShellCommand("systemctl --user is-system-running >/dev/null 2>&1") ==
         0;
#endif
}

}  // namespace naim::launcher
