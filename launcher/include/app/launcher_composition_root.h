#pragma once

#include <memory>

#include "app/hostd_registration_service.h"
#include "app/launcher_doctor_service.h"
#include "cli/launcher_command_line.h"
#include "config/generated_config_loader.h"
#include "config/install_layout.h"
#include "install/launcher_install_service.h"
#include "platform/launcher_path_resolver.h"
#include "platform/process_runner.h"
#include "platform/signal_manager.h"
#include "run/launcher_run_service.h"

namespace naim::launcher {

class LauncherCompositionRoot {
 public:
  LauncherCompositionRoot();

  int Run(const LauncherCommandLine& command_line, const char* argv0) const;

 private:
  bool RunningInManagedServiceMode() const;
  bool SystemdAvailable() const;

  std::unique_ptr<SignalManager> signal_manager_;
  std::unique_ptr<LauncherPathResolver> path_resolver_;
  std::unique_ptr<ProcessRunner> process_runner_;
  std::unique_ptr<InstallLayoutResolver> install_layout_resolver_;
  std::unique_ptr<GeneratedConfigLoader> config_loader_;
  std::unique_ptr<LauncherInstallService> install_service_;
  std::unique_ptr<LauncherRunService> run_service_;
  std::unique_ptr<LauncherDoctorService> doctor_service_;
  std::unique_ptr<HostdRegistrationService> hostd_registration_service_;
};

}  // namespace naim::launcher
