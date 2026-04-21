#pragma once

#include <filesystem>
#include <string>
#include <vector>

#include "cli/launcher_command_line.h"
#include "config/generated_config_loader.h"
#include "config/install_layout.h"
#include "config/launcher_options.h"
#include "platform/process_runner.h"

namespace naim::launcher {

class LauncherInstallService {
 public:
  LauncherInstallService(
      const InstallLayoutResolver& install_layout_resolver,
      const ProcessRunner& process_runner);

  InstallLayout ParseLayout(const LauncherCommandLine& command_line) const;
  void InstallController(const std::filesystem::path& self_path, const LauncherCommandLine& command_line) const;
  void InstallHostd(const std::filesystem::path& self_path, const LauncherCommandLine& command_line) const;
  void ServiceCommand(
      const std::string& action,
      const std::string& role,
      const LauncherCommandLine& command_line) const;

 private:
  std::string RenderConfigToml(
      const ControllerInstallOptions* controller,
      const HostdInstallOptions* hostd,
      const std::filesystem::path& controller_private_key,
      const std::filesystem::path& controller_public_key,
      const std::filesystem::path& hostd_private_key,
      const std::filesystem::path& hostd_public_key,
      const std::string& controller_fingerprint) const;
  std::string RenderControllerUnit(
      const ControllerInstallOptions& options,
      const std::filesystem::path& config_path) const;
  std::string RenderHostdUnit(
      const HostdInstallOptions& options,
      const std::filesystem::path& config_path) const;
  std::vector<std::string> ParseRoleTargets(const std::string& role) const;
  void MaybeRunSystemctl(
      const std::vector<std::string>& units,
      const std::vector<std::string>& actions,
      bool skip_systemctl) const;
  void EnsureKeypair(
      const std::filesystem::path& private_key_path,
      const std::filesystem::path& public_key_path) const;
  std::string ComputePublicKeyFingerprint(const std::filesystem::path& public_key_path) const;
  std::string ReadTextFile(const std::filesystem::path& path) const;
  void WriteTextFile(const std::filesystem::path& path, const std::string& content) const;
  std::string Trim(const std::string& value) const;
  std::string ShellEscape(const std::string& value) const;
  std::string DefaultInternalListenHost() const;

  const InstallLayoutResolver& install_layout_resolver_;
  const ProcessRunner& process_runner_;
};

}  // namespace naim::launcher
