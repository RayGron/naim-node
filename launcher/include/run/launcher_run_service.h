#pragma once

#include <filesystem>
#include <optional>

#include "config/generated_config_loader.h"
#include "config/install_layout.h"
#include "config/launcher_options.h"
#include "install/launcher_install_service.h"
#include "platform/process_runner.h"
#include "platform/signal_manager.h"

namespace naim::launcher {

class LauncherRunService {
 public:
  LauncherRunService(
      const InstallLayoutResolver& install_layout_resolver,
      const GeneratedConfigLoader& config_loader,
      const ProcessRunner& process_runner,
      const LauncherInstallService& install_service);

  int RunController(
      SignalManager& signal_manager,
      const std::filesystem::path& self_path,
      const std::filesystem::path& controller_binary,
      const LauncherCommandLine& command_line) const;
  int RunHostd(
      SignalManager& signal_manager,
      const std::filesystem::path& hostd_binary,
      const std::filesystem::path& self_path,
      const LauncherCommandLine& command_line) const;

 private:
  int RunHostdLoop(
      SignalManager& signal_manager,
      const std::filesystem::path& hostd_binary,
      const HostdRunOptions& options) const;
  void PrepareControllerRuntime(
      const std::filesystem::path& owner_probe_path,
      const ControllerRunOptions& options) const;
  int RunControllerSupervisor(
      SignalManager& signal_manager,
      const std::filesystem::path& self_path,
      const std::filesystem::path& controller_binary,
      const ControllerRunOptions& options) const;
  void PrepareSharedStateAccess(
      const std::filesystem::path& owner_probe_path,
      const std::filesystem::path& db_path) const;
  std::optional<unsigned int> ResolveSharedStateGroupId(
      const std::filesystem::path& owner_probe_path) const;
  void EnsureSharedDirectoryAccess(
      const std::filesystem::path& path,
      unsigned int group_id) const;
  void EnsureSharedFileAccess(
      const std::filesystem::path& path,
      unsigned int group_id) const;
  std::string DefaultNodeName() const;
  std::string DefaultInternalListenHost() const;
  std::string DefaultWebUiControllerUpstream(
      const std::string& internal_listen_host,
      int listen_port) const;
  std::string Trim(const std::string& value) const;
  std::string ReadTextFile(const std::filesystem::path& path) const;
  std::string ComputePublicKeyFingerprint(const std::filesystem::path& public_key_path) const;

  const InstallLayoutResolver& install_layout_resolver_;
  const GeneratedConfigLoader& config_loader_;
  const ProcessRunner& process_runner_;
  const LauncherInstallService& install_service_;
};

}  // namespace naim::launcher
