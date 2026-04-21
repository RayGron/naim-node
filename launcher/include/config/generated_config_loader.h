#pragma once

#include <filesystem>
#include <optional>
#include <string>

#include "config/install_layout.h"

namespace naim::launcher {

namespace fs = std::filesystem;

struct GeneratedControllerConfig {
  std::optional<std::string> listen_host;
  std::optional<int> listen_port;
  std::optional<std::string> internal_listen_host;
  std::optional<fs::path> db_path;
  std::optional<fs::path> artifacts_root;
  std::optional<bool> web_ui_enabled;
  std::optional<bool> local_hostd_enabled;
  std::optional<fs::path> controller_private_key;
  std::optional<fs::path> controller_public_key;
};

struct GeneratedHostdConfig {
  std::optional<std::string> node_name;
  std::optional<std::string> controller_url;
  std::optional<std::string> onboarding_key;
  std::optional<std::string> transport_mode;
  std::optional<std::string> execution_mode;
  std::optional<std::string> listen_address;
  std::optional<fs::path> runtime_root;
  std::optional<fs::path> state_root;
  std::optional<std::string> compose_mode;
  std::optional<fs::path> host_private_key;
  std::optional<fs::path> host_public_key;
  std::optional<std::string> trusted_controller_fingerprint;
  std::optional<int> inventory_scan_interval_sec;
};

struct GeneratedConfig {
  GeneratedControllerConfig controller;
  GeneratedHostdConfig hostd;
};

class GeneratedConfigLoader {
 public:
  explicit GeneratedConfigLoader(const InstallLayoutResolver& install_layout_resolver);

  std::optional<fs::path> ResolveConfigPathFromEnvOrDefault() const;
  GeneratedConfig Load(const fs::path& path) const;

 private:
  const InstallLayoutResolver& install_layout_resolver_;
};

}  // namespace naim::launcher
