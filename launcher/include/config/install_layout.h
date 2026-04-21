#pragma once

#include <filesystem>
#include <optional>

namespace naim::launcher {

namespace fs = std::filesystem;

struct InstallLayout {
  fs::path config_path = "/etc/naim-node/config.toml";
  fs::path state_root = "/var/lib/naim-node";
  fs::path log_root = "/var/log/naim-node";
  fs::path systemd_dir = "/etc/systemd/system";
};

class InstallLayoutResolver {
 public:
  std::optional<fs::path> InstallRootOverride() const;
  InstallLayout DefaultInstallLayout() const;
  fs::path DefaultControllerDbPath() const;
  fs::path DefaultArtifactsRoot() const;
  fs::path DefaultWebUiRoot() const;
  fs::path DefaultRuntimeRoot() const;
  fs::path DefaultHostdStateRoot() const;
  bool IsUserServiceLayout(const InstallLayout& layout) const;
};

}  // namespace naim::launcher
