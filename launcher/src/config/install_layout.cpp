#include "config/install_layout.h"

#include <cstdlib>

#include "naim/core/platform_compat.h"

namespace naim::launcher {

std::optional<fs::path> InstallLayoutResolver::InstallRootOverride() const {
  const char* value = std::getenv("NAIM_INSTALL_ROOT");
  if (value == nullptr || *value == '\0') {
    return std::nullopt;
  }
  return fs::path(value);
}

InstallLayout InstallLayoutResolver::DefaultInstallLayout() const {
  if (const auto root = InstallRootOverride(); root.has_value()) {
    return InstallLayout{
        *root / "etc/naim-node/config.toml",
        *root / "var/lib/naim-node",
        *root / "var/log/naim-node",
        *root / "etc/systemd/system",
    };
  }
  if (!naim::platform::HasElevatedPrivileges()) {
    const fs::path home = std::getenv("HOME") != nullptr ? fs::path(std::getenv("HOME"))
                                                         : fs::current_path();
    return InstallLayout{
        home / ".config/naim-node/config.toml",
        home / ".local/share/naim-node",
        home / ".local/state/naim-node",
        home / ".config/systemd/user",
    };
  }
  return InstallLayout{};
}

fs::path InstallLayoutResolver::DefaultControllerDbPath() const {
  return DefaultInstallLayout().state_root / "controller.sqlite";
}

fs::path InstallLayoutResolver::DefaultArtifactsRoot() const {
  return DefaultInstallLayout().state_root / "artifacts";
}

fs::path InstallLayoutResolver::DefaultWebUiRoot() const {
  return DefaultInstallLayout().state_root / "web-ui";
}

fs::path InstallLayoutResolver::DefaultRuntimeRoot() const {
  return DefaultInstallLayout().state_root / "runtime";
}

fs::path InstallLayoutResolver::DefaultHostdStateRoot() const {
  return DefaultInstallLayout().state_root / "hostd-state";
}

bool InstallLayoutResolver::IsUserServiceLayout(const InstallLayout& layout) const {
  const std::string rendered = layout.systemd_dir.string();
  return rendered.find(".config/systemd/user") != std::string::npos;
}

}  // namespace naim::launcher
