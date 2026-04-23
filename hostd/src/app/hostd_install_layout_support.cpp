#include "app/hostd_install_layout_support.h"

namespace naim::hostd {

std::filesystem::path HostdInstallLayoutSupport::ResolveHostdRootFromPrivateKeyPath(
    const std::filesystem::path& host_private_key_path) const {
  const std::filesystem::path key_dir = host_private_key_path.parent_path();
  if (key_dir.filename() == "keys" && key_dir.parent_path().filename() == "install-state") {
    return key_dir.parent_path().parent_path();
  }
  return key_dir.parent_path();
}

}  // namespace naim::hostd
