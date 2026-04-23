#pragma once

#include <filesystem>

namespace naim::hostd {

class HostdInstallLayoutSupport final {
 public:
  std::filesystem::path ResolveHostdRootFromPrivateKeyPath(
      const std::filesystem::path& host_private_key_path) const;
};

}  // namespace naim::hostd
