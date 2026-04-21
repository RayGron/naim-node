#pragma once

#include <optional>
#include <string>

namespace naim::hostd {

struct NaimNodeConfig {
  std::string storage_root = "/var/lib/naim";
};

class NodeConfigLoader {
 public:
  NaimNodeConfig Load(
      const std::optional<std::string>& config_arg,
      const char* argv0) const;

 private:
  std::optional<std::string> FindNodeConfigPath(const char* argv0) const;
};

}  // namespace naim::hostd
