#include "platform/launcher_path_resolver.h"

#include <stdexcept>

#include "naim/core/platform_compat.h"

namespace naim::launcher {

namespace fs = std::filesystem;

fs::path LauncherPathResolver::ResolveSelfPath(const char* argv0) const {
  const std::string executable_path = naim::platform::ExecutablePath();
  if (!executable_path.empty()) {
    return fs::path(executable_path);
  }
  return fs::weakly_canonical(fs::path(argv0));
}

fs::path LauncherPathResolver::ResolveSiblingBinary(
    const fs::path& self_path,
    const std::string& binary_name) const {
  const fs::path sibling = self_path.parent_path() / binary_name;
  if (!fs::exists(sibling)) {
    throw std::runtime_error("required binary not found: " + sibling.string());
  }
  return sibling;
}

}  // namespace naim::launcher
