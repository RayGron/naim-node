#pragma once

#include <filesystem>
#include <optional>
#include <string>

#include "naim/state/models.h"

namespace naim::hostd {

class HostdRepoRootSupport final {
 public:
  std::optional<std::filesystem::path> DetectNaimRepoRoot() const;
  std::optional<std::filesystem::path> ResolvePlaneOwnedPath(
      const naim::DesiredState& state,
      const std::string& relative_path,
      const std::string& artifacts_root) const;

 private:
  static std::optional<std::filesystem::path> DetectNaimRepoRootNear(
      const std::filesystem::path& start);
  static std::optional<std::filesystem::path> FindRepoRootFromPath(
      std::filesystem::path current);
  static std::optional<std::filesystem::path> FindRepoRootInSiblingRepos(
      std::filesystem::path current);
  static bool LooksLikeNaimRepoRoot(const std::filesystem::path& path);
  static std::string StripBundlePrefixIfPresent(const std::string& value);
};

}  // namespace naim::hostd
