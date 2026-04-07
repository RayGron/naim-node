#include "app/hostd_repo_root_support.h"

#include <filesystem>
#include <string>
#include <vector>

#include "comet/core/platform_compat.h"

namespace comet::hostd::appsupport {
namespace {

bool LooksLikeCometRepoRoot(const std::filesystem::path& path) {
  std::error_code error;
  return std::filesystem::exists(path / "scripts" / "build-runtime-images.sh", error) && !error &&
         std::filesystem::exists(path / "runtime" / "base" / "Dockerfile", error) && !error;
}

std::optional<std::filesystem::path> FindRepoRootFromPath(std::filesystem::path current) {
  while (!current.empty()) {
    if (LooksLikeCometRepoRoot(current)) {
      return current;
    }
    const auto parent = current.parent_path();
    if (parent.empty() || parent == current) {
      break;
    }
    current = parent;
  }
  return std::nullopt;
}

std::optional<std::filesystem::path> FindRepoRootInSiblingRepos(std::filesystem::path current) {
  while (!current.empty()) {
    const auto candidate = current / "repos" / "comet-node";
    if (LooksLikeCometRepoRoot(candidate)) {
      return candidate;
    }
    const auto parent = current.parent_path();
    if (parent.empty() || parent == current) {
      break;
    }
    current = parent;
  }
  return std::nullopt;
}

std::optional<std::filesystem::path> DetectCometRepoRootNear(
    const std::filesystem::path& start) {
  if (const auto from_path = FindRepoRootFromPath(start); from_path.has_value()) {
    return from_path;
  }
  return FindRepoRootInSiblingRepos(start);
}

std::string StripBundlePrefixIfPresent(const std::string& value) {
  constexpr std::string_view kBundlePrefix = "bundle://";
  if (value.rfind(kBundlePrefix.data(), 0) == 0) {
    return value.substr(kBundlePrefix.size());
  }
  return value;
}

}  // namespace

std::optional<std::filesystem::path> DetectCometRepoRoot() {
  try {
    if (const auto from_cwd = DetectCometRepoRootNear(std::filesystem::current_path());
        from_cwd.has_value()) {
      return from_cwd;
    }
  } catch (...) {
  }

  const std::string executable_path = comet::platform::ExecutablePath();
  if (executable_path.empty()) {
    return std::nullopt;
  }
  return DetectCometRepoRootNear(std::filesystem::path(executable_path).parent_path());
}

std::optional<std::filesystem::path> ResolvePlaneOwnedPath(
    const comet::DesiredState& state,
    const std::string& relative_path,
    const std::string& artifacts_root) {
  const std::string normalized_path = StripBundlePrefixIfPresent(relative_path);
  if (normalized_path.empty()) {
    return std::nullopt;
  }

  const std::filesystem::path input(normalized_path);
  if (input.is_absolute()) {
    std::error_code error;
    if (std::filesystem::exists(input, error) && !error) {
      return input.lexically_normal();
    }
    return std::nullopt;
  }

  std::vector<std::filesystem::path> candidates;
  try {
    candidates.push_back(std::filesystem::current_path() / input);
  } catch (...) {
  }

  if (!artifacts_root.empty()) {
    candidates.push_back(std::filesystem::path(artifacts_root) / state.plane_name / input);
  }

  if (const auto comet_repo_root = DetectCometRepoRoot(); comet_repo_root.has_value()) {
    candidates.push_back(*comet_repo_root / input);
    candidates.push_back(comet_repo_root->parent_path() / state.plane_name / input);
  }

  for (const auto& candidate : candidates) {
    std::error_code error;
    if (std::filesystem::exists(candidate, error) && !error) {
      return candidate.lexically_normal();
    }
  }
  return std::nullopt;
}

}  // namespace comet::hostd::appsupport
