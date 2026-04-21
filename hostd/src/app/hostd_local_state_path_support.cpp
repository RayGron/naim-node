#include "app/hostd_local_state_path_support.h"

#include <algorithm>
#include <filesystem>

namespace naim::hostd {

std::string HostdLocalStatePathSupport::LocalPlaneRoot(
    const std::string& state_root,
    const std::string& node_name,
    const std::string& plane_name) const {
  return (std::filesystem::path(state_root) / node_name / "planes" / plane_name).string();
}

std::string HostdLocalStatePathSupport::LocalGenerationPath(
    const std::string& state_root,
    const std::string& node_name) const {
  return (std::filesystem::path(state_root) / node_name / "applied-generation.txt").string();
}

std::string HostdLocalStatePathSupport::LocalPlaneGenerationPath(
    const std::string& state_root,
    const std::string& node_name,
    const std::string& plane_name) const {
  return (std::filesystem::path(LocalPlaneRoot(state_root, node_name, plane_name)) /
          "applied-generation.txt")
      .string();
}

std::string HostdLocalStatePathSupport::LocalStatePath(
    const std::string& state_root,
    const std::string& node_name) const {
  return (std::filesystem::path(state_root) / node_name / "applied-state.json").string();
}

std::string HostdLocalStatePathSupport::LocalPlaneStatePath(
    const std::string& state_root,
    const std::string& node_name,
    const std::string& plane_name) const {
  return (std::filesystem::path(LocalPlaneRoot(state_root, node_name, plane_name)) /
          "applied-state.json")
      .string();
}

std::vector<std::string> HostdLocalStatePathSupport::ListLocalPlaneNames(
    const std::string& state_root,
    const std::string& node_name) const {
  std::vector<std::string> result;
  const std::filesystem::path planes_root =
      std::filesystem::path(state_root) / node_name / "planes";
  if (!std::filesystem::exists(planes_root)) {
    return result;
  }
  for (const auto& entry : std::filesystem::directory_iterator(planes_root)) {
    if (entry.is_directory()) {
      result.push_back(entry.path().filename().string());
    }
  }
  std::sort(result.begin(), result.end());
  return result;
}

}  // namespace naim::hostd
