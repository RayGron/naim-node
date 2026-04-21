#include "app/hostd_bootstrap_model_artifact_support.h"

#include <filesystem>
#include <stdexcept>

namespace naim::hostd {

namespace fs = std::filesystem;

HostdBootstrapModelArtifactSupport::HostdBootstrapModelArtifactSupport(
    const HostdDesiredStatePathSupport& path_support)
    : path_support_(path_support) {}

const naim::DiskSpec& HostdBootstrapModelArtifactSupport::RequirePlaneSharedDiskForNode(
    const naim::DesiredState& state,
    const std::string& node_name) const {
  for (const auto& disk : state.disks) {
    if (disk.node_name == node_name &&
        disk.kind == naim::DiskKind::PlaneShared) {
      return disk;
    }
  }
  throw std::runtime_error(
      "plane '" + state.plane_name + "' is missing a plane-shared disk for node '" + node_name +
      "'");
}

std::vector<HostdBootstrapModelArtifact> HostdBootstrapModelArtifactSupport::BuildArtifacts(
    const naim::DesiredState& state,
    const std::string& node_name) const {
  const auto& shared_disk = RequirePlaneSharedDiskForNode(state, node_name);
  const fs::path target_root = path_support_.SharedDiskHostPathForContainerPath(
      shared_disk,
      state.inference.gguf_cache_dir,
      "models/gguf");

  std::vector<HostdBootstrapModelArtifact> artifacts;
  std::string filename = "model.gguf";
  if (!state.bootstrap_model.has_value()) {
    artifacts.push_back(HostdBootstrapModelArtifact{
        std::nullopt,
        std::nullopt,
        (target_root / filename).string(),
    });
    return artifacts;
  }

  const auto& bootstrap_model = *state.bootstrap_model;
  if (!bootstrap_model.source_urls.empty()) {
    artifacts.reserve(bootstrap_model.source_urls.size());
    for (const auto& source_url : bootstrap_model.source_urls) {
      artifacts.push_back(HostdBootstrapModelArtifact{
          std::nullopt,
          source_url,
          (target_root / FilenameFromUrl(source_url)).string(),
      });
    }
    return artifacts;
  }

  if (bootstrap_model.target_filename.has_value() && !bootstrap_model.target_filename->empty()) {
    filename = *bootstrap_model.target_filename;
  } else if (bootstrap_model.local_path.has_value() && !bootstrap_model.local_path->empty()) {
    filename = fs::path(*bootstrap_model.local_path).filename().string();
  } else if (bootstrap_model.source_url.has_value() && !bootstrap_model.source_url->empty()) {
    filename = FilenameFromUrl(*bootstrap_model.source_url);
  }
  if (filename.empty()) {
    filename = "model.gguf";
  }
  artifacts.push_back(HostdBootstrapModelArtifact{
      bootstrap_model.local_path,
      bootstrap_model.source_url,
      (target_root / filename).string(),
  });
  return artifacts;
}

std::string HostdBootstrapModelArtifactSupport::TargetPath(
    const naim::DesiredState& state,
    const std::string& node_name) const {
  const auto artifacts = BuildArtifacts(state, node_name);
  if (artifacts.empty()) {
    throw std::runtime_error(
        "failed to resolve bootstrap model target path for plane '" + state.plane_name + "'");
  }
  return artifacts.front().target_host_path;
}

std::string HostdBootstrapModelArtifactSupport::SharedModelBootstrapOwnerNode(
    const naim::DesiredState& state) const {
  if (!state.inference.primary_infer_node.empty()) {
    return state.inference.primary_infer_node;
  }
  return state.nodes.empty() ? std::string{} : state.nodes.front().name;
}

bool HostdBootstrapModelArtifactSupport::LooksLikeRecognizedModelDirectory(
    const std::string& path) {
  std::error_code error;
  const fs::path root(path);
  if (!fs::exists(root, error) || error || !fs::is_directory(root, error) || error) {
    return false;
  }
  return fs::exists(root / "config.json", error) || fs::exists(root / "params.json", error);
}

std::string HostdBootstrapModelArtifactSupport::FilenameFromUrl(
    const std::string& source_url) {
  const auto query = source_url.find_first_of("?#");
  const std::string without_query =
      query == std::string::npos ? source_url : source_url.substr(0, query);
  const std::string filename = fs::path(without_query).filename().string();
  if (filename.empty()) {
    throw std::runtime_error("failed to infer filename from bootstrap model URL: " + source_url);
  }
  return filename;
}

}  // namespace naim::hostd
