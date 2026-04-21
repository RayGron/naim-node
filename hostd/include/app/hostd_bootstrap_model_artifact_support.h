#pragma once

#include <optional>
#include <string>
#include <vector>

#include "app/hostd_desired_state_path_support.h"
#include "naim/state/models.h"

namespace naim::hostd {

struct HostdBootstrapModelArtifact {
  std::optional<std::string> local_path;
  std::optional<std::string> source_url;
  std::string target_host_path;
};

class HostdBootstrapModelArtifactSupport final {
 public:
  explicit HostdBootstrapModelArtifactSupport(
      const HostdDesiredStatePathSupport& path_support);

  const naim::DiskSpec& RequirePlaneSharedDiskForNode(
      const naim::DesiredState& state,
      const std::string& node_name) const;
  std::vector<HostdBootstrapModelArtifact> BuildArtifacts(
      const naim::DesiredState& state,
      const std::string& node_name) const;
  std::string TargetPath(
      const naim::DesiredState& state,
      const std::string& node_name) const;
  std::string SharedModelBootstrapOwnerNode(const naim::DesiredState& state) const;
  static bool LooksLikeRecognizedModelDirectory(const std::string& path);

 private:
  static std::string FilenameFromUrl(const std::string& source_url);

  const HostdDesiredStatePathSupport& path_support_;
};

}  // namespace naim::hostd
