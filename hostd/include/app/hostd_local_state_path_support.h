#pragma once

#include <string>
#include <vector>

namespace naim::hostd {

class HostdLocalStatePathSupport final {
 public:
  std::string LocalPlaneRoot(
      const std::string& state_root,
      const std::string& node_name,
      const std::string& plane_name) const;
  std::string LocalGenerationPath(
      const std::string& state_root,
      const std::string& node_name) const;
  std::string LocalPlaneGenerationPath(
      const std::string& state_root,
      const std::string& node_name,
      const std::string& plane_name) const;
  std::string LocalStatePath(
      const std::string& state_root,
      const std::string& node_name) const;
  std::string LocalPlaneStatePath(
      const std::string& state_root,
      const std::string& node_name,
      const std::string& plane_name) const;
  std::vector<std::string> ListLocalPlaneNames(
      const std::string& state_root,
      const std::string& node_name) const;
};

}  // namespace naim::hostd
