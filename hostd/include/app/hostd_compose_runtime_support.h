#pragma once

#include <filesystem>
#include <set>
#include <string>

#include "app/hostd_command_support.h"
#include "app/hostd_repo_root_support.h"
#include "cli/hostd_command_line.h"
#include "naim/planning/execution_plan.h"

namespace naim::hostd {

class HostdComposeRuntimeSupport final {
 public:
  explicit HostdComposeRuntimeSupport(const HostdCommandSupport& command_support);

  std::string PlaneMeshNetworkName(const std::string& plane_name) const;
  void EnsureComposeMeshNetworkAvailable(
      const naim::NodeComposePlan& compose_plan,
      ComposeMode compose_mode) const;
  void RemoveComposeMeshNetworkIfUnused(
      const std::string& plane_name,
      ComposeMode compose_mode) const;
  void RunComposeCommand(
      const std::string& compose_file_path,
      const std::string& subcommand,
      ComposeMode compose_mode) const;
  void EnsureComposeImagesAvailable(
      const naim::NodeComposePlan& compose_plan,
      ComposeMode compose_mode) const;

 private:
  bool ComposeProjectHasContainers(const std::string& compose_file_path) const;
  bool DockerImageExists(const std::string& image) const;
  bool LocalRuntimeBinaryExists(
      const std::filesystem::path& repo_root,
      const std::string& image) const;
  void EnsureLocalRuntimeBinary(
      const std::filesystem::path& repo_root,
      const std::string& image) const;
  void BuildNaimRuntimeImage(
      const std::filesystem::path& repo_root,
      const std::string& image) const;
  void EnsureRuntimeImageAvailable(const std::string& image) const;

  const HostdCommandSupport& command_support_;
  HostdRepoRootSupport repo_root_support_;
};

}  // namespace naim::hostd
