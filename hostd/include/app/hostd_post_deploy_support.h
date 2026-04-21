#pragma once

#include <optional>
#include <string>

#include <nlohmann/json.hpp>

#include "app/hostd_command_support.h"
#include "app/hostd_local_state_path_support.h"
#include "app/hostd_repo_root_support.h"
#include "backend/hostd_backend.h"
#include "naim/state/models.h"

namespace naim::hostd {

class HostdPostDeploySupport final {
 public:
  explicit HostdPostDeploySupport(const HostdCommandSupport& command_support);

  bool ShouldRunForNode(
      const naim::DesiredState& desired_node_state,
      const std::string& node_name) const;

  void RunIfNeeded(
      const naim::DesiredState& desired_node_state,
      const std::string& node_name,
      const std::string& artifacts_root,
      const std::string& storage_root,
      const std::optional<std::string>& runtime_root,
      const std::string& state_root,
      const std::optional<int>& desired_generation,
      const std::optional<int>& assignment_id,
      HostdBackend* backend) const;

 private:
  bool NodeHasAppInstance(
      const naim::DesiredState& desired_node_state,
      const std::string& node_name) const;
  std::string TailTextFile(const std::string& path, std::size_t max_bytes = 4096) const;
  nlohmann::json BuildProgressPayload(
      const std::string& phase,
      const std::string& title,
      const std::string& detail,
      int percent,
      const std::string& plane_name,
      const std::string& node_name) const;
  void PublishProgress(
      HostdBackend* backend,
      const std::optional<int>& assignment_id,
      const nlohmann::json& progress) const;

  const HostdCommandSupport& command_support_;
  HostdLocalStatePathSupport local_state_path_support_;
  HostdRepoRootSupport repo_root_support_;
};

}  // namespace naim::hostd
