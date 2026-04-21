#pragma once

#include <optional>
#include <string>
#include <vector>

#include "app/hostd_local_state_path_support.h"
#include "naim/state/models.h"

namespace naim::hostd {

class HostdLocalStateRepository final {
 public:
  explicit HostdLocalStateRepository(const HostdLocalStatePathSupport& path_support);

  std::optional<int> LoadLocalAppliedGeneration(
      const std::string& state_root,
      const std::string& node_name,
      const std::optional<std::string>& plane_name = std::nullopt) const;
  void SaveLocalAppliedGeneration(
      const std::string& state_root,
      const std::string& node_name,
      int generation,
      const std::optional<std::string>& plane_name = std::nullopt) const;
  std::vector<naim::DesiredState> LoadAllLocalAppliedStates(
      const std::string& state_root,
      const std::string& node_name) const;
  std::optional<naim::DesiredState> LoadLocalAppliedState(
      const std::string& state_root,
      const std::string& node_name,
      const std::optional<std::string>& plane_name = std::nullopt) const;
  void RewriteAggregateLocalState(
      const std::string& state_root,
      const std::string& node_name) const;
  void RewriteAggregateLocalGeneration(
      const std::string& state_root,
      const std::string& node_name) const;
  void SaveLocalAppliedState(
      const std::string& state_root,
      const std::string& node_name,
      const naim::DesiredState& state,
      const std::optional<std::string>& plane_name = std::nullopt) const;
  void RemoveLocalAppliedPlaneState(
      const std::string& state_root,
      const std::string& node_name,
      const std::string& plane_name) const;
  void PrintLocalStateSummary(
      const naim::DesiredState& state,
      const std::string& state_path,
      const std::string& node_name,
      const std::optional<int>& generation) const;
  std::string RequireSingleNodeName(const naim::DesiredState& state) const;

 private:
  std::optional<int> LoadGenerationFromPath(const std::string& path) const;
  std::optional<naim::DesiredState> LoadStateFromPath(const std::string& path) const;
  naim::DesiredState MergeLocalAppliedStates(const std::vector<naim::DesiredState>& states) const;
  void RemoveStateFileIfExists(const std::string& path) const;
  void WriteLocalStateFile(
      const naim::DesiredState& state,
      const std::string& path) const;

  const HostdLocalStatePathSupport& path_support_;
};

}  // namespace naim::hostd
