#pragma once

#include <ctime>
#include <optional>
#include <string>
#include <vector>

#include "app/hostd_desired_state_path_support.h"
#include "app/hostd_local_runtime_state_support.h"
#include "app/hostd_local_state_path_support.h"
#include "app/hostd_local_state_repository.h"
#include "naim/planning/execution_plan.h"
#include "naim/state/models.h"

namespace naim::hostd {

class HostdDesiredStateDisplaySupport final {
 public:
  explicit HostdDesiredStateDisplaySupport(const HostdDesiredStatePathSupport& path_support);

  void ShowDemoOps(
      const std::string& node_name,
      const std::string& storage_root,
      const std::optional<std::string>& runtime_root) const;
  void ShowStateOps(
      const std::string& db_path,
      const std::string& node_name,
      const std::string& artifacts_root,
      const std::string& storage_root,
      const std::optional<std::string>& runtime_root,
      const std::string& state_root) const;
  void ShowLocalState(const std::string& node_name, const std::string& state_root) const;
  void ShowRuntimeStatus(const std::string& node_name, const std::string& state_root) const;
  static std::string RuntimeConfigSummary(const naim::DesiredState& state);
  static naim::NodeExecutionPlan ResolveNodeExecutionPlan(
      const std::vector<naim::NodeExecutionPlan>& plans,
      const std::optional<naim::DesiredState>& current_state,
      const naim::DesiredState& desired_state,
      const std::string& node_name,
      const std::string& artifacts_root);

 private:
  static std::string DefaultArtifactsRoot();
  static naim::NodeExecutionPlan FindNodeExecutionPlan(
      const std::vector<naim::NodeExecutionPlan>& plans,
      const std::string& node_name);
  static bool StateHasNode(const naim::DesiredState& state, const std::string& node_name);
  static std::string ComposePathForNode(
      const std::string& artifacts_root,
      const std::string& plane_name,
      const std::string& node_name);
  static std::optional<std::tm> ParseDisplayTimestamp(const std::string& value);
  static std::string FormatDisplayTimestamp(const std::string& value);
  void ShowDesiredNodeOps(
      const naim::DesiredState& desired_node_state,
      const std::string& artifacts_root,
      const std::optional<std::string>& runtime_root,
      const std::string& state_root,
      const std::string& source_label,
      const std::optional<int>& desired_generation) const;

  const HostdDesiredStatePathSupport& path_support_;
  HostdRuntimeTelemetrySupport runtime_telemetry_support_;
  HostdLocalStatePathSupport local_state_path_support_;
  HostdLocalStateRepository local_state_repository_;
  HostdLocalRuntimeStateSupport local_runtime_state_support_;
};

}  // namespace naim::hostd
