#pragma once

#include <functional>
#include <optional>
#include <string>

#include "app/hostd_command_support.h"
#include "app/hostd_compose_runtime_support.h"
#include "app/hostd_disk_runtime_support.h"
#include "app/hostd_file_support.h"
#include "backend/hostd_backend.h"
#include "cli/hostd_command_line.h"
#include "naim/planning/execution_plan.h"
#include "naim/state/models.h"

namespace naim::hostd {

class HostdDesiredStateApplyPlanSupport final {
 public:
  using ProgressPublisher = std::function<void(
      const std::string& phase,
      const std::string& title,
      const std::string& detail,
      int percent,
      const std::string& plane_name,
      const std::string& node_name)>;

  HostdDesiredStateApplyPlanSupport(
      const HostdCommandSupport& command_support,
      const HostdComposeRuntimeSupport& compose_runtime_support,
      const HostdDiskRuntimeSupport& disk_runtime_support,
      const HostdFileSupport& file_support);

  void ApplyNodePlan(
      const naim::NodeExecutionPlan& plan,
      const naim::DesiredState& desired_node_state,
      const naim::NodeComposePlan& compose_plan,
      const std::string& storage_root,
      const std::optional<std::string>& runtime_root,
      ComposeMode compose_mode,
      HostdBackend* backend,
      const ProgressPublisher& publish_progress) const;

  std::size_t ExpectedRuntimeStatusCountForComposePlan(
      const naim::NodeComposePlan& compose_plan) const;

  static bool IsDesiredNodeStateEmpty(const naim::DesiredState& state);

 private:
  void PrintOperationApplied(
      const naim::HostOperation& operation,
      const std::string& status) const;
  bool IsUnderRoot(
      const std::filesystem::path& path,
      const std::optional<std::string>& runtime_root) const;
  void RemoveDiskDirectory(
      const std::string& path,
      const std::optional<std::string>& runtime_root) const;

  const HostdCommandSupport& command_support_;
  const HostdComposeRuntimeSupport& compose_runtime_support_;
  const HostdDiskRuntimeSupport& disk_runtime_support_;
  const HostdFileSupport& file_support_;
};

}  // namespace naim::hostd
