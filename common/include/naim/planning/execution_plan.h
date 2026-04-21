#pragma once

#include <optional>
#include <string>
#include <vector>

#include "naim/state/models.h"

namespace naim {

enum class HostOperationKind {
  EnsureDisk,
  RemoveDisk,
  EnsureService,
  RemoveService,
  WriteInferRuntimeConfig,
  RemoveInferRuntimeConfig,
  WriteComposeFile,
  RemoveComposeFile,
  ComposeUp,
  ComposeDown,
};

struct HostOperation {
  HostOperationKind kind;
  std::string target;
  std::string details;
};

struct NodeExecutionPlan {
  std::string plane_name;
  std::string node_name;
  std::string compose_file_path;
  std::vector<HostOperation> operations;
};

std::vector<NodeExecutionPlan> BuildNodeExecutionPlans(
    const std::optional<DesiredState>& current_state,
    const DesiredState& desired_state,
    const std::string& artifacts_root);

std::string RenderNodeExecutionPlans(const std::vector<NodeExecutionPlan>& plans);
std::string ToString(HostOperationKind kind);

}  // namespace naim
