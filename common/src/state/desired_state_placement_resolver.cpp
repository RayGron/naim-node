#include "naim/state/desired_state_placement_resolver.h"

#include <string_view>

namespace naim {

DesiredStatePlacementResolver::DesiredStatePlacementResolver(const DesiredState& state)
    : state_(state) {}

bool DesiredStatePlacementResolver::HasExecutionNode() const {
  return ExecutionNodeName().has_value();
}

std::optional<std::string> DesiredStatePlacementResolver::ExecutionNodeName() const {
  return ResolvePlacementTargetAlias();
}

std::string DesiredStatePlacementResolver::DefaultNodeName() const {
  if (const auto execution_node = ExecutionNodeName(); execution_node.has_value()) {
    return *execution_node;
  }
  if (!state_.nodes.empty()) {
    return state_.nodes.front().name;
  }
  return "local-hostd";
}

bool DesiredStatePlacementResolver::ShouldEmitTopology() const {
  return !HasExecutionNode() && !IsDefaultSingleNodeTopology();
}

bool DesiredStatePlacementResolver::IsDefaultSingleNodeTopology() const {
  if (state_.nodes.size() != 1) {
    return false;
  }
  const auto& node = state_.nodes.front();
  return node.name == "local-hostd" && node.platform == "linux" &&
         node.execution_mode == HostExecutionMode::Mixed &&
         node.gpu_memory_mb.empty();
}

std::optional<std::string> DesiredStatePlacementResolver::ResolvePlacementTargetAlias() const {
  if (!state_.placement_target.has_value() || state_.placement_target->empty()) {
    return std::nullopt;
  }
  constexpr std::string_view kNodePrefix = "node:";
  if (state_.placement_target->rfind(kNodePrefix.data(), 0) == 0) {
    const std::string node_name = state_.placement_target->substr(kNodePrefix.size());
    if (!node_name.empty()) {
      return node_name;
    }
  }
  if (*state_.placement_target == "local") {
    return std::string("local-hostd");
  }
  return std::nullopt;
}

}  // namespace naim
