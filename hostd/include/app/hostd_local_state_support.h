#pragma once

#include <chrono>
#include <functional>
#include <optional>
#include <string>
#include <vector>

#include "comet/runtime/runtime_status.h"
#include "comet/state/models.h"

namespace comet::hostd::local_state_support {

using RuntimeStatusPathResolver =
    std::function<std::optional<std::string>(const comet::DesiredState&, const std::string&)>;
using InstanceRuntimeStatusLoader = std::function<std::vector<comet::RuntimeProcessStatus>(
    const std::string&,
    const std::string&,
    const std::optional<std::string>&)>;

std::string LocalPlaneRoot(
    const std::string& state_root,
    const std::string& node_name,
    const std::string& plane_name);
std::string LocalGenerationPath(const std::string& state_root, const std::string& node_name);
std::string LocalPlaneGenerationPath(
    const std::string& state_root,
    const std::string& node_name,
    const std::string& plane_name);
std::string LocalStatePath(const std::string& state_root, const std::string& node_name);
std::string LocalPlaneStatePath(
    const std::string& state_root,
    const std::string& node_name,
    const std::string& plane_name);
std::vector<std::string> ListLocalPlaneNames(
    const std::string& state_root,
    const std::string& node_name);
std::optional<int> LoadGenerationFromPath(const std::string& path);
std::optional<int> LoadLocalAppliedGeneration(
    const std::string& state_root,
    const std::string& node_name,
    const std::optional<std::string>& plane_name = std::nullopt);
void SaveLocalAppliedGeneration(
    const std::string& state_root,
    const std::string& node_name,
    int generation,
    const std::optional<std::string>& plane_name = std::nullopt);
std::optional<comet::DesiredState> LoadStateFromPath(const std::string& path);
comet::DesiredState MergeLocalAppliedStates(const std::vector<comet::DesiredState>& states);
std::vector<comet::DesiredState> LoadAllLocalAppliedStates(
    const std::string& state_root,
    const std::string& node_name);
std::optional<comet::DesiredState> LoadLocalAppliedState(
    const std::string& state_root,
    const std::string& node_name,
    const std::optional<std::string>& plane_name = std::nullopt);
void RewriteAggregateLocalState(const std::string& state_root, const std::string& node_name);
void RewriteAggregateLocalGeneration(
    const std::string& state_root,
    const std::string& node_name);
std::optional<comet::RuntimeStatus> LoadLocalRuntimeStatus(
    const std::string& state_root,
    const std::string& node_name,
    const RuntimeStatusPathResolver& runtime_status_path_resolver,
    const std::optional<std::string>& plane_name = std::nullopt);
void SaveLocalAppliedState(
    const std::string& state_root,
    const std::string& node_name,
    const comet::DesiredState& state,
    const std::optional<std::string>& plane_name = std::nullopt);
void RemoveLocalAppliedPlaneState(
    const std::string& state_root,
    const std::string& node_name,
    const std::string& plane_name);
void WaitForLocalRuntimeStatus(
    const std::string& state_root,
    const std::string& node_name,
    const RuntimeStatusPathResolver& runtime_status_path_resolver,
    const std::optional<std::string>& plane_name,
    std::chrono::seconds timeout);
std::size_t ExpectedRuntimeStatusCountForNode(
    const comet::DesiredState& desired_node_state,
    const std::string& node_name);
void WaitForLocalInstanceRuntimeStatuses(
    const std::string& state_root,
    const std::string& node_name,
    const InstanceRuntimeStatusLoader& status_loader,
    const std::optional<std::string>& plane_name,
    std::size_t expected_count,
    std::chrono::seconds timeout);
void PrintLocalStateSummary(
    const comet::DesiredState& state,
    const std::string& state_path,
    const std::string& node_name,
    const std::optional<int>& generation);
std::string RequireSingleNodeName(const comet::DesiredState& state);

}  // namespace comet::hostd::local_state_support
