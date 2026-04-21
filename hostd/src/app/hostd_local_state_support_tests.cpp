#include <algorithm>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <stdexcept>
#include <string>
#include <vector>

#include <nlohmann/json.hpp>

#include "app/hostd_desired_state_path_support.h"
#include "app/hostd_local_runtime_state_support.h"
#include "app/hostd_local_state_path_support.h"
#include "app/hostd_local_state_repository.h"
#include "app/hostd_runtime_telemetry_support.h"
#include "naim/state/desired_state_v2_renderer.h"

namespace {

using nlohmann::json;

void Expect(bool condition, const std::string& message) {
  if (!condition) {
    throw std::runtime_error(message);
  }
}

naim::DesiredState BuildState(
    const std::string& plane_name,
    const std::string& node_name) {
  return naim::DesiredStateV2Renderer::Render(json{
      {"version", 2},
      {"plane_name", plane_name},
      {"plane_mode", "llm"},
      {"model",
       {
           {"source", {{"type", "local"}, {"path", "/models/qwen"}}},
           {"materialization", {{"mode", "reference"}, {"local_path", "/models/qwen"}}},
           {"served_model_name", "qwen-test"},
       }},
      {"runtime",
       {{"engine", "llama.cpp"}, {"distributed_backend", "llama_rpc"}, {"workers", 1}}},
      {"topology",
       {{"nodes",
         json::array(
             {{{"name", node_name},
               {"execution_mode", "mixed"},
               {"gpu_memory_mb", {{"0", 24576}}}}})}}},
      {"infer", {{"node", node_name}, {"replicas", 1}}},
      {"worker", {{"node", node_name}, {"gpu_device", "0"}}},
      {"skills", {{"enabled", true}, {"node", node_name}}},
      {"app", {{"enabled", false}}},
  });
}

naim::DesiredState BuildPartialLlmStateWithoutInfer(
    const std::string& plane_name,
    const std::string& node_name) {
  auto state = BuildState(plane_name, node_name);
  state.instances.erase(
      std::remove_if(
          state.instances.begin(),
          state.instances.end(),
          [](const naim::InstanceSpec& instance) {
            return instance.role == naim::InstanceRole::Infer;
          }),
      state.instances.end());
  state.disks.erase(
      std::remove_if(
          state.disks.begin(),
          state.disks.end(),
          [](const naim::DiskSpec& disk) { return disk.kind == naim::DiskKind::InferPrivate; }),
      state.disks.end());
  state.worker_group.infer_instance_name.clear();
  for (auto& member : state.worker_group.members) {
    member.infer_instance_name.clear();
  }
  return state;
}

}  // namespace

int main() {
  try {
    namespace fs = std::filesystem;
    const fs::path temp_root =
        fs::temp_directory_path() / "naim-hostd-local-state-tests";
    std::error_code cleanup_error;
    fs::remove_all(temp_root, cleanup_error);
    fs::create_directories(temp_root);

    const std::string state_root = temp_root.string();
    const std::string node_name = "local-hostd";
    const std::string plane_a = "plane-a";
    const std::string plane_b = "plane-b";
    const naim::hostd::HostdDesiredStatePathSupport desired_state_path_support;
    const naim::hostd::HostdRuntimeTelemetrySupport runtime_telemetry_support;
    const naim::hostd::HostdLocalStatePathSupport local_state_path_support;
    const naim::hostd::HostdLocalStateRepository local_state_repository(local_state_path_support);
    const naim::hostd::HostdLocalRuntimeStateSupport local_runtime_state_support(
        desired_state_path_support,
        local_state_repository,
        runtime_telemetry_support);

    local_state_repository.SaveLocalAppliedState(
        state_root,
        node_name,
        BuildState(plane_a, node_name),
        plane_a);
    local_state_repository.SaveLocalAppliedGeneration(
        state_root,
        node_name,
        11,
        plane_a);
    local_state_repository.SaveLocalAppliedState(
        state_root,
        node_name,
        BuildState(plane_b, node_name),
        plane_b);
    local_state_repository.SaveLocalAppliedGeneration(
        state_root,
        node_name,
        12,
        plane_b);
    local_state_repository.RewriteAggregateLocalState(state_root, node_name);
    local_state_repository.RewriteAggregateLocalGeneration(state_root, node_name);

    const fs::path plane_a_root =
        local_state_path_support.LocalPlaneRoot(state_root, node_name, plane_a);
    fs::create_directories(plane_a_root / "nested");
    {
      std::ofstream log_file(plane_a_root / "post-deploy.log");
      log_file << "hello";
    }
    {
      std::ofstream nested_file(plane_a_root / "nested" / "extra.txt");
      nested_file << "still here";
    }

    local_state_repository.RemoveLocalAppliedPlaneState(
        state_root,
        node_name,
        plane_a);
    local_state_repository.RewriteAggregateLocalState(state_root, node_name);
    local_state_repository.RewriteAggregateLocalGeneration(state_root, node_name);

    Expect(!fs::exists(plane_a_root), "plane-a root should be removed recursively");
    Expect(
        !local_state_repository.LoadLocalAppliedState(state_root, node_name, plane_a).has_value(),
        "plane-a state should be absent after removal");
    Expect(
        local_state_repository.LoadLocalAppliedState(state_root, node_name, plane_b)
            .has_value(),
        "plane-b state should remain present");

    const auto aggregate_state = local_state_repository.LoadLocalAppliedState(state_root, node_name);
    Expect(aggregate_state.has_value(), "aggregate state should still exist");
    Expect(aggregate_state->instances.size() == 4,
           "aggregate should only contain plane-b infer aggregator, infer leaf, worker, and skills instances");
    Expect(
        std::all_of(
            aggregate_state->instances.begin(),
            aggregate_state->instances.end(),
            [&](const naim::InstanceSpec& instance) { return instance.plane_name == plane_b; }),
        "aggregate should only contain plane-b instances");
    Expect(
        local_runtime_state_support.ExpectedRuntimeStatusCountForNode(
            *aggregate_state,
            node_name) == 4,
        "infer aggregator, infer leaf, worker, and skills instances should all contribute runtime statuses");

    const std::string partial_plane = "plane-partial";
    const naim::DesiredState partial_state =
        BuildPartialLlmStateWithoutInfer(partial_plane, node_name);
    local_state_repository.SaveLocalAppliedState(
        state_root,
        node_name,
        partial_state,
        partial_plane);
    const auto reloaded_partial_state =
        local_state_repository.LoadLocalAppliedState(
            state_root,
            node_name,
            partial_plane);
    Expect(reloaded_partial_state.has_value(),
           "partial llm state should round-trip through local state persistence");
    Expect(reloaded_partial_state->plane_name == partial_plane,
           "partial llm state should preserve plane name");
    Expect(
        std::none_of(
            reloaded_partial_state->instances.begin(),
            reloaded_partial_state->instances.end(),
            [](const naim::InstanceSpec& instance) {
              return instance.role == naim::InstanceRole::Infer;
            }),
        "partial llm state should remain infer-free after round-trip");

    naim::DesiredState bad_state = BuildState("plane-c", node_name);
    naim::NodeInventory other_node;
    other_node.name = "remote-worker-a";
    other_node.platform = "linux";
    other_node.execution_mode = naim::HostExecutionMode::WorkerOnly;
    bad_state.nodes.push_back(std::move(other_node));
    bool threw = false;
    try {
      (void)local_state_repository.RequireSingleNodeName(bad_state);
    } catch (const std::exception&) {
      threw = true;
    }
    Expect(threw, "RequireSingleNodeName should reject non-sliced multi-node desired state");

    fs::remove_all(temp_root, cleanup_error);
    std::cout << "ok: recursive-plane-state-removal\n";
    std::cout << "ok: partial-llm-state-roundtrip\n";
    std::cout << "ok: require-single-node-sliced-state\n";
    return 0;
  } catch (const std::exception& ex) {
    std::cerr << ex.what() << '\n';
    return 1;
  }
}
