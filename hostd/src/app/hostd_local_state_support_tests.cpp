#include <algorithm>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <stdexcept>
#include <string>

#include <nlohmann/json.hpp>

#include "app/hostd_local_state_support.h"
#include "comet/state/desired_state_v2_renderer.h"

namespace {

using nlohmann::json;

void Expect(bool condition, const std::string& message) {
  if (!condition) {
    throw std::runtime_error(message);
  }
}

comet::DesiredState BuildState(
    const std::string& plane_name,
    const std::string& node_name) {
  return comet::DesiredStateV2Renderer::Render(json{
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

}  // namespace

int main() {
  try {
    namespace fs = std::filesystem;
    const fs::path temp_root =
        fs::temp_directory_path() / "comet-hostd-local-state-tests";
    std::error_code cleanup_error;
    fs::remove_all(temp_root, cleanup_error);
    fs::create_directories(temp_root);

    const std::string state_root = temp_root.string();
    const std::string node_name = "local-hostd";
    const std::string plane_a = "plane-a";
    const std::string plane_b = "plane-b";

    comet::hostd::local_state_support::SaveLocalAppliedState(
        state_root,
        node_name,
        BuildState(plane_a, node_name),
        plane_a);
    comet::hostd::local_state_support::SaveLocalAppliedGeneration(
        state_root,
        node_name,
        11,
        plane_a);
    comet::hostd::local_state_support::SaveLocalAppliedState(
        state_root,
        node_name,
        BuildState(plane_b, node_name),
        plane_b);
    comet::hostd::local_state_support::SaveLocalAppliedGeneration(
        state_root,
        node_name,
        12,
        plane_b);
    comet::hostd::local_state_support::RewriteAggregateLocalState(state_root, node_name);
    comet::hostd::local_state_support::RewriteAggregateLocalGeneration(state_root, node_name);

    const fs::path plane_a_root =
        comet::hostd::local_state_support::LocalPlaneRoot(state_root, node_name, plane_a);
    fs::create_directories(plane_a_root / "nested");
    {
      std::ofstream log_file(plane_a_root / "post-deploy.log");
      log_file << "hello";
    }
    {
      std::ofstream nested_file(plane_a_root / "nested" / "extra.txt");
      nested_file << "still here";
    }

    comet::hostd::local_state_support::RemoveLocalAppliedPlaneState(
        state_root,
        node_name,
        plane_a);
    comet::hostd::local_state_support::RewriteAggregateLocalState(state_root, node_name);
    comet::hostd::local_state_support::RewriteAggregateLocalGeneration(state_root, node_name);

    Expect(!fs::exists(plane_a_root), "plane-a root should be removed recursively");
    Expect(
        !comet::hostd::local_state_support::LoadLocalAppliedState(state_root, node_name, plane_a)
             .has_value(),
        "plane-a state should be absent after removal");
    Expect(
        comet::hostd::local_state_support::LoadLocalAppliedState(state_root, node_name, plane_b)
            .has_value(),
        "plane-b state should remain present");

    const auto aggregate_state =
        comet::hostd::local_state_support::LoadLocalAppliedState(state_root, node_name);
    Expect(aggregate_state.has_value(), "aggregate state should still exist");
    Expect(aggregate_state->instances.size() == 4,
           "aggregate should only contain plane-b infer aggregator, infer leaf, worker, and skills instances");
    Expect(
        std::all_of(
            aggregate_state->instances.begin(),
            aggregate_state->instances.end(),
            [&](const comet::InstanceSpec& instance) { return instance.plane_name == plane_b; }),
        "aggregate should only contain plane-b instances");
    Expect(
        comet::hostd::local_state_support::ExpectedRuntimeStatusCountForNode(
            *aggregate_state,
            node_name) == 4,
        "infer aggregator, infer leaf, worker, and skills instances should all contribute runtime statuses");

    comet::DesiredState bad_state = BuildState("plane-c", node_name);
    comet::NodeInventory other_node;
    other_node.name = "remote-worker-a";
    other_node.platform = "linux";
    other_node.execution_mode = comet::HostExecutionMode::WorkerOnly;
    bad_state.nodes.push_back(std::move(other_node));
    bool threw = false;
    try {
      (void)comet::hostd::local_state_support::RequireSingleNodeName(bad_state);
    } catch (const std::exception&) {
      threw = true;
    }
    Expect(threw, "RequireSingleNodeName should reject non-sliced multi-node desired state");

    fs::remove_all(temp_root, cleanup_error);
    std::cout << "ok: recursive-plane-state-removal\n";
    std::cout << "ok: require-single-node-sliced-state\n";
    return 0;
  } catch (const std::exception& ex) {
    std::cerr << ex.what() << '\n';
    return 1;
  }
}
