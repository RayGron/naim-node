#include <filesystem>
#include <fstream>
#include <iostream>
#include <stdexcept>
#include <string>

#include "app/hostd_local_state_support.h"

namespace {

void Expect(bool condition, const std::string& message) {
  if (!condition) {
    throw std::runtime_error(message);
  }
}

comet::DesiredState BuildState(
    const std::string& plane_name,
    const std::string& node_name) {
  comet::DesiredState state;
  state.plane_name = plane_name;

  comet::NodeInventory node;
  node.name = node_name;
  node.platform = "linux";
  node.execution_mode = comet::HostExecutionMode::Mixed;
  state.nodes.push_back(std::move(node));

  comet::InstanceSpec instance;
  instance.name = "worker-" + plane_name;
  instance.plane_name = plane_name;
  instance.node_name = node_name;
  instance.role = comet::InstanceRole::Worker;
  instance.image = "example/worker:dev";
  instance.command = "/app/run.sh";
  state.instances.push_back(std::move(instance));
  return state;
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
    Expect(aggregate_state->instances.size() == 1, "aggregate should only contain plane-b");
    Expect(
        aggregate_state->instances.front().plane_name == plane_b,
        "aggregate should only contain plane-b instances");

    fs::remove_all(temp_root, cleanup_error);
    std::cout << "ok: recursive-plane-state-removal\n";
    return 0;
  } catch (const std::exception& ex) {
    std::cerr << ex.what() << '\n';
    return 1;
  }
}
