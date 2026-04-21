#include <filesystem>
#include <fstream>
#include <iostream>
#include <stdexcept>
#include <string>

#include "app/hostd_command_support.h"
#include "app/hostd_local_state_path_support.h"
#include "app/hostd_post_deploy_support.h"

namespace {

void Expect(bool condition, const std::string& message) {
  if (!condition) {
    throw std::runtime_error(message);
  }
}

naim::DesiredState BuildState(
    const std::string& plane_name,
    const std::string& node_name) {
  naim::DesiredState state;
  state.plane_name = plane_name;
  naim::NodeInventory node;
  node.name = node_name;
  state.nodes.push_back(std::move(node));
  return state;
}

}  // namespace

int main() {
  try {
    const naim::hostd::HostdCommandSupport command_support;
    const naim::hostd::HostdLocalStatePathSupport local_state_path_support;
    const naim::hostd::HostdPostDeploySupport support(command_support);

    {
      auto state = BuildState("plane-a", "node-a");
      Expect(!support.ShouldRunForNode(state, "node-a"), "scriptless state should not run");
    }

    {
      auto state = BuildState("plane-b", "node-b");
      state.post_deploy_script = "scripts/post.sh";
      state.inference.primary_infer_node = "node-b";
      Expect(support.ShouldRunForNode(state, "node-b"), "primary infer node should run");
      Expect(!support.ShouldRunForNode(state, "node-c"), "other node should not run");
    }

    {
      auto state = BuildState("plane-c", "node-c");
      state.post_deploy_script = "scripts/post.sh";
      naim::InstanceSpec app;
      app.name = "app-plane-c";
      app.node_name = "node-c";
      app.role = naim::InstanceRole::App;
      state.instances.push_back(std::move(app));
      Expect(support.ShouldRunForNode(state, "node-c"), "app node should run");
    }

    {
      namespace fs = std::filesystem;
      const fs::path temp_root = fs::temp_directory_path() / "naim-hostd-post-deploy-tests";
      std::error_code cleanup_error;
      fs::remove_all(temp_root, cleanup_error);
      const fs::path artifacts_root = temp_root / "artifacts";
      fs::create_directories(artifacts_root / "plane-d" / "scripts");
      const fs::path script_path = artifacts_root / "plane-d" / "scripts" / "post.sh";
      {
        std::ofstream script(script_path);
        script << "#!/bin/sh\n";
        script << "printf '%s' \"$NAIM_PLANE_NAME:$NAIM_NODE_NAME\" > \"$NAIM_POST_DEPLOY_LOG.out\"\n";
      }
      fs::permissions(
          script_path,
          fs::perms::owner_exec | fs::perms::owner_read | fs::perms::owner_write,
          fs::perm_options::replace);

      auto state = BuildState("plane-d", "node-d");
      state.post_deploy_script = "scripts/post.sh";
      const std::string state_root = (temp_root / "state").string();
      support.RunIfNeeded(
          state,
          "node-d",
          artifacts_root.string(),
          "/storage",
          std::nullopt,
          state_root,
          7,
          std::nullopt,
          nullptr);

      const fs::path output_path =
          fs::path(local_state_path_support.LocalPlaneRoot(
              state_root,
              "node-d",
              "plane-d")) /
          "post-deploy.log.out";
      std::ifstream input(output_path);
      std::string text;
      std::getline(input, text);
      Expect(text == "plane-d:node-d", "post deploy script should receive env vars");
      fs::remove_all(temp_root, cleanup_error);
    }

    std::cout << "ok: hostd-post-deploy-selection\n";
    std::cout << "ok: hostd-post-deploy-run\n";
    return 0;
  } catch (const std::exception& ex) {
    std::cerr << ex.what() << '\n';
    return 1;
  }
}
