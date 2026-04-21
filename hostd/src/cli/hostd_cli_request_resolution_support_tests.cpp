#include <iostream>
#include <stdexcept>
#include <string>

#include "cli/hostd_cli_request_resolution_support.h"

namespace {

void Expect(bool condition, const std::string& message) {
  if (!condition) {
    throw std::runtime_error(message);
  }
}

}  // namespace

int main() {
  try {
    naim::hostd::HostdCliRequestResolutionSupport support;
    naim::hostd::NaimNodeConfig node_config;
    node_config.storage_root = "/srv/naim";

    {
      char command[] = "show-local-state";
      char node_flag[] = "--node";
      char node_value[] = "gamma";
      char state_flag[] = "--state-root";
      char state_value[] = "/tmp/local-state";
      char* argv[] = {
          command,
          command,
          node_flag,
          node_value,
          state_flag,
          state_value,
      };
      const naim::hostd::HostdCommandLine command_line(6, argv);
      const auto request = support.ResolveNodeStateRequest(command_line, "gamma");
      Expect(request.node_name == "gamma", "node state request should preserve node name");
      Expect(
          request.state_root == "/tmp/local-state",
          "node state request should resolve state root");
      std::cout << "ok: hostd-cli-request-resolution-node-state\n";
    }

    {
      char command[] = "show-state-ops";
      char node_flag[] = "--node";
      char node_value[] = "alpha";
      char db_flag[] = "--db";
      char db_value[] = "/tmp/controller.sqlite";
      char artifacts_flag[] = "--artifacts-root";
      char artifacts_value[] = "/tmp/artifacts";
      char runtime_flag[] = "--runtime-root";
      char runtime_value[] = "/tmp/runtime";
      char state_flag[] = "--state-root";
      char state_value[] = "/tmp/state";
      char compose_flag[] = "--compose-mode";
      char compose_value[] = "exec";
      char* argv[] = {
          command,
          command,
          node_flag,
          node_value,
          db_flag,
          db_value,
          artifacts_flag,
          artifacts_value,
          runtime_flag,
          runtime_value,
          state_flag,
          state_value,
          compose_flag,
          compose_value,
      };
      const naim::hostd::HostdCommandLine command_line(14, argv);
      const auto request =
          support.ResolveStateOpsRequest(command_line, node_config, "alpha");
      Expect(request.common.node_name == "alpha", "node name should be preserved");
      Expect(request.common.storage_root == "/srv/naim", "storage root should come from config");
      Expect(
          request.common.runtime_root.has_value() &&
              *request.common.runtime_root == "/tmp/runtime",
          "runtime root should be resolved");
      Expect(request.common.state_root == "/tmp/state", "state root should be resolved");
      Expect(request.db_path == "/tmp/controller.sqlite", "db path should be resolved");
      Expect(request.artifacts_root == "/tmp/artifacts", "artifacts root should be resolved");
      Expect(
          request.compose_mode == naim::hostd::ComposeMode::Exec,
          "compose mode should be resolved");
      std::cout << "ok: hostd-cli-request-resolution-state-ops\n";
    }

    {
      char command[] = "apply-next-assignment";
      char node_flag[] = "--node";
      char node_value[] = "beta";
      char controller_flag[] = "--controller";
      char controller_value[] = "http://127.0.0.1:28081";
      char key_flag[] = "--host-private-key";
      char key_value[] = "/tmp/hostd.key";
      char fingerprint_flag[] = "--controller-fingerprint";
      char fingerprint_value[] = "sha256:abc";
      char onboarding_flag[] = "--onboarding-key";
      char onboarding_value[] = "token";
      char* argv[] = {
          command,
          command,
          node_flag,
          node_value,
          controller_flag,
          controller_value,
          key_flag,
          key_value,
          fingerprint_flag,
          fingerprint_value,
          onboarding_flag,
          onboarding_value,
      };
      const naim::hostd::HostdCommandLine command_line(12, argv);
      const auto request =
          support.ResolveAssignmentRequest(command_line, node_config, "beta");
      Expect(request.common.node_name == "beta", "assignment node name should be preserved");
      Expect(
          request.controller_url.has_value() &&
              *request.controller_url == "http://127.0.0.1:28081",
          "controller url should be preserved");
      Expect(
          request.host_private_key_path.has_value() &&
              *request.host_private_key_path == "/tmp/hostd.key",
          "host private key should be preserved");
      Expect(
          request.controller_fingerprint.has_value() &&
              *request.controller_fingerprint == "sha256:abc",
          "controller fingerprint should be preserved");
      Expect(
          request.onboarding_key.has_value() && *request.onboarding_key == "token",
          "onboarding key should be preserved");
      Expect(
          request.compose_mode == naim::hostd::ComposeMode::Skip,
          "compose mode should default to skip");
      std::cout << "ok: hostd-cli-request-resolution-assignment\n";
    }

    return 0;
  } catch (const std::exception& ex) {
    std::cerr << ex.what() << '\n';
    return 1;
  }
}
