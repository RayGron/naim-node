#include "cli/hostd_cli_request_resolution_support.h"

namespace naim::hostd {

HostdCliNodeStateRequest HostdCliRequestResolutionSupport::ResolveNodeStateRequest(
    const HostdCommandLine& command_line,
    const std::string& node_name) const {
  HostdCliNodeStateRequest request;
  request.node_name = node_name;
  request.state_root = command_line.ResolveStateRoot(command_line.state_root());
  return request;
}

HostdCliCommonRequest HostdCliRequestResolutionSupport::ResolveCommonRequest(
    const HostdCommandLine& command_line,
    const NaimNodeConfig& node_config,
    const std::string& node_name) const {
  HostdCliCommonRequest request;
  const auto state_request = ResolveNodeStateRequest(command_line, node_name);
  request.node_name = state_request.node_name;
  request.storage_root = node_config.storage_root;
  request.runtime_root = command_line.runtime_root();
  request.state_root = state_request.state_root;
  return request;
}

HostdCliStateOpsRequest HostdCliRequestResolutionSupport::ResolveStateOpsRequest(
    const HostdCommandLine& command_line,
    const NaimNodeConfig& node_config,
    const std::string& node_name) const {
  HostdCliStateOpsRequest request;
  request.common = ResolveCommonRequest(command_line, node_config, node_name);
  request.db_path = command_line.ResolveDbPath(command_line.db());
  request.artifacts_root =
      command_line.ResolveArtifactsRoot(command_line.artifacts_root());
  request.compose_mode =
      command_line.ResolveComposeMode(command_line.compose_mode_raw());
  return request;
}

HostdCliAssignmentRequest HostdCliRequestResolutionSupport::ResolveAssignmentRequest(
    const HostdCommandLine& command_line,
    const NaimNodeConfig& node_config,
    const std::string& node_name) const {
  HostdCliAssignmentRequest request;
  request.common = ResolveCommonRequest(command_line, node_config, node_name);
  request.db_path = command_line.db();
  request.controller_url = command_line.controller();
  request.host_private_key_path = command_line.host_private_key();
  request.controller_fingerprint = command_line.controller_fingerprint();
  request.onboarding_key = command_line.onboarding_key();
  request.compose_mode =
      command_line.ResolveComposeMode(command_line.compose_mode_raw());
  return request;
}

}  // namespace naim::hostd
