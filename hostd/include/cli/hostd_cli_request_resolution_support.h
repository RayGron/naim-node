#pragma once

#include <optional>
#include <string>

#include "cli/hostd_command_line.h"
#include "config/node_config_loader.h"

namespace naim::hostd {

struct HostdCliCommonRequest {
  std::string node_name;
  std::string storage_root;
  std::optional<std::string> runtime_root;
  std::string state_root;
};

struct HostdCliNodeStateRequest {
  std::string node_name;
  std::string state_root;
};

struct HostdCliStateOpsRequest {
  HostdCliCommonRequest common;
  std::string db_path;
  std::string artifacts_root;
  ComposeMode compose_mode = ComposeMode::Skip;
};

struct HostdCliAssignmentRequest {
  HostdCliCommonRequest common;
  std::optional<std::string> db_path;
  std::optional<std::string> controller_url;
  std::optional<std::string> host_private_key_path;
  std::optional<std::string> controller_fingerprint;
  std::optional<std::string> onboarding_key;
  ComposeMode compose_mode = ComposeMode::Skip;
};

class HostdCliRequestResolutionSupport final {
 public:
  HostdCliNodeStateRequest ResolveNodeStateRequest(
      const HostdCommandLine& command_line,
      const std::string& node_name) const;

  HostdCliCommonRequest ResolveCommonRequest(
      const HostdCommandLine& command_line,
      const NaimNodeConfig& node_config,
      const std::string& node_name) const;

  HostdCliStateOpsRequest ResolveStateOpsRequest(
      const HostdCommandLine& command_line,
      const NaimNodeConfig& node_config,
      const std::string& node_name) const;

  HostdCliAssignmentRequest ResolveAssignmentRequest(
      const HostdCommandLine& command_line,
      const NaimNodeConfig& node_config,
      const std::string& node_name) const;
};

}  // namespace naim::hostd
