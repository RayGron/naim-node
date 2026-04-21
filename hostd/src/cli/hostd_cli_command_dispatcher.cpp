#include "cli/hostd_cli_command_dispatcher.h"

#include "observation/hostd_observation_service.h"
#include "state_apply/hostd_assignment_service.h"

namespace naim::hostd {

HostdCliCommandDispatcher::HostdCliCommandDispatcher(
    const HostdAssignmentService& assignment_service,
    const HostdObservationService& observation_service)
    : assignment_service_(assignment_service),
      observation_service_(observation_service) {}

bool HostdCliCommandDispatcher::Dispatch(
    const HostdCommandLine& command_line,
    const NaimNodeConfig& node_config,
    const std::string& node_name) const {
  return DispatchShowDemoOps(command_line, node_config, node_name) ||
         DispatchShowStateOps(command_line, node_config, node_name) ||
         DispatchShowLocalState(command_line, node_name) ||
         DispatchShowRuntimeStatus(command_line, node_name) ||
         DispatchReportObservedState(command_line, node_config, node_name) ||
         DispatchApplyStateOps(command_line, node_config, node_name) ||
         DispatchApplyNextAssignment(command_line, node_config, node_name);
}

bool HostdCliCommandDispatcher::DispatchShowDemoOps(
    const HostdCommandLine& command_line,
    const NaimNodeConfig& node_config,
    const std::string& node_name) const {
  if (command_line.command() != "show-demo-ops") {
    return false;
  }
  const auto request =
      resolution_support_.ResolveCommonRequest(command_line, node_config, node_name);
  assignment_service_.ShowDemoOps(
      request.node_name,
      request.storage_root,
      request.runtime_root);
  return true;
}

bool HostdCliCommandDispatcher::DispatchShowStateOps(
    const HostdCommandLine& command_line,
    const NaimNodeConfig& node_config,
    const std::string& node_name) const {
  if (command_line.command() != "show-state-ops") {
    return false;
  }
  const auto request =
      resolution_support_.ResolveStateOpsRequest(command_line, node_config, node_name);
  assignment_service_.ShowStateOps(
      request.db_path,
      request.common.node_name,
      request.artifacts_root,
      request.common.storage_root,
      request.common.runtime_root,
      request.common.state_root);
  return true;
}

bool HostdCliCommandDispatcher::DispatchShowLocalState(
    const HostdCommandLine& command_line,
    const std::string& node_name) const {
  if (command_line.command() != "show-local-state") {
    return false;
  }
  const auto request = resolution_support_.ResolveNodeStateRequest(command_line, node_name);
  observation_service_.ShowLocalState(request.node_name, request.state_root);
  return true;
}

bool HostdCliCommandDispatcher::DispatchShowRuntimeStatus(
    const HostdCommandLine& command_line,
    const std::string& node_name) const {
  if (command_line.command() != "show-runtime-status") {
    return false;
  }
  const auto request = resolution_support_.ResolveNodeStateRequest(command_line, node_name);
  observation_service_.ShowRuntimeStatus(request.node_name, request.state_root);
  return true;
}

bool HostdCliCommandDispatcher::DispatchReportObservedState(
    const HostdCommandLine& command_line,
    const NaimNodeConfig& node_config,
    const std::string& node_name) const {
  if (command_line.command() != "report-observed-state") {
    return false;
  }
  const auto request =
      resolution_support_.ResolveAssignmentRequest(command_line, node_config, node_name);
  observation_service_.ReportLocalObservedState(
      request.db_path,
      request.controller_url,
      request.host_private_key_path,
      request.controller_fingerprint,
      request.onboarding_key,
      request.common.node_name,
      request.common.storage_root,
      request.common.state_root);
  return true;
}

bool HostdCliCommandDispatcher::DispatchApplyStateOps(
    const HostdCommandLine& command_line,
    const NaimNodeConfig& node_config,
    const std::string& node_name) const {
  if (command_line.command() != "apply-state-ops") {
    return false;
  }
  const auto request =
      resolution_support_.ResolveStateOpsRequest(command_line, node_config, node_name);
  assignment_service_.ApplyStateOps(
      request.db_path,
      request.common.node_name,
      request.artifacts_root,
      request.common.storage_root,
      request.common.runtime_root,
      request.common.state_root,
      request.compose_mode);
  return true;
}

bool HostdCliCommandDispatcher::DispatchApplyNextAssignment(
    const HostdCommandLine& command_line,
    const NaimNodeConfig& node_config,
    const std::string& node_name) const {
  if (command_line.command() != "apply-next-assignment") {
    return false;
  }
  const auto request =
      resolution_support_.ResolveAssignmentRequest(command_line, node_config, node_name);
  assignment_service_.ApplyNextAssignment(
      request.db_path,
      request.controller_url,
      request.host_private_key_path,
      request.controller_fingerprint,
      request.onboarding_key,
      request.common.node_name,
      request.common.storage_root,
      request.common.runtime_root,
      request.common.state_root,
      request.compose_mode);
  return true;
}

}  // namespace naim::hostd
