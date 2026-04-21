#pragma once

#include <string>

#include "cli/hostd_command_line.h"
#include "cli/hostd_cli_request_resolution_support.h"
#include "config/node_config_loader.h"

namespace naim::hostd {

class HostdObservationService;
class HostdAssignmentService;

class HostdCliCommandDispatcher final {
 public:
  HostdCliCommandDispatcher(
      const HostdAssignmentService& assignment_service,
      const HostdObservationService& observation_service);

  bool Dispatch(
      const HostdCommandLine& command_line,
      const NaimNodeConfig& node_config,
      const std::string& node_name) const;

 private:
  bool DispatchShowDemoOps(
      const HostdCommandLine& command_line,
      const NaimNodeConfig& node_config,
      const std::string& node_name) const;
  bool DispatchShowStateOps(
      const HostdCommandLine& command_line,
      const NaimNodeConfig& node_config,
      const std::string& node_name) const;
  bool DispatchShowLocalState(
      const HostdCommandLine& command_line,
      const std::string& node_name) const;
  bool DispatchShowRuntimeStatus(
      const HostdCommandLine& command_line,
      const std::string& node_name) const;
  bool DispatchReportObservedState(
      const HostdCommandLine& command_line,
      const NaimNodeConfig& node_config,
      const std::string& node_name) const;
  bool DispatchApplyStateOps(
      const HostdCommandLine& command_line,
      const NaimNodeConfig& node_config,
      const std::string& node_name) const;
  bool DispatchApplyNextAssignment(
      const HostdCommandLine& command_line,
      const NaimNodeConfig& node_config,
      const std::string& node_name) const;

  const HostdAssignmentService& assignment_service_;
  const HostdObservationService& observation_service_;
  HostdCliRequestResolutionSupport resolution_support_;
};

}  // namespace naim::hostd
