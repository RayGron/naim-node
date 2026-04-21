#pragma once

#include <optional>
#include <string>

#include "cli/hostd_cli_command_dispatcher.h"
#include "cli/hostd_command_line.h"

namespace naim::hostd {

class NodeConfigLoader;
class HostdObservationService;
class HostdAssignmentService;

class HostdCli {
 public:
  HostdCli(
      const HostdAssignmentService& assignment_service,
      const HostdObservationService& observation_service);

  int Run(const HostdCommandLine& command_line, const NodeConfigLoader& config_loader, const char* argv0) const;

 private:
  HostdCliCommandDispatcher dispatcher_;
};

}  // namespace naim::hostd
