#include "cli/hostd_cli.h"

#include <iostream>
#include <stdexcept>

#include "config/node_config_loader.h"

namespace naim::hostd {

HostdCli::HostdCli(
    const HostdAssignmentService& assignment_service,
    const HostdObservationService& observation_service)
    : dispatcher_(assignment_service, observation_service) {}

int HostdCli::Run(
    const HostdCommandLine& command_line,
    const NodeConfigLoader& config_loader,
    const char* argv0) const {
  if (!command_line.HasCommand()) {
    command_line.PrintUsage(std::cout);
    return 1;
  }

  const auto node_name = command_line.node();
  if (!node_name.has_value()) {
    std::cerr << "error: --node is required\n";
    return 1;
  }

  try {
    const NaimNodeConfig node_config = config_loader.Load(command_line.config_path(), argv0);
    if (dispatcher_.Dispatch(command_line, node_config, *node_name)) {
      return 0;
    }
  } catch (const std::exception& error) {
    std::cerr << "error: " << error.what() << "\n";
    return 1;
  }

  command_line.PrintUsage(std::cout);
  return 1;
}

}  // namespace naim::hostd
