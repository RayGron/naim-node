#pragma once

#include <string>

#include "app/controller_command_line.h"
#include "http/controller_http_transport.h"
#include "app/controller_service_interfaces.h"

namespace naim::controller {

class RemoteControllerCliService : public IRemoteControllerCliService {
 public:
  int ExecuteCommand(
      const ControllerEndpointTarget& target,
      const std::string& command,
      const ControllerCommandLine& cli) const override;

 private:
  int EmitRemoteJsonPayload(const nlohmann::json& payload) const;
  int EmitRemoteControllerActionPayload(const nlohmann::json& payload) const;
};

}  // namespace naim::controller
