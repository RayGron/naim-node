#pragma once

#include <functional>
#include <optional>
#include <string>

#include <nlohmann/json.hpp>

#include "comet/sqlite_store.h"

namespace comet::controller {

using HostRegistryEventSink = std::function<void(
    comet::ControllerStore& store,
    const std::string& event_type,
    const std::string& message,
    const nlohmann::json& payload,
    const std::string& node_name,
    const std::string& severity)>;

class HostRegistryService {
 public:
  HostRegistryService(std::string db_path, HostRegistryEventSink event_sink);

  nlohmann::json BuildPayload(const std::optional<std::string>& node_name) const;

  int ShowHosts(const std::optional<std::string>& node_name) const;

  int RevokeHost(
      const std::string& node_name,
      const std::optional<std::string>& status_message) const;

  int RotateHostKey(
      const std::string& node_name,
      const std::string& public_key_base64,
      const std::optional<std::string>& status_message) const;

 private:
  std::string db_path_;
  HostRegistryEventSink event_sink_;
};

}  // namespace comet::controller
