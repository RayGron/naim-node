#pragma once

#include <functional>
#include <optional>
#include <string>

#include <nlohmann/json.hpp>

#include "app/controller_service_interfaces.h"
#include "naim/state/sqlite_store.h"

namespace naim::controller {

using HostRegistryEventSink = std::function<void(
    naim::ControllerStore& store,
    const std::string& event_type,
    const std::string& message,
    const nlohmann::json& payload,
    const std::string& node_name,
    const std::string& severity)>;

class HostRegistryService : public IHostRegistryService {
 public:
  HostRegistryService(std::string db_path, HostRegistryEventSink event_sink);

  nlohmann::json BuildPayload(const std::optional<std::string>& node_name) const;

  int ShowHosts(const std::optional<std::string>& node_name) const override;

  int RevokeHost(
      const std::string& node_name,
      const std::optional<std::string>& status_message) const override;

  int RotateHostKey(
      const std::string& node_name,
      const std::string& public_key_base64,
      const std::optional<std::string>& status_message) const override;

  nlohmann::json ResetHostOnboardingPayload(
      const std::string& node_name,
      const std::optional<std::string>& status_message) const;

  int ResetHostOnboarding(
      const std::string& node_name,
      const std::optional<std::string>& status_message) const override;

  nlohmann::json SetHostStorageRolePayload(
      const std::string& node_name,
      bool enabled,
      const std::optional<std::string>& status_message) const;

  int SetHostStorageRole(
      const std::string& node_name,
      bool enabled,
      const std::optional<std::string>& status_message) const override;

  nlohmann::json NotifyConnectedHostsOfReleasePayload(
      const std::string& manifest_path,
      const std::optional<std::string>& status_message) const;

  int NotifyConnectedHostsOfRelease(
      const std::string& manifest_path,
      const std::optional<std::string>& status_message) const override;

 private:
  std::string db_path_;
  HostRegistryEventSink event_sink_;
};

}  // namespace naim::controller
