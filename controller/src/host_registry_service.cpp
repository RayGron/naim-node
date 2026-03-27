#include "../include/host_registry_service.h"

#include <cctype>
#include <iostream>
#include <stdexcept>
#include <utility>

#include "comet/crypto_utils.h"

namespace comet::controller {

namespace {

using nlohmann::json;

std::string TrimWhitespace(const std::string& value) {
  std::size_t start = 0;
  while (start < value.size() &&
         std::isspace(static_cast<unsigned char>(value[start])) != 0) {
    ++start;
  }

  std::size_t end = value.size();
  while (end > start &&
         std::isspace(static_cast<unsigned char>(value[end - 1])) != 0) {
    --end;
  }

  return value.substr(start, end - start);
}

}  // namespace

HostRegistryService::HostRegistryService(
    std::string db_path,
    HostRegistryEventSink event_sink)
    : db_path_(std::move(db_path)), event_sink_(std::move(event_sink)) {}

json HostRegistryService::BuildPayload(
    const std::optional<std::string>& node_name) const {
  comet::ControllerStore store(db_path_);
  store.Initialize();

  json items = json::array();
  for (const auto& host : store.LoadRegisteredHosts(node_name)) {
    items.push_back(json{
        {"node_name", host.node_name},
        {"advertised_address",
         host.advertised_address.empty() ? json(nullptr) : json(host.advertised_address)},
        {"transport_mode", host.transport_mode},
        {"execution_mode",
         host.execution_mode.empty() ? json("mixed") : json(host.execution_mode)},
        {"registration_state", host.registration_state},
        {"session_state", host.session_state},
        {"controller_public_key_fingerprint",
         host.controller_public_key_fingerprint.empty()
             ? json(nullptr)
             : json(host.controller_public_key_fingerprint)},
        {"host_public_key_fingerprint",
         host.public_key_base64.empty()
             ? json(nullptr)
             : json(comet::ComputeKeyFingerprintHex(host.public_key_base64))},
        {"status_message",
         host.status_message.empty() ? json(nullptr) : json(host.status_message)},
        {"last_session_at",
         host.last_session_at.empty() ? json(nullptr) : json(host.last_session_at)},
        {"session_expires_at",
         host.session_expires_at.empty() ? json(nullptr) : json(host.session_expires_at)},
        {"last_heartbeat_at",
         host.last_heartbeat_at.empty() ? json(nullptr) : json(host.last_heartbeat_at)},
        {"updated_at", host.updated_at},
    });
  }

  return json{
      {"service", "comet-controller"},
      {"db_path", db_path_},
      {"node_name", node_name.has_value() ? json(*node_name) : json(nullptr)},
      {"items", items},
  };
}

int HostRegistryService::ShowHosts(const std::optional<std::string>& node_name) const {
  std::cout << BuildPayload(node_name).dump(2) << "\n";
  return 0;
}

int HostRegistryService::RevokeHost(
    const std::string& node_name,
    const std::optional<std::string>& status_message) const {
  comet::ControllerStore store(db_path_);
  store.Initialize();

  auto host = store.LoadRegisteredHost(node_name);
  if (!host.has_value()) {
    throw std::runtime_error("registered host '" + node_name + "' not found");
  }

  const std::string previous_state = host->registration_state;
  host->registration_state = "revoked";
  host->session_state = "revoked";
  host->session_token.clear();
  host->session_expires_at.clear();
  host->session_host_sequence = 0;
  host->session_controller_sequence = 0;
  host->status_message = status_message.value_or("revoked by operator");
  store.UpsertRegisteredHost(*host);
  event_sink_(
      store,
      "revoked",
      host->status_message,
      json{{"previous_registration_state", previous_state}},
      node_name,
      "warning");

  std::cout << "host revoked: " << node_name
            << " previous_registration_state=" << previous_state << "\n";
  return 0;
}

int HostRegistryService::RotateHostKey(
    const std::string& node_name,
    const std::string& public_key_base64,
    const std::optional<std::string>& status_message) const {
  comet::ControllerStore store(db_path_);
  store.Initialize();

  auto host = store.LoadRegisteredHost(node_name);
  if (!host.has_value()) {
    throw std::runtime_error("registered host '" + node_name + "' not found");
  }

  const std::string previous_fingerprint =
      host->public_key_base64.empty()
          ? std::string{}
          : comet::ComputeKeyFingerprintHex(host->public_key_base64);
  host->public_key_base64 = TrimWhitespace(public_key_base64);
  host->registration_state = "registered";
  host->session_state = "rotation-pending";
  host->session_token.clear();
  host->session_expires_at.clear();
  host->session_host_sequence = 0;
  host->session_controller_sequence = 0;
  host->status_message = status_message.value_or("host public key rotated by operator");
  store.UpsertRegisteredHost(*host);
  event_sink_(
      store,
      "rotated-key",
      host->status_message,
      json{
          {"previous_fingerprint",
           previous_fingerprint.empty() ? json(nullptr) : json(previous_fingerprint)},
          {"next_fingerprint", comet::ComputeKeyFingerprintHex(host->public_key_base64)},
      },
      node_name,
      "info");

  std::cout << "host key rotated: " << node_name
            << " fingerprint=" << comet::ComputeKeyFingerprintHex(host->public_key_base64)
            << "\n";
  return 0;
}

}  // namespace comet::controller
