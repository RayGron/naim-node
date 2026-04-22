#include "host/host_registry_service.h"

#include <cctype>
#include <cstdint>
#include <iostream>
#include <map>
#include <set>
#include <stdexcept>
#include <utility>

#include "app/controller_time_support.h"
#include "naim/security/crypto_utils.h"
#include "naim/runtime/runtime_status.h"

namespace naim::controller {

namespace {

using nlohmann::json;

constexpr int kPeerLinkFreshSeconds = 120;

struct HostInventorySummary {
  std::string storage_root;
  int gpu_count = 0;
  std::uint64_t total_memory_bytes = 0;
  std::uint64_t storage_total_bytes = 0;
  std::uint64_t storage_free_bytes = 0;
  bool has_storage_capacity = false;
};

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

json ParseCapabilitiesJson(const std::string& capabilities_json) {
  if (capabilities_json.empty()) {
    return json::object();
  }
  return json::parse(capabilities_json, nullptr, false).is_discarded()
             ? json::object()
             : json::parse(capabilities_json, nullptr, false);
}

HostInventorySummary BuildInventorySummary(
    const naim::RegisteredHostRecord& host,
    const std::optional<naim::HostObservation>& observation) {
  HostInventorySummary summary;
  const json capabilities = ParseCapabilitiesJson(host.capabilities_json);
  if (capabilities.contains("storage_root") && capabilities["storage_root"].is_string()) {
    summary.storage_root = capabilities["storage_root"].get<std::string>();
  }
  if (observation.has_value()) {
    if (!observation->gpu_telemetry_json.empty()) {
      summary.gpu_count = static_cast<int>(
          naim::DeserializeGpuTelemetryJson(observation->gpu_telemetry_json).devices.size());
    }
    if (!observation->cpu_telemetry_json.empty()) {
      summary.total_memory_bytes =
          naim::DeserializeCpuTelemetryJson(observation->cpu_telemetry_json).total_memory_bytes;
    }
    if (!observation->disk_telemetry_json.empty()) {
      const auto disk = naim::DeserializeDiskTelemetryJson(observation->disk_telemetry_json);
      for (const auto& item : disk.items) {
        if (item.disk_name == "storage-root" ||
            (!summary.storage_root.empty() && item.mount_point == summary.storage_root)) {
          summary.storage_total_bytes = item.total_bytes;
          summary.storage_free_bytes = item.free_bytes;
          summary.has_storage_capacity = item.total_bytes > 0;
          if (summary.storage_root.empty()) {
            summary.storage_root = item.mount_point;
          }
          break;
        }
      }
    }
  }
  return summary;
}

constexpr std::uint64_t kStorageRoleMinDiskBytes =
    100ULL * 1024ULL * 1024ULL * 1024ULL;
constexpr std::uint64_t kWorkerMinRamBytes =
    32ULL * 1024ULL * 1024ULL * 1024ULL;

std::pair<std::string, std::string> DeriveRole(const HostInventorySummary& summary) {
  if (summary.gpu_count == 0 &&
      summary.storage_total_bytes > kStorageRoleMinDiskBytes) {
    return {"storage", "eligible: no gpu, disk > 100 GB"};
  }
  if (summary.gpu_count >= 1 &&
      summary.total_memory_bytes >= kWorkerMinRamBytes &&
      summary.storage_total_bytes > kStorageRoleMinDiskBytes) {
    return {"worker", "eligible: gpu >= 1, ram >= 32 GB, disk > 100 GB"};
  }
  if (summary.storage_total_bytes <= kStorageRoleMinDiskBytes) {
    return {"ineligible", "disk <= 100 GB"};
  }
  if (summary.gpu_count >= 1 && summary.total_memory_bytes < kWorkerMinRamBytes) {
    return {"ineligible", "gpu present but ram < 32 GB"};
  }
  return {"ineligible", "inventory does not match storage or worker role"};
}

bool IsStorageRoleEligible(
    const HostInventorySummary& inventory,
    const std::string& derived_role) {
  return !inventory.storage_root.empty() &&
         inventory.storage_total_bytes > kStorageRoleMinDiskBytes &&
         (derived_role == "storage" || derived_role == "worker");
}

}  // namespace

HostRegistryService::HostRegistryService(
    std::string db_path,
    HostRegistryEventSink event_sink)
    : db_path_(std::move(db_path)), event_sink_(std::move(event_sink)) {}

json HostRegistryService::BuildPayload(
    const std::optional<std::string>& node_name) const {
  naim::ControllerStore store(db_path_);
  store.Initialize();
  const auto observations = store.LoadHostObservations(node_name);
  store.DeleteStaleHostPeerLinks(
      ControllerTimeSupport::SqlTimestampAfterSeconds(-kPeerLinkFreshSeconds));
  const auto all_peer_links = store.LoadHostPeerLinks();
  std::map<std::string, std::vector<naim::HostPeerLinkRecord>> peer_links_by_node;
  for (const auto& link : all_peer_links) {
    peer_links_by_node[link.observer_node_name].push_back(link);
  }
  std::map<std::string, naim::HostObservation> observation_by_node;
  for (const auto& observation : observations) {
    observation_by_node[observation.node_name] = observation;
  }

  json items = json::array();
  for (const auto& host : store.LoadRegisteredHosts(node_name)) {
    const auto observation_it = observation_by_node.find(host.node_name);
    const std::optional<naim::HostObservation> observation =
        observation_it == observation_by_node.end()
            ? std::nullopt
            : std::optional<naim::HostObservation>(observation_it->second);
    const auto inventory = BuildInventorySummary(host, observation);
    const auto [derived_role, role_reason] =
        host.derived_role.empty() ? DeriveRole(inventory)
                                  : std::pair<std::string, std::string>(
                                        host.derived_role,
                                        host.role_reason.empty() ? DeriveRole(inventory).second
                                                                 : host.role_reason);
    std::set<std::string> plane_participation;
    if (observation.has_value() && !observation->plane_name.empty()) {
      plane_participation.insert(observation->plane_name);
    }
    json planes = json::array();
    for (const auto& plane_name_value : plane_participation) {
      planes.push_back(plane_name_value);
    }
    json lan_peers = json::array();
    if (const auto peer_it = peer_links_by_node.find(host.node_name);
        peer_it != peer_links_by_node.end()) {
      for (const auto& link : peer_it->second) {
        lan_peers.push_back(json{
            {"peer_node_name", link.peer_node_name},
            {"peer_endpoint", link.peer_endpoint.empty() ? json(nullptr) : json(link.peer_endpoint)},
            {"local_interface",
             link.local_interface.empty() ? json(nullptr) : json(link.local_interface)},
            {"remote_address",
             link.remote_address.empty() ? json(nullptr) : json(link.remote_address)},
            {"seen_udp", link.seen_udp},
            {"tcp_reachable", link.tcp_reachable},
            {"rtt_ms", link.rtt_ms > 0 ? json(link.rtt_ms) : json(nullptr)},
            {"last_seen_at", link.last_seen_at.empty() ? json(nullptr) : json(link.last_seen_at)},
            {"last_probe_at", link.last_probe_at.empty() ? json(nullptr) : json(link.last_probe_at)},
        });
      }
    }
    items.push_back(json{
        {"node_name", host.node_name},
        {"advertised_address",
         host.advertised_address.empty() ? json(nullptr) : json(host.advertised_address)},
        {"transport_mode", host.transport_mode},
        {"execution_mode",
         host.execution_mode.empty() ? json("mixed") : json(host.execution_mode)},
        {"registration_state", host.registration_state},
        {"onboarding_state",
         host.onboarding_state.empty() ? json("none") : json(host.onboarding_state)},
        {"session_state", host.session_state},
        {"derived_role", derived_role},
        {"role_eligible", derived_role == "storage" || derived_role == "worker"},
        {"storage_role_enabled", host.storage_role_enabled},
        {"storage_role_eligible",
         IsStorageRoleEligible(inventory, derived_role)},
        {"role_reason", role_reason.empty() ? json(nullptr) : json(role_reason)},
        {"last_inventory_scan_at",
         host.last_inventory_scan_at.empty() ? json(nullptr) : json(host.last_inventory_scan_at)},
        {"controller_public_key_fingerprint",
         host.controller_public_key_fingerprint.empty()
             ? json(nullptr)
             : json(host.controller_public_key_fingerprint)},
        {"host_public_key_fingerprint",
         host.public_key_base64.empty()
             ? json(nullptr)
             : json(naim::ComputeKeyFingerprintHex(host.public_key_base64))},
        {"status_message",
         host.status_message.empty() ? json(nullptr) : json(host.status_message)},
        {"last_session_at",
         host.last_session_at.empty() ? json(nullptr) : json(host.last_session_at)},
        {"session_expires_at",
         host.session_expires_at.empty() ? json(nullptr) : json(host.session_expires_at)},
        {"last_heartbeat_at",
         host.last_heartbeat_at.empty() ? json(nullptr) : json(host.last_heartbeat_at)},
        {"capacity_summary",
         json{
             {"storage_root",
              inventory.storage_root.empty() ? json(nullptr) : json(inventory.storage_root)},
             {"storage_total_bytes", inventory.has_storage_capacity
                                         ? json(inventory.storage_total_bytes)
                                         : json(nullptr)},
             {"storage_free_bytes", inventory.has_storage_capacity
                                        ? json(inventory.storage_free_bytes)
                                        : json(nullptr)},
             {"gpu_count", inventory.gpu_count},
             {"total_memory_bytes", inventory.total_memory_bytes > 0
                                        ? json(inventory.total_memory_bytes)
                                        : json(nullptr)},
         }},
        {"plane_participation", planes},
        {"lan_peers", std::move(lan_peers)},
        {"updated_at", host.updated_at},
    });
  }

  return json{
      {"service", "naim-controller"},
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
  naim::ControllerStore store(db_path_);
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
  naim::ControllerStore store(db_path_);
  store.Initialize();

  auto host = store.LoadRegisteredHost(node_name);
  if (!host.has_value()) {
    throw std::runtime_error("registered host '" + node_name + "' not found");
  }

  const std::string previous_fingerprint =
      host->public_key_base64.empty()
          ? std::string{}
          : naim::ComputeKeyFingerprintHex(host->public_key_base64);
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
          {"next_fingerprint", naim::ComputeKeyFingerprintHex(host->public_key_base64)},
      },
      node_name,
      "info");

  std::cout << "host key rotated: " << node_name
            << " fingerprint=" << naim::ComputeKeyFingerprintHex(host->public_key_base64)
            << "\n";
  return 0;
}

nlohmann::json HostRegistryService::ResetHostOnboardingPayload(
    const std::string& node_name,
    const std::optional<std::string>& status_message) const {
  naim::ControllerStore store(db_path_);
  store.Initialize();

  auto host = store.LoadRegisteredHost(node_name);
  if (!host.has_value()) {
    throw std::runtime_error("registered host '" + node_name + "' not found");
  }
  if (host->session_state == "connected") {
    throw std::runtime_error(
        "registered host '" + node_name +
        "' is connected; stop hostd or revoke the host before resetting onboarding");
  }

  const auto pending_assignments =
      store.LoadHostAssignments(node_name, naim::HostAssignmentStatus::Pending);
  const auto claimed_assignments =
      store.LoadHostAssignments(node_name, naim::HostAssignmentStatus::Claimed);
  if (!pending_assignments.empty() || !claimed_assignments.empty()) {
    throw std::runtime_error(
        "registered host '" + node_name +
        "' has pending or claimed assignments; drain or finish assignments before resetting");
  }

  const std::string previous_registration_state = host->registration_state;
  const std::string previous_onboarding_state = host->onboarding_state;
  const std::string previous_session_state = host->session_state;
  const std::string previous_fingerprint =
      host->public_key_base64.empty()
          ? std::string{}
          : naim::ComputeKeyFingerprintHex(host->public_key_base64);

  const std::string onboarding_key = naim::RandomTokenBase64(24);
  host->public_key_base64.clear();
  host->controller_public_key_fingerprint.clear();
  host->registration_state = "provisioned";
  host->onboarding_key_hash = naim::ComputeSha256Hex(onboarding_key);
  host->onboarding_state = "pending";
  host->derived_role = "ineligible";
  host->role_reason = "awaiting first inventory scan";
  host->storage_role_enabled = false;
  host->last_inventory_scan_at.clear();
  host->session_state = "disconnected";
  host->session_token.clear();
  host->session_expires_at.clear();
  host->session_host_sequence = 0;
  host->session_controller_sequence = 0;
  host->capabilities_json = "{}";
  host->status_message =
      status_message.value_or("host onboarding reset by operator");
  host->last_session_at.clear();
  host->last_heartbeat_at.clear();
  store.UpsertRegisteredHost(*host);

  event_sink_(
      store,
      "reset-onboarding",
      host->status_message,
      json{
          {"previous_registration_state", previous_registration_state},
          {"previous_onboarding_state", previous_onboarding_state},
          {"previous_session_state", previous_session_state},
          {"previous_fingerprint",
           previous_fingerprint.empty() ? json(nullptr) : json(previous_fingerprint)},
      },
      node_name,
      "warning");

  return json{
      {"service", "naim-controller"},
      {"node_name", node_name},
      {"registration_state", host->registration_state},
      {"onboarding_state", host->onboarding_state},
      {"onboarding_key", onboarding_key},
      {"status_message", host->status_message},
  };
}

int HostRegistryService::ResetHostOnboarding(
    const std::string& node_name,
    const std::optional<std::string>& status_message) const {
  std::cout << ResetHostOnboardingPayload(node_name, status_message).dump(2) << "\n";
  return 0;
}

nlohmann::json HostRegistryService::SetHostStorageRolePayload(
    const std::string& node_name,
    bool enabled,
    const std::optional<std::string>& status_message) const {
  naim::ControllerStore store(db_path_);
  store.Initialize();

  auto host = store.LoadRegisteredHost(node_name);
  if (!host.has_value()) {
    throw std::runtime_error("registered host '" + node_name + "' not found");
  }

  const auto observations = store.LoadHostObservations(node_name);
  const std::optional<naim::HostObservation> observation =
      observations.empty()
          ? std::nullopt
          : std::optional<naim::HostObservation>(observations.front());
  const auto inventory = BuildInventorySummary(*host, observation);
  const std::string derived_role =
      host->derived_role.empty() ? DeriveRole(inventory).first : host->derived_role;
  if (enabled && !IsStorageRoleEligible(inventory, derived_role)) {
    throw std::runtime_error(
        "registered host '" + node_name +
        "' is not eligible for storage role");
  }

  const bool previous_enabled = host->storage_role_enabled;
  host->storage_role_enabled = enabled;
  host->status_message =
      status_message.value_or(enabled ? "storage role enabled by operator"
                                      : "storage role disabled by operator");
  store.UpsertRegisteredHost(*host);

  event_sink_(
      store,
      "storage-role-updated",
      host->status_message,
      json{
          {"previous_enabled", previous_enabled},
          {"enabled", enabled},
          {"storage_root",
           inventory.storage_root.empty() ? json(nullptr) : json(inventory.storage_root)},
          {"storage_total_bytes", inventory.has_storage_capacity
                                      ? json(inventory.storage_total_bytes)
                                      : json(nullptr)},
          {"storage_free_bytes", inventory.has_storage_capacity
                                     ? json(inventory.storage_free_bytes)
                                     : json(nullptr)},
      },
      node_name,
      "info");

  return json{
      {"service", "naim-controller"},
      {"node_name", node_name},
      {"storage_role_enabled", host->storage_role_enabled},
      {"storage_root",
       inventory.storage_root.empty() ? json(nullptr) : json(inventory.storage_root)},
      {"status_message", host->status_message},
  };
}

int HostRegistryService::SetHostStorageRole(
    const std::string& node_name,
    bool enabled,
    const std::optional<std::string>& status_message) const {
  std::cout << SetHostStorageRolePayload(node_name, enabled, status_message).dump(2) << "\n";
  return 0;
}

}  // namespace naim::controller
