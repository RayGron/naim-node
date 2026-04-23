#include <filesystem>
#include <fstream>
#include <iostream>
#include <optional>
#include <stdexcept>
#include <string>

#include <nlohmann/json.hpp>

#include "naim/runtime/runtime_status.h"
#include "naim/state/sqlite_store.h"
#include "app/controller_time_support.h"
#include "host/host_registry_service.h"

namespace fs = std::filesystem;
using nlohmann::json;

namespace {

void Expect(bool condition, const std::string& message) {
  if (!condition) {
    throw std::runtime_error(message);
  }
}

std::string MakeTempDbPath(const std::string& test_name) {
  const fs::path root = fs::temp_directory_path() / "naim-host-registry-tests" / test_name;
  std::error_code error;
  fs::remove_all(root, error);
  fs::create_directories(root);
  return (root / "controller.sqlite").string();
}

void SeedHost(
    naim::ControllerStore& store,
    const std::string& node_name,
    const std::string& storage_root) {
  naim::RegisteredHostRecord host;
  host.node_name = node_name;
  host.registration_state = "registered";
  host.onboarding_state = "completed";
  host.session_state = "connected";
  host.transport_mode = "out";
  host.execution_mode = "mixed";
  host.capabilities_json = json{{"storage_root", storage_root}}.dump();
  store.UpsertRegisteredHost(host);
}

naim::controller::HostRegistryEventSink TestEventSink() {
  return [](naim::ControllerStore& store,
            const std::string& event_type,
            const std::string& message,
            const json& payload,
            const std::string& node_name,
            const std::string& severity) {
    store.AppendEvent(naim::EventRecord{
        0,
        "",
        node_name,
        "",
        std::nullopt,
        std::nullopt,
        "host-registry",
        event_type,
        severity,
        message,
        payload.dump(),
        "",
    });
  };
}

void SeedObservation(
    naim::ControllerStore& store,
    const std::string& node_name,
    const std::string& storage_root,
    int gpu_count,
    std::uint64_t total_memory_bytes,
    std::uint64_t storage_total_bytes,
    std::uint64_t storage_free_bytes) {
  naim::HostObservation observation;
  observation.node_name = node_name;
  observation.status = naim::HostObservationStatus::Idle;
  observation.heartbeat_at = "2026-04-09 11:30:00";

  naim::GpuTelemetrySnapshot gpu;
  gpu.source = "test";
  gpu.collected_at = observation.heartbeat_at;
  for (int index = 0; index < gpu_count; ++index) {
    gpu.devices.push_back(naim::GpuDeviceTelemetry{
        std::to_string(index),
        24576,
        0,
        24576,
        0,
        0,
        false,
        {},
    });
  }
  observation.gpu_telemetry_json = naim::SerializeGpuTelemetryJson(gpu);

  naim::CpuTelemetrySnapshot cpu;
  cpu.source = "test";
  cpu.collected_at = observation.heartbeat_at;
  cpu.total_memory_bytes = total_memory_bytes;
  cpu.available_memory_bytes = total_memory_bytes;
  cpu.used_memory_bytes = 0;
  observation.cpu_telemetry_json = naim::SerializeCpuTelemetryJson(cpu);

  naim::DiskTelemetrySnapshot disk;
  disk.source = "test";
  disk.collected_at = observation.heartbeat_at;
  naim::DiskTelemetryRecord record;
  record.disk_name = "storage-root";
  record.node_name = node_name;
  record.mount_point = storage_root;
  record.total_bytes = storage_total_bytes;
  record.free_bytes = storage_free_bytes;
  record.used_bytes = storage_total_bytes > storage_free_bytes
                          ? storage_total_bytes - storage_free_bytes
                          : 0;
  disk.items.push_back(record);
  observation.disk_telemetry_json = naim::SerializeDiskTelemetryJson(disk);

  store.UpsertHostObservation(observation);
}

json LoadSingleHostPayload(
    const std::string& test_name,
    int gpu_count,
    std::uint64_t total_memory_bytes,
    std::uint64_t storage_total_bytes) {
  const std::string db_path = MakeTempDbPath(test_name);
  naim::ControllerStore store(db_path);
  store.Initialize();
  SeedHost(store, test_name, "/srv/" + test_name);
  SeedObservation(
      store,
      test_name,
      "/srv/" + test_name,
      gpu_count,
      total_memory_bytes,
      storage_total_bytes,
      storage_total_bytes / 2);

  const naim::controller::HostRegistryService service(
      db_path,
      [](naim::ControllerStore&,
         const std::string&,
         const std::string&,
         const json&,
         const std::string&,
         const std::string&) {});
  const json payload = service.BuildPayload(test_name);
  Expect(payload.at("items").size() == 1, "expected one host registry item");
  return payload.at("items").at(0);
}

void TestDerivesStorageRole() {
  constexpr std::uint64_t kGiB = 1024ULL * 1024ULL * 1024ULL;
  const json item = LoadSingleHostPayload(
      "storage-node",
      0,
      128ULL * kGiB,
      200ULL * kGiB);
  Expect(item.at("derived_role").get<std::string>() == "storage", "expected storage role");
  Expect(item.at("role_eligible").get<bool>(), "storage role should be eligible");
  Expect(
      item.at("role_reason").get<std::string>() == "eligible: no gpu, disk > 100 GB",
      "storage reason should not include a RAM threshold");
}

void TestDerivesWorkerRole() {
  constexpr std::uint64_t kGiB = 1024ULL * 1024ULL * 1024ULL;
  const json item = LoadSingleHostPayload(
      "worker-node",
      2,
      32ULL * kGiB,
      500ULL * kGiB);
  Expect(item.at("derived_role").get<std::string>() == "worker", "expected worker role");
  Expect(item.at("role_eligible").get<bool>(), "worker role should be eligible");
  Expect(
      item.at("role_reason").get<std::string>() ==
          "eligible: gpu >= 1, ram >= 32 GB, disk > 100 GB",
      "worker role should use 32 GB RAM threshold");
  Expect(
      item.at("storage_role_eligible").get<bool>(),
      "worker with storage capacity should also be storage-role eligible");
}

void TestDerivesIneligibleRole() {
  constexpr std::uint64_t kGiB = 1024ULL * 1024ULL * 1024ULL;
  const json item = LoadSingleHostPayload(
      "ineligible-node",
      1,
      16ULL * kGiB,
      200ULL * kGiB);
  Expect(item.at("derived_role").get<std::string>() == "ineligible", "expected ineligible role");
  Expect(
      item.at("role_reason").get<std::string>() ==
          "gpu present but ram < 32 GB",
      "expected ineligible reason for low worker RAM");
  Expect(!item.at("role_eligible").get<bool>(), "ineligible role should not be eligible");
  Expect(
      !item.at("storage_role_eligible").get<bool>(),
      "ineligible node should not be storage-role eligible");
}

void TestHostRegistryPrunesExpiredLanPeers() {
  const std::string db_path = MakeTempDbPath("registry-prunes-expired-lan-peers");
  naim::ControllerStore store(db_path);
  store.Initialize();
  SeedHost(store, "hpc1", "/srv/hpc1");
  SeedHost(store, "storage1", "/srv/storage1");
  const std::string now = naim::controller::ControllerTimeSupport::UtcNowSqlTimestamp();

  naim::HostPeerLinkRecord active_link;
  active_link.observer_node_name = "hpc1";
  active_link.peer_node_name = "storage1";
  active_link.peer_endpoint = "http://192.168.88.252:29999";
  active_link.seen_udp = true;
  active_link.tcp_reachable = true;
  active_link.last_seen_at = now;
  active_link.last_probe_at = now;
  store.UpsertHostPeerLink(active_link);

  naim::HostPeerLinkRecord expired_link;
  expired_link.observer_node_name = "hpc1";
  expired_link.peer_node_name = "sandbox-old";
  expired_link.peer_endpoint = "http://192.168.88.40:29999";
  expired_link.seen_udp = true;
  expired_link.tcp_reachable = true;
  expired_link.last_seen_at = "2000-01-01 00:00:00";
  expired_link.last_probe_at = "2000-01-01 00:00:00";
  store.UpsertHostPeerLink(expired_link);

  const naim::controller::HostRegistryService service(db_path, TestEventSink());
  const json payload = service.BuildPayload("hpc1");
  const auto& lan_peers = payload.at("items").at(0).at("lan_peers");
  Expect(lan_peers.size() == 1, "host registry should expose only fresh LAN peers");
  Expect(
      lan_peers.at(0).at("peer_node_name").get<std::string>() == "storage1",
      "host registry should keep the fresh storage peer");
  Expect(
      store.LoadHostPeerLinks(
               std::optional<std::string>("hpc1"),
               std::optional<std::string>("sandbox-old"))
          .empty(),
      "expired LAN peer should be pruned from the store");
}

void TestResetOnboardingIssuesNewKeyAndClearsIdentity() {
  const std::string db_path = MakeTempDbPath("reset-onboarding");
  naim::ControllerStore store(db_path);
  store.Initialize();

  naim::RegisteredHostRecord host;
  host.node_name = "reset-node";
  host.registration_state = "registered";
  host.onboarding_state = "completed";
  host.session_state = "disconnected";
  host.public_key_base64 = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=";
  host.controller_public_key_fingerprint = "old-controller-fingerprint";
  host.session_token = "old-session";
  host.session_expires_at = "2026-04-09 11:30:00";
  host.session_host_sequence = 7;
  host.session_controller_sequence = 9;
  host.capabilities_json = json{{"storage_root", "/srv/reset-node"}}.dump();
  host.derived_role = "worker";
  host.role_reason = "old role";
  host.storage_role_enabled = true;
  host.last_inventory_scan_at = "2026-04-09 11:30:00";
  host.last_session_at = "2026-04-09 11:30:00";
  host.last_heartbeat_at = "2026-04-09 11:30:00";
  store.UpsertRegisteredHost(host);

  const naim::controller::HostRegistryService service(db_path, TestEventSink());
  const json payload = service.ResetHostOnboardingPayload(
      "reset-node",
      std::make_optional<std::string>("operator requested reprovision"));

  Expect(payload.at("node_name").get<std::string>() == "reset-node", "reset node mismatch");
  Expect(
      payload.at("registration_state").get<std::string>() == "provisioned",
      "reset should provision host");
  Expect(
      payload.at("onboarding_state").get<std::string>() == "pending",
      "reset should create pending onboarding");
  Expect(
      !payload.at("onboarding_key").get<std::string>().empty(),
      "reset should return onboarding key");

  const auto updated = store.LoadRegisteredHost("reset-node");
  Expect(updated.has_value(), "reset host should still exist");
  Expect(updated->registration_state == "provisioned", "updated registration mismatch");
  Expect(updated->onboarding_state == "pending", "updated onboarding mismatch");
  Expect(!updated->onboarding_key_hash.empty(), "updated host should store key hash");
  Expect(updated->public_key_base64.empty(), "reset should clear public key");
  Expect(updated->session_state == "disconnected", "reset session state mismatch");
  Expect(updated->session_token.empty(), "reset should clear session token");
  Expect(updated->session_host_sequence == 0, "reset should clear host sequence");
  Expect(updated->session_controller_sequence == 0, "reset should clear controller sequence");
  Expect(updated->capabilities_json == "{}", "reset should clear capabilities");
  Expect(updated->derived_role == "ineligible", "reset should clear derived role");
  Expect(!updated->storage_role_enabled, "reset should disable storage role");

  const auto events = store.LoadEvents(std::nullopt, "reset-node", std::nullopt, "host-registry");
  Expect(events.size() == 1, "reset should append one host registry event");
  Expect(events.at(0).event_type == "reset-onboarding", "reset event type mismatch");
  Expect(events.at(0).severity == "warning", "reset event severity mismatch");
}

void TestSetHostStorageRole() {
  constexpr std::uint64_t kGiB = 1024ULL * 1024ULL * 1024ULL;
  const std::string db_path = MakeTempDbPath("set-storage-role");
  naim::ControllerStore store(db_path);
  store.Initialize();
  SeedHost(store, "storage-toggle-node", "/srv/storage-toggle-node");
  SeedObservation(
      store,
      "storage-toggle-node",
      "/srv/storage-toggle-node",
      4,
      128ULL * kGiB,
      500ULL * kGiB,
      400ULL * kGiB);

  const naim::controller::HostRegistryService service(db_path, TestEventSink());
  const json enabled_payload = service.SetHostStorageRolePayload(
      "storage-toggle-node",
      true,
      std::make_optional<std::string>("operator enabled storage role"));
  Expect(
      enabled_payload.at("storage_role_enabled").get<bool>(),
      "storage role should be enabled");
  auto updated = store.LoadRegisteredHost("storage-toggle-node");
  Expect(updated.has_value(), "storage role host should still exist");
  Expect(updated->storage_role_enabled, "storage role flag should persist");
  const json listed =
      service.BuildPayload(std::make_optional<std::string>("storage-toggle-node"))
          .at("items")
          .at(0);
  Expect(
      listed.at("storage_role_enabled").get<bool>(),
      "host listing should expose enabled storage role");
  Expect(
      listed.at("storage_role_eligible").get<bool>(),
      "host listing should expose storage eligibility");

  const json disabled_payload = service.SetHostStorageRolePayload(
      "storage-toggle-node",
      false,
      std::nullopt);
  Expect(
      !disabled_payload.at("storage_role_enabled").get<bool>(),
      "storage role should be disabled");
}

void TestResetOnboardingRejectsConnectedHost() {
  const std::string db_path = MakeTempDbPath("reset-connected");
  naim::ControllerStore store(db_path);
  store.Initialize();
  SeedHost(store, "connected-node", "/srv/connected-node");

  const naim::controller::HostRegistryService service(db_path, TestEventSink());
  bool threw = false;
  try {
    (void)service.ResetHostOnboardingPayload("connected-node", std::nullopt);
  } catch (const std::exception& error) {
    threw = std::string(error.what()).find("connected") != std::string::npos;
  }
  Expect(threw, "reset should reject connected host");
}

void TestResetOnboardingRejectsActiveAssignments() {
  const std::string db_path = MakeTempDbPath("reset-active-assignment");
  naim::ControllerStore store(db_path);
  store.Initialize();

  naim::RegisteredHostRecord host;
  host.node_name = "assigned-node";
  host.registration_state = "registered";
  host.onboarding_state = "completed";
  host.session_state = "disconnected";
  store.UpsertRegisteredHost(host);

  naim::HostAssignment assignment;
  assignment.node_name = "assigned-node";
  assignment.plane_name = "plane-a";
  assignment.desired_generation = 1;
  assignment.assignment_type = "apply-desired-state";
  assignment.desired_state_json = "{}";
  assignment.status = naim::HostAssignmentStatus::Pending;
  store.EnqueueHostAssignments({assignment});

  const naim::controller::HostRegistryService service(db_path, TestEventSink());
  bool threw = false;
  try {
    (void)service.ResetHostOnboardingPayload("assigned-node", std::nullopt);
  } catch (const std::exception& error) {
    threw = std::string(error.what()).find("assignments") != std::string::npos;
  }
  Expect(threw, "reset should reject active host assignments");
}

void TestNotifyConnectedHostsOfRelease() {
  const std::string db_path = MakeTempDbPath("notify-connected-hosts-of-release");
  const fs::path manifest_path =
      fs::temp_directory_path() / "naim-host-registry-tests" / "notify-release-manifest.json";
  fs::create_directories(manifest_path.parent_path());
  {
    std::ofstream out(manifest_path);
    out << R"({
  "registry": "chainzano.com",
  "project": "naim",
  "tag": "abc123",
  "images": {
    "hostd": "chainzano.com/naim/hostd@sha256:deadbeef"
  }
})";
  }

  naim::ControllerStore store(db_path);
  store.Initialize();
  SeedHost(store, "connected-a", "/srv/connected-a");
  SeedHost(store, "connected-b", "/srv/connected-b");
  naim::RegisteredHostRecord disconnected;
  disconnected.node_name = "disconnected-c";
  disconnected.registration_state = "registered";
  disconnected.onboarding_state = "completed";
  disconnected.session_state = "disconnected";
  store.UpsertRegisteredHost(disconnected);

  const naim::controller::HostRegistryService service(db_path, TestEventSink());
  const json payload =
      service.NotifyConnectedHostsOfReleasePayload(manifest_path.string(), std::nullopt);

  Expect(payload.at("release_tag").get<std::string>() == "abc123", "release tag mismatch");
  Expect(payload.at("targeted_count").get<int>() == 2, "expected two connected hosts");

  const auto connected_a_assignments =
      store.LoadHostAssignments("connected-a", naim::HostAssignmentStatus::Pending);
  Expect(connected_a_assignments.size() == 1, "connected host should receive one assignment");
  Expect(
      connected_a_assignments.front().assignment_type == "hostd-self-update",
      "assignment type mismatch");
  const json connected_a_payload =
      json::parse(connected_a_assignments.front().desired_state_json);
  Expect(
      connected_a_payload.at("hostd_image").get<std::string>() ==
          "chainzano.com/naim/hostd@sha256:deadbeef",
      "hostd image mismatch");

  const auto disconnected_assignments =
      store.LoadHostAssignments("disconnected-c", naim::HostAssignmentStatus::Pending);
  Expect(disconnected_assignments.empty(), "disconnected host should not receive rollout");
}

}  // namespace

int main() {
  TestDerivesStorageRole();
  TestDerivesWorkerRole();
  TestDerivesIneligibleRole();
  TestHostRegistryPrunesExpiredLanPeers();
  TestSetHostStorageRole();
  TestResetOnboardingIssuesNewKeyAndClearsIdentity();
  TestResetOnboardingRejectsConnectedHost();
  TestResetOnboardingRejectsActiveAssignments();
  TestNotifyConnectedHostsOfRelease();
  std::cout << "host registry service tests passed\n";
  return 0;
}
