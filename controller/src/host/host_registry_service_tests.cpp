#include <filesystem>
#include <iostream>
#include <optional>
#include <stdexcept>
#include <string>

#include <nlohmann/json.hpp>

#include "naim/runtime/runtime_status.h"
#include "naim/state/sqlite_store.h"
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
      16ULL * kGiB,
      200ULL * kGiB);
  Expect(item.at("derived_role").get<std::string>() == "storage", "expected storage role");
  Expect(item.at("role_eligible").get<bool>(), "storage role should be eligible");
}

void TestDerivesWorkerRole() {
  constexpr std::uint64_t kGiB = 1024ULL * 1024ULL * 1024ULL;
  const json item = LoadSingleHostPayload(
      "worker-node",
      2,
      128ULL * kGiB,
      500ULL * kGiB);
  Expect(item.at("derived_role").get<std::string>() == "worker", "expected worker role");
  Expect(item.at("role_eligible").get<bool>(), "worker role should be eligible");
}

void TestDerivesIneligibleRole() {
  constexpr std::uint64_t kGiB = 1024ULL * 1024ULL * 1024ULL;
  const json item = LoadSingleHostPayload(
      "ineligible-node",
      0,
      48ULL * kGiB,
      200ULL * kGiB);
  Expect(item.at("derived_role").get<std::string>() == "ineligible", "expected ineligible role");
  Expect(
      item.at("role_reason").get<std::string>() ==
          "no gpu and ram outside storage threshold",
      "expected ineligible reason for mid-range RAM");
  Expect(!item.at("role_eligible").get<bool>(), "ineligible role should not be eligible");
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

}  // namespace

int main() {
  TestDerivesStorageRole();
  TestDerivesWorkerRole();
  TestDerivesIneligibleRole();
  TestSetHostStorageRole();
  TestResetOnboardingIssuesNewKeyAndClearsIdentity();
  TestResetOnboardingRejectsConnectedHost();
  TestResetOnboardingRejectsActiveAssignments();
  std::cout << "host registry service tests passed\n";
  return 0;
}
