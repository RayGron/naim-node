#include <filesystem>
#include <iostream>
#include <stdexcept>
#include <string>

#include <nlohmann/json.hpp>

#include "comet/runtime/runtime_status.h"
#include "comet/state/sqlite_store.h"
#include "comet/state/state_json.h"
#include "infra/controller_runtime_support_service.h"
#include "read_model/read_model_service.h"

namespace fs = std::filesystem;
using nlohmann::json;

namespace {

void Expect(bool condition, const std::string& message) {
  if (!condition) {
    throw std::runtime_error(message);
  }
}

std::string MakeTempDbPath(const std::string& test_name) {
  const fs::path root = fs::temp_directory_path() / "comet-read-model-tests" / test_name;
  std::error_code error;
  fs::remove_all(root, error);
  fs::create_directories(root);
  return (root / "controller.sqlite").string();
}

comet::DesiredState BuildMixedObservedState() {
  comet::DesiredState state;
  state.plane_name = "";

  comet::NodeInventory node;
  node.name = "local-hostd";
  node.gpu_devices = {"0", "1", "2"};
  state.nodes.push_back(node);

  state.disks.push_back(comet::DiskSpec{
      "lt-shared",
      comet::DiskKind::PlaneShared,
      "lt-cypher-ai",
      "lt-cypher-ai",
      "local-hostd",
      "/lt/shared",
      "/comet/shared",
      100,
  });
  state.disks.push_back(comet::DiskSpec{
      "maglev-shared",
      comet::DiskKind::PlaneShared,
      "maglev",
      "maglev",
      "local-hostd",
      "/maglev/shared",
      "/comet/shared",
      100,
  });

  const auto push_instance =
      [&](const std::string& name,
          comet::InstanceRole role,
          const std::string& plane_name,
          const std::optional<std::string>& gpu_device) {
        comet::InstanceSpec instance;
        instance.name = name;
        instance.role = role;
        instance.plane_name = plane_name;
        instance.node_name = "local-hostd";
        instance.image = "comet/runtime:dev";
        instance.command = "sleep infinity";
        instance.gpu_device = gpu_device;
        state.instances.push_back(std::move(instance));
      };

  push_instance("infer-lt-cypher-ai", comet::InstanceRole::Infer, "lt-cypher-ai", std::nullopt);
  push_instance("worker-lt-cypher-ai", comet::InstanceRole::Worker, "lt-cypher-ai", "0");
  push_instance("infer-maglev", comet::InstanceRole::Infer, "maglev", std::nullopt);
  push_instance("worker-maglev-a", comet::InstanceRole::Worker, "maglev", "1");
  push_instance("worker-maglev-b", comet::InstanceRole::Worker, "maglev", "2");

  const auto make_worker_member =
      [](const std::string& name,
         const std::string& infer_instance_name,
         const std::string& gpu_device) {
        comet::WorkerGroupMemberSpec member;
        member.name = name;
        member.infer_instance_name = infer_instance_name;
        member.node_name = "local-hostd";
        member.gpu_device = gpu_device;
        return member;
      };
  const auto make_runtime_gpu_node =
      [](const std::string& name, const std::string& gpu_device) {
        comet::RuntimeGpuNode node;
        node.name = name;
        node.node_name = "local-hostd";
        node.gpu_device = gpu_device;
        return node;
      };

  state.worker_group.infer_instance_name = "infer-maglev";
  state.worker_group.members = {
      make_worker_member("worker-lt-cypher-ai", "infer-lt-cypher-ai", "0"),
      make_worker_member("worker-maglev-a", "infer-maglev", "1"),
      make_worker_member("worker-maglev-b", "infer-maglev", "2"),
  };
  state.runtime_gpu_nodes = {
      make_runtime_gpu_node("worker-lt-cypher-ai", "0"),
      make_runtime_gpu_node("worker-maglev-a", "1"),
      make_runtime_gpu_node("worker-maglev-b", "2"),
  };

  return state;
}

void TestPlaneScopedHostObservationsPayloadFiltersForeignEntities() {
  const std::string db_path = MakeTempDbPath("plane-scoped-observations");
  comet::ControllerStore store(db_path);
  store.Initialize();

  const comet::controller::ControllerRuntimeSupportService runtime_support_service;
  const std::string now = runtime_support_service.UtcNowSqlTimestamp();

  comet::HostObservation observation;
  observation.node_name = "local-hostd";
  observation.status = comet::HostObservationStatus::Applied;
  observation.heartbeat_at = now;
  observation.observed_state_json =
      comet::SerializeDesiredStateJson(BuildMixedObservedState());

  comet::RuntimeStatus lt_runtime_status;
  lt_runtime_status.plane_name = "lt-cypher-ai";
  lt_runtime_status.instance_name = "infer-lt-cypher-ai";
  lt_runtime_status.runtime_backend = "llama.cpp";
  lt_runtime_status.runtime_phase = "running";
  lt_runtime_status.launch_ready = true;
  observation.runtime_status_json = comet::SerializeRuntimeStatusJson(lt_runtime_status);

  observation.instance_runtime_json = comet::SerializeRuntimeStatusListJson({
      comet::RuntimeProcessStatus{
          "infer-lt-cypher-ai", "infer", "local-hostd", "", "", "running", now, now, 101, 0,
          true},
      comet::RuntimeProcessStatus{
          "worker-lt-cypher-ai", "worker", "local-hostd", "", "0", "running", now, now, 102, 0,
          true},
      comet::RuntimeProcessStatus{
          "infer-maglev", "infer", "local-hostd", "", "", "running", now, now, 201, 0, true},
      comet::RuntimeProcessStatus{
          "worker-maglev-a", "worker", "local-hostd", "", "1", "running", now, now, 202, 0,
          true},
      comet::RuntimeProcessStatus{
          "worker-maglev-b", "worker", "local-hostd", "", "2", "running", now, now, 203, 0,
          true},
  });

  comet::GpuTelemetrySnapshot gpu_telemetry;
  gpu_telemetry.source = "nvidia-smi";
  gpu_telemetry.collected_at = now;
  gpu_telemetry.devices = {
      comet::GpuDeviceTelemetry{
          "0", 24576, 8192, 16384, 10, 0, false,
          {comet::GpuProcessTelemetry{102, 8192, "worker-lt-cypher-ai"}}},
      comet::GpuDeviceTelemetry{
          "1", 24576, 8192, 16384, 20, 0, false,
          {comet::GpuProcessTelemetry{202, 6144, "worker-maglev-a"},
           comet::GpuProcessTelemetry{999, 2048, "unknown"}}},
  };
  observation.gpu_telemetry_json = comet::SerializeGpuTelemetryJson(gpu_telemetry);

  comet::DiskTelemetrySnapshot disk_telemetry;
  disk_telemetry.source = "hostd";
  disk_telemetry.collected_at = now;
  const auto make_disk_telemetry_record =
      [](const std::string& disk_name,
         const std::string& plane_name,
         const std::string& mount_point) {
        comet::DiskTelemetryRecord record;
        record.disk_name = disk_name;
        record.plane_name = plane_name;
        record.node_name = "local-hostd";
        record.mount_point = mount_point;
        record.runtime_state = "mounted";
        record.health = "ok";
        return record;
      };
  disk_telemetry.items = {
      make_disk_telemetry_record("lt-shared", "lt-cypher-ai", "/lt/shared"),
      make_disk_telemetry_record("maglev-shared", "maglev", "/maglev/shared"),
  };
  observation.disk_telemetry_json = comet::SerializeDiskTelemetryJson(disk_telemetry);

  store.UpsertHostObservation(observation);

  const comet::controller::ReadModelService service(runtime_support_service);
  const json payload =
      service.BuildHostObservationsPayload(db_path, std::nullopt, "maglev", 300);

  Expect(
      payload.at("observations").size() == 1,
      "expected one observation in plane-scoped payload");
  const json& item = payload.at("observations").at(0);

  Expect(
      item.at("runtime_status").at("available").get<bool>() == false,
      "foreign runtime_status_json must not leak into plane-scoped payload");

  const json observed_state = item.at("observed_state");
  Expect(observed_state.is_object(), "plane-scoped observed_state should be materialized");
  Expect(
      observed_state.at("plane_name").get<std::string>() == "maglev",
      "observed_state should be rewritten to the selected plane");
  Expect(
      observed_state.at("instances").size() == 3,
      "observed_state should contain only maglev instances");
  for (const auto& instance : observed_state.at("instances")) {
    Expect(
        instance.at("plane_name").get<std::string>() == "maglev",
        "foreign instances must be removed from observed_state");
  }

  const json instance_runtimes = item.at("instance_runtimes").at("items");
  Expect(instance_runtimes.size() == 3, "expected only maglev runtime processes");
  for (const auto& runtime_item : instance_runtimes) {
    const std::string instance_name = runtime_item.at("instance_name").get<std::string>();
    Expect(
        instance_name.find("lt-cypher-ai") == std::string::npos,
        "foreign runtime process leaked into plane-scoped payload");
  }

  const json gpu_devices = item.at("gpu_telemetry").at("devices");
  Expect(gpu_devices.size() == 2, "gpu device list should remain node-scoped");
  for (const auto& device : gpu_devices) {
    for (const auto& process : device.at("processes")) {
      const std::string instance_name = process.at("instance_name").get<std::string>();
      Expect(
          instance_name == "unknown" || instance_name.find("lt-cypher-ai") == std::string::npos,
          "foreign gpu process leaked into plane-scoped payload");
    }
  }

  const json disk_items = item.at("disk_telemetry").at("items");
  Expect(disk_items.size() == 1, "expected only plane disk telemetry");
  Expect(
      disk_items.at(0).at("plane_name").get<std::string>() == "maglev",
      "foreign disk telemetry leaked into plane-scoped payload");

  std::cout << "ok: plane-scoped-host-observations-filter-foreign-entities" << '\n';
}

}  // namespace

int main() {
  try {
    TestPlaneScopedHostObservationsPayloadFiltersForeignEntities();
    return 0;
  } catch (const std::exception& error) {
    std::cerr << "read_model_service_tests failed: " << error.what() << '\n';
    return 1;
  }
}
