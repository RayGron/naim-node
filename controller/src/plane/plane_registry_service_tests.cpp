#include <filesystem>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <string>

#include <nlohmann/json.hpp>

#include "plane/plane_lifecycle_support.h"
#include "plane/plane_registry_query_support.h"
#include "plane/plane_registry_service.h"

namespace fs = std::filesystem;
using nlohmann::json;

namespace {

void Expect(bool condition, const std::string& message) {
  if (!condition) {
    throw std::runtime_error(message);
  }
}

std::string MakeTempDbPath(const std::string& test_name) {
  const fs::path root = fs::temp_directory_path() / "naim-plane-registry-tests" / test_name;
  std::error_code error;
  fs::remove_all(root, error);
  fs::create_directories(root);
  return (root / "controller.sqlite").string();
}

json FindPlaneItem(const json& payload, const std::string& plane_name) {
  for (const auto& item : payload.at("items")) {
    if (item.value("name", std::string()) == plane_name) {
      return item;
    }
  }
  throw std::runtime_error("missing plane item " + plane_name);
}

class TestPlaneLifecycleSupport final : public naim::controller::PlaneLifecycleSupport {
 public:
  void PrepareDesiredState(
      naim::ControllerStore&,
      naim::DesiredState*) const override {}

  void AppendPlaneEvent(
      naim::ControllerStore&,
      const std::string&,
      const std::string&,
      const nlohmann::json&,
      const std::string&) const override {}

  bool CanFinalizeDeletedPlane(
      naim::ControllerStore&,
      const std::string&) const override {
    return false;
  }

  std::optional<naim::HostAssignment> FindLatestHostAssignmentForPlane(
      const std::vector<naim::HostAssignment>&,
      const std::string&) const override {
    return std::nullopt;
  }

  std::vector<naim::HostAssignment> BuildStartAssignments(
      const naim::DesiredState&,
      const std::string&,
      int,
      const std::vector<naim::NodeAvailabilityOverride>&,
      const std::vector<naim::HostObservation>&,
      const naim::SchedulingPolicyReport&) const override {
    return {};
  }

  std::vector<naim::HostAssignment> BuildStopAssignments(
      const naim::DesiredState&,
      int,
      const std::string&,
      const std::vector<naim::NodeAvailabilityOverride>&) const override {
    return {};
  }

  std::vector<naim::HostAssignment> BuildDeleteAssignments(
      const naim::DesiredState&,
      int,
      const std::string&) const override {
    return {};
  }

  std::string DefaultArtifactsRoot() const override { return "/tmp/artifacts"; }
};

class TestPlaneRegistryQuerySupport final
    : public naim::controller::PlaneRegistryQuerySupport {
 public:
  std::vector<naim::HostObservation> FilterHostObservationsForPlane(
      const std::vector<naim::HostObservation>& observations,
      const std::string&) const override {
    return observations;
  }

  int ComputeEffectiveAppliedGeneration(
      const naim::PlaneRecord& plane,
      const std::optional<naim::DesiredState>&,
      const std::optional<int>&,
      const std::vector<naim::HostObservation>&) const override {
    return plane.applied_generation;
  }

  std::map<std::string, naim::HostAssignment> BuildLatestAssignmentsByNode(
      const std::vector<naim::HostAssignment>& assignments) const override {
    std::map<std::string, naim::HostAssignment> result;
    for (const auto& assignment : assignments) {
      auto it = result.find(assignment.node_name);
      if (it == result.end() || assignment.id >= it->second.id) {
        result[assignment.node_name] = assignment;
      }
    }
    return result;
  }
};

void TestExecutionNodeRegistryPayload() {
  const auto db_path = MakeTempDbPath("execution-node");
  naim::ControllerStore store(db_path);
  store.Initialize();

  naim::DesiredState state;
  state.plane_name = "placement-plane";
  state.plane_mode = naim::PlaneMode::Llm;
  state.placement_target = std::string("node:worker-a");
  state.app_host = naim::ExternalAppHostConfig{
      "10.0.0.15",
      std::optional<std::string>("/tmp/id_ed25519"),
      std::nullopt,
      std::nullopt,
  };
  state.skills = naim::SkillsSettings{true, {"skill-a"}};
  naim::BootstrapModelSpec model;
  model.model_id = "catalog://qwen-placement";
  model.served_model_name = std::string("qwen-placement");
  model.materialization_mode = "reference";
  model.local_path = std::string("/models/qwen-placement");
  state.bootstrap_model = model;
  state.inference.runtime_engine = "llama.cpp";
  state.inference.distributed_backend = "llama_rpc";

  naim::NodeInventory node;
  node.name = "worker-a";
  state.nodes.push_back(node);

  naim::InstanceSpec infer;
  infer.name = "infer-placement-plane";
  infer.role = naim::InstanceRole::Infer;
  infer.plane_name = state.plane_name;
  infer.node_name = "worker-a";
  state.instances.push_back(infer);

  naim::InstanceSpec worker;
  worker.name = "worker-placement-plane";
  worker.role = naim::InstanceRole::Worker;
  worker.plane_name = state.plane_name;
  worker.node_name = "worker-a";
  worker.gpu_fraction = 1.0;
  state.instances.push_back(worker);

  naim::InstanceSpec app;
  app.name = "app-placement-plane";
  app.role = naim::InstanceRole::App;
  app.plane_name = state.plane_name;
  app.node_name = "worker-a";
  app.image = "example/app:dev";
  state.instances.push_back(app);

  naim::InstanceSpec skills;
  skills.name = "skills-placement-plane";
  skills.role = naim::InstanceRole::Skills;
  skills.plane_name = state.plane_name;
  skills.node_name = "worker-a";
  skills.image = "example/skills:dev";
  state.instances.push_back(skills);

  store.ReplaceDesiredState(state);

  const auto lifecycle_support =
      std::make_shared<TestPlaneLifecycleSupport>();
  const auto query_support =
      std::make_shared<TestPlaneRegistryQuerySupport>();
  const naim::controller::PlaneRegistryService service(
      lifecycle_support,
      query_support);

  const auto payload = service.BuildPlanesPayload(db_path);
  const auto item = FindPlaneItem(payload, "placement-plane");
  const auto& placement = item.at("placement");
  Expect(
      placement.at("mode").get<std::string>() == "execution-node",
      "registry payload should expose execution-node mode");
  Expect(
      placement.at("execution_node").get<std::string>() == "worker-a",
      "registry payload should expose selected execution node");
  Expect(
      placement.at("app_host").at("auth_mode").get<std::string>() == "ssh-key",
      "registry payload should expose app host auth mode");
  Expect(
      placement.at("service_targets").size() >= 4,
      "registry payload should expose service targets");
  std::cout << "ok: execution-node-registry-payload" << '\n';
}

void TestLegacyCompatibilityRegistryPayload() {
  const auto db_path = MakeTempDbPath("legacy-compatibility");
  naim::ControllerStore store(db_path);
  store.Initialize();

  naim::DesiredState state;
  state.plane_name = "legacy-plane";
  state.plane_mode = naim::PlaneMode::Compute;

  naim::NodeInventory controller_node;
  controller_node.name = "controller-node";
  state.nodes.push_back(controller_node);
  naim::NodeInventory worker_node;
  worker_node.name = "worker-node-a";
  state.nodes.push_back(worker_node);

  naim::InstanceSpec infer;
  infer.name = "infer-legacy-plane";
  infer.role = naim::InstanceRole::Infer;
  infer.plane_name = state.plane_name;
  infer.node_name = "controller-node";
  state.instances.push_back(infer);

  naim::InstanceSpec worker;
  worker.name = "worker-legacy-plane";
  worker.role = naim::InstanceRole::Worker;
  worker.plane_name = state.plane_name;
  worker.node_name = "worker-node-a";
  worker.gpu_fraction = 1.0;
  state.instances.push_back(worker);

  store.ReplaceDesiredState(state);

  const auto lifecycle_support =
      std::make_shared<TestPlaneLifecycleSupport>();
  const auto query_support =
      std::make_shared<TestPlaneRegistryQuerySupport>();
  const naim::controller::PlaneRegistryService service(
      lifecycle_support,
      query_support);

  const auto payload = service.BuildPlanesPayload(db_path);
  const auto item = FindPlaneItem(payload, "legacy-plane");
  const auto& placement = item.at("placement");
  Expect(
      placement.at("mode").get<std::string>() == "legacy-topology-compatibility",
      "registry payload should expose legacy compatibility mode");
  Expect(
      placement.at("execution_node").is_null(),
      "registry payload should not invent an execution node for legacy state");
  std::cout << "ok: legacy-compatibility-registry-payload" << '\n';
}

}  // namespace

int main() {
  try {
    TestExecutionNodeRegistryPayload();
    TestLegacyCompatibilityRegistryPayload();
    return 0;
  } catch (const std::exception& error) {
    std::cerr << "plane_registry_service_tests failed: " << error.what() << '\n';
    return 1;
  }
}
