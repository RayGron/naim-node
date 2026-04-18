#include <filesystem>
#include <iostream>
#include <optional>
#include <stdexcept>
#include <string>

#include "naim/state/sqlite_store.h"
#include "host/host_assignment_reconciliation_service.h"
#include "plane/controller_state_service.h"
#include "plane/plane_lifecycle_support.h"
#include "plane/plane_deletion_support.h"
#include "plane/plane_service.h"
#include "plane/plane_state_presentation_support.h"

namespace fs = std::filesystem;

namespace {

void Expect(bool condition, const std::string& message) {
  if (!condition) {
    throw std::runtime_error(message);
  }
}

naim::DesiredState BuildDesiredState(
    const std::string& plane_name,
    const std::vector<std::string>& node_names = {}) {
  naim::DesiredState state;
  state.plane_name = plane_name;
  state.plane_mode = naim::PlaneMode::Llm;
  state.control_root = "/tmp/" + plane_name;
  for (const auto& node_name : node_names) {
    naim::NodeInventory node;
    node.name = node_name;
    state.nodes.push_back(node);
  }
  return state;
}

naim::HostAssignment BuildHostAssignment(
    const std::string& plane_name,
    const std::string& node_name,
    int desired_generation,
    naim::HostAssignmentStatus status,
    const std::string& assignment_type = "apply-node-state") {
  naim::HostAssignment assignment;
  assignment.node_name = node_name;
  assignment.plane_name = plane_name;
  assignment.desired_generation = desired_generation;
  assignment.assignment_type = assignment_type;
  assignment.desired_state_json = "{}";
  assignment.artifacts_root = "/tmp/artifacts";
  assignment.status = status;
  assignment.attempt_count = status == naim::HostAssignmentStatus::Claimed ? 1 : 0;
  return assignment;
}

naim::HostObservation BuildHostObservation(
    const std::string& plane_name,
    const std::string& node_name,
    int applied_generation,
    naim::HostObservationStatus status = naim::HostObservationStatus::Applied,
    std::optional<int> last_assignment_id = std::nullopt) {
  naim::HostObservation observation;
  observation.node_name = node_name;
  observation.plane_name = plane_name;
  observation.applied_generation = applied_generation;
  observation.last_assignment_id = last_assignment_id;
  observation.status = status;
  return observation;
}

class TestPlaneStatePresentationSupport final
    : public naim::controller::PlaneStatePresentationSupport {
 public:
  std::string FormatTimestamp(const std::string& value) const override { return value; }

  void PrintStateSummary(const naim::DesiredState&) const override {}
};

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
    return true;
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

naim::controller::PlaneService BuildPlaneService(const std::string& db_path) {
  auto state_presentation_support =
      std::make_shared<TestPlaneStatePresentationSupport>();
  auto lifecycle_support = std::make_shared<TestPlaneLifecycleSupport>();
  return naim::controller::PlaneService(
      db_path,
      std::move(state_presentation_support),
      std::move(lifecycle_support));
}

}  // namespace

int main() {
  try {
    const fs::path db_path = fs::temp_directory_path() / "naim-plane-deletion-tests.sqlite";
    std::error_code error;
    fs::remove(db_path, error);

    {
      naim::ControllerStore store(db_path.string());
      store.Initialize();
      store.ReplaceDesiredState(BuildDesiredState("plane-a"), 7);
      Expect(store.UpdatePlaneState("plane-a", "deleting"), "plane-a should enter deleting");

      naim::controller::ControllerStateService state_service(
          naim::controller::ControllerStateService::Deps{
              [](naim::ControllerStore&, const std::string&) { return true; },
              [](naim::ControllerStore&,
                 const std::string&,
                 const std::string&,
                 const std::string&,
                 const nlohmann::json&,
                 const std::string&) {},
          });
      const auto payload = state_service.BuildPayload(db_path.string(), std::nullopt);
      Expect(payload.at("planes").is_array(), "planes payload should be an array");
      Expect(payload.at("planes").empty(), "deleting plane should be finalized on state read");
    }

    {
      naim::ControllerStore store(db_path.string());
      store.Initialize();
      store.ReplaceDesiredState(BuildDesiredState("plane-b"), 8);
      Expect(store.UpdatePlaneState("plane-b", "deleting"), "plane-b should enter deleting");

      auto plane_service = BuildPlaneService(db_path.string());
      bool threw_not_found = false;
      try {
        (void)plane_service.ShowPlane("plane-b");
      } catch (const std::exception& ex) {
        threw_not_found = std::string(ex.what()).find("not found") != std::string::npos;
      }
      Expect(threw_not_found, "show-plane should finalize deleted plane before reading it");
      Expect(!store.LoadPlane("plane-b").has_value(), "plane-b should be removed from store");
    }

    {
      naim::ControllerStore store(db_path.string());
      store.Initialize();
      const naim::controller::HostAssignmentReconciliationService reconciliation_service;

      store.ReplaceDesiredState(BuildDesiredState("plane-c", {"node-c"}), 4);
      Expect(
          store.UpdatePlaneAppliedGeneration("plane-c", 4),
          "plane-c applied generation should update");
      store.ReplaceHostAssignments(
          {BuildHostAssignment(
              "plane-c",
              "node-c",
              4,
              naim::HostAssignmentStatus::Claimed)});
      const auto inserted_assignments =
          store.LoadHostAssignments(std::nullopt, std::nullopt, "plane-c");
      Expect(inserted_assignments.size() == 1, "plane-c should have one inserted assignment");
      store.UpsertHostObservation(BuildHostObservation(
          "plane-c",
          "node-c",
          4,
          naim::HostObservationStatus::Applied,
          inserted_assignments.front().id));

      const auto result = reconciliation_service.Reconcile(store, "plane-c");
      const auto assignment = store.LoadHostAssignments(
          std::nullopt, std::nullopt, "plane-c");
      Expect(result.applied == 1, "controller should mark converged claimed assignment applied");
      Expect(result.superseded == 0, "controller should not supersede converged assignment");
      Expect(assignment.size() == 1, "plane-c should have one assignment");
      Expect(
          assignment.front().status == naim::HostAssignmentStatus::Applied,
          "plane-c claimed assignment should become applied");
    }

    {
      naim::ControllerStore store(db_path.string());
      store.Initialize();
      const naim::controller::HostAssignmentReconciliationService reconciliation_service;

      store.ReplaceDesiredState(BuildDesiredState("plane-c-foreign", {"node-c-foreign"}), 2);
      Expect(
          store.UpdatePlaneAppliedGeneration("plane-c-foreign", 2),
          "plane-c-foreign applied generation should update");
      store.ReplaceHostAssignments(
          {BuildHostAssignment(
              "plane-c-foreign",
              "node-c-foreign",
              2,
              naim::HostAssignmentStatus::Claimed)});
      const auto inserted_assignments =
          store.LoadHostAssignments(std::nullopt, std::nullopt, "plane-c-foreign");
      Expect(inserted_assignments.size() == 1, "plane-c-foreign should have one assignment");
      store.UpsertHostObservation(BuildHostObservation(
          "other-plane",
          "node-c-foreign",
          99,
          naim::HostObservationStatus::Applied,
          inserted_assignments.front().id));

      const auto result = reconciliation_service.Reconcile(store, "plane-c-foreign");
      const auto assignment = store.LoadHostAssignments(
          std::nullopt, std::nullopt, "plane-c-foreign");
      Expect(
          result.applied == 0,
          "controller should not use another plane observation for claimed assignment");
      Expect(assignment.size() == 1, "plane-c-foreign should still have one assignment");
      Expect(
          assignment.front().status == naim::HostAssignmentStatus::Claimed,
          "foreign-observation claimed assignment should remain claimed");
    }

    {
      naim::ControllerStore store(db_path.string());
      store.Initialize();
      const naim::controller::HostAssignmentReconciliationService reconciliation_service;

      store.ReplaceDesiredState(BuildDesiredState("plane-c-no-assignment-id", {"node-c-no-id"}), 2);
      Expect(
          store.UpdatePlaneAppliedGeneration("plane-c-no-assignment-id", 2),
          "plane-c-no-assignment-id applied generation should update");
      store.ReplaceHostAssignments(
          {BuildHostAssignment(
              "plane-c-no-assignment-id",
              "node-c-no-id",
              2,
              naim::HostAssignmentStatus::Claimed)});
      store.UpsertHostObservation(BuildHostObservation(
          "plane-c-no-assignment-id",
          "node-c-no-id",
          2));

      const auto result = reconciliation_service.Reconcile(store, "plane-c-no-assignment-id");
      const auto assignment = store.LoadHostAssignments(
          std::nullopt, std::nullopt, "plane-c-no-assignment-id");
      Expect(
          result.applied == 0,
          "controller should require a matching last assignment id before reconciliation");
      Expect(assignment.size() == 1, "plane-c-no-assignment-id should still have one assignment");
      Expect(
          assignment.front().status == naim::HostAssignmentStatus::Claimed,
          "missing-assignment-id claimed assignment should remain claimed");
    }

    {
      naim::ControllerStore store(db_path.string());
      store.Initialize();
      const naim::controller::HostAssignmentReconciliationService reconciliation_service;

      store.ReplaceDesiredState(BuildDesiredState("plane-c-stale", {"node-c-stale"}), 7);
      Expect(
          store.UpdatePlaneAppliedGeneration("plane-c-stale", 7),
          "plane-c-stale applied generation should update");
      store.ReplaceHostAssignments(
          {BuildHostAssignment(
              "plane-c-stale",
              "node-c-stale",
              7,
              naim::HostAssignmentStatus::Claimed)});
      const auto inserted_assignments =
          store.LoadHostAssignments(std::nullopt, std::nullopt, "plane-c-stale");
      Expect(inserted_assignments.size() == 1, "plane-c-stale should have one assignment");
      store.UpsertHostObservation(BuildHostObservation(
          "plane-c-stale",
          "node-c-stale",
          7,
          naim::HostObservationStatus::Applied,
          inserted_assignments.front().id - 1));

      const auto result = reconciliation_service.Reconcile(store, "plane-c-stale");
      const auto assignment = store.LoadHostAssignments(
          std::nullopt, std::nullopt, "plane-c-stale");
      Expect(result.applied == 0, "controller should not mark stale observation applied");
      Expect(assignment.size() == 1, "plane-c-stale should still have one assignment");
      Expect(
          assignment.front().status == naim::HostAssignmentStatus::Claimed,
          "plane-c-stale claimed assignment should remain claimed");
    }

    {
      naim::ControllerStore store(db_path.string());
      store.Initialize();
      const naim::controller::HostAssignmentReconciliationService reconciliation_service;

      store.ReplaceDesiredState(BuildDesiredState("plane-d", {"node-d"}), 6);
      store.ReplaceHostAssignments(
          {BuildHostAssignment(
               "plane-d",
               "node-d",
               5,
               naim::HostAssignmentStatus::Claimed),
           BuildHostAssignment(
               "plane-d",
               "node-d",
               6,
               naim::HostAssignmentStatus::Pending)});

      const auto result = reconciliation_service.Reconcile(store, "plane-d");
      const auto assignments = store.LoadHostAssignments(
          std::nullopt, std::nullopt, "plane-d");
      Expect(result.superseded == 1, "controller should supersede replaced claimed assignment");
      Expect(result.applied == 0, "controller should not mark replaced assignment applied");
      Expect(assignments.size() == 2, "plane-d should have two assignments");
      Expect(
          assignments.front().status == naim::HostAssignmentStatus::Superseded,
          "older claimed assignment should be superseded");
      Expect(
          assignments.back().status == naim::HostAssignmentStatus::Pending,
          "newest assignment should remain pending");
    }

    {
      naim::ControllerStore store(db_path.string());
      store.Initialize();
      const naim::controller::HostAssignmentReconciliationService reconciliation_service;

      store.ReplaceDesiredState(BuildDesiredState("plane-e", {"node-e"}), 5);
      Expect(
          store.UpdatePlaneAppliedGeneration("plane-e", 4),
          "plane-e applied generation should update");
      store.UpsertHostObservation(BuildHostObservation("plane-e", "node-e", 4));
      store.ReplaceHostAssignments(
          {BuildHostAssignment(
              "plane-e",
              "node-e",
              5,
              naim::HostAssignmentStatus::Claimed)});

      const auto result = reconciliation_service.Reconcile(store, "plane-e");
      const auto assignment = store.LoadHostAssignments(
          std::nullopt, std::nullopt, "plane-e");
      Expect(result.Total() == 0, "controller should not reconcile unconverged assignment");
      Expect(assignment.size() == 1, "plane-e should have one assignment");
      Expect(
          assignment.front().status == naim::HostAssignmentStatus::Claimed,
          "unconverged assignment should remain claimed");
    }

    fs::remove(db_path, error);
    std::cout << "controller plane deletion support tests passed\n";
    return 0;
  } catch (const std::exception& ex) {
    std::cerr << ex.what() << "\n";
    return 1;
  }
}
