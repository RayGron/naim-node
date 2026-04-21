#include "plane/plane_registry_service.h"

#include <memory>
#include <stdexcept>
#include <utility>

#include "host/host_assignment_reconciliation_service.h"
#include "plane/plane_placement_payload_builder.h"

using nlohmann::json;

namespace naim::controller {

PlaneRegistryService::PlaneRegistryService(
    std::shared_ptr<const PlaneLifecycleSupport> lifecycle_support,
    std::shared_ptr<const PlaneRegistryQuerySupport> query_support)
    : lifecycle_support_(std::move(lifecycle_support)),
      query_support_(std::move(query_support)) {}

json PlaneRegistryService::BuildPlanesPayload(const std::string& db_path) const {
  if (!lifecycle_support_ || !query_support_) {
    throw std::runtime_error(
        "plane registry service dependencies are not configured");
  }

  naim::ControllerStore store(db_path);
  store.Initialize();
  const HostAssignmentReconciliationService reconciliation_service;

  for (const auto& plane : store.LoadPlanes()) {
    if (plane.state != "deleting" ||
        !lifecycle_support_->CanFinalizeDeletedPlane(store, plane.name)) {
      continue;
    }
    store.DeletePlane(plane.name);
    lifecycle_support_->AppendPlaneEvent(
        store,
        "deleted",
        "plane deleted from controller registry after cleanup convergence",
        nlohmann::json{
            {"plane_name", plane.name},
            {"deleted_generation", plane.generation},
        },
        "");
  }

  json items = json::array();
  for (const auto& plane : store.LoadPlanes()) {
    const auto desired_state = store.LoadDesiredState(plane.name);
    const auto desired_generation = store.LoadDesiredGeneration(plane.name);
    const auto observations = query_support_->FilterHostObservationsForPlane(
        store.LoadHostObservations(),
        plane.name);
    const int effective_applied_generation =
        query_support_->ComputeEffectiveAppliedGeneration(
            plane,
            desired_state,
            desired_generation,
            observations);
    if (effective_applied_generation > plane.applied_generation) {
      store.UpdatePlaneAppliedGeneration(plane.name, effective_applied_generation);
    }
    (void)reconciliation_service.Reconcile(store, plane.name);
    const auto assignments =
        store.LoadHostAssignments(std::nullopt, std::nullopt, plane.name);
    const auto latest_assignments_by_node =
        query_support_->BuildLatestAssignmentsByNode(assignments);
    int failed_assignments = 0;
    int in_flight_assignments = 0;
    for (const auto& [node_name, assignment] : latest_assignments_by_node) {
      (void)node_name;
      if (assignment.status == naim::HostAssignmentStatus::Failed) {
        ++failed_assignments;
      } else if (
          assignment.status == naim::HostAssignmentStatus::Pending ||
          assignment.status == naim::HostAssignmentStatus::Claimed) {
        ++in_flight_assignments;
      }
    }
    items.push_back(json{
        {"name", plane.name},
        {"state", plane.state},
        {"plane_mode", plane.plane_mode},
        {"model_id",
         desired_state.has_value() && desired_state->bootstrap_model.has_value()
             ? json(desired_state->bootstrap_model->model_id)
             : json(nullptr)},
        {"served_model_name",
         desired_state.has_value() && desired_state->bootstrap_model.has_value() &&
                 desired_state->bootstrap_model->served_model_name.has_value()
             ? json(*desired_state->bootstrap_model->served_model_name)
             : json(nullptr)},
        {"generation", plane.generation},
        {"applied_generation", effective_applied_generation},
        {"staged_update", plane.generation > effective_applied_generation},
        {"failed_assignments", failed_assignments},
        {"in_flight_assignments", in_flight_assignments},
        {"rebalance_iteration", plane.rebalance_iteration},
        {"shared_disk_name", plane.shared_disk_name},
        {"control_root", plane.control_root},
        {"created_at", plane.created_at},
        {"placement",
         desired_state.has_value()
             ? PlanePlacementPayloadBuilder(*desired_state).Build()
             : json(nullptr)},
        {"node_count",
         desired_state.has_value() ? json(desired_state->nodes.size())
                                   : json(nullptr)},
        {"instance_count",
         desired_state.has_value() ? json(desired_state->instances.size())
                                   : json(nullptr)},
        {"disk_count",
         desired_state.has_value() ? json(desired_state->disks.size())
                                   : json(nullptr)},
    });
  }

  return json{
      {"service", "naim-controller"},
      {"db_path", db_path},
      {"items", std::move(items)},
  };
}

}  // namespace naim::controller
