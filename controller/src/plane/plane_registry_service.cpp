#include "plane/plane_registry_service.h"

#include <stdexcept>
#include <utility>

#include "plane/plane_deletion_support.h"

using nlohmann::json;

namespace comet::controller {

PlaneRegistryService::PlaneRegistryService(Deps deps) : deps_(std::move(deps)) {}

json PlaneRegistryService::BuildPlanesPayload(const std::string& db_path) const {
  if (!deps_.can_finalize_deleted_plane || !deps_.event_appender ||
      !deps_.filter_host_observations_for_plane ||
      !deps_.compute_effective_applied_generation ||
      !deps_.build_latest_assignments_by_node) {
    throw std::runtime_error(
        "plane registry service dependencies are not configured");
  }

  comet::ControllerStore store(db_path);
  store.Initialize();

  plane_deletion_support::FinalizeDeletedPlanesIfReady(
      store,
      deps_.can_finalize_deleted_plane,
      deps_.event_appender);

  json items = json::array();
  for (const auto& plane : store.LoadPlanes()) {
    const auto desired_state = store.LoadDesiredState(plane.name);
    const auto desired_generation = store.LoadDesiredGeneration(plane.name);
    const auto observations = deps_.filter_host_observations_for_plane(
        store.LoadHostObservations(),
        plane.name);
    const auto assignments =
        store.LoadHostAssignments(std::nullopt, std::nullopt, plane.name);
    const int effective_applied_generation =
        deps_.compute_effective_applied_generation(
            plane,
            desired_state,
            desired_generation,
            observations);
    if (effective_applied_generation > plane.applied_generation) {
      store.UpdatePlaneAppliedGeneration(plane.name, effective_applied_generation);
    }
    const auto latest_assignments_by_node =
        deps_.build_latest_assignments_by_node(assignments);
    int failed_assignments = 0;
    int in_flight_assignments = 0;
    for (const auto& [node_name, assignment] : latest_assignments_by_node) {
      (void)node_name;
      if (assignment.status == comet::HostAssignmentStatus::Failed) {
        ++failed_assignments;
      } else if (
          assignment.status == comet::HostAssignmentStatus::Pending ||
          assignment.status == comet::HostAssignmentStatus::Claimed) {
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
      {"service", "comet-controller"},
      {"db_path", db_path},
      {"items", std::move(items)},
  };
}

}  // namespace comet::controller
