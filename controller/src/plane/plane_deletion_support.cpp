#include "plane/plane_deletion_support.h"

#include <utility>

namespace naim::controller::plane_deletion_support {

bool FinalizeDeletedPlaneIfReady(
    naim::ControllerStore& store,
    const std::string& plane_name,
    const PlaneDeleteFinalizer& can_finalize_deleted_plane,
    const PlaneEventAppender& event_appender) {
  if (!can_finalize_deleted_plane || !event_appender) {
    return false;
  }

  const auto plane = store.LoadPlane(plane_name);
  if (!plane.has_value() || plane->state != "deleting" ||
      !can_finalize_deleted_plane(store, plane_name)) {
    return false;
  }

  store.DeletePlane(plane_name);
  event_appender(
      store,
      "plane",
      "deleted",
      "plane deleted from controller registry after cleanup convergence",
      nlohmann::json{
          {"plane_name", plane_name},
          {"deleted_generation", plane->generation},
      },
      "");
  return true;
}

void FinalizeDeletedPlanesIfReady(
    naim::ControllerStore& store,
    const PlaneDeleteFinalizer& can_finalize_deleted_plane,
    const PlaneEventAppender& event_appender) {
  for (const auto& plane : store.LoadPlanes()) {
    if (plane.state != "deleting") {
      continue;
    }
    FinalizeDeletedPlaneIfReady(
        store,
        plane.name,
        can_finalize_deleted_plane,
        event_appender);
  }
}

}  // namespace naim::controller::plane_deletion_support
