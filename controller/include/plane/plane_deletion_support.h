#pragma once

#include <functional>
#include <string>

#include <nlohmann/json.hpp>

#include "naim/state/sqlite_store.h"

namespace naim::controller::plane_deletion_support {

using PlaneDeleteFinalizer =
    std::function<bool(naim::ControllerStore&, const std::string&)>;
using PlaneEventAppender = std::function<void(
    naim::ControllerStore&,
    const std::string&,
    const std::string&,
    const std::string&,
    const nlohmann::json&,
    const std::string&)>;

bool FinalizeDeletedPlaneIfReady(
    naim::ControllerStore& store,
    const std::string& plane_name,
    const PlaneDeleteFinalizer& can_finalize_deleted_plane,
    const PlaneEventAppender& event_appender);

void FinalizeDeletedPlanesIfReady(
    naim::ControllerStore& store,
    const PlaneDeleteFinalizer& can_finalize_deleted_plane,
    const PlaneEventAppender& event_appender);

}  // namespace naim::controller::plane_deletion_support
