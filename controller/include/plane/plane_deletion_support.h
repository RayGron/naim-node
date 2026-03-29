#pragma once

#include <functional>
#include <string>

#include <nlohmann/json.hpp>

#include "comet/state/sqlite_store.h"

namespace comet::controller::plane_deletion_support {

using PlaneDeleteFinalizer =
    std::function<bool(comet::ControllerStore&, const std::string&)>;
using PlaneEventAppender = std::function<void(
    comet::ControllerStore&,
    const std::string&,
    const std::string&,
    const std::string&,
    const nlohmann::json&,
    const std::string&)>;

bool FinalizeDeletedPlaneIfReady(
    comet::ControllerStore& store,
    const std::string& plane_name,
    const PlaneDeleteFinalizer& can_finalize_deleted_plane,
    const PlaneEventAppender& event_appender);

void FinalizeDeletedPlanesIfReady(
    comet::ControllerStore& store,
    const PlaneDeleteFinalizer& can_finalize_deleted_plane,
    const PlaneEventAppender& event_appender);

}  // namespace comet::controller::plane_deletion_support
