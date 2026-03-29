#pragma once

#include <optional>
#include <string>

#include <nlohmann/json.hpp>

#include "plane/plane_deletion_support.h"

namespace comet::controller {

class ControllerStateService {
 public:
  struct Deps {
    plane_deletion_support::PlaneDeleteFinalizer can_finalize_deleted_plane;
    plane_deletion_support::PlaneEventAppender event_appender;
  };

  explicit ControllerStateService(Deps deps = {});

  nlohmann::json BuildPayload(
      const std::string& db_path,
      const std::optional<std::string>& plane_name) const;

 private:
  Deps deps_;
};

}  // namespace comet::controller
