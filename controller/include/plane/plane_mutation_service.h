#pragma once

#include <functional>
#include <optional>
#include <string>

#include "infra/controller_action.h"
#include "plane/plane_service.h"

#include "naim/state/models.h"

namespace naim::controller {

class PlaneMutationService {
 public:
  using ApplyDesiredStateFn = std::function<int(
      const std::string&,
      const naim::DesiredState&,
      const std::string&,
      const std::string&)>;
  using MakePlaneServiceFn = std::function<PlaneService(const std::string&)>;

  struct Deps {
    ApplyDesiredStateFn apply_desired_state;
    MakePlaneServiceFn make_plane_service;
  };

  explicit PlaneMutationService(Deps deps);

  ControllerActionResult ExecuteUpsertPlaneStateAction(
      const std::string& db_path,
      const std::string& desired_state_json,
      const std::string& artifacts_root,
      const std::optional<std::string>& expected_plane_name,
      const std::string& source_label) const;

  ControllerActionResult ExecuteStartPlaneAction(
      const std::string& db_path,
      const std::string& plane_name) const;

  ControllerActionResult ExecuteStopPlaneAction(
      const std::string& db_path,
      const std::string& plane_name) const;

  ControllerActionResult ExecuteDeletePlaneAction(
      const std::string& db_path,
      const std::string& plane_name) const;

 private:
  Deps deps_;
};

}  // namespace naim::controller
