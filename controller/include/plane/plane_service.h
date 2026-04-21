#pragma once

#include <memory>
#include <optional>
#include <string>
#include <vector>

#include <nlohmann/json.hpp>

#include "app/controller_service_interfaces.h"
#include "naim/state/models.h"
#include "naim/planning/scheduling_policy.h"
#include "naim/state/sqlite_store.h"
#include "plane/plane_lifecycle_support.h"
#include "plane/plane_state_presentation_support.h"

namespace naim::controller {

class PlaneService : public IPlaneService {
 public:
  PlaneService(
      std::string db_path,
      std::shared_ptr<const PlaneStatePresentationSupport> state_presentation_support,
      std::shared_ptr<const PlaneLifecycleSupport> lifecycle_support);

  int ListPlanes() const override;
  int ShowPlane(const std::string& plane_name) const override;
  int StartPlane(const std::string& plane_name) const override;
  int StopPlane(const std::string& plane_name) const override;
  int DeletePlane(const std::string& plane_name) const override;

 private:
  bool FinalizeDeletedPlaneIfReady(
      naim::ControllerStore& store,
      const std::string& plane_name) const;
  std::string ResolveArtifactsRoot(
      naim::ControllerStore& store,
      const naim::PlaneRecord& plane,
      const std::string& plane_name) const;

  std::string db_path_;
  std::shared_ptr<const PlaneStatePresentationSupport> state_presentation_support_;
  std::shared_ptr<const PlaneLifecycleSupport> lifecycle_support_;
};

}  // namespace naim::controller
