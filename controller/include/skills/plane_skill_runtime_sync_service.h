#pragma once

#include <string>

#include "naim/state/models.h"

namespace naim::controller {

class PlaneSkillRuntimeSyncService final {
 public:
  bool SyncPlane(const std::string& db_path, const naim::DesiredState& desired_state) const;

 private:
  static bool IsReadyForSync(const std::string& db_path, const naim::DesiredState& desired_state);
};

}  // namespace naim::controller
