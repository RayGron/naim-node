#include "app/controller_component_defaults.h"

#include <filesystem>

#include "naim/state/sqlite_store.h"

namespace naim::controller {

std::string ControllerComponentDefaults::DefaultArtifactsRoot() const {
  return (std::filesystem::path("var") / "artifacts").string();
}

std::string ControllerComponentDefaults::ResolvePlaneArtifactsRoot(
    const std::string& db_path,
    const std::string& plane_name,
    const std::string& fallback_artifacts_root) const {
  naim::ControllerStore store(db_path);
  store.Initialize();
  const auto plane = store.LoadPlane(plane_name);
  if (plane.has_value() && !plane->artifacts_root.empty()) {
    return plane->artifacts_root;
  }
  return fallback_artifacts_root;
}

int ControllerComponentDefaults::DefaultStaleAfterSeconds() const {
  return 300;
}

int ControllerComponentDefaults::MinimumSafeDirectRebalanceScore() const {
  return 100;
}

int ControllerComponentDefaults::MaximumRebalanceIterationsPerGeneration() const {
  return 1;
}

int ControllerComponentDefaults::WorkerMinimumResidencySeconds() const {
  return 300;
}

int ControllerComponentDefaults::NodeCooldownAfterMoveSeconds() const {
  return 60;
}

int ControllerComponentDefaults::VerificationStableSamplesRequired() const {
  return 3;
}

int ControllerComponentDefaults::VerificationTimeoutSeconds() const {
  return 45;
}

}  // namespace naim::controller
