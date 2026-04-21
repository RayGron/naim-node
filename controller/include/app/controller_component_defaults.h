#pragma once

#include <string>

namespace naim::controller {

class ControllerComponentDefaults final {
 public:
  std::string DefaultArtifactsRoot() const;

  std::string ResolvePlaneArtifactsRoot(
      const std::string& db_path,
      const std::string& plane_name,
      const std::string& fallback_artifacts_root) const;

  int DefaultStaleAfterSeconds() const;
  int MinimumSafeDirectRebalanceScore() const;
  int MaximumRebalanceIterationsPerGeneration() const;
  int WorkerMinimumResidencySeconds() const;
  int NodeCooldownAfterMoveSeconds() const;
  int VerificationStableSamplesRequired() const;
  int VerificationTimeoutSeconds() const;
};

}  // namespace naim::controller
