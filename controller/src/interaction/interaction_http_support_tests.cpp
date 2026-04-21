#include <iostream>
#include <stdexcept>
#include <string>

#include "interaction/interaction_http_support.h"

#include "naim/runtime/runtime_status.h"

namespace {

void Expect(bool condition, const std::string& message) {
  if (!condition) {
    throw std::runtime_error(message);
  }
}

void TestInteractionSupportMatchesRuntimeOnlyPlaneObservation() {
  const naim::controller::ControllerRuntimeSupportService runtime_support_service;
  const naim::controller::DesiredStatePolicyService desired_state_policy_service;
  const naim::controller::InteractionRuntimeSupportService interaction_runtime_support_service;
  const InteractionHttpSupport support(
      runtime_support_service,
      desired_state_policy_service,
      interaction_runtime_support_service);

  naim::HostObservation observation;
  observation.node_name = "remote-app-host";
  observation.instance_runtime_json = naim::SerializeRuntimeStatusListJson({
      naim::RuntimeProcessStatus{
          "app-cypher", "app", "remote-app-host", "", "", "running", "", "", 101, 0, true},
      naim::RuntimeProcessStatus{
          "skills-cypher", "skills", "remote-app-host", "", "", "running", "", "", 102, 0,
          true},
      naim::RuntimeProcessStatus{
          "webgateway-cypher", "browsing", "remote-app-host", "", "", "running", "", "", 103,
          0, true},
  });

  Expect(
      support.ObservationMatchesPlane(observation, "cypher"),
      "interaction support should match runtime-only app/skills/webgateway observations");
  Expect(
      !support.ObservationMatchesPlane(observation, "other-plane"),
      "interaction support should reject foreign runtime-only observations");

  std::cout << "ok: interaction-support-runtime-only-plane-match" << '\n';
}

}  // namespace

int main() {
  try {
    TestInteractionSupportMatchesRuntimeOnlyPlaneObservation();
    return 0;
  } catch (const std::exception& error) {
    std::cerr << "interaction_http_support_tests failed: " << error.what() << '\n';
    return 1;
  }
}
