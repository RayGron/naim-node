#include <filesystem>
#include <iostream>
#include <stdexcept>
#include <string>

#include "comet/state/sqlite_store.h"
#include "plane/controller_state_service.h"
#include "plane/plane_deletion_support.h"
#include "plane/plane_service.h"

namespace fs = std::filesystem;

namespace {

void Expect(bool condition, const std::string& message) {
  if (!condition) {
    throw std::runtime_error(message);
  }
}

comet::DesiredState BuildDesiredState(const std::string& plane_name) {
  comet::DesiredState state;
  state.plane_name = plane_name;
  state.plane_mode = comet::PlaneMode::Llm;
  state.control_root = "/tmp/" + plane_name;
  return state;
}

comet::controller::PlaneService BuildPlaneService(const std::string& db_path) {
  return comet::controller::PlaneService(
      db_path,
      [](const std::string& value) { return value; },
      [](const comet::DesiredState&) {},
      [](comet::ControllerStore&, comet::DesiredState*) {},
      [](comet::ControllerStore&,
         const std::string&,
         const std::string&,
         const std::string&,
         const nlohmann::json&,
         const std::string&) {},
      [](comet::ControllerStore&, const std::string&) { return true; },
      [](const std::vector<comet::HostAssignment>&, const std::string&) {
        return std::optional<comet::HostAssignment>{};
      },
      [](const comet::DesiredState&,
         const std::string&,
         int,
         const std::vector<comet::NodeAvailabilityOverride>&,
         const std::vector<comet::HostObservation>&,
         const comet::SchedulingPolicyReport&) {
        return std::vector<comet::HostAssignment>{};
      },
      [](const comet::DesiredState&,
         int,
         const std::string&,
         const std::vector<comet::NodeAvailabilityOverride>&) {
        return std::vector<comet::HostAssignment>{};
      },
      [](const comet::DesiredState&, int, const std::string&) {
        return std::vector<comet::HostAssignment>{};
      },
      []() { return std::string("/tmp/artifacts"); });
}

}  // namespace

int main() {
  try {
    const fs::path db_path = fs::temp_directory_path() / "comet-plane-deletion-tests.sqlite";
    std::error_code error;
    fs::remove(db_path, error);

    {
      comet::ControllerStore store(db_path.string());
      store.Initialize();
      store.ReplaceDesiredState(BuildDesiredState("plane-a"), 7);
      Expect(store.UpdatePlaneState("plane-a", "deleting"), "plane-a should enter deleting");

      comet::controller::ControllerStateService state_service(
          comet::controller::ControllerStateService::Deps{
              [](comet::ControllerStore&, const std::string&) { return true; },
              [](comet::ControllerStore&,
                 const std::string&,
                 const std::string&,
                 const std::string&,
                 const nlohmann::json&,
                 const std::string&) {},
          });
      const auto payload = state_service.BuildPayload(db_path.string(), std::nullopt);
      Expect(payload.at("planes").is_array(), "planes payload should be an array");
      Expect(payload.at("planes").empty(), "deleting plane should be finalized on state read");
    }

    {
      comet::ControllerStore store(db_path.string());
      store.Initialize();
      store.ReplaceDesiredState(BuildDesiredState("plane-b"), 8);
      Expect(store.UpdatePlaneState("plane-b", "deleting"), "plane-b should enter deleting");

      auto plane_service = BuildPlaneService(db_path.string());
      bool threw_not_found = false;
      try {
        (void)plane_service.ShowPlane("plane-b");
      } catch (const std::exception& ex) {
        threw_not_found = std::string(ex.what()).find("not found") != std::string::npos;
      }
      Expect(threw_not_found, "show-plane should finalize deleted plane before reading it");
      Expect(!store.LoadPlane("plane-b").has_value(), "plane-b should be removed from store");
    }

    fs::remove(db_path, error);
    std::cout << "controller plane deletion support tests passed\n";
    return 0;
  } catch (const std::exception& ex) {
    std::cerr << ex.what() << "\n";
    return 1;
  }
}
