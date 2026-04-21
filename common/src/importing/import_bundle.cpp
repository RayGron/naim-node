#include "naim/importing/import_bundle.h"

#include <filesystem>
#include <stdexcept>

#include "naim/state/state_json.h"

namespace naim {

DesiredState ImportPlaneBundle(const std::string& bundle_dir) {
  const std::filesystem::path bundle_path(bundle_dir);
  const std::filesystem::path desired_state_v2_path =
      bundle_path / "desired-state.v2.json";

  if (!std::filesystem::exists(desired_state_v2_path)) {
    if (std::filesystem::exists(bundle_path / "plane.json") ||
        std::filesystem::exists(bundle_path / "infer.json") ||
        std::filesystem::exists(bundle_path / "workers")) {
      throw std::runtime_error(
          "legacy bundle importer has been removed; provide desired-state.v2.json");
    }
    throw std::runtime_error(
        "bundle is missing desired-state.v2.json at '" +
        desired_state_v2_path.string() + "'");
  }

  const auto desired_state = LoadDesiredStateJson(desired_state_v2_path.string());
  if (!desired_state.has_value()) {
    throw std::runtime_error(
        "bundle is missing desired-state.v2.json at '" +
        desired_state_v2_path.string() + "'");
  }

  return *desired_state;
}

}  // namespace naim
