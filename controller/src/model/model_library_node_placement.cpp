#include "model/model_library_node_placement.h"

#include <algorithm>

#include <nlohmann/json.hpp>

namespace {

using nlohmann::json;

json ParseCapabilitiesJson(const std::string& capabilities_json) {
  if (capabilities_json.empty()) {
    return json::object();
  }
  const json parsed = json::parse(capabilities_json, nullptr, false);
  return parsed.is_discarded() ? json::object() : parsed;
}

std::uint64_t ReadNonNegativeCapacityBytes(
    const json& capabilities,
    const char* field_name) {
  if (!capabilities.contains(field_name)) {
    return 0;
  }
  const auto& value = capabilities.at(field_name);
  if (value.is_number_unsigned()) {
    return value.get<std::uint64_t>();
  }
  if (value.is_number_integer()) {
    return static_cast<std::uint64_t>(
        std::max<std::int64_t>(0, value.get<std::int64_t>()));
  }
  return 0;
}

}  // namespace

ModelLibraryNodeSummary ModelLibraryNodePlacement::BuildSummary(
    const naim::RegisteredHostRecord& host) {
  ModelLibraryNodeSummary summary;
  summary.node_name = host.node_name;
  summary.registration_state = host.registration_state;
  summary.session_state = host.session_state;
  summary.derived_role = host.derived_role;
  summary.role_reason = host.role_reason;
  summary.storage_role_enabled = host.storage_role_enabled;
  const json capabilities = ParseCapabilitiesJson(host.capabilities_json);
  if (capabilities.contains("storage_root") &&
      capabilities.at("storage_root").is_string()) {
    summary.storage_root = capabilities.at("storage_root").get<std::string>();
  }
  summary.storage_total_bytes =
      ReadNonNegativeCapacityBytes(capabilities, "storage_total_bytes");
  summary.storage_free_bytes =
      ReadNonNegativeCapacityBytes(capabilities, "storage_free_bytes");
  summary.has_storage_capacity = summary.storage_total_bytes > 0;
  return summary;
}

bool ModelLibraryNodePlacement::AllowsModelPlacementRole(
    const std::string& derived_role,
    bool storage_role_enabled,
    bool quantization_required) {
  if (quantization_required) {
    return derived_role == "worker";
  }
  return storage_role_enabled || derived_role == "worker" || derived_role == "storage";
}

bool ModelLibraryNodePlacement::PathBelongsToRoot(
    const std::filesystem::path& path,
    const std::filesystem::path& root) {
  const auto normalized_path = path.lexically_normal();
  const auto normalized_root = root.lexically_normal();
  if (normalized_path == normalized_root) {
    return true;
  }
  const std::string root_text = normalized_root.generic_string();
  const std::string path_text = normalized_path.generic_string();
  if (root_text.empty() || path_text.size() <= root_text.size()) {
    return false;
  }
  return path_text.rfind(root_text + "/", 0) == 0;
}
