#pragma once

#include <cstdint>
#include <filesystem>
#include <string>

#include "naim/state/sqlite_store.h"

struct ModelLibraryNodeSummary {
  std::string node_name;
  std::string registration_state;
  std::string session_state;
  std::string derived_role;
  std::string role_reason;
  std::string storage_root;
  std::uint64_t storage_total_bytes = 0;
  std::uint64_t storage_free_bytes = 0;
  bool has_storage_capacity = false;
  bool storage_role_enabled = false;
};

class ModelLibraryNodePlacement final {
 public:
  static ModelLibraryNodeSummary BuildSummary(
      const naim::RegisteredHostRecord& host);
  static bool AllowsModelPlacementRole(
      const std::string& derived_role,
      bool storage_role_enabled,
      bool quantization_required);
  static bool PathBelongsToRoot(
      const std::filesystem::path& path,
      const std::filesystem::path& root);
};
