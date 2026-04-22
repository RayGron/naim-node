#pragma once

#include <cstdint>
#include <filesystem>
#include <optional>
#include <string>

#include <nlohmann/json.hpp>

namespace naim::hostd {

class HostdModelArtifactRequestSupport final {
 public:
  static constexpr std::uintmax_t kMaxChunkBytes = 4ULL * 1024ULL * 1024ULL;

  std::string NormalizePathString(const std::filesystem::path& path) const;
  bool IsSafeRelativePath(const std::filesystem::path& path) const;
  std::optional<std::uintmax_t> JsonUintmax(
      const nlohmann::json& payload,
      const std::string& key) const;
  std::string BuildManifestCanonicalText(const nlohmann::json& files) const;
};

}  // namespace naim::hostd
