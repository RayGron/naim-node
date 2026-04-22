#include "app/hostd_model_artifact_request_support.h"

#include <algorithm>
#include <vector>

namespace naim::hostd {

std::string HostdModelArtifactRequestSupport::NormalizePathString(
    const std::filesystem::path& path) const {
  return path.lexically_normal().string();
}

bool HostdModelArtifactRequestSupport::IsSafeRelativePath(
    const std::filesystem::path& path) const {
  const auto normalized = path.lexically_normal();
  const std::string text = normalized.generic_string();
  return !text.empty() && text != "." && text.front() != '/' &&
         text != ".." && text.rfind("../", 0) != 0;
}

std::optional<std::uintmax_t> HostdModelArtifactRequestSupport::JsonUintmax(
    const nlohmann::json& payload,
    const std::string& key) const {
  if (!payload.contains(key) || !payload.at(key).is_number_unsigned()) {
    return std::nullopt;
  }
  return payload.at(key).get<std::uintmax_t>();
}

std::string HostdModelArtifactRequestSupport::BuildManifestCanonicalText(
    const nlohmann::json& files) const {
  std::vector<nlohmann::json> sorted_files = files.get<std::vector<nlohmann::json>>();
  std::sort(
      sorted_files.begin(),
      sorted_files.end(),
      [](const nlohmann::json& lhs, const nlohmann::json& rhs) {
        const int lhs_root = lhs.value("root_index", 0);
        const int rhs_root = rhs.value("root_index", 0);
        if (lhs_root != rhs_root) {
          return lhs_root < rhs_root;
        }
        return lhs.value("relative_path", std::string{}) <
               rhs.value("relative_path", std::string{});
      });

  std::string canonical = "naim-model-manifest-v1\n";
  for (const auto& file : sorted_files) {
    canonical += "file ";
    canonical += std::to_string(file.value("root_index", 0));
    canonical += " ";
    canonical += file.value("relative_path", std::string{});
    canonical += " ";
    canonical += std::to_string(JsonUintmax(file, "size_bytes").value_or(0));
    canonical += " ";
    canonical += file.value("sha256", std::string{});
    canonical += "\n";
  }
  return canonical;
}

}  // namespace naim::hostd
