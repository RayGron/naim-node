#include "worker_model_resolver.h"

#include <algorithm>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <stdexcept>
#include <vector>

#include <nlohmann/json.hpp>

namespace fs = std::filesystem;

namespace naim::worker {

std::optional<std::string> WorkerModelResolver::ResolveModelPath(
    const WorkerConfig& config) const {
  if (const auto resolved = ResolveGgufPath(config.model_path); resolved.has_value()) {
    return resolved;
  }
  if (config.control_root.empty()) {
    return std::nullopt;
  }
  const nlohmann::json active_model =
      LoadJsonOrEmpty(fs::path(config.control_root) / "active-model.json");
  if (active_model.empty()) {
    return std::nullopt;
  }
  const std::string runtime_path =
      active_model.value(
          "cached_runtime_model_path",
          active_model.value("runtime_model_path", std::string{}));
  if (const auto resolved = ResolveGgufPath(runtime_path); resolved.has_value()) {
    return resolved;
  }
  return ResolveGgufPath(active_model.value("cached_local_model_path", std::string{}));
}

std::string WorkerModelResolver::ExpandUserPath(const std::string& value) {
  if (value.empty() || value[0] != '~') {
    return value;
  }
  const char* home = std::getenv("HOME");
  if (home == nullptr || std::strlen(home) == 0) {
    return value;
  }
  if (value == "~") {
    return home;
  }
  if (value.size() > 1 && value[1] == '/') {
    return std::string(home) + value.substr(1);
  }
  return value;
}

nlohmann::json WorkerModelResolver::LoadJsonOrEmpty(const fs::path& path) {
  if (!fs::exists(path)) {
    return nlohmann::json::object();
  }
  std::ifstream input(path);
  if (!input.is_open()) {
    throw std::runtime_error("failed to open json file: " + path.string());
  }
  nlohmann::json value;
  input >> value;
  return value;
}

std::optional<std::string> WorkerModelResolver::ResolveGgufPath(const std::string& path_text) {
  if (path_text.empty()) {
    return std::nullopt;
  }
  const fs::path path(ExpandUserPath(path_text));
  if (fs::is_regular_file(path) && path.extension() == ".gguf") {
    return path.string();
  }
  if (!fs::exists(path) || !fs::is_directory(path)) {
    return std::nullopt;
  }
  std::vector<fs::path> candidates;
  for (const auto& entry : fs::recursive_directory_iterator(path)) {
    if (entry.is_regular_file() && entry.path().extension() == ".gguf") {
      candidates.push_back(entry.path());
    }
  }
  if (candidates.empty()) {
    return std::nullopt;
  }
  std::sort(candidates.begin(), candidates.end());
  return candidates.front().string();
}

}  // namespace naim::worker
