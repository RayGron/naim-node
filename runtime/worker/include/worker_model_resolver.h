#pragma once

#include <filesystem>
#include <optional>
#include <string>

#include <nlohmann/json.hpp>

#include "worker_config.h"

namespace naim::worker {

class WorkerModelResolver final {
 public:
  WorkerModelResolver() = default;

  std::optional<std::string> ResolveModelPath(const WorkerConfig& config) const;

 private:
  static std::string ExpandUserPath(const std::string& value);
  static nlohmann::json LoadJsonOrEmpty(const std::filesystem::path& path);
  static std::optional<std::string> ResolveGgufPath(const std::string& path_text);
};

}  // namespace naim::worker
