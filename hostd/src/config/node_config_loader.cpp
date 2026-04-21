#include "config/node_config_loader.h"

#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <stdexcept>
#include <vector>

#include <nlohmann/json.hpp>

namespace naim::hostd {
namespace {

using nlohmann::json;

constexpr const char* kDefaultNodeConfigRelativePath = "config/naim-node-config.json";

}  // namespace

std::optional<std::string> NodeConfigLoader::FindNodeConfigPath(const char* argv0) const {
  std::vector<std::filesystem::path> candidates;
  candidates.emplace_back(std::filesystem::current_path() / kDefaultNodeConfigRelativePath);

  if (argv0 != nullptr && *argv0 != '\0') {
    std::error_code error;
    const std::filesystem::path executable_path = std::filesystem::absolute(argv0, error);
    if (!error) {
      std::filesystem::path current = executable_path.parent_path();
      for (int depth = 0; depth < 4 && !current.empty(); ++depth) {
        candidates.emplace_back(current / kDefaultNodeConfigRelativePath);
        current = current.parent_path();
      }
    }
  }

  for (const auto& candidate : candidates) {
    std::error_code error;
    if (std::filesystem::exists(candidate, error) && !error) {
      return candidate.lexically_normal().string();
    }
  }
  return std::nullopt;
}

NaimNodeConfig NodeConfigLoader::Load(
    const std::optional<std::string>& config_arg,
    const char* argv0) const {
  std::optional<std::filesystem::path> config_path;
  bool explicit_path = false;

  if (config_arg.has_value()) {
    config_path = *config_arg;
    explicit_path = true;
  } else if (const char* env_path = std::getenv("NAIM_NODE_CONFIG_PATH");
             env_path != nullptr && *env_path != '\0') {
    config_path = env_path;
    explicit_path = true;
  } else if (const auto discovered_path = FindNodeConfigPath(argv0); discovered_path.has_value()) {
    config_path = *discovered_path;
  }

  if (!config_path.has_value()) {
    return {};
  }

  if (!std::filesystem::exists(*config_path)) {
    if (explicit_path) {
      throw std::runtime_error(
          "naim node config file not found: " + config_path->string());
    }
    return {};
  }

  std::ifstream input(*config_path);
  if (!input.is_open()) {
    throw std::runtime_error(
        "failed to open naim node config file '" + config_path->string() + "'");
  }

  const json value = json::parse(input, nullptr, true, true);
  if (!value.is_object()) {
    throw std::runtime_error(
        "naim node config must be a JSON object: " + config_path->string());
  }

  NaimNodeConfig config;
  if (value.contains("paths")) {
    if (!value.at("paths").is_object()) {
      throw std::runtime_error(
          "naim node config field 'paths' must be an object: " + config_path->string());
    }
    const auto& paths = value.at("paths");
    if (paths.contains("storage_root")) {
      if (!paths.at("storage_root").is_string()) {
        throw std::runtime_error(
            "naim node config field 'paths.storage_root' must be a string: " +
            config_path->string());
      }
      config.storage_root = paths.at("storage_root").get<std::string>();
    }
  } else if (value.contains("storage_root")) {
    if (!value.at("storage_root").is_string()) {
      throw std::runtime_error(
          "naim node config field 'storage_root' must be a string: " +
          config_path->string());
    }
    config.storage_root = value.at("storage_root").get<std::string>();
  }

  return config;
}

}  // namespace naim::hostd
