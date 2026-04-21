#include "config/generated_config_loader.h"

#include <cctype>
#include <cstdlib>
#include <fstream>
#include <sstream>
#include <stdexcept>

namespace naim::launcher {
namespace {

std::string Trim(const std::string& value) {
  std::size_t begin = 0;
  while (begin < value.size() &&
         std::isspace(static_cast<unsigned char>(value[begin])) != 0) {
    ++begin;
  }
  std::size_t end = value.size();
  while (end > begin &&
         std::isspace(static_cast<unsigned char>(value[end - 1])) != 0) {
    --end;
  }
  return value.substr(begin, end - begin);
}

std::string UnquoteTomlValue(const std::string& value) {
  const std::string trimmed = Trim(value);
  if (trimmed.size() >= 2 && trimmed.front() == '"' && trimmed.back() == '"') {
    return trimmed.substr(1, trimmed.size() - 2);
  }
  return trimmed;
}

bool ParseTomlBool(const std::string& value) {
  const std::string lowered = Trim(value);
  if (lowered == "true") {
    return true;
  }
  if (lowered == "false") {
    return false;
  }
  throw std::runtime_error("invalid boolean value '" + value + "'");
}

}  // namespace

GeneratedConfigLoader::GeneratedConfigLoader(
    const InstallLayoutResolver& install_layout_resolver)
    : install_layout_resolver_(install_layout_resolver) {}

std::optional<fs::path> GeneratedConfigLoader::ResolveConfigPathFromEnvOrDefault() const {
  const char* env = std::getenv("NAIM_CONFIG");
  if (env != nullptr && *env != '\0') {
    return fs::path(env);
  }
  const fs::path default_path = install_layout_resolver_.DefaultInstallLayout().config_path;
  if (fs::exists(default_path)) {
    return default_path;
  }
  return std::nullopt;
}

GeneratedConfig GeneratedConfigLoader::Load(const fs::path& path) const {
  std::ifstream input(path);
  if (!input.is_open()) {
    throw std::runtime_error("failed to read config '" + path.string() + "'");
  }

  GeneratedConfig config;
  std::string current_section;
  std::string line;
  while (std::getline(input, line)) {
    const std::string trimmed = Trim(line);
    if (trimmed.empty() || trimmed[0] == '#') {
      continue;
    }
    if (trimmed.size() >= 3 && trimmed.front() == '[' && trimmed.back() == ']') {
      current_section = trimmed.substr(1, trimmed.size() - 2);
      continue;
    }
    const std::size_t equals = trimmed.find('=');
    if (equals == std::string::npos) {
      continue;
    }
    const std::string key = Trim(trimmed.substr(0, equals));
    const std::string raw_value = Trim(trimmed.substr(equals + 1));
    if (current_section == "controller") {
      if (key == "listen_host") {
        config.controller.listen_host = UnquoteTomlValue(raw_value);
      } else if (key == "listen_port") {
        config.controller.listen_port = std::stoi(raw_value);
      } else if (key == "internal_listen_host") {
        config.controller.internal_listen_host = UnquoteTomlValue(raw_value);
      } else if (key == "db_path") {
        config.controller.db_path = fs::path(UnquoteTomlValue(raw_value));
      } else if (key == "artifacts_root") {
        config.controller.artifacts_root = fs::path(UnquoteTomlValue(raw_value));
      } else if (key == "web_ui_enabled") {
        config.controller.web_ui_enabled = ParseTomlBool(raw_value);
      } else if (key == "local_hostd_enabled") {
        config.controller.local_hostd_enabled = ParseTomlBool(raw_value);
      } else if (key == "controller_private_key") {
        config.controller.controller_private_key = fs::path(UnquoteTomlValue(raw_value));
      } else if (key == "controller_public_key") {
        config.controller.controller_public_key = fs::path(UnquoteTomlValue(raw_value));
      }
    } else if (current_section == "hostd") {
      if (key == "node_name") {
        config.hostd.node_name = UnquoteTomlValue(raw_value);
      } else if (key == "controller_url") {
        config.hostd.controller_url = UnquoteTomlValue(raw_value);
      } else if (key == "onboarding_key") {
        config.hostd.onboarding_key = UnquoteTomlValue(raw_value);
      } else if (key == "transport_mode") {
        config.hostd.transport_mode = UnquoteTomlValue(raw_value);
      } else if (key == "execution_mode") {
        config.hostd.execution_mode = UnquoteTomlValue(raw_value);
      } else if (key == "listen_address") {
        config.hostd.listen_address = UnquoteTomlValue(raw_value);
      } else if (key == "runtime_root") {
        config.hostd.runtime_root = fs::path(UnquoteTomlValue(raw_value));
      } else if (key == "state_root") {
        config.hostd.state_root = fs::path(UnquoteTomlValue(raw_value));
      } else if (key == "compose_mode") {
        config.hostd.compose_mode = UnquoteTomlValue(raw_value);
      } else if (key == "host_private_key") {
        config.hostd.host_private_key = fs::path(UnquoteTomlValue(raw_value));
      } else if (key == "host_public_key") {
        config.hostd.host_public_key = fs::path(UnquoteTomlValue(raw_value));
      } else if (key == "trusted_controller_fingerprint") {
        config.hostd.trusted_controller_fingerprint = UnquoteTomlValue(raw_value);
      } else if (key == "inventory_scan_interval_sec") {
        config.hostd.inventory_scan_interval_sec = std::stoi(raw_value);
      }
    }
  }
  return config;
}

}  // namespace naim::launcher
