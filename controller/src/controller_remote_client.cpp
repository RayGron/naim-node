#include "../include/controller_remote_client.h"

#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <sstream>

namespace {

std::string TrimCopy(const std::string& value) {
  std::size_t start = 0;
  while (start < value.size() &&
         std::isspace(static_cast<unsigned char>(value[start])) != 0) {
    ++start;
  }
  std::size_t end = value.size();
  while (end > start &&
         std::isspace(static_cast<unsigned char>(value[end - 1])) != 0) {
    --end;
  }
  return value.substr(start, end - start);
}

std::string BuildQueryString(
    const std::vector<std::pair<std::string, std::string>>& params) {
  if (params.empty()) {
    return {};
  }

  std::ostringstream query;
  bool first = true;
  for (const auto& [key, value] : params) {
    query << (first ? '?' : '&');
    first = false;
    query << key << '=' << value;
  }
  return query.str();
}

std::optional<std::string> LoadControllerTargetConfig() {
  const char* xdg_config_home = std::getenv("XDG_CONFIG_HOME");
  std::filesystem::path config_path;
  if (xdg_config_home != nullptr && *xdg_config_home != '\0') {
    config_path = std::filesystem::path(xdg_config_home) / "comet" / "controller";
  } else {
    const char* home = std::getenv("HOME");
    if (home == nullptr || *home == '\0') {
      return std::nullopt;
    }
    config_path = std::filesystem::path(home) / ".config" / "comet" / "controller";
  }
  if (!std::filesystem::exists(config_path)) {
    return std::nullopt;
  }
  std::ifstream in(config_path);
  std::stringstream buffer;
  buffer << in.rdbuf();
  const std::string value = TrimCopy(buffer.str());
  if (value.empty()) {
    return std::nullopt;
  }
  return value;
}

}  // namespace

std::optional<std::string> ResolveControllerTarget(
    const std::optional<std::string>& explicit_target,
    const std::optional<std::string>& db_arg) {
  if (db_arg.has_value()) {
    return std::nullopt;
  }
  if (explicit_target.has_value()) {
    return explicit_target;
  }
  if (const char* env_target = std::getenv("COMET_CONTROLLER");
      env_target != nullptr && *env_target != '\0') {
    return std::string(env_target);
  }
  return LoadControllerTargetConfig();
}

nlohmann::json SendControllerJsonRequest(
    const comet::controller::ControllerEndpointTarget& target,
    const std::string& method,
    const std::string& path,
    const std::vector<std::pair<std::string, std::string>>& params) {
  const HttpResponse response = SendControllerHttpRequest(
      target, method, path + BuildQueryString(params));
  nlohmann::json payload =
      response.body.empty() ? nlohmann::json::object()
                            : nlohmann::json::parse(response.body);
  payload["_http_status"] = response.status_code;
  return payload;
}
