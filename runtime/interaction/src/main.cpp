#include "interaction/interaction_runtime_server.h"

#include <cstdlib>
#include <exception>
#include <iostream>

namespace {

std::string GetEnvOr(const char* name, const char* fallback) {
  const char* value = std::getenv(name);
  return value != nullptr && *value != '\0' ? std::string(value) : std::string(fallback);
}

int GetEnvIntOr(const char* name, int fallback) {
  const char* value = std::getenv(name);
  if (value == nullptr || *value == '\0') {
    return fallback;
  }
  return std::stoi(value);
}

}  // namespace

int main() {
  try {
    naim::interaction_runtime::InteractionRuntimeConfig config;
    config.plane_name = GetEnvOr("NAIM_PLANE_NAME", "unknown");
    config.instance_name = GetEnvOr("NAIM_INSTANCE_NAME", "interaction-unknown");
    config.instance_role = GetEnvOr("NAIM_INSTANCE_ROLE", "interaction");
    config.node_name = GetEnvOr("NAIM_NODE_NAME", "unknown");
    config.control_root = GetEnvOr("NAIM_CONTROL_ROOT", "");
    config.controller_url = GetEnvOr("NAIM_CONTROLLER_URL", "http://controller.internal:18080");
    config.status_path = GetEnvOr(
        "NAIM_INTERACTION_RUNTIME_STATUS_PATH",
        "/naim/private/interaction-runtime-status.json");
    config.listen_host = GetEnvOr("NAIM_INTERACTION_LISTEN_HOST", "0.0.0.0");
    config.port = GetEnvIntOr("NAIM_INTERACTION_PORT", 18110);
    config.upstream_base = GetEnvOr(
        "NAIM_INTERACTION_UPSTREAM_BASE",
        "http://127.0.0.1:8000/v1");

    std::cout << "[naim-interaction] booting plane=" << config.plane_name
              << " instance=" << config.instance_name << "\n";
    std::cout << "[naim-interaction] status_path=" << config.status_path.string() << "\n";
    std::cout << "[naim-interaction] upstream_base=" << config.upstream_base << "\n";
    std::cout << "[naim-interaction] port=" << config.port << "\n";
    std::cout.flush();

    naim::interaction_runtime::InteractionRuntimeServer server(std::move(config));
    return server.Run();
  } catch (const std::exception& error) {
    std::cerr << "naim-interactiond: " << error.what() << "\n";
    return 1;
  }
}
