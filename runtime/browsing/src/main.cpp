#include "browsing/browsing_server.h"
#include "browsing/cef_support.h"

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

int main(int argc, char** argv) {
  try {
    const int cef_subprocess_code = comet::browsing::MaybeRunCefSubprocess(argc, argv);
    if (cef_subprocess_code >= 0) {
      return cef_subprocess_code;
    }

    comet::browsing::BrowsingRuntimeConfig config;
    config.plane_name = GetEnvOr("COMET_PLANE_NAME", "unknown");
    config.instance_name = GetEnvOr("COMET_INSTANCE_NAME", "browsing-unknown");
    config.instance_role = GetEnvOr("COMET_INSTANCE_ROLE", "browsing");
    config.node_name = GetEnvOr("COMET_NODE_NAME", "unknown");
    config.control_root = GetEnvOr("COMET_CONTROL_ROOT", "");
    config.controller_url = GetEnvOr("COMET_CONTROLLER_URL", "http://controller.internal:18080");
    config.status_path =
        GetEnvOr("COMET_BROWSING_RUNTIME_STATUS_PATH", "/comet/private/browsing-runtime-status.json");
    config.state_root = GetEnvOr("COMET_BROWSING_STATE_ROOT", "/comet/private/sessions");
    config.port = GetEnvIntOr("COMET_BROWSING_PORT", 18130);
    config.policy =
        comet::browsing::BrowsingServer::ParsePolicyJson(GetEnvOr("COMET_BROWSING_POLICY_JSON", "{}"));

    std::cout << "[comet-browsing] booting plane=" << config.plane_name
              << " instance=" << config.instance_name << "\n";
    std::cout << "[comet-browsing] state_root=" << config.state_root.string() << "\n";
    std::cout << "[comet-browsing] status_path=" << config.status_path.string() << "\n";
    std::cout << "[comet-browsing] port=" << config.port << "\n";
    std::cout << "[comet-browsing] cef_build=" << comet::browsing::CefBuildSummary() << "\n";
    std::cout.flush();

    if (comet::browsing::CefBuildEnabled()) {
      comet::browsing::InitializeCefOrThrow(
          argc,
          argv,
          config.state_root,
          comet::browsing::CurrentExecutablePath());
    }

    comet::browsing::BrowsingServer server(std::move(config));
    const int exit_code = server.Run();
    if (comet::browsing::CefBuildEnabled()) {
      comet::browsing::ShutdownCef();
    }
    return exit_code;
  } catch (const std::exception& error) {
    std::cerr << "comet-browsingd: " << error.what() << "\n";
    if (comet::browsing::CefBuildEnabled() && comet::browsing::CefRuntimeEnabled()) {
      comet::browsing::ShutdownCef();
    }
    return 1;
  }
}
