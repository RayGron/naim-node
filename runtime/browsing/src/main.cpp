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

std::string GetEnvAnyOr(
    std::initializer_list<const char*> names,
    const char* fallback) {
  for (const char* name : names) {
    const char* value = std::getenv(name);
    if (value != nullptr && *value != '\0') {
      return std::string(value);
    }
  }
  return std::string(fallback);
}

std::filesystem::path DefaultCefCacheRoot(const std::filesystem::path& state_root) {
  if (state_root.has_parent_path()) {
    return state_root.parent_path() / "cef-cache";
  }
  return state_root / "cef-cache";
}

int GetEnvIntAnyOr(std::initializer_list<const char*> names, int fallback) {
  for (const char* name : names) {
    const char* value = std::getenv(name);
    if (value != nullptr && *value != '\0') {
      return std::stoi(value);
    }
  }
  return fallback;
}

}  // namespace

int main(int argc, char** argv) {
  try {
    const int cef_subprocess_code = naim::browsing::MaybeRunCefSubprocess(argc, argv);
    if (cef_subprocess_code >= 0) {
      return cef_subprocess_code;
    }

    naim::browsing::BrowsingRuntimeConfig config;
    config.plane_name = GetEnvOr("NAIM_PLANE_NAME", "unknown");
    config.instance_name = GetEnvOr("NAIM_INSTANCE_NAME", "webgateway-unknown");
    config.instance_role = GetEnvOr("NAIM_INSTANCE_ROLE", "webgateway");
    config.node_name = GetEnvOr("NAIM_NODE_NAME", "unknown");
    config.control_root = GetEnvOr("NAIM_CONTROL_ROOT", "");
    config.controller_url = GetEnvOr("NAIM_CONTROLLER_URL", "http://controller.internal:18080");
    config.status_path = GetEnvAnyOr(
        {"NAIM_WEBGATEWAY_RUNTIME_STATUS_PATH", "NAIM_BROWSING_RUNTIME_STATUS_PATH"},
        "/naim/private/webgateway-runtime-status.json");
    config.state_root = GetEnvAnyOr(
        {"NAIM_WEBGATEWAY_STATE_ROOT", "NAIM_BROWSING_STATE_ROOT"},
        "/naim/private/sessions");
    const auto cef_cache_root = GetEnvAnyOr(
        {"NAIM_WEBGATEWAY_CEF_CACHE_ROOT", "NAIM_BROWSING_CEF_CACHE_ROOT"},
        DefaultCefCacheRoot(config.state_root).string().c_str());
    config.port =
        GetEnvIntAnyOr({"NAIM_WEBGATEWAY_PORT", "NAIM_BROWSING_PORT"}, 18130);
    config.policy = naim::browsing::BrowsingServer::ParsePolicyJson(
        GetEnvAnyOr({"NAIM_WEBGATEWAY_POLICY_JSON", "NAIM_BROWSING_POLICY_JSON"}, "{}"));

    std::cout << "[naim-webgateway] booting plane=" << config.plane_name
              << " instance=" << config.instance_name << "\n";
    std::cout << "[naim-webgateway] state_root=" << config.state_root.string() << "\n";
    std::cout << "[naim-webgateway] cef_cache_root=" << cef_cache_root << "\n";
    std::cout << "[naim-webgateway] status_path=" << config.status_path.string() << "\n";
    std::cout << "[naim-webgateway] port=" << config.port << "\n";
    std::cout << "[naim-webgateway] cef_build=" << naim::browsing::CefBuildSummary() << "\n";
    std::cout.flush();

    if (naim::browsing::CefBuildEnabled()) {
      naim::browsing::InitializeCefOrThrow(
          argc,
          argv,
          cef_cache_root,
          naim::browsing::CurrentExecutablePath());
    }

    naim::browsing::BrowsingServer server(std::move(config));
    const int exit_code = server.Run();
    if (naim::browsing::CefBuildEnabled()) {
      naim::browsing::ShutdownCef();
    }
    return exit_code;
  } catch (const std::exception& error) {
    std::cerr << "naim-webgatewayd: " << error.what() << "\n";
    if (naim::browsing::CefBuildEnabled() && naim::browsing::CefRuntimeEnabled()) {
      naim::browsing::ShutdownCef();
    }
    return 1;
  }
}
