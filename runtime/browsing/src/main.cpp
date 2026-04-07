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
    const int cef_subprocess_code = comet::browsing::MaybeRunCefSubprocess(argc, argv);
    if (cef_subprocess_code >= 0) {
      return cef_subprocess_code;
    }

    comet::browsing::BrowsingRuntimeConfig config;
    config.plane_name = GetEnvOr("COMET_PLANE_NAME", "unknown");
    config.instance_name = GetEnvOr("COMET_INSTANCE_NAME", "webgateway-unknown");
    config.instance_role = GetEnvOr("COMET_INSTANCE_ROLE", "webgateway");
    config.node_name = GetEnvOr("COMET_NODE_NAME", "unknown");
    config.control_root = GetEnvOr("COMET_CONTROL_ROOT", "");
    config.controller_url = GetEnvOr("COMET_CONTROLLER_URL", "http://controller.internal:18080");
    config.status_path = GetEnvAnyOr(
        {"COMET_WEBGATEWAY_RUNTIME_STATUS_PATH", "COMET_BROWSING_RUNTIME_STATUS_PATH"},
        "/comet/private/webgateway-runtime-status.json");
    config.state_root = GetEnvAnyOr(
        {"COMET_WEBGATEWAY_STATE_ROOT", "COMET_BROWSING_STATE_ROOT"},
        "/comet/private/sessions");
    const auto cef_cache_root = GetEnvAnyOr(
        {"COMET_WEBGATEWAY_CEF_CACHE_ROOT", "COMET_BROWSING_CEF_CACHE_ROOT"},
        DefaultCefCacheRoot(config.state_root).string().c_str());
    config.port =
        GetEnvIntAnyOr({"COMET_WEBGATEWAY_PORT", "COMET_BROWSING_PORT"}, 18130);
    config.policy = comet::browsing::BrowsingServer::ParsePolicyJson(
        GetEnvAnyOr({"COMET_WEBGATEWAY_POLICY_JSON", "COMET_BROWSING_POLICY_JSON"}, "{}"));

    std::cout << "[comet-webgateway] booting plane=" << config.plane_name
              << " instance=" << config.instance_name << "\n";
    std::cout << "[comet-webgateway] state_root=" << config.state_root.string() << "\n";
    std::cout << "[comet-webgateway] cef_cache_root=" << cef_cache_root << "\n";
    std::cout << "[comet-webgateway] status_path=" << config.status_path.string() << "\n";
    std::cout << "[comet-webgateway] port=" << config.port << "\n";
    std::cout << "[comet-webgateway] cef_build=" << comet::browsing::CefBuildSummary() << "\n";
    std::cout.flush();

    if (comet::browsing::CefBuildEnabled()) {
      comet::browsing::InitializeCefOrThrow(
          argc,
          argv,
          cef_cache_root,
          comet::browsing::CurrentExecutablePath());
    }

    comet::browsing::BrowsingServer server(std::move(config));
    const int exit_code = server.Run();
    if (comet::browsing::CefBuildEnabled()) {
      comet::browsing::ShutdownCef();
    }
    return exit_code;
  } catch (const std::exception& error) {
    std::cerr << "comet-webgatewayd: " << error.what() << "\n";
    if (comet::browsing::CefBuildEnabled() && comet::browsing::CefRuntimeEnabled()) {
      comet::browsing::ShutdownCef();
    }
    return 1;
  }
}
