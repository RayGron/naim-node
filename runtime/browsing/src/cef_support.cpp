#include "browsing/cef_support.h"

#include <mutex>
#include <stdexcept>
#include <system_error>
#include <vector>

#if COMET_WITH_CEF
#include <cstdlib>
#include <unistd.h>

#include "include/cef_app.h"
#include "include/cef_browser_process_handler.h"
#include "include/cef_command_line.h"
#include "include/cef_version.h"
#endif

namespace comet::browsing {

#if COMET_WITH_CEF
namespace {

void AppendSwitchValue(
    CefRefPtr<CefCommandLine> command_line,
    const char* name,
    const char* value) {
  if (!command_line->HasSwitch(name)) {
    command_line->AppendSwitchWithValue(name, value);
  }
}

class CometCefApp final : public CefApp, public CefBrowserProcessHandler {
 public:
  CefRefPtr<CefBrowserProcessHandler> GetBrowserProcessHandler() override {
    return this;
  }

  void OnBeforeCommandLineProcessing(
      const CefString& process_type,
      CefRefPtr<CefCommandLine> command_line) override {
    command_line->AppendSwitch("disable-gpu");
    command_line->AppendSwitch("disable-gpu-compositing");
    command_line->AppendSwitch("disable-background-networking");
    command_line->AppendSwitch("disable-component-update");
    command_line->AppendSwitch("disable-dev-shm-usage");
    command_line->AppendSwitch("disable-domain-reliability");
    command_line->AppendSwitch("disable-sync");
    command_line->AppendSwitch("metrics-recording-only");
    command_line->AppendSwitch("mute-audio");
    command_line->AppendSwitch("no-default-browser-check");
    command_line->AppendSwitch("no-first-run");
    AppendSwitchValue(command_line, "password-store", "basic");
    AppendSwitchValue(
        command_line,
        "disable-features",
        "AutofillServerCommunication,CertificateTransparencyComponentUpdater,OptimizationHints,MediaRouter");
    if (process_type.empty()) {
      command_line->AppendSwitch("headless");
    }
  }

  IMPLEMENT_REFCOUNTING(CometCefApp);
};

std::mutex g_cef_mutex;
bool g_cef_runtime_enabled = false;
CefRefPtr<CometCefApp> g_cef_app;

std::string ReadExecutablePath() {
  std::string path(4096, '\0');
  const ssize_t read_count = readlink("/proc/self/exe", path.data(), path.size() - 1);
  if (read_count <= 0) {
    throw std::runtime_error("failed to resolve /proc/self/exe for CEF subprocess path");
  }
  path.resize(static_cast<std::size_t>(read_count));
  return path;
}

std::filesystem::path ExecutableDirectory(const std::filesystem::path& executable_path) {
  if (executable_path.has_parent_path()) {
    return executable_path.parent_path();
  }
  return std::filesystem::current_path();
}

std::filesystem::path EnsureDirectory(const std::filesystem::path& path) {
  const auto absolute_path = std::filesystem::absolute(path);
  std::error_code error;
  std::filesystem::create_directories(absolute_path, error);
  if (error) {
    throw std::runtime_error(
        "failed to prepare CEF cache directory at " + absolute_path.string() + ": " + error.message());
  }
  return absolute_path;
}

std::filesystem::path ResetDirectory(const std::filesystem::path& path) {
  const auto absolute_path = std::filesystem::absolute(path);
  std::error_code error;
  std::filesystem::remove_all(absolute_path, error);
  if (error) {
    throw std::runtime_error(
        "failed to reset CEF cache directory at " + absolute_path.string() + ": " + error.message());
  }
  return EnsureDirectory(absolute_path);
}

void ApplyEnvironmentIsolation(const std::filesystem::path& cache_root) {
  const auto isolated_home = EnsureDirectory(cache_root / "home");
  const auto isolated_config = EnsureDirectory(cache_root / "xdg-config");
  const auto isolated_cache = EnsureDirectory(cache_root / "xdg-cache");
  const auto isolated_data = EnsureDirectory(cache_root / "xdg-data");

  setenv("HOME", isolated_home.string().c_str(), 1);
  setenv("XDG_CONFIG_HOME", isolated_config.string().c_str(), 1);
  setenv("XDG_CACHE_HOME", isolated_cache.string().c_str(), 1);
  setenv("XDG_DATA_HOME", isolated_data.string().c_str(), 1);

  const std::vector<const char*> secret_env_keys = {
      "OPENAI_API_KEY",
      "ANTHROPIC_API_KEY",
      "GEMINI_API_KEY",
      "GOOGLE_API_KEY",
      "AWS_ACCESS_KEY_ID",
      "AWS_SECRET_ACCESS_KEY",
      "AWS_SESSION_TOKEN",
      "AZURE_OPENAI_API_KEY",
      "GH_TOKEN",
      "GITHUB_TOKEN",
      "SSH_AUTH_SOCK",
  };
  for (const char* key : secret_env_keys) {
    unsetenv(key);
  }
}

}  // namespace
#endif

bool CefBuildEnabled() {
#if COMET_WITH_CEF
  return true;
#else
  return false;
#endif
}

std::string CefBuildSummary() {
#if COMET_WITH_CEF
  return "cef-chromium-" + std::to_string(CHROME_VERSION_MAJOR);
#else
  return "cef-disabled";
#endif
}

int MaybeRunCefSubprocess(int argc, char** argv) {
#if COMET_WITH_CEF
  std::lock_guard<std::mutex> lock(g_cef_mutex);
  if (!g_cef_app) {
    g_cef_app = new CometCefApp();
  }
  CefMainArgs main_args(argc, argv);
  return CefExecuteProcess(main_args, g_cef_app, nullptr);
#else
  (void)argc;
  (void)argv;
  return -1;
#endif
}

void InitializeCefOrThrow(
    int argc,
    char** argv,
    const std::filesystem::path& state_root,
    const std::filesystem::path& executable_path) {
#if COMET_WITH_CEF
  std::lock_guard<std::mutex> lock(g_cef_mutex);
  if (g_cef_runtime_enabled) {
    return;
  }

  if (!g_cef_app) {
    g_cef_app = new CometCefApp();
  }

  CefMainArgs main_args(argc, argv);
  CefSettings settings;
  settings.no_sandbox = true;
  settings.multi_threaded_message_loop = true;
  settings.windowless_rendering_enabled = true;
  settings.log_severity = LOGSEVERITY_FATAL;
  const auto executable_dir = ExecutableDirectory(executable_path);
  const auto cache_root = EnsureDirectory(state_root);
  ApplyEnvironmentIsolation(cache_root);
  CefString(&settings.browser_subprocess_path) = executable_path.string();
  CefString(&settings.resources_dir_path) = executable_dir.string();
  CefString(&settings.locales_dir_path) = (executable_dir / "locales").string();
  auto initialize_with_cache = [&](const std::filesystem::path& root_cache_path) {
    CefString(&settings.root_cache_path) = root_cache_path.string();
    return CefInitialize(main_args, settings, g_cef_app, nullptr);
  };

  if (!initialize_with_cache(cache_root)) {
    const auto cleaned_cache_root = ResetDirectory(cache_root);
    ApplyEnvironmentIsolation(cleaned_cache_root);
    if (!initialize_with_cache(cleaned_cache_root)) {
      throw std::runtime_error("CEF initialization failed");
    }
  }
  g_cef_runtime_enabled = true;
#else
  (void)argc;
  (void)argv;
  (void)state_root;
  (void)executable_path;
  throw std::runtime_error("CEF runtime is not compiled into this build");
#endif
}

void ShutdownCef() {
#if COMET_WITH_CEF
  std::lock_guard<std::mutex> lock(g_cef_mutex);
  if (!g_cef_runtime_enabled) {
    return;
  }
  CefShutdown();
  g_cef_runtime_enabled = false;
#endif
}

bool CefRuntimeEnabled() {
#if COMET_WITH_CEF
  std::lock_guard<std::mutex> lock(g_cef_mutex);
  return g_cef_runtime_enabled;
#else
  return false;
#endif
}

std::string CurrentExecutablePath() {
#if COMET_WITH_CEF
  return ReadExecutablePath();
#else
  return {};
#endif
}

}  // namespace comet::browsing
