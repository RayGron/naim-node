#include "browsing/cef_support.h"

#include <mutex>
#include <stdexcept>

#if COMET_WITH_CEF
#include <unistd.h>

#include "include/cef_app.h"
#include "include/cef_browser_process_handler.h"
#include "include/cef_command_line.h"
#include "include/cef_version.h"
#endif

namespace comet::browsing {

#if COMET_WITH_CEF
namespace {

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
    command_line->AppendSwitch("disable-dev-shm-usage");
    command_line->AppendSwitch("mute-audio");
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
  CefString(&settings.browser_subprocess_path) = executable_path.string();
  CefString(&settings.root_cache_path) = (state_root / "cef-global").string();

  if (!CefInitialize(main_args, settings, g_cef_app, nullptr)) {
    throw std::runtime_error("CEF initialization failed");
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
