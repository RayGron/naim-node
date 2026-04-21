#include <filesystem>
#include <functional>
#include <iostream>
#include <sstream>
#include <stdexcept>
#include <string>

#include "app/hostd_desired_state_display_support.h"
#include "app/hostd_desired_state_path_support.h"

namespace {

void Expect(bool condition, const std::string& message) {
  if (!condition) {
    throw std::runtime_error(message);
  }
}

std::string CaptureStdout(const std::function<void()>& action) {
  std::ostringstream output;
  auto* original = std::cout.rdbuf(output.rdbuf());
  try {
    action();
    std::cout.rdbuf(original);
  } catch (...) {
    std::cout.rdbuf(original);
    throw;
  }
  return output.str();
}

}  // namespace

int main() {
  try {
    namespace fs = std::filesystem;
    const naim::hostd::HostdDesiredStatePathSupport path_support;
    const naim::hostd::HostdDesiredStateDisplaySupport support(path_support);

    {
      const std::string output = CaptureStdout([&]() {
        support.ShowDemoOps("node-a", "/storage-root", "/runtime-root");
      });
      Expect(
          output.find("hostd demo ops for node=node-a") != std::string::npos,
          "ShowDemoOps should print the selected node");
      Expect(
          output.find("docker-compose.yml") != std::string::npos,
          "ShowDemoOps should render node execution operations");
    }

    {
      const fs::path temp_root =
          fs::temp_directory_path() / "naim-hostd-desired-state-display-support-tests";
      std::error_code cleanup_error;
      fs::remove_all(temp_root, cleanup_error);
      fs::create_directories(temp_root);

      const std::string output = CaptureStdout([&]() {
        support.ShowLocalState("node-z", temp_root.string());
      });
      Expect(
          output.find("hostd local state for node=node-z") != std::string::npos,
          "ShowLocalState should print the requested node");
      Expect(
          output.find("state: empty") != std::string::npos,
          "ShowLocalState should report empty local state when nothing is applied");

      const std::string runtime_output = CaptureStdout([&]() {
        support.ShowRuntimeStatus("node-z", temp_root.string());
      });
      Expect(
          runtime_output.find("runtime_status: unavailable (no local applied state)") !=
              std::string::npos,
          "ShowRuntimeStatus should report unavailable status without local state");

      fs::remove_all(temp_root, cleanup_error);
    }

    std::cout << "ok: hostd-desired-state-display-support-demo-ops\n";
    std::cout << "ok: hostd-desired-state-display-support-empty-local-state\n";
    std::cout << "ok: hostd-desired-state-display-support-empty-runtime-status\n";
    return 0;
  } catch (const std::exception& ex) {
    std::cerr << ex.what() << '\n';
    return 1;
  }
}
