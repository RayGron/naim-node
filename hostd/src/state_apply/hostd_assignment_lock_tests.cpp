#include "state_apply/hostd_assignment_lock.h"

#include <sys/wait.h>
#include <unistd.h>

#include <filesystem>
#include <iostream>
#include <optional>
#include <string>

namespace fs = std::filesystem;

namespace {

void Expect(bool condition, const std::string& message) {
  if (!condition) {
    std::cerr << "FAIL: " << message << "\n";
    std::exit(1);
  }
}

bool ChildCanAcquire(const std::string& root, const std::string& node) {
  const pid_t pid = ::fork();
  if (pid < 0) {
    throw std::runtime_error("fork failed");
  }
  if (pid == 0) {
    const auto lock = naim::hostd::HostdAssignmentLock::TryAcquire(root, node);
    _exit(lock.has_value() ? 0 : 2);
  }
  int status = 0;
  Expect(::waitpid(pid, &status, 0) == pid, "waitpid failed");
  Expect(WIFEXITED(status), "child did not exit cleanly");
  return WEXITSTATUS(status) == 0;
}

}  // namespace

int main() {
  const fs::path root = fs::temp_directory_path() / "naim-hostd-assignment-lock-tests";
  std::error_code error;
  fs::remove_all(root, error);
  fs::create_directories(root);

  {
    const auto lock = naim::hostd::HostdAssignmentLock::TryAcquire(root.string(), "node-a");
    Expect(lock.has_value(), "parent should acquire node lock");
    Expect(!ChildCanAcquire(root.string(), "node-a"),
           "child should not acquire node lock while parent holds it");
  }

  Expect(ChildCanAcquire(root.string(), "node-a"),
         "child should acquire node lock after parent releases it");

  fs::remove_all(root, error);
  if (error) {
    std::cerr << "cleanup warning: " << error.message() << "\n";
  }
  std::cout << "hostd assignment lock tests passed\n";
  return 0;
}
