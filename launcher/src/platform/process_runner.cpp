#include "platform/process_runner.h"

#include <array>
#include <cstdio>
#include <cstdlib>
#include <stdexcept>
#include <string>
#include <vector>

#if defined(_WIN32)
#include <process.h>
#else
#include <sys/wait.h>
#include <unistd.h>
#endif

#include "naim/core/platform_compat.h"

namespace naim::launcher {
namespace {

std::string ShellEscape(const std::string& value) {
  std::string escaped = "'";
  for (const char ch : value) {
    if (ch == '\'') {
      escaped += "'\"'\"'";
    } else {
      escaped += ch;
    }
  }
  escaped += "'";
  return escaped;
}

}  // namespace

bool ProcessRunner::CommandExists(const std::string& command) const {
#if defined(_WIN32)
  const std::string probe = "where " + command + " >NUL 2>NUL";
#else
  const std::string probe = "command -v " + ShellEscape(command) + " >/dev/null 2>&1";
#endif
  return std::system(probe.c_str()) == 0;
}

int ProcessRunner::RunShellCommand(const std::string& command) const {
  return std::system(command.c_str());
}

std::string ProcessRunner::CaptureShellOutput(const std::string& command) const {
  FILE* pipe = naim::platform::OpenPipe(command.c_str(), "r");
  if (pipe == nullptr) {
    return "";
  }
  std::array<char, 256> buffer{};
  std::string output;
  while (fgets(buffer.data(), static_cast<int>(buffer.size()), pipe) != nullptr) {
    output += buffer.data();
  }
  naim::platform::ClosePipe(pipe);
  return output;
}

int ProcessRunner::RunCommand(const std::vector<std::string>& args) const {
  if (args.empty()) {
    return 1;
  }

  std::vector<const char*> raw_args;
  raw_args.reserve(args.size() + 1);
  for (const std::string& arg : args) {
    raw_args.push_back(arg.c_str());
  }
  raw_args.push_back(nullptr);

#if defined(_WIN32)
  const intptr_t exit_code = _spawnv(_P_WAIT, raw_args.front(), raw_args.data());
  if (exit_code == -1) {
    throw std::runtime_error("_spawnv failed");
  }
  return static_cast<int>(exit_code);
#else
  pid_t pid = fork();
  if (pid < 0) {
    throw std::runtime_error("fork failed");
  }
  if (pid == 0) {
    std::vector<char*> exec_args;
    exec_args.reserve(args.size() + 1);
    for (const std::string& arg : args) {
      exec_args.push_back(const_cast<char*>(arg.c_str()));
    }
    exec_args.push_back(nullptr);
    execv(exec_args.front(), exec_args.data());
    std::perror("execv");
    _exit(127);
  }

  int status = 0;
  if (waitpid(pid, &status, 0) < 0) {
    throw std::runtime_error("waitpid failed");
  }
  if (WIFEXITED(status)) {
    return WEXITSTATUS(status);
  }
  if (WIFSIGNALED(status)) {
    return 128 + WTERMSIG(status);
  }
  return 1;
#endif
}

int ProcessRunner::SpawnCommand(const std::vector<std::string>& args) const {
  if (args.empty()) {
    throw std::runtime_error("cannot spawn empty command");
  }

  std::vector<const char*> raw_args;
  raw_args.reserve(args.size() + 1);
  for (const std::string& arg : args) {
    raw_args.push_back(arg.c_str());
  }
  raw_args.push_back(nullptr);

#if defined(_WIN32)
  const intptr_t pid = _spawnv(_P_NOWAIT, raw_args.front(), raw_args.data());
  if (pid == -1) {
    throw std::runtime_error("_spawnv failed");
  }
  return static_cast<int>(pid);
#else
  pid_t pid = fork();
  if (pid < 0) {
    throw std::runtime_error("fork failed");
  }
  if (pid == 0) {
    std::vector<char*> exec_args;
    exec_args.reserve(args.size() + 1);
    for (const std::string& arg : args) {
      exec_args.push_back(const_cast<char*>(arg.c_str()));
    }
    exec_args.push_back(nullptr);
    execv(exec_args.front(), exec_args.data());
    std::perror("execv");
    _exit(127);
  }
  return static_cast<int>(pid);
#endif
}

}  // namespace naim::launcher
