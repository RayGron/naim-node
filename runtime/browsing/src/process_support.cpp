#include "browsing/process_support.h"

#include <array>
#include <stdexcept>
#include <vector>

#include <fcntl.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

namespace comet::browsing {

CommandResult RunCommandCapture(const CommandRequest& request) {
  if (request.args.empty()) {
    throw std::invalid_argument("command args must not be empty");
  }

  int pipe_fds[2];
  if (pipe(pipe_fds) != 0) {
    throw std::runtime_error("pipe failed");
  }

  const pid_t pid = fork();
  if (pid < 0) {
    close(pipe_fds[0]);
    close(pipe_fds[1]);
    throw std::runtime_error("fork failed");
  }

  if (pid == 0) {
    close(pipe_fds[0]);
    dup2(pipe_fds[1], STDOUT_FILENO);
    if (request.merge_stderr_into_stdout) {
      dup2(pipe_fds[1], STDERR_FILENO);
    } else {
      const int dev_null = open("/dev/null", O_WRONLY);
      if (dev_null >= 0) {
        dup2(dev_null, STDERR_FILENO);
        close(dev_null);
      }
    }
    close(pipe_fds[1]);

    if (request.clear_environment) {
      clearenv();
    }

    for (const auto& [key, value] : request.environment) {
      setenv(key.c_str(), value.c_str(), 1);
    }

    if (request.working_directory.has_value() &&
        chdir(request.working_directory->c_str()) != 0) {
      _exit(125);
    }

    std::vector<char*> argv;
    argv.reserve(request.args.size() + 1);
    for (const auto& arg : request.args) {
      argv.push_back(const_cast<char*>(arg.c_str()));
    }
    argv.push_back(nullptr);

    if (request.args.front().find('/') != std::string::npos) {
      execv(argv[0], argv.data());
    } else {
      execvp(argv[0], argv.data());
    }
    _exit(127);
  }

  close(pipe_fds[1]);
  CommandResult result;
  std::array<char, 4096> buffer{};
  while (true) {
    const ssize_t read_count = read(pipe_fds[0], buffer.data(), buffer.size());
    if (read_count <= 0) {
      break;
    }
    result.output.append(buffer.data(), static_cast<std::size_t>(read_count));
  }
  close(pipe_fds[0]);

  int status = 0;
  waitpid(pid, &status, 0);
  if (WIFEXITED(status)) {
    result.exit_code = WEXITSTATUS(status);
  } else {
    result.exit_code = 1;
  }
  return result;
}

}  // namespace comet::browsing
