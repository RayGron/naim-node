#include "app/hostd_command_support.h"

#include <array>
#include <cctype>
#include <cstdio>
#include <filesystem>
#include <string>

#include "naim/core/platform_compat.h"

namespace naim::hostd {

std::string HostdCommandSupport::Trim(const std::string& value) const {
  std::size_t start = 0;
  while (start < value.size() &&
         std::isspace(static_cast<unsigned char>(value[start])) != 0) {
    ++start;
  }
  std::size_t end = value.size();
  while (end > start &&
         std::isspace(static_cast<unsigned char>(value[end - 1])) != 0) {
    --end;
  }
  return value.substr(start, end - start);
}

std::string HostdCommandSupport::RunCommandCapture(const std::string& command) const {
  std::array<char, 512> buffer{};
  std::string output;
  FILE* pipe = naim::platform::OpenPipe(command.c_str(), "r");
  if (pipe == nullptr) {
    return output;
  }
  while (fgets(buffer.data(), static_cast<int>(buffer.size()), pipe) != nullptr) {
    output.append(buffer.data());
  }
  naim::platform::ClosePipe(pipe);
  return output;
}

std::string HostdCommandSupport::ShellQuote(const std::string& value) const {
  std::string quoted = "'";
  for (char ch : value) {
    if (ch == '\'') {
      quoted += "'\"'\"'";
    } else {
      quoted.push_back(ch);
    }
  }
  quoted += "'";
  return quoted;
}

bool HostdCommandSupport::RunCommandOk(const std::string& command) const {
  return std::system(command.c_str()) == 0;
}

std::string HostdCommandSupport::ResolvedDockerCommand() const {
  static const std::string resolved = []() -> std::string {
    if (std::system("docker version >/dev/null 2>&1") == 0) {
      return "docker";
    }
    const std::string windows_docker =
        "/mnt/c/Program Files/Docker/Docker/resources/bin/docker.exe";
    if (std::filesystem::exists(windows_docker) &&
        std::system(("'" + windows_docker + "' version >/dev/null 2>&1").c_str()) == 0) {
      return "'" + windows_docker + "'";
    }
    return "docker";
  }();
  return resolved;
}

std::string HostdCommandSupport::ResolvedDockerComposeCommand() const {
  static const std::string resolved = [this]() -> std::string {
    if (RunCommandOk(ResolvedDockerCommand() + " compose version >/dev/null 2>&1")) {
      return ResolvedDockerCommand() + " compose";
    }
    if (RunCommandOk("command -v docker-compose >/dev/null 2>&1")) {
      return "docker-compose";
    }
    return ResolvedDockerCommand() + " compose";
  }();
  return resolved;
}

}  // namespace naim::hostd
