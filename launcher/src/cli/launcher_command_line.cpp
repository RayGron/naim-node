#include "cli/launcher_command_line.h"

#include <algorithm>
#include <stdexcept>

namespace naim::launcher {

LauncherCommandLine LauncherCommandLine::FromArgv(int argc, char** argv) {
  return LauncherCommandLine(
      argc > 1 ? std::vector<std::string>(argv + 1, argv + argc)
               : std::vector<std::string>{});
}

LauncherCommandLine::LauncherCommandLine(std::vector<std::string> args)
    : args_(std::move(args)) {}

bool LauncherCommandLine::HasCommand() const {
  return !args_.empty();
}

const std::string& LauncherCommandLine::command() const {
  return args_.front();
}

const std::vector<std::string>& LauncherCommandLine::args() const {
  return args_;
}

std::vector<std::string> LauncherCommandLine::Tail(const std::size_t offset) const {
  if (offset >= args_.size()) {
    return {};
  }
  return std::vector<std::string>(args_.begin() + static_cast<std::ptrdiff_t>(offset), args_.end());
}

void LauncherCommandLine::PrintUsage(std::ostream& out) const {
  out
      << "usage:\n"
      << "  quick start (controller + local hostd + web ui):\n"
      << "    naim-node install controller --with-hostd --with-web-ui\n"
      << "    naim-node run controller\n"
      << "  quick start (remote hostd):\n"
      << "    naim-node install hostd --controller http://controller:18080\n"
      << "    naim-node install hostd --controller http://controller:18080 --onboarding-key <key>\n"
      << "    naim-node run hostd\n"
      << "  naim-node version\n"
      << "  naim-node doctor [controller|hostd]\n"
      << "  naim-node run controller [--db <path>] [--artifacts-root <path>] [--listen-host <host>] [--listen-port <port>] [--internal-listen-host <host>] [--skills-factory-listen-port <port>] [--with-hostd] [--with-web-ui] [--hostd-compose-mode exec|skip]\n"
      << "  naim-node run hostd [--node <name>] [--db <path>] [--controller <url>] [--controller-fingerprint <sha256>] [--runtime-root <path>] [--state-root <path>] [--compose-mode exec|skip]\n"
      << "  naim-node install controller [--with-hostd] [--with-web-ui] [--listen-host <host>] [--listen-port <port>] [--internal-listen-host <host>] [--config <path>] [--state-root <path>] [--log-root <path>] [--systemd-dir <path>] [--skip-systemctl]\n"
      << "  naim-node install hostd [--node <name>] [--controller <url>] [--controller-fingerprint <sha256>] [--onboarding-key <key>] [--transport out|in|hybrid] [--listen <addr>] [--config <path>] [--state-root <path>] [--log-root <path>] [--systemd-dir <path>] [--skip-systemctl]\n"
      << "  naim-node service status|start|stop|restart|uninstall|verify <controller|hostd|controller-hostd> [--systemd-dir <path>] [--skip-systemctl]\n"
      << "  naim-node connect-hostd --db <path> --node <name> --public-key <base64-or-file> [--address <hostd-url>] [--transport out|in|hybrid] [--controller-fingerprint <sha256>]\n";
}

std::optional<std::string> LauncherCommandLine::FindFlagValue(const std::string& flag) const {
  for (std::size_t index = 0; index < args_.size(); ++index) {
    if (args_[index] == flag) {
      if (index + 1 >= args_.size()) {
        throw std::runtime_error("missing value for " + flag);
      }
      return args_[index + 1];
    }
  }
  return std::nullopt;
}

bool LauncherCommandLine::HasFlag(const std::string& flag) const {
  return std::find(args_.begin(), args_.end(), flag) != args_.end();
}

int LauncherCommandLine::ParseIntValue(
    const std::optional<std::string>& value,
    const int fallback) {
  if (!value.has_value()) {
    return fallback;
  }
  return std::stoi(*value);
}

}  // namespace naim::launcher
