#include "cli/hostd_command_line.h"

#include <filesystem>
#include <iostream>
#include <stdexcept>

namespace naim::hostd {

HostdCommandLine::HostdCommandLine(int argc, char** argv)
    : argc_(argc), argv_(argv), command_(argc > 1 ? argv[1] : "") {}

void HostdCommandLine::PrintUsage(std::ostream& out) const {
  out << "Usage:\n"
      << "  naim-hostd show-demo-ops --node <node-name> [--config <path>]\n"
      << "  naim-hostd show-state-ops --node <node-name> [--db <path>] [--artifacts-root <path>] [--runtime-root <path>] [--state-root <path>] [--config <path>]\n"
      << "  naim-hostd show-local-state --node <node-name> [--state-root <path>]\n"
      << "  naim-hostd show-runtime-status --node <node-name> [--state-root <path>]\n"
      << "  naim-hostd report-observed-state --node <node-name> [--db <path> | --controller <url>] [--host-private-key <path>] [--controller-fingerprint <sha256>] [--onboarding-key <token>] [--state-root <path>]\n"
      << "  naim-hostd apply-state-ops --node <node-name> [--db <path>] [--artifacts-root <path>] [--runtime-root <path>] [--state-root <path>] [--compose-mode skip|exec] [--config <path>]\n"
      << "  naim-hostd apply-next-assignment --node <node-name> [--db <path> | --controller <url>] [--host-private-key <path>] [--controller-fingerprint <sha256>] [--onboarding-key <token>] [--runtime-root <path>] [--state-root <path>] [--compose-mode skip|exec] [--config <path>]\n";
}

bool HostdCommandLine::HasCommand() const {
  return !command_.empty();
}

const std::string& HostdCommandLine::command() const {
  return command_;
}

std::optional<std::string> HostdCommandLine::node() const {
  return FindOptionValue("--node");
}

std::optional<std::string> HostdCommandLine::db() const {
  return FindOptionValue("--db");
}

std::optional<std::string> HostdCommandLine::controller() const {
  return FindOptionValue("--controller");
}

std::optional<std::string> HostdCommandLine::artifacts_root() const {
  return FindOptionValue("--artifacts-root");
}

std::optional<std::string> HostdCommandLine::runtime_root() const {
  return FindOptionValue("--runtime-root");
}

std::optional<std::string> HostdCommandLine::compose_mode_raw() const {
  return FindOptionValue("--compose-mode");
}

std::optional<std::string> HostdCommandLine::state_root() const {
  return FindOptionValue("--state-root");
}

std::optional<std::string> HostdCommandLine::host_private_key() const {
  return FindOptionValue("--host-private-key");
}

std::optional<std::string> HostdCommandLine::controller_fingerprint() const {
  return FindOptionValue("--controller-fingerprint");
}

std::optional<std::string> HostdCommandLine::onboarding_key() const {
  return FindOptionValue("--onboarding-key");
}

std::optional<std::string> HostdCommandLine::config_path() const {
  return FindOptionValue("--config");
}

std::string HostdCommandLine::ResolveDbPath(
    const std::optional<std::string>& db_arg) const {
  return db_arg.value_or(DefaultDbPath());
}

std::string HostdCommandLine::ResolveArtifactsRoot(
    const std::optional<std::string>& artifacts_root_arg) const {
  return artifacts_root_arg.value_or(DefaultArtifactsRoot());
}

std::string HostdCommandLine::ResolveStateRoot(
    const std::optional<std::string>& state_root_arg) const {
  return state_root_arg.value_or(DefaultStateRoot());
}

ComposeMode HostdCommandLine::ResolveComposeMode(
    const std::optional<std::string>& compose_mode_arg) const {
  if (!compose_mode_arg.has_value() || *compose_mode_arg == "skip") {
    return ComposeMode::Skip;
  }
  if (*compose_mode_arg == "exec") {
    return ComposeMode::Exec;
  }
  throw std::runtime_error("unsupported compose mode '" + *compose_mode_arg + "'");
}

std::optional<std::string> HostdCommandLine::FindOptionValue(
    const std::string& option_name) const {
  for (int index = 2; index < argc_; ++index) {
    const std::string arg = argv_[index];
    if (arg == option_name && index + 1 < argc_) {
      return std::string(argv_[index + 1]);
    }
  }
  return std::nullopt;
}

std::string HostdCommandLine::DefaultDbPath() {
  return (std::filesystem::path("var") / "controller.sqlite").string();
}

std::string HostdCommandLine::DefaultArtifactsRoot() {
  return (std::filesystem::path("var") / "artifacts").string();
}

std::string HostdCommandLine::DefaultStateRoot() {
  return (std::filesystem::path("var") / "hostd-state").string();
}

}  // namespace naim::hostd
