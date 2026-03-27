#include "../include/controller_command_line.h"

#include <cctype>
#include <filesystem>
#include <fstream>
#include <sstream>
#include <stdexcept>

namespace comet::controller {

namespace {

constexpr int kFirstOptionIndex = 2;

}  // namespace

ControllerCommandLine::ControllerCommandLine(int argc, char** argv) {
  args_.reserve(static_cast<std::size_t>(argc));
  for (int index = 0; index < argc; ++index) {
    args_.emplace_back(argv[index] == nullptr ? "" : argv[index]);
  }
  if (argc > 1 && argv[1] != nullptr) {
    command_ = argv[1];
  }
}

bool ControllerCommandLine::HasCommand() const {
  return !command_.empty();
}

const std::string& ControllerCommandLine::command() const {
  return command_;
}

void ControllerCommandLine::PrintUsage(std::ostream& output) const {
  output
      << "Usage:\n"
      << "  comet-controller show-demo-plan\n"
      << "  comet-controller render-demo-compose [--node <node-name>]\n"
      << "  comet-controller init-db [--db <path>]\n"
      << "  comet-controller seed-demo [--db <path>]\n"
      << "  comet-controller validate-bundle --bundle <dir>\n"
      << "  comet-controller preview-bundle --bundle <dir> [--node <node-name>]\n"
      << "  comet-controller plan-bundle --bundle <dir> [--db <path>]\n"
      << "  comet-controller plan-host-ops --bundle <dir> [--db <path>] [--artifacts-root <path>] [--node <node-name>]\n"
      << "  comet-controller apply-state-file --state <path> [--db <path>] [--artifacts-root <path>]\n"
      << "  comet-controller apply-bundle --bundle <dir> [--db <path>] [--artifacts-root <path>]\n"
      << "  comet-controller import-bundle --bundle <dir> [--db <path>]\n"
      << "  comet-controller show-host-assignments [--db <path>] [--node <node-name>]\n"
      << "  comet-controller show-host-observations [--db <path>] [--plane <plane-name>] [--node <node-name>] [--stale-after <seconds>]\n"
      << "  comet-controller show-host-health [--db <path>] [--node <node-name>] [--stale-after <seconds>]\n"
      << "  comet-controller show-disk-state [--db <path>] [--plane <plane-name>] [--node <node-name>]\n"
      << "  comet-controller show-rollout-actions [--db <path>] [--plane <plane-name>] [--node <node-name>]\n"
      << "  comet-controller show-rebalance-plan [--db <path>] [--plane <plane-name>] [--node <node-name>]\n"
      << "  comet-controller show-events [--db <path>] [--plane <plane-name>] [--node <node-name>] [--worker <worker-name>] [--category <category>] [--limit <count>]\n"
      << "  comet-controller apply-rebalance-proposal --worker <worker-name> [--db <path>] [--artifacts-root <path>]\n"
      << "  comet-controller reconcile-rebalance-proposals [--db <path>] [--artifacts-root <path>]\n"
      << "  comet-controller scheduler-tick [--db <path>] [--artifacts-root <path>]\n"
      << "  comet-controller set-rollout-action-status --id <action-id> --status <pending|acknowledged|ready-to-retry> [--message <text>] [--db <path>]\n"
      << "  comet-controller enqueue-rollout-eviction --id <action-id> [--db <path>]\n"
      << "  comet-controller reconcile-rollout-actions [--db <path>] [--artifacts-root <path>]\n"
      << "  comet-controller apply-ready-rollout-action --id <action-id> [--db <path>] [--artifacts-root <path>]\n"
      << "  comet-controller show-node-availability [--db <path>] [--node <node-name>]\n"
      << "  comet-controller set-node-availability --node <node-name> --availability <active|draining|unavailable> [--message <text>] [--db <path>]\n"
      << "  comet-controller retry-host-assignment --id <assignment-id> [--db <path>]\n"
      << "  comet-controller list-planes [--db <path>]\n"
      << "  comet-controller show-plane --plane <plane-name> [--db <path>]\n"
      << "  comet-controller start-plane --plane <plane-name> [--db <path>]\n"
      << "  comet-controller stop-plane --plane <plane-name> [--db <path>]\n"
      << "  comet-controller delete-plane --plane <plane-name> [--db <path>]\n"
      << "  comet-controller show-hostd-hosts [--db <path>] [--node <node-name>]\n"
      << "  comet-controller revoke-hostd --node <node-name> [--db <path>] [--message <text>]\n"
      << "  comet-controller rotate-hostd-key --node <node-name> --public-key <base64-or-file> [--db <path>] [--message <text>]\n"
      << "  comet-controller ensure-web-ui [--db <path>] [--web-ui-root <path>] [--listen-port <port>] [--controller-upstream <url>] [--compose-mode skip|exec]\n"
      << "  comet-controller show-web-ui-status [--db <path>] [--web-ui-root <path>]\n"
      << "  comet-controller stop-web-ui [--db <path>] [--web-ui-root <path>] [--compose-mode skip|exec]\n"
      << "  comet-controller show-state [--db <path>]\n"
      << "  comet-controller render-infer-runtime [--db <path>]\n"
      << "  comet-controller render-compose [--db <path>] [--node <node-name>]\n"
      << "  comet-controller serve [--db <path>] [--listen-host <host>] [--listen-port <port>] [--ui-root <path>]\n"
      << "\n"
      << "Remote operator CLI:\n"
      << "  most inspection and mutation commands also accept --controller <http://host:port>\n"
      << "  target resolution order: --controller, COMET_CONTROLLER, ~/.config/comet/controller\n"
      << "  explicit --db keeps the command local\n";
}

std::optional<std::string> ControllerCommandLine::node() const {
  return FindOptionValue("--node");
}

std::optional<std::string> ControllerCommandLine::db() const {
  return FindOptionValue("--db");
}

std::optional<std::string> ControllerCommandLine::bundle() const {
  return FindOptionValue("--bundle");
}

std::optional<std::string> ControllerCommandLine::plane() const {
  return FindOptionValue("--plane");
}

std::optional<std::string> ControllerCommandLine::artifacts_root() const {
  return FindOptionValue("--artifacts-root");
}

std::optional<std::string> ControllerCommandLine::listen_host() const {
  return FindOptionValue("--listen-host");
}

std::optional<int> ControllerCommandLine::listen_port() const {
  return FindIntOption("--listen-port");
}

std::optional<std::string> ControllerCommandLine::ui_root() const {
  return FindOptionValue("--ui-root");
}

std::optional<std::string> ControllerCommandLine::web_ui_root() const {
  return FindOptionValue("--web-ui-root");
}

std::optional<std::string> ControllerCommandLine::controller_upstream() const {
  return FindOptionValue("--controller-upstream");
}

std::optional<std::string> ControllerCommandLine::compose_mode() const {
  return FindOptionValue("--compose-mode");
}

std::optional<std::string> ControllerCommandLine::controller() const {
  return FindOptionValue("--controller");
}

std::optional<int> ControllerCommandLine::id() const {
  return FindIntOption("--id");
}

std::optional<int> ControllerCommandLine::stale_after() const {
  return FindIntOption("--stale-after");
}

std::optional<int> ControllerCommandLine::limit() const {
  return FindIntOption("--limit");
}

std::optional<std::string> ControllerCommandLine::availability() const {
  return FindOptionValue("--availability");
}

std::optional<std::string> ControllerCommandLine::message() const {
  return FindOptionValue("--message");
}

std::optional<std::string> ControllerCommandLine::status() const {
  return FindOptionValue("--status");
}

std::optional<std::string> ControllerCommandLine::worker() const {
  return FindOptionValue("--worker");
}

std::optional<std::string> ControllerCommandLine::category() const {
  return FindOptionValue("--category");
}

std::optional<std::string> ControllerCommandLine::public_key() const {
  return FindOptionValue("--public-key");
}

std::optional<std::string> ControllerCommandLine::public_key_base64() const {
  const auto value = public_key();
  if (!value.has_value()) {
    return std::nullopt;
  }
  const std::filesystem::path candidate(*value);
  if (std::filesystem::exists(candidate)) {
    return Trim(ReadTextFile(candidate.string()));
  }
  return value;
}

std::optional<std::string> ControllerCommandLine::state_file() const {
  return FindOptionValue("--state");
}

std::optional<std::string> ControllerCommandLine::FindOptionValue(const char* name) const {
  for (std::size_t index = kFirstOptionIndex; index < args_.size(); ++index) {
    if (args_[index] == name && index + 1 < args_.size()) {
      return args_[index + 1];
    }
  }
  return std::nullopt;
}

std::optional<int> ControllerCommandLine::FindIntOption(const char* name) const {
  const auto value = FindOptionValue(name);
  if (!value.has_value()) {
    return std::nullopt;
  }
  return std::stoi(*value);
}

std::string ControllerCommandLine::ReadTextFile(const std::string& path) {
  std::ifstream input(path);
  if (!input.is_open()) {
    throw std::runtime_error("failed to read file '" + path + "'");
  }
  std::ostringstream buffer;
  buffer << input.rdbuf();
  return buffer.str();
}

std::string ControllerCommandLine::Trim(std::string value) {
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

}  // namespace comet::controller
