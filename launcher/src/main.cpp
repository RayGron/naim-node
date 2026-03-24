#include <algorithm>
#include <atomic>
#include <chrono>
#include <cctype>
#include <csignal>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <optional>
#include <set>
#include <sstream>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

#include "comet/sqlite_store.h"
#include "comet/crypto_utils.h"

namespace {

namespace fs = std::filesystem;

std::atomic<bool> g_stop_requested{false};
std::vector<pid_t> g_child_pids;

struct InstallLayout {
  fs::path config_path = "/etc/comet-node/config.toml";
  fs::path state_root = "/var/lib/comet-node";
  fs::path log_root = "/var/log/comet-node";
  fs::path systemd_dir = "/etc/systemd/system";
};

std::optional<fs::path> InstallRootOverride() {
  const char* value = std::getenv("COMET_INSTALL_ROOT");
  if (value == nullptr || *value == '\0') {
    return std::nullopt;
  }
  return fs::path(value);
}

InstallLayout DefaultInstallLayout() {
  if (const auto root = InstallRootOverride(); root.has_value()) {
    return InstallLayout{
        *root / "etc/comet-node/config.toml",
        *root / "var/lib/comet-node",
        *root / "var/log/comet-node",
        *root / "etc/systemd/system",
    };
  }
  if (geteuid() != 0) {
    const fs::path home = std::getenv("HOME") != nullptr ? fs::path(std::getenv("HOME"))
                                                         : fs::current_path();
    return InstallLayout{
        home / ".config/comet-node/config.toml",
        home / ".local/share/comet-node",
        home / ".local/state/comet-node",
        home / ".config/systemd/user",
    };
  }
  return InstallLayout{};
}

fs::path DefaultControllerDbPath() {
  return DefaultInstallLayout().state_root / "controller.sqlite";
}

fs::path DefaultArtifactsRoot() {
  return DefaultInstallLayout().state_root / "artifacts";
}

fs::path DefaultWebUiRoot() {
  return DefaultInstallLayout().state_root / "web-ui";
}

fs::path DefaultRuntimeRoot() {
  return DefaultInstallLayout().state_root / "runtime";
}

fs::path DefaultHostdStateRoot() {
  return DefaultInstallLayout().state_root / "hostd-state";
}

struct ControllerInstallOptions {
  InstallLayout layout;
  fs::path binary_path;
  std::string listen_host = "0.0.0.0";
  int listen_port = 18080;
  bool with_hostd = false;
  bool with_web_ui = false;
  std::string compose_mode = "exec";
  std::string node_name = "local-hostd";
};

struct HostdInstallOptions {
  InstallLayout layout;
  fs::path binary_path;
  std::string controller_url;
  std::string controller_fingerprint;
  std::string node_name;
  std::string transport_mode = "out";
  std::string listen_address;
  std::string compose_mode = "exec";
};

struct ControllerRunOptions {
  fs::path db_path;
  fs::path artifacts_root;
  fs::path web_ui_root;
  std::string listen_host = "0.0.0.0";
  int listen_port = 18080;
  bool with_hostd = false;
  bool with_web_ui = false;
  std::string controller_upstream;
  std::string compose_mode = "exec";
  std::string hostd_compose_mode = "exec";
  std::string node_name = "local-hostd";
  fs::path runtime_root;
  fs::path state_root;
  int hostd_poll_interval_sec = 2;
};

struct HostdRunOptions {
  fs::path db_path;
  std::string controller_url;
  std::string controller_fingerprint;
  std::string node_name;
  fs::path runtime_root;
  fs::path state_root;
  fs::path host_private_key_path;
  std::string compose_mode = "exec";
  int poll_interval_sec = 2;
};

struct GeneratedControllerConfig {
  std::optional<std::string> listen_host;
  std::optional<int> listen_port;
  std::optional<fs::path> db_path;
  std::optional<fs::path> artifacts_root;
  std::optional<bool> web_ui_enabled;
  std::optional<bool> local_hostd_enabled;
  std::optional<fs::path> controller_private_key;
  std::optional<fs::path> controller_public_key;
};

struct GeneratedHostdConfig {
  std::optional<std::string> node_name;
  std::optional<std::string> controller_url;
  std::optional<std::string> transport_mode;
  std::optional<std::string> listen_address;
  std::optional<fs::path> runtime_root;
  std::optional<fs::path> state_root;
  std::optional<std::string> compose_mode;
  std::optional<fs::path> host_private_key;
  std::optional<fs::path> host_public_key;
  std::optional<std::string> trusted_controller_fingerprint;
};

struct GeneratedConfig {
  GeneratedControllerConfig controller;
  GeneratedHostdConfig hostd;
};

bool CommandExists(const std::string& command);
int RunShellCommand(const std::string& command);

void SignalHandler(int) {
  g_stop_requested.store(true);
  for (const pid_t pid : g_child_pids) {
    if (pid > 0) {
      kill(pid, SIGTERM);
    }
  }
}

void RegisterSignalHandlers() {
  struct sigaction action {};
  action.sa_handler = SignalHandler;
  sigemptyset(&action.sa_mask);
  action.sa_flags = 0;
  sigaction(SIGINT, &action, nullptr);
  sigaction(SIGTERM, &action, nullptr);
}

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

std::string Trim(const std::string& value) {
  std::size_t begin = 0;
  while (begin < value.size() && std::isspace(static_cast<unsigned char>(value[begin])) != 0) {
    ++begin;
  }
  std::size_t end = value.size();
  while (end > begin && std::isspace(static_cast<unsigned char>(value[end - 1])) != 0) {
    --end;
  }
  return value.substr(begin, end - begin);
}

std::string UnquoteTomlValue(const std::string& value) {
  const std::string trimmed = Trim(value);
  if (trimmed.size() >= 2 && trimmed.front() == '"' && trimmed.back() == '"') {
    return trimmed.substr(1, trimmed.size() - 2);
  }
  return trimmed;
}

bool ParseTomlBool(const std::string& value) {
  const std::string lowered = Trim(value);
  if (lowered == "true") {
    return true;
  }
  if (lowered == "false") {
    return false;
  }
  throw std::runtime_error("invalid boolean value '" + value + "'");
}

bool SystemdAvailable() {
  if (!CommandExists("systemctl")) {
    return false;
  }
  if (geteuid() == 0) {
    return RunShellCommand("systemctl is-system-running >/dev/null 2>&1") == 0;
  }
  return RunShellCommand("systemctl --user is-system-running >/dev/null 2>&1") == 0;
}

bool RunningInManagedServiceMode() {
  const char* value = std::getenv("COMET_SERVICE_MODE");
  return value != nullptr && std::string(value) == "1";
}

bool IsUserServiceLayout(const InstallLayout& layout) {
  const std::string rendered = layout.systemd_dir.string();
  return rendered.find(".config/systemd/user") != std::string::npos;
}

std::optional<fs::path> ResolveConfigPathFromEnvOrDefault() {
  const char* env = std::getenv("COMET_CONFIG");
  if (env != nullptr && *env != '\0') {
    return fs::path(env);
  }
  const fs::path default_path = DefaultInstallLayout().config_path;
  if (fs::exists(default_path)) {
    return default_path;
  }
  return std::nullopt;
}

GeneratedConfig LoadGeneratedConfig(const fs::path& path) {
  std::ifstream input(path);
  if (!input.is_open()) {
    throw std::runtime_error("failed to read config '" + path.string() + "'");
  }

  GeneratedConfig config;
  std::string current_section;
  std::string line;
  while (std::getline(input, line)) {
    const std::string trimmed = Trim(line);
    if (trimmed.empty() || trimmed[0] == '#') {
      continue;
    }
    if (trimmed.size() >= 3 && trimmed.front() == '[' && trimmed.back() == ']') {
      current_section = trimmed.substr(1, trimmed.size() - 2);
      continue;
    }
    const std::size_t equals = trimmed.find('=');
    if (equals == std::string::npos) {
      continue;
    }
    const std::string key = Trim(trimmed.substr(0, equals));
    const std::string raw_value = Trim(trimmed.substr(equals + 1));
    if (current_section == "controller") {
      if (key == "listen_host") {
        config.controller.listen_host = UnquoteTomlValue(raw_value);
      } else if (key == "listen_port") {
        config.controller.listen_port = std::stoi(raw_value);
      } else if (key == "db_path") {
        config.controller.db_path = fs::path(UnquoteTomlValue(raw_value));
      } else if (key == "artifacts_root") {
        config.controller.artifacts_root = fs::path(UnquoteTomlValue(raw_value));
      } else if (key == "web_ui_enabled") {
        config.controller.web_ui_enabled = ParseTomlBool(raw_value);
      } else if (key == "local_hostd_enabled") {
        config.controller.local_hostd_enabled = ParseTomlBool(raw_value);
      } else if (key == "controller_private_key") {
        config.controller.controller_private_key = fs::path(UnquoteTomlValue(raw_value));
      } else if (key == "controller_public_key") {
        config.controller.controller_public_key = fs::path(UnquoteTomlValue(raw_value));
      }
    } else if (current_section == "hostd") {
      if (key == "node_name") {
        config.hostd.node_name = UnquoteTomlValue(raw_value);
      } else if (key == "controller_url") {
        config.hostd.controller_url = UnquoteTomlValue(raw_value);
      } else if (key == "transport_mode") {
        config.hostd.transport_mode = UnquoteTomlValue(raw_value);
      } else if (key == "listen_address") {
        config.hostd.listen_address = UnquoteTomlValue(raw_value);
      } else if (key == "runtime_root") {
        config.hostd.runtime_root = fs::path(UnquoteTomlValue(raw_value));
      } else if (key == "state_root") {
        config.hostd.state_root = fs::path(UnquoteTomlValue(raw_value));
      } else if (key == "compose_mode") {
        config.hostd.compose_mode = UnquoteTomlValue(raw_value);
      } else if (key == "host_private_key") {
        config.hostd.host_private_key = fs::path(UnquoteTomlValue(raw_value));
      } else if (key == "host_public_key") {
        config.hostd.host_public_key = fs::path(UnquoteTomlValue(raw_value));
      } else if (key == "trusted_controller_fingerprint") {
        config.hostd.trusted_controller_fingerprint = UnquoteTomlValue(raw_value);
      }
    }
  }
  return config;
}

std::string DefaultNodeName() {
  return "local-hostd";
}

std::string ReadTextFile(const fs::path& path) {
  std::ifstream input(path);
  if (!input) {
    throw std::runtime_error("failed to read file '" + path.string() + "'");
  }
  std::ostringstream buffer;
  buffer << input.rdbuf();
  return buffer.str();
}

void WriteTextFile(const fs::path& path, const std::string& content) {
  fs::create_directories(path.parent_path());
  std::ofstream output(path);
  if (!output) {
    throw std::runtime_error("failed to write file '" + path.string() + "'");
  }
  output << content;
}

bool CommandExists(const std::string& command) {
  const std::string probe =
      "command -v " + ShellEscape(command) + " >/dev/null 2>&1";
  return std::system(probe.c_str()) == 0;
}

int RunShellCommand(const std::string& command) {
  return std::system(command.c_str());
}

std::string CaptureShellOutput(const std::string& command) {
  FILE* pipe = popen(command.c_str(), "r");
  if (pipe == nullptr) {
    return "";
  }
  std::array<char, 256> buffer{};
  std::string output;
  while (fgets(buffer.data(), static_cast<int>(buffer.size()), pipe) != nullptr) {
    output += buffer.data();
  }
  pclose(pipe);
  return output;
}

int RunCommand(const std::vector<std::string>& args) {
  if (args.empty()) {
    return 1;
  }

  pid_t pid = fork();
  if (pid < 0) {
    throw std::runtime_error("fork failed");
  }

  if (pid == 0) {
    std::vector<char*> raw_args;
    raw_args.reserve(args.size() + 1);
    for (const std::string& arg : args) {
      raw_args.push_back(const_cast<char*>(arg.c_str()));
    }
    raw_args.push_back(nullptr);
    execv(raw_args.front(), raw_args.data());
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
}

pid_t SpawnCommand(const std::vector<std::string>& args) {
  if (args.empty()) {
    throw std::runtime_error("cannot spawn empty command");
  }

  pid_t pid = fork();
  if (pid < 0) {
    throw std::runtime_error("fork failed");
  }
  if (pid == 0) {
    std::vector<char*> raw_args;
    raw_args.reserve(args.size() + 1);
    for (const std::string& arg : args) {
      raw_args.push_back(const_cast<char*>(arg.c_str()));
    }
    raw_args.push_back(nullptr);
    execv(raw_args.front(), raw_args.data());
    std::perror("execv");
    _exit(127);
  }
  g_child_pids.push_back(pid);
  return pid;
}

void RemoveChildPid(pid_t pid) {
  g_child_pids.erase(
      std::remove(g_child_pids.begin(), g_child_pids.end(), pid),
      g_child_pids.end());
}

std::optional<std::string> FindFlagValue(
    const std::vector<std::string>& args,
    const std::string& flag) {
  for (std::size_t index = 0; index < args.size(); ++index) {
    if (args[index] == flag) {
      if (index + 1 >= args.size()) {
        throw std::runtime_error("missing value for " + flag);
      }
      return args[index + 1];
    }
  }
  return std::nullopt;
}

bool HasFlag(const std::vector<std::string>& args, const std::string& flag) {
  return std::find(args.begin(), args.end(), flag) != args.end();
}

int ParseInt(const std::optional<std::string>& value, int fallback) {
  if (!value.has_value()) {
    return fallback;
  }
  return std::stoi(*value);
}

fs::path ResolveSelfPath(const char* argv0) {
  std::error_code error;
  const fs::path proc_path = fs::read_symlink("/proc/self/exe", error);
  if (!error && !proc_path.empty()) {
    return proc_path;
  }
  return fs::weakly_canonical(fs::path(argv0));
}

fs::path ResolveSiblingBinary(const fs::path& self_path, const std::string& binary_name) {
  const fs::path sibling = self_path.parent_path() / binary_name;
  if (!fs::exists(sibling)) {
    throw std::runtime_error("required binary not found: " + sibling.string());
  }
  return sibling;
}

void EnsureKeypair(const fs::path& private_key_path, const fs::path& public_key_path) {
  if (fs::exists(private_key_path) && fs::exists(public_key_path)) {
    return;
  }
  fs::create_directories(private_key_path.parent_path());
  const auto keypair = comet::GenerateSigningKeypair();
  WriteTextFile(private_key_path, keypair.private_key_base64 + "\n");
  WriteTextFile(public_key_path, keypair.public_key_base64 + "\n");
  fs::permissions(
      private_key_path,
      fs::perms::owner_read | fs::perms::owner_write,
      fs::perm_options::replace);
}

std::string ComputePublicKeyFingerprint(const fs::path& public_key_path) {
  return comet::ComputeKeyFingerprintHex(Trim(ReadTextFile(public_key_path)));
}

std::string ReadPublicKeyArgument(const std::string& value) {
  const fs::path candidate(value);
  if (fs::exists(candidate)) {
    return ReadTextFile(candidate);
  }
  return value;
}

std::string DefaultWebUiControllerUpstream(int listen_port) {
  const std::string host_ips = Trim(CaptureShellOutput("hostname -I 2>/dev/null"));
  if (!host_ips.empty()) {
    const std::size_t first_space = host_ips.find_first_of(" \t\r\n");
    const std::string host_ip =
        first_space == std::string::npos ? host_ips : host_ips.substr(0, first_space);
    if (!host_ip.empty()) {
      return "http://" + host_ip + ":" + std::to_string(listen_port);
    }
  }
  return "http://host.docker.internal:" + std::to_string(listen_port);
}

std::string RenderConfigToml(
    const ControllerInstallOptions* controller,
    const HostdInstallOptions* hostd,
    const fs::path& controller_private_key,
    const fs::path& controller_public_key,
    const fs::path& hostd_private_key,
    const fs::path& hostd_public_key,
    const std::string& controller_fingerprint) {
  std::ostringstream out;
  out << "# generated by comet-node\n";
  if (controller != nullptr) {
    out << "[controller]\n";
    out << "listen_host = \"" << controller->listen_host << "\"\n";
    out << "listen_port = " << controller->listen_port << "\n";
    out << "db_path = \"" << (controller->layout.state_root / "controller.sqlite").string() << "\"\n";
    out << "artifacts_root = \"" << (controller->layout.state_root / "artifacts").string() << "\"\n";
    out << "web_ui_enabled = " << (controller->with_web_ui ? "true" : "false") << "\n";
    out << "local_hostd_enabled = " << (controller->with_hostd ? "true" : "false") << "\n";
    out << "controller_private_key = \"" << controller_private_key.string() << "\"\n";
    out << "controller_public_key = \"" << controller_public_key.string() << "\"\n";
    out << "web_ui_image = \"comet/web-ui:dev\"\n";
    out << "worker_image = \"comet/worker-runtime:dev\"\n";
    out << "infer_image = \"comet/infer-runtime:dev\"\n\n";
  }
  if (hostd != nullptr) {
    out << "[hostd]\n";
    out << "node_name = \"" << hostd->node_name << "\"\n";
    out << "controller_url = \"" << hostd->controller_url << "\"\n";
    out << "transport_mode = \"" << hostd->transport_mode << "\"\n";
    out << "listen_address = \"" << hostd->listen_address << "\"\n";
    out << "runtime_root = \"" << (hostd->layout.state_root / "runtime").string() << "\"\n";
    out << "state_root = \"" << (hostd->layout.state_root / "hostd-state").string() << "\"\n";
    out << "compose_mode = \"" << hostd->compose_mode << "\"\n";
    out << "host_private_key = \"" << hostd_private_key.string() << "\"\n";
    out << "host_public_key = \"" << hostd_public_key.string() << "\"\n";
    out << "trusted_controller_fingerprint = \"" << controller_fingerprint << "\"\n\n";
  }
  return out.str();
}

std::string RenderControllerUnit(
    const ControllerInstallOptions& options,
    const fs::path& config_path) {
  const bool user_service = IsUserServiceLayout(options.layout);
  std::ostringstream out;
  out << "[Unit]\n";
  out << "Description=Comet Node Controller\n";
  out << "After=network-online.target";
  if (!user_service) {
    out << " docker.service";
  }
  out << "\n";
  out << "Wants=network-online.target\n\n";
  out << "[Service]\n";
  out << "Type=simple\n";
  out << "WorkingDirectory=" << options.layout.state_root.string() << "\n";
  out << "ExecStart=" << options.binary_path.string()
      << " run controller"
      << " --db " << (options.layout.state_root / "controller.sqlite").string()
      << " --artifacts-root " << (options.layout.state_root / "artifacts").string()
      << " --listen-host " << options.listen_host
      << " --listen-port " << options.listen_port
      << " --web-ui-root " << (options.layout.state_root / "web-ui").string()
      << " --runtime-root " << (options.layout.state_root / "runtime").string()
      << " --state-root " << (options.layout.state_root / "hostd-state").string()
      << " --node " << options.node_name
      << " --compose-mode " << options.compose_mode;
  if (options.with_web_ui) {
    out << " --with-web-ui";
  }
  out << "\n";
  out << "Restart=always\n";
  out << "RestartSec=2\n";
  out << "Environment=COMET_CONFIG=" << config_path.string() << "\n";
  out << "Environment=COMET_SERVICE_MODE=1\n\n";
  out << "[Install]\n";
  out << "WantedBy=" << (user_service ? "default.target" : "multi-user.target") << "\n";
  return out.str();
}

std::string RenderHostdUnit(
    const HostdInstallOptions& options,
    const fs::path& config_path) {
  const bool user_service = IsUserServiceLayout(options.layout);
  std::ostringstream out;
  out << "[Unit]\n";
  out << "Description=Comet Node Host Agent\n";
  out << "After=network-online.target";
  if (!user_service) {
    out << " docker.service";
  }
  out << "\n";
  out << "Wants=network-online.target\n\n";
  out << "[Service]\n";
  out << "Type=simple\n";
  out << "WorkingDirectory=" << options.layout.state_root.string() << "\n";
  out << "ExecStart=" << options.binary_path.string()
      << " run hostd"
      << " --node " << options.node_name
      << " --runtime-root " << (options.layout.state_root / "runtime").string()
      << " --state-root " << (options.layout.state_root / "hostd-state").string()
      << " --compose-mode " << options.compose_mode;
  if (!options.controller_url.empty()) {
    out << " --controller " << options.controller_url;
    if (!options.controller_fingerprint.empty()) {
      out << " --controller-fingerprint " << options.controller_fingerprint;
    }
  } else {
    out << " --db " << (options.layout.state_root / "controller.sqlite").string();
  }
  out << "\n";
  out << "Restart=always\n";
  out << "RestartSec=2\n";
  out << "Environment=COMET_CONFIG=" << config_path.string() << "\n";
  out << "Environment=COMET_SERVICE_MODE=1\n\n";
  out << "[Install]\n";
  out << "WantedBy=" << (user_service ? "default.target" : "multi-user.target") << "\n";
  return out.str();
}

std::vector<std::string> ParseRoleTargets(const std::string& role) {
  if (role == "controller") {
    return {"comet-node-controller.service"};
  }
  if (role == "hostd") {
    return {"comet-node-hostd.service"};
  }
  if (role == "controller-hostd") {
    return {"comet-node-controller.service", "comet-node-hostd.service"};
  }
  throw std::runtime_error("unknown service role '" + role + "'");
}

void MaybeRunSystemctl(
    const std::vector<std::string>& units,
    const std::vector<std::string>& actions,
    bool skip_systemctl) {
  if (skip_systemctl) {
    return;
  }
  if (!CommandExists("systemctl")) {
    throw std::runtime_error("systemctl is required for service lifecycle commands");
  }
  for (const std::string& action : actions) {
    std::ostringstream command;
    command << "systemctl";
    if (geteuid() != 0) {
      command << " --user";
    }
    command << " " << action;
    if (action != "daemon-reload") {
      for (const std::string& unit : units) {
        command << " " << unit;
      }
    }
    if (RunShellCommand(command.str()) != 0) {
      throw std::runtime_error("systemctl " + action + " failed");
    }
  }
}

void PrintUsage() {
  std::cout
      << "usage:\n"
      << "  quick start (controller + local hostd + web ui):\n"
      << "    comet-node install controller --with-hostd --with-web-ui\n"
      << "    comet-node run controller\n"
      << "  quick start (remote hostd):\n"
      << "    comet-node install hostd --controller http://controller:18080\n"
      << "    comet-node connect-hostd --db /var/lib/comet-node/controller.sqlite --node <node> --public-key /var/lib/comet-node/keys/hostd.pub.pem\n"
      << "    comet-node run hostd\n"
      << "  comet-node version\n"
      << "  comet-node doctor [controller|hostd]\n"
      << "  comet-node run controller [--db <path>] [--artifacts-root <path>] [--listen-host <host>] [--listen-port <port>] [--with-hostd] [--with-web-ui] [--hostd-compose-mode exec|skip]\n"
      << "  comet-node run hostd [--node <name>] [--db <path>] [--controller <url>] [--controller-fingerprint <sha256>] [--runtime-root <path>] [--state-root <path>] [--compose-mode exec|skip]\n"
      << "  comet-node install controller [--with-hostd] [--with-web-ui] [--config <path>] [--state-root <path>] [--log-root <path>] [--systemd-dir <path>] [--skip-systemctl]\n"
      << "  comet-node install hostd [--node <name>] [--controller <url>] [--controller-fingerprint <sha256>] [--transport out|in|hybrid] [--listen <addr>] [--config <path>] [--state-root <path>] [--log-root <path>] [--systemd-dir <path>] [--skip-systemctl]\n"
      << "  comet-node service status|start|stop|restart|uninstall|verify <controller|hostd|controller-hostd> [--systemd-dir <path>] [--skip-systemctl]\n"
      << "  comet-node connect-hostd --db <path> --node <name> --public-key <pem-or-file> [--address <hostd-url>] [--transport out|in|hybrid] [--controller-fingerprint <sha256>]\n";
}

int RunHostdLoop(const fs::path& hostd_binary, const HostdRunOptions& options) {
  if (options.controller_url.empty() && options.db_path.empty()) {
    throw std::runtime_error("--db is required for current hostd run mode");
  }

  std::cout << "hostd_node=" << options.node_name << "\n";
  if (!options.controller_url.empty()) {
    std::cout << "hostd_mode=remote\n";
    std::cout << "controller_url=" << options.controller_url << "\n";
  } else {
    std::cout << "hostd_mode=local-db\n";
    std::cout << "db_path=" << options.db_path << "\n";
  }
  std::cout << "next_step=leave hostd running so it can receive assignments and upload telemetry\n";

  while (!g_stop_requested.load()) {
    std::vector<std::string> apply_args = {
        hostd_binary.string(),
        "apply-next-assignment",
        "--node",
        options.node_name,
        "--runtime-root",
        options.runtime_root.string(),
        "--state-root",
        options.state_root.string(),
        "--compose-mode",
        options.compose_mode,
    };
    if (!options.controller_url.empty()) {
      apply_args.insert(apply_args.end(), {"--controller", options.controller_url});
      if (!options.controller_fingerprint.empty()) {
        apply_args.insert(
            apply_args.end(),
            {"--controller-fingerprint", options.controller_fingerprint});
      }
      if (!options.host_private_key_path.empty()) {
        apply_args.insert(
            apply_args.end(),
            {"--host-private-key", options.host_private_key_path.string()});
      }
    } else {
      apply_args.insert(apply_args.end(), {"--db", options.db_path.string()});
    }
    const int apply_code = RunCommand(apply_args);
    if (apply_code != 0) {
      std::cerr << "comet-node: hostd apply-next-assignment exit=" << apply_code << "\n";
    }

    std::vector<std::string> report_args = {
        hostd_binary.string(),
        "report-observed-state",
        "--node",
        options.node_name,
        "--state-root",
        options.state_root.string(),
    };
    if (!options.controller_url.empty()) {
      report_args.insert(report_args.end(), {"--controller", options.controller_url});
      if (!options.controller_fingerprint.empty()) {
        report_args.insert(
            report_args.end(),
            {"--controller-fingerprint", options.controller_fingerprint});
      }
      if (!options.host_private_key_path.empty()) {
        report_args.insert(
            report_args.end(),
            {"--host-private-key", options.host_private_key_path.string()});
      }
    } else {
      report_args.insert(report_args.end(), {"--db", options.db_path.string()});
    }
    const int report_code = RunCommand(report_args);
    if (report_code != 0) {
      std::cerr << "comet-node: hostd report-observed-state exit=" << report_code << "\n";
    }

    for (int second = 0; second < options.poll_interval_sec && !g_stop_requested.load(); ++second) {
      std::this_thread::sleep_for(std::chrono::seconds(1));
    }
  }
  return 0;
}

void PrepareControllerRuntime(const ControllerRunOptions& options) {
  fs::create_directories(options.db_path.parent_path());
  fs::create_directories(options.artifacts_root);
  fs::create_directories(options.runtime_root);
  fs::create_directories(options.state_root);
  if (options.with_web_ui) {
    fs::create_directories(options.web_ui_root);
  }

  const fs::path keys_root = options.state_root.parent_path() / "keys";
  EnsureKeypair(keys_root / "controller.pem", keys_root / "controller.pub.pem");
  if (options.with_hostd) {
    EnsureKeypair(keys_root / "hostd.pem", keys_root / "hostd.pub.pem");
  }

  comet::ControllerStore store(options.db_path.string());
  store.Initialize();
}

int RunControllerSupervisor(
    const fs::path& self_path,
    const fs::path& controller_binary,
    const ControllerRunOptions& options) {
  PrepareControllerRuntime(options);
  const std::string local_controller_url =
      "http://127.0.0.1:" + std::to_string(options.listen_port);
  const std::string web_ui_controller_upstream =
      options.controller_upstream.empty()
          ? DefaultWebUiControllerUpstream(options.listen_port)
          : options.controller_upstream;
  const fs::path controller_public_key_path = options.state_root.parent_path() / "keys" / "controller.pub.pem";
  const std::string controller_fingerprint =
      fs::exists(controller_public_key_path)
          ? ComputePublicKeyFingerprint(controller_public_key_path)
          : "";
  if (options.with_web_ui) {
    std::vector<std::string> ensure_args = {
        controller_binary.string(),
        "ensure-web-ui",
        "--db",
        options.db_path.string(),
        "--web-ui-root",
        options.web_ui_root.string(),
        "--listen-port",
        "18081",
        "--controller-upstream",
        web_ui_controller_upstream,
        "--compose-mode",
        options.compose_mode,
    };
    const int ensure_code = RunCommand(ensure_args);
    if (ensure_code != 0) {
      throw std::runtime_error("failed to ensure comet-web-ui");
    }
  }

  const pid_t controller_pid = SpawnCommand({
      controller_binary.string(),
      "serve",
      "--db",
      options.db_path.string(),
      "--artifacts-root",
      options.artifacts_root.string(),
      "--listen-host",
      options.listen_host,
      "--listen-port",
      std::to_string(options.listen_port),
  });

  pid_t hostd_pid = -1;
  if (options.with_hostd) {
    comet::ControllerStore store(options.db_path.string());
    store.Initialize();
    comet::RegisteredHostRecord host;
    if (const auto current = store.LoadRegisteredHost(options.node_name); current.has_value()) {
      host = *current;
    }
    host.node_name = options.node_name;
    host.advertised_address = local_controller_url;
    host.public_key_pem = Trim(ReadTextFile(options.state_root.parent_path() / "keys" / "hostd.pub.pem"));
    host.transport_mode = "out";
    host.registration_state = "registered";
    host.session_state = "disconnected";
    host.status_message = "auto-registered local hostd by comet-node run controller";
    store.UpsertRegisteredHost(host);
    hostd_pid = SpawnCommand({
      self_path.string(),
      "run",
      "hostd",
      "--controller",
      local_controller_url,
      "--controller-fingerprint",
      controller_fingerprint,
      "--node",
      options.node_name,
      "--runtime-root",
      options.runtime_root.string(),
      "--state-root",
      options.state_root.string(),
      "--host-private-key",
      (options.state_root.parent_path() / "keys" / "hostd.pem").string(),
      "--compose-mode",
      options.hostd_compose_mode,
      "--poll-interval-sec",
      std::to_string(options.hostd_poll_interval_sec),
    });
  }

  std::cout << "controller_api_url=" << local_controller_url << "\n";
  if (options.with_web_ui) {
    std::cout << "web_ui_url=http://127.0.0.1:18081\n";
    std::cout << "next_step=open the Web UI and load a plane\n";
  } else {
    std::cout << "next_step=use controller API or CLI to load a plane\n";
  }

  while (!g_stop_requested.load()) {
    int status = 0;
    const pid_t exited = waitpid(-1, &status, 0);
    if (exited < 0) {
      break;
    }
    RemoveChildPid(exited);
    if (exited == controller_pid || exited == hostd_pid) {
      g_stop_requested.store(true);
      break;
    }
  }

  if (controller_pid > 0) {
    kill(controller_pid, SIGTERM);
  }
  if (hostd_pid > 0) {
    kill(hostd_pid, SIGTERM);
  }
  return 0;
}

InstallLayout ParseLayout(const std::vector<std::string>& args) {
  InstallLayout layout = DefaultInstallLayout();
  if (const auto value = FindFlagValue(args, "--config")) {
    layout.config_path = *value;
  }
  if (const auto value = FindFlagValue(args, "--state-root")) {
    layout.state_root = *value;
  }
  if (const auto value = FindFlagValue(args, "--log-root")) {
    layout.log_root = *value;
  }
  if (const auto value = FindFlagValue(args, "--systemd-dir")) {
    layout.systemd_dir = *value;
  }
  return layout;
}

void InstallController(const fs::path& self_path, const std::vector<std::string>& args) {
  ControllerInstallOptions options;
  options.layout = ParseLayout(args);
  options.binary_path = self_path;
  options.listen_host = FindFlagValue(args, "--listen-host").value_or(options.listen_host);
  options.listen_port = ParseInt(FindFlagValue(args, "--listen-port"), options.listen_port);
  options.compose_mode = FindFlagValue(args, "--compose-mode").value_or(options.compose_mode);
  options.node_name = FindFlagValue(args, "--node").value_or(options.node_name);
  options.with_hostd = HasFlag(args, "--with-hostd");
  options.with_web_ui = HasFlag(args, "--with-web-ui");
  const bool skip_systemctl = HasFlag(args, "--skip-systemctl");

  const fs::path keys_root = options.layout.state_root / "keys";
  const fs::path controller_private_key = keys_root / "controller.pem";
  const fs::path controller_public_key = keys_root / "controller.pub.pem";
  const fs::path hostd_private_key = keys_root / "hostd.pem";
  const fs::path hostd_public_key = keys_root / "hostd.pub.pem";
  fs::create_directories(options.layout.state_root);
  fs::create_directories(options.layout.log_root);
  fs::create_directories(options.layout.systemd_dir);
  EnsureKeypair(controller_private_key, controller_public_key);
  if (options.with_hostd) {
    EnsureKeypair(hostd_private_key, hostd_public_key);
  }
  const std::string controller_fingerprint = ComputePublicKeyFingerprint(controller_public_key);
  std::optional<HostdInstallOptions> local_hostd_options;
  if (options.with_hostd) {
    local_hostd_options = HostdInstallOptions{
        options.layout,
        self_path,
        "http://127.0.0.1:" + std::to_string(options.listen_port),
        controller_fingerprint,
        options.node_name,
        "out",
        "",
        options.compose_mode,
      };
  }
  {
    comet::ControllerStore store((options.layout.state_root / "controller.sqlite").string());
    store.Initialize();
    if (options.with_hostd) {
      comet::RegisteredHostRecord host;
      host.node_name = options.node_name;
      host.advertised_address = "http://127.0.0.1:" + std::to_string(options.listen_port);
      host.public_key_pem = Trim(ReadTextFile(hostd_public_key));
      host.controller_public_key_fingerprint = controller_fingerprint;
      host.transport_mode = "out";
      host.registration_state = "registered";
      host.session_state = "disconnected";
      host.status_message = "prepared by comet-node install controller";
      store.UpsertRegisteredHost(host);
    }
  }
  WriteTextFile(
      options.layout.config_path,
      RenderConfigToml(
          &options,
          local_hostd_options.has_value() ? &*local_hostd_options : nullptr,
          controller_private_key,
          controller_public_key,
          hostd_private_key,
          hostd_public_key,
          controller_fingerprint));

  WriteTextFile(
      options.layout.systemd_dir / "comet-node-controller.service",
      RenderControllerUnit(options, options.layout.config_path));
  if (options.with_hostd) {
    WriteTextFile(
        options.layout.systemd_dir / "comet-node-hostd.service",
        RenderHostdUnit(
            HostdInstallOptions{
                options.layout,
                self_path,
                "http://127.0.0.1:" + std::to_string(options.listen_port),
                controller_fingerprint,
                options.node_name,
                "out",
                "",
                options.compose_mode,
            },
            options.layout.config_path));
  }

  MaybeRunSystemctl({}, {"daemon-reload"}, skip_systemctl);
  MaybeRunSystemctl({"comet-node-controller.service"}, {"enable", "restart"}, skip_systemctl);
  if (options.with_hostd) {
    MaybeRunSystemctl({"comet-node-hostd.service"}, {"enable", "restart"}, skip_systemctl);
  }

  std::cout << "installed controller\n";
  std::cout << "controller_api_url=http://127.0.0.1:" << options.listen_port << "\n";
  if (options.with_web_ui) {
    std::cout << "web_ui_url=http://127.0.0.1:18081\n";
  }
  std::cout << "next_step="
            << (IsUserServiceLayout(options.layout) ? "systemctl --user" : "systemctl")
            << " status comet-node-controller.service\n";
}

void InstallHostd(const fs::path& self_path, const std::vector<std::string>& args) {
  HostdInstallOptions options;
  options.layout = ParseLayout(args);
  options.binary_path = self_path;
  options.controller_url = FindFlagValue(args, "--controller").value_or("");
  options.controller_fingerprint = FindFlagValue(args, "--controller-fingerprint").value_or("");
  options.node_name = FindFlagValue(args, "--node").value_or(DefaultNodeName());
  options.transport_mode = FindFlagValue(args, "--transport").value_or(options.transport_mode);
  options.listen_address = FindFlagValue(args, "--listen").value_or("");
  options.compose_mode = FindFlagValue(args, "--compose-mode").value_or(options.compose_mode);
  const bool skip_systemctl = HasFlag(args, "--skip-systemctl");
  const fs::path keys_root = options.layout.state_root / "keys";
  const fs::path controller_private_key = keys_root / "controller.pem";
  const fs::path controller_public_key = keys_root / "controller.pub.pem";
  const fs::path hostd_private_key = keys_root / "hostd.pem";
  const fs::path hostd_public_key = keys_root / "hostd.pub.pem";
  fs::create_directories(options.layout.state_root);
  fs::create_directories(options.layout.log_root);
  fs::create_directories(options.layout.systemd_dir);
  EnsureKeypair(hostd_private_key, hostd_public_key);
  const std::string controller_fingerprint =
      !options.controller_fingerprint.empty()
          ? options.controller_fingerprint
          : (fs::exists(controller_public_key) ? ComputePublicKeyFingerprint(controller_public_key) : "");
  WriteTextFile(
      options.layout.config_path,
      RenderConfigToml(
          nullptr,
          &options,
          controller_private_key,
          controller_public_key,
          hostd_private_key,
          hostd_public_key,
          controller_fingerprint));
  WriteTextFile(
      options.layout.systemd_dir / "comet-node-hostd.service",
      RenderHostdUnit(options, options.layout.config_path));
  MaybeRunSystemctl({}, {"daemon-reload"}, skip_systemctl);
  MaybeRunSystemctl({"comet-node-hostd.service"}, {"enable", "restart"}, skip_systemctl);
  std::cout << "installed hostd\n";
  std::cout << "node=" << options.node_name << "\n";
  if (!options.controller_url.empty()) {
    std::cout << "controller_url=" << options.controller_url << "\n";
    std::cout << "next_step_register=comet-node connect-hostd --db <controller-db> --node "
              << options.node_name
              << " --public-key " << (options.layout.state_root / "keys/hostd.pub.pem").string()
              << "\n";
  }
  std::cout << "next_step="
            << (IsUserServiceLayout(options.layout) ? "systemctl --user" : "systemctl")
            << " status comet-node-hostd.service\n";
}

void ServiceCommand(const std::string& action, const std::string& role, const std::vector<std::string>& args) {
  const InstallLayout layout = ParseLayout(args);
  const bool skip_systemctl = HasFlag(args, "--skip-systemctl");
  const std::vector<std::string> units = ParseRoleTargets(role);
  const auto verify_units = [&]() {
    if (!CommandExists("systemd-analyze")) {
      throw std::runtime_error("systemd-analyze is required for service verify");
    }
    std::ostringstream command;
    command << "systemd-analyze verify";
    for (const std::string& unit : units) {
      const fs::path unit_path = layout.systemd_dir / unit;
      if (!fs::exists(unit_path)) {
        throw std::runtime_error("missing unit file '" + unit_path.string() + "'");
      }
      command << " " << ShellEscape(unit_path.string());
    }
    if (RunShellCommand(command.str()) != 0) {
      throw std::runtime_error("systemd-analyze verify failed");
    }
  };
  if (action == "status") {
    for (const std::string& unit : units) {
      const bool exists = fs::exists(layout.systemd_dir / unit);
      std::cout << unit << " installed=" << (exists ? "yes" : "no") << "\n";
    }
    if (!skip_systemctl) {
      MaybeRunSystemctl(units, {"status"}, false);
    }
    return;
  }
  if (action == "start") {
    MaybeRunSystemctl(units, {"daemon-reload", "start"}, skip_systemctl);
    return;
  }
  if (action == "stop") {
    MaybeRunSystemctl(units, {"stop"}, skip_systemctl);
    return;
  }
  if (action == "restart") {
    MaybeRunSystemctl(units, {"daemon-reload", "restart"}, skip_systemctl);
    return;
  }
  if (action == "uninstall") {
    MaybeRunSystemctl(units, {"disable", "stop"}, skip_systemctl);
    for (const std::string& unit : units) {
      fs::remove(layout.systemd_dir / unit);
    }
    if (!skip_systemctl) {
      MaybeRunSystemctl({"comet-node-controller.service"}, {"daemon-reload"}, false);
    }
    return;
  }
  if (action == "verify") {
    verify_units();
    std::cout << "verified units for role=" << role << "\n";
    return;
  }
  throw std::runtime_error("unsupported service action '" + action + "'");
}

void RunDoctor(const fs::path& self_path, const std::optional<std::string>& role) {
  const std::set<std::string> required_commands = {"docker"};
  const fs::path controller_binary = self_path.parent_path() / "comet-controller";
  const fs::path hostd_binary = self_path.parent_path() / "comet-hostd";
  std::cout << "doctor\n";
  std::cout << "binary=" << self_path << "\n";
  if (!role.has_value() || *role == "controller") {
    std::cout << "controller_binary=" << (fs::exists(controller_binary) ? "yes" : "no") << "\n";
  }
  if (!role.has_value() || *role == "hostd") {
    std::cout << "hostd_binary=" << (fs::exists(hostd_binary) ? "yes" : "no") << "\n";
  }
  for (const std::string& command : required_commands) {
    std::cout << command << "=" << (CommandExists(command) ? "yes" : "no") << "\n";
  }
  std::cout << "systemctl=" << (CommandExists("systemctl") ? "yes" : "no") << "\n";
  std::cout << "systemd_analyze=" << (CommandExists("systemd-analyze") ? "yes" : "no") << "\n";
}

void ConnectHostd(const std::vector<std::string>& args) {
  const auto db_path = FindFlagValue(args, "--db");
  const auto node_name = FindFlagValue(args, "--node");
  const auto public_key = FindFlagValue(args, "--public-key");
  if (!db_path.has_value() || !node_name.has_value() || !public_key.has_value()) {
    throw std::runtime_error("--db, --node and --public-key are required for connect-hostd");
  }
  comet::ControllerStore store(*db_path);
  store.Initialize();
  comet::RegisteredHostRecord record;
  record.node_name = *node_name;
  record.advertised_address = FindFlagValue(args, "--address").value_or("");
  record.public_key_pem = ReadPublicKeyArgument(*public_key);
  record.controller_public_key_fingerprint =
      FindFlagValue(args, "--controller-fingerprint").value_or("");
  record.transport_mode = FindFlagValue(args, "--transport").value_or("out");
  record.registration_state = "registered";
  record.session_state = "disconnected";
  record.capabilities_json = "{}";
  record.status_message = "registered by comet-node connect-hostd";
  store.UpsertRegisteredHost(record);
  store.AppendEvent(comet::EventRecord{
      0,
      "",
      *node_name,
      "",
      std::nullopt,
      std::nullopt,
      "host-registry",
      "registered",
      "info",
      "registered hostd node",
      "{\"source\":\"comet-node connect-hostd\"}",
      "",
  });
  std::cout << "registered hostd node=" << *node_name << "\n";
}

}  // namespace

int main(int argc, char** argv) {
  RegisterSignalHandlers();
  const fs::path self_path = ResolveSelfPath(argv[0]);

  if (argc < 2) {
    PrintUsage();
    return 1;
  }

  const std::vector<std::string> args(argv + 1, argv + argc);
  const std::string command = args.front();

  try {
    if (command == "version") {
      std::cout << "comet-node 0.1.0\n";
      return 0;
    }

    if (command == "doctor") {
      if (args.size() > 1) {
        RunDoctor(self_path, std::optional<std::string>(args[1]));
      } else {
        RunDoctor(self_path, std::nullopt);
      }
      return 0;
    }

    if (command == "connect-hostd") {
      ConnectHostd(args);
      return 0;
    }

    if (command == "install") {
      if (args.size() < 2) {
        throw std::runtime_error("install requires role");
      }
      const std::vector<std::string> install_args(args.begin() + 2, args.end());
      if (args[1] == "controller") {
        InstallController(self_path, install_args);
        return 0;
      }
      if (args[1] == "hostd") {
        InstallHostd(self_path, install_args);
        return 0;
      }
      throw std::runtime_error("unknown install role '" + args[1] + "'");
    }

    if (command == "service") {
      if (args.size() < 3) {
        throw std::runtime_error("service requires action and role");
      }
      const std::vector<std::string> service_args(args.begin() + 3, args.end());
      ServiceCommand(args[1], args[2], service_args);
      return 0;
    }

    if (command == "run") {
      if (args.size() < 2) {
        throw std::runtime_error("run requires role");
      }
      if (args[1] == "controller") {
        ControllerRunOptions options;
        std::optional<GeneratedConfig> loaded_config;
        if (const auto config_path = ResolveConfigPathFromEnvOrDefault(); config_path.has_value()) {
          loaded_config = LoadGeneratedConfig(*config_path);
        }
        options.db_path =
            FindFlagValue(args, "--db")
                .value_or(loaded_config && loaded_config->controller.db_path.has_value()
                              ? loaded_config->controller.db_path->string()
                              : DefaultControllerDbPath().string());
        options.artifacts_root =
            FindFlagValue(args, "--artifacts-root")
                .value_or(loaded_config && loaded_config->controller.artifacts_root.has_value()
                              ? loaded_config->controller.artifacts_root->string()
                              : DefaultArtifactsRoot().string());
        options.web_ui_root =
            FindFlagValue(args, "--web-ui-root").value_or(DefaultWebUiRoot().string());
        options.listen_host =
            FindFlagValue(args, "--listen-host")
                .value_or(loaded_config && loaded_config->controller.listen_host.has_value()
                              ? *loaded_config->controller.listen_host
                              : options.listen_host);
        options.listen_port =
            ParseInt(
                FindFlagValue(args, "--listen-port"),
                loaded_config && loaded_config->controller.listen_port.has_value()
                    ? *loaded_config->controller.listen_port
                    : options.listen_port);
        options.controller_upstream = FindFlagValue(args, "--controller-upstream").value_or("");
        options.compose_mode =
            FindFlagValue(args, "--compose-mode")
                .value_or(loaded_config && loaded_config->hostd.compose_mode.has_value()
                              ? *loaded_config->hostd.compose_mode
                              : options.compose_mode);
        options.hostd_compose_mode =
            FindFlagValue(args, "--hostd-compose-mode")
                .value_or(loaded_config && loaded_config->hostd.compose_mode.has_value()
                              ? *loaded_config->hostd.compose_mode
                              : options.hostd_compose_mode);
        options.node_name =
            FindFlagValue(args, "--node")
                .value_or(loaded_config && loaded_config->hostd.node_name.has_value()
                              ? *loaded_config->hostd.node_name
                              : options.node_name);
        options.runtime_root =
            FindFlagValue(args, "--runtime-root")
                .value_or(loaded_config && loaded_config->hostd.runtime_root.has_value()
                              ? loaded_config->hostd.runtime_root->string()
                              : DefaultRuntimeRoot().string());
        options.state_root =
            FindFlagValue(args, "--state-root")
                .value_or(loaded_config && loaded_config->hostd.state_root.has_value()
                              ? loaded_config->hostd.state_root->string()
                              : DefaultHostdStateRoot().string());
        const bool config_local_hostd_enabled =
            loaded_config && loaded_config->controller.local_hostd_enabled.value_or(false);
        const bool managed_hostd_service_present =
            RunningInManagedServiceMode() &&
            fs::exists((ParseLayout(args)).systemd_dir / "comet-node-hostd.service");
        options.with_hostd =
            (HasFlag(args, "--with-hostd") ||
             (!HasFlag(args, "--without-hostd") && config_local_hostd_enabled)) &&
            !managed_hostd_service_present;
        options.with_web_ui =
            HasFlag(args, "--with-web-ui") ||
            (!HasFlag(args, "--without-web-ui") &&
             loaded_config && loaded_config->controller.web_ui_enabled.value_or(false));
        options.hostd_poll_interval_sec =
            ParseInt(FindFlagValue(args, "--poll-interval-sec"), options.hostd_poll_interval_sec);
        if (!RunningInManagedServiceMode() &&
            !HasFlag(args, "--foreground") &&
            !HasFlag(args, "--skip-systemctl") &&
            SystemdAvailable()) {
          const InstallLayout layout = ParseLayout(args);
          std::vector<std::string> install_args;
          if (layout.config_path != DefaultInstallLayout().config_path) {
            install_args.insert(
                install_args.end(), {"--config", layout.config_path.string()});
          }
          if (layout.state_root != DefaultInstallLayout().state_root) {
            install_args.insert(
                install_args.end(), {"--state-root", layout.state_root.string()});
          }
          if (layout.log_root != DefaultInstallLayout().log_root) {
            install_args.insert(
                install_args.end(), {"--log-root", layout.log_root.string()});
          }
          if (layout.systemd_dir != DefaultInstallLayout().systemd_dir) {
            install_args.insert(
                install_args.end(), {"--systemd-dir", layout.systemd_dir.string()});
          }
          install_args.insert(install_args.end(), {"--listen-host", options.listen_host});
          install_args.insert(
              install_args.end(), {"--listen-port", std::to_string(options.listen_port)});
          install_args.insert(
              install_args.end(), {"--compose-mode", options.hostd_compose_mode});
          install_args.insert(install_args.end(), {"--node", options.node_name});
          if (options.with_hostd) {
            install_args.push_back("--with-hostd");
          }
          if (options.with_web_ui) {
            install_args.push_back("--with-web-ui");
          }
          InstallController(self_path, install_args);
          std::cout << "service_mode=systemd\n";
          std::cout << "controller_service=comet-node-controller.service\n";
          if (options.with_hostd) {
            std::cout << "hostd_service=comet-node-hostd.service\n";
          }
          return 0;
        }
        const fs::path controller_binary = ResolveSiblingBinary(self_path, "comet-controller");
        return RunControllerSupervisor(self_path, controller_binary, options);
      }

      if (args[1] == "hostd") {
        HostdRunOptions options;
        options.db_path = FindFlagValue(args, "--db").value_or("");
        std::optional<GeneratedConfig> loaded_config;
        if (const auto config_path = ResolveConfigPathFromEnvOrDefault(); config_path.has_value()) {
          loaded_config = LoadGeneratedConfig(*config_path);
        }
        options.controller_url =
            FindFlagValue(args, "--controller")
                .value_or(loaded_config && loaded_config->hostd.controller_url.has_value()
                              ? *loaded_config->hostd.controller_url
                              : "");
        options.controller_fingerprint =
            FindFlagValue(args, "--controller-fingerprint")
                .value_or(loaded_config &&
                                  loaded_config->hostd.trusted_controller_fingerprint.has_value()
                              ? *loaded_config->hostd.trusted_controller_fingerprint
                              : "");
        options.node_name =
            FindFlagValue(args, "--node")
                .value_or(loaded_config && loaded_config->hostd.node_name.has_value()
                              ? *loaded_config->hostd.node_name
                              : DefaultNodeName());
        options.runtime_root =
            FindFlagValue(args, "--runtime-root")
                .value_or(loaded_config && loaded_config->hostd.runtime_root.has_value()
                              ? loaded_config->hostd.runtime_root->string()
                              : DefaultRuntimeRoot().string());
        options.state_root =
            FindFlagValue(args, "--state-root")
                .value_or(loaded_config && loaded_config->hostd.state_root.has_value()
                              ? loaded_config->hostd.state_root->string()
                              : DefaultHostdStateRoot().string());
        options.host_private_key_path =
            FindFlagValue(args, "--host-private-key")
                .value_or(loaded_config && loaded_config->hostd.host_private_key.has_value()
                              ? loaded_config->hostd.host_private_key->string()
                              : "");
        options.compose_mode = FindFlagValue(args, "--compose-mode").value_or(options.compose_mode);
        options.poll_interval_sec =
            ParseInt(FindFlagValue(args, "--poll-interval-sec"), options.poll_interval_sec);
        if (!RunningInManagedServiceMode() &&
            !HasFlag(args, "--foreground") &&
            !HasFlag(args, "--skip-systemctl") &&
            SystemdAvailable()) {
          const InstallLayout layout = ParseLayout(args);
          std::vector<std::string> install_args;
          if (layout.config_path != DefaultInstallLayout().config_path) {
            install_args.insert(
                install_args.end(), {"--config", layout.config_path.string()});
          }
          if (layout.state_root != DefaultInstallLayout().state_root) {
            install_args.insert(
                install_args.end(), {"--state-root", layout.state_root.string()});
          }
          if (layout.log_root != DefaultInstallLayout().log_root) {
            install_args.insert(
                install_args.end(), {"--log-root", layout.log_root.string()});
          }
          if (layout.systemd_dir != DefaultInstallLayout().systemd_dir) {
            install_args.insert(
                install_args.end(), {"--systemd-dir", layout.systemd_dir.string()});
          }
          if (!options.controller_url.empty()) {
            install_args.insert(
                install_args.end(), {"--controller", options.controller_url});
          }
          if (!options.controller_fingerprint.empty()) {
            install_args.insert(
                install_args.end(),
                {"--controller-fingerprint", options.controller_fingerprint});
          }
          install_args.insert(install_args.end(), {"--node", options.node_name});
          install_args.insert(
              install_args.end(), {"--compose-mode", options.compose_mode});
          InstallHostd(self_path, install_args);
          std::cout << "service_mode=systemd\n";
          std::cout << "hostd_service=comet-node-hostd.service\n";
          return 0;
        }
        const fs::path hostd_binary = ResolveSiblingBinary(self_path, "comet-hostd");
        return RunHostdLoop(hostd_binary, options);
      }

      throw std::runtime_error("unknown run role '" + args[1] + "'");
    }
  } catch (const std::exception& error) {
    std::cerr << "error: " << error.what() << "\n";
    return 1;
  }

  PrintUsage();
  return 1;
}
