#include "install/launcher_install_service.h"

#include <algorithm>
#include <cctype>
#include <fstream>
#include <iostream>
#include <optional>
#include <set>
#include <sstream>
#include <stdexcept>
#include <vector>

#include "naim/core/platform_compat.h"
#include "naim/security/crypto_utils.h"
#include "naim/state/sqlite_store.h"

namespace naim::launcher {

namespace {

bool IsIpv4Address(const std::string& candidate) {
  int octet_count = 0;
  std::string octet;
  std::istringstream input(candidate);
  while (std::getline(input, octet, '.')) {
    if (octet.empty() || octet.size() > 3) {
      return false;
    }
    for (const char ch : octet) {
      if (!std::isdigit(static_cast<unsigned char>(ch))) {
        return false;
      }
    }
    const int value = std::stoi(octet);
    if (value < 0 || value > 255) {
      return false;
    }
    ++octet_count;
  }
  return octet_count == 4;
}

bool IsPrivateIpv4Address(const std::string& candidate) {
  if (!IsIpv4Address(candidate)) {
    return false;
  }
  std::istringstream input(candidate);
  std::string octet_text;
  std::vector<int> octets;
  while (std::getline(input, octet_text, '.')) {
    octets.push_back(std::stoi(octet_text));
  }
  if (octets.size() != 4) {
    return false;
  }
  return octets[0] == 10 ||
         (octets[0] == 172 && octets[1] >= 16 && octets[1] <= 31) ||
         (octets[0] == 192 && octets[1] == 168);
}

}  // namespace

LauncherInstallService::LauncherInstallService(
    const InstallLayoutResolver& install_layout_resolver,
    const ProcessRunner& process_runner)
    : install_layout_resolver_(install_layout_resolver),
      process_runner_(process_runner) {}

InstallLayout LauncherInstallService::ParseLayout(
    const LauncherCommandLine& command_line) const {
  InstallLayout layout = install_layout_resolver_.DefaultInstallLayout();
  if (const auto value = command_line.FindFlagValue("--config")) {
    layout.config_path = *value;
  }
  if (const auto value = command_line.FindFlagValue("--state-root")) {
    layout.state_root = *value;
  }
  if (const auto value = command_line.FindFlagValue("--log-root")) {
    layout.log_root = *value;
  }
  if (const auto value = command_line.FindFlagValue("--systemd-dir")) {
    layout.systemd_dir = *value;
  }
  return layout;
}

void LauncherInstallService::InstallController(
    const std::filesystem::path& self_path,
    const LauncherCommandLine& command_line) const {
  ControllerInstallOptions options;
  options.layout = ParseLayout(command_line);
  options.binary_path = self_path;
  options.listen_host =
      command_line.FindFlagValue("--listen-host").value_or(options.listen_host);
  options.listen_port = LauncherCommandLine::ParseIntValue(
      command_line.FindFlagValue("--listen-port"), options.listen_port);
  options.internal_listen_host =
      command_line.FindFlagValue("--internal-listen-host").value_or(DefaultInternalListenHost());
  options.compose_mode =
      command_line.FindFlagValue("--compose-mode").value_or(options.compose_mode);
  options.node_name = command_line.FindFlagValue("--node").value_or(options.node_name);
  options.with_hostd = command_line.HasFlag("--with-hostd");
  options.with_web_ui = command_line.HasFlag("--with-web-ui");
  const bool skip_systemctl = command_line.HasFlag("--skip-systemctl");

  const auto keys_root = options.layout.state_root / "keys";
  const auto controller_private_key = keys_root / "controller.key.b64";
  const auto controller_public_key = keys_root / "controller.pub.b64";
  const auto hostd_private_key = keys_root / "hostd.key.b64";
  const auto hostd_public_key = keys_root / "hostd.pub.b64";
  std::filesystem::create_directories(options.layout.state_root);
  std::filesystem::create_directories(options.layout.log_root);
  std::filesystem::create_directories(options.layout.systemd_dir);
  EnsureKeypair(controller_private_key, controller_public_key);
  if (options.with_hostd) {
    EnsureKeypair(hostd_private_key, hostd_public_key);
  }
  const std::string controller_fingerprint =
      ComputePublicKeyFingerprint(controller_public_key);
  std::optional<HostdInstallOptions> local_hostd_options;
  if (options.with_hostd) {
    local_hostd_options = HostdInstallOptions{
        options.layout,
        self_path,
        "http://127.0.0.1:" + std::to_string(options.listen_port),
        controller_fingerprint,
        "",
        options.node_name,
        "out",
        "mixed",
        "",
        options.compose_mode,
    };
  }

  {
    naim::ControllerStore store(
        (options.layout.state_root / "controller.sqlite").string());
    store.Initialize();
    if (options.with_hostd) {
      naim::RegisteredHostRecord host;
      host.node_name = options.node_name;
      host.advertised_address =
          "http://127.0.0.1:" + std::to_string(options.listen_port);
      host.public_key_base64 = Trim(ReadTextFile(hostd_public_key));
      host.controller_public_key_fingerprint = controller_fingerprint;
      host.transport_mode = "out";
      host.execution_mode = "mixed";
      host.registration_state = "registered";
      host.session_state = "disconnected";
      host.status_message = "prepared by naim-node install controller";
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
      options.layout.systemd_dir / "naim-node-controller.service",
      RenderControllerUnit(options, options.layout.config_path));
  if (options.with_hostd) {
    WriteTextFile(
        options.layout.systemd_dir / "naim-node-hostd.service",
        RenderHostdUnit(
            HostdInstallOptions{
                options.layout,
                self_path,
                "http://127.0.0.1:" + std::to_string(options.listen_port),
                controller_fingerprint,
                "",
                options.node_name,
                "out",
                "mixed",
                "",
                options.compose_mode,
            },
            options.layout.config_path));
  }

  MaybeRunSystemctl({}, {"daemon-reload"}, skip_systemctl);
  MaybeRunSystemctl(
      {"naim-node-controller.service"}, {"enable", "restart"}, skip_systemctl);
  if (options.with_hostd) {
    MaybeRunSystemctl(
        {"naim-node-hostd.service"}, {"enable", "restart"}, skip_systemctl);
  }

  std::cout << "installed controller\n";
  std::cout << "controller_api_url=http://127.0.0.1:" << options.listen_port << "\n";
  std::cout << "controller_internal_url=http://" << options.internal_listen_host << ":"
            << options.listen_port << "\n";
  if (options.with_web_ui) {
    std::cout << "web_ui_url=http://127.0.0.1:18081\n";
  }
  std::cout << "next_step="
            << (install_layout_resolver_.IsUserServiceLayout(options.layout)
                    ? "systemctl --user"
                    : "systemctl")
            << " status naim-node-controller.service\n";
}

void LauncherInstallService::InstallHostd(
    const std::filesystem::path& self_path,
    const LauncherCommandLine& command_line) const {
  HostdInstallOptions options;
  options.layout = ParseLayout(command_line);
  options.binary_path = self_path;
  options.controller_url = command_line.FindFlagValue("--controller").value_or("");
  options.controller_fingerprint =
      command_line.FindFlagValue("--controller-fingerprint").value_or("");
  options.onboarding_key = command_line.FindFlagValue("--onboarding-key").value_or("");
  options.node_name = command_line.FindFlagValue("--node").value_or("local-hostd");
  options.transport_mode =
      command_line.FindFlagValue("--transport").value_or(options.transport_mode);
  options.execution_mode =
      command_line.FindFlagValue("--execution-mode").value_or(options.execution_mode);
  options.listen_address = command_line.FindFlagValue("--listen").value_or("");
  options.compose_mode =
      command_line.FindFlagValue("--compose-mode").value_or(options.compose_mode);
  const bool skip_systemctl = command_line.HasFlag("--skip-systemctl");

  const auto keys_root = options.layout.state_root / "keys";
  const auto controller_private_key = keys_root / "controller.key.b64";
  const auto controller_public_key = keys_root / "controller.pub.b64";
  const auto hostd_private_key = keys_root / "hostd.key.b64";
  const auto hostd_public_key = keys_root / "hostd.pub.b64";
  std::filesystem::create_directories(options.layout.state_root);
  std::filesystem::create_directories(options.layout.log_root);
  std::filesystem::create_directories(options.layout.systemd_dir);
  EnsureKeypair(hostd_private_key, hostd_public_key);
  const std::string controller_fingerprint =
      !options.controller_fingerprint.empty()
          ? options.controller_fingerprint
          : (std::filesystem::exists(controller_public_key)
                 ? ComputePublicKeyFingerprint(controller_public_key)
                 : "");
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
      options.layout.systemd_dir / "naim-node-hostd.service",
      RenderHostdUnit(options, options.layout.config_path));
  MaybeRunSystemctl({}, {"daemon-reload"}, skip_systemctl);
  MaybeRunSystemctl({"naim-node-hostd.service"}, {"enable", "restart"}, skip_systemctl);
  std::cout << "installed hostd\n";
  std::cout << "node=" << options.node_name << "\n";
  if (!options.controller_url.empty()) {
    std::cout << "controller_url=" << options.controller_url << "\n";
    std::cout << "next_step_register=naim-node connect-hostd --db <controller-db> --node "
              << options.node_name << " --public-key "
              << (options.layout.state_root / "keys/hostd.pub.b64").string() << "\n";
  }
  std::cout << "next_step="
            << (install_layout_resolver_.IsUserServiceLayout(options.layout)
                    ? "systemctl --user"
                    : "systemctl")
            << " status naim-node-hostd.service\n";
}

void LauncherInstallService::ServiceCommand(
    const std::string& action,
    const std::string& role,
    const LauncherCommandLine& command_line) const {
  const InstallLayout layout = ParseLayout(command_line);
  const bool skip_systemctl = command_line.HasFlag("--skip-systemctl");
  const std::vector<std::string> units = ParseRoleTargets(role);

  const auto verify_units = [&]() {
    for (const std::string& unit : units) {
      const auto unit_path = layout.systemd_dir / unit;
      if (!std::filesystem::exists(unit_path)) {
        throw std::runtime_error("missing unit file '" + unit_path.string() + "'");
      }
    }
    if (skip_systemctl && !process_runner_.CommandExists("systemd-analyze")) {
      return;
    }
    if (!process_runner_.CommandExists("systemd-analyze")) {
      throw std::runtime_error("systemd-analyze is required for service verify");
    }
    std::ostringstream command;
    command << "systemd-analyze verify";
    for (const std::string& unit : units) {
      command << " " << ShellEscape((layout.systemd_dir / unit).string());
    }
    if (process_runner_.RunShellCommand(command.str()) != 0) {
      throw std::runtime_error("systemd-analyze verify failed");
    }
  };

  if (action == "status") {
    for (const std::string& unit : units) {
      const bool exists = std::filesystem::exists(layout.systemd_dir / unit);
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
      std::filesystem::remove(layout.systemd_dir / unit);
    }
    if (!skip_systemctl) {
      MaybeRunSystemctl({"naim-node-controller.service"}, {"daemon-reload"}, false);
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

std::string LauncherInstallService::RenderConfigToml(
    const ControllerInstallOptions* controller,
    const HostdInstallOptions* hostd,
    const std::filesystem::path& controller_private_key,
    const std::filesystem::path& controller_public_key,
    const std::filesystem::path& hostd_private_key,
    const std::filesystem::path& hostd_public_key,
    const std::string& controller_fingerprint) const {
  std::ostringstream out;
  out << "# generated by naim-node\n";
  if (controller != nullptr) {
    out << "[controller]\n";
    out << "listen_host = \"" << controller->listen_host << "\"\n";
    out << "listen_port = " << controller->listen_port << "\n";
    out << "internal_listen_host = \"" << controller->internal_listen_host << "\"\n";
    out << "db_path = \"" << (controller->layout.state_root / "controller.sqlite").string()
        << "\"\n";
    out << "artifacts_root = \"" << (controller->layout.state_root / "artifacts").string()
        << "\"\n";
    out << "web_ui_enabled = " << (controller->with_web_ui ? "true" : "false") << "\n";
    out << "local_hostd_enabled = " << (controller->with_hostd ? "true" : "false") << "\n";
    out << "controller_private_key = \"" << controller_private_key.string() << "\"\n";
    out << "controller_public_key = \"" << controller_public_key.string() << "\"\n";
    out << "web_ui_image = \"naim/web-ui:dev\"\n";
    out << "worker_image = \"naim/worker-runtime:dev\"\n";
    out << "infer_image = \"naim/infer-runtime:dev\"\n\n";
  }
  if (hostd != nullptr) {
    out << "[hostd]\n";
    out << "node_name = \"" << hostd->node_name << "\"\n";
    out << "controller_url = \"" << hostd->controller_url << "\"\n";
    if (!hostd->onboarding_key.empty()) {
      out << "onboarding_key = \"" << hostd->onboarding_key << "\"\n";
    }
    out << "transport_mode = \"" << hostd->transport_mode << "\"\n";
    out << "execution_mode = \"" << hostd->execution_mode << "\"\n";
    out << "listen_address = \"" << hostd->listen_address << "\"\n";
    out << "runtime_root = \"" << (hostd->layout.state_root / "runtime").string() << "\"\n";
    out << "state_root = \"" << (hostd->layout.state_root / "hostd-state").string() << "\"\n";
    out << "compose_mode = \"" << hostd->compose_mode << "\"\n";
    out << "host_private_key = \"" << hostd_private_key.string() << "\"\n";
    out << "host_public_key = \"" << hostd_public_key.string() << "\"\n";
    out << "trusted_controller_fingerprint = \"" << controller_fingerprint << "\"\n\n";
    out << "inventory_scan_interval_sec = " << hostd->inventory_scan_interval_sec << "\n\n";
  }
  return out.str();
}

std::string LauncherInstallService::RenderControllerUnit(
    const ControllerInstallOptions& options,
    const std::filesystem::path& config_path) const {
  const bool user_service = install_layout_resolver_.IsUserServiceLayout(options.layout);
  std::ostringstream out;
  out << "[Unit]\n";
  out << "Description=Naim Node Controller\n";
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
      << " --internal-listen-host " << options.internal_listen_host
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
  out << "Environment=NAIM_CONFIG=" << config_path.string() << "\n";
  out << "Environment=NAIM_SERVICE_MODE=1\n";
  out << "Environment=NAIM_CONTROLLER_INTERNAL_HOST=" << options.internal_listen_host << "\n";
  out << "Environment=NAIM_CONTROLLER_INTERNAL_UPSTREAM=http://"
      << options.internal_listen_host << ":" << options.listen_port << "\n\n";
  out << "[Install]\n";
  out << "WantedBy=" << (user_service ? "default.target" : "multi-user.target") << "\n";
  return out.str();
}

std::string LauncherInstallService::RenderHostdUnit(
    const HostdInstallOptions& options,
    const std::filesystem::path& config_path) const {
  const bool user_service = install_layout_resolver_.IsUserServiceLayout(options.layout);
  std::ostringstream out;
  out << "[Unit]\n";
  out << "Description=Naim Node Host Agent\n";
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
  out << "Environment=NAIM_CONFIG=" << config_path.string() << "\n";
  out << "Environment=NAIM_SERVICE_MODE=1\n\n";
  out << "[Install]\n";
  out << "WantedBy=" << (user_service ? "default.target" : "multi-user.target") << "\n";
  return out.str();
}

std::string LauncherInstallService::DefaultInternalListenHost() const {
  const std::string route_probe = Trim(process_runner_.CaptureShellOutput(
      "sh -c \"ip -4 route get 1.1.1.1 2>/dev/null | sed -n 's/.* src \\([0-9.]*\\).*/\\1/p'\""));
  if (IsPrivateIpv4Address(route_probe)) {
    return route_probe;
  }

  const std::string host_ips = Trim(process_runner_.CaptureShellOutput("hostname -I 2>/dev/null"));
  if (!host_ips.empty()) {
    std::istringstream input(host_ips);
    std::string host_ip;
    while (input >> host_ip) {
      if (IsPrivateIpv4Address(host_ip)) {
        return host_ip;
      }
    }
    input.clear();
    input.str(host_ips);
    while (input >> host_ip) {
      if (IsIpv4Address(host_ip) && host_ip != "127.0.0.1") {
        return host_ip;
      }
    }
  }
  return "127.0.0.1";
}

std::vector<std::string> LauncherInstallService::ParseRoleTargets(
    const std::string& role) const {
  if (role == "controller") {
    return {"naim-node-controller.service"};
  }
  if (role == "hostd") {
    return {"naim-node-hostd.service"};
  }
  if (role == "controller-hostd") {
    return {"naim-node-controller.service", "naim-node-hostd.service"};
  }
  throw std::runtime_error("unknown service role '" + role + "'");
}

void LauncherInstallService::MaybeRunSystemctl(
    const std::vector<std::string>& units,
    const std::vector<std::string>& actions,
    const bool skip_systemctl) const {
  if (skip_systemctl) {
    return;
  }
  if (!process_runner_.CommandExists("systemctl")) {
    throw std::runtime_error("systemctl is required for service lifecycle commands");
  }
  for (const std::string& action : actions) {
    std::ostringstream command;
    command << "systemctl";
    if (!naim::platform::HasElevatedPrivileges()) {
      command << " --user";
    }
    command << " " << action;
    if (action != "daemon-reload") {
      for (const std::string& unit : units) {
        command << " " << unit;
      }
    }
    if (process_runner_.RunShellCommand(command.str()) != 0) {
      throw std::runtime_error("systemctl " + action + " failed");
    }
  }
}

void LauncherInstallService::EnsureKeypair(
    const std::filesystem::path& private_key_path,
    const std::filesystem::path& public_key_path) const {
  if (std::filesystem::exists(private_key_path) &&
      std::filesystem::exists(public_key_path)) {
    return;
  }
  std::filesystem::create_directories(private_key_path.parent_path());
  const auto keypair = naim::GenerateSigningKeypair();
  WriteTextFile(private_key_path, keypair.private_key_base64 + "\n");
  WriteTextFile(public_key_path, keypair.public_key_base64 + "\n");
  std::filesystem::permissions(
      private_key_path,
      std::filesystem::perms::owner_read | std::filesystem::perms::owner_write,
      std::filesystem::perm_options::replace);
}

std::string LauncherInstallService::ComputePublicKeyFingerprint(
    const std::filesystem::path& public_key_path) const {
  return naim::ComputeKeyFingerprintHex(Trim(ReadTextFile(public_key_path)));
}

std::string LauncherInstallService::ReadTextFile(
    const std::filesystem::path& path) const {
  std::ifstream input(path);
  if (!input) {
    throw std::runtime_error("failed to read file '" + path.string() + "'");
  }
  std::ostringstream buffer;
  buffer << input.rdbuf();
  return buffer.str();
}

void LauncherInstallService::WriteTextFile(
    const std::filesystem::path& path,
    const std::string& content) const {
  std::filesystem::create_directories(path.parent_path());
  std::ofstream output(path);
  if (!output) {
    throw std::runtime_error("failed to write file '" + path.string() + "'");
  }
  output << content;
}

std::string LauncherInstallService::Trim(const std::string& value) const {
  std::size_t begin = 0;
  while (begin < value.size() &&
         std::isspace(static_cast<unsigned char>(value[begin])) != 0) {
    ++begin;
  }
  std::size_t end = value.size();
  while (end > begin &&
         std::isspace(static_cast<unsigned char>(value[end - 1])) != 0) {
    --end;
  }
  return value.substr(begin, end - begin);
}

std::string LauncherInstallService::ShellEscape(const std::string& value) const {
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

}  // namespace naim::launcher
