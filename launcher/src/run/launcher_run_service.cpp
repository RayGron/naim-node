#include "run/launcher_run_service.h"

#include <chrono>
#include <cctype>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <sstream>
#include <stdexcept>
#include <thread>
#include <vector>

#if !defined(_WIN32)
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#endif

#include "comet/core/platform_compat.h"
#include "comet/security/crypto_utils.h"
#include "comet/state/sqlite_store.h"

namespace comet::launcher {

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

bool SetEnvVar(const std::string& key, const std::string& value) {
#if defined(_WIN32)
  return _putenv_s(key.c_str(), value.c_str()) == 0;
#else
  return ::setenv(key.c_str(), value.c_str(), 1) == 0;
#endif
}

}  // namespace

LauncherRunService::LauncherRunService(
    const InstallLayoutResolver& install_layout_resolver,
    const GeneratedConfigLoader& config_loader,
    const ProcessRunner& process_runner,
    const LauncherInstallService& install_service)
    : install_layout_resolver_(install_layout_resolver),
      config_loader_(config_loader),
      process_runner_(process_runner),
      install_service_(install_service) {}

int LauncherRunService::RunController(
    SignalManager& signal_manager,
    const std::filesystem::path& self_path,
    const std::filesystem::path& controller_binary,
    const LauncherCommandLine& command_line) const {
  ControllerRunOptions options;
  std::optional<GeneratedConfig> loaded_config;
  if (const auto config_path = config_loader_.ResolveConfigPathFromEnvOrDefault();
      config_path.has_value()) {
    loaded_config = config_loader_.Load(*config_path);
  }

  options.db_path =
      command_line.FindFlagValue("--db")
          .value_or(loaded_config && loaded_config->controller.db_path.has_value()
                        ? loaded_config->controller.db_path->string()
                        : install_layout_resolver_.DefaultControllerDbPath().string());
  options.artifacts_root =
      command_line.FindFlagValue("--artifacts-root")
          .value_or(loaded_config && loaded_config->controller.artifacts_root.has_value()
                        ? loaded_config->controller.artifacts_root->string()
                        : install_layout_resolver_.DefaultArtifactsRoot().string());
  options.web_ui_root =
      command_line.FindFlagValue("--web-ui-root")
          .value_or(install_layout_resolver_.DefaultWebUiRoot().string());
  options.listen_host =
      command_line.FindFlagValue("--listen-host")
          .value_or(loaded_config && loaded_config->controller.listen_host.has_value()
                        ? *loaded_config->controller.listen_host
                        : options.listen_host);
  options.listen_port = LauncherCommandLine::ParseIntValue(
      command_line.FindFlagValue("--listen-port"),
      loaded_config && loaded_config->controller.listen_port.has_value()
          ? *loaded_config->controller.listen_port
          : options.listen_port);
  options.internal_listen_host =
      command_line.FindFlagValue("--internal-listen-host")
          .value_or(loaded_config && loaded_config->controller.internal_listen_host.has_value()
                        ? *loaded_config->controller.internal_listen_host
                        : DefaultInternalListenHost());
  options.controller_upstream =
      command_line.FindFlagValue("--controller-upstream").value_or("");
  options.compose_mode =
      command_line.FindFlagValue("--compose-mode")
          .value_or(loaded_config && loaded_config->hostd.compose_mode.has_value()
                        ? *loaded_config->hostd.compose_mode
                        : options.compose_mode);
  options.hostd_compose_mode =
      command_line.FindFlagValue("--hostd-compose-mode")
          .value_or(loaded_config && loaded_config->hostd.compose_mode.has_value()
                        ? *loaded_config->hostd.compose_mode
                        : options.hostd_compose_mode);
  options.node_name =
      command_line.FindFlagValue("--node")
          .value_or(loaded_config && loaded_config->hostd.node_name.has_value()
                        ? *loaded_config->hostd.node_name
                        : options.node_name);
  options.runtime_root =
      command_line.FindFlagValue("--runtime-root")
          .value_or(loaded_config && loaded_config->hostd.runtime_root.has_value()
                        ? loaded_config->hostd.runtime_root->string()
                        : install_layout_resolver_.DefaultRuntimeRoot().string());
  options.state_root =
      command_line.FindFlagValue("--state-root")
          .value_or(loaded_config && loaded_config->hostd.state_root.has_value()
                        ? loaded_config->hostd.state_root->string()
                        : install_layout_resolver_.DefaultHostdStateRoot().string());

  const bool config_local_hostd_enabled =
      loaded_config && loaded_config->controller.local_hostd_enabled.value_or(false);
  const bool managed_hostd_service_present =
      std::getenv("COMET_SERVICE_MODE") != nullptr &&
      std::string(std::getenv("COMET_SERVICE_MODE")) == "1" &&
      std::filesystem::exists(
          install_service_.ParseLayout(command_line).systemd_dir / "comet-node-hostd.service");
  options.with_hostd =
      (command_line.HasFlag("--with-hostd") ||
       (!command_line.HasFlag("--without-hostd") && config_local_hostd_enabled)) &&
      !managed_hostd_service_present;
  options.with_web_ui =
      command_line.HasFlag("--with-web-ui") ||
      (!command_line.HasFlag("--without-web-ui") &&
       loaded_config && loaded_config->controller.web_ui_enabled.value_or(false));
  options.hostd_poll_interval_sec = LauncherCommandLine::ParseIntValue(
      command_line.FindFlagValue("--poll-interval-sec"),
      options.hostd_poll_interval_sec);

  if (!(std::getenv("COMET_SERVICE_MODE") != nullptr &&
        std::string(std::getenv("COMET_SERVICE_MODE")) == "1") &&
      !command_line.HasFlag("--foreground") &&
      !command_line.HasFlag("--skip-systemctl")) {
    const auto layout = install_service_.ParseLayout(command_line);
    const auto default_layout = install_layout_resolver_.DefaultInstallLayout();
    if (process_runner_.CommandExists("systemctl")) {
      std::vector<std::string> install_args;
      if (layout.config_path != default_layout.config_path) {
        install_args.insert(install_args.end(), {"--config", layout.config_path.string()});
      }
      if (layout.state_root != default_layout.state_root) {
        install_args.insert(install_args.end(), {"--state-root", layout.state_root.string()});
      }
      if (layout.log_root != default_layout.log_root) {
        install_args.insert(install_args.end(), {"--log-root", layout.log_root.string()});
      }
      if (layout.systemd_dir != default_layout.systemd_dir) {
        install_args.insert(install_args.end(), {"--systemd-dir", layout.systemd_dir.string()});
      }
      install_args.insert(install_args.end(), {"--listen-host", options.listen_host});
      install_args.insert(
          install_args.end(), {"--listen-port", std::to_string(options.listen_port)});
      install_args.insert(
          install_args.end(), {"--internal-listen-host", options.internal_listen_host});
      install_args.insert(
          install_args.end(), {"--compose-mode", options.hostd_compose_mode});
      install_args.insert(install_args.end(), {"--node", options.node_name});
      if (options.with_hostd) {
        install_args.push_back("--with-hostd");
      }
      if (options.with_web_ui) {
        install_args.push_back("--with-web-ui");
      }
      install_service_.InstallController(self_path, LauncherCommandLine(std::move(install_args)));
      std::cout << "service_mode=systemd\n";
      std::cout << "controller_service=comet-node-controller.service\n";
      if (options.with_hostd) {
        std::cout << "hostd_service=comet-node-hostd.service\n";
      }
      return 0;
    }
  }

  return RunControllerSupervisor(signal_manager, self_path, controller_binary, options);
}

int LauncherRunService::RunHostd(
    SignalManager& signal_manager,
    const std::filesystem::path& hostd_binary,
    const std::filesystem::path& self_path,
    const LauncherCommandLine& command_line) const {
  HostdRunOptions options;
  options.db_path = command_line.FindFlagValue("--db").value_or("");

  std::optional<GeneratedConfig> loaded_config;
  if (const auto config_path = config_loader_.ResolveConfigPathFromEnvOrDefault();
      config_path.has_value()) {
    loaded_config = config_loader_.Load(*config_path);
  }

  options.controller_url =
      command_line.FindFlagValue("--controller")
          .value_or(loaded_config && loaded_config->hostd.controller_url.has_value()
                        ? *loaded_config->hostd.controller_url
                        : "");
  options.controller_fingerprint =
      command_line.FindFlagValue("--controller-fingerprint")
          .value_or(loaded_config &&
                            loaded_config->hostd.trusted_controller_fingerprint.has_value()
                        ? *loaded_config->hostd.trusted_controller_fingerprint
                        : "");
  options.node_name =
      command_line.FindFlagValue("--node")
          .value_or(loaded_config && loaded_config->hostd.node_name.has_value()
                        ? *loaded_config->hostd.node_name
                        : DefaultNodeName());
  options.runtime_root =
      command_line.FindFlagValue("--runtime-root")
          .value_or(loaded_config && loaded_config->hostd.runtime_root.has_value()
                        ? loaded_config->hostd.runtime_root->string()
                        : install_layout_resolver_.DefaultRuntimeRoot().string());
  options.state_root =
      command_line.FindFlagValue("--state-root")
          .value_or(loaded_config && loaded_config->hostd.state_root.has_value()
                        ? loaded_config->hostd.state_root->string()
                        : install_layout_resolver_.DefaultHostdStateRoot().string());
  options.host_private_key_path =
      command_line.FindFlagValue("--host-private-key")
          .value_or(loaded_config && loaded_config->hostd.host_private_key.has_value()
                        ? loaded_config->hostd.host_private_key->string()
                        : "");
  options.compose_mode =
      command_line.FindFlagValue("--compose-mode").value_or(options.compose_mode);
  options.poll_interval_sec = LauncherCommandLine::ParseIntValue(
      command_line.FindFlagValue("--poll-interval-sec"), options.poll_interval_sec);

  if (!(std::getenv("COMET_SERVICE_MODE") != nullptr &&
        std::string(std::getenv("COMET_SERVICE_MODE")) == "1") &&
      !command_line.HasFlag("--foreground") &&
      !command_line.HasFlag("--skip-systemctl") &&
      process_runner_.CommandExists("systemctl")) {
    const auto layout = install_service_.ParseLayout(command_line);
    const auto default_layout = install_layout_resolver_.DefaultInstallLayout();
    std::vector<std::string> install_args;
    if (layout.config_path != default_layout.config_path) {
      install_args.insert(install_args.end(), {"--config", layout.config_path.string()});
    }
    if (layout.state_root != default_layout.state_root) {
      install_args.insert(install_args.end(), {"--state-root", layout.state_root.string()});
    }
    if (layout.log_root != default_layout.log_root) {
      install_args.insert(install_args.end(), {"--log-root", layout.log_root.string()});
    }
    if (layout.systemd_dir != default_layout.systemd_dir) {
      install_args.insert(install_args.end(), {"--systemd-dir", layout.systemd_dir.string()});
    }
    if (!options.controller_url.empty()) {
      install_args.insert(install_args.end(), {"--controller", options.controller_url});
    }
    if (!options.controller_fingerprint.empty()) {
      install_args.insert(
          install_args.end(), {"--controller-fingerprint", options.controller_fingerprint});
    }
    install_args.insert(install_args.end(), {"--node", options.node_name});
    install_args.insert(install_args.end(), {"--compose-mode", options.compose_mode});
    install_service_.InstallHostd(self_path, LauncherCommandLine(std::move(install_args)));
    std::cout << "service_mode=systemd\n";
    std::cout << "hostd_service=comet-node-hostd.service\n";
    return 0;
  }

  return RunHostdLoop(signal_manager, hostd_binary, options);
}

int LauncherRunService::RunHostdLoop(
    SignalManager& signal_manager,
    const std::filesystem::path& hostd_binary,
    const HostdRunOptions& options) const {
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
  std::cout
      << "next_step=leave hostd running so it can receive assignments and upload telemetry\n";

  while (!signal_manager.stop_requested()) {
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
            apply_args.end(), {"--controller-fingerprint", options.controller_fingerprint});
      }
      if (!options.host_private_key_path.empty()) {
        apply_args.insert(
            apply_args.end(), {"--host-private-key", options.host_private_key_path.string()});
      }
    } else {
      apply_args.insert(apply_args.end(), {"--db", options.db_path.string()});
    }
    const int apply_code = process_runner_.RunCommand(apply_args);
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
            report_args.end(), {"--controller-fingerprint", options.controller_fingerprint});
      }
      if (!options.host_private_key_path.empty()) {
        report_args.insert(
            report_args.end(), {"--host-private-key", options.host_private_key_path.string()});
      }
    } else {
      report_args.insert(report_args.end(), {"--db", options.db_path.string()});
    }
    const int report_code = process_runner_.RunCommand(report_args);
    if (report_code != 0) {
      std::cerr << "comet-node: hostd report-observed-state exit=" << report_code << "\n";
    }

    for (int second = 0;
         second < options.poll_interval_sec && !signal_manager.stop_requested();
         ++second) {
      std::this_thread::sleep_for(std::chrono::seconds(1));
    }
  }
  return 0;
}

void LauncherRunService::PrepareControllerRuntime(
    const std::filesystem::path& owner_probe_path,
    const ControllerRunOptions& options) const {
  std::filesystem::create_directories(options.db_path.parent_path());
  std::filesystem::create_directories(options.artifacts_root);
  std::filesystem::create_directories(options.runtime_root);
  std::filesystem::create_directories(options.state_root);
  if (options.with_web_ui) {
    std::filesystem::create_directories(options.web_ui_root);
  }

  const auto keys_root = options.state_root.parent_path() / "keys";
  const auto ensure_keypair = [&](const std::filesystem::path& private_key_path,
                                  const std::filesystem::path& public_key_path) {
    if (std::filesystem::exists(private_key_path) &&
        std::filesystem::exists(public_key_path)) {
      return;
    }
    std::filesystem::create_directories(private_key_path.parent_path());
    const auto keypair = comet::GenerateSigningKeypair();
    std::ofstream priv(private_key_path);
    std::ofstream pub(public_key_path);
    priv << keypair.private_key_base64 << "\n";
    pub << keypair.public_key_base64 << "\n";
  };
  ensure_keypair(keys_root / "controller.key.b64", keys_root / "controller.pub.b64");
  if (options.with_hostd) {
    ensure_keypair(keys_root / "hostd.key.b64", keys_root / "hostd.pub.b64");
  }

  PrepareSharedStateAccess(owner_probe_path, options.db_path);

  comet::ControllerStore store(options.db_path.string());
  store.Initialize();
  PrepareSharedStateAccess(owner_probe_path, options.db_path);
}

int LauncherRunService::RunControllerSupervisor(
    SignalManager& signal_manager,
    const std::filesystem::path& self_path,
    const std::filesystem::path& controller_binary,
    const ControllerRunOptions& options) const {
  PrepareControllerRuntime(self_path, options);
  const std::string admin_dial_host =
      options.listen_host.empty() || options.listen_host == "0.0.0.0"
          ? "127.0.0.1"
          : options.listen_host;
  const std::string local_controller_url =
      "http://" + admin_dial_host + ":" + std::to_string(options.listen_port);
  const std::string internal_controller_url =
      "http://" + options.internal_listen_host + ":" + std::to_string(options.listen_port);
  const std::string local_skills_factory_url =
      "http://" + options.skills_factory_listen_host + ":" +
      std::to_string(options.skills_factory_listen_port);
  const std::string web_ui_controller_upstream =
      options.controller_upstream.empty()
          ? DefaultWebUiControllerUpstream(options.internal_listen_host, options.listen_port)
          : options.controller_upstream;
  const auto controller_public_key_path =
      options.state_root.parent_path() / "keys" / "controller.pub.b64";
  const std::string controller_fingerprint =
      std::filesystem::exists(controller_public_key_path)
          ? ComputePublicKeyFingerprint(controller_public_key_path)
          : "";

  if (!SetEnvVar("COMET_CONTROLLER_ADMIN_UPSTREAM", local_controller_url) ||
      !SetEnvVar("COMET_CONTROLLER_INTERNAL_HOST", options.internal_listen_host) ||
      !SetEnvVar("COMET_CONTROLLER_INTERNAL_UPSTREAM", internal_controller_url) ||
      !SetEnvVar("COMET_SKILLS_FACTORY_UPSTREAM", local_skills_factory_url) ||
      !SetEnvVar("COMET_WEB_UI_ROOT", options.web_ui_root.string()) ||
      !SetEnvVar("COMET_HOSTD_NODE_NAME", options.node_name)) {
    throw std::runtime_error("failed to export controller internal routing environment");
  }

  if (options.with_web_ui) {
    std::vector<std::string> ensure_args = {
        controller_binary.string(), "ensure-web-ui", "--db", options.db_path.string(),
        "--web-ui-root",           options.web_ui_root.string(),
        "--listen-port",           "18081",
        "--controller-upstream",   web_ui_controller_upstream,
        "--compose-mode",          options.compose_mode,
    };
    if (process_runner_.RunCommand(ensure_args) != 0) {
      throw std::runtime_error("failed to ensure comet-web-ui");
    }
  }

  const pid_t skills_factory_pid = static_cast<pid_t>(process_runner_.SpawnCommand({
      controller_binary.string(), "serve-skills-factory", "--db", options.db_path.string(),
      "--artifacts-root", options.artifacts_root.string(), "--listen-host",
      options.skills_factory_listen_host, "--listen-port",
      std::to_string(options.skills_factory_listen_port),
  }));
  signal_manager.TrackChild(static_cast<int>(skills_factory_pid));

  const pid_t internal_controller_pid = static_cast<pid_t>(process_runner_.SpawnCommand({
      controller_binary.string(), "serve", "--db", options.db_path.string(),
      "--artifacts-root", options.artifacts_root.string(), "--listen-host",
      options.internal_listen_host, "--listen-port", std::to_string(options.listen_port),
      "--skills-factory-upstream", local_skills_factory_url,
  }));
  signal_manager.TrackChild(static_cast<int>(internal_controller_pid));

  pid_t controller_pid = internal_controller_pid;
  if (options.internal_listen_host != options.listen_host) {
    controller_pid = static_cast<pid_t>(process_runner_.SpawnCommand({
        controller_binary.string(), "serve", "--db", options.db_path.string(),
        "--artifacts-root", options.artifacts_root.string(), "--listen-host",
        options.listen_host, "--listen-port", std::to_string(options.listen_port),
        "--skills-factory-upstream", local_skills_factory_url,
    }));
    signal_manager.TrackChild(static_cast<int>(controller_pid));
  }

  pid_t hostd_pid = -1;
  if (options.with_hostd) {
    comet::ControllerStore store(options.db_path.string());
    store.Initialize();
    comet::RegisteredHostRecord host;
    if (const auto current = store.LoadRegisteredHost(options.node_name);
        current.has_value()) {
      host = *current;
    }
    host.node_name = options.node_name;
    host.advertised_address = local_controller_url;
    host.public_key_base64 =
        Trim(ReadTextFile(options.state_root.parent_path() / "keys" / "hostd.pub.b64"));
    host.transport_mode = "out";
    host.execution_mode = "mixed";
    host.registration_state = "registered";
    host.session_state = "disconnected";
    host.status_message = "auto-registered local hostd by comet-node run controller";
    store.UpsertRegisteredHost(host);

    hostd_pid = static_cast<pid_t>(process_runner_.SpawnCommand({
        self_path.string(), "run", "hostd", "--controller", local_controller_url,
        "--controller-fingerprint", controller_fingerprint, "--node", options.node_name,
        "--runtime-root", options.runtime_root.string(), "--state-root",
        options.state_root.string(), "--host-private-key",
        (options.state_root.parent_path() / "keys" / "hostd.key.b64").string(),
        "--compose-mode", options.hostd_compose_mode, "--poll-interval-sec",
        std::to_string(options.hostd_poll_interval_sec),
    }));
    signal_manager.TrackChild(static_cast<int>(hostd_pid));
  }

  std::cout << "controller_api_url=" << local_controller_url << "\n";
  std::cout << "controller_internal_url=" << internal_controller_url << "\n";
  std::cout << "skills_factory_url=" << local_skills_factory_url << "\n";
  if (options.with_web_ui) {
    std::cout << "web_ui_url=http://127.0.0.1:18081\n";
    std::cout << "next_step=open the Web UI and load a plane\n";
  } else {
    std::cout << "next_step=use controller API or CLI to load a plane\n";
  }

  while (!signal_manager.stop_requested()) {
    int status = 0;
    const auto exited = signal_manager.WaitForAnyChildProcess(&status);
    if (!exited.has_value()) {
      break;
    }
    signal_manager.RemoveChild(*exited);
    if (*exited == static_cast<int>(controller_pid) ||
        *exited == static_cast<int>(internal_controller_pid) ||
        *exited == static_cast<int>(skills_factory_pid) ||
        *exited == static_cast<int>(hostd_pid)) {
      signal_manager.RequestStop();
      break;
    }
  }

  if (controller_pid > 0) {
    signal_manager.TerminateChildProcess(static_cast<int>(controller_pid));
  }
  if (internal_controller_pid > 0 &&
      internal_controller_pid != controller_pid) {
    signal_manager.TerminateChildProcess(static_cast<int>(internal_controller_pid));
  }
  if (skills_factory_pid > 0) {
    signal_manager.TerminateChildProcess(static_cast<int>(skills_factory_pid));
  }
  if (hostd_pid > 0) {
    signal_manager.TerminateChildProcess(static_cast<int>(hostd_pid));
  }
  return 0;
}

void LauncherRunService::PrepareSharedStateAccess(
    const std::filesystem::path& owner_probe_path,
    const std::filesystem::path& db_path) const {
#if defined(_WIN32)
  (void)owner_probe_path;
  (void)db_path;
#else
  if (!comet::platform::HasElevatedPrivileges()) {
    return;
  }

  const auto group_id = ResolveSharedStateGroupId(owner_probe_path);
  if (!group_id.has_value()) {
    return;
  }

  try {
    EnsureSharedDirectoryAccess(db_path.parent_path(), *group_id);
    EnsureSharedFileAccess(db_path, *group_id);
    EnsureSharedFileAccess(db_path.string() + "-wal", *group_id);
    EnsureSharedFileAccess(db_path.string() + "-shm", *group_id);
    ::umask(0002);
  } catch (const std::exception& error) {
    std::cerr << "comet-node: warning: failed to prepare shared controller DB access: "
              << error.what() << "\n";
  }
#endif
}

std::optional<unsigned int> LauncherRunService::ResolveSharedStateGroupId(
    const std::filesystem::path& owner_probe_path) const {
#if defined(_WIN32)
  (void)owner_probe_path;
  return std::nullopt;
#else
  struct stat metadata {};
  if (::stat(owner_probe_path.c_str(), &metadata) != 0) {
    return std::nullopt;
  }
  return static_cast<unsigned int>(metadata.st_gid);
#endif
}

void LauncherRunService::EnsureSharedDirectoryAccess(
    const std::filesystem::path& path,
    unsigned int group_id) const {
#if defined(_WIN32)
  (void)path;
  (void)group_id;
#else
  if (path.empty() || !std::filesystem::exists(path)) {
    return;
  }

  struct stat metadata {};
  if (::stat(path.c_str(), &metadata) != 0) {
    throw std::runtime_error("stat failed for '" + path.string() + "'");
  }
  if (::chown(path.c_str(), metadata.st_uid, static_cast<gid_t>(group_id)) != 0) {
    throw std::runtime_error("chown failed for '" + path.string() + "'");
  }

  constexpr mode_t kSharedDirectoryMode = 02775;
  if (::chmod(path.c_str(), kSharedDirectoryMode) != 0) {
    throw std::runtime_error("chmod failed for '" + path.string() + "'");
  }
#endif
}

void LauncherRunService::EnsureSharedFileAccess(
    const std::filesystem::path& path,
    unsigned int group_id) const {
#if defined(_WIN32)
  (void)path;
  (void)group_id;
#else
  if (path.empty() || !std::filesystem::exists(path)) {
    return;
  }

  struct stat metadata {};
  if (::stat(path.c_str(), &metadata) != 0) {
    throw std::runtime_error("stat failed for '" + path.string() + "'");
  }
  if (::chown(path.c_str(), metadata.st_uid, static_cast<gid_t>(group_id)) != 0) {
    throw std::runtime_error("chown failed for '" + path.string() + "'");
  }

  constexpr mode_t kSharedFileMode = 0664;
  if (::chmod(path.c_str(), kSharedFileMode) != 0) {
    throw std::runtime_error("chmod failed for '" + path.string() + "'");
  }
#endif
}

std::string LauncherRunService::DefaultNodeName() const {
  return "local-hostd";
}

std::string LauncherRunService::DefaultInternalListenHost() const {
  const std::string route_probe =
      Trim(process_runner_.CaptureShellOutput(
          "sh -c \"ip -4 route get 1.1.1.1 2>/dev/null | sed -n 's/.* src \\([0-9.]*\\).*/\\1/p'\""));
  if (IsPrivateIpv4Address(route_probe)) {
    return route_probe;
  }

  const std::string host_ips =
      Trim(process_runner_.CaptureShellOutput("hostname -I 2>/dev/null"));
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

std::string LauncherRunService::DefaultWebUiControllerUpstream(
    const std::string& internal_listen_host,
    int listen_port) const {
  if (!internal_listen_host.empty()) {
    return "http://" + internal_listen_host + ":" + std::to_string(listen_port);
  }
  return "http://host.docker.internal:" + std::to_string(listen_port);
}

std::string LauncherRunService::Trim(const std::string& value) const {
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

std::string LauncherRunService::ReadTextFile(
    const std::filesystem::path& path) const {
  std::ifstream input(path);
  if (!input) {
    throw std::runtime_error("failed to read file '" + path.string() + "'");
  }
  std::ostringstream buffer;
  buffer << input.rdbuf();
  return buffer.str();
}

std::string LauncherRunService::ComputePublicKeyFingerprint(
    const std::filesystem::path& public_key_path) const {
  return comet::ComputeKeyFingerprintHex(Trim(ReadTextFile(public_key_path)));
}

}  // namespace comet::launcher
