#include "../include/web_ui_service.h"

#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <sstream>
#include <stdexcept>
#include <vector>

namespace comet::controller {

namespace {

using nlohmann::json;

std::string NormalizeControllerUpstreamForCompose(
    const std::string& controller_upstream,
    WebUiComposeMode compose_mode) {
  if (compose_mode != WebUiComposeMode::Exec) {
    return controller_upstream;
  }

  const std::vector<std::pair<std::string, std::string>> prefixes = {
      {"http://127.0.0.1", "http://host.docker.internal"},
      {"https://127.0.0.1", "https://host.docker.internal"},
      {"http://localhost", "http://host.docker.internal"},
      {"https://localhost", "https://host.docker.internal"},
      {"http://0.0.0.0", "http://host.docker.internal"},
      {"https://0.0.0.0", "https://host.docker.internal"},
  };

  for (const auto& [prefix, replacement] : prefixes) {
    if (controller_upstream.rfind(prefix, 0) == 0) {
      return replacement + controller_upstream.substr(prefix.size());
    }
  }
  return controller_upstream;
}

std::string ShellQuote(const std::string& value) {
  std::string quoted = "'";
  for (char ch : value) {
    if (ch == '\'') {
      quoted += "'\\''";
    } else {
      quoted.push_back(ch);
    }
  }
  quoted.push_back('\'');
  return quoted;
}

bool CommandExists(const std::string& command) {
  const std::string probe = "command -v " + command + " >/dev/null 2>&1";
  return std::system(probe.c_str()) == 0;
}

std::string ResolvedDockerCommand() {
  if (CommandExists("docker")) {
    return "docker";
  }
  if (CommandExists("podman")) {
    return "podman";
  }
  throw std::runtime_error("no working Docker CLI found for web-ui lifecycle");
}

void RunCommandOrThrow(const std::string& command) {
  const int exit_code = std::system(command.c_str());
  if (exit_code != 0) {
    throw std::runtime_error(
        "command failed with exit code " + std::to_string(exit_code) + ": " + command);
  }
}

void WriteTextFile(const std::string& path, const std::string& content) {
  std::filesystem::create_directories(std::filesystem::path(path).parent_path());
  std::ofstream output(path, std::ios::binary);
  if (!output.is_open()) {
    throw std::runtime_error("failed to write file: " + path);
  }
  output << content;
}

void RemoveFileIfExists(const std::string& path) {
  std::error_code error;
  std::filesystem::remove(path, error);
}

std::string ComposePath(const std::string& web_ui_root) {
  return (std::filesystem::path(web_ui_root) / "docker-compose.yml").string();
}

std::string StatePath(const std::string& web_ui_root) {
  return (std::filesystem::path(web_ui_root) / "web-ui-state.json").string();
}

json LoadStateJson(const std::string& web_ui_root) {
  const std::string path = StatePath(web_ui_root);
  if (!std::filesystem::exists(path)) {
    return json::object();
  }
  std::ifstream input(path);
  if (!input.is_open()) {
    throw std::runtime_error("failed to open web-ui state file: " + path);
  }
  return json::parse(input, nullptr, true, true);
}

void SaveStateJson(const std::string& web_ui_root, const json& state) {
  WriteTextFile(StatePath(web_ui_root), state.dump(2) + "\n");
}

std::string RenderComposeYaml(
    const std::string& image,
    int listen_port,
    const std::string& controller_upstream) {
  std::ostringstream out;
  out << "services:\n";
  out << "  comet-web-ui:\n";
  out << "    image: " << image << "\n";
  out << "    container_name: comet-web-ui\n";
  out << "    restart: unless-stopped\n";
  out << "    environment:\n";
  out << "      COMET_CONTROLLER_UPSTREAM: " << controller_upstream << "\n";
  out << "    security_opt:\n";
  out << "      - apparmor=unconfined\n";
  out << "    ports:\n";
  out << "      - \"" << listen_port << ":8080\"\n";
  out << "    extra_hosts:\n";
  out << "      - \"host.docker.internal:host-gateway\"\n";
  return out.str();
}

bool ComposeRunning(const std::string& web_ui_root) {
  const std::string compose_path = ComposePath(web_ui_root);
  if (!std::filesystem::exists(compose_path)) {
    return false;
  }
  const std::string command =
      ResolvedDockerCommand() + " compose -f " + ShellQuote(compose_path) +
      " ps --services --status running | grep -Fx 'comet-web-ui' >/dev/null 2>&1";
  return std::system(command.c_str()) == 0;
}

}  // namespace

WebUiService::WebUiService(std::string db_path, WebUiEventSink event_sink)
    : db_path_(std::move(db_path)), event_sink_(std::move(event_sink)) {}

int WebUiService::DefaultWebUiPort() {
  return 18081;
}

std::string WebUiService::DefaultWebUiRoot() {
  return (std::filesystem::path("var") / "web-ui").string();
}

std::string WebUiService::DefaultWebUiImage() {
  return "comet/web-ui:dev";
}

std::string WebUiService::DefaultControllerUpstream() {
  return "http://host.docker.internal:18080";
}

std::string WebUiService::ResolveWebUiRoot(
    const std::optional<std::string>& web_ui_root_arg) {
  return web_ui_root_arg.value_or(DefaultWebUiRoot());
}

WebUiComposeMode WebUiService::ResolveComposeMode(
    const std::optional<std::string>& compose_mode_arg) {
  if (!compose_mode_arg.has_value() || *compose_mode_arg == "exec") {
    return WebUiComposeMode::Exec;
  }
  if (*compose_mode_arg == "skip") {
    return WebUiComposeMode::Skip;
  }
  throw std::runtime_error("unsupported compose mode '" + *compose_mode_arg + "'");
}

int WebUiService::Ensure(
    const std::string& web_ui_root,
    int listen_port,
    const std::string& controller_upstream,
    WebUiComposeMode compose_mode) const {
  const std::string image = DefaultWebUiImage();
  const std::string effective_controller_upstream =
      NormalizeControllerUpstreamForCompose(controller_upstream, compose_mode);
  const std::string compose_path = ComposePath(web_ui_root);

  WriteTextFile(
      compose_path,
      RenderComposeYaml(image, listen_port, effective_controller_upstream));

  json state{
      {"image", image},
      {"listen_port", listen_port},
      {"controller_upstream", effective_controller_upstream},
      {"requested_controller_upstream", controller_upstream},
      {"compose_path", compose_path},
      {"web_ui_root", web_ui_root},
      {"materialized", true},
      {"running", false},
      {"status", compose_mode == WebUiComposeMode::Exec ? "starting" : "materialized"},
  };
  if (compose_mode == WebUiComposeMode::Exec) {
    RunCommandOrThrow(
        ResolvedDockerCommand() + " compose -f " + ShellQuote(compose_path) + " up -d");
    state["running"] = true;
    state["status"] = "running";
  }
  SaveStateJson(web_ui_root, state);

  comet::ControllerStore store(db_path_);
  store.Initialize();
  event_sink_(
      store,
      "ensured",
      "materialized comet-web-ui sidecar",
      json{
          {"web_ui_root", web_ui_root},
          {"listen_port", listen_port},
          {"controller_upstream", effective_controller_upstream},
          {"requested_controller_upstream", controller_upstream},
          {"compose_mode", compose_mode == WebUiComposeMode::Exec ? "exec" : "skip"},
      });

  std::cout << "web-ui ensured\n";
  std::cout << "root=" << web_ui_root << "\n";
  std::cout << "compose_path=" << compose_path << "\n";
  std::cout << "image=" << image << "\n";
  std::cout << "listen_port=" << listen_port << "\n";
  std::cout << "controller_upstream=" << effective_controller_upstream << "\n";
  if (effective_controller_upstream != controller_upstream) {
    std::cout << "requested_controller_upstream=" << controller_upstream << "\n";
  }
  std::cout << "compose_mode="
            << (compose_mode == WebUiComposeMode::Exec ? "exec" : "skip") << "\n";
  return 0;
}

int WebUiService::ShowStatus(const std::string& web_ui_root) const {
  const json state = LoadStateJson(web_ui_root);
  const bool compose_exists = std::filesystem::exists(ComposePath(web_ui_root));
  const bool running = ComposeRunning(web_ui_root);

  std::cout << "web-ui:\n";
  std::cout << "  root=" << web_ui_root << "\n";
  std::cout << "  state_path=" << StatePath(web_ui_root) << "\n";
  std::cout << "  compose_path=" << ComposePath(web_ui_root) << "\n";
  std::cout << "  materialized=" << (compose_exists ? "yes" : "no") << "\n";
  std::cout << "  running=" << (running ? "yes" : "no") << "\n";
  if (!state.empty()) {
    std::cout << "  image=" << state.value("image", DefaultWebUiImage()) << "\n";
    std::cout << "  listen_port=" << state.value("listen_port", DefaultWebUiPort()) << "\n";
    std::cout << "  controller_upstream="
              << state.value("controller_upstream", DefaultControllerUpstream()) << "\n";
    std::cout << "  status=" << state.value("status", std::string("unknown")) << "\n";
  }
  return 0;
}

int WebUiService::Stop(
    const std::string& web_ui_root,
    WebUiComposeMode compose_mode) const {
  const std::string compose_path = ComposePath(web_ui_root);
  if (compose_mode == WebUiComposeMode::Exec && std::filesystem::exists(compose_path)) {
    RunCommandOrThrow(
        ResolvedDockerCommand() + " compose -f " + ShellQuote(compose_path) +
        " down --remove-orphans");
  }
  RemoveFileIfExists(compose_path);

  json state = LoadStateJson(web_ui_root);
  state["materialized"] = false;
  state["running"] = false;
  state["status"] = "stopped";
  SaveStateJson(web_ui_root, state);

  comet::ControllerStore store(db_path_);
  store.Initialize();
  event_sink_(
      store,
      "stopped",
      "stopped comet-web-ui sidecar",
      json{
          {"web_ui_root", web_ui_root},
          {"compose_mode", compose_mode == WebUiComposeMode::Exec ? "exec" : "skip"},
      });

  std::cout << "web-ui stopped\n";
  std::cout << "root=" << web_ui_root << "\n";
  std::cout << "compose_path=" << compose_path << "\n";
  std::cout << "compose_mode="
            << (compose_mode == WebUiComposeMode::Exec ? "exec" : "skip") << "\n";
  return 0;
}

}  // namespace comet::controller
