#include "app/hostd_compose_runtime_support.h"

#include <stdexcept>

#include "app/hostd_repo_root_support.h"

namespace naim::hostd {

HostdComposeRuntimeSupport::HostdComposeRuntimeSupport(
    const HostdCommandSupport& command_support)
    : command_support_(command_support) {}

std::string HostdComposeRuntimeSupport::PlaneMeshNetworkName(
    const std::string& plane_name) const {
  return "naim-" + plane_name + "-mesh";
}

bool HostdComposeRuntimeSupport::ComposeProjectHasContainers(
    const std::string& compose_file_path) const {
  const std::string command =
      command_support_.ResolvedDockerComposeCommand() + " -f " +
      command_support_.ShellQuote(compose_file_path) + " ps -a --quiet 2>/dev/null";
  const std::string output = command_support_.RunCommandCapture(command);
  return output.find_first_not_of(" \t\r\n") != std::string::npos;
}

void HostdComposeRuntimeSupport::EnsureComposeMeshNetworkAvailable(
    const naim::NodeComposePlan& compose_plan,
    ComposeMode compose_mode) const {
  if (compose_mode != ComposeMode::Exec) {
    return;
  }
  const std::string network_name = PlaneMeshNetworkName(compose_plan.plane_name);
  const std::string inspect_command =
      command_support_.ResolvedDockerCommand() + " network inspect " +
      command_support_.ShellQuote(network_name) + " >/dev/null 2>&1";
  if (command_support_.RunCommandOk(inspect_command)) {
    return;
  }
  const std::string create_command =
      command_support_.ResolvedDockerCommand() + " network create " +
      command_support_.ShellQuote(network_name) + " >/dev/null";
  if (!command_support_.RunCommandOk(create_command)) {
    throw std::runtime_error("failed to create compose mesh network: " + network_name);
  }
}

void HostdComposeRuntimeSupport::RemoveComposeMeshNetworkIfUnused(
    const std::string& plane_name,
    ComposeMode compose_mode) const {
  if (compose_mode != ComposeMode::Exec) {
    return;
  }
  const std::string network_name = PlaneMeshNetworkName(plane_name);
  const std::string inspect_command =
      command_support_.ResolvedDockerCommand() + " network inspect " +
      command_support_.ShellQuote(network_name) + " >/dev/null 2>&1";
  if (!command_support_.RunCommandOk(inspect_command)) {
    return;
  }
  const std::string attached_count_command =
      command_support_.ResolvedDockerCommand() + " network inspect " +
      command_support_.ShellQuote(network_name) +
      " --format '{{len .Containers}}' 2>/dev/null";
  const std::string attached_count =
      command_support_.Trim(command_support_.RunCommandCapture(attached_count_command));
  if (!attached_count.empty() && attached_count != "0") {
    return;
  }
  const std::string remove_command =
      command_support_.ResolvedDockerCommand() + " network rm " +
      command_support_.ShellQuote(network_name) + " >/dev/null 2>&1";
  command_support_.RunCommandOk(remove_command);
}

void HostdComposeRuntimeSupport::RunComposeCommand(
    const std::string& compose_file_path,
    const std::string& subcommand,
    ComposeMode compose_mode) const {
  if (compose_mode == ComposeMode::Skip) {
    return;
  }

  std::string effective_subcommand = subcommand;
  if (subcommand == "up -d") {
    effective_subcommand += " --remove-orphans";
  }
  const std::string command =
      command_support_.ResolvedDockerComposeCommand() + " -f " +
      command_support_.ShellQuote(compose_file_path) + " " + effective_subcommand;
  const int rc = std::system(command.c_str());
  if (rc != 0) {
    if (subcommand == "down" && !ComposeProjectHasContainers(compose_file_path)) {
      return;
    }
    throw std::runtime_error(
        "compose command failed with exit code " + std::to_string(rc) + ": " + command);
  }
}

bool HostdComposeRuntimeSupport::DockerImageExists(const std::string& image) const {
  return command_support_.RunCommandOk(
      command_support_.ResolvedDockerCommand() + " image inspect " +
      command_support_.ShellQuote(image) + " >/dev/null 2>&1");
}

bool HostdComposeRuntimeSupport::LocalRuntimeBinaryExists(
    const std::filesystem::path& repo_root,
    const std::string& image) const {
  if (image == "naim/infer-runtime:dev") {
    return std::filesystem::exists(repo_root / "build" / "linux" / "x64" / "naim-inferctl") &&
           TurboQuantRuntimeBinaryExists(repo_root, image);
  }
  if (image == "naim/worker-runtime:dev") {
    return std::filesystem::exists(repo_root / "build" / "linux" / "x64" / "naim-workerd") &&
           TurboQuantRuntimeBinaryExists(repo_root, image);
  }
  if (image == "naim/skills-runtime:dev") {
    return std::filesystem::exists(repo_root / "build" / "linux" / "x64" / "naim-skillsd");
  }
  if (image == "naim/webgateway-runtime:dev") {
    return std::filesystem::exists(repo_root / "build" / "linux" / "x64" / "naim-webgatewayd");
  }
  if (image == "naim/interaction-runtime:dev") {
    return std::filesystem::exists(repo_root / "build" / "linux" / "x64" / "naim-interactiond");
  }
  return true;
}

bool HostdComposeRuntimeSupport::TurboQuantRuntimeBinaryExists(
    const std::filesystem::path& repo_root,
    const std::string& image) const {
  if (image == "naim/infer-runtime:dev") {
    return std::filesystem::exists(
        repo_root / "build-turboquant" / "linux" / "x64" / "bin" / "llama-server");
  }
  if (image == "naim/worker-runtime:dev") {
    return std::filesystem::exists(
        repo_root / "build-turboquant" / "linux" / "x64" / "bin" / "rpc-server");
  }
  return true;
}

void HostdComposeRuntimeSupport::EnsureLocalRuntimeBinary(
    const std::filesystem::path& repo_root,
    const std::string& image) const {
  if (LocalRuntimeBinaryExists(repo_root, image)) {
    return;
  }

  const std::filesystem::path build_script = repo_root / "scripts" / "build-target.sh";
  if (!std::filesystem::exists(build_script)) {
    throw std::runtime_error(
        "runtime image requires local binary, but build-target.sh is unavailable");
  }

  const std::string command =
      "cd " + command_support_.ShellQuote(repo_root.string()) +
      " && " + command_support_.ShellQuote(build_script.string()) + " linux x64 Debug";
  if (!command_support_.RunCommandOk(command)) {
    throw std::runtime_error(
        "failed to auto-build local naim binaries required for " + image);
  }

  if (!TurboQuantRuntimeBinaryExists(repo_root, image)) {
    const std::filesystem::path turboquant_script =
        repo_root / "scripts" / "build-turboquant-runtime.sh";
    if (!std::filesystem::exists(turboquant_script)) {
      throw std::runtime_error(
          "runtime image requires turboquant binary, but build-turboquant-runtime.sh is unavailable");
    }
    const std::string turboquant_command =
        "cd " + command_support_.ShellQuote(repo_root.string()) +
        " && " + command_support_.ShellQuote(turboquant_script.string()) + " linux x64 Debug";
    if (!command_support_.RunCommandOk(turboquant_command)) {
      throw std::runtime_error(
          "failed to auto-build local turboquant binaries required for " + image);
    }
  }

  if (!LocalRuntimeBinaryExists(repo_root, image)) {
    throw std::runtime_error(
        "local runtime binary is still missing after build for " + image);
  }
}

void HostdComposeRuntimeSupport::BuildNaimRuntimeImage(
    const std::filesystem::path& repo_root,
    const std::string& image) const {
  const std::string repo_root_quoted = command_support_.ShellQuote(repo_root.string());
  const std::string docker = command_support_.ResolvedDockerCommand();

  auto build_base = [&]() {
    const std::string command =
        docker + " build -f " +
        command_support_.ShellQuote((repo_root / "runtime" / "base" / "Dockerfile").string()) +
        " -t " + command_support_.ShellQuote("naim/base-runtime:dev") + " " + repo_root_quoted;
    if (!command_support_.RunCommandOk(command)) {
      throw std::runtime_error("failed to auto-build naim/base-runtime:dev");
    }
  };

  auto build_runtime = [&](const std::string& dockerfile, const std::string& target_image) {
    const std::string command =
        docker + " build " +
        " -f " + command_support_.ShellQuote((repo_root / dockerfile).string()) +
        " -t " + command_support_.ShellQuote(target_image) + " " + repo_root_quoted;
    if (!command_support_.RunCommandOk(command)) {
      throw std::runtime_error("failed to auto-build " + target_image);
    }
  };

  if (image == "naim/base-runtime:dev") {
    build_base();
    return;
  }
  if (image == "naim/infer-runtime:dev") {
    if (!DockerImageExists("naim/base-runtime:dev")) {
      build_base();
    }
    EnsureLocalRuntimeBinary(repo_root, image);
    build_runtime("runtime/infer/Dockerfile", image);
    return;
  }
  if (image == "naim/worker-runtime:dev") {
    if (!DockerImageExists("naim/base-runtime:dev")) {
      build_base();
    }
    EnsureLocalRuntimeBinary(repo_root, image);
    build_runtime("runtime/worker/Dockerfile", image);
    return;
  }
  if (image == "naim/skills-runtime:dev") {
    if (!DockerImageExists("naim/base-runtime:dev")) {
      build_base();
    }
    EnsureLocalRuntimeBinary(repo_root, image);
    build_runtime("runtime/skills/Dockerfile", image);
    return;
  }
  if (image == "naim/webgateway-runtime:dev") {
    if (!DockerImageExists("naim/base-runtime:dev")) {
      build_base();
    }
    EnsureLocalRuntimeBinary(repo_root, image);
    build_runtime("runtime/browsing/Dockerfile", image);
    return;
  }
  if (image == "naim/interaction-runtime:dev") {
    if (!DockerImageExists("naim/base-runtime:dev")) {
      build_base();
    }
    EnsureLocalRuntimeBinary(repo_root, image);
    build_runtime("runtime/interaction/Dockerfile", image);
    return;
  }
  if (image == "naim/web-ui:dev") {
    const std::string command =
        docker + " build -f " +
        command_support_.ShellQuote((repo_root / "runtime" / "web-ui" / "Dockerfile").string()) +
        " -t " + command_support_.ShellQuote(image) + " " + repo_root_quoted;
    if (!command_support_.RunCommandOk(command)) {
      throw std::runtime_error("failed to auto-build " + image);
    }
    return;
  }

  throw std::runtime_error("unsupported auto-build image '" + image + "'");
}

void HostdComposeRuntimeSupport::EnsureRuntimeImageAvailable(const std::string& image) const {
  static std::set<std::string> ensured_images;
  if (image.empty() || ensured_images.count(image) > 0 || DockerImageExists(image)) {
    if (!image.empty()) {
      ensured_images.insert(image);
    }
    return;
  }

  if (image.rfind("naim/", 0) == 0 && image.ends_with(":dev")) {
    if (const auto repo_root = repo_root_support_.DetectNaimRepoRoot(); repo_root.has_value()) {
      BuildNaimRuntimeImage(*repo_root, image);
      if (DockerImageExists(image)) {
        ensured_images.insert(image);
        return;
      }
    }
  }

  if (!command_support_.RunCommandOk(
          command_support_.ResolvedDockerCommand() + " pull " +
          command_support_.ShellQuote(image))) {
    throw std::runtime_error(
        "required runtime image is unavailable locally and auto-build/pull failed: " + image);
  }
  if (!DockerImageExists(image)) {
    throw std::runtime_error("required runtime image is still unavailable after pull: " + image);
  }
  ensured_images.insert(image);
}

void HostdComposeRuntimeSupport::EnsureComposeImagesAvailable(
    const naim::NodeComposePlan& compose_plan,
    ComposeMode compose_mode) const {
  if (compose_mode != ComposeMode::Exec) {
    return;
  }
  std::set<std::string> images;
  for (const auto& service : compose_plan.services) {
    if (!service.image.empty()) {
      images.insert(service.image);
    }
  }
  for (const auto& image : images) {
    EnsureRuntimeImageAvailable(image);
  }
}

}  // namespace naim::hostd
