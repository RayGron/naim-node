#include "app/infer_command_line.h"

#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <iostream>
#include <stdexcept>
#include <string_view>

namespace fs = std::filesystem;

namespace comet::infer {

std::string InferCommandLine::DefaultConfigPath() const {
  if (const char* path = std::getenv("COMET_INFER_RUNTIME_CONFIG")) {
    if (std::strlen(path) > 0) {
      return path;
    }
  }
  const char* control_root = std::getenv("COMET_CONTROL_ROOT");
  const char* plane_name = std::getenv("COMET_PLANE_NAME");
  const std::string root =
      control_root != nullptr && std::strlen(control_root) > 0
          ? control_root
          : std::string("/comet/shared/control/") +
                ((plane_name != nullptr && std::strlen(plane_name) > 0) ? plane_name : "unknown");
  return root + "/infer-runtime.json";
}

std::string InferCommandLine::DefaultProfilesPath() const {
  if (const char* path = std::getenv("COMET_INFER_PROFILES_PATH")) {
    if (std::strlen(path) > 0) {
      return path;
    }
  }
  if (fs::exists("/runtime/infer/runtime-profiles.json")) {
    return "/runtime/infer/runtime-profiles.json";
  }
  return "runtime/infer/runtime-profiles.json";
}

void InferCommandLine::PrintUsage() const {
  std::cout
      << "Usage:\n"
      << "  comet-inferctl list-profiles [--profiles <path>]\n"
      << "  comet-inferctl show-config [--config <path>]\n"
      << "  comet-inferctl show-active-model [--config <path>]\n"
      << "  comet-inferctl validate-config [--config <path>]\n"
      << "  comet-inferctl prepare-runtime [--config <path>] [--apply]\n"
      << "  comet-inferctl bootstrap-runtime [--config <path>] [--profile <name>] [--profiles <path>] [--apply]\n"
      << "  comet-inferctl runtime-assets-status [--config <path>]\n"
      << "  comet-inferctl preload-model [--config <path>] --alias <name> --source-model-id <id> --local-model-path <path> [--runtime-model-path <path>] [--apply]\n"
      << "  comet-inferctl cache-status [--config <path>] --alias <name> --local-model-path <path>\n"
      << "  comet-inferctl switch-model [--config <path>] --model-id <id> [--served-model-name <name>] [--tp <n>] [--pp <n>] [--gpu-memory-utilization <0-1>] [--runtime-profile <name>] [--apply]\n"
      << "  comet-inferctl gateway-plan [--config <path>] [--apply]\n"
      << "  comet-inferctl gateway-status [--config <path>]\n"
      << "  comet-inferctl status [--config <path>] [--apply]\n"
      << "  comet-inferctl stop [--config <path>] [--apply]\n"
      << "  comet-inferctl plan-launch [--config <path>]\n"
      << "  comet-inferctl doctor [--config <path>] [--checks <csv>]\n"
      << "  comet-inferctl bootstrap-dry-run [--config <path>] [--profile <name>] [--profiles <path>] [--apply]\n"
      << "  comet-inferctl container-boot [--config <path>] [--backend <auto|embedded|llama|llama-rpc-head>]\n"
      << "  comet-inferctl launch-embedded-runtime [--config <path>]\n"
      << "  comet-inferctl launch-runtime [--config <path>] [--backend <auto|embedded|llama|llama-rpc-head>]\n"
      << "  comet-inferctl probe-url <url>\n";
}

InferCommandLineOptions InferCommandLine::Parse(int argc, char** argv) const {
  if (argc < 2) {
    PrintUsage();
    std::exit(2);
  }

  InferCommandLineOptions options;
  options.command = argv[1];
  options.config_path = DefaultConfigPath();
  options.profiles_path = DefaultProfilesPath();
  if (const char* backend = std::getenv("COMET_INFER_RUNTIME_BACKEND")) {
    if (std::strlen(backend) > 0) {
      options.backend = backend;
    }
  }

  int index = 2;
  if (options.command == "probe-url") {
    if (argc < 3) {
      throw std::runtime_error("probe-url requires a url");
    }
    options.probe_url = argv[2];
    return options;
  }

  while (index < argc) {
    const std::string_view option = argv[index];
    auto need_value = [&](const char* name) -> std::string {
      if (index + 1 >= argc) {
        throw std::runtime_error(std::string(name) + " requires a value");
      }
      ++index;
      return argv[index];
    };

    if (option == "--config") {
      options.config_path = need_value("--config");
    } else if (option == "--profiles") {
      options.profiles_path = need_value("--profiles");
    } else if (option == "--profile" || option == "--runtime-profile") {
      options.profile = need_value("--profile");
    } else if (option == "--checks") {
      options.checks = need_value("--checks");
    } else if (option == "--alias") {
      options.alias = need_value("--alias");
    } else if (option == "--source-model-id") {
      options.source_model_id = need_value("--source-model-id");
    } else if (option == "--local-model-path") {
      options.local_model_path = need_value("--local-model-path");
    } else if (option == "--runtime-model-path") {
      options.runtime_model_path = need_value("--runtime-model-path");
    } else if (option == "--model-id") {
      options.model_id = need_value("--model-id");
    } else if (option == "--served-model-name") {
      options.served_model_name = need_value("--served-model-name");
    } else if (option == "--tp") {
      options.tp = std::stoi(need_value("--tp"));
    } else if (option == "--pp") {
      options.pp = std::stoi(need_value("--pp"));
    } else if (option == "--gpu-memory-utilization") {
      options.gpu_memory_utilization = std::stod(need_value("--gpu-memory-utilization"));
    } else if (option == "--backend") {
      options.backend = need_value("--backend");
    } else if (option == "--apply") {
      options.apply = true;
    } else if (option == "-h" || option == "--help") {
      PrintUsage();
      std::exit(0);
    } else {
      throw std::runtime_error("unknown argument: " + std::string(option));
    }
    ++index;
  }

  return options;
}

}  // namespace comet::infer
