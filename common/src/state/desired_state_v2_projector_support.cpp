#include "naim/state/desired_state_v2_projector_support.h"

namespace naim {

nlohmann::json DesiredStateV2ProjectorSupport::ProjectServiceStart(
    const InstanceSpec& instance,
    const std::string& default_command) {
  if (instance.command.empty() || instance.command == default_command) {
    return nullptr;
  }
  return {
      {"type", "command"},
      {"command", instance.command},
  };
}

nlohmann::json DesiredStateV2ProjectorSupport::ProjectPublishedPorts(
    const InstanceSpec& instance) {
  if (instance.published_ports.empty()) {
    return nlohmann::json::array();
  }
  nlohmann::json ports = nlohmann::json::array();
  for (const auto& port : instance.published_ports) {
    ports.push_back({
        {"host_ip", port.host_ip},
        {"host_port", port.host_port},
        {"container_port", port.container_port},
    });
  }
  return ports;
}

nlohmann::json DesiredStateV2ProjectorSupport::ProjectServiceStorage(
    const DiskSpec* disk) {
  if (disk == nullptr) {
    return nullptr;
  }
  const bool default_size = disk->kind == DiskKind::InferPrivate
          ? disk->size_gb == kDefaultInferPrivateDiskSizeGb
          : disk->kind == DiskKind::WorkerPrivate
              ? disk->size_gb == kDefaultWorkerPrivateDiskSizeGb
              : disk->kind == DiskKind::SkillsPrivate
                  ? disk->size_gb == kDefaultSkillsPrivateDiskSizeGb
                  : disk->kind == DiskKind::BrowsingPrivate
                      ? disk->size_gb == kDefaultWebGatewayPrivateDiskSizeGb
                      : disk->kind == DiskKind::InteractionPrivate
                          ? disk->size_gb == kDefaultAppPrivateDiskSizeGb
                      : disk->size_gb == kDefaultAppPrivateDiskSizeGb;
  const bool default_mount = disk->container_path == "/naim/private";
  if (default_size && default_mount) {
    return nullptr;
  }
  return {
      {"size_gb", disk->size_gb},
      {"mount_path", disk->container_path},
  };
}

nlohmann::json DesiredStateV2ProjectorSupport::ProjectAppVolumes(
    const DiskSpec* disk) {
  if (disk == nullptr) {
    return nlohmann::json::array();
  }
  return nlohmann::json::array(
      {{{"name", "private-data"},
        {"type", "persistent"},
        {"size_gb", disk->size_gb},
        {"mount_path", disk->container_path},
        {"access", "rw"}}});
}

std::map<std::string, std::string> DesiredStateV2ProjectorSupport::ProjectCustomEnv(
    const InstanceSpec& instance,
    bool strip_naim_env) {
  std::map<std::string, std::string> env;
  for (const auto& [key, value] : instance.environment) {
    if (strip_naim_env && key.rfind("NAIM_", 0) == 0) {
      continue;
    }
    env[key] = value;
  }
  return env;
}

bool DesiredStateV2ProjectorSupport::IsDefaultWorkerImage(
    const std::string& image) {
  return image == kDefaultWorkerImage;
}

}  // namespace naim
