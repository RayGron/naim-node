#pragma once

#include <map>
#include <string>
#include <string_view>

#include <nlohmann/json.hpp>

#include "naim/state/models.h"

namespace naim {

class DesiredStateV2ProjectorSupport {
 public:
  static constexpr int kDefaultSharedDiskSizeGb = 40;
  static constexpr int kDefaultInferPrivateDiskSizeGb = 12;
  static constexpr int kDefaultWorkerPrivateDiskSizeGb = 2;
  static constexpr int kDefaultAppPrivateDiskSizeGb = 8;
  static constexpr int kDefaultSkillsPrivateDiskSizeGb = 1;
  static constexpr int kDefaultWebGatewayPrivateDiskSizeGb = 1;

  static constexpr std::string_view kDefaultInferImage = "naim/infer-runtime:dev";
  static constexpr std::string_view kDefaultWorkerImage = "naim/worker-runtime:dev";
  static constexpr std::string_view kDefaultSkillsImage = "naim/skills-runtime:dev";
  static constexpr std::string_view kDefaultWebGatewayImage =
      "naim/webgateway-runtime:dev";

  static constexpr std::string_view kDefaultInferCommand =
      "/runtime/bin/naim-inferctl container-boot";
  static constexpr std::string_view kDefaultWorkerCommand =
      "/runtime/bin/naim-workerd";
  static constexpr std::string_view kDefaultSkillsCommand =
      "/runtime/bin/naim-skillsd";
  static constexpr std::string_view kDefaultWebGatewayCommand =
      "/runtime/bin/naim-webgatewayd";

  static nlohmann::json ProjectServiceStart(
      const InstanceSpec& instance,
      const std::string& default_command);
  static nlohmann::json ProjectPublishedPorts(const InstanceSpec& instance);
  static nlohmann::json ProjectServiceStorage(const DiskSpec* disk);
  static nlohmann::json ProjectAppVolumes(const DiskSpec* disk);
  static std::map<std::string, std::string> ProjectCustomEnv(
      const InstanceSpec& instance,
      bool strip_naim_env);

  static bool IsDefaultWorkerImage(const std::string& image);
};

}  // namespace naim
