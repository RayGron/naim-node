#pragma once

#include <optional>
#include <string>
#include <vector>

#include "naim/state/models.h"

namespace naim {

class DesiredStateSqliteCodec final {
 public:
  static std::string SerializeInferenceSettings(const InferenceRuntimeSettings& settings);
  static std::string SerializeBootstrapModelSpec(
      const std::optional<BootstrapModelSpec>& bootstrap_model);
  static std::optional<BootstrapModelSpec> DeserializeBootstrapModelSpec(
      const std::string& json_text);
  static std::optional<InteractionSettings> DeserializeInteractionSettings(
      const std::string& json_text);
  static InferenceRuntimeSettings DeserializeInferenceSettings(const std::string& json_text);
  static std::string SerializeGatewaySettings(const GatewaySettings& settings);
  static std::string SerializeRuntimeGpuNodes(const std::vector<RuntimeGpuNode>& gpu_nodes);
  static std::vector<RuntimeGpuNode> DeserializeRuntimeGpuNodes(const std::string& json_text);
  static GatewaySettings DeserializeGatewaySettings(const std::string& json_text);
  static DiskKind ParseDiskKind(const std::string& value);
  static InstanceRole ParseInstanceRole(const std::string& value);
};

}  // namespace naim
