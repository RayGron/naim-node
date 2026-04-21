#pragma once

#include <nlohmann/json.hpp>

#include "naim/state/models.h"

namespace naim {

class StateJsonRuntimeCodecs {
 public:
  static nlohmann::json ToJson(const PublishedPort& port);
  static nlohmann::json ToJson(const NodeInventory& node);
  static nlohmann::json ToJson(const WorkerGroupMemberSpec& member);
  static nlohmann::json ToJson(const WorkerGroupSpec& group);
  static nlohmann::json ToJson(const RuntimeGpuNode& gpu_node);
  static nlohmann::json ToJson(const DiskSpec& disk);
  static nlohmann::json ToJson(const InstanceSpec& instance);

  static PublishedPort PublishedPortFromJson(const nlohmann::json& value);
  static NodeInventory NodeInventoryFromJson(const nlohmann::json& value);
  static WorkerGroupMemberSpec WorkerGroupMemberSpecFromJson(
      const nlohmann::json& value);
  static WorkerGroupSpec WorkerGroupSpecFromJson(const nlohmann::json& value);
  static RuntimeGpuNode RuntimeGpuNodeFromJson(const nlohmann::json& value);
  static DiskSpec DiskSpecFromJson(const nlohmann::json& value);
  static InstanceSpec InstanceSpecFromJson(const nlohmann::json& value);

  static DiskKind ParseDiskKind(const std::string& value);
  static InstanceRole ParseInstanceRole(const std::string& value);
};

}  // namespace naim
