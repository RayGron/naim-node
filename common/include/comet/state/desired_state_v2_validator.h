#pragma once

#include <nlohmann/json.hpp>

namespace comet {

class DesiredStateV2Validator final {
 public:
  static void ValidateOrThrow(const nlohmann::json& value);

 private:
  explicit DesiredStateV2Validator(const nlohmann::json& value);

  void Validate();
  void ValidateTopLevel() const;
  void ValidateModel() const;
  void ValidateFeatures() const;
  void ValidateRuntime() const;
  void ValidateTopology() const;
  void ValidateInfer() const;
  void ValidateWorker() const;
  void ValidateWorkerResources() const;
  void ValidateApp() const;
  void ValidateSkills() const;
  void ValidateBrowsing() const;
  void ValidateHooks() const;

  void RequireObject(const char* field_name) const;
  void ValidateStartBlock(const nlohmann::json& service_json, const char* service_name) const;
  std::optional<std::string> TopologyNodeExecutionMode(const std::string& node_name) const;
  void ValidateNodeRoleCompatibility(
      const std::string& node_name,
      const char* service_name,
      const char* required_role) const;

  const nlohmann::json& value_;
};

}  // namespace comet
