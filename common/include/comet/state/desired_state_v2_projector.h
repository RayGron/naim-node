#pragma once

#include <nlohmann/json.hpp>

#include "comet/state/models.h"

namespace comet {

class DesiredStateV2Projector final {
 public:
  static nlohmann::json Project(const DesiredState& state);

 private:
  explicit DesiredStateV2Projector(const DesiredState& state);

  nlohmann::json ProjectJson();

  void CollectInstancesAndDisks();
  void ProjectIdentity();
  void ProjectHooks();
  void ProjectModel();
  void ProjectTopology();
  void ProjectInteraction();
  void ProjectRuntime();
  void ProjectNetwork();
  void ProjectInfer();
  void ProjectWorker();
  void ProjectApp();
  void ProjectSkills();
  void ProjectResources();

  bool ShouldEmitTopology() const;
  bool IsDefaultSingleNodeTopology() const;
  std::string DefaultNodeName() const;
  int InferReplicaCount() const;

  const InstanceSpec* FindInstance(InstanceRole role) const;
  std::vector<const InstanceSpec*> FindWorkerInstances() const;
  const DiskSpec* FindDiskByName(const std::string& name) const;
  const DiskSpec* FindSharedDisk() const;

  nlohmann::json ProjectModelSource() const;
  nlohmann::json ProjectServiceStart(
      const InstanceSpec& instance,
      const std::string& default_command) const;
  nlohmann::json ProjectPublishedPorts(const InstanceSpec& instance) const;
  nlohmann::json ProjectServiceStorage(const DiskSpec* disk) const;
  nlohmann::json ProjectAppVolumes(const DiskSpec* disk) const;
  std::map<std::string, std::string> ProjectCustomEnv(
      const InstanceSpec& instance,
      bool strip_comet_env) const;

  bool IsDefaultWorkerImage(const std::string& image) const;
  std::string PreferredModelSourceType() const;
  std::string AddBundlePrefixIfRelative(const std::string& value) const;

  const DesiredState& state_;
  nlohmann::json value_;
  const InstanceSpec* infer_instance_ = nullptr;
  const InstanceSpec* app_instance_ = nullptr;
  const InstanceSpec* skills_instance_ = nullptr;
  std::vector<const InstanceSpec*> worker_instances_;
  const DiskSpec* shared_disk_ = nullptr;
};

}  // namespace comet
