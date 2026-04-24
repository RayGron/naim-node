#include "plane/plane_placement_payload_builder.h"

#include <algorithm>
#include <utility>

#include "naim/state/desired_state_placement_resolver.h"

namespace naim::controller {

PlanePlacementPayloadBuilder::PlanePlacementPayloadBuilder(
    const naim::DesiredState& desired_state)
    : desired_state_(desired_state) {}

nlohmann::json PlanePlacementPayloadBuilder::Build() const {
  const naim::DesiredStatePlacementResolver placement_resolver(desired_state_);
  const auto execution_node_name = placement_resolver.ExecutionNodeName();
  const auto app_host_auth_mode = ResolveExternalAppHostAuthMode();

  nlohmann::json payload{
      {"mode",
       execution_node_name.has_value() ? nlohmann::json("execution-node")
                                     : nlohmann::json("legacy-topology-compatibility")},
      {"execution_node",
       execution_node_name.has_value() ? nlohmann::json(*execution_node_name)
                                     : nlohmann::json(nullptr)},
      {"app_host",
       desired_state_.app_host.has_value()
           ? nlohmann::json{
                 {"enabled", true},
                 {"address", desired_state_.app_host->address},
                 {"auth_mode",
                  app_host_auth_mode.has_value() ? nlohmann::json(*app_host_auth_mode)
                                                 : nlohmann::json(nullptr)},
             }
           : nlohmann::json{
                 {"enabled", false},
                 {"address", nullptr},
                 {"auth_mode", nullptr},
             }},
  };

  nlohmann::json service_targets = nlohmann::json::array();
  if (const auto infer_node_name = FindFirstInstanceNodeName(naim::InstanceRole::Infer);
      infer_node_name.has_value()) {
    service_targets.push_back(nlohmann::json{
        {"service", "infer"},
        {"target_type", "node"},
        {"target", *infer_node_name},
    });
  }

  const auto worker_node_names = FindInstanceNodeNames(naim::InstanceRole::Worker);
  if (!worker_node_names.empty()) {
    service_targets.push_back(nlohmann::json{
        {"service", "worker"},
        {"target_type", "node-group"},
        {"targets", nlohmann::json(worker_node_names)},
    });
  }

  const auto app_instances = FindInstances(naim::InstanceRole::App);
  if (!app_instances.empty()) {
    const bool external_app_host =
        desired_state_.app_host.has_value() && !desired_state_.app_host->address.empty();
    for (const auto* instance : app_instances) {
      if (instance == nullptr) {
        continue;
      }
      const auto app_name_it = instance->environment.find("NAIM_APP_NAME");
      const std::string app_name = app_name_it == instance->environment.end()
                                       ? instance->name
                                       : app_name_it->second;
      const auto primary_it = instance->environment.find("NAIM_APP_PRIMARY");
      const bool primary = primary_it != instance->environment.end() && primary_it->second == "true";
      service_targets.push_back(nlohmann::json{
          {"service", primary ? nlohmann::json("app")
                              : nlohmann::json("app:" + app_name)},
          {"target_type", primary && external_app_host ? nlohmann::json("external-app-host")
                                                       : nlohmann::json("node")},
          {"target",
           primary && external_app_host ? nlohmann::json(desired_state_.app_host->address)
                                        : nlohmann::json(instance->node_name)},
      });
    }
  }

  if (const auto skills_node_name = FindFirstInstanceNodeName(naim::InstanceRole::Skills);
      skills_node_name.has_value()) {
    const bool external_app_host =
        desired_state_.app_host.has_value() && !desired_state_.app_host->address.empty();
    service_targets.push_back(nlohmann::json{
        {"service", "skills-runtime"},
        {"target_type", external_app_host ? nlohmann::json("external-app-host")
                                          : nlohmann::json("node")},
        {"target",
         external_app_host ? nlohmann::json(desired_state_.app_host->address)
                           : nlohmann::json(*skills_node_name)},
        {"binding", external_app_host ? nlohmann::json("skills-follow-app")
                                      : nlohmann::json("local-node")},
    });
  }

  if (desired_state_.skills.has_value() && desired_state_.skills->enabled) {
    service_targets.push_back(nlohmann::json{
        {"service", "skills-factory"},
        {"target_type", "naim"},
        {"target", "naim-controller"},
    });
  }

  if (const auto browsing_node_name = FindFirstInstanceNodeName(naim::InstanceRole::Browsing);
      browsing_node_name.has_value()) {
    service_targets.push_back(nlohmann::json{
        {"service", "webgateway"},
        {"target_type", "node"},
        {"target", *browsing_node_name},
    });
  }

  if (const auto interaction_node_name = FindFirstInstanceNodeName(naim::InstanceRole::Interaction);
      interaction_node_name.has_value()) {
    service_targets.push_back(nlohmann::json{
        {"service", "interaction-runtime"},
        {"target_type", "node"},
        {"target", *interaction_node_name},
    });
  }

  payload["service_targets"] = std::move(service_targets);
  return payload;
}

std::optional<std::string> PlanePlacementPayloadBuilder::ResolveExternalAppHostAuthMode() const {
  if (!desired_state_.app_host.has_value() || desired_state_.app_host->address.empty()) {
    return std::nullopt;
  }
  if (desired_state_.app_host->ssh_key_path.has_value() &&
      !desired_state_.app_host->ssh_key_path->empty()) {
    return std::string("ssh-key");
  }
  if (desired_state_.app_host->username.has_value() &&
      !desired_state_.app_host->username->empty() &&
      desired_state_.app_host->password.has_value() &&
      !desired_state_.app_host->password->empty()) {
    return std::string("password");
  }
  return std::nullopt;
}

std::optional<std::string> PlanePlacementPayloadBuilder::FindFirstInstanceNodeName(
    naim::InstanceRole role) const {
  const auto instance_it = std::find_if(
      desired_state_.instances.begin(),
      desired_state_.instances.end(),
      [&](const naim::InstanceSpec& instance) {
        return instance.role == role;
      });
  if (instance_it == desired_state_.instances.end()) {
    return std::nullopt;
  }
  return instance_it->node_name;
}

std::vector<const naim::InstanceSpec*> PlanePlacementPayloadBuilder::FindInstances(
    naim::InstanceRole role) const {
  std::vector<const naim::InstanceSpec*> instances;
  for (const auto& instance : desired_state_.instances) {
    if (instance.role == role) {
      instances.push_back(&instance);
    }
  }
  return instances;
}

std::set<std::string> PlanePlacementPayloadBuilder::FindInstanceNodeNames(
    naim::InstanceRole role) const {
  std::set<std::string> node_names;
  for (const auto& instance : desired_state_.instances) {
    if (instance.role == role && !instance.node_name.empty()) {
      node_names.insert(instance.node_name);
    }
  }
  return node_names;
}

}  // namespace naim::controller
