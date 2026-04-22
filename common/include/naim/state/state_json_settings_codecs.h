#pragma once

#include <nlohmann/json.hpp>

#include "naim/state/models.h"

namespace naim {

class StateJsonSettingsCodecs {
 public:
  static nlohmann::json ToJson(const BootstrapModelSpec& bootstrap_model);
  static nlohmann::json ToJson(const InteractionSettings& interaction);
  static nlohmann::json ToJson(const SkillsSettings& skills);
  static nlohmann::json ToJson(const BrowsingPolicySettings& policy);
  static nlohmann::json ToJson(const BrowsingSettings& browsing);
  static nlohmann::json ToJson(const KnowledgeContextPolicySettings& policy);
  static nlohmann::json ToJson(const KnowledgeSettings& knowledge);
  static nlohmann::json ToJson(const ExternalAppHostConfig& app_host);

  static BootstrapModelSpec BootstrapModelSpecFromJson(
      const nlohmann::json& value);
  static InteractionSettings InteractionSettingsFromJson(
      const nlohmann::json& value);
  static SkillsSettings SkillsSettingsFromJson(const nlohmann::json& value);
  static BrowsingSettings BrowsingSettingsFromJson(const nlohmann::json& value);
  static KnowledgeSettings KnowledgeSettingsFromJson(const nlohmann::json& value);
  static ExternalAppHostConfig ExternalAppHostConfigFromJson(
      const nlohmann::json& value);
};

}  // namespace naim
