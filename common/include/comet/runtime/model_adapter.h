#pragma once

#include <string>
#include <string_view>
#include <vector>

#include <nlohmann/json.hpp>

namespace comet::runtime {

struct ModelIdentity {
  std::string model_id;
  std::string served_model_name;
  std::string cached_local_model_path;
  std::string cached_runtime_model_path;
  std::string runtime_profile;
  std::vector<std::string> llama_args;
};

enum class ModelFamily {
  Generic,
  QwenLegacyChat,
  Gemma4,
};

struct ModelAdapterPolicy {
  bool thinking_enabled = false;
};

class ModelAdapter final {
 public:
  static ModelIdentity IdentityFromActiveModelJson(const nlohmann::json& value);
  static ModelFamily DetectFamily(const ModelIdentity& identity);

  static void AdaptInteractionPayload(
      nlohmann::json* payload,
      const ModelIdentity& identity,
      const ModelAdapterPolicy& policy);
  static void AdaptLaunchArgs(
      std::vector<std::string>* args,
      const ModelIdentity& identity);
  static std::string BuildLegacyChatPrompt(
      const nlohmann::json& payload,
      const ModelIdentity& identity);
  static std::string SanitizeVisibleText(
      std::string text,
      const ModelIdentity& identity);
  static void SanitizeChatCompletionPayload(
      nlohmann::json* payload,
      const ModelIdentity& identity);
};

std::string ToString(ModelFamily family);

}  // namespace comet::runtime
