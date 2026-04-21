#include "naim/state/state_json_settings_codecs.h"

namespace naim {

namespace {

using nlohmann::json;

json CompletionPolicyToJson(
    const InteractionSettings::CompletionPolicy& completion_policy) {
  json result = {
      {"response_mode", completion_policy.response_mode},
      {"max_tokens", completion_policy.max_tokens},
      {"max_continuations", completion_policy.max_continuations},
      {"max_total_completion_tokens",
       completion_policy.max_total_completion_tokens},
      {"max_elapsed_time_ms", completion_policy.max_elapsed_time_ms},
  };
  if (completion_policy.target_completion_tokens.has_value()) {
    result["target_completion_tokens"] =
        *completion_policy.target_completion_tokens;
  }
  if (completion_policy.semantic_goal.has_value()) {
    result["semantic_goal"] = *completion_policy.semantic_goal;
  }
  return result;
}

std::optional<InteractionSettings::CompletionPolicy> CompletionPolicyFromJson(
    const json& value,
    const std::string& field_name) {
  if (!value.contains(field_name) || !value.at(field_name).is_object()) {
    return std::nullopt;
  }

  InteractionSettings::CompletionPolicy completion_policy;
  const auto& policy_value = value.at(field_name);
  completion_policy.response_mode =
      policy_value.value("response_mode", completion_policy.response_mode);
  completion_policy.max_tokens =
      policy_value.value("max_tokens", completion_policy.max_tokens);
  if (policy_value.contains("target_completion_tokens") &&
      !policy_value.at("target_completion_tokens").is_null()) {
    completion_policy.target_completion_tokens =
        policy_value.at("target_completion_tokens").get<int>();
  }
  completion_policy.max_continuations =
      policy_value.value("max_continuations", completion_policy.max_continuations);
  completion_policy.max_total_completion_tokens = policy_value.value(
      "max_total_completion_tokens",
      completion_policy.max_total_completion_tokens);
  completion_policy.max_elapsed_time_ms =
      policy_value.value("max_elapsed_time_ms", completion_policy.max_elapsed_time_ms);
  if (policy_value.contains("semantic_goal") &&
      !policy_value.at("semantic_goal").is_null()) {
    completion_policy.semantic_goal = policy_value.at("semantic_goal").get<std::string>();
  }
  return completion_policy;
}

}  // namespace

json StateJsonSettingsCodecs::ToJson(
    const BootstrapModelSpec& bootstrap_model) {
  json result = {
      {"model_id", bootstrap_model.model_id},
      {"materialization_mode", bootstrap_model.materialization_mode},
  };
  if (bootstrap_model.served_model_name.has_value()) {
    result["served_model_name"] = *bootstrap_model.served_model_name;
  }
  if (bootstrap_model.local_path.has_value()) {
    result["local_path"] = *bootstrap_model.local_path;
  }
  if (bootstrap_model.source_node_name.has_value()) {
    result["source_node_name"] = *bootstrap_model.source_node_name;
  }
  if (!bootstrap_model.source_paths.empty()) {
    result["source_paths"] = bootstrap_model.source_paths;
  }
  if (bootstrap_model.source_format.has_value()) {
    result["source_format"] = *bootstrap_model.source_format;
  }
  if (bootstrap_model.source_quantization.has_value()) {
    result["source_quantization"] = *bootstrap_model.source_quantization;
  }
  if (bootstrap_model.desired_output_format.has_value()) {
    result["desired_output_format"] = *bootstrap_model.desired_output_format;
  }
  if (bootstrap_model.quantization.has_value()) {
    result["quantization"] = *bootstrap_model.quantization;
  }
  if (!bootstrap_model.keep_source) {
    result["keep_source"] = bootstrap_model.keep_source;
  }
  if (bootstrap_model.writeback_enabled) {
    result["writeback"] = {
        {"enabled", bootstrap_model.writeback_enabled},
        {"if_missing", bootstrap_model.writeback_if_missing},
    };
    if (bootstrap_model.writeback_target_node_name.has_value()) {
      result["writeback"]["target_node_name"] = *bootstrap_model.writeback_target_node_name;
    }
  }
  if (bootstrap_model.source_url.has_value()) {
    result["source_url"] = *bootstrap_model.source_url;
  }
  if (!bootstrap_model.source_urls.empty()) {
    result["source_urls"] = bootstrap_model.source_urls;
  }
  if (bootstrap_model.target_filename.has_value()) {
    result["target_filename"] = *bootstrap_model.target_filename;
  }
  if (bootstrap_model.sha256.has_value()) {
    result["sha256"] = *bootstrap_model.sha256;
  }
  return result;
}

json StateJsonSettingsCodecs::ToJson(const InteractionSettings& interaction) {
  json result = {
      {"thinking_enabled", interaction.thinking_enabled},
      {"default_response_language", interaction.default_response_language},
      {"supported_response_languages", interaction.supported_response_languages},
      {"follow_user_language", interaction.follow_user_language},
  };
  if (interaction.system_prompt.has_value()) {
    result["system_prompt"] = *interaction.system_prompt;
  }
  if (interaction.analysis_system_prompt.has_value()) {
    result["analysis_system_prompt"] = *interaction.analysis_system_prompt;
  }
  if (interaction.default_temperature.has_value()) {
    result["default_temperature"] = *interaction.default_temperature;
  }
  if (interaction.default_top_p.has_value()) {
    result["default_top_p"] = *interaction.default_top_p;
  }
  if (interaction.completion_policy.has_value()) {
    result["completion_policy"] =
        CompletionPolicyToJson(*interaction.completion_policy);
  }
  if (interaction.long_completion_policy.has_value()) {
    result["long_completion_policy"] =
        CompletionPolicyToJson(*interaction.long_completion_policy);
  }
  if (interaction.analysis_completion_policy.has_value()) {
    result["analysis_completion_policy"] =
        CompletionPolicyToJson(*interaction.analysis_completion_policy);
  }
  if (interaction.analysis_long_completion_policy.has_value()) {
    result["analysis_long_completion_policy"] =
        CompletionPolicyToJson(*interaction.analysis_long_completion_policy);
  }
  return result;
}

json StateJsonSettingsCodecs::ToJson(const SkillsSettings& skills) {
  json result = {
      {"enabled", skills.enabled},
  };
  if (!skills.factory_skill_ids.empty()) {
    result["factory_skill_ids"] = skills.factory_skill_ids;
  }
  return result;
}

json StateJsonSettingsCodecs::ToJson(const BrowsingPolicySettings& policy) {
  json result = {
      {"browser_session_enabled", policy.browser_session_enabled},
      {"rendered_browser_enabled", policy.rendered_browser_enabled},
      {"login_enabled", policy.login_enabled},
      {"max_search_results", policy.max_search_results},
      {"max_fetch_bytes", policy.max_fetch_bytes},
  };
  if (!policy.cef_enabled) {
    result["cef_enabled"] = policy.cef_enabled;
  }
  if (!policy.allowed_domains.empty()) {
    result["allowed_domains"] = policy.allowed_domains;
  }
  if (!policy.blocked_domains.empty()) {
    result["blocked_domains"] = policy.blocked_domains;
  }
  if (!policy.blocked_targets.empty()) {
    result["blocked_targets"] = policy.blocked_targets;
  }
  if (!policy.response_review_enabled) {
    result["response_review_enabled"] = policy.response_review_enabled;
  }
  if (policy.policy_version != "webgateway-v1") {
    result["policy_version"] = policy.policy_version;
  }
  return result;
}

json StateJsonSettingsCodecs::ToJson(const BrowsingSettings& browsing) {
  json result = {
      {"enabled", browsing.enabled},
  };
  if (browsing.policy.has_value()) {
    result["policy"] = ToJson(*browsing.policy);
  }
  return result;
}

json StateJsonSettingsCodecs::ToJson(const ExternalAppHostConfig& app_host) {
  json result = {
      {"address", app_host.address},
  };
  if (app_host.ssh_key_path.has_value()) {
    result["ssh_key_path"] = *app_host.ssh_key_path;
  }
  if (app_host.username.has_value()) {
    result["username"] = *app_host.username;
  }
  if (app_host.password.has_value()) {
    result["password"] = *app_host.password;
  }
  return result;
}

BootstrapModelSpec StateJsonSettingsCodecs::BootstrapModelSpecFromJson(
    const json& value) {
  BootstrapModelSpec bootstrap_model;
  bootstrap_model.model_id = value.value("model_id", std::string{});
  bootstrap_model.materialization_mode =
      value.value("materialization_mode", bootstrap_model.materialization_mode);
  if (value.contains("served_model_name") && !value.at("served_model_name").is_null()) {
    bootstrap_model.served_model_name = value.at("served_model_name").get<std::string>();
  }
  if (value.contains("local_path") && !value.at("local_path").is_null()) {
    bootstrap_model.local_path = value.at("local_path").get<std::string>();
  }
  if (value.contains("source_node_name") && !value.at("source_node_name").is_null()) {
    bootstrap_model.source_node_name = value.at("source_node_name").get<std::string>();
  }
  if (value.contains("source_paths") && value.at("source_paths").is_array()) {
    bootstrap_model.source_paths = value.at("source_paths").get<std::vector<std::string>>();
  }
  if (value.contains("source_format") && !value.at("source_format").is_null()) {
    bootstrap_model.source_format = value.at("source_format").get<std::string>();
  }
  if (value.contains("source_quantization") && !value.at("source_quantization").is_null()) {
    bootstrap_model.source_quantization = value.at("source_quantization").get<std::string>();
  }
  if (value.contains("desired_output_format") && !value.at("desired_output_format").is_null()) {
    bootstrap_model.desired_output_format = value.at("desired_output_format").get<std::string>();
  }
  if (value.contains("quantization") && !value.at("quantization").is_null()) {
    bootstrap_model.quantization = value.at("quantization").get<std::string>();
  }
  bootstrap_model.keep_source = value.value("keep_source", bootstrap_model.keep_source);
  if (value.contains("writeback") && value.at("writeback").is_object()) {
    const auto& writeback = value.at("writeback");
    bootstrap_model.writeback_enabled =
        writeback.value("enabled", bootstrap_model.writeback_enabled);
    bootstrap_model.writeback_if_missing =
        writeback.value("if_missing", bootstrap_model.writeback_if_missing);
    if (writeback.contains("target_node_name") &&
        !writeback.at("target_node_name").is_null()) {
      bootstrap_model.writeback_target_node_name =
          writeback.at("target_node_name").get<std::string>();
    }
  }
  if (value.contains("source_url") && !value.at("source_url").is_null()) {
    bootstrap_model.source_url = value.at("source_url").get<std::string>();
  }
  if (value.contains("source_urls") && value.at("source_urls").is_array()) {
    bootstrap_model.source_urls =
        value.at("source_urls").get<std::vector<std::string>>();
  }
  if (value.contains("target_filename") && !value.at("target_filename").is_null()) {
    bootstrap_model.target_filename = value.at("target_filename").get<std::string>();
  }
  if (value.contains("sha256") && !value.at("sha256").is_null()) {
    bootstrap_model.sha256 = value.at("sha256").get<std::string>();
  }
  return bootstrap_model;
}

InteractionSettings StateJsonSettingsCodecs::InteractionSettingsFromJson(
    const json& value) {
  InteractionSettings interaction;
  if (value.contains("system_prompt") && !value.at("system_prompt").is_null()) {
    interaction.system_prompt = value.at("system_prompt").get<std::string>();
  }
  if (value.contains("analysis_system_prompt") &&
      !value.at("analysis_system_prompt").is_null()) {
    interaction.analysis_system_prompt =
        value.at("analysis_system_prompt").get<std::string>();
  }
  interaction.thinking_enabled =
      value.value("thinking_enabled", interaction.thinking_enabled);
  if (value.contains("default_temperature") &&
      !value.at("default_temperature").is_null()) {
    interaction.default_temperature = value.at("default_temperature").get<double>();
  }
  if (value.contains("default_top_p") && !value.at("default_top_p").is_null()) {
    interaction.default_top_p = value.at("default_top_p").get<double>();
  }
  interaction.default_response_language =
      value.value("default_response_language", interaction.default_response_language);
  interaction.supported_response_languages =
      value.value("supported_response_languages", std::vector<std::string>{});
  interaction.follow_user_language =
      value.value("follow_user_language", interaction.follow_user_language);
  interaction.completion_policy =
      CompletionPolicyFromJson(value, "completion_policy");
  interaction.long_completion_policy =
      CompletionPolicyFromJson(value, "long_completion_policy");
  interaction.analysis_completion_policy =
      CompletionPolicyFromJson(value, "analysis_completion_policy");
  interaction.analysis_long_completion_policy =
      CompletionPolicyFromJson(value, "analysis_long_completion_policy");
  return interaction;
}

SkillsSettings StateJsonSettingsCodecs::SkillsSettingsFromJson(
    const json& value) {
  SkillsSettings skills;
  skills.enabled = value.value("enabled", skills.enabled);
  if (value.contains("factory_skill_ids") &&
      value.at("factory_skill_ids").is_array()) {
    for (const auto& item : value.at("factory_skill_ids")) {
      if (item.is_string()) {
        skills.factory_skill_ids.push_back(item.get<std::string>());
      }
    }
  }
  return skills;
}

BrowsingSettings StateJsonSettingsCodecs::BrowsingSettingsFromJson(
    const json& value) {
  BrowsingSettings browsing;
  browsing.enabled = value.value("enabled", browsing.enabled);
  if (value.contains("policy") && value.at("policy").is_object()) {
    BrowsingPolicySettings policy;
    const auto& policy_json = value.at("policy");
    policy.cef_enabled =
        policy_json.value("cef_enabled", policy.cef_enabled);
    policy.browser_session_enabled =
        policy_json.value("browser_session_enabled", policy.browser_session_enabled);
    policy.rendered_browser_enabled =
        policy_json.value("rendered_browser_enabled", policy.rendered_browser_enabled);
    policy.login_enabled =
        policy_json.value("login_enabled", policy.login_enabled);
    policy.response_review_enabled =
        policy_json.value("response_review_enabled", policy.response_review_enabled);
    policy.policy_version =
        policy_json.value("policy_version", policy.policy_version);
    policy.max_search_results =
        policy_json.value("max_search_results", policy.max_search_results);
    policy.max_fetch_bytes =
        policy_json.value("max_fetch_bytes", policy.max_fetch_bytes);
    if (policy_json.contains("allowed_domains") &&
        policy_json.at("allowed_domains").is_array()) {
      for (const auto& item : policy_json.at("allowed_domains")) {
        if (item.is_string()) {
          policy.allowed_domains.push_back(item.get<std::string>());
        }
      }
    }
    if (policy_json.contains("blocked_domains") &&
        policy_json.at("blocked_domains").is_array()) {
      for (const auto& item : policy_json.at("blocked_domains")) {
        if (item.is_string()) {
          policy.blocked_domains.push_back(item.get<std::string>());
        }
      }
    }
    if (policy_json.contains("blocked_targets") &&
        policy_json.at("blocked_targets").is_array()) {
      for (const auto& item : policy_json.at("blocked_targets")) {
        if (item.is_string()) {
          policy.blocked_targets.push_back(item.get<std::string>());
        }
      }
    }
    browsing.policy = std::move(policy);
  }
  return browsing;
}

ExternalAppHostConfig StateJsonSettingsCodecs::ExternalAppHostConfigFromJson(
    const json& value) {
  ExternalAppHostConfig app_host;
  app_host.address = value.value("address", std::string{});
  if (value.contains("ssh_key_path") && !value.at("ssh_key_path").is_null()) {
    app_host.ssh_key_path = value.at("ssh_key_path").get<std::string>();
  }
  if (value.contains("username") && !value.at("username").is_null()) {
    app_host.username = value.at("username").get<std::string>();
  }
  if (value.contains("password") && !value.at("password").is_null()) {
    app_host.password = value.at("password").get<std::string>();
  }
  return app_host;
}

}  // namespace naim
