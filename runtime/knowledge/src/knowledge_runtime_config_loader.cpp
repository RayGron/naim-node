#include "knowledge/knowledge_runtime_config_loader.h"

#include <cstdlib>

namespace naim::knowledge_runtime {

KnowledgeRuntimeConfig KnowledgeRuntimeConfigLoader::LoadFromEnvironment() const {
  KnowledgeRuntimeConfig config;
  config.service_id = GetEnvOr("NAIM_KNOWLEDGE_SERVICE_ID", "kv_default");
  config.node_name = GetEnvOr("NAIM_NODE_NAME", "unknown");
  config.store_path = GetEnvOr("NAIM_KNOWLEDGE_STORE_PATH", "/naim/knowledge/store");
  config.status_path = GetEnvOr(
      "NAIM_KNOWLEDGE_STATUS_PATH",
      "/naim/knowledge/status.json");
  config.ready_path = GetEnvOr("NAIM_READY_FILE", "/tmp/naim-ready");
  config.listen_host = GetEnvOr("NAIM_KNOWLEDGE_LISTEN_HOST", "127.0.0.1");
  config.port = GetEnvIntOr("NAIM_KNOWLEDGE_PORT", 18200);
  return config;
}

std::string KnowledgeRuntimeConfigLoader::GetEnvOr(
    const char* name,
    const char* fallback) {
  const char* value = std::getenv(name);
  return value != nullptr && *value != '\0' ? std::string(value) : std::string(fallback);
}

int KnowledgeRuntimeConfigLoader::GetEnvIntOr(const char* name, int fallback) {
  const char* value = std::getenv(name);
  if (value == nullptr || *value == '\0') {
    return fallback;
  }
  return std::stoi(value);
}

}  // namespace naim::knowledge_runtime
