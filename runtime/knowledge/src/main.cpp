#include "knowledge/knowledge_server.h"

#include <cstdlib>
#include <exception>
#include <iostream>

namespace {

std::string GetEnvOr(const char* name, const char* fallback) {
  const char* value = std::getenv(name);
  return value != nullptr && *value != '\0' ? std::string(value) : std::string(fallback);
}

std::string GetStorePath() {
  if (const char* value = std::getenv("NAIM_KNOWLEDGE_STORE_PATH");
      value != nullptr && *value != '\0') {
    return value;
  }
  return "/naim/knowledge/store";
}

int GetEnvIntOr(const char* name, int fallback) {
  const char* value = std::getenv(name);
  if (value == nullptr || *value == '\0') {
    return fallback;
  }
  return std::stoi(value);
}

}  // namespace

int main() {
  try {
    naim::knowledge_runtime::KnowledgeRuntimeConfig config;
    config.service_id = GetEnvOr("NAIM_KNOWLEDGE_SERVICE_ID", "kv_default");
    config.node_name = GetEnvOr("NAIM_NODE_NAME", "unknown");
    config.store_path = GetStorePath();
    config.status_path = GetEnvOr("NAIM_KNOWLEDGE_STATUS_PATH", "/naim/knowledge/status.json");
    config.listen_host = GetEnvOr("NAIM_KNOWLEDGE_LISTEN_HOST", "127.0.0.1");
    config.port = GetEnvIntOr("NAIM_KNOWLEDGE_PORT", 18200);

    std::cout << "[naim-knowledge] booting service=" << config.service_id
              << " node=" << config.node_name << "\n";
    std::cout << "[naim-knowledge] store_path=" << config.store_path.string() << "\n";
    std::cout << "[naim-knowledge] listen=" << config.listen_host << ":" << config.port << "\n";
    std::cout.flush();

    naim::knowledge_runtime::KnowledgeServer server(std::move(config));
    return server.Run();
  } catch (const std::exception& error) {
    std::cerr << "naim-knowledged: " << error.what() << "\n";
    return 1;
  }
}
