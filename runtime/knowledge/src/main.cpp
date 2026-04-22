#include <exception>
#include <iostream>

#include "knowledge/knowledge_runtime_config_loader.h"
#include "knowledge/knowledge_server.h"

int main() {
  try {
    const naim::knowledge_runtime::KnowledgeRuntimeConfigLoader config_loader;
    naim::knowledge_runtime::KnowledgeRuntimeConfig config =
        config_loader.LoadFromEnvironment();

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
