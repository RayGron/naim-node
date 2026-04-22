#pragma once

#include "knowledge/knowledge_server.h"

namespace naim::knowledge_runtime {

class KnowledgeRuntimeConfigLoader final {
 public:
  KnowledgeRuntimeConfig LoadFromEnvironment() const;

 private:
  static std::string GetEnvOr(const char* name, const char* fallback);
  static int GetEnvIntOr(const char* name, int fallback);
};

}  // namespace naim::knowledge_runtime
