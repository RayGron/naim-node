#pragma once

#include <optional>

#include "http/controller_http_transport.h"
#include "http/controller_http_types.h"
#include "knowledge/knowledge_vault_service.h"

namespace naim::controller {

class KnowledgeVaultHttpService final {
 public:
  explicit KnowledgeVaultHttpService(KnowledgeVaultService service = {});

  std::optional<HttpResponse> HandleRequest(
      const std::string& db_path,
      const HttpRequest& request) const;

 private:
  static bool StartsWith(const std::string& value, const std::string& prefix);
  static std::string StripPrefix(const std::string& value, const std::string& prefix);

  KnowledgeVaultService service_;
};

}  // namespace naim::controller
