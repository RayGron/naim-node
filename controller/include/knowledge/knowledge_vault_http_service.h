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
  KnowledgeVaultService service_;
};

}  // namespace naim::controller
