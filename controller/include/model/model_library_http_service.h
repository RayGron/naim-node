#pragma once

#include <optional>
#include <string>

#include <nlohmann/json.hpp>

#include "http/controller_http_transport.h"
#include "http/controller_http_types.h"
#include "model/model_library_http_support.h"
#include "model/model_library_service.h"

class ModelLibraryHttpService {
 public:
  explicit ModelLibraryHttpService(ModelLibraryHttpSupport support);

  std::optional<HttpResponse> HandleRequest(
      const std::string& db_path,
      const HttpRequest& request) const;

 private:
  ModelLibraryHttpSupport support_;
};
