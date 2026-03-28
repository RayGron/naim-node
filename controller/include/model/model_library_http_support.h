#pragma once

#include <map>
#include <string>

#include <nlohmann/json.hpp>

#include "http/controller_http_transport.h"
#include "http/controller_http_types.h"

class ModelLibraryService;

class ModelLibraryHttpSupport final {
 public:
  explicit ModelLibraryHttpSupport(const ModelLibraryService& model_library_service);

  HttpResponse build_json_response(
      int status_code,
      const nlohmann::json& payload,
      const std::map<std::string, std::string>& headers) const;
  const ModelLibraryService& model_library_service() const;

 private:
  const ModelLibraryService& model_library_service_;
};
