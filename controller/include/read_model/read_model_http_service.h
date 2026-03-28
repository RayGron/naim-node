#pragma once

#include <optional>
#include <string>

#include <nlohmann/json.hpp>

#include "http/controller_http_transport.h"
#include "http/controller_http_types.h"
#include "read_model/read_model_http_support.h"
#include "read_model/read_model_service.h"
#include "scheduler/scheduler_view_service.h"

class ReadModelHttpService {
 public:
  explicit ReadModelHttpService(ReadModelHttpSupport support);

  std::optional<HttpResponse> HandleRequest(
      const std::string& db_path,
      const HttpRequest& request) const;

 private:
  ReadModelHttpSupport support_;
};
