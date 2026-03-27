#pragma once

#include <functional>
#include <optional>
#include <string>

#include <nlohmann/json.hpp>

#include "controller_http_transport.h"
#include "controller_http_types.h"

class ModelLibraryHttpService {
 public:
  using BuildJsonResponseFn = std::function<HttpResponse(
      int,
      const nlohmann::json&,
      const std::map<std::string, std::string>&)>;
  using BuildModelLibraryPayloadFn =
      std::function<nlohmann::json(const std::string&)>;
  using DeleteModelLibraryEntryByPathFn =
      std::function<HttpResponse(const std::string&, const HttpRequest&)>;
  using EnqueueModelLibraryDownloadFn =
      std::function<HttpResponse(const HttpRequest&)>;

  struct Deps {
    BuildJsonResponseFn build_json_response;
    BuildModelLibraryPayloadFn build_model_library_payload;
    DeleteModelLibraryEntryByPathFn delete_model_library_entry_by_path;
    EnqueueModelLibraryDownloadFn enqueue_model_library_download;
  };

  explicit ModelLibraryHttpService(Deps deps);

  std::optional<HttpResponse> HandleRequest(
      const std::string& db_path,
      const HttpRequest& request) const;

 private:
  Deps deps_;
};
