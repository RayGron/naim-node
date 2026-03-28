#include "model/model_library_http_service.h"

#include <stdexcept>
#include <utility>

using nlohmann::json;

ModelLibraryHttpService::ModelLibraryHttpService(ModelLibraryHttpSupport support)
    : support_(std::move(support)) {}

std::optional<HttpResponse> ModelLibraryHttpService::HandleRequest(
    const std::string& db_path,
    const HttpRequest& request) const {
  if (request.path == "/api/v1/model-library") {
    if (request.method == "GET") {
      try {
        return support_.build_json_response(
            200, support_.model_library_service().BuildPayload(db_path), {});
      } catch (const std::exception& error) {
        return support_.build_json_response(
            500,
            json{{"status", "internal_error"},
                 {"message", error.what()},
                 {"path", request.path}},
            {});
      }
    }
    if (request.method == "DELETE") {
      try {
        return support_.model_library_service().DeleteEntryByPath(db_path, request);
      } catch (const std::exception& error) {
        return support_.build_json_response(
            500,
            json{{"status", "internal_error"},
                 {"message", error.what()},
                 {"path", request.path}},
            {});
      }
    }
    return support_.build_json_response(
        405, json{{"status", "method_not_allowed"}}, {});
  }

  if (request.path == "/api/v1/model-library/download") {
    if (request.method != "POST") {
      return support_.build_json_response(
          405, json{{"status", "method_not_allowed"}}, {});
    }
    try {
      return support_.model_library_service().EnqueueDownload(request);
    } catch (const std::exception& error) {
      return support_.build_json_response(
          500,
          json{{"status", "internal_error"},
               {"message", error.what()},
               {"path", request.path}},
          {});
    }
  }

  return std::nullopt;
}
