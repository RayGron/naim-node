#include "../include/model_library_http_service.h"

#include <stdexcept>
#include <utility>

using nlohmann::json;

ModelLibraryHttpService::ModelLibraryHttpService(Deps deps)
    : deps_(std::move(deps)) {}

std::optional<HttpResponse> ModelLibraryHttpService::HandleRequest(
    const std::string& db_path,
    const HttpRequest& request) const {
  if (request.path == "/api/v1/model-library") {
    if (request.method == "GET") {
      try {
        return deps_.build_json_response(
            200, deps_.build_model_library_payload(db_path), {});
      } catch (const std::exception& error) {
        return deps_.build_json_response(
            500,
            json{{"status", "internal_error"},
                 {"message", error.what()},
                 {"path", request.path}},
            {});
      }
    }
    if (request.method == "DELETE") {
      try {
        return deps_.delete_model_library_entry_by_path(db_path, request);
      } catch (const std::exception& error) {
        return deps_.build_json_response(
            500,
            json{{"status", "internal_error"},
                 {"message", error.what()},
                 {"path", request.path}},
            {});
      }
    }
    return deps_.build_json_response(
        405, json{{"status", "method_not_allowed"}}, {});
  }

  if (request.path == "/api/v1/model-library/download") {
    if (request.method != "POST") {
      return deps_.build_json_response(
          405, json{{"status", "method_not_allowed"}}, {});
    }
    try {
      return deps_.enqueue_model_library_download(request);
    } catch (const std::exception& error) {
      return deps_.build_json_response(
          500,
          json{{"status", "internal_error"},
               {"message", error.what()},
               {"path", request.path}},
          {});
    }
  }

  return std::nullopt;
}
