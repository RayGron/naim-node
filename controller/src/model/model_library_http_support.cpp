#include "model/model_library_http_support.h"

#include "app/controller_composition_support.h"

ModelLibraryHttpSupport::ModelLibraryHttpSupport(
    const ModelLibraryService& model_library_service)
    : model_library_service_(model_library_service) {}

HttpResponse ModelLibraryHttpSupport::build_json_response(
    int status_code,
    const nlohmann::json& payload,
    const std::map<std::string, std::string>& headers) const {
  return comet::controller::composition_support::BuildJsonResponse(
      status_code,
      payload,
      headers);
}

const ModelLibraryService& ModelLibraryHttpSupport::model_library_service() const {
  return model_library_service_;
}
