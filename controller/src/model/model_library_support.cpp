#include "model/model_library_support.h"

#include "app/controller_composition_support.h"
#include "app/controller_time_support.h"

ModelLibrarySupport::ModelLibrarySupport(
    const naim::controller::ControllerRequestSupport& request_support)
    : request_support_(request_support) {}

HttpResponse ModelLibrarySupport::build_json_response(
    int status_code,
    const nlohmann::json& payload,
    const std::map<std::string, std::string>& headers) const {
  return naim::controller::composition_support::BuildJsonResponse(
      status_code,
      payload,
      headers);
}

nlohmann::json ModelLibrarySupport::parse_json_request_body(
    const HttpRequest& request) const {
  return request_support_.ParseJsonRequestBody(request);
}

std::optional<std::string> ModelLibrarySupport::find_query_string(
    const HttpRequest& request,
    const std::string& key) const {
  return naim::controller::composition_support::FindQueryString(request, key);
}

std::string ModelLibrarySupport::utc_now_sql_timestamp() const {
  return naim::controller::ControllerTimeSupport::UtcNowSqlTimestamp();
}
