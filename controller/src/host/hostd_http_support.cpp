#include "host/hostd_http_support.h"

#include "app/controller_composition_support.h"
#include "app/controller_time_support.h"

HostdHttpSupport::HostdHttpSupport(
    comet::controller::HostRegistryEventSink host_registry_event_sink)
    : host_registry_event_sink_(std::move(host_registry_event_sink)) {}

HttpResponse HostdHttpSupport::build_json_response(
    int status_code,
    const nlohmann::json& payload,
    const std::map<std::string, std::string>& headers) const {
  return comet::controller::composition_support::BuildJsonResponse(
      status_code,
      payload,
      headers);
}

std::string HostdHttpSupport::utc_now_sql_timestamp() const {
  return comet::controller::ControllerTimeSupport::UtcNowSqlTimestamp();
}

std::string HostdHttpSupport::sql_timestamp_after_seconds(int seconds) const {
  return comet::controller::ControllerTimeSupport::SqlTimestampAfterSeconds(seconds);
}

std::optional<long long> HostdHttpSupport::timestamp_age_seconds(
    const std::string& value) const {
  return comet::controller::ControllerTimeSupport::TimestampAgeSeconds(value);
}

const comet::controller::HostRegistryEventSink& HostdHttpSupport::host_registry_event_sink() const {
  return host_registry_event_sink_;
}
