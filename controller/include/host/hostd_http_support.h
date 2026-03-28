#pragma once

#include <map>
#include <optional>
#include <string>

#include <nlohmann/json.hpp>

#include "host/host_registry_service.h"
#include "http/controller_http_transport.h"
#include "http/controller_http_types.h"

class HostdHttpSupport final {
 public:
  explicit HostdHttpSupport(comet::controller::HostRegistryEventSink host_registry_event_sink);

  HttpResponse build_json_response(
      int status_code,
      const nlohmann::json& payload,
      const std::map<std::string, std::string>& headers) const;
  std::string utc_now_sql_timestamp() const;
  std::string sql_timestamp_after_seconds(int seconds) const;
  std::optional<long long> timestamp_age_seconds(const std::string& value) const;
  const comet::controller::HostRegistryEventSink& host_registry_event_sink() const;

 private:
  comet::controller::HostRegistryEventSink host_registry_event_sink_;
};
