#pragma once

#include <functional>
#include <optional>
#include <string>

#include <nlohmann/json.hpp>

#include "controller_http_transport.h"
#include "controller_http_types.h"
#include "host_registry_service.h"

class HostdHttpService {
 public:
  using BuildJsonResponseFn = std::function<HttpResponse(
      int,
      const nlohmann::json&,
      const std::map<std::string, std::string>&)>;
  using UtcNowSqlTimestampFn = std::function<std::string()>;
  using SqlTimestampAfterSecondsFn = std::function<std::string(int)>;
  using TimestampAgeSecondsFn =
      std::function<std::optional<long long>(const std::string&)>;

  struct Deps {
    BuildJsonResponseFn build_json_response;
    UtcNowSqlTimestampFn utc_now_sql_timestamp;
    SqlTimestampAfterSecondsFn sql_timestamp_after_seconds;
    TimestampAgeSecondsFn timestamp_age_seconds;
    comet::controller::HostRegistryEventSink host_registry_event_sink;
  };

  explicit HostdHttpService(Deps deps);

  std::optional<HttpResponse> HandleRequest(
      const std::string& db_path,
      const HttpRequest& request) const;

 private:
  HttpResponse HandleRegister(
      const std::string& db_path,
      const HttpRequest& request) const;
  HttpResponse HandleHosts(
      const std::string& db_path,
      const HttpRequest& request) const;
  HttpResponse HandleHostPath(
      const std::string& db_path,
      const HttpRequest& request) const;
  HttpResponse HandleSessionOpen(
      const std::string& db_path,
      const HttpRequest& request) const;
  HttpResponse HandleSessionHeartbeat(
      const std::string& db_path,
      const HttpRequest& request) const;
  HttpResponse HandleNextAssignment(
      const std::string& db_path,
      const HttpRequest& request) const;
  HttpResponse HandleAssignmentAction(
      const std::string& db_path,
      const HttpRequest& request) const;
  HttpResponse HandleObservations(
      const std::string& db_path,
      const HttpRequest& request) const;
  HttpResponse HandleEvents(
      const std::string& db_path,
      const HttpRequest& request) const;
  HttpResponse HandleDiskRuntimeState(
      const std::string& db_path,
      const HttpRequest& request) const;
  HttpResponse HandleDiskRuntimeStateLoad(
      const std::string& db_path,
      const HttpRequest& request) const;

  Deps deps_;
};
