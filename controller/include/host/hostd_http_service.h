#pragma once

#include <optional>
#include <string>

#include <nlohmann/json.hpp>

#include "host/hostd_http_support.h"
#include "http/controller_http_transport.h"
#include "http/controller_http_types.h"
#include "host/host_registry_service.h"

class HostdHttpService {
 public:
  explicit HostdHttpService(HostdHttpSupport support);

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

  HostdHttpSupport support_;
};
