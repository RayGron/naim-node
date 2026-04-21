#pragma once

#include <optional>
#include <string>

#include "auth/auth_http_service.h"
#include "bundle/bundle_http_service.h"
#include "http/controller_http_route_handler.h"
#include "host/hostd_http_service.h"
#include "model/model_library_http_service.h"
#include "plane/plane_http_service.h"
#include "read_model/read_model_http_service.h"
#include "scheduler/scheduler_http_service.h"
#include "skills/skills_factory_http_service.h"

namespace naim::controller {

class AuthHttpRouteHandler final : public IControllerHttpRouteHandler {
 public:
  explicit AuthHttpRouteHandler(AuthHttpService& service);
  std::optional<HttpResponse> TryHandle(
      const std::string& db_path,
      const std::string& default_artifacts_root,
      const HttpRequest& request) const override;

 private:
  AuthHttpService& service_;
};

class HostdHttpRouteHandler final : public IControllerHttpRouteHandler {
 public:
  explicit HostdHttpRouteHandler(HostdHttpService& service);
  std::optional<HttpResponse> TryHandle(
      const std::string& db_path,
      const std::string& default_artifacts_root,
      const HttpRequest& request) const override;

 private:
  HostdHttpService& service_;
};

class BundleHttpRouteHandler final : public IControllerHttpRouteHandler {
 public:
  explicit BundleHttpRouteHandler(BundleHttpService& service);
  std::optional<HttpResponse> TryHandle(
      const std::string& db_path,
      const std::string& default_artifacts_root,
      const HttpRequest& request) const override;

 private:
  BundleHttpService& service_;
};

class ModelLibraryHttpRouteHandler final : public IControllerHttpRouteHandler {
 public:
  explicit ModelLibraryHttpRouteHandler(ModelLibraryHttpService& service);
  std::optional<HttpResponse> TryHandle(
      const std::string& db_path,
      const std::string& default_artifacts_root,
      const HttpRequest& request) const override;

 private:
  ModelLibraryHttpService& service_;
};

class PlaneHttpRouteHandler final : public IControllerHttpRouteHandler {
 public:
  explicit PlaneHttpRouteHandler(PlaneHttpService& service);
  std::optional<HttpResponse> TryHandle(
      const std::string& db_path,
      const std::string& default_artifacts_root,
      const HttpRequest& request) const override;

 private:
  PlaneHttpService& service_;
};

class SkillsFactoryHttpRouteHandler final : public IControllerHttpRouteHandler {
 public:
  explicit SkillsFactoryHttpRouteHandler(SkillsFactoryHttpService& service);
  std::optional<HttpResponse> TryHandle(
      const std::string& db_path,
      const std::string& default_artifacts_root,
      const HttpRequest& request) const override;

 private:
  SkillsFactoryHttpService& service_;
};

class ReadModelHttpRouteHandler final : public IControllerHttpRouteHandler {
 public:
  explicit ReadModelHttpRouteHandler(ReadModelHttpService& service);
  std::optional<HttpResponse> TryHandle(
      const std::string& db_path,
      const std::string& default_artifacts_root,
      const HttpRequest& request) const override;

 private:
  ReadModelHttpService& service_;
};

class SchedulerHttpRouteHandler final : public IControllerHttpRouteHandler {
 public:
  explicit SchedulerHttpRouteHandler(SchedulerHttpService& service);
  std::optional<HttpResponse> TryHandle(
      const std::string& db_path,
      const std::string& default_artifacts_root,
      const HttpRequest& request) const override;

 private:
  SchedulerHttpService& service_;
};

}  // namespace naim::controller
