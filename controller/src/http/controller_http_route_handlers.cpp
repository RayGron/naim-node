#include "http/controller_http_route_handlers.h"

namespace naim::controller {

AuthHttpRouteHandler::AuthHttpRouteHandler(AuthHttpService& service) : service_(service) {}

std::optional<HttpResponse> AuthHttpRouteHandler::TryHandle(
    const std::string& db_path,
    const std::string&,
    const HttpRequest& request) const {
  return service_.HandleRequest(db_path, request);
}

HostdHttpRouteHandler::HostdHttpRouteHandler(HostdHttpService& service) : service_(service) {}

std::optional<HttpResponse> HostdHttpRouteHandler::TryHandle(
    const std::string& db_path,
    const std::string&,
    const HttpRequest& request) const {
  return service_.HandleRequest(db_path, request);
}

BundleHttpRouteHandler::BundleHttpRouteHandler(BundleHttpService& service) : service_(service) {}

std::optional<HttpResponse> BundleHttpRouteHandler::TryHandle(
    const std::string& db_path,
    const std::string& default_artifacts_root,
    const HttpRequest& request) const {
  return service_.HandleRequest(db_path, default_artifacts_root, request);
}

ModelLibraryHttpRouteHandler::ModelLibraryHttpRouteHandler(ModelLibraryHttpService& service)
    : service_(service) {}

std::optional<HttpResponse> ModelLibraryHttpRouteHandler::TryHandle(
    const std::string& db_path,
    const std::string&,
    const HttpRequest& request) const {
  return service_.HandleRequest(db_path, request);
}

PlaneHttpRouteHandler::PlaneHttpRouteHandler(PlaneHttpService& service) : service_(service) {}

std::optional<HttpResponse> PlaneHttpRouteHandler::TryHandle(
    const std::string& db_path,
    const std::string& default_artifacts_root,
    const HttpRequest& request) const {
  return service_.HandleRequest(db_path, default_artifacts_root, request);
}

SkillsFactoryHttpRouteHandler::SkillsFactoryHttpRouteHandler(
    SkillsFactoryHttpService& service)
    : service_(service) {}

std::optional<HttpResponse> SkillsFactoryHttpRouteHandler::TryHandle(
    const std::string& db_path,
    const std::string& default_artifacts_root,
    const HttpRequest& request) const {
  return service_.HandleRequest(db_path, default_artifacts_root, request);
}

ReadModelHttpRouteHandler::ReadModelHttpRouteHandler(ReadModelHttpService& service)
    : service_(service) {}

std::optional<HttpResponse> ReadModelHttpRouteHandler::TryHandle(
    const std::string& db_path,
    const std::string&,
    const HttpRequest& request) const {
  return service_.HandleRequest(db_path, request);
}

SchedulerHttpRouteHandler::SchedulerHttpRouteHandler(SchedulerHttpService& service)
    : service_(service) {}

std::optional<HttpResponse> SchedulerHttpRouteHandler::TryHandle(
    const std::string& db_path,
    const std::string& default_artifacts_root,
    const HttpRequest& request) const {
  return service_.HandleRequest(db_path, default_artifacts_root, request);
}

}  // namespace naim::controller
