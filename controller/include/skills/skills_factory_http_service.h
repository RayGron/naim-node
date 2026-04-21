#pragma once

#include <optional>
#include <string>
#include <vector>

#include "http/controller_http_transport.h"
#include "http/controller_http_types.h"
#include "infra/controller_request_support.h"
#include "skills/skills_factory_service.h"

namespace naim::controller {

class SkillsFactoryHttpService final {
 public:
  SkillsFactoryHttpService(
      const ControllerRequestSupport& request_support,
      SkillsFactoryService service,
      std::optional<std::string> upstream_target = std::nullopt);

  std::optional<HttpResponse> HandleRequest(
      const std::string& db_path,
      const std::string& default_artifacts_root,
      const HttpRequest& request) const;

 private:
  std::optional<HttpResponse> HandleRequestLocal(
      const std::string& db_path,
      const std::string& default_artifacts_root,
      const HttpRequest& request) const;
  HttpResponse ProxyRequest(const HttpRequest& request) const;
  static bool ShouldHandlePath(const std::string& path);
  static std::string BuildPathAndQuery(const HttpRequest& request);
  static std::vector<std::pair<std::string, std::string>> BuildProxyHeaders(
      const HttpRequest& request);

  const ControllerRequestSupport& request_support_;
  SkillsFactoryService service_;
  std::optional<std::string> upstream_target_;
};

}  // namespace naim::controller
