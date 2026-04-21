#include "skills/skills_factory_http_service.h"

#include <utility>

#include <nlohmann/json.hpp>

namespace naim::controller {

namespace {

using nlohmann::json;

HttpResponse BuildJsonResponse(
    int status_code,
    const json& payload) {
  HttpResponse response;
  response.status_code = status_code;
  response.content_type = "application/json";
  response.body = payload.dump(2);
  return response;
}

}  // namespace

SkillsFactoryHttpService::SkillsFactoryHttpService(
    const ControllerRequestSupport& request_support,
    SkillsFactoryService service,
    std::optional<std::string> upstream_target)
    : request_support_(request_support),
      service_(std::move(service)),
      upstream_target_(std::move(upstream_target)) {}

bool SkillsFactoryHttpService::ShouldHandlePath(const std::string& path) {
  return path == "/api/v1/skills-factory" ||
         path == "/api/v1/skills-factory/groups" ||
         path == "/api/v1/skills-factory/groups/rename" ||
         path == "/api/v1/skills-factory/groups/delete" ||
         path.starts_with("/api/v1/skills-factory/");
}

std::string SkillsFactoryHttpService::BuildPathAndQuery(const HttpRequest& request) {
  if (request.query_params.empty()) {
    return request.path;
  }
  std::string path = request.path + "?";
  bool first = true;
  for (const auto& [key, value] : request.query_params) {
    if (!first) {
      path += "&";
    }
    first = false;
    path += key + "=" + value;
  }
  return path;
}

std::vector<std::pair<std::string, std::string>> SkillsFactoryHttpService::BuildProxyHeaders(
    const HttpRequest& request) {
  std::vector<std::pair<std::string, std::string>> headers;
  const auto content_type = request.headers.find("content-type");
  if (content_type != request.headers.end() && !content_type->second.empty()) {
    headers.emplace_back("Content-Type", content_type->second);
  }
  const auto accept = request.headers.find("accept");
  if (accept != request.headers.end() && !accept->second.empty()) {
    headers.emplace_back("Accept", accept->second);
  }
  return headers;
}

HttpResponse SkillsFactoryHttpService::ProxyRequest(const HttpRequest& request) const {
  if (!upstream_target_.has_value()) {
    throw std::runtime_error("skills factory upstream is not configured");
  }
  return SendControllerHttpRequest(
      ParseControllerEndpointTarget(*upstream_target_),
      request.method,
      BuildPathAndQuery(request),
      request.body,
      BuildProxyHeaders(request));
}

std::optional<HttpResponse> SkillsFactoryHttpService::HandleRequest(
    const std::string& db_path,
    const std::string& default_artifacts_root,
    const HttpRequest& request) const {
  if (!ShouldHandlePath(request.path)) {
    return std::nullopt;
  }

  if (upstream_target_.has_value()) {
    try {
      return ProxyRequest(request);
    } catch (const std::exception&) {
    }
  }

  return HandleRequestLocal(db_path, default_artifacts_root, request);
}

std::optional<HttpResponse> SkillsFactoryHttpService::HandleRequestLocal(
    const std::string& db_path,
    const std::string& default_artifacts_root,
    const HttpRequest& request) const {
  if (request.path == "/api/v1/skills-factory") {
    try {
      if (request.method == "GET") {
        return BuildJsonResponse(200, service_.BuildListPayload(db_path));
      }
      if (request.method == "POST") {
        return BuildJsonResponse(
            200,
            service_.CreateSkill(db_path, request_support_.ParseJsonRequestBody(request)));
      }
      return BuildJsonResponse(405, json{{"status", "method_not_allowed"}});
    } catch (const std::invalid_argument& error) {
      return BuildJsonResponse(
          400,
          json{{"status", "bad_request"}, {"message", error.what()}, {"path", request.path}});
    } catch (const std::runtime_error& error) {
      return BuildJsonResponse(
          404,
          json{{"status", "not_found"}, {"message", error.what()}, {"path", request.path}});
    } catch (const std::exception& error) {
      return BuildJsonResponse(
          500,
          json{{"status", "internal_error"}, {"message", error.what()}, {"path", request.path}});
    }
  }

  if (request.path == "/api/v1/skills-factory/groups") {
    try {
      if (request.method == "POST") {
        return BuildJsonResponse(
            200,
            service_.CreateGroup(db_path, request_support_.ParseJsonRequestBody(request)));
      }
      return BuildJsonResponse(405, json{{"status", "method_not_allowed"}});
    } catch (const std::invalid_argument& error) {
      return BuildJsonResponse(
          400,
          json{{"status", "bad_request"}, {"message", error.what()}, {"path", request.path}});
    } catch (const std::runtime_error& error) {
      return BuildJsonResponse(
          404,
          json{{"status", "not_found"}, {"message", error.what()}, {"path", request.path}});
    } catch (const std::exception& error) {
      return BuildJsonResponse(
          500,
          json{{"status", "internal_error"}, {"message", error.what()}, {"path", request.path}});
    }
  }

  if (request.path == "/api/v1/skills-factory/groups/rename") {
    try {
      if (request.method == "POST") {
        return BuildJsonResponse(
            200,
            service_.RenameGroup(db_path, request_support_.ParseJsonRequestBody(request)));
      }
      return BuildJsonResponse(405, json{{"status", "method_not_allowed"}});
    } catch (const std::invalid_argument& error) {
      return BuildJsonResponse(
          400,
          json{{"status", "bad_request"}, {"message", error.what()}, {"path", request.path}});
    } catch (const std::runtime_error& error) {
      return BuildJsonResponse(
          404,
          json{{"status", "not_found"}, {"message", error.what()}, {"path", request.path}});
    } catch (const std::exception& error) {
      return BuildJsonResponse(
          500,
          json{{"status", "internal_error"}, {"message", error.what()}, {"path", request.path}});
    }
  }

  if (request.path == "/api/v1/skills-factory/groups/delete") {
    try {
      if (request.method == "POST") {
        return BuildJsonResponse(
            200,
            service_.DeleteGroup(db_path, request_support_.ParseJsonRequestBody(request)));
      }
      return BuildJsonResponse(405, json{{"status", "method_not_allowed"}});
    } catch (const std::invalid_argument& error) {
      return BuildJsonResponse(
          400,
          json{{"status", "bad_request"}, {"message", error.what()}, {"path", request.path}});
    } catch (const std::runtime_error& error) {
      return BuildJsonResponse(
          404,
          json{{"status", "not_found"}, {"message", error.what()}, {"path", request.path}});
    } catch (const std::exception& error) {
      return BuildJsonResponse(
          500,
          json{{"status", "internal_error"}, {"message", error.what()}, {"path", request.path}});
    }
  }

  constexpr std::string_view kPrefix = "/api/v1/skills-factory/";
  if (!request.path.starts_with(kPrefix)) {
    return std::nullopt;
  }
  const std::string skill_id = request.path.substr(kPrefix.size());
  if (skill_id.empty() || skill_id.contains('/')) {
    return BuildJsonResponse(404, json{{"status", "not_found"}});
  }

  try {
    if (request.method == "GET") {
      return BuildJsonResponse(200, service_.BuildSkillPayload(db_path, skill_id));
    }
    if (request.method == "PUT") {
      return BuildJsonResponse(
          200,
          service_.UpdateSkill(
              db_path,
              skill_id,
              request_support_.ParseJsonRequestBody(request),
              false,
              default_artifacts_root));
    }
    if (request.method == "PATCH") {
      return BuildJsonResponse(
          200,
          service_.UpdateSkill(
              db_path,
              skill_id,
              request_support_.ParseJsonRequestBody(request),
              true,
              default_artifacts_root));
    }
    if (request.method == "DELETE") {
      return BuildJsonResponse(
          200,
          service_.DeleteSkill(db_path, skill_id, default_artifacts_root));
    }
    return BuildJsonResponse(405, json{{"status", "method_not_allowed"}});
  } catch (const std::invalid_argument& error) {
    return BuildJsonResponse(
        400,
        json{{"status", "bad_request"}, {"message", error.what()}, {"path", request.path}});
  } catch (const std::runtime_error& error) {
    return BuildJsonResponse(
        404,
        json{{"status", "not_found"}, {"message", error.what()}, {"path", request.path}});
  } catch (const std::exception& error) {
    return BuildJsonResponse(
        500,
        json{{"status", "internal_error"}, {"message", error.what()}, {"path", request.path}});
  }
}

}  // namespace naim::controller
