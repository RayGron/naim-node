#include "skills/skills_factory_http_service.h"

#include <utility>

#include <nlohmann/json.hpp>

namespace comet::controller {

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
    SkillsFactoryService service)
    : request_support_(request_support), service_(std::move(service)) {}

std::optional<HttpResponse> SkillsFactoryHttpService::HandleRequest(
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

}  // namespace comet::controller
